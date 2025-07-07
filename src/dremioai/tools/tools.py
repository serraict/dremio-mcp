#
#  Copyright (C) 2017-2025 Dremio Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from typing import (
    List,
    Dict,
    Any,
    Optional,
    Literal,
    TypeAlias,
    Union,
    Annotated,
    ClassVar,
    get_args,
    get_type_hints,
)

from pathlib import Path
from dataclasses import dataclass, asdict, field
from enum import auto, IntFlag
from dremioai import log
import re

import pandas as pd

from pathlib import Path

from dremioai.api.dremio import sql, projects, usage, engines, search
from dremioai.config import settings
from dremioai.config.tools import ToolType
from dremioai.api.prometheus import vm
from dremioai.api.dremio.catalog import get_schema, get_lineage, get_descriptions
from csv import reader
from io import StringIO
from sqlglot import parse_one
from sqlglot import expressions

logger = log.logger(__name__)


@dataclass
class Property:
    type: Optional[str] = "string"
    description: Optional[str] = ""


@dataclass
class Parameters:
    # parameters: Optional[Dict[str, Parameter]] = field(default_factory=dict)
    type: Optional[str] = "object"
    properties: Optional[Dict[str, Property]] = field(default_factory=dict)
    required: Optional[List[str]] = field(default_factory=list)


@dataclass
class Function:
    name: str
    description: str
    parameters: Parameters


@dataclass
class Tool:
    """
    A wrapper for integrating the same tool with LangChain based tool calling agents.
    """

    type: Optional[str] = "function"
    function: Optional[Function] = None

    def as_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if not self.function.parameters.properties:
            del d["function"]["parameters"]
        return d


class Tools:
    def __init__(self, uri=None, pat=None, project_id=None):
        settings.instance().with_overrides(
            {"dremio.uri": uri, "dremio.pat": pat, "dremio.project_id": project_id}
        )

    @property
    def dremio_uri(self):
        return settings.instance().dremio.uri

    @property
    def pat(self):
        return settings.instance().dremio.pat

    @property
    def project_id(self):
        return settings.instance().dremio.project_id

    async def invoke(self):
        raise NotImplementedError("Subclasses should implement this method")

    def get_parameters(self):
        return Parameters()

    # support for LangChain tools as compatiblity
    def as_tool(self):
        return Tool(
            function=Function(
                name=self.__class__.__name__,
                description=self.invoke.__doc__,
                parameters=self.get_parameters(),
            )
        )


JobType: TypeAlias = Union[
    List[Literal["UI", "ACCELERATION", "INTERNAL", "EXTERNAL"]], str
]
StatusType: TypeAlias = Union[List[Literal["COMPLETED", "CANCELED", "FAILED"]], str]


def _get_class_var_hints(tool: Tools, name: str) -> bool:
    if class_var := get_type_hints(tool, include_extras=True).get(name):
        if cls_args := get_args(class_var):
            if (annot := get_args(cls_args[0])) and len(annot) == 2:
                return annot[-1]


get_for = lambda tool: _get_class_var_hints(tool, "For")
get_project_id_required = lambda tool: _get_class_var_hints(tool, "project_id_required")


def is_tool_for(
    tool: Tools, tool_type: ToolType, dremio: settings.Dremio = None
) -> bool:
    if dremio is None and settings.instance().dremio:
        dremio = settings.instance().dremio

    if project_id_required := get_project_id_required(tool):
        if dremio is not None and dremio.project_id is None:
            return False

    if (For := get_for(tool)) is not None:
        if For & ToolType.EXPERIMENTAL and not dremio.enable_experimental:
            return False
        return (For & tool_type) != 0  # == tool_type
    return False


class GetFailedJobDetails(Tools):
    For: ClassVar[Annotated[ToolType, ToolType.FOR_SELF]]

    def group_by(self, df, by):
        return df.groupby(by).size().reset_index(name="count").to_dict(orient="records")

    async def invoke(self) -> Dict[str, Any]:
        """Get the stats and details of failed or canceled jobs executed in the Dremio cluster in the past 7 days
        along with a split by job type

        Returns:
            A dictionary with the following keys:
            - Number of jobs in over 7 days
            - Job categories: A list of dictionaries with the following keys:
                - day: date of the job
                - query_type: type of the job
                - cnt: count of jobs
            - Job count by day, queryType and engine: A list of dictionaries with the following keys
                - day: date of the job
                - queryType: type of the job
                - engine: engine used
                - count: count of jobs
            - Job count by day, queryType and user: A list of dictionaries with the following keys
                - day: date of the job
                - queryType: type of the job
                - user: user who submitted the job
                - count: count of jobs
            - Job count by day, queriedDataset and state: A list of dictionaries with the following keys
                - day: date of the job
                - queriedDataset: dataset queried
                - state: state of the job
                - count: count of jobs
            - Job count by day, queryType and error: A list of dictionaries with the following keys
                - day: date of the job
                - queryType: type of the job
                - error: error message
                - count: count of jobs
        """
        table = (
            "sys.project.jobs_recent"
            if settings.instance().dremio.project_id
            else "sys.jobs_recent"
        )
        query = f"""/* dremioai: submitter={self.__class__.__name__} */
            select job_id as id,
            query_type as queryType,
            status as state,
            submitted_ts as startTime,
            query,
            (final_state_epoch_millis - submitted_epoch_millis) / 1000 as duration,
            queried_datasets as queriedDatasets,
                    user_name as "user",
            engine,
            error_msg
            from   {table}
            where to_date(submitted_ts) >= current_date - interval '7' day
            and status in ('CANCELED', 'FAILED')"""
        try:
            jdf = await sql.run_query(query=query, use_df=True)
            jdf["date"] = jdf["startTime"].dt.date

            # lookup only those who have erorrs to get detailed error messages
            return {
                "Number of jobs over 7 days": jdf.shape[0],
                "Job categories by day, queryType and state": self.group_by(
                    jdf, ["date", "queryType", "state"]
                ),
                "Job count by day, queryType and engine": self.group_by(
                    jdf, ["date", "queryType", "engine"]
                ),
                "Job count by day, queryType, user": self.group_by(
                    jdf, ["date", "queryType", "user"]
                ),
                "Job count by day, queriedDataset and state": self.group_by(
                    jdf.explode("queriedDatasets"), ["date", "queriedDatasets", "state"]
                ),
                "Job count by day, queryType and error": self.group_by(
                    jdf, ["date", "queryType", "error_msg"]
                ),
            }
        except RuntimeError as e:
            return {
                "error": str(e),
                "message": "The query failed. Please check the syntax and try again",
            }


class RunSqlQuery(Tools):
    For: ClassVar[Annotated[ToolType, ToolType.FOR_SELF | ToolType.FOR_DATA_PATTERNS]]
    _safe = [
        expressions.Select,
        expressions.With,
        expressions.Union,
    ]

    @staticmethod
    def ensure_query_allowed(s: str):
        if settings.instance().dremio.allow_dml:
            return

        try:
            q = parse_one(s)
            if any(isinstance(q, t) for t in RunSqlQuery._safe):
                return
        except:
            if not re.search(
                r"\b(drop|insert|update|truncate|delete|copy into|alter|create)\b",
                s,
                re.IGNORECASE,
            ):
                return
        raise ValueError(
            "The query contains a DML statement. Only select queries are allowed"
        )

    async def invoke(self, s: str) -> Dict[str, Any]:
        """Run a SELECT sql query on the Dremio cluster and return the results.
        Ensure that SQL keywords like 'day', 'month', 'count', 'table' etc are enclosed in double quotes
        You are premitted to run only SELECT queries. No DML statements are allowed.

        Args:
            s: sql query
        """
        RunSqlQuery.ensure_query_allowed(s)
        try:
            s = f"/* dremioai: submitter={self.__class__.__name__} */\n{s}"
            df = await sql.run_query(query=s, use_df=True)

            # Return in a structured format for better MCP client parsing
            return {
                "rows": df.to_dict(orient="records"),
                "columns": list(df.columns),
                "row_count": len(df),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            }
        except RuntimeError as e:
            return {
                "error": str(e),
                "message": "The query failed. Please check the syntax and try again",
            }

    def get_parameters(self):
        return Parameters(
            properties={
                "sql": Property(type="string", description="The sql query to run")
            },
            required=["sql"],
        )


class BuildUsageReport(Tools):
    For: ClassVar[Annotated[ToolType, ToolType.FOR_SELF]]
    project_id_required: ClassVar[Annotated[bool, True]]

    async def invoke(
        self, by: Optional[Literal["PROJECT", "ENGINE"]] = "ENGINE"
    ) -> Dict[str, Any]:
        """Build a usage report for the project grouped by engines for the past 7 days

        Hint: This is useful to plot a visualization

        Args:
            by: grouping the usage by 'PROJECT' or 'ENGINE'
        """
        _, projects_usage, engines_usage = await usage.get_consolidated_usage()
        if by == "PROJECT":
            return projects_usage.to_dict(orient="records")
        return engines_usage.to_dict(orient="records")


class Resource(Tools):
    @property
    def resource_path(self):
        raise NotImplementedError("Subclasses should implement this method")


class GetNameOfJobsRecentTable(Tools):
    For: ClassVar[Annotated[ToolType, ToolType.FOR_SELF]]

    async def invoke(self) -> Dict[str, str]:
        """Gets the schema full name of the table that stores the jobs information"""
        return {"name": "sys.project.jobs_recent"}


class Hints(Resource):
    For: ClassVar[Annotated[ToolType, ToolType.FOR_SELF]]

    @property
    def resource_path(self):
        return "dremio://hints"

    async def invoke(self) -> Dict[str, str]:
        """Dremio cluster has few key diminsions that can be used to analyze and optimize the cluster.
        Looking at the number of jobs and its statistics and failure rates, and overall system usage
        """
        return self.invoke.__doc__


class GetUsefulSystemTableNames(Tools):
    For: ClassVar[Annotated[ToolType, ToolType.FOR_SELF | ToolType.FOR_DATA_PATTERNS]]

    async def invoke(self) -> List[Dict[str, str]]:
        """Gets the names of system tables in the dremio cluster, useful for various analysis.
        Use Get Schema of Table tool to get the schema of the table"""
        return [
            {
                "table_name": 'information_schema."tables"',
                "description": (
                    "Information about tables in this cluster. "
                    "Be sure to filter out SYSTEM_TABLE for "
                    "looking at user tables. "
                    "You must encapsulate TABLES in double quotes."
                ),
            }
        ]


class GetSchemaOfTable(Tools):
    For: ClassVar[Annotated[ToolType, ToolType.FOR_SELF | ToolType.FOR_DATA_PATTERNS]]

    async def invoke(self, table_name: str) -> List[Dict[str, str]]:
        """Gets the schema of the given table.

        Args:
            table_name: name of the table, including the schema

        Returns:
            A list containing a dictionary with information about the table.
            The field "fields" is a list of dictionaries that give column
            names and types. Optionally "text" field and "tag" field can
            provide more information about the table
        """
        paths = list(reader(StringIO(table_name), delimiter="."))
        result = await get_schema(paths[0], include_tags=True)
        if result and "sql" in result:
            del result["sql"]

        if result:
            # Convert complex fields to strings for MCP client compatibility
            formatted_result = {}
            for key, value in result.items():
                if key == "path" and isinstance(value, list):
                    formatted_result[key] = ".".join(value)
                elif key == "fields" and isinstance(value, list):
                    # Convert fields list to a readable string format
                    field_strings = []
                    for field_dict in value:
                        if isinstance(field_dict, dict):
                            field_name = field_dict.get("name", "unknown")
                            field_type = field_dict.get("type", {})
                            if isinstance(field_type, dict):
                                type_name = field_type.get("name", "unknown")
                            else:
                                type_name = str(field_type)
                            field_strings.append(f"{field_name}: {type_name}")
                    formatted_result[key] = "\n".join(field_strings)
                elif key == "tags" and isinstance(value, list):
                    # Convert tags list to comma-separated string
                    if value:
                        tag_str = ", ".join(str(tag) for tag in value)
                    else:
                        tag_str = ""
                    formatted_result[key] = tag_str
                else:
                    if value is not None:
                        formatted_result[key] = str(value)
                    else:
                        formatted_result[key] = ""

            return [formatted_result]
        return []


class GetTableOrViewLineage(Tools):
    For: ClassVar[Annotated[ToolType, ToolType.FOR_SELF | ToolType.FOR_DATA_PATTERNS]]

    async def invoke(self, table_name: Union[str, List[str]]) -> Dict[str, Any]:
        """Finds the lineage of a table or view in the Dremio cluster

        Args:
            table_name: name of the table or view, including the schema. Be sure to quote the table name if it contains special characters

        Returns:
            A json representation with the lineage of the table or view.
        """
        return await get_lineage(table_name)


class SemanticSearch(Tools):
    For: ClassVar[
        Annotated[
            ToolType,
            ToolType.FOR_SELF | ToolType.FOR_DATA_PATTERNS | ToolType.EXPERIMENTAL,
        ]
    ]

    async def invoke(
        self, query: str, category: Optional[str] = None
    ) -> Dict[str, Any]:
        """Runs a semantic search on the Dremio cluster using the given query

        Args:
            query: The query to run
            category: Optionally a category to search for. One of TABLE, VIEW, JOB, SOURCE, FOLDER. Search all categories if unspecified

        Returns:
            A json representation with the results of the search
        """
        if category:
            category = search.Category[category.upper()]
        sq = search.Search(query=query, filter=category)
        res = await search.get_search_results(sq)
        return res.model_dump_json(exclude_none=True, indent=2)


def _subclasses(cls):
    for sub in cls.__subclasses__():
        yield from _subclasses(sub)
        yield sub


def get_tools(For: ToolType = None) -> List[Tools]:
    return [
        sc
        for sc in _subclasses(Tools)
        if sc is not Resource
        and not issubclass(sc, Resource)
        and (For is None or is_tool_for(sc, For))
    ]


def get_resources(For: ToolType = None):
    return [
        sc
        for sc in _subclasses(Resource)
        if sc is not Resource and (For is None or is_tool_for(sc, For))
    ]


def system_prompt():
    For = settings.instance().tools.server_mode
    get_tools_prompt = lambda t: "\n\t".join(t.invoke.__doc__.splitlines())
    all_tools = "\n".join(
        f"{t.__name__}: {get_tools_prompt(t)}"
        for t in (get_tools(For) + get_resources(For))
    )

    return f"""
    You are helpful AI bot with access to several tools for analyzing Dremio cluster, data, tables and jobs.
    Note:
    - In general prefer to illustrate results using interactive graphical plots
    - Use UNNEST instead of FLATTEN for arrays like queriedDatasets
    - Use ARRAY_TO_STRING([array], ',') to convert arrays to strings
    - Make sure to ensure reserved words like count, etc are enclosed in double quotes
    - Components in paths to views and tables must be double-quoted.
    - You must distinguish between user requests that intend to get a result of a SQL query or to generate SQL. The result of the former is the SQL query's result, the result of the latter is a SQL query.
    - You must use correct SQL syntax, you may use "EXPLAIN" to validate SQL or run it with LIMIT 1 to validate the syntax.
    - You must use the GetDescriptionOfTableOrSchema tool to get the descriptions of multiple tables and schemas before deciding the relevance.
    - You must consider views/tables in all search results not just top 1 or 2. The search is not perfect.
    - Consider sampling rows from multiple tables/views to understand what's in the data before deciding what table to use.
    - If the user prompt is in non English language, you must first translate it to English before attempting to search. Respond in the language of the user's prompt.
    - You must check your answer before finalizing the Result.
    - You must use various SQL select statements to calculate statistics and distribution of columns from the table;
    - You must use GetSchemaOfTable tool to get the schema of the table before running any queries on it.
    - You must use organization ids instead of name when generating a final report
    """


class GetRelevantMetrics(Tools):
    For: ClassVar[Annotated[ToolType, ToolType.FOR_PROMETHEUS]]

    async def invoke(self) -> Dict[str, Any]:
        """
        Get the names and descriptions of the relevant prometheus metrics for the Dremio cluster.
        A metric that shares the same value for label 'daas_dremio_com_coordinator_project_id'
        belongs to the same project

        Returns: A dictionary with
            - key: name of the metric
            - value: description of the metric
        """
        return {
            "jobs_total": "Total number of jobs executed in the Dremio cluster",
            "jobs_failed_total": "Total number of failed jobs executed in the Dremio cluster",
            "jobs_command_pool_queue_size": "Total number of jobs queued before planning",
            "jvm_gc_pause_seconds": "Indicates how long the JVM was paused for garbage collection, and also is a rubric to know if the system is in use",
            "memory_heap_usage": "Indicates the amount of memory used by the JVM",
            "memory_heap_committed": "Indicates the amount of memory committed by the JVM",
            "dremio_engine_executors": "Number of executors running in the Dremio engine. It correlates to dremio_engine_replica_running using engine_id label",
            "dremio_engine_replica_running": "Number of running replicas in the Dremio engine. It correlates to dremio_engine_executors using engine_id label",
        }


class GetMetricSchema(Tools):
    For: ClassVar[Annotated[ToolType, ToolType.FOR_PROMETHEUS]]

    async def invoke(self, metric: str) -> Dict[str, Any]:
        """
        Given the name of the metric, this will return all the labels you can expect to see
        for that metric.

        Args:
          metric: The name of the metric

        Returns: A dictionary with
            - key: name of the label
            - value: a sample value of the label
        """
        return await vm.get_metrics_schema(metric)


class RunPromQL(Tools):
    For: ClassVar[Annotated[ToolType, ToolType.FOR_PROMETHEUS]]

    async def invoke(self, promql_query: str) -> Dict[str, Any]:
        """
        Runs a prometheus query, over the last 7 days and returns the results

        Args:
          promql_query: The PromQL query to run
        """
        df = await vm.get_promql_result(
            promql_query, start="-7d", step="1h", use_df=True
        )
        # Return in a structured format for better MCP client parsing
        return {
            "metrics": df.to_dict(orient="records"),
            "columns": list(df.columns),
            "row_count": len(df),
        }


class GetDescriptionOfTableOrSchema(Tools):
    For: ClassVar[Annotated[ToolType, ToolType.FOR_SELF | ToolType.FOR_DATA_PATTERNS]]

    async def invoke(self, name: Union[List[str], str]) -> Dict[str, Any]:
        """
        Given one or more table names or schema names, this will return the description of the table or schema, if any exists
        as well as the description of any parent schemas

        Args:
          name: The name of the table or schema or a list of names of tables or schemas

        Returns: A dictionary with
            - key: a part of the table or schema name's heirarchy
            - value: a dictionary with the description and tags
        """
        if isinstance(name, str):
            name = [name]
        return await get_descriptions(name)
