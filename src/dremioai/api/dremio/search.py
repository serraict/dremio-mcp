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

from pydantic import (
    BaseModel,
    Field,
    AfterValidator,
    ValidationError,
    ConfigDict,
    field_validator,
)
from typing import (
    Annotated,
    List,
    Dict,
    AnyStr,
    Any,
    Literal,
    Union,
    Optional,
)
from dremioai.api.util import UStrEnum
from datetime import datetime
from enum import auto, StrEnum
from dremioai.config import settings
from dremioai.api.transport import DremioAsyncHttpClient as AsyncHttpClient


class QueryType(UStrEnum):
    UI_RUN = auto()
    UI_PREVIEW = auto()
    UI_INTERNAL_PREVIEW = auto()
    UI_INTERNAL_RUN = auto()
    UI_EXPORT = auto()
    ODBC = auto()
    JDBC = auto()
    REST = auto()
    ACCELERATOR_CREATE = auto()
    ACCELERATOR_DROP = auto()
    UNKNOWN = auto()
    PREPARE_INTERNAL = auto()
    ACCELERATOR_EXPLAIN = auto()
    UI_INITIAL_PREVIEW = auto()
    FLIGHT = auto()
    METADATA_REFRESH = auto()
    INTERNAL_ICEBERG_METADATA_DROP = auto()
    D2D = auto()
    ACCELERATOR_OPTIMIZE = auto()
    COPY_ERRORS_PLAN = auto()


class JobStatus(UStrEnum):
    NOT_SUBMITTED = auto()
    STARTING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    CANCELED = auto()
    FAILED = auto()
    CANCELLATION_REQUESTED = auto()
    ENQUEUED = auto()
    PLANNING = auto()
    PENDING = auto()
    METADATA_RETRIEVAL = auto()
    QUEUED = auto()
    ENGINE_START = auto()
    EXECUTION_PLANNING = auto()
    INVALID_STATE = auto()


class Category(StrEnum):
    JOB = auto()
    VIEW = auto()
    TABLE = auto()
    FOLDER = auto()
    UDF = auto()
    SPACE = auto()
    REFLECTION = auto()
    SCRIPT = auto()
    SOURCE = auto()


class UserOrRole(UStrEnum):
    USER_TYPE_UNSPECIFIED = auto()
    USER_TYPE_USER = auto()
    USER_TYPE_ROLE = auto()


class EnterpriseDatasetType(UStrEnum):
    TABLE = auto()
    VIEW = auto()


class EnterpriseSearchUserOrRoleObject(BaseModel):
    id: Optional[str] = None
    type: Optional[UserOrRole] = None
    username: Optional[str] = None
    rolename: Optional[str] = None


class EnterpriseDatasetObject(BaseModel):
    type: Optional[str] = Field(None, alias="datasetType")
    path: Optional[List[str]] = Field(None, alias="datasetPath")


class EnterpriseSearchJobObject(BaseModel):
    id: Optional[str] = None
    queried_ds: Optional[List[EnterpriseDatasetObject]] = Field(
        None, alias="queriedDatasets"
    )
    sql: Optional[str] = None
    job_type: Optional[QueryType] = Field(None, alias="jobType")
    user: Optional[EnterpriseSearchUserOrRoleObject] = None
    start_time: Optional[datetime] = Field(None, alias="startTime")
    finish_time: Optional[datetime] = Field(None, alias="finishTime")
    job_status: Optional[JobStatus] = Field(None, alias="jobStatus")
    error: Optional[str] = None


class EnterpriseSearchScriptObject(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    owner: Optional[EnterpriseSearchUserOrRoleObject] = None
    description: Optional[str] = None
    content: Optional[str] = None
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    modified_at: Optional[datetime] = Field(default=None, alias="lastModifiedAt")


class EnterpriseSearchReflectionObject(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = Field(default=None, alias="datasetType")
    path: Optional[List[str]] = Field(default=None, alias="datasetPath")
    branch: Optional[str] = Field(default=None, alias="datasetBranch")
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    modified_at: Optional[datetime] = Field(default=None, alias="lastModifiedAt")


class EnterpriseSearchCatalogObject(BaseModel):
    path: Optional[List[str]] = None
    sub_paths: Optional[List[str]] = Field(default=None, alias="subPaths")
    type: Optional[str] = None
    branch: Optional[str] = None
    labels: Optional[List[str]] = None
    wiki: Optional[str] = None
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    modified_at: Optional[datetime] = Field(default=None, alias="lastModifiedAt")
    func_sql: Optional[str] = Field(default=None, alias="functionSql")
    owner: Optional[EnterpriseSearchUserOrRoleObject] = None


class EnterpriseSearchResultsObject(BaseModel):
    category: Optional[Category] = None
    job: Optional[EnterpriseSearchJobObject] = Field(default=None, alias="jobObject")
    script: Optional[EnterpriseSearchScriptObject] = Field(
        default=None, alias="scriptObject"
    )
    reflection: Optional[EnterpriseSearchReflectionObject] = Field(
        default=None, alias="reflectionObject"
    )
    catalog: Optional[EnterpriseSearchCatalogObject] = Field(
        default=None, alias="catalogObject"
    )


class EnterpriseSearchResults(BaseModel):
    session_id: Optional[str] = Field(default=None, alias="sessionId")
    next_page_token: Optional[str] = Field(default=None, alias="nextPageToken")
    results: Optional[List[EnterpriseSearchResultsObject]] = Field(default_factory=list)
    error: Optional[str] = Field(default=None, alias="errorMessage")
    more: Optional[str] = Field(default=None, alias="moreInfo")


class Search(BaseModel):
    max_results: Optional[int] = Field(default=50, alias="maxResults")
    next_page_token: Optional[str] = Field(default=None, alias="pageToken")
    filter: Optional[Union[str, List[Category]]] = ""
    query: str = None

    @field_validator("filter", mode="after")
    @classmethod
    def validate_filter(cls, v: Union[str, List[Category]]) -> str:
        if isinstance(v, str) and v:
            v = f'category in ["{Category[v.upper()].name}"]'
        elif isinstance(v, list):
            v = ",".join([f'"{c.name}"' for c in v if isinstance(c, Category)])
            v = f"category in [{v}]"
        else:
            v = ""
        return v

    model_config: ConfigDict = ConfigDict(serialize_by_alias=True)


class EnterpriseSearchResultsWrapper(BaseModel):
    results: List[EnterpriseSearchResultsObject] = Field(default_factory=list)


async def get_search_results(search: str | Search) -> EnterpriseSearchResultsWrapper:
    if isinstance(search, str):
        search = Search(query=search)

    client = AsyncHttpClient(
        settings.instance().dremio.uri, settings.instance().dremio.pat
    )

    result = []
    response = await client.post(
        "/api/v3/search",
        body=search.model_dump(exclude_none=True),
        deser=EnterpriseSearchResults,
    )
    while response.results and response.error is None and response.more is None:
        result.extend(response.results)
        if response.next_page_token is None:
            break
        search.next_page_token = response.next_page_token
        response = await client.post(
            "/api/v3/search",
            body=search.model_dump(exclude_none=True),
            deser=EnterpriseSearchResults,
        )

    return EnterpriseSearchResultsWrapper(results=result)
