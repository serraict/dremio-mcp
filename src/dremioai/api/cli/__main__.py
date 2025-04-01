#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#

from typer import Typer, Option, BadParameter, Argument
from rich import print_json as pj, print as pp
from typing import Annotated, Optional, List
from pathlib import Path
from dremioai.api.dremio import catalog, sql
from dremioai.api.dremio.ui import jobs
import asyncio
from dremioai.log import configure, set_level
from dremioai.config import settings

from dremioai.api.cli.engines import app as engines_app
from dremioai.api.cli.prometheus import app as prometheus_app


def common_args(
    config_file: Annotated[
        Path,
        Option("-c", "--config", help="The config file with Dremio connection details"),
    ],
):
    settings.configure(config_file)


app = Typer(context_settings=dict(help_option_names=["-h", "--help"]))

catalog_app = Typer(
    context_settings=dict(help_option_names=["-h", "--help"]), callback=common_args
)
sql_app = Typer(context_settings=dict(help_option_names=["-h", "--help"]))
jobs_app = Typer(context_settings=dict(help_option_names=["-h", "--help"]))

app.add_typer(catalog_app, name="catalog", help="Run catalog oriented commands")
app.add_typer(sql_app, name="sql", help="Run SQL commands")
app.add_typer(
    jobs_app, name="jobs", help="Run commands related to job history and details"
)
app.add_typer(engines_app)
app.add_typer(prometheus_app)


@catalog_app.command(name="lineage")
def run_catalog(dataset_id: Annotated[str, Option(...)]):
    lineage = asyncio.run(catalog.get_lineage(dataset_id))
    pp(lineage)


# _qg = "Query / Job ID "
@sql_app.command("run")
def run_sql(
    uri: Annotated[str, Option(envvar="DREMIO_URI", show_envvar=True, default=...)],
    project_id: Annotated[
        str, Option(envvar="DREMIO_PROJECT_ID", show_envvar=True, default=...)
    ],
    pat: Annotated[str, Option(envvar="DREMIO_PAT", show_envvar=True, default=...)],
    query: Annotated[
        Optional[str],
        Option(
            help="SQL query to run. If it startswith'@' then treat the rest as a filename"
        ),
    ] = None,
    job_id: Annotated[
        Optional[str], Option(help="Retrieve results of existing job")
    ] = None,
    use_df: Annotated[
        Optional[bool], Option(help="Convert results to pandas dataframe")
    ] = False,
):
    if query is None and job_id is None:
        raise BadParameter("Either query or job_id must be provided")

    if query is not None:
        query = Path(query[1:]).read_text().strip() if query.startswith("@") else query
        result = asyncio.run(sql.run_query(uri, pat, project_id, query, as_df=use_df))
    else:
        result = asyncio.run(
            sql.get_results(project_id, job_id, as_df=use_df, uri=uri, pat=pat)
        )

    pp(result if use_df else [r for jr in result for r in jr.rows])


@jobs_app.command("list")
def list_jobs(
    uri: Annotated[str, Option(envvar="DREMIO_URI", default=...)],
    project_id: Annotated[str, Option(envvar="DREMIO_PROJECT_ID", default=...)],
    pat: Annotated[str, Option(envvar="DREMIO_PAT", default=...)],
    limit: Annotated[
        Optional[int], Option(help="Limit number of jobs to return")
    ] = 5000,
    job_type: Annotated[
        Optional[List[jobs.JobTypeFilter]], Option(help="Job type to filter on")
    ] = None,
    job_status: Annotated[
        Optional[List[jobs.JobStatusFilter]], Option(help="Job type to filter on")
    ] = None,
    use_df: Annotated[
        Optional[bool], Option(help="Convert results to pandas dataframe")
    ] = False,
):
    result = asyncio.run(
        jobs.get_jobs(
            uri,
            pat,
            project_id,
            max_results=limit,
            job_type=job_type,
            job_status=job_status,
            use_df=use_df,
        )
    )
    pp(result)


@jobs_app.command("detail")
def job_detail(
    uri: Annotated[str, Option(envvar="DREMIO_URI", default=...)],
    project_id: Annotated[str, Option(envvar="DREMIO_PROJECT_ID", default=...)],
    pat: Annotated[str, Option(envvar="DREMIO_PAT", default=...)],
    job_ids: Annotated[List[str], Argument(help="Job IDs to retrieve details for")],
    get_profile: Annotated[
        Optional[bool], Option(help="Get profile instead of job detail")
    ] = False,
    header_only: Annotated[
        Optional[bool],
        Option(
            help="Get only the profile header (for --get-profile) instead of job detail"
        ),
    ] = False,
    use_df: Annotated[
        Optional[bool], Option(help="Convert results to pandas dataframe")
    ] = False,
):

    result = asyncio.run(
        jobs.get_job_details(
            uri,
            pat,
            project_id,
            job_ids=job_ids,
            use_df=use_df,
            download_profile=get_profile,
            header_only=header_only,
        )
    )
    pp(result)


if __name__ == "__main__":
    configure()
    set_level("DEBUG")
    app()
