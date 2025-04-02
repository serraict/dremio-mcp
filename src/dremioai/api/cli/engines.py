#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#

from typing import Annotated, Optional, List
from typer import Option, Argument, Typer
import asyncio
from rich import print as pp
from dremioai.api.cli import engines

app = Typer(
    no_args_is_help=True,
    name="engines",
    help="Run commands related to engines",
    context_settings=dict(help_option_names=["-h", "--help"]),
)


@app.command("list")
def elist(
    uri: Annotated[str, Option(envvar="DREMIO_URI", default=...)],
    project_id: Annotated[str, Option(envvar="DREMIO_PROJECT_ID", default=...)],
    pat: Annotated[str, Option(envvar="DREMIO_PAT", default=...)],
    use_df: Annotated[
        Optional[bool], Option(help="Convert results to pandas dataframe")
    ] = False,
):
    result = asyncio.run(engines.get_engines(uri, pat, project_id, use_df=use_df))
    pp(result)


@app.command("get")
def eget(
    uri: Annotated[str, Option(envvar="DREMIO_URI", default=...)],
    project_id: Annotated[str, Option(envvar="DREMIO_PROJECT_ID", default=...)],
    pat: Annotated[str, Option(envvar="DREMIO_PAT", default=...)],
    engine_ids: Annotated[
        List[str], Argument(help="Engine IDs to retrieve details for")
    ],
    use_df: Annotated[
        Optional[bool], Option(help="Convert results to pandas dataframe")
    ] = False,
):
    result = asyncio.run(
        engines.get_engines(uri, pat, project_id, engine_ids=engine_ids, use_df=use_df)
    )
    pp(result)
