#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#

from typing import Annotated, Optional, List
from typer import Option, Argument, Typer
import asyncio
from rich import print as pp
from pathlib import Path
from dremioai.api.dremio import search
from dremioai.config import settings
from dremioai.log import logger

app = Typer(
    no_args_is_help=True,
    name="search",
    help="Run semantic search apis",
    context_settings=dict(help_option_names=["-h", "--help"]),
)


@app.command("run")
def do_search(
    query: Annotated[str, Argument(...)],
    uri: Annotated[str, Option(envvar="DREMIO_URI", show_envvar=True)] = None,
    project_id: Annotated[
        str, Option(envvar="DREMIO_PROJECT_ID", show_envvar=True)
    ] = None,
    pat: Annotated[str, Option(envvar="DREMIO_PAT", show_envvar=True)] = None,
    config_file: Annotated[Path, Option(help="Path to config file")] = None,
    use_df: Annotated[
        Optional[bool], Option(help="Convert results to pandas dataframe")
    ] = False,
):

    settings.configure(config_file)
    settings.instance().get().with_overrides(
        {"dremio.uri": uri, "dremio.project_id": project_id, "dremio.pat": pat}
    )
    logger().info(f"settings = {settings.instance()}")

    result = asyncio.run(search.get_search_results(search=search.Search(query=query)))
    pp(result)
