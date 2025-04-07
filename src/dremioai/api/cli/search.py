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
    category: Annotated[List[search.Category], Option(help="Search category")] = None,
    use_df: Annotated[
        Optional[bool], Option(help="Convert results to pandas dataframe")
    ] = False,
):
    if category:
        category = ",".join([f'"{c.name}"' for c in category])
    s = search.Search(
        query=query, filter=f"category in [{category}]" if category else ""
    )
    result = asyncio.run(search.get_search_results(search=s))
    pp(result)
