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

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.prompts import Prompt
from mcp.server.fastmcp.resources import FunctionResource
from pydantic.networks import AnyUrl
from dremioai.tools import tools
import os
from typing import List, Union, Annotated, Optional, Tuple
from functools import reduce
from operator import ior
from pathlib import Path
from dremioai import log
from typer import Typer, Option, Argument, BadParameter
from rich import console, table, print as pp
from click import Choice
from dremioai.config import settings
import asyncio
from yaml import dump


def init(
    uri: str = None,
    pat: str = None,
    project_id: str = None,
    mode: Union[tools.ToolType, List[tools.ToolType]] = None,
) -> FastMCP:
    mcp = FastMCP("Dremio", level="DEBUG")
    mode = reduce(ior, mode) if mode is not None else None
    for tool in tools.get_tools(For=mode):
        tool_instance = tool()
        mcp.add_tool(
            tool_instance.invoke,
            name=tool.__name__,
            description=tool_instance.invoke.__doc__,
        )

    for resource in tools.get_resources(For=mode):
        resource_instance = resource()
        mcp.add_resource(
            FunctionResource(
                uri=AnyUrl(resource_instance.resource_path),
                name=resource.__name__,
                description=resource.__doc__,
                mime_type="application/json",
                fn=resource_instance.invoke,
            )
        )
    # if mode is None or (mode & tools.ToolType.FOR_SELF) != 0:
    mcp.add_prompt(
        Prompt.from_function(tools.system_prompt, "System Prompt", "System Prompt")
    )
    return mcp


app = None
if __name__ != "__main__":
    if mode := os.environ.get("MODE"):
        mode = [tools.ToolType[m.upper()] for m in ",".split(mode)]
    app = init(mode=mode)


def _mode() -> List[str]:
    return [tt.name for tt in tools.ToolType]


ty = Typer(context_settings=dict(help_option_names=["-h", "--help"]))


@ty.command(name="run", help="Run the DremioAI MCP server")
def main(
    dremio_uri: Annotated[Optional[str], Option(help="Dremio URI")] = None,
    dremio_pat: Annotated[Optional[str], Option(help="Dremio PAT")] = None,
    dremio_project_id: Annotated[
        Optional[str], Option(help="Dremio Project Id")
    ] = None,
    config_file: Annotated[
        Optional[Path],
        Option("-c", "--cfg", help="The config yaml for various options"),
    ] = None,
    mode: Annotated[
        Optional[List[str]],
        Option("-m", "--mode", help="MCP server mode", click_type=Choice(_mode())),
    ] = None,
    list_tools: Annotated[
        bool, Option(help="List available tools for this mode and exit")
    ] = False,
    log_to_file: Annotated[Optional[bool], Option(help="Log to file")] = False,
):
    if not list_tools:
        log.configure(enable_json_logging=True, to_file=True)
    else:
        log.configure(enable_json_logging=True, to_file=log_to_file)
        log.set_level("DEBUG")

    if mode is not None:
        mode = [tools.ToolType[m.upper()] for m in mode]

    cfg = (
        settings.configure(config_file)
        .get()
        .with_overrides(
            {
                "dremio.uri": dremio_uri,
                "dremio.pat": dremio_pat,
                "dremio.project_id": dremio_project_id,
                "tools.server_mode": mode,
            }
        )
    )
    if list_tools:
        log.logger().info(f"Starting Dremio tools with {cfg}")
        mode = reduce(ior, mode) if mode is not None else None
        log.logger().info(f"Listing available tools for mode={mode}")
        for tool in tools.get_tools(For=mode):
            print(tool.__name__)
        return

    app = init(
        uri=cfg.dremio.uri,
        pat=cfg.dremio.pat,
        project_id=cfg.dremio.project_id,
        mode=cfg.tools.server_mode,
    )
    app.run()


tc = Typer(
    context_settings=dict(help_option_names=["-h", "--help"]),
    name="config",
    help="Configuration management",
)


@tc.command("list", help="Show default configuration, if it exists")
def show_default_config(
    show_filename: Annotated[
        bool, Option(help="Show the filename for default config file")
    ] = False,
):
    dc = settings.default_config()
    pp(f"Default config file: {dc!s} (exists = {dc.exists()!s})")
    if not show_filename:
        settings.configure(dc)
        pp(
            dump(
                settings.instance().model_dump(
                    exclude_none=True, mode="json", exclude_unset=True
                )
            )
        )


# --------------------------------------------------------------------------------
# testing support

tl = Typer(
    context_settings=dict(help_option_names=["-h", "--help"]),
    name="tools",
    help="Support for testing tools directly",
)

# tl.add_typer(call)


@tl.command(
    name="list",
    help="List the available tools",
    context_settings=dict(help_option_names=["-h", "--help"]),
)
def tools_list(
    mode: Annotated[
        Optional[List[str]],
        Option("-m", "--mode", help="MCP server mode", click_type=Choice(_mode())),
    ] = [tools.ToolType.FOR_SELF.name],
):
    mode = reduce(ior, [tools.ToolType[m.upper()] for m in mode])
    tab = table.Table(
        table.Column("Tool", justify="left", style="cyan"),
        "Description",
        "For",
        title="Tools list",
        show_lines=True,
    )

    for tool in tools.get_tools(For=mode):
        For = tools.get_for(tool)
        try:
            tab.add_row(tool.__name__, tool.invoke.__doc__.strip(), For.name)
        except Exception as e:
            tab.add_row(tool.__name__, "No Description", For.name)
    console.Console().print(tab)


@tl.command(
    name="invoke",
    help="Execute an available tools",
    context_settings=dict(help_option_names=["-h", "--help"]),
)
def tools_exec(
    tool: Annotated[str, Option("-t", "--tool", help="The tool to execute")],
    config_file: Annotated[
        Optional[Path],
        Option("-c", "--cfg", help="The config yaml for various options"),
    ] = None,
    args: Annotated[
        Optional[List[str]],
        Argument(help="The arguments to pass to the tool (arg=value ...)"),
    ] = None,
):
    def _to_kw(arg: str) -> Tuple[str, str]:
        if "=" not in arg:
            raise BadParameter(f"Argument {arg} is not in the form arg=value")
        return tuple(arg.split("=", 1))

    settings.configure(config_file)

    if args is None:
        args = {}
    elif type(args) == str:
        args = [args]
    args = dict(map(_to_kw, args))
    for_all = reduce(ior, tools.ToolType.__members__.values())
    all_tools = {t.__name__: t for t in tools.get_tools(for_all)}

    if selected := all_tools.get(tool):
        tool_instance = selected()  # get arguments from settings
        result = asyncio.run(tool_instance.invoke(**args))
        pp(result)
    else:
        raise BadParameter(f"Tool {tool} not found")


ty.add_typer(tl)
ty.add_typer(tc)


def cli():
    ty()


if __name__ == "__main__":
    cli()
