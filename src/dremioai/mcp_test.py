#
# Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
#

from dremioai import log
from dremioai.servers import tools, mcp as mcp_server
import os

#log.configure(enable_json_logging=True, to_file=True)
if mode := os.environ.get("MODE"):
    mode = [tools.ToolType[m.upper()] for m in ','.split(mode)]
app = mcp_server.init(mode=mode)

def dev():
    import mcp.cli.cli as cli
    cli.dev(__file__)

def run():
    import mcp.cli.cli as cli
    cli.run(__file__)
