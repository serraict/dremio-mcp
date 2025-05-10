import pytest
from unittest.mock import patch, MagicMock

from dremioai.config.tools import ToolType
from dremioai.servers.mcp import create_default_mcpserver_config
from dremioai.tools.tools import get_tools
from dremioai.config import settings

from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters
from contextlib import asynccontextmanager, contextmanager
from rich import print as pp
from tempfile import TemporaryDirectory
from pathlib import Path


@contextmanager
def mock_settings(mode: ToolType):
    """Create mock settings for testing MCP server"""
    # Create a mock settings instance
    try:
        old = settings.instance()
        with TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            settings._settings.set(
                settings.Settings.model_validate(
                    {
                        "dremio": {
                            "uri": "https://test-dremio-uri.com",
                            "pat": "test-pat",
                        },
                        "tools": {"server_mode": mode},
                    }
                )
            )
            cfg = temp_dir / "config.yaml"
            settings.write_settings(cfg=cfg, inst=settings.instance())
            yield settings.instance(), cfg
    finally:
        settings._settings.set(old)


@asynccontextmanager
async def mcp_server_session(cfg: Path):
    """Create an MCP server instance with mock settings"""
    params = create_default_mcpserver_config()
    params["args"].extend(["--cfg", str(cfg)])
    params = StdioServerParameters(command=params["command"], args=params["args"])
    async with (
        stdio_client(params) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        yield session


@pytest.mark.parametrize(
    "mode",
    [
        ToolType.FOR_SELF,
        ToolType.FOR_DATA_PATTERNS,
        ToolType.FOR_SELF | ToolType.FOR_DATA_PATTERNS,
    ],
)
@pytest.mark.asyncio
async def test_mcp_server_initialization(mode: ToolType):
    with mock_settings(mode) as (_, cfg):
        async with mcp_server_session(cfg) as session:
            tools = await session.list_tools()
            assert len(tools.tools) > 0
            names = {tool.name for tool in tools.tools}
            exp = {t.__name__ for t in get_tools(For=mode)}
            assert names == exp
