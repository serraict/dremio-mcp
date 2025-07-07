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

"""
Streamable HTTP server implementation for MCP.
Provides HTTP endpoints with WebSocket and SSE support.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)


class StreamableHTTPServer:
    """HTTP server wrapper for FastMCP with streamable support."""

    def __init__(self, mcp_app: FastMCP):
        self.mcp_app = mcp_app

        # Debug: log FastMCP structure
        logger.info(f"FastMCP attributes: {dir(self.mcp_app)}")
        if hasattr(self.mcp_app, "_server"):
            logger.info(f"FastMCP._server attributes: {dir(self.mcp_app._server)}")
            if hasattr(self.mcp_app._server, "_tools"):
                logger.info(
                    f"Found {len(self.mcp_app._server._tools)} tools in _server._tools"
                )
        if hasattr(self.mcp_app, "_tools"):
            logger.info(f"Found {len(self.mcp_app._tools)} tools in _tools")

        self.app = self._create_app()

    def _create_app(self) -> Starlette:
        """Create the Starlette application with all endpoints."""
        middleware = [
            Middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"],
            )
        ]

        return Starlette(
            debug=True,
            middleware=middleware,
            routes=[
                Route("/", self.health_check),
                Route("/health", self.health_check),
                Route("/info", self.server_info),
                Route("/mcp", self.mcp_endpoint, methods=["GET", "POST"]),
                Route("/mcp/", self.mcp_endpoint, methods=["GET", "POST"]),
                WebSocketRoute("/ws", self.websocket_endpoint),
                Route("/sse", self.sse_endpoint),
            ],
        )

    async def health_check(self, request) -> JSONResponse:
        """Health check endpoint."""
        return JSONResponse(
            {
                "status": "healthy",
                "service": "dremio-mcp-server",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "version": "0.1.0",
            }
        )

    async def server_info(self, request) -> JSONResponse:
        """Server information endpoint."""
        tools = []

        try:
            # Use FastMCP's built-in list_tools method
            tool_list = await self.mcp_app.list_tools()
            for tool in tool_list:
                tools.append(
                    {
                        "name": tool.name,
                        "description": tool.description or f"Tool: {tool.name}",
                    }
                )
        except Exception as e:
            logger.error(f"Error getting tools for server info: {e}")

        return JSONResponse(
            {
                "server": "dremio-mcp-server",
                "protocol": "streamable-http",
                "capabilities": ["tools", "resources", "prompts", "streaming"],
                "tools": tools,
                "endpoints": {
                    "health": "/health",
                    "info": "/info",
                    "mcp": "/mcp",
                    "websocket": "/ws",
                    "sse": "/sse",
                },
            }
        )

    async def mcp_endpoint(self, request) -> JSONResponse:
        """Main MCP protocol endpoint."""
        try:
            if request.method == "GET":
                # Handle GET requests for MCP discovery/capabilities
                return JSONResponse(
                    {
                        "server": "dremio-mcp-server",
                        "protocol": "mcp",
                        "version": "1.0",
                        "capabilities": {
                            "tools": True,
                            "resources": True,
                            "prompts": True,
                        },
                        "endpoints": {"mcp": "/mcp", "websocket": "/ws", "sse": "/sse"},
                    }
                )
            elif request.method == "POST":
                # Handle POST requests for MCP protocol messages
                data = await request.json()

                # Handle MCP JSON-RPC protocol
                if "method" in data:
                    method = data["method"]
                    request_id = data.get("id")

                    if method == "initialize":
                        # Handle initialize request
                        return JSONResponse(
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "result": {
                                    "protocolVersion": "2024-11-05",
                                    "capabilities": {
                                        "tools": {"listChanged": True},
                                        "resources": {
                                            "subscribe": True,
                                            "listChanged": True,
                                        },
                                        "prompts": {"listChanged": True},
                                    },
                                    "serverInfo": {
                                        "name": "dremio-mcp-server",
                                        "version": "0.1.0",
                                    },
                                },
                            }
                        )
                    elif method == "tools/list":
                        # Handle tools list request
                        tools = []

                        try:
                            # Use FastMCP's built-in list_tools method
                            tool_list = await self.mcp_app.list_tools()

                            # tool_list is a list of Tool objects
                            for tool in tool_list:
                                tools.append(
                                    {
                                        "name": tool.name,
                                        "description": tool.description
                                        or f"Tool: {tool.name}",
                                        "inputSchema": tool.inputSchema
                                        or {
                                            "type": "object",
                                            "properties": {},
                                            "required": [],
                                        },
                                    }
                                )
                        except Exception as e:
                            logger.error(f"Error getting tools from FastMCP: {e}")

                        logger.info(
                            f"Returning {len(tools)} tools: {[t['name'] for t in tools]}"
                        )

                        return JSONResponse(
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "result": {"tools": tools},
                            }
                        )
                    elif method == "resources/list":
                        # Handle resources list request
                        return JSONResponse(
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "result": {"resources": []},
                            }
                        )
                    elif method == "tools/call":
                        # Handle tool execution request
                        params = data.get("params", {})
                        tool_name = params.get("name")
                        arguments = params.get("arguments", {})
                        
                        if not tool_name:
                            return JSONResponse(
                                {
                                    "jsonrpc": "2.0",
                                    "id": request_id,
                                    "error": {
                                        "code": -32602,
                                        "message": "Invalid params: missing tool name",
                                    },
                                }
                            )
                        
                        try:
                            # Use FastMCP's built-in call_tool method
                            result = await self.mcp_app.call_tool(tool_name, arguments)
                            
                            logger.info(f"Tool {tool_name} executed successfully")
                            
                            return JSONResponse(
                                {
                                    "jsonrpc": "2.0",
                                    "id": request_id,
                                    "result": {
                                        "content": [
                                            {
                                                "type": "text",
                                                "text": str(result)
                                            }
                                        ]
                                    },
                                }
                            )
                        except Exception as e:
                            logger.error(f"Error executing tool {tool_name}: {e}")
                            return JSONResponse(
                                {
                                    "jsonrpc": "2.0",
                                    "id": request_id,
                                    "error": {
                                        "code": -32603,
                                        "message": f"Tool execution failed: {str(e)}",
                                    },
                                }
                            )
                    elif method == "prompts/list":
                        # Handle prompts list request
                        return JSONResponse(
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "result": {"prompts": []},
                            }
                        )
                    else:
                        # Unknown method
                        return JSONResponse(
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "error": {
                                    "code": -32601,
                                    "message": f"Method not found: {method}",
                                },
                            }
                        )
                else:
                    # Not a valid JSON-RPC request
                    return JSONResponse(
                        {
                            "jsonrpc": "2.0",
                            "error": {"code": -32600, "message": "Invalid Request"},
                        }
                    )
            else:
                return JSONResponse(
                    {"error": f"Method {request.method} not allowed"}, status_code=405
                )
        except Exception as e:
            logger.error(f"MCP endpoint error: {e}")
            return JSONResponse({"error": str(e)}, status_code=400)

    async def websocket_endpoint(self, websocket: WebSocket):
        """WebSocket endpoint for real-time communication."""
        await websocket.accept()
        try:
            while True:
                data = await websocket.receive_text()
                message = json.loads(data)

                # Echo back the message for now
                response = {
                    "type": "response",
                    "data": message,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                await websocket.send_text(json.dumps(response))
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            await websocket.close()

    async def sse_endpoint(self, request) -> StreamingResponse:
        """Server-Sent Events endpoint for streaming data."""

        async def event_stream():
            count = 0
            while True:
                # Send a test event every 5 seconds
                data = {
                    "message": f"Event {count}",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                yield f"data: {json.dumps(data)}\n\n"
                count += 1
                await asyncio.sleep(5)

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )

    def run(self, host: str = "127.0.0.1", port: int = 8000):
        """Run the HTTP server."""
        logger.info(f"Starting streamable HTTP server on {host}:{port}")
        uvicorn.run(self.app, host=host, port=port, log_level="info")


def create_streamable_http_server(mcp_app: FastMCP) -> StreamableHTTPServer:
    """Create a streamable HTTP server instance."""
    return StreamableHTTPServer(mcp_app)
