#!/usr/bin/env python3
"""
Example client for the Dremio MCP Streamable HTTP Server.

This demonstrates how to interact with the server using:
- HTTP requests for basic operations
- WebSocket for real-time communication
- Server-Sent Events for streaming data

Run the server first:
    dremio-mcp-server run --transport http --host 127.0.0.1 --port 8000

Then run this client:
    python examples/streamable_client.py
"""

import asyncio
import json
import aiohttp
import websockets
from typing import Dict, Any
import time


class DremioMCPClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8000"):
        self.base_url = base_url
        self.ws_url = base_url.replace("http://", "ws://").replace("https://", "wss://")

    async def health_check(self) -> Dict[str, Any]:
        """Check server health status"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/health") as response:
                return await response.json()

    async def get_server_info(self) -> Dict[str, Any]:
        """Get server information and capabilities"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/info") as response:
                return await response.json()

    async def test_websocket(self):
        """Test WebSocket communication"""
        print("Testing WebSocket connection...")
        uri = f"{self.ws_url}/ws"
        
        try:
            async with websockets.connect(uri) as websocket:
                # Send a test message
                test_message = {
                    "type": "test",
                    "content": "Hello from WebSocket client!",
                    "timestamp": time.time()
                }
                
                await websocket.send(json.dumps(test_message))
                print(f"Sent: {test_message}")
                
                # Receive response
                response = await websocket.recv()
                response_data = json.loads(response)
                print(f"Received: {response_data}")
                
        except Exception as e:
            print(f"WebSocket error: {e}")

    async def test_sse(self, duration: int = 10):
        """Test Server-Sent Events"""
        print(f"Testing SSE for {duration} seconds...")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/sse") as response:
                async for line in response.content:
                    if line.startswith(b'data: '):
                        data = line[6:].decode().strip()
                        try:
                            event_data = json.loads(data)
                            print(f"SSE Event: {event_data}")
                        except json.JSONDecodeError:
                            print(f"Raw SSE: {data}")
                        
                        # Break after receiving a few events
                        if duration <= 0:
                            break
                        duration -= 1

    async def test_mcp_protocol(self):
        """Test MCP protocol communication"""
        print("Testing MCP protocol...")
        
        # Example MCP initialize request
        mcp_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "1.0.0",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0.0"
                }
            }
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"{self.base_url}/mcp",
                    json=mcp_request,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    result = await response.json()
                    print(f"MCP Response: {result}")
            except Exception as e:
                print(f"MCP request error: {e}")

    async def run_all_tests(self):
        """Run all client tests"""
        print("=" * 50)
        print("Dremio MCP Streamable HTTP Client Tests")
        print("=" * 50)
        
        try:
            # Test health check
            print("\n1. Health Check:")
            health = await self.health_check()
            print(json.dumps(health, indent=2))
            
            # Test server info
            print("\n2. Server Info:")
            info = await self.get_server_info()
            print(json.dumps(info, indent=2))
            
            # Test WebSocket
            print("\n3. WebSocket Test:")
            await self.test_websocket()
            
            # Test SSE (briefly)
            print("\n4. Server-Sent Events Test:")
            await self.test_sse(duration=3)
            
            # Test MCP protocol
            print("\n5. MCP Protocol Test:")
            await self.test_mcp_protocol()
            
            print("\n" + "=" * 50)
            print("All tests completed!")
            
        except Exception as e:
            print(f"Test error: {e}")


async def main():
    client = DremioMCPClient()
    await client.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())
