# Dremio MCP Streamable HTTP Interface

This document describes the streamable HTTP interface for the Dremio MCP server, which provides enhanced capabilities for web-based clients and real-time communication.

## Overview

The streamable HTTP interface extends the standard MCP protocol with:

- **HTTP/REST API**: Standard HTTP endpoints for health checks and server information
- **WebSocket Support**: Real-time bidirectional communication
- **Server-Sent Events (SSE)**: Streaming data from server to client
- **CORS Support**: Cross-origin requests for web applications
- **Session Management**: Resumable connections and state management

## Starting the Server

To start the MCP server with HTTP transport:

```bash
# Basic HTTP server
dremio-mcp-server run --transport http

# Custom host and port
dremio-mcp-server run --transport http --host 0.0.0.0 --port 8080

# With specific Dremio configuration
dremio-mcp-server run \
    --transport http \
    --dremio-uri https://your-dremio-instance.com \
    --dremio-pat your-personal-access-token \
    --dremio-project-id your-project-id
```

## API Endpoints

### Health Check
- **GET** `/` or `/health`
- Returns server health status and basic information

```json
{
  "status": "healthy",
  "service": "dremio-mcp-server",
  "timestamp": "2025-07-05T10:30:00Z",
  "version": "0.1.0"
}
```

### Server Information
- **GET** `/info`
- Returns detailed server capabilities and available tools

```json
{
  "server": "dremio-mcp-server",
  "protocol": "streamable-http",
  "capabilities": ["tools", "resources", "prompts", "streaming", "websockets"],
  "tools": [
    {
      "name": "sql_query",
      "description": "Execute SQL queries against Dremio"
    }
  ],
  "endpoints": {
    "health": "/health",
    "info": "/info",
    "mcp": "/mcp",
    "websocket": "/ws",
    "sse": "/sse"
  }
}
```

### MCP Protocol
- **POST** `/mcp`
- Standard MCP JSON-RPC protocol endpoint
- Accepts MCP requests and returns MCP responses

Example request:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "1.0.0",
    "capabilities": {},
    "clientInfo": {
      "name": "my-client",
      "version": "1.0.0"
    }
  }
}
```

### WebSocket
- **WebSocket** `/ws`
- Real-time bidirectional communication
- Supports JSON message exchange

### Server-Sent Events
- **GET** `/sse`
- Streaming events from server to client
- Includes connection status and periodic heartbeats

## Client Examples

### Python Client

```python
import asyncio
import aiohttp
import websockets
import json

async def test_mcp_http():
    # Health check
    async with aiohttp.ClientSession() as session:
        async with session.get("http://localhost:8000/health") as response:
            health = await response.json()
            print(f"Health: {health}")

    # WebSocket communication
    async with websockets.connect("ws://localhost:8000/ws") as websocket:
        message = {"type": "test", "data": "hello"}
        await websocket.send(json.dumps(message))
        response = await websocket.recv()
        print(f"WebSocket response: {response}")

asyncio.run(test_mcp_http())
```

### JavaScript/Web Client

```javascript
// Health check
const healthResponse = await fetch('http://localhost:8000/health');
const health = await healthResponse.json();
console.log('Health:', health);

// WebSocket
const ws = new WebSocket('ws://localhost:8000/ws');
ws.onopen = () => {
    ws.send(JSON.stringify({type: 'test', data: 'hello'}));
};
ws.onmessage = (event) => {
    console.log('Received:', JSON.parse(event.data));
};

// Server-Sent Events
const eventSource = new EventSource('http://localhost:8000/sse');
eventSource.onmessage = (event) => {
    const data = JSON.parse(event.data);
    console.log('SSE Event:', data);
};
```

### curl Examples

```bash
# Health check
curl http://localhost:8000/health

# Server info
curl http://localhost:8000/info

# MCP initialize request
curl -X POST http://localhost:8000/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "1.0.0",
      "capabilities": {}
    }
  }'
```

## Testing

### Using the Test Scripts

1. **Python Test Client**: Run the comprehensive test suite
   ```bash
   python examples/streamable_client.py
   ```

2. **Web Test Interface**: Open the HTML test page
   ```bash
   # Start server
   dremio-mcp-server run --transport http
   
   # Open in browser
   open examples/web_test_interface.html
   ```

### Manual Testing

1. Start the server:
   ```bash
   dremio-mcp-server run --transport http --host 127.0.0.1 --port 8000
   ```

2. Test basic connectivity:
   ```bash
   curl http://127.0.0.1:8000/health
   ```

3. Test WebSocket with a tool like `wscat`:
   ```bash
   npm install -g wscat
   wscat -c ws://127.0.0.1:8000/ws
   ```

## Configuration

The streamable HTTP interface supports all standard MCP server configuration options:

- **Environment Variables**: Set via `DREMIO_URI`, `DREMIO_PAT`, etc.
- **Config Files**: Use `--config-file config.yaml`
- **Command Line**: Override with CLI arguments

Example config.yaml:
```yaml
dremio:
  uri: "https://your-dremio-instance.com"
  pat: "your-personal-access-token"
  project_id: "your-project-id"

tools:
  server_mode: ["FOR_SQL", "FOR_CATALOG"]

http:
  host: "0.0.0.0"
  port: 8000
  cors_origins: ["*"]
```

## Security Considerations

- **CORS**: Configure `allow_origins` appropriately for production
- **Authentication**: Consider adding authentication middleware
- **Rate Limiting**: Implement rate limiting for production deployments
- **TLS**: Use HTTPS in production environments

## Troubleshooting

### Common Issues

1. **Port Already in Use**
   ```bash
   # Find process using port 8000
   lsof -i :8000
   # Kill the process or use a different port
   dremio-mcp-server run --transport http --port 8001
   ```

2. **Connection Refused**
   - Check if server is running
   - Verify host/port configuration
   - Check firewall settings

3. **WebSocket Connection Failed**
   - Ensure server supports WebSocket upgrades
   - Check for proxy/load balancer issues
   - Verify CORS configuration

### Debug Mode

Enable debug logging:
```bash
dremio-mcp-server run --transport http --log-to-file
```

### Logs

Check server logs for detailed error information:
```bash
tail -f ~/.local/share/dremioai/logs/dremioai.log
```

## Architecture

The streamable HTTP interface is built using:

- **Starlette**: ASGI web framework for Python
- **MCP StreamableHTTPSessionManager**: Session management from MCP SDK
- **Uvicorn**: ASGI server for production deployment

## Integration with Claude Desktop

The streamable HTTP interface is compatible with Claude Desktop configuration:

```json
{
  "mcpServers": {
    "dremio": {
      "command": "dremio-mcp-server",
      "args": ["run", "--transport", "http", "--port", "8000"],
      "env": {
        "DREMIO_URI": "https://your-dremio-instance.com",
        "DREMIO_PAT": "your-personal-access-token"
      }
    }
  }
}
```
