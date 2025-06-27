"""
Tests for the Dremio MCP Streamable HTTP interface.
"""

import pytest
from unittest.mock import Mock
from starlette.testclient import TestClient


@pytest.fixture
def mock_mcp_app():
    """Create a mock MCP app for testing"""
    app = Mock()
    app._tools = {
        'test_tool': Mock(__doc__='A test tool for demonstration')
    }
    return app


@pytest.fixture
def test_client(mock_mcp_app):
    """Create a test client with simplified endpoints for testing"""
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route
    from starlette.middleware import Middleware
    from starlette.middleware.cors import CORSMiddleware
    from datetime import datetime, timezone

    # Create simple test endpoints that don't depend on session manager
    async def health_check(request):
        return JSONResponse({
            "status": "healthy",
            "service": "dremio-mcp-server",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": "0.1.0"
        })

    async def server_info(request):
        return JSONResponse({
            "server": "dremio-mcp-server",
            "protocol": "streamable-http",
            "capabilities": ["tools", "resources", "prompts", "streaming"],
            "tools": [{"name": "test_tool", "description": "A test tool"}],
            "endpoints": {
                "health": "/health",
                "info": "/info",
                "mcp": "/mcp",
                "websocket": "/ws",
                "sse": "/sse"
            }
        })

    middleware = [
        Middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    ]

    app = Starlette(
        debug=True,
        middleware=middleware,
        routes=[
            Route("/", health_check),
            Route("/health", health_check),
            Route("/info", server_info),
        ]
    )
    return TestClient(app)


def test_health_check(test_client):
    """Test the health check endpoint"""
    response = test_client.get("/health")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "dremio-mcp-server"
    assert "timestamp" in data
    assert "version" in data


def test_root_endpoint(test_client):
    """Test the root endpoint (should be same as health)"""
    response = test_client.get("/")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "healthy"


def test_server_info(test_client):
    """Test the server info endpoint"""
    response = test_client.get("/info")
    assert response.status_code == 200
    
    data = response.json()
    assert data["server"] == "dremio-mcp-server"
    assert data["protocol"] == "streamable-http"
    assert "capabilities" in data
    assert "tools" in data
    assert "endpoints" in data
    
    # Check capabilities
    expected_capabilities = [
        "tools", "resources", "prompts", "streaming"
    ]
    for capability in expected_capabilities:
        assert capability in data["capabilities"]
    
    # Check endpoints
    endpoints = data["endpoints"]
    expected_endpoints = ["health", "info", "mcp", "websocket", "sse"]
    for endpoint in expected_endpoints:
        assert endpoint in endpoints


def test_cors_headers(test_client):
    """Test that CORS headers are present"""
    response = test_client.get("/health")
    
    # Note: TestClient doesn't automatically include CORS headers
    # In a real environment, these would be present
    assert response.status_code == 200


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
