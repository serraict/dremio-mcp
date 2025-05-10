"""
Global pytest fixtures for dremio-mcp tests.
"""
import os
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from dremioai.config import settings
from dremioai.config.tools import ToolType


@pytest.fixture
def temp_config_dir():
    """Create a temporary directory for config files"""
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_config_dir(temp_config_dir):
    """Mock the home directory to use our temporary directory"""
    with patch.object(Path, "home", return_value=temp_config_dir):
        # Also patch XDG_CONFIG_HOME environment variable
        old_env = os.environ.get("XDG_CONFIG_HOME")
        os.environ["XDG_CONFIG_HOME"] = str(temp_config_dir)
        yield temp_config_dir
        # Restore original environment
        if old_env:
            os.environ["XDG_CONFIG_HOME"] = old_env
        else:
            os.environ.pop("XDG_CONFIG_HOME", None)


@pytest.fixture
def mock_settings_instance():
    """Create a mock settings instance with default values"""
    old_settings = settings.instance()
    try:
        settings._settings.set(
            settings.Settings.model_validate(
                {
                    "dremio": {
                        "uri": "https://test-dremio-uri.com",
                        "pat": "test-pat",
                        "project_id": "test-project-id",
                    },
                    "tools": {"server_mode": ToolType.FOR_SELF.name},
                }
            )
        )
        yield settings.instance()
    finally:
        settings._settings.set(old_settings)
