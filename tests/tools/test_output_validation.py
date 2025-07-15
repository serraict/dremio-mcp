"""
Test script to demonstrate the actual MCP output validation bug.

This script shows how FastMCP validates tool outputs by using the actual
function type annotations, which is where the validation occurs in the MCP
framework.
"""

import pytest
from typing import get_type_hints
from pydantic import TypeAdapter
from unittest.mock import patch

from dremioai.tools.tools import GetUsefulSystemTableNames, GetSchemaOfTable


async def mock_mcp_validate_tool_output(tool):
    """
    Test how FastMCP validates tool outputs using function type annotations.

    FastMCP uses the function's return type annotation to create a Pydantic
    validator that checks the actual return value from the tool's invoke()
    method.
    """
    type_hints = get_type_hints(tool.invoke)
    expected_type = type_hints["return"]
    actual_output = await tool.invoke()

    type_adapter = TypeAdapter(expected_type)
    type_adapter.validate_python(actual_output)


@pytest.mark.asyncio
async def test_GetUsefulSystemTableNames_validation():
    tool1 = GetUsefulSystemTableNames()
    await mock_mcp_validate_tool_output(tool1)


@pytest.mark.asyncio
async def test_get_schema_of_table_validation():
    """Test GetSchemaOfTable with mocked output to demonstrate bug"""
    tool = GetSchemaOfTable()

    # Mock the get_schema function to return a dict (simulating the bug)
    # According to the bug report, the tool returns a dict when it should
    # return a list
    mock_schema_result = {
        "fields": [
            {"name": "job_id", "type": "VARCHAR"},
            {"name": "user_name", "type": "VARCHAR"},
        ],
        "text": "System jobs table",
    }

    with patch("dremioai.tools.tools.get_schema", return_value=mock_schema_result):
        type_hints = get_type_hints(tool.invoke)
        expected_type = type_hints["return"]
        actual_output = await tool.invoke("sys.jobs")

        type_adapter = TypeAdapter(expected_type)
        # This should fail because actual_output is a dict but
        # expected_type is List[Dict[str, str]]
        type_adapter.validate_python(actual_output)
