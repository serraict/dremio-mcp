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
from contextlib import contextmanager
from typing import List, Optional

import pytest

from dremioai.config import settings
from dremioai.tools import allowlist, tools


@contextmanager
def mock_settings(allowed_schemas: Optional[List[str]]):
    old_settings = settings.instance()
    try:
        settings.set_base_settings(
            settings.Settings.model_validate(
                {
                    "dremio": {
                        "uri": "https://test-dremio-uri.com",
                        "allowed_schemas": allowed_schemas,
                    }
                }
            )
        )
        yield
    finally:
        settings.set_base_settings(old_settings)


ALLOWED = ["Sales", "Marketing.Reports", "INFORMATION_SCHEMA"]

queries = [
    ("SELECT * FROM Sales.orders", True),
    ('SELECT * FROM "Sales"."orders" o JOIN sales.customers c ON o.id = c.id', True),
    ("SELECT * FROM Marketing.Reports.weekly", True),
    ('SELECT * FROM INFORMATION_SCHEMA."TABLES"', True),
    ("WITH w AS (SELECT * FROM Sales.orders) SELECT * FROM w", True),
    ("SELECT * FROM Sales.orders WHERE id IN (SELECT id FROM Sales.returns)", True),
    ("SELECT 1", True),
    ("SELECT * FROM Marketing.Raw.events", False),
    ("SELECT * FROM Marketing.Reports_old.x", False),
    ("SELECT * FROM chat.messages", False),
    ("SELECT * FROM Sales.orders UNION ALL SELECT * FROM chat.messages", False),
    ("SELECT * FROM Sales.orders WHERE id IN (SELECT id FROM chat.messages)", False),
    ("WITH w AS (SELECT * FROM chat.messages) SELECT * FROM w", False),
    ("SELECT * FROM orders", False),
    ("SELECT * FROM sys.jobs", False),
    ("SELECT * FROM TABLE(chat.external_query('select * from messages'))", False),
]


@pytest.mark.parametrize("query,allowed", queries, ids=[q for q, _ in queries])
def test_query_allowlist(query: str, allowed: bool):
    with mock_settings(ALLOWED):
        try:
            allowlist.ensure_query_allowed(query)
            assert allowed, f"should not be allowed: {query}"
        except allowlist.NotAllowedError:
            assert not allowed, f"should be allowed: {query}"


def test_no_allowlist_allows_everything():
    with mock_settings(None):
        allowlist.ensure_query_allowed("SELECT * FROM chat.messages")
        allowlist.ensure_path_allowed("chat.messages")


@pytest.mark.parametrize(
    "name,allow_parents,allowed",
    [
        ("Sales.orders", False, True),
        (["Marketing", "Reports", "weekly"], False, True),
        ('"Marketing"."Reports"', False, True),
        ("Marketing", False, False),
        ("Marketing", True, True),
        ("Marketing.Raw", True, False),
        ("chat", True, False),
    ],
)
def test_path_allowlist(name, allow_parents: bool, allowed: bool):
    with mock_settings(ALLOWED):
        try:
            allowlist.ensure_path_allowed(name, allow_parents=allow_parents)
            assert allowed
        except allowlist.NotAllowedError:
            assert not allowed


@pytest.mark.asyncio
async def test_run_sql_query_refuses_disallowed_table():
    with mock_settings(ALLOWED):
        result = await tools.RunSqlQuery().invoke("SELECT * FROM chat.messages")
        assert result.isError
        assert "not allowed" in result.content[0].text
        assert "Sales, Marketing.Reports, INFORMATION_SCHEMA" in result.content[0].text


@pytest.mark.asyncio
async def test_get_schema_refuses_disallowed_table():
    with mock_settings(ALLOWED):
        result = await tools.GetSchemaOfTable().invoke("chat.messages")
        assert "error" in result
