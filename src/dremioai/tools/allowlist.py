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
"""Restrict tools to an allowlist of catalog paths (``dremio.allowed_schemas``).

Dremio editions without fine-grained privileges let every user read every
source. ``allowed_schemas`` lets an MCP deployment limit what the LLM can reach,
e.g. ``["Sales", "Marketing.Reports"]``: a table is allowed when its path starts
with one of these prefixes (case-insensitive, per path component). When the
setting is unset, nothing is restricted.

SQL is checked by extracting every table reference with sqlglot. Anything that
cannot be resolved to a plain catalog path (a table function, an unparsable
query) is refused: the check fails closed.
"""

from csv import reader
from io import StringIO
from typing import Iterable, List, Optional, Sequence, Union

from sqlglot import expressions, parse_one

from dremioai.config import settings

Path = List[str]


class NotAllowedError(ValueError):
    pass


def _split(name: str) -> Path:
    """Split a dotted name, honouring double quotes: '"a.b".c' -> ['a.b', 'c']."""
    return [p for p in next(reader(StringIO(name), delimiter=".")) if p != ""]


def to_path(name: Union[str, Sequence[str]]) -> Path:
    if isinstance(name, str):
        return _split(name)
    return [str(p) for p in name]


def allowed_prefixes() -> Optional[List[Path]]:
    dremio = settings.instance().dremio if settings.instance() else None
    raw = dremio.get("allowed_schemas") if dremio is not None else None
    if raw is None:
        return None
    return [[p.lower() for p in _split(entry)] for entry in raw]


def describe() -> str:
    """The configured allowlist for messages to the LLM, e.g. 'Sales, Marketing.Reports'."""
    dremio = settings.instance().dremio if settings.instance() else None
    raw = dremio.get("allowed_schemas") if dremio is not None else None
    return "any" if raw is None else ", ".join(raw)


def _under(path: Path, prefix: Path) -> bool:
    return len(path) >= len(prefix) and [p.lower() for p in path[: len(prefix)]] == prefix


def _above(path: Path, prefix: Path) -> bool:
    return len(path) < len(prefix) and [p.lower() for p in path] == prefix[: len(path)]


def ensure_path_allowed(
    name: Union[str, Sequence[str]], allow_parents: bool = False
) -> None:
    """Raise NotAllowedError unless ``name`` is under an allowed prefix.

    ``allow_parents`` also admits the parents of an allowed prefix (e.g. the
    space ``Marketing`` for ``Marketing.Reports``), for tools that describe
    containers rather than return data.
    """
    prefixes = allowed_prefixes()
    if prefixes is None:
        return
    path = to_path(name)
    if path and any(
        _under(path, p) or (allow_parents and _above(path, p)) for p in prefixes
    ):
        return
    raise NotAllowedError(f"Access to {'.'.join(path) or name!r} is not allowed")


def table_paths(query: str) -> List[Path]:
    """Catalog paths of all tables a query reads; raises NotAllowedError when a
    source cannot be resolved to a plain path."""
    try:
        tree = parse_one(query)
    except Exception as e:
        raise NotAllowedError(f"Query could not be parsed: {e}") from e
    if tree is None:
        raise NotAllowedError("Empty query")

    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(expressions.CTE)}
    paths = []
    for table in tree.find_all(expressions.Table):
        if not isinstance(table.this, expressions.Identifier):
            raise NotAllowedError("Table functions are not allowed")
        path = [part.name for part in table.parts]
        if len(path) == 1 and path[0].lower() in cte_names:
            continue
        paths.append(path)
    return paths


def ensure_query_allowed(query: str) -> None:
    if allowed_prefixes() is None:
        return
    for path in table_paths(query):
        ensure_path_allowed(path)


def ensure_paths_allowed(
    names: Iterable[Union[str, Sequence[str]]], allow_parents: bool = False
) -> None:
    for name in names:
        ensure_path_allowed(name, allow_parents=allow_parents)
