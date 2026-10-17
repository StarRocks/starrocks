# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the ``starrocks_temp_view_schema`` Alembic option: the schema in which the
transient canonicalization view is created for view/MV definition comparison."""

from types import SimpleNamespace

from sqlalchemy import MetaData

from starrocks.alembic import compare
from starrocks.alembic.compare import (
    TEMP_VIEW_SCHEMA_OPT,
    _compare_view_definition_and_columns,
    _configured_temp_view_schema,
    _get_canonical_sql_via_temp_view,
)
from starrocks.alembic.ops import AlterViewOp
from starrocks.sql.schema import View


def _autogen_context(opts):
    """Minimal stand-in exposing ``migration_context.opts`` like Alembic's AutogenContext."""
    return SimpleNamespace(migration_context=SimpleNamespace(opts=opts))


class TestConfiguredTempViewSchema:
    def test_returns_configured_value(self):
        ctx = _autogen_context({TEMP_VIEW_SCHEMA_OPT: "migrations"})
        assert _configured_temp_view_schema(ctx) == "migrations"

    def test_none_when_unset(self):
        assert _configured_temp_view_schema(_autogen_context({})) is None

    def test_empty_string_treated_as_unset(self):
        assert _configured_temp_view_schema(_autogen_context({TEMP_VIEW_SCHEMA_OPT: ""})) is None

    def test_missing_migration_context(self):
        assert _configured_temp_view_schema(SimpleNamespace()) is None


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    """Records exec_driver_sql calls and returns a canned reflected definition."""

    def __init__(self, reflected_definition):
        self.calls = []
        self._reflected = reflected_definition

    def exec_driver_sql(self, sql, params=None):
        self.calls.append((sql, params))
        if sql.strip().upper().startswith("SELECT VIEW_DEFINITION"):
            return _FakeResult((self._reflected,))
        return _FakeResult(None)


class TestTempViewHostSchema:
    def test_temp_view_created_and_read_in_given_schema(self):
        conn = _FakeConn("SELECT a FROM t")
        result = _get_canonical_sql_via_temp_view(conn, "locked_schema", "SELECT a FROM t")
        assert result == "SELECT a FROM t"
        create = conn.calls[0][0]
        assert "CREATE OR REPLACE VIEW `locked_schema`.`_alembic_cmp_" in create
        # read-back filters information_schema.views on the same schema
        read = conn.calls[1]
        assert read[1][0] == "locked_schema"
        # temp view dropped from the same schema
        assert any("DROP VIEW IF EXISTS `locked_schema`.`_alembic_cmp_" in c[0] for c in conn.calls)


class TestViewCompareRoutesToConfiguredSchema:
    def test_temp_view_schema_overrides_object_schema(self, monkeypatch):
        captured = {}

        def fake_temp_view(conn, schema, sql):
            captured["schema"] = schema
            return sql  # canonical form == input, so no phantom delta

        monkeypatch.setattr(compare, "_get_canonical_sql_via_temp_view", fake_temp_view)

        md = MetaData()
        conn_view = View("v", md, definition="SELECT a FROM t")
        meta_view = View("v", MetaData(), definition="SELECT a FROM t")
        changed = _compare_view_definition_and_columns(
            AlterViewOp(view_name="v", schema="app"),
            "app.v", conn_view, meta_view,
            conn=object(), schema="app", temp_view_schema="locked_schema",
        )
        assert changed is False
        assert captured["schema"] == "locked_schema"

    def test_defaults_to_object_schema_when_unset(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(compare, "_get_canonical_sql_via_temp_view",
                            lambda conn, schema, sql: captured.setdefault("schema", schema) or sql)
        conn_view = View("v", MetaData(), definition="SELECT a FROM t")
        meta_view = View("v", MetaData(), definition="SELECT a FROM t")
        _compare_view_definition_and_columns(
            AlterViewOp(view_name="v", schema="app"),
            "app.v", conn_view, meta_view,
            conn=object(), schema="app",  # temp_view_schema not passed
        )
        assert captured["schema"] == "app"
