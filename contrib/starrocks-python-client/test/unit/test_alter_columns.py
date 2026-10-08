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

"""Tests for coalescing multiple ADD/DROP COLUMN operations into a single
StarRocks ``ALTER TABLE`` statement (one schema-change job)."""

import re

from alembic.autogenerate.api import AutogenContext
from alembic.operations import ops
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column, MetaData, Table

from starrocks import INTEGER, VARCHAR
from starrocks.alembic.ops import AlterTableColumnsOp, _combine_column_alters
from starrocks.alembic.render import _render_alter_table_columns
from starrocks.dialect import StarRocksDialect
from starrocks.sql.ddl import AlterTableColumns


def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _autogen_context() -> AutogenContext:
    """Build a real AutogenContext bound to the StarRocks dialect (offline)."""
    mc = MigrationContext.configure(dialect_name="starrocks")
    opts = {
        "sqlalchemy_module_prefix": "sa.",
        "alembic_module_prefix": "op.",
        "user_module_prefix": None,
        "render_item": None,
    }
    return AutogenContext(mc, metadata=MetaData(), opts=opts, autogenerate=False)


class TestCombineColumnAltersRewriter:
    def test_multiple_column_ops_collapse_to_one(self):
        mto = ops.ModifyTableOps("my_table", ops=[
            ops.AddColumnOp("my_table", Column("a", INTEGER)),
            ops.DropColumnOp("my_table", "c"),
            ops.AddColumnOp("my_table", Column("b", VARCHAR(50))),
        ], schema="mydb")

        res = _combine_column_alters(None, None, mto)

        assert len(res.ops) == 1
        combined = res.ops[0]
        assert isinstance(combined, AlterTableColumnsOp)
        assert combined.table_name == "my_table"
        assert combined.schema == "mydb"
        assert [c.name for c in combined.adds] == ["a", "b"]
        assert [c.name for c in combined.drops] == ["c"]

    def test_single_column_op_is_unchanged(self):
        mto = ops.ModifyTableOps("t2", ops=[ops.AddColumnOp("t2", Column("x", INTEGER))])
        res = _combine_column_alters(None, None, mto)
        assert len(res.ops) == 1
        assert isinstance(res.ops[0], ops.AddColumnOp)

    def test_non_column_op_is_preserved_while_columns_coalesce(self):
        # A non-index op (here a MODIFY on a different column) is kept; the
        # add/drop on either side of it still coalesce into one op.
        other = ops.AlterColumnOp("t3", "keep", modify_comment="hi")
        mto = ops.ModifyTableOps("t3", ops=[
            ops.AddColumnOp("t3", Column("a", INTEGER)),
            other,
            ops.DropColumnOp("t3", "c"),
        ])
        res = _combine_column_alters(None, None, mto)

        assert len(res.ops) == 2
        assert isinstance(res.ops[0], AlterTableColumnsOp)
        assert [c.name for c in res.ops[0].adds] == ["a"]
        assert [c.name for c in res.ops[0].drops] == ["c"]
        assert res.ops[1] is other

    def test_only_drops_still_collapse(self):
        mto = ops.ModifyTableOps("t4", ops=[
            ops.DropColumnOp("t4", "c1"),
            ops.DropColumnOp("t4", "c2"),
        ])
        res = _combine_column_alters(None, None, mto)
        assert len(res.ops) == 1
        combined = res.ops[0]
        assert [c.name for c in combined.drops] == ["c1", "c2"]
        assert combined.adds == []


class TestAlterTableColumnsOp:
    def test_reverse_swaps_adds_and_drops(self):
        add_col = Column("a", INTEGER)
        drop_col = Column("c", VARCHAR(50))
        op = AlterTableColumnsOp("t", adds=[add_col], drops=[drop_col], schema="db")

        rev = op.reverse()
        assert [c.name for c in rev.adds] == ["c"]
        assert [c.name for c in rev.drops] == ["a"]
        assert rev.schema == "db"

    def test_to_diff_tuple(self):
        op = AlterTableColumnsOp(
            "t",
            adds=[Column("a", INTEGER)],
            drops=[Column("c", INTEGER)],
            schema="db",
        )
        assert op.to_diff_tuple() == ("alter_table_columns", "db", "t", ["a"], ["c"])


class TestAlterTableColumnsCompile:
    def _compile(self, ddl) -> str:
        return _normalize(str(ddl.compile(dialect=StarRocksDialect())))

    def test_combined_add_and_drop(self):
        m = MetaData()
        t = Table("my_table", m,
                  Column("a", INTEGER, nullable=True),
                  Column("b", VARCHAR(50), nullable=False),
                  schema="mydb")
        ddl = AlterTableColumns("my_table", adds=[t.c.a, t.c.b], drops=["c"], schema="mydb")
        sql = self._compile(ddl)
        assert sql == (
            "ALTER TABLE mydb.my_table ADD COLUMN a INTEGER, "
            "ADD COLUMN b VARCHAR(50) NOT NULL, DROP COLUMN c"
        )

    def test_adds_only_no_schema(self):
        m = MetaData()
        t = Table("t", m, Column("x", INTEGER))
        ddl = AlterTableColumns("t", adds=[t.c.x])
        assert self._compile(ddl) == "ALTER TABLE t ADD COLUMN x INTEGER"

    def test_empty_raises(self):
        import pytest
        from sqlalchemy import exc
        ddl = AlterTableColumns("t")
        with pytest.raises(exc.CompileError):
            ddl.compile(dialect=StarRocksDialect())


class TestRenderAlterTableColumns:
    def test_render_add_and_drop(self):
        ctx = _autogen_context()
        op = AlterTableColumnsOp(
            "my_table",
            adds=[Column("a", INTEGER), Column("b", VARCHAR(50))],
            drops=[Column("c", INTEGER)],
            schema="mydb",
        )
        rendered = _normalize(_render_alter_table_columns(ctx, op))
        assert rendered.startswith("op.alter_table_columns(")
        assert "'my_table'" in rendered
        assert "adds=[" in rendered
        assert "sa.Column('a'" in rendered
        assert "sa.Column('b'" in rendered
        assert "drops=[sa.Column('c')]" in rendered
        assert "schema='mydb'" in rendered


class TestRangeSortKeyCoalescing:
    """Pin the SQL shape the FE rejects on shared-data RANGE-distributed tables.

    ``SchemaChangeHandler`` routes an ADD of a key column or a DROP of a
    sort-key column on such a table to a dedicated rewrite job and throws
    "... can not be combined with other alter operations" when the statement
    carries more than one clause. These tests record that the rewriter is
    currently distribution-blind, so it emits exactly that shape; the live
    behaviour is covered by
    ``test/integration/test_combine_column_alters_range_key.py``.
    """

    def test_sort_key_drop_is_coalesced_with_another_change(self):
        # k2 would be part of the sort key of a DUPLICATE KEY(k1, k2) table.
        mto = ops.ModifyTableOps("t_range", ops=[
            ops.DropColumnOp("t_range", "k2"),
            ops.AddColumnOp("t_range", Column("v2", INTEGER)),
        ], schema="mydb")

        res = _combine_column_alters(None, None, mto)

        assert len(res.ops) == 1
        combined = res.ops[0]
        assert isinstance(combined, AlterTableColumnsOp)
        # Both changes land in one statement -- rejected by the FE on a
        # shared-data RANGE table. A fix must leave the sort-key drop standalone.
        assert [c.name for c in combined.drops] == ["k2"]
        assert [c.name for c in combined.adds] == ["v2"]

    def test_coalesced_ddl_renders_as_one_multi_clause_statement(self):
        add_col = Column("v2", INTEGER)
        # Bind the column to a table so the compiler can render its full
        # specification, mirroring the toimpl implementation.
        Table("t_range", MetaData(), add_col, schema="mydb")
        ddl = AlterTableColumns(
            "t_range",
            adds=[add_col],
            drops=["k2"],
            schema="mydb",
        )
        sql = _normalize(str(ddl.compile(dialect=StarRocksDialect())))

        assert sql.count("ALTER TABLE") == 1
        assert "ADD COLUMN" in sql and "DROP COLUMN" in sql
        # One ALTER carrying two clauses: the shape the routed range paths refuse.
        assert sql.index("DROP COLUMN") > sql.index("ALTER TABLE")
