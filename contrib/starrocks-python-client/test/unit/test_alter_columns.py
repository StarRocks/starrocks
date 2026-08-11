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
from starrocks.alembic.ops import StarRocksAlterColumnsOp, _combine_column_alters
from starrocks.alembic.render import _render_starrocks_alter_columns
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
        assert isinstance(combined, StarRocksAlterColumnsOp)
        assert combined.table_name == "my_table"
        assert combined.schema == "mydb"
        assert [c.name for c in combined.adds] == ["a", "b"]
        assert [c.name for c in combined.drops] == ["c"]

    def test_single_column_op_is_unchanged(self):
        mto = ops.ModifyTableOps("t2", ops=[ops.AddColumnOp("t2", Column("x", INTEGER))])
        res = _combine_column_alters(None, None, mto)
        assert len(res.ops) == 1
        assert isinstance(res.ops[0], ops.AddColumnOp)

    def test_non_column_ops_are_preserved_in_order(self):
        # A comment change interleaved with column changes must remain, and the
        # single combined op takes the position of the first column change.
        other = ops.AlterColumnOp("t3", "keep", modify_comment="hi")
        mto = ops.ModifyTableOps("t3", ops=[
            ops.AddColumnOp("t3", Column("a", INTEGER)),
            other,
            ops.DropColumnOp("t3", "c"),
        ])
        res = _combine_column_alters(None, None, mto)

        assert len(res.ops) == 2
        assert isinstance(res.ops[0], StarRocksAlterColumnsOp)
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


class TestStarRocksAlterColumnsOp:
    def test_reverse_swaps_adds_and_drops(self):
        add_col = Column("a", INTEGER)
        drop_col = Column("c", VARCHAR(50))
        op = StarRocksAlterColumnsOp("t", adds=[add_col], drops=[drop_col], schema="db")

        rev = op.reverse()
        assert [c.name for c in rev.adds] == ["c"]
        assert [c.name for c in rev.drops] == ["a"]
        assert rev.schema == "db"

    def test_to_diff_tuple(self):
        op = StarRocksAlterColumnsOp(
            "t",
            adds=[Column("a", INTEGER)],
            drops=[Column("c", INTEGER)],
            schema="db",
        )
        assert op.to_diff_tuple() == ("starrocks_alter_columns", "db", "t", ["a"], ["c"])


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


class TestRenderStarRocksAlterColumns:
    def test_render_add_and_drop(self):
        ctx = _autogen_context()
        op = StarRocksAlterColumnsOp(
            "my_table",
            adds=[Column("a", INTEGER), Column("b", VARCHAR(50))],
            drops=[Column("c", INTEGER)],
            schema="mydb",
        )
        rendered = _normalize(_render_starrocks_alter_columns(ctx, op))
        assert rendered.startswith("op.starrocks_alter_columns(")
        assert "'my_table'" in rendered
        assert "adds=[" in rendered
        assert "sa.Column('a'" in rendered
        assert "sa.Column('b'" in rendered
        assert "drops=[sa.Column('c')]" in rendered
        assert "schema='mydb'" in rendered
