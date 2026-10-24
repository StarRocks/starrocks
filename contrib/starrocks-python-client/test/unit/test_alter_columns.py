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

"""Coalescing several ADD/DROP COLUMN operations into one StarRocks ``ALTER TABLE`` (one schema-change job)."""

from alembic.autogenerate.api import AutogenContext
from alembic.operations import ops
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column, MetaData, Table, exc
from sqlalchemy.testing import expect_raises, fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

from starrocks import INTEGER, VARCHAR
from starrocks.alembic.ops import (
    AlterTableColumnsOp,
    AlterTableDistributionOp,
    AlterTableOrderOp,
    AlterTablePropertiesOp,
    _combine_column_alters,
)
from starrocks.alembic.render import _render_alter_table_columns
from starrocks.dialect import StarRocksDialect
from starrocks.sql.ddl import AlterTableColumns


class CombineColumnAltersTest(fixtures.TestBase):
    def test_multiple_column_ops_collapse_to_one(self):
        mto = ops.ModifyTableOps(
            "my_table",
            ops=[
                ops.AddColumnOp("my_table", Column("a", INTEGER)),
                ops.DropColumnOp("my_table", "c"),
                ops.AddColumnOp("my_table", Column("b", VARCHAR(50))),
            ],
            schema="mydb",
        )

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
        """A MODIFY on another column is kept, and the ADD and DROP on either side of it still coalesce."""
        other = ops.AlterColumnOp("t3", "keep", modify_comment="hi")
        mto = ops.ModifyTableOps(
            "t3",
            ops=[
                ops.AddColumnOp("t3", Column("a", INTEGER)),
                other,
                ops.DropColumnOp("t3", "c"),
            ],
        )
        res = _combine_column_alters(None, None, mto)

        assert len(res.ops) == 2
        assert isinstance(res.ops[0], AlterTableColumnsOp)
        assert [c.name for c in res.ops[0].adds] == ["a"]
        assert [c.name for c in res.ops[0].drops] == ["c"]
        assert res.ops[1] is other

    def test_adds_go_before_a_sort_key_change_and_drops_after_it(self):
        """The new sort key needs its column, and the old sort key column can only go once the key has moved."""
        order = AlterTableOrderOp("t5", "x")
        mto = ops.ModifyTableOps(
            "t5",
            ops=[
                ops.DropColumnOp("t5", "v"),
                order,
                ops.AddColumnOp("t5", Column("x", INTEGER)),
                ops.AddColumnOp("t5", Column("y", INTEGER)),
            ],
        )
        res = _combine_column_alters(None, None, mto)

        assert [type(o).__name__ for o in res.ops] == ["AlterTableColumnsOp", "AlterTableOrderOp", "AlterTableColumnsOp"]
        assert ([c.name for c in res.ops[0].adds], res.ops[0].drops) == (["x", "y"], [])
        assert res.ops[1] is order
        assert (res.ops[2].adds, [c.name for c in res.ops[2].drops]) == ([], ["v"])

    def test_adds_go_before_a_property_change_and_drops_after_it(self):
        """``bloom_filter_columns`` may name an added column, and StarRocks refuses to SET it without a dropped one."""
        properties = AlterTablePropertiesOp(
            "t7", {"bloom_filter_columns": "a, x"}, existing_properties={"bloom_filter_columns": "a, v"}
        )
        mto = ops.ModifyTableOps(
            "t7",
            ops=[
                ops.AddColumnOp("t7", Column("x", INTEGER)),
                properties,
                ops.DropColumnOp("t7", "v"),
                ops.DropColumnOp("t7", "w"),
            ],
        )
        res = _combine_column_alters(None, None, mto)

        assert [type(o).__name__ for o in res.ops] == [
            "AlterTableColumnsOp", "AlterTablePropertiesOp", "AlterTableColumnsOp"
        ]
        assert ([c.name for c in res.ops[0].adds], res.ops[0].drops) == (["x"], [])
        assert res.ops[1] is properties
        assert (res.ops[2].adds, [c.name for c in res.ops[2].drops]) == ([], ["v", "w"])

    def test_adds_go_before_a_distribution_change_and_drops_after_it(self):
        """StarRocks refuses to drop a column the distribution names, and to distribute by one it does not have."""
        distribution = AlterTableDistributionOp("t8", "HASH(d)", 1, existing_distribution_method="HASH(b)")
        mto = ops.ModifyTableOps(
            "t8", ops=[ops.AddColumnOp("t8", Column("d", INTEGER)), ops.DropColumnOp("t8", "b"), distribution]
        )
        res = _combine_column_alters(None, None, mto)

        assert [type(o).__name__ for o in res.ops] == [
            "AlterTableColumnsOp", "AlterTableDistributionOp", "AlterTableColumnsOp"
        ]
        assert ([c.name for c in res.ops[0].adds], res.ops[0].drops) == (["d"], [])
        assert res.ops[1] is distribution
        assert (res.ops[2].adds, [c.name for c in res.ops[2].drops]) == ([], ["b"])

    def test_a_distribution_change_naming_no_changed_column_leaves_one_combined_op(self):
        """A change of bucket count alone needs no column order, so the column changes stay one schema-change job."""
        distribution = AlterTableDistributionOp(
            "t9", "HASH(`id`)", 4, existing_distribution_method="HASH(id)", existing_buckets=1
        )
        mto = ops.ModifyTableOps(
            "t9", ops=[ops.AddColumnOp("t9", Column("d", INTEGER)), ops.DropColumnOp("t9", "b"), distribution]
        )
        res = _combine_column_alters(None, None, mto)

        assert [type(o).__name__ for o in res.ops] == ["AlterTableColumnsOp", "AlterTableDistributionOp"]

    def test_only_drops_still_collapse(self):
        mto = ops.ModifyTableOps(
            "t4",
            ops=[
                ops.DropColumnOp("t4", "c1"),
                ops.DropColumnOp("t4", "c2"),
            ],
        )
        res = _combine_column_alters(None, None, mto)
        assert len(res.ops) == 1
        combined = res.ops[0]
        assert [c.name for c in combined.drops] == ["c1", "c2"]
        assert combined.adds == []


class AlterTableColumnsOpTest(fixtures.TestBase):
    def test_reverse_swaps_adds_and_drops(self):
        op = AlterTableColumnsOp("t", adds=[Column("a", INTEGER)], drops=[Column("c", VARCHAR(50))], schema="db")

        rev = op.reverse()
        assert [c.name for c in rev.adds] == ["c"]
        assert [c.name for c in rev.drops] == ["a"]
        assert rev.schema == "db"

    def test_to_diff_tuple(self):
        op = AlterTableColumnsOp("t", adds=[Column("a", INTEGER)], drops=[Column("c", INTEGER)], schema="db")
        assert op.to_diff_tuple() == ("alter_table_columns", "db", "t", ["a"], ["c"])


class AlterTableColumnsCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()

    def test_combined_add_and_drop(self):
        t = Table(
            "my_table",
            MetaData(),
            Column("a", INTEGER, nullable=True),
            Column("b", VARCHAR(50), nullable=False),
            schema="mydb",
        )
        self.assert_compile(
            AlterTableColumns("my_table", adds=[t.c.a, t.c.b], drops=["c"], schema="mydb"),
            "ALTER TABLE mydb.my_table ADD COLUMN a INTEGER, ADD COLUMN b VARCHAR(50) NOT NULL, DROP COLUMN c",
        )

    def test_adds_only_no_schema(self):
        t = Table("t", MetaData(), Column("x", INTEGER))
        self.assert_compile(AlterTableColumns("t", adds=[t.c.x]), "ALTER TABLE t ADD COLUMN x INTEGER")

    def test_empty_raises(self):
        with expect_raises(exc.CompileError):
            AlterTableColumns("t").compile(dialect=self.__dialect__)


class RenderAlterTableColumnsTest(fixtures.TestBase):
    def test_render_add_and_drop(self):
        autogen_context = AutogenContext(
            MigrationContext.configure(dialect_name="starrocks"),
            metadata=MetaData(),
            opts={
                "sqlalchemy_module_prefix": "sa.",
                "alembic_module_prefix": "op.",
                "user_module_prefix": None,
                "render_item": None,
            },
            autogenerate=False,
        )
        op = AlterTableColumnsOp(
            "my_table",
            adds=[Column("a", INTEGER), Column("b", VARCHAR(50))],
            drops=[Column("c", INTEGER)],
            schema="mydb",
        )
        assert _render_alter_table_columns(autogen_context, op) == (
            "op.alter_table_columns(\n"
            "    'my_table',\n"
            "    adds=[sa.Column('a', starrocks.datatype.INTEGER(), nullable=True), "
            "sa.Column('b', starrocks.datatype.VARCHAR(length=50), nullable=True)],\n"
            "    drops=[sa.Column('c')],\n"
            "    schema='mydb'\n"
            ")"
        )


class RangeSortKeyCoalescingTest(fixtures.TestBase, AssertsCompiledSQL):
    """Pin the SQL shape the FE rejects on shared-data RANGE-distributed tables.

    ``SchemaChangeHandler`` routes an ADD of a key column or a DROP of a sort-key column on such a table to a
    dedicated rewrite job, and throws "... can not be combined with other alter operations" when the statement
    carries more than one clause. These tests record that the rewriter is currently distribution-blind, so it
    emits exactly that shape; a fix must leave the sort-key drop standalone. The live behaviour is covered by
    ``test/integration/test_combine_column_alters_range_key.py``.
    """

    __dialect__ = StarRocksDialect()

    def test_sort_key_drop_is_coalesced_with_another_change(self):
        """``k2`` stands for a sort-key column of a ``DUPLICATE KEY(k1, k2)`` table."""
        mto = ops.ModifyTableOps(
            "t_range",
            ops=[
                ops.DropColumnOp("t_range", "k2"),
                ops.AddColumnOp("t_range", Column("v2", INTEGER)),
            ],
            schema="mydb",
        )

        res = _combine_column_alters(None, None, mto)

        assert len(res.ops) == 1
        combined = res.ops[0]
        assert isinstance(combined, AlterTableColumnsOp)
        assert [c.name for c in combined.drops] == ["k2"]
        assert [c.name for c in combined.adds] == ["v2"]

    def test_coalesced_ddl_renders_as_one_multi_clause_statement(self):
        """The added column is bound to a table, as the Alembic implementation does, so its spec can be compiled."""
        add_col = Column("v2", INTEGER)
        Table("t_range", MetaData(), add_col, schema="mydb")
        self.assert_compile(
            AlterTableColumns("t_range", adds=[add_col], drops=["k2"], schema="mydb"),
            "ALTER TABLE mydb.t_range ADD COLUMN v2 INTEGER, DROP COLUMN k2",
        )
