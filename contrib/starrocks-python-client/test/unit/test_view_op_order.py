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

"""Where autogenerate puts view and materialized view operations, in the upgrade and in its reversed downgrade."""

from types import SimpleNamespace

from alembic.operations import ops
from sqlalchemy import Column, MetaData, Table, testing
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_

from starrocks import INTEGER, VARCHAR
from starrocks.alembic.compare import order_view_and_mv_ops
from starrocks.alembic.ops import (
    AlterMaterializedViewOp,
    AlterViewOp,
    CreateMaterializedViewOp,
    CreateViewOp,
    DropMaterializedViewOp,
    DropViewOp,
)
from starrocks.common import sql_canonical


def _ordered(*operations):
    upgrade_ops = ops.UpgradeOps(ops=list(operations))
    context = SimpleNamespace(dialect=SimpleNamespace(name="starrocks", default_schema_name="db"))
    order_view_and_mv_ops(context, upgrade_ops, {None})
    return upgrade_ops


def _labels(operations):
    labels = []
    for op in operations.ops:
        name = getattr(op, "view_name", None) or op.table_name
        schema = getattr(op, "schema", None)
        labels.append(f"{type(op).__name__}:{schema + '.' if schema else ''}{name}")
    return labels


def _table(name, *columns, schema=None):
    return Table(name, MetaData(), Column("id", INTEGER), *columns, schema=schema)


class ViewOperationOrderTest(fixtures.TestBase):
    def test_a_removed_view_is_dropped_before_its_table_and_created_after_it(self):
        upgrade = _ordered(
            ops.DropTableOp.from_table(_table("t1")),
            DropViewOp("v1", existing_definition="SELECT id FROM t1"),
            DropMaterializedViewOp("mv1", existing_definition="SELECT id FROM t1"),
        )
        eq_(_labels(upgrade), ["DropMaterializedViewOp:mv1", "DropViewOp:v1", "DropTableOp:t1"])
        eq_(_labels(upgrade.reverse()), ["CreateTableOp:t1", "CreateViewOp:v1", "CreateMaterializedViewOp:mv1"])

    def test_an_added_view_is_created_after_its_table(self):
        upgrade = _ordered(
            CreateViewOp("a_view", "SELECT id FROM z_table"),
            ops.CreateTableOp.from_table(_table("z_table")),
        )
        eq_(_labels(upgrade), ["CreateTableOp:z_table", "CreateViewOp:a_view"])

    def test_an_alter_reading_a_column_the_revision_drops_becomes_a_drop_and_a_create(self):
        drop_name = ops.ModifyTableOps(
            "t1", [ops.DropColumnOp.from_column_and_tablename(None, "t1", Column("name", VARCHAR(20)))]
        )
        upgrade = _ordered(
            drop_name,
            AlterViewOp(
                "v1",
                definition="SELECT id FROM t1",
                existing_definition="SELECT id, name FROM t1",
                existing_security="INVOKER",
            ),
        )
        eq_(_labels(upgrade), ["DropViewOp:v1", "ModifyTableOps:t1", "CreateViewOp:v1"])
        eq_(upgrade.ops[0].kwargs, {"starrocks_security": "INVOKER"})
        eq_((upgrade.ops[2].definition, upgrade.ops[2].kwargs), ("SELECT id FROM t1", {"starrocks_security": "INVOKER"}))
        downgrade = upgrade.reverse()
        eq_(_labels(downgrade), ["DropViewOp:v1", "ModifyTableOps:t1", "CreateViewOp:v1"])
        eq_(downgrade.ops[2].definition, "SELECT id, name FROM t1")

    def test_an_alter_reading_a_view_the_revision_drops_becomes_a_drop_and_a_create(self):
        upgrade = _ordered(
            DropViewOp("vb", existing_definition="SELECT id, a FROM t"),
            AlterViewOp("va", definition="SELECT id FROM t", existing_definition="SELECT id FROM vb"),
        )
        eq_(_labels(upgrade), ["DropViewOp:va", "DropViewOp:vb", "CreateViewOp:va"])
        downgrade = upgrade.reverse()
        eq_(_labels(downgrade), ["DropViewOp:va", "CreateViewOp:vb", "CreateViewOp:va"])
        eq_(downgrade.ops[2].definition, "SELECT id FROM vb")

    def test_an_alter_reading_an_mv_the_revision_drops_becomes_a_drop_and_a_create(self):
        upgrade = _ordered(
            DropMaterializedViewOp("mv", existing_definition="SELECT id, a FROM t"),
            AlterViewOp("v", definition="SELECT id FROM t", existing_definition="SELECT id FROM mv"),
        )
        eq_(_labels(upgrade), ["DropViewOp:v", "DropMaterializedViewOp:mv", "CreateViewOp:v"])
        eq_(_labels(upgrade.reverse()), ["DropViewOp:v", "CreateMaterializedViewOp:mv", "CreateViewOp:v"])

    def test_an_alter_reading_an_mv_the_revision_recreates_becomes_a_drop_and_a_create(self):
        upgrade = _ordered(
            DropMaterializedViewOp("mv", existing_definition="SELECT id, a, b FROM t"),
            CreateMaterializedViewOp("mv", "SELECT id, a FROM t"),
            AlterViewOp("v", definition="SELECT id, a FROM mv", existing_definition="SELECT id, b FROM mv"),
        )
        eq_(
            _labels(upgrade),
            ["DropViewOp:v", "DropMaterializedViewOp:mv", "CreateMaterializedViewOp:mv", "CreateViewOp:v"],
        )
        downgrade = upgrade.reverse()
        eq_(
            _labels(downgrade),
            ["DropViewOp:v", "DropMaterializedViewOp:mv", "CreateMaterializedViewOp:mv", "CreateViewOp:v"],
        )
        eq_((downgrade.ops[2].definition, downgrade.ops[3].definition), ("SELECT id, a, b FROM t", "SELECT id, b FROM mv"))

    @testing.combinations(
        ("a_renamed_table", lambda: ops.RenameTableOp("t1", "t2"), "SELECT id, name FROM t2"),
        (
            "a_renamed_column",
            lambda: ops.ModifyTableOps("t1", [ops.AlterColumnOp("t1", "name", modify_name="full_name")]),
            "SELECT id, full_name FROM t1",
        ),
        argnames="rename, definition",
        id_="iaa",
    )
    def test_an_alter_reading_a_renamed_table_or_column_becomes_a_drop_and_a_create(self, rename, definition):
        """Autogenerate writes no rename itself, but a comparator of the user's may; the old definition reads the old
        name, so the downgrade can create the view only once the rename is undone."""
        upgrade = _ordered(
            rename(), AlterViewOp("v1", definition=definition, existing_definition="SELECT id, name FROM t1")
        )
        labels = _labels(upgrade)
        eq_((labels[0], labels[-1]), ("DropViewOp:v1", "CreateViewOp:v1"))
        eq_(upgrade.ops[0].existing_definition, "SELECT id, name FROM t1")

    @testing.combinations(
        ("w", "v", "SELECT id FROM w"),
        ("b_inner", "a_outer", "SELECT id FROM b_inner"),
        ("w", "v", "SELECT id FROM t"),
        ("a_inner", "b_outer", "SELECT id FROM t"),
        argnames="inner, outer, outer_definition",
    )
    def test_an_alter_reading_a_view_the_revision_redefines_becomes_a_drop_and_a_create(
        self, inner, outer, outer_definition
    ):
        """The downgrade restores the outer view's old definition, which reads a column the inner view only had before."""
        upgrade = _ordered(
            AlterViewOp(inner, definition="SELECT id FROM t", existing_definition="SELECT id, a FROM t"),
            AlterViewOp(outer, definition=outer_definition, existing_definition=f"SELECT id, a FROM {inner}"),
        )
        eq_(sorted(_labels(upgrade)), sorted([f"DropViewOp:{outer}", f"AlterViewOp:{inner}", f"CreateViewOp:{outer}"]))
        eq_(_labels(upgrade)[0], f"DropViewOp:{outer}")
        downgrade = _labels(upgrade.reverse())
        create_outer = downgrade.index(f"CreateViewOp:{outer}")
        assert downgrade.index(f"AlterViewOp:{inner}") < create_outer, downgrade
        eq_(upgrade.reverse().ops[create_outer].definition, f"SELECT id, a FROM {inner}")

    def test_an_alter_over_a_table_losing_a_column_it_does_not_read_stays_an_alter_after_the_tables(self, caplog):
        """The downgrade alters the view back while the table still has what the old definition reads, so the view
        keeps its grants, which a drop and a create would lose."""
        change_columns = ops.ModifyTableOps("t1", [
            ops.AddColumnOp.from_column_and_tablename(None, "t1", Column("b", INTEGER)),
            ops.DropColumnOp.from_column_and_tablename(None, "t1", Column("note", VARCHAR(20))),
        ])
        upgrade = _ordered(
            change_columns,
            AlterViewOp("v1", definition="SELECT id, a, b FROM t1", existing_definition="SELECT id, a FROM t1"),
        )
        eq_(_labels(upgrade), ["ModifyTableOps:t1", "AlterViewOp:v1"])
        eq_(_labels(upgrade.reverse()), ["AlterViewOp:v1", "ModifyTableOps:t1"])
        assert "grants" not in caplog.text

    def test_a_split_alter_warns_that_the_view_loses_its_grants(self, caplog):
        drop_note = ops.ModifyTableOps(
            "t1", [ops.DropColumnOp.from_column_and_tablename(None, "t1", Column("note", VARCHAR(20)))]
        )
        _ordered(
            drop_note, AlterViewOp("v1", definition="SELECT id FROM t1", existing_definition="SELECT id, note FROM t1")
        )
        assert "The view 'v1' reads what the revision removes" in caplog.text, caplog.text
        assert "StarRocks drops a view's grants with the view" in caplog.text

    @staticmethod
    def _over_a_view_gaining_a_column(definition, columns):
        return _ordered(
            AlterViewOp(
                "w", definition=definition, columns=columns, existing_definition="SELECT id, a FROM t",
                existing_columns=[{"name": "id"}, {"name": "a"}],
            ),
            AlterViewOp("v", definition="SELECT id, a, b FROM w", existing_definition="SELECT id, a FROM w"),
        )

    def test_an_alter_reading_a_view_that_only_gains_columns_stays_an_alter(self):
        """The downgrade restores the outer view's old definition while the inner one still has every column it
        reads. The model's column list tells the inner view's new columns."""
        columns = [{"name": "id"}, {"name": "a"}, {"name": "b"}]
        eq_(_labels(self._over_a_view_gaining_a_column("SELECT id, a, x AS b FROM t", columns)),
            ["AlterViewOp:w", "AlterViewOp:v"])

    @testing.skip_if(lambda: not sql_canonical.is_available(), "sqlglot is not installed")
    def test_the_new_columns_of_a_view_without_a_column_list_are_read_from_its_definition(self):
        eq_(_labels(self._over_a_view_gaining_a_column("SELECT id, a, x AS b FROM t", None)),
            ["AlterViewOp:w", "AlterViewOp:v"])

    def test_a_view_whose_new_columns_cannot_be_read_counts_as_losing_every_column(self):
        """``a + 1`` has no alias, so StarRocks names that column itself."""
        eq_(_labels(self._over_a_view_gaining_a_column("SELECT id, a, a + 1 FROM t", None)),
            ["DropViewOp:v", "AlterViewOp:w", "CreateViewOp:v"])

    @testing.combinations(
        ("a_column_it_reads", "SELECT id, b FROM w", ["DropViewOp:v", "AlterViewOp:w", "CreateViewOp:v"]),
        ("only_columns_it_does_not_read", "SELECT id, a FROM w", ["AlterViewOp:w", "AlterViewOp:v"]),
        argnames="outer_definition, labels",
        id_="iaa",
    )
    def test_an_alter_reading_a_view_that_loses_columns_is_split_only_for_a_column_it_reads(
        self, outer_definition, labels
    ):
        upgrade = _ordered(
            AlterViewOp(
                "w", definition="SELECT id, a FROM t", existing_definition="SELECT id, a, b FROM t",
                existing_columns=[{"name": "id"}, {"name": "a"}, {"name": "b"}],
            ),
            AlterViewOp("v", definition="SELECT id FROM w", existing_definition=outer_definition),
        )
        eq_(_labels(upgrade), labels)

    def test_a_comment_change_over_a_redefined_view_stays_an_alter(self):
        """Its definition stays the same, so it reads the view before and after the change alike."""
        upgrade = _ordered(
            AlterViewOp("w", definition="SELECT id, a, b FROM t", existing_definition="SELECT id, a FROM t"),
            AlterViewOp("v", comment="new", existing_comment="old", existing_definition="SELECT id, a FROM w"),
        )
        eq_(_labels(upgrade), ["AlterViewOp:w", "AlterViewOp:v"])

    def test_an_alter_reading_a_view_whose_alter_is_split_is_split_too(self):
        drop_name = ops.ModifyTableOps(
            "t1", [ops.DropColumnOp.from_column_and_tablename(None, "t1", Column("name", VARCHAR(20)))]
        )
        upgrade = _ordered(
            drop_name,
            AlterViewOp("vb", definition="SELECT id FROM t1", existing_definition="SELECT id, name FROM t1"),
            AlterViewOp("va", definition="SELECT id FROM vb", existing_definition="SELECT id, name FROM vb"),
        )
        eq_(
            _labels(upgrade),
            ["DropViewOp:va", "DropViewOp:vb", "ModifyTableOps:t1", "CreateViewOp:vb", "CreateViewOp:va"],
        )
        eq_(
            _labels(upgrade.reverse()),
            ["DropViewOp:va", "DropViewOp:vb", "ModifyTableOps:t1", "CreateViewOp:vb", "CreateViewOp:va"],
        )

    def test_an_alter_of_a_view_over_unchanged_tables_stays_an_alter_after_the_tables(self):
        add_name = ops.ModifyTableOps("t1", [ops.AddColumnOp.from_column_and_tablename(None, "t1", Column("name", VARCHAR(20)))])
        upgrade = _ordered(
            AlterViewOp("v1", definition="SELECT id, name FROM t1", existing_definition="SELECT id FROM t1"),
            add_name,
            AlterMaterializedViewOp("mv1", refresh="MANUAL", existing_refresh="ASYNC"),
        )
        eq_(_labels(upgrade), ["ModifyTableOps:t1", "AlterMaterializedViewOp:mv1", "AlterViewOp:v1"])

    def test_views_and_mvs_reading_each_other_follow_what_they_read(self):
        upgrade = _ordered(
            CreateViewOp("a_view", "SELECT id FROM m_view"),
            CreateViewOp("m_view", "SELECT id FROM `z_mv`"),
            CreateMaterializedViewOp("z_mv", "SELECT id FROM db.t1"),
            ops.CreateTableOp.from_table(_table("t1")),
        )
        eq_(
            _labels(upgrade),
            ["CreateTableOp:t1", "CreateMaterializedViewOp:z_mv", "CreateViewOp:m_view", "CreateViewOp:a_view"],
        )
        eq_(
            _labels(upgrade.reverse()),
            ["DropViewOp:a_view", "DropViewOp:m_view", "DropMaterializedViewOp:z_mv", "DropTableOp:t1"],
        )

    def test_views_named_alike_in_two_schemas_are_ordered_by_name(self):
        upgrade = _ordered(
            CreateViewOp("orders", "SELECT id FROM staging.orders", schema="reporting"),
            CreateViewOp("orders", "SELECT id FROM staging.orders", schema="archive"),
        )
        eq_(_labels(upgrade), ["CreateViewOp:archive.orders", "CreateViewOp:reporting.orders"])

    def test_a_cycle_keeps_name_order(self):
        upgrade = _ordered(CreateViewOp("b", "SELECT id FROM a"), CreateViewOp("a", "SELECT id FROM b"))
        eq_(_labels(upgrade), ["CreateViewOp:a", "CreateViewOp:b"])

    def test_without_views_the_operations_are_left_alone(self):
        tables = [ops.CreateTableOp.from_table(_table("z")), ops.CreateTableOp.from_table(_table("a"))]
        eq_(_labels(_ordered(*tables)), ["CreateTableOp:z", "CreateTableOp:a"])
