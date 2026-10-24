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

"""Materialized view comparison in autogenerate."""

from unittest.mock import Mock

from alembic.operations import ops
from sqlalchemy import Column, MetaData, Table, testing
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.testing import eq_, expect_raises, fixtures

from starrocks.alembic.compare import UnalterableChange, _compare_mv, _compare_mv_definition
from starrocks.alembic.ops import (
    AlterMaterializedViewOp,
    CreateMaterializedViewOp,
    CreateViewOp,
    DropMaterializedViewOp,
    DropViewOp,
)
from starrocks.common import sql_canonical
from starrocks.common.params import DialectName, TableInfoKey, TableKind, TableObjectInfoKey
from starrocks.datatype import INTEGER
from starrocks.sql.schema import MaterializedView


class CompareMaterializedViewTest(fixtures.TestBase):
    """``_compare_mv``: refresh and properties are altered in place; other changes are refused."""

    __requires__ = ("sqlalchemy_2",)

    def setup_test(self):
        self.mock_autogen_context = Mock()
        self.mock_autogen_context.dialect = Mock()
        self.mock_autogen_context.dialect.name = DialectName
        self.mock_autogen_context.dialect.default_schema_name = None
        self.mock_autogen_context.dialect.server_version_info = (4, 1, 6)
        self.mock_autogen_context.metadata = MetaData()
        Table(
            "t", self.mock_autogen_context.metadata, Column("dt", INTEGER),
            starrocks_partition_by="date_trunc('day', dt)",
        )
        self.mock_autogen_context.inspector.get_view_names.return_value = []
        self.mock_autogen_context.inspector.get_materialized_view_names.return_value = []
        self.mock_autogen_context.inspector.get_table_options.side_effect = NoSuchTableError("not in the database")

    def _create_reflected_mv(
        self,
        name: str,
        definition: str,
        comment: str = None,
        partition_by: str = None,
        distributed_by: str = None,
        order_by: str = None,
        refresh: str = None,
        properties: dict = None,
        schema: str = None,
    ) -> Table:
        """Helper to create a Table object simulating a reflected MV."""
        metadata = MetaData()
        # Reflected MVs are represented as Table objects. MV is also OK.
        # table = MaterializedView(name, metadata, schema=schema, definition=definition)
        table = Table(name, metadata, schema=schema)
        table.info[TableObjectInfoKey.DEFINITION] = definition
        table.info[TableObjectInfoKey.TABLE_KIND] = TableKind.MATERIALIZED_VIEW

        if comment:
            table.comment = comment

        dialect_opts = table.dialect_options[DialectName]
        if partition_by:
            dialect_opts[TableInfoKey.PARTITION_BY] = partition_by
        if distributed_by:
            dialect_opts[TableInfoKey.DISTRIBUTED_BY] = distributed_by
        if order_by:
            dialect_opts[TableInfoKey.ORDER_BY] = order_by
        if refresh:
            dialect_opts[TableInfoKey.REFRESH] = refresh
        if properties:
            dialect_opts[TableInfoKey.PROPERTIES] = properties

        return table

    # ========================================================================
    # ALTER MV - Mutable Attributes (Refresh & Properties)
    # ========================================================================

    def test_alter_mv_refresh_changed(self):
        """ALTER: Refresh strategy changed (generates AlterMaterializedViewOp)."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", refresh="ASYNC")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_refresh="MANUAL")

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(len(upgrade_ops.ops), 1)
        op = upgrade_ops.ops[0]
        assert isinstance(op, AlterMaterializedViewOp)
        eq_(op.view_name, "my_mv")
        eq_(op.refresh, "MANUAL")
        eq_(op.existing_refresh, "ASYNC")
        # Other attributes should be None as they are not changed
        assert op.properties is None
        assert op.existing_properties is None

    def test_spacing_around_parentheses_is_no_refresh_change(self):
        """SHOW CREATE writes EVERY(INTERVAL ...), the model may write EVERY (INTERVAL ...)."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv(
            "my_mv", definition="SELECT 1", refresh="DEFERRED ASYNC EVERY(INTERVAL 2 HOUR)"
        )
        meta_mv = MaterializedView(
            "my_mv", MetaData(), definition="SELECT 1", starrocks_refresh="DEFERRED ASYNC EVERY ( INTERVAL 2 HOUR )"
        )

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(upgrade_ops.ops, [])

    def test_an_omitted_refresh_is_left_alone(self):
        """An MV with a distribution may omit its refresh; StarRocks then reports MANUAL, which is no change."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", distributed_by="RANDOM", refresh="MANUAL")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_distributed_by="RANDOM")

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(upgrade_ops.ops, [])

    def test_a_refresh_the_database_does_not_report_has_no_downgrade_value(self):
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_refresh="MANUAL")

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_([(op.refresh, op.existing_refresh) for op in upgrade_ops.ops], [("MANUAL", None)])

    def test_alter_mv_properties_changed(self):
        """ALTER: Properties changed (generates AlterMaterializedViewOp)."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv(
            "my_mv", definition="SELECT 1", properties={"mv_rewrite_staleness_second": "0"}
        )
        meta_mv = MaterializedView(
            "my_mv", MetaData(), definition="SELECT 1", starrocks_properties={"mv_rewrite_staleness_second": "60"}
        )

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(len(upgrade_ops.ops), 1)
        op = upgrade_ops.ops[0]
        assert isinstance(op, AlterMaterializedViewOp)
        eq_(op.properties, {"mv_rewrite_staleness_second": "60"})
        eq_(op.existing_properties, {"mv_rewrite_staleness_second": "0"})
        assert op.refresh is None

    def test_the_properties_the_database_qualifies_are_not_a_change(self):
        """StarRocks reports ``unique_constraints`` with the catalog and database, which the model need not write."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv(
            "my_mv", definition="SELECT 1", properties={"unique_constraints": "default_catalog.db.t.id,dt"}
        )
        meta_mv = MaterializedView(
            "my_mv", MetaData(), definition="SELECT 1", starrocks_properties={"unique_constraints": "t.id,t.dt"}
        )

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(upgrade_ops.ops, [])

    # ========================================================================
    # DROP/CREATE MV - Immutable Attributes
    # ========================================================================

    @staticmethod
    def _assert_recreated(upgrade_ops, caplog, reason):
        """The materialized view is dropped and created again, and the warning names the change behind it."""
        eq_([type(op) for op in upgrade_ops.ops], [DropMaterializedViewOp, CreateMaterializedViewOp])
        assert "drops it and creates it again" in caplog.text
        assert reason in caplog.text

    def test_modify_mv_definition_changed(self, caplog):
        """DROP/CREATE: Definition changed."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 2")

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)
        self._assert_recreated(upgrade_ops, caplog, "does not support altering MV definition")

    _ALWAYS_KEPT = {"mv_rewrite_staleness_second": "60", "replication_num": "1"}

    _EXCLUDED = {"excluded_trigger_tables": "t", "excluded_refresh_tables": "t"}

    @testing.combinations(
        (
            None, None, {**_ALWAYS_KEPT, **_EXCLUDED},
            {"partition_ttl_number": "5", "auto_refresh_partitions_limit": "3"}, "no PARTITION BY.",
        ),
        (
            "dt", None,
            {**_ALWAYS_KEPT, "partition_ttl_number": "5", "auto_refresh_partitions_limit": "3", **_EXCLUDED},
            {}, None,
        ),
        (
            "dt", "DEFERRED MANUAL", {**_ALWAYS_KEPT, "partition_ttl_number": "5"},
            {"auto_refresh_partitions_limit": "3", **_EXCLUDED}, "a MANUAL refresh.",
        ),
        (
            None, "MANUAL", _ALWAYS_KEPT,
            {"partition_ttl_number": "5", "auto_refresh_partitions_limit": "3", **_EXCLUDED},
            "no PARTITION BY or a MANUAL refresh.",
        ),
        argnames="model_partition_by, model_refresh, kept, lost, reason",
    )
    def test_a_recreated_mv_keeps_the_properties_the_model_leaves_out(
        self, caplog, model_partition_by, model_refresh, kept, lost, reason
    ):
        """Properties other than StarRocks' defaults carry over, except those CREATE refuses for the new MV.

        CREATE MATERIALIZED VIEW refuses the partition properties for an MV with no PARTITION BY, and
        ``auto_refresh_partitions_limit`` and ``excluded_trigger_tables`` for a MANUAL one.
        """
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv(
            "my_mv",
            definition="SELECT 1",
            partition_by="dt",
            refresh="ASYNC",
            properties={
                "mv_rewrite_staleness_second": "60",
                "replication_num": "1",
                "storage_medium": "HDD",
                "partition_ttl_number": "5",
                "auto_refresh_partitions_limit": "3",
                "excluded_trigger_tables": "t",
                "excluded_refresh_tables": "t",
            },
        )
        options = {"starrocks_partition_by": model_partition_by} if model_partition_by else {}
        if model_refresh:
            options["starrocks_refresh"] = model_refresh
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT dt FROM t", **options)

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)
        eq_(upgrade_ops.ops[1].kwargs["starrocks_properties"], kept)
        eq_(f"loses the database's properties {lost!r}" in caplog.text, bool(lost))
        if reason:
            assert f"refuses for a materialized view with {reason}" in caplog.text, caplog.text

    def _recreated_properties(self, model_definition, database_properties, model_partition_by="dt"):
        """Recreate an ASYNC MV partitioned by ``dt`` as one reading ``model_definition``, and return its properties."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv(
            "my_mv", definition="SELECT 1", partition_by="dt", refresh="ASYNC", properties=database_properties
        )
        meta_mv = MaterializedView(
            "my_mv", MetaData(), definition=model_definition, starrocks_partition_by=model_partition_by
        )
        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)
        eq_([type(op) for op in upgrade_ops.ops], [DropMaterializedViewOp, CreateMaterializedViewOp])
        return upgrade_ops.ops[1].kwargs.get("starrocks_properties") or {}

    @testing.combinations(
        ("an_expression_range_table", "date_trunc('day', dt)", "dt", (4, 1, 6), None),
        ("a_range_table", "RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2024-01-01'))", "dt", (4, 1, 6), None),
        ("a_column_list_table", "(dt)", "dt", (4, 1, 6), "LIST partitions"),
        ("a_list_table", "LIST(dt) (PARTITION p1 VALUES IN ('1'))", "dt", (4, 1, 6), "LIST partitions"),
        ("two_partition_columns", "date_trunc('day', dt)", "dt, id", (4, 1, 6), "LIST partitions"),
        ("a_column_list_table_on_3_4_0", "(dt)", "dt", (3, 4, 0), None),
        ("a_column_list_table_on_3_3", "(dt)", "dt", (3, 3, 22), None),
        argnames="table_partition_by, model_partition_by, server_version, reason",
        id_="iaaaa",
    )
    def test_a_recreated_mv_keeps_partition_ttl_number_only_for_range_partitions(
        self, caplog, table_partition_by, model_partition_by, server_version, reason
    ):
        """StarRocks gives an MV the kind of partitions its PARTITION BY's base table has, and LIST ones for two
        columns. From 3.4.1 it takes ``partition_ttl_number`` only for RANGE ones; it takes ``partition_ttl`` for
        any."""
        self.mock_autogen_context.dialect.server_version_info = server_version
        self.mock_autogen_context.metadata = MetaData()
        Table(
            "t", self.mock_autogen_context.metadata, Column("id", INTEGER), Column("dt", INTEGER),
            starrocks_partition_by=table_partition_by,
        )
        ttls = {"partition_ttl_number": "5", "partition_ttl": "2 MONTH"}
        properties = self._recreated_properties("SELECT id, dt FROM t", ttls, model_partition_by)
        eq_(properties, {"partition_ttl": "2 MONTH"} if reason else ttls)
        if reason:
            refused = f"{{'partition_ttl_number': '5'}}, which StarRocks refuses for a materialized view with {reason}."
            assert refused in caplog.text, caplog.text

    def test_partitions_of_a_table_the_dialect_cannot_find_are_not_known_to_be_range(self, caplog):
        properties = self._recreated_properties("SELECT dt FROM elsewhere", {"partition_ttl_number": "5"})
        eq_(properties, {})
        assert "refuses for a materialized view with partitions not known to be RANGE ones." in caplog.text

    def test_partitions_of_a_database_table_the_model_leaves_out_are_read_from_the_database(self):
        self.mock_autogen_context.inspector.get_table_options.side_effect = None
        self.mock_autogen_context.inspector.get_table_options.return_value = {
            "starrocks_partition_by": "RANGE(`dt`) (PARTITION p1 VALUES [('0000-01-01'), ('2024-01-01')))"
        }
        eq_(self._recreated_properties("SELECT dt FROM elsewhere", {"partition_ttl_number": "5"}),
            {"partition_ttl_number": "5"})

    @testing.combinations(("excluded_trigger_tables",), ("excluded_refresh_tables",), argnames="key")
    def test_a_recreated_mv_keeps_only_the_excluded_tables_its_definition_reads(self, caplog, key):
        """StarRocks refuses a table the MV does not read, matching a name without a database by name alone."""
        self.mock_autogen_context.dialect.default_schema_name = "db"
        self.mock_autogen_context.migration_context.opts = {}
        self.mock_autogen_context.connection = None
        properties = self._recreated_properties(
            "SELECT t.dt, d.name FROM t JOIN dim AS d ON t.dt = d.id", {key: "t, gone, db.dim, other.dim, t2"}
        )
        eq_(properties, {key: "t,db.dim"})
        assert (
            f"loses the database's properties {{{key!r}: 'gone,other.dim,t2'}}, which StarRocks refuses for a "
            "materialized view with a definition that does not read the tables they name." in caplog.text
        ), caplog.text

    @testing.skip_if(lambda: not sql_canonical.is_available(), "sqlglot is not installed")
    @testing.combinations(
        ("its_column_kept", "SELECT id, dt FROM t", "dt", None, None),
        (
            "its_column_renamed", "SELECT id, dt AS day FROM t", "day", None,
            "columns that leave out one its condition names.",
        ),
        ("its_column_partitioning_an_expression", "SELECT id, dt + 0 FROM t", "dt", None, None),
        ("its_column_in_the_column_list", "SELECT id, x FROM t", "id", [{"name": "id"}, {"name": "dt"}], None),
        (
            "columns_the_dialect_cannot_read", "SELECT id, dt + 0 FROM t", "id", None,
            "columns the dialect cannot match against its condition.",
        ),
        argnames="model_definition, model_partition_by, model_columns, reason",
        id_="iaaaa",
    )
    def test_a_recreated_mv_keeps_a_retention_condition_only_with_the_columns_it_names(
        self, caplog, model_definition, model_partition_by, model_columns, reason
    ):
        """CREATE refuses a ``partition_retention_condition`` naming a column the MV lacks, after the DROP."""
        upgrade_ops = ops.UpgradeOps([])
        condition = {"partition_retention_condition": "dt >= current_date() - interval 3 month"}
        conn_mv = self._create_reflected_mv(
            "my_mv", definition="SELECT 1", partition_by="dt", refresh="ASYNC", properties=condition
        )
        meta_mv = MaterializedView(
            "my_mv", MetaData(), *(Column(column["name"], INTEGER) for column in model_columns or []),
            definition=model_definition, starrocks_partition_by=model_partition_by,
        )
        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)
        eq_(upgrade_ops.ops[1].kwargs.get("starrocks_properties") or {}, {} if reason else condition)
        if reason:
            assert f"which StarRocks refuses for a materialized view with {reason}" in caplog.text, caplog.text

    def test_an_excluded_table_list_left_with_no_table_is_dropped(self):
        eq_(self._recreated_properties("SELECT dt FROM t", {"excluded_trigger_tables": "t2"}), {})

    def test_modify_mv_partition_by_changed(self, caplog):
        """DROP/CREATE: Partition by changed."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", partition_by="date_trunc('day', dt)")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_partition_by="dt")

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)
        self._assert_recreated(upgrade_ops, caplog, "does not support 'ALTER MATERIALIZED VIEW PARTITION BY'")

    def test_modify_mv_distributed_by_changed(self, caplog):
        """DROP/CREATE: Distributed by changed."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", distributed_by="HASH(id)")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_distributed_by="RANDOM")

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)
        self._assert_recreated(upgrade_ops, caplog, "does not support 'ALTER MATERIALIZED VIEW DISTRIBUTED BY'")

    @testing.combinations(
        ("explicit_bucket_count", "RANDOM BUCKETS 10"),
        ("older_default_hash_on_first_column", "HASH(id)"),
        ("default_random", "RANDOM"),
        argnames="conn_distribution",
        id_="ia",
    )
    def test_no_change_mv_distribution_omitted(self, conn_distribution):
        """No change: metadata that omits DISTRIBUTED BY must not diff or crash.

        StarRocks always auto-assigns a distribution and SHOW CREATE renders it (e.g.
        ``DISTRIBUTED BY RANDOM BUCKETS 10`` or a defaulted ``HASH(...)``). When the user does
        not declare distribution in metadata, the reflected value must be left untouched — not
        compared against the default and 'reset', which raises for this immutable attribute.
        """
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", distributed_by=conn_distribution)
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1")  # no starrocks_distributed_by

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(len(upgrade_ops.ops), 0)

    def test_modify_mv_order_by_changed(self, caplog):
        """DROP/CREATE: Order by changed."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", order_by="id")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_order_by="name")

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)
        self._assert_recreated(upgrade_ops, caplog, "does not support 'ALTER MATERIALIZED VIEW ORDER BY'")

    # ========================================================================
    # No-Change Tests
    # ========================================================================

    def test_no_change_mv(self):
        """No change: Identical MVs."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv(
            name="my_mv",
            definition="SELECT id, name FROM users",
            comment="Test comment",
            partition_by="id",
            distributed_by="HASH(id)",
            order_by="name",
            refresh="ASYNC",
            properties={"replication_num": "1"},
        )
        meta_mv = MaterializedView(
            "my_mv",
            MetaData(),
            definition="SELECT id, name FROM users",
            comment="Test comment",
            starrocks_partition_by="id",
            starrocks_distributed_by="HASH(id)",
            starrocks_order_by="name",
            starrocks_refresh="ASYNC",
            starrocks_properties={"replication_num": "1"},
        )

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(len(upgrade_ops.ops), 0)

    def test_no_change_mv_sql_normalization(self):
        """No change: SQL definition differs only by formatting."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv(name="my_mv", definition="SELECT 1 AS `val`")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="select 1 as val")

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(len(upgrade_ops.ops), 0)

    @testing.combinations(
        # StarRocks 4.1+ reflects a periodic async refresh with the SCHEDULE keyword,
        # while metadata declares it as ASYNC. These must be treated as equivalent.
        ("SCHEDULE EVERY(INTERVAL 10 MINUTE)", "ASYNC EVERY(INTERVAL 10 MINUTE)"),
        # START() quoting differs between SHOW CREATE (double) and metadata (single).
        (
            'SCHEDULE START("2024-01-01 10:00:00") EVERY(INTERVAL 1 HOUR)',
            "ASYNC START('2024-01-01 10:00:00') EVERY(INTERVAL 1 HOUR)",
        ),
        # Case/whitespace only differences.
        ("async every(interval 10 minute)", "ASYNC EVERY(INTERVAL 10 MINUTE)"),
        # SCHEDULE may appear after an IMMEDIATE/DEFERRED moment prefix. StarRocks 4.1 emits
        # "REFRESH DEFERRED SCHEDULE EVERY(...)" for a deferred scheduled MV, which reflection
        # canonicalizes to "DEFERRED ASYNC ..."; metadata using the 4.1 SCHEDULE spelling must
        # still be treated as equivalent.
        ("DEFERRED ASYNC EVERY(INTERVAL 1 HOUR)", "DEFERRED SCHEDULE EVERY(INTERVAL 1 HOUR)"),
        ("IMMEDIATE ASYNC EVERY(INTERVAL 1 HOUR)", "IMMEDIATE SCHEDULE EVERY(INTERVAL 1 HOUR)"),
        # Both sides using the SCHEDULE spelling with a moment prefix.
        ("DEFERRED SCHEDULE EVERY(INTERVAL 30 MINUTE)", "DEFERRED SCHEDULE EVERY(INTERVAL 30 MINUTE)"),
        # IMMEDIATE is the default refresh moment and StarRocks never renders it in SHOW CREATE
        # (only DEFERRED is emitted), so reflection yields no moment. Metadata that explicitly
        # declares IMMEDIATE must still be treated as equivalent to the reflected form.
        ("ASYNC EVERY(INTERVAL 1 HOUR)", "IMMEDIATE ASYNC EVERY(INTERVAL 1 HOUR)"),
        ("ASYNC EVERY(INTERVAL 1 HOUR)", "IMMEDIATE SCHEDULE EVERY(INTERVAL 1 HOUR)"),
        ("ASYNC", "IMMEDIATE ASYNC"),
        # StarRocks keeps a START time in full: midnight for a date alone, and seconds for a time without them.
        ('ASYNC START("2026-10-25 00:00:00") EVERY(INTERVAL 1 DAY)', "ASYNC START('2026-10-25') EVERY (INTERVAL 1 DAY)"),
        ('ASYNC START("2026-10-25 10:30:00") EVERY(INTERVAL 1 HOUR)', "ASYNC START('2026-10-25 10:30') EVERY(INTERVAL 1 HOUR)"),
        argnames="conn_refresh, meta_refresh",
    )
    def test_no_change_mv_refresh_schedule_alias(self, conn_refresh, meta_refresh):
        """No change: 4.1 SCHEDULE refresh keyword is equivalent to metadata ASYNC."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", refresh=conn_refresh)
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_refresh=meta_refresh)

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(len(upgrade_ops.ops), 0)

    def test_another_refresh_start_is_a_change(self):
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv(
            "my_mv", definition="SELECT 1", refresh='ASYNC START("2026-10-25 00:00:00") EVERY(INTERVAL 1 DAY)'
        )
        meta_mv = MaterializedView(
            "my_mv", MetaData(), definition="SELECT 1", starrocks_refresh="ASYNC START('2026-10-26') EVERY(INTERVAL 1 DAY)"
        )

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(len(upgrade_ops.ops), 1)


    @testing.combinations(
        ("DEFERRED ASYNC", "ASYNC"),
        ("DEFERRED ASYNC EVERY(INTERVAL 1 HOUR)", "ASYNC EVERY(INTERVAL 1 HOUR)"),
        ("DEFERRED MANUAL", "MANUAL"),
        ("DEFERRED ASYNC", "DEFERRED ASYNC"),
        ("ASYNC", "ASYNC"),
        argnames="conn_refresh, meta_refresh",
    )
    def test_no_change_when_model_states_no_refresh_moment(self, conn_refresh, meta_refresh):
        """ALTER ... REFRESH ASYNC leaves a DEFERRED view deferred, so a model that states no moment is unchanged."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", refresh=conn_refresh)
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_refresh=meta_refresh)

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(len(upgrade_ops.ops), 0)

    @testing.combinations(
        ("DEFERRED ASYNC", "IMMEDIATE ASYNC", "from DEFERRED to IMMEDIATE"),
        ("ASYNC", "DEFERRED ASYNC", "from IMMEDIATE to DEFERRED"),
        argnames="conn_refresh, meta_refresh, reason",
    )
    def test_refresh_moment_the_model_states_is_recreated(self, caplog, conn_refresh, meta_refresh, reason):
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", refresh=conn_refresh)
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_refresh=meta_refresh)

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        self._assert_recreated(upgrade_ops, caplog, reason)

    def test_refresh_scheme_change_on_a_deferred_view_is_altered(self):
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", refresh="DEFERRED MANUAL")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_refresh="ASYNC")

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_([type(op) for op in upgrade_ops.ops], [AlterMaterializedViewOp])
        eq_(upgrade_ops.ops[0].refresh, "ASYNC")


def _connection_without_temp_views(failing_only_for=None, error=None):
    """A connection on which temporary views fail to be created, as for a user without CREATE VIEW.

    With ``failing_only_for``, only a temporary view whose SQL holds that text fails, as for a definition that reads
    a column the database does not have yet. ``error`` is what StarRocks says instead.
    """
    def execute(statement, *args, **kwargs):
        if statement.startswith("CREATE") and (failing_only_for is None or failing_only_for in statement):
            raise RuntimeError(error or ("Access denied" if failing_only_for is None else "Column cannot be resolved"))
        return Mock(fetchone=Mock(return_value=("SELECT canonical",)))

    return Mock(exec_driver_sql=Mock(side_effect=execute))


class MaterializedViewDefinitionWithoutTempViewTest(fixtures.TestBase):
    """Without the temporary view, a difference only the in-process normalizer sees is a change only sometimes."""

    __requires__ = ("sqlalchemy_2",)

    @staticmethod
    def _compare(server_version, model_definition, revision_creates=frozenset(), failing_only_for=None, error=None):
        database_mv = MaterializedView("mv", MetaData(), definition="SELECT `db`.`t`.`id` FROM `db`.`t`")
        model_mv = MaterializedView("mv", MetaData(), definition=model_definition)
        _compare_mv_definition(
            [], "db", "mv", database_mv, model_mv,
            conn=_connection_without_temp_views(failing_only_for, error), server_version=server_version,
            revision_creates=set(revision_creates), default_schema="db",
        )

    @testing.combinations(((4, 0, 2),), ((4, 1, 0),), ((4, 1, 6),), argnames="server_version")
    def test_from_4_0_2_a_text_difference_is_a_change(self, server_version):
        """StarRocks shows an MV's definition as written from 4.0.2 (StarRocks #64318, backported as #65603)."""
        with expect_raises(UnalterableChange):
            self._compare(server_version, "SELECT id FROM t")

    def test_before_4_0_2_a_definition_reading_a_new_table_is_a_change(self):
        with expect_raises(UnalterableChange):
            self._compare((3, 3, 22), "SELECT id FROM t_new", {(None, "t_new")})

    def test_before_4_0_2_a_definition_only_the_model_cannot_create_a_temporary_view_for_is_a_change(self):
        """The database's definition goes through the temporary view, so the model's reads something new."""
        with expect_raises(UnalterableChange):
            self._compare((3, 3, 22), "SELECT id, added FROM t", failing_only_for="added")

    @testing.combinations(((3, 3, 22),), ((3, 5, 21),), ((4, 0, 1),), (None,), argnames="server_version")
    def test_before_4_0_2_any_other_text_difference_is_warned_about(self, server_version, caplog):
        self._compare(server_version, "SELECT id FROM t", {(None, "t_other")})
        assert "could not canonicalize them through a temporary view" in caplog.text

    @testing.combinations(
        ("access_denied", "Access denied; you need the CREATE VIEW privilege on DATABASE tmp", "Grant the migration user CREATE VIEW"),
        ("unknown_database", "Unknown database 'db'", "Create the schema"),
        ("table_select_denied", "Access denied; you need (at least one of) the SELECT privilege(s) on TABLE t2 for this operation.", None),
        ("a_refused_definition", "Duplicate column name 'id'", None),
        argnames="error, advice",
        id_="iaa",
    )
    def test_the_warning_says_what_starrocks_said_and_advises_only_what_would_cure_it(
        self, caplog, error, advice
    ):
        """Both temporary views failed alike, so StarRocks' error is said once."""
        self._compare((3, 3, 22), "SELECT id FROM t", error=error)
        assert f"StarRocks said: {error}." in caplog.text, caplog.text
        for step in ("Grant the migration user CREATE VIEW", "Create the schema"):
            eq_(step in caplog.text, step == advice)


def _table_named(name):
    return Table(name, MetaData(), Column("id", INTEGER))


class MaterializedViewDefinitionAgainstTheRevisionTest(fixtures.TestBase):
    """Before 4.0.2, without the temporary view, what the revision creates or drops makes a difference certain."""

    __requires__ = ("sqlalchemy_2",)

    @staticmethod
    def _operations_for(revision_operations, database_reads, model_reads):
        """Compare an MV reading ``database_reads`` in the database with one reading ``model_reads`` in the model."""
        context = Mock()
        context.dialect.name = DialectName
        context.dialect.default_schema_name = "db"
        context.dialect.server_version_info = (3, 3, 22)
        context.connection = _connection_without_temp_views()
        context.migration_context.opts = {}
        upgrade_ops = ops.UpgradeOps(list(revision_operations))
        database_mv = MaterializedView(
            "mv", MetaData(), definition=f"SELECT `db`.`{database_reads}`.`id` FROM `db`.`{database_reads}`"
        )
        model_mv = MaterializedView("mv", MetaData(), definition=f"SELECT id FROM {model_reads}")
        _compare_mv(context, upgrade_ops, None, "mv", database_mv, model_mv)
        return [type(op) for op in upgrade_ops.ops[len(revision_operations):]]

    @testing.combinations(
        ("a_table", lambda: ops.CreateTableOp.from_table(_table_named("x"))),
        ("a_view", lambda: CreateViewOp("x", "SELECT id FROM t")),
        ("an_mv", lambda: CreateMaterializedViewOp("x", "SELECT id FROM t")),
        argnames="created",
        id_="ia",
    )
    def test_a_definition_reading_what_the_revision_creates_is_recreated(self, created):
        """The temporary view failed because the object does not exist yet, so the definition did change."""
        eq_(self._operations_for([created()], "t", "x"), [DropMaterializedViewOp, CreateMaterializedViewOp])

    @testing.combinations(
        ("a_table", lambda: ops.DropTableOp.from_table(_table_named("x"))),
        ("a_view", lambda: DropViewOp("x", existing_definition="SELECT id FROM t")),
        ("an_mv", lambda: DropMaterializedViewOp("x", existing_definition="SELECT id FROM t")),
        argnames="dropped",
        id_="ia",
    )
    def test_a_database_definition_reading_what_the_revision_drops_is_recreated(self, dropped):
        """The model cannot read what it removes, so the definition did change; StarRocks would leave the MV inactive."""
        eq_(self._operations_for([dropped()], "x", "t"), [DropMaterializedViewOp, CreateMaterializedViewOp])

    @staticmethod
    def _dropping(table, column):
        return ops.ModifyTableOps(table, [ops.DropColumnOp.from_column_and_tablename(None, table, Column(column))])

    def test_a_database_definition_reading_a_column_the_revision_drops_is_recreated(self):
        """StarRocks would leave the MV inactive once the column is gone, and could not refresh it again."""
        eq_(
            self._operations_for([self._dropping("x", "id")], "x", "t"),
            [DropMaterializedViewOp, CreateMaterializedViewOp],
        )

    def test_a_column_the_database_definition_does_not_read_makes_no_change_certain(self, caplog):
        eq_(self._operations_for([self._dropping("x", "other")], "x", "t"), [])
        assert "could not canonicalize them through a temporary view" in caplog.text

    def test_a_definition_reading_what_the_revision_recreates_is_not_certain(self, caplog):
        recreated = [
            DropMaterializedViewOp("x", existing_definition="SELECT id FROM t"),
            CreateMaterializedViewOp("x", "SELECT id, 1 AS one FROM t"),
        ]
        eq_(self._operations_for(recreated, "x", "t"), [])
        assert "could not canonicalize them through a temporary view" in caplog.text

    def test_a_difference_unrelated_to_the_revision_is_warned_about(self, caplog):
        eq_(self._operations_for([ops.DropTableOp.from_table(_table_named("y"))], "x", "t"), [])
        assert "could not canonicalize them through a temporary view" in caplog.text
