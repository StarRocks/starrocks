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

import logging

from alembic.autogenerate import api
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy.testing import config, fixtures
from sqlalchemy import Column, MetaData, Table, testing, text

from starrocks.alembic.compare import combine_include_object
from starrocks.alembic.ops import (
    AlterMaterializedViewOp,
    CreateMaterializedViewOp,
    DropMaterializedViewOp,
)
from starrocks.common.types import SystemRunMode
from starrocks.datatype import INTEGER
from starrocks.sql.schema import MaterializedView
from test.util import (
    MV_PARTITION_RETENTION_NEEDS_3_5,
    MV_TEXT_REWRITTEN_BEFORE_4_0_2,
    MV_TEXT_STORED_AS_WRITTEN_FROM_4_0_2,
)


logger = logging.getLogger(__name__)


def create_migration_context(conn, target_metadata: MetaData, **opts) -> MigrationContext:
    """Create a MigrationContext with standard view/MV configuration, and any further ``opts``."""
    def _exclude_tables(object, name, type_, reflected, compare_to):
        # Exclude tables from autogenerate in MV tests to avoid unrelated DropTableOps
        if type_ == "table":
            return False
        return True
    return MigrationContext.configure(
        connection=conn,
        opts={
            "target_metadata": target_metadata,
            "include_object": combine_include_object(_exclude_tables),
            **opts,
        },
    )


class _MaterializedViewAutogenerateBase(fixtures.TestBase):
    """Creates the ``t_autogen`` table the materialized views select from.

    Gated on sqlalchemy_2: MaterializedView and the dialect's Alembic integration need it.
    """

    __backend__ = True
    __only_on__ = "starrocks"
    __requires__ = ("sqlalchemy_2",)

    @classmethod
    def setup_test_class(cls):
        with config.db.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS t_autogen"))
            conn.execute(text(
                "CREATE TABLE t_autogen (val INT) "
                "DISTRIBUTED BY HASH(val) "
                "PROPERTIES ('replication_num' = '1')"
            ))

    @classmethod
    def teardown_test_class(cls):
        with config.db.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS t_autogen"))


class CreateMaterializedViewTest(_MaterializedViewAutogenerateBase):
    """CREATE MATERIALIZED VIEW related tests."""

    def test_basic_mv_creation(self, connection) -> None:
        """CREATE: Basic MV creation."""
        mv_name = "test_create_mv_basic"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name,
                target_metadata,
                definition="SELECT val FROM t_autogen",
                starrocks_refresh="MANUAL",
            )
            mc = create_migration_context(connection, target_metadata)
            logger.debug("start to produce migrations")
            migration_script = api.produce_migrations(mc, target_metadata)
            logger.debug("migration_script: %s", migration_script)

            assert len(migration_script.upgrade_ops.ops) == 1
            create_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(create_op, CreateMaterializedViewOp)
            assert create_op.view_name == mv_name
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_comprehensive_mv_creation(self, connection) -> None:
        """CREATE: Comprehensive MV with all attributes."""
        mv_name = "test_create_mv_comprehensive"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name,
                target_metadata,
                definition="SELECT val FROM t_autogen",
                comment="Test Comment",
                starrocks_partition_by="val",
                starrocks_distributed_by="HASH(val)",
                starrocks_order_by="val",
                starrocks_refresh="ASYNC",
                starrocks_properties={"replication_num": "1"},
            )
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            assert len(migration_script.upgrade_ops.ops) == 1
            op = migration_script.upgrade_ops.ops[0]
            assert isinstance(op, CreateMaterializedViewOp)
            assert op.comment == "Test Comment"
            assert op.kwargs["starrocks_partition_by"] == "val"
            assert op.kwargs["starrocks_distributed_by"] == "HASH(val)"
            assert op.kwargs["starrocks_order_by"] == "val"
            assert op.kwargs["starrocks_refresh"] == "ASYNC"
            assert op.kwargs["starrocks_properties"] == {"replication_num": "1"}
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))


class DropMaterializedViewTest(_MaterializedViewAutogenerateBase):
    """DROP MATERIALIZED VIEW related tests."""

    def test_basic_mv_drop(self, connection) -> None:
        """DROP: Basic MV deletion."""
        mv_name = "test_drop_mv_basic"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(f"CREATE MATERIALIZED VIEW {mv_name} REFRESH MANUAL AS SELECT val FROM t_autogen"))
        try:
            target_metadata = MetaData()  # Empty metadata
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            assert len(migration_script.upgrade_ops.ops) == 1
            drop_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(drop_op, DropMaterializedViewOp)
            assert drop_op.view_name == mv_name
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))


class AlterMaterializedViewTest(_MaterializedViewAutogenerateBase):
    """ALTER MATERIALIZED VIEW related tests."""

    def test_alter_mutable_attributes(self, connection) -> None:
        """ALTER: Change mutable attributes (refresh, properties)."""
        mv_name = "test_alter_mv_mutable"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(f"CREATE MATERIALIZED VIEW {mv_name} REFRESH ASYNC AS SELECT val FROM t_autogen"))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name,
                target_metadata,
                definition="SELECT val FROM t_autogen",
                starrocks_refresh="MANUAL",
                starrocks_properties={"mv_rewrite_staleness_second": "60"},
            )
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            assert len(migration_script.upgrade_ops.ops) == 2
            alter_op_refresh = migration_script.upgrade_ops.ops[0]
            alter_op_properties = migration_script.upgrade_ops.ops[1]
            assert isinstance(alter_op_refresh, AlterMaterializedViewOp)
            assert isinstance(alter_op_properties, AlterMaterializedViewOp)
            assert alter_op_refresh.refresh == "MANUAL"
            assert alter_op_properties.properties == {"mv_rewrite_staleness_second": "60"}
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_the_generated_property_changes_apply(self, connection, caplog) -> None:
        """A property ALTER MATERIALIZED VIEW cannot change is warned about and skipped; the others are set."""
        mv_name = "test_alter_mv_refused_properties"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} REFRESH MANUAL PROPERTIES('replication_num' = '1') "
            "AS SELECT val FROM t_autogen"
        ))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name,
                target_metadata,
                definition="SELECT val FROM t_autogen",
                starrocks_refresh="MANUAL",
                starrocks_properties={"replication_num": "3", "storage_medium": "SSD", "mv_rewrite_staleness_second": "60"},
            )
            script = api.produce_migrations(create_migration_context(connection, target_metadata), target_metadata)
            operations = Operations(MigrationContext.configure(connection))
            for op in script.upgrade_ops.ops:
                operations.invoke(op)
            ddl = connection.exec_driver_sql(f"SHOW CREATE MATERIALIZED VIEW {mv_name}").fetchone()[1]
            assert '"mv_rewrite_staleness_second" = "60"' in ddl, ddl
            assert "would set property 'replication_num' to '3'" in caplog.text
            assert "would set property 'storage_medium' to 'SSD'" in caplog.text
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    @testing.combinations(
        ("session_variable", "session.query_timeout", "500", "1000"),
        ("rewrite_consistency", "query_rewrite_consistency", "loose", "checked"),
        argnames="key,in_database,in_model",
        id_="iaaa",
    )
    def test_a_property_tables_config_leaves_out_is_compared(self, connection, key, in_database, in_model) -> None:
        mv_name = "test_alter_mv_ddl_only_property"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} REFRESH MANUAL PROPERTIES('{key}' = '{in_database}') "
            "AS SELECT val FROM t_autogen"
        ))
        try:
            def migration_for(value):
                target_metadata = MetaData()
                MaterializedView(
                    mv_name, target_metadata, definition="SELECT val FROM t_autogen", starrocks_refresh="MANUAL",
                    starrocks_properties={key: value},
                )
                return api.produce_migrations(create_migration_context(connection, target_metadata), target_metadata)

            assert migration_for(in_database).upgrade_ops.ops == []
            changed = migration_for(in_model)
            assert [op.properties for op in changed.upgrade_ops.ops] == [{key: in_model}]
            restored = [{name: value.lower() for name, value in op.properties.items()} for op in changed.downgrade_ops.ops]
            assert restored == [{key: in_database}]
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    @testing.combinations(
        ("with_a_distribution", {"starrocks_distributed_by": "HASH(val)"}),
        ("with_nothing_else", {}),
        argnames="model_options",
        id_="ia",
    )
    def test_a_recreated_mv_keeps_what_the_model_leaves_out(self, connection, model_options) -> None:
        """A definition change recreates the MV; the database's refresh and non-default properties carry over."""
        mv_name = "test_recreate_keeps_refresh"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} REFRESH ASYNC EVERY (INTERVAL 1 DAY) "
            "PROPERTIES('session.query_timeout' = '900', 'mv_rewrite_staleness_second' = '60') "
            "AS SELECT val FROM t_autogen"
        ))
        try:
            target_metadata = MetaData()
            MaterializedView(mv_name, target_metadata, definition="SELECT val + 1 AS val FROM t_autogen", **model_options)
            script = api.produce_migrations(create_migration_context(connection, target_metadata), target_metadata)
            operations = Operations(MigrationContext.configure(connection))
            for op in script.upgrade_ops.ops:
                operations.invoke(op)
            ddl = connection.exec_driver_sql(f"SHOW CREATE MATERIALIZED VIEW {mv_name}").fetchone()[1]
            assert "INTERVAL 1 DAY" in ddl and '"session.query_timeout" = "900"' in ddl, ddl
            assert '"mv_rewrite_staleness_second" = "60"' in ddl, ddl
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    @testing.combinations(
        ("now_manual", {"starrocks_partition_by": "date_trunc('month', dt)", "starrocks_refresh": "DEFERRED MANUAL"}),
        ("now_unpartitioned", {"starrocks_refresh": "ASYNC", "comment": "unpartitioned now"}),
        argnames="model_options",
        id_="ia",
    )
    def test_a_recreated_mv_leaves_out_the_properties_create_refuses(self, connection, model_options) -> None:
        """CREATE refuses ``auto_refresh_partitions_limit`` for a MANUAL or unpartitioned MV, and
        ``excluded_trigger_tables`` for a MANUAL one, and by then the migration has dropped the old MV."""
        base_table, mv_name = "t_autogen_by_month", "test_recreate_refused_properties"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(f"DROP TABLE IF EXISTS {base_table}"))
        connection.execute(text(
            f"CREATE TABLE {base_table} (id INT, dt DATE NOT NULL) PARTITION BY date_trunc('month', dt) "
            "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')"
        ))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} PARTITION BY date_trunc('month', dt) DISTRIBUTED BY HASH(id) BUCKETS 1 "
            "REFRESH ASYNC PROPERTIES ('replication_num' = '1', 'auto_refresh_partitions_limit' = '3', "
            f"'excluded_trigger_tables' = '{base_table}') AS SELECT id, dt FROM {base_table}"
        ))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name, target_metadata, definition=f"SELECT id, dt FROM {base_table}",
                starrocks_distributed_by="HASH(id) BUCKETS 1", starrocks_properties={"replication_num": "1"},
                **model_options,
            )
            script = api.produce_migrations(create_migration_context(connection, target_metadata), target_metadata)
            assert [type(op) for op in script.upgrade_ops.ops] == [DropMaterializedViewOp, CreateMaterializedViewOp]
            operations = Operations(MigrationContext.configure(connection))
            for op in script.upgrade_ops.ops:
                operations.invoke(op)
            ddl = connection.exec_driver_sql(f"SHOW CREATE MATERIALIZED VIEW {mv_name}").fetchone()[1]
            assert "auto_refresh_partitions_limit" not in ddl, ddl
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
            connection.execute(text(f"DROP TABLE IF EXISTS {base_table}"))

    def _recreate(self, connection, mv_name, database_mv, model_mv, tables):
        """Create ``tables`` and ``database_mv``, then apply autogenerate's recreate of it as ``model_mv``.

        Returns the new MV's SHOW CREATE.
        """
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        for name, ddl in tables.items():
            connection.execute(text(f"DROP TABLE IF EXISTS {name}"))
            connection.execute(text(ddl))
        connection.execute(text(database_mv))
        target_metadata = MetaData()
        model_mv(target_metadata)
        script = api.produce_migrations(create_migration_context(connection, target_metadata), target_metadata)
        assert [type(op) for op in script.upgrade_ops.ops] == [DropMaterializedViewOp, CreateMaterializedViewOp]
        operations = Operations(MigrationContext.configure(connection))
        for op in script.upgrade_ops.ops:
            operations.invoke(op)
        return connection.exec_driver_sql(f"SHOW CREATE MATERIALIZED VIEW {mv_name}").fetchone()[1]

    def _drop(self, connection, mv_name, tables):
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        for name in tables:
            connection.execute(text(f"DROP TABLE IF EXISTS {name}"))

    def test_a_recreated_mv_keeps_only_the_excluded_tables_its_definition_reads(self, connection) -> None:
        """CREATE refuses a table in ``excluded_trigger_tables`` that the MV does not read, after the DROP."""
        mv_name = "test_recreate_excluded_tables"
        tables = {
            name: f"CREATE TABLE {name} (id INT, a INT) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                  "PROPERTIES ('replication_num' = '1')"
            for name in ("t_autogen_fact", "t_autogen_dim")
        }
        try:
            ddl = self._recreate(
                connection, mv_name,
                f"CREATE MATERIALIZED VIEW {mv_name} DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH ASYNC "
                "PROPERTIES ('replication_num' = '1', 'excluded_trigger_tables' = 't_autogen_dim') "
                "AS SELECT f.id, d.a FROM t_autogen_fact AS f JOIN t_autogen_dim AS d ON f.id = d.id",
                lambda metadata: MaterializedView(
                    mv_name, metadata, definition="SELECT id, a FROM t_autogen_fact",
                    starrocks_distributed_by="HASH(id) BUCKETS 1", starrocks_properties={"replication_num": "1"},
                ),
                tables,
            )
            assert "excluded_trigger_tables" not in ddl, ddl
        finally:
            self._drop(connection, mv_name, tables)

    def test_a_recreated_mv_over_list_partitions_keeps_partition_ttl_number_where_starrocks_takes_it(
        self, connection
    ) -> None:
        """From 3.4.1 CREATE refuses ``partition_ttl_number`` for an MV over a LIST-partitioned table, after the DROP;
        3.3 takes it."""
        mv_name = "test_recreate_list_ttl_number"
        tables = {
            "t_autogen_by_day": "CREATE TABLE t_autogen_by_day (id INT, dt DATE NOT NULL) "
                                "PARTITION BY date_trunc('day', dt) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                                "PROPERTIES ('replication_num' = '1')",
            "t_autogen_by_city": "CREATE TABLE t_autogen_by_city (id INT, city VARCHAR(20) NOT NULL) "
                                 "PARTITION BY (city) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                                 "PROPERTIES ('replication_num' = '1')",
        }
        try:
            ddl = self._recreate(
                connection, mv_name,
                f"CREATE MATERIALIZED VIEW {mv_name} PARTITION BY dt DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH ASYNC "
                "PROPERTIES ('replication_num' = '1', 'partition_ttl_number' = '5', 'partition_ttl' = '2 MONTH') "
                "AS SELECT id, dt FROM t_autogen_by_day",
                lambda metadata: MaterializedView(
                    mv_name, metadata, definition="SELECT id, city FROM t_autogen_by_city",
                    starrocks_partition_by="city", starrocks_distributed_by="HASH(id) BUCKETS 1",
                    starrocks_properties={"replication_num": "1"},
                ),
                tables,
            )
            assert ('"partition_ttl_number" = "5"' in ddl) == (config.db.dialect.server_version_info < (3, 4, 1)), ddl
            assert '"partition_ttl" = "2 MONTH"' in ddl, ddl
        finally:
            self._drop(connection, mv_name, tables)

    @MV_PARTITION_RETENTION_NEEDS_3_5
    def test_a_recreated_mv_leaves_out_a_retention_condition_on_a_column_it_no_longer_has(self, connection):
        """CREATE refuses the condition after the DROP once it names a column the new MV lacks ("Column 'dt' cannot
        be resolved"). Without reading the condition back, a recreate loses it anyway."""
        mv_name = "test_recreate_retention_condition"
        tables = {
            "t_autogen_by_day_ret": "CREATE TABLE t_autogen_by_day_ret (id INT, dt DATE NOT NULL) "
                                    "PARTITION BY date_trunc('day', dt) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                                    "PROPERTIES ('replication_num' = '1')",
        }
        try:
            ddl = self._recreate(
                connection, mv_name,
                f"CREATE MATERIALIZED VIEW {mv_name} PARTITION BY dt DISTRIBUTED BY HASH(id) BUCKETS 1 REFRESH ASYNC "
                "PROPERTIES ('replication_num' = '1', 'partition_retention_condition' = \"dt >= '2020-01-01'\") "
                "AS SELECT id, dt FROM t_autogen_by_day_ret",
                lambda metadata: MaterializedView(
                    mv_name, metadata, definition="SELECT id, dt AS day FROM t_autogen_by_day_ret",
                    starrocks_partition_by="day", starrocks_distributed_by="HASH(id) BUCKETS 1",
                    starrocks_properties={"replication_num": "1"},
                ),
                tables,
            )
            assert "partition_retention_condition" not in ddl, ddl
        finally:
            self._drop(connection, mv_name, tables)

    _NOT_COMPARED_MV = "test_mv_not_compared"
    _COMPARED_DEFINITION = "SELECT val, count(*) AS n FROM t_autogen GROUP BY val"

    def _ops_without_the_temp_view(self, connection, model_definition):
        """Autogenerate against an MV with a temp-view schema that does not exist, as for a user without CREATE VIEW."""
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {self._NOT_COMPARED_MV}"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {self._NOT_COMPARED_MV} REFRESH MANUAL AS {self._COMPARED_DEFINITION}"
        ))
        target_metadata = MetaData()
        MaterializedView(self._NOT_COMPARED_MV, target_metadata, definition=model_definition, starrocks_refresh="MANUAL")
        context = create_migration_context(
            connection, target_metadata, starrocks_temp_view_schema="no_such_schema_for_temp_views"
        )
        return [type(op) for op in api.produce_migrations(context, target_metadata).upgrade_ops.ops]

    def test_an_unchanged_definition_starrocks_cannot_compare_is_not_recreated(self, connection) -> None:
        """Before 4.0.2 the stored definition has table-qualified columns, which the normalizer reads as a change."""
        try:
            assert self._ops_without_the_temp_view(connection, self._COMPARED_DEFINITION) == []
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {self._NOT_COMPARED_MV}"))

    @MV_TEXT_STORED_AS_WRITTEN_FROM_4_0_2
    def test_a_changed_definition_starrocks_cannot_compare_is_warned_about(self, connection, caplog):
        """Before 4.0.2 a difference only the normalizer sees is not trusted, unless the model reads a new object."""
        try:
            ops = self._ops_without_the_temp_view(connection, "SELECT val, sum(val) AS n FROM t_autogen GROUP BY val")
            assert ops == []
            assert "could not canonicalize them through a temporary view" in caplog.text
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {self._NOT_COMPARED_MV}"))

    @MV_TEXT_REWRITTEN_BEFORE_4_0_2
    def test_a_changed_definition_is_recreated_from_the_text_from_4_0_2(self, connection):
        """From 4.0.2 StarRocks stores the definition as written, so the normalizer's comparison is trusted."""
        try:
            ops = self._ops_without_the_temp_view(connection, "SELECT val, sum(val) AS n FROM t_autogen GROUP BY val")
            assert ops == [DropMaterializedViewOp, CreateMaterializedViewOp]
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {self._NOT_COMPARED_MV}"))

    _JOINED = "SELECT mv_dup_a.val, mv_dup_b.val FROM mv_dup_a JOIN mv_dup_b ON mv_dup_a.val = mv_dup_b.val"

    def _ops_for_a_column_list_naming_repeated_names(self, connection, model_definition):
        """Autogenerate an MV whose SELECT list names ``val`` twice, which only its column list names apart."""
        for name in ("mv_dup_a", "mv_dup_b"):
            connection.execute(text(f"CREATE TABLE IF NOT EXISTS {name} (val INT) PROPERTIES ('replication_num'='1')"))
        connection.execute(text("DROP MATERIALIZED VIEW IF EXISTS mv_dup"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW mv_dup (val1, val2) DISTRIBUTED BY HASH(val1) REFRESH MANUAL AS {self._JOINED}"
        ))
        try:
            target_metadata = MetaData()
            MaterializedView(
                "mv_dup", target_metadata, definition=model_definition, columns=[{"name": "val1"}, {"name": "val2"}],
                starrocks_distributed_by="HASH(val1)", starrocks_refresh="MANUAL",
            )
            script = api.produce_migrations(create_migration_context(connection, target_metadata), target_metadata)
            return [type(op) for op in script.upgrade_ops.ops]
        finally:
            connection.execute(text("DROP MATERIALIZED VIEW IF EXISTS mv_dup"))
            for name in ("mv_dup_a", "mv_dup_b"):
                connection.execute(text(f"DROP TABLE IF EXISTS {name}"))

    def test_an_unchanged_mv_whose_column_list_names_repeated_names_apart_is_compared(self, connection, caplog):
        assert self._ops_for_a_column_list_naming_repeated_names(connection, self._JOINED) == []
        assert "could not canonicalize" not in caplog.text

    def test_a_changed_mv_whose_column_list_names_repeated_names_apart_is_recreated(self, connection):
        """Before 4.0.2 only the temporary views tell the change, and only under the column lists."""
        changed = f"{self._JOINED} WHERE mv_dup_a.val > 5"
        assert self._ops_for_a_column_list_naming_repeated_names(connection, changed) == [
            DropMaterializedViewOp, CreateMaterializedViewOp
        ]

    def test_a_definition_reading_a_table_the_revision_creates_is_recreated(self, connection, caplog) -> None:
        """The model's definition reads a table the same revision creates, so the temporary view fails, on any version."""
        mv_name, new_table = "test_mv_reads_new_table", "t_autogen_created_with_the_mv"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(f"DROP TABLE IF EXISTS {new_table}"))
        connection.execute(text(f"CREATE MATERIALIZED VIEW {mv_name} REFRESH MANUAL AS SELECT val FROM t_autogen"))
        try:
            target_metadata = MetaData()
            Table(
                new_table, target_metadata, Column("val", INTEGER),
                starrocks_distributed_by="HASH(val)", starrocks_properties={"replication_num": "1"},
            )
            MaterializedView(
                mv_name, target_metadata, definition=f"SELECT val FROM {new_table}", starrocks_refresh="MANUAL"
            )
            context = create_migration_context(
                connection,
                target_metadata,
                include_object=combine_include_object(
                    lambda object, name, type_, reflected, compare_to: type_ != "table" or name == new_table
                ),
            )
            script = api.produce_migrations(context, target_metadata)
            assert [type(op).__name__ for op in script.upgrade_ops.ops] == [
                "DropMaterializedViewOp", "CreateTableOp", "CreateMaterializedViewOp"
            ]
            operations = Operations(MigrationContext.configure(connection))
            for op in script.upgrade_ops.ops:
                operations.invoke(op)
            ddl = connection.exec_driver_sql(f"SHOW CREATE MATERIALIZED VIEW {mv_name}").fetchone()[1]
            assert new_table in ddl, ddl
            assert "could not canonicalize them through a temporary view" not in caplog.text
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
            connection.execute(text(f"DROP TABLE IF EXISTS {new_table}"))

    def test_a_definition_reading_a_table_the_revision_drops_is_recreated(self, connection) -> None:
        """The model cannot read a table it removes, so the definition did change, though no temporary view compares
        them; StarRocks would otherwise leave the MV inactive once the table is gone."""
        mv_name, dropped_table = "test_mv_reads_dropped_table", "t_autogen_dropped_with_the_mv"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(f"DROP TABLE IF EXISTS {dropped_table}"))
        connection.execute(text(
            f"CREATE TABLE {dropped_table} (val INT) DISTRIBUTED BY HASH(val) PROPERTIES ('replication_num' = '1')"
        ))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} REFRESH MANUAL AS "
            f"SELECT t_autogen.val FROM t_autogen JOIN {dropped_table} ON t_autogen.val = {dropped_table}.val"
        ))
        try:
            target_metadata = MetaData()
            MaterializedView(mv_name, target_metadata, definition="SELECT val FROM t_autogen", starrocks_refresh="MANUAL")
            context = create_migration_context(
                connection,
                target_metadata,
                starrocks_temp_view_schema="no_such_schema_for_temp_views",
                include_object=combine_include_object(
                    lambda object, name, type_, reflected, compare_to: type_ != "table" or name == dropped_table
                ),
            )
            script = api.produce_migrations(context, target_metadata)
            assert [type(op).__name__ for op in script.upgrade_ops.ops] == [
                "DropMaterializedViewOp", "DropTableOp", "CreateMaterializedViewOp"
            ]
            operations = Operations(MigrationContext.configure(connection))
            for op in script.upgrade_ops.ops:
                operations.invoke(op)
            active = connection.exec_driver_sql(
                "SELECT IS_ACTIVE FROM information_schema.materialized_views "
                f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{mv_name}'"
            ).scalar()
            assert str(active).lower() == "true", active
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
            connection.execute(text(f"DROP TABLE IF EXISTS {dropped_table}"))

    def test_a_definition_reading_a_column_the_revision_drops_is_recreated(self, connection) -> None:
        """The model cannot read a column it removes, so the definition did change, though no temporary view compares
        them; StarRocks would otherwise leave the MV inactive, and could not refresh it once the column is gone."""
        mv_name, table = "test_mv_reads_dropped_column", "t_autogen_losing_a_column"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
        connection.execute(text(
            f"CREATE TABLE {table} (id INT, a INT, b INT) DISTRIBUTED BY HASH(id) PROPERTIES ('replication_num' = '1')"
        ))
        connection.execute(text(f"CREATE MATERIALIZED VIEW {mv_name} REFRESH MANUAL AS SELECT id, a FROM {table}"))
        try:
            target_metadata = MetaData()
            Table(
                table, target_metadata, Column("id", INTEGER), Column("b", INTEGER),
                starrocks_distributed_by="HASH(id)", starrocks_properties={"replication_num": "1"},
            )
            MaterializedView(
                mv_name, target_metadata, definition=f"SELECT id, b FROM {table}", starrocks_refresh="MANUAL"
            )
            context = create_migration_context(
                connection,
                target_metadata,
                starrocks_temp_view_schema="no_such_schema_for_temp_views",
                include_object=combine_include_object(
                    lambda object, name, type_, reflected, compare_to: type_ != "table" or name == table
                ),
            )
            script = api.produce_migrations(context, target_metadata)
            assert [type(op).__name__ for op in script.upgrade_ops.ops] == [
                "DropMaterializedViewOp", "ModifyTableOps", "CreateMaterializedViewOp"
            ]
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
            connection.execute(text(f"DROP TABLE IF EXISTS {table}"))

    def test_a_change_of_case_inside_a_string_literal_is_recreated(self, connection) -> None:
        """StarRocks compares strings by case, so an MV selecting 'Active' and one selecting 'ACTIVE' differ."""
        mv_name = "test_mv_literal_case"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} REFRESH MANUAL AS SELECT val, 'Active' AS status FROM t_autogen"
        ))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name, target_metadata, definition="SELECT val, 'ACTIVE' AS status FROM t_autogen",
                starrocks_refresh="MANUAL",
            )
            script = api.produce_migrations(create_migration_context(connection, target_metadata), target_metadata)
            assert [type(op) for op in script.upgrade_ops.ops] == [DropMaterializedViewOp, CreateMaterializedViewOp]
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_alter_immutable_attributes(self, connection) -> None:
        """ALTER: Change immutable attributes (definition) triggers DROP/CREATE."""
        mv_name = "test_alter_mv_immutable"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(f"CREATE MATERIALIZED VIEW {mv_name} REFRESH MANUAL AS SELECT val FROM t_autogen"))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name, target_metadata, definition="SELECT val + 1 AS val FROM t_autogen", starrocks_refresh="MANUAL"
            )
            mc = create_migration_context(connection, target_metadata)
            script = api.produce_migrations(mc, target_metadata)

            upgrade = [(type(op), op.view_name) for op in script.upgrade_ops.ops]
            assert upgrade == [(DropMaterializedViewOp, mv_name), (CreateMaterializedViewOp, mv_name)]
            assert "val + 1" in script.upgrade_ops.ops[1].definition
            downgrade = script.downgrade_ops.ops
            assert [(type(op), op.view_name) for op in downgrade] == [
                (DropMaterializedViewOp, mv_name),
                (CreateMaterializedViewOp, mv_name),
            ]
            assert "val + 1" not in downgrade[1].definition
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    @staticmethod
    def _refresh_alter_ops(migration_script):
        """Return the AlterMaterializedViewOps that carry a REFRESH change."""
        return [
            op
            for op in migration_script.upgrade_ops.ops
            if isinstance(op, AlterMaterializedViewOp) and op.refresh is not None
        ]

    def test_no_refresh_diff_for_periodic_async(self, connection) -> None:
        """No change: a periodic ASYNC refresh must not produce a spurious REFRESH diff.

        Regression test for the repeated refresh-interval diff. StarRocks 4.1+ renders
        ``REFRESH ASYNC EVERY(...)`` back as ``REFRESH SCHEDULE EVERY(...)`` in SHOW CREATE,
        which must be treated as equivalent to the ``ASYNC EVERY(...)`` metadata declaration.
        (On 3.5.x the reflected form is already ``ASYNC EVERY(...)``, so this also asserts no
        regression there.)
        """
        mv_name = "test_mv_refresh_periodic_async"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} "
            "REFRESH ASYNC EVERY(INTERVAL 10 MINUTE) "
            "PROPERTIES ('replication_num' = '1') "
            "AS SELECT val FROM t_autogen"
        ))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name,
                target_metadata,
                definition="SELECT val FROM t_autogen",
                starrocks_refresh="ASYNC EVERY(INTERVAL 10 MINUTE)",
                starrocks_properties={"replication_num": "1"},
            )
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            refresh_ops = self._refresh_alter_ops(migration_script)
            assert refresh_ops == [], (
                f"Unexpected REFRESH diff for equivalent periodic async refresh: "
                f"{[(op.refresh, op.existing_refresh) for op in refresh_ops]}"
            )
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_no_refresh_diff_for_deferred_periodic_async(self, connection) -> None:
        """No change: a DEFERRED periodic async refresh must not produce a spurious REFRESH diff.

        StarRocks 4.1+ emits ``REFRESH DEFERRED SCHEDULE EVERY(...)`` in SHOW CREATE for a
        deferred scheduled MV (the moment prefix is only rendered for DEFERRED). Reflection
        canonicalizes this to ``DEFERRED ASYNC EVERY(...)``, and metadata written with the 4.1
        SCHEDULE spelling must still be treated as equivalent — the SCHEDULE keyword can follow
        an IMMEDIATE/DEFERRED prefix, not only appear at the start.
        """
        mv_name = "test_mv_refresh_deferred_periodic"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} "
            "REFRESH DEFERRED ASYNC EVERY(INTERVAL 1 HOUR) "
            "PROPERTIES ('replication_num' = '1') "
            "AS SELECT val FROM t_autogen"
        ))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name,
                target_metadata,
                definition="SELECT val FROM t_autogen",
                # Metadata uses the 4.1 SHOW CREATE spelling with a moment prefix.
                starrocks_refresh="DEFERRED SCHEDULE EVERY(INTERVAL 1 HOUR)",
                starrocks_properties={"replication_num": "1"},
            )
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            refresh_ops = self._refresh_alter_ops(migration_script)
            assert refresh_ops == [], (
                f"Unexpected REFRESH diff for equivalent deferred periodic async refresh: "
                f"{[(op.refresh, op.existing_refresh) for op in refresh_ops]}"
            )
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_no_refresh_diff_for_explicit_immediate_moment(self, connection) -> None:
        """No change: metadata that spells out the default IMMEDIATE moment must not diff.

        IMMEDIATE is the default refresh moment and StarRocks never renders it in SHOW CREATE
        (only DEFERRED is emitted), so an MV created with ``REFRESH IMMEDIATE ASYNC EVERY(...)``
        reflects back as ``ASYNC EVERY(...)``. Metadata that explicitly declares IMMEDIATE must
        still be treated as equivalent to the reflected (moment-less) form.
        """
        mv_name = "test_mv_refresh_immediate_moment"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} "
            "REFRESH IMMEDIATE ASYNC EVERY(INTERVAL 1 HOUR) "
            "PROPERTIES ('replication_num' = '1') "
            "AS SELECT val FROM t_autogen"
        ))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name,
                target_metadata,
                definition="SELECT val FROM t_autogen",
                # Metadata explicitly declares the default IMMEDIATE moment.
                starrocks_refresh="IMMEDIATE ASYNC EVERY(INTERVAL 1 HOUR)",
                starrocks_properties={"replication_num": "1"},
            )
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            refresh_ops = self._refresh_alter_ops(migration_script)
            assert refresh_ops == [], (
                f"Unexpected REFRESH diff for explicit IMMEDIATE moment: "
                f"{[(op.refresh, op.existing_refresh) for op in refresh_ops]}"
            )
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_deferred_view_converges_after_altering_its_refresh(self, connection) -> None:
        """A DEFERRED MANUAL view altered to REFRESH ASYNC reflects as DEFERRED ASYNC, which a model saying ASYNC matches."""
        mv_name = "test_mv_deferred_after_alter"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} REFRESH DEFERRED MANUAL "
            "PROPERTIES ('replication_num' = '1') AS SELECT val FROM t_autogen"
        ))
        connection.execute(text(f"ALTER MATERIALIZED VIEW {mv_name} REFRESH ASYNC"))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name,
                target_metadata,
                definition="SELECT val FROM t_autogen",
                starrocks_refresh="ASYNC",
                starrocks_properties={"replication_num": "1"},
            )
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            assert migration_script.upgrade_ops.ops == []
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_no_diff_when_distribution_omitted(self, connection) -> None:
        """No change/crash: omitting DISTRIBUTED BY in metadata must not diff or raise.

        StarRocks always auto-assigns a distribution and SHOW CREATE renders it with an explicit
        bucket count (e.g. ``DISTRIBUTED BY RANDOM BUCKETS 10``). Since distribution is immutable
        and the user did not declare it, autogenerate must leave it untouched rather than try to
        reset it to the default (which raised NotImplementedError).
        """
        mv_name = "test_mv_distribution_omitted"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} "
            "REFRESH ASYNC "
            "DISTRIBUTED BY RANDOM BUCKETS 10 "
            "PROPERTIES ('replication_num' = '1') "
            "AS SELECT val FROM t_autogen"
        ))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name,
                target_metadata,
                definition="SELECT val FROM t_autogen",
                starrocks_refresh="ASYNC",
                starrocks_properties={"replication_num": "1"},
                # NOTE: distribution intentionally omitted.
            )
            mc = create_migration_context(connection, target_metadata)
            # Must not raise, and must produce no op for this MV.
            migration_script = api.produce_migrations(mc, target_metadata)
            mv_ops = [op for op in migration_script.upgrade_ops.ops if getattr(op, "view_name", None) == mv_name]
            assert mv_ops == [], f"Unexpected op for MV with omitted distribution: {mv_ops}"
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_no_diff_for_colocated_mv(self, connection) -> None:
        """No change: an MV whose metadata declares colocate_with must not diff every run.

        Regression test for the reported failure. information_schema.tables_config does not
        report colocate_with for MVs, so the reflected side looked as though the property were
        absent while the metadata declared it. Autogenerate reported the same change on every
        run and emitted ``ALTER MATERIALIZED VIEW ... SET ("colocate_with" = ...)``, which
        StarRocks rejects outright: colocate_with is create-only for an MV in a shared-nothing
        cluster, so the migration could never succeed.
        """
        if connection.dialect.run_mode == SystemRunMode.SHARED_DATA:
            config.skip_test("MV colocation behaviour on shared-data clusters is not verified")

        mv_name = "test_mv_colocate_no_diff"
        base_table = "t_autogen_colocate"
        group_name = "test_mv_colocate_no_diff_group"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(f"DROP TABLE IF EXISTS {base_table}"))
        # The base table defines the colocation group; the MV must match its bucket count,
        # replication count and distribution column type to join it.
        connection.execute(text(
            f"CREATE TABLE {base_table} (val INT) "
            "DISTRIBUTED BY HASH(val) BUCKETS 3 "
            f"PROPERTIES ('replication_num' = '1', 'colocate_with' = '{group_name}')"
        ))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} "
            "DISTRIBUTED BY HASH(val) BUCKETS 3 "
            "REFRESH ASYNC "
            f"PROPERTIES ('replication_num' = '1', 'colocate_with' = '{group_name}') "
            f"AS SELECT val FROM {base_table}"
        ))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name,
                target_metadata,
                definition=f"SELECT val FROM {base_table}",
                starrocks_refresh="ASYNC",
                starrocks_distributed_by="HASH(val) BUCKETS 3",
                starrocks_properties={"replication_num": "1", "colocate_with": group_name},
            )
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            mv_ops = [op for op in migration_script.upgrade_ops.ops if getattr(op, "view_name", None) == mv_name]
            assert mv_ops == [], (
                f"Unexpected op for colocated MV whose metadata matches the database: "
                f"{[getattr(op, 'properties', op) for op in mv_ops]}"
            )
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
            connection.execute(text(f"DROP TABLE IF EXISTS {base_table}"))

    def test_refresh_interval_change_detected(self, connection) -> None:
        """ALTER: a genuine change to the refresh interval is still detected.

        Guards against the SCHEDULE/ASYNC normalization being over-eager and masking a real
        interval change (10 MINUTE -> 20 MINUTE).
        """
        mv_name = "test_mv_refresh_interval_change"
        connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW {mv_name} "
            "REFRESH ASYNC EVERY(INTERVAL 10 MINUTE) "
            "PROPERTIES ('replication_num' = '1') "
            "AS SELECT val FROM t_autogen"
        ))
        try:
            target_metadata = MetaData()
            MaterializedView(
                mv_name,
                target_metadata,
                definition="SELECT val FROM t_autogen",
                starrocks_refresh="ASYNC EVERY(INTERVAL 20 MINUTE)",
                starrocks_properties={"replication_num": "1"},
            )
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            refresh_ops = self._refresh_alter_ops(migration_script)
            assert len(refresh_ops) == 1, (
                f"Expected exactly one REFRESH alter op, got: "
                f"{[(op.refresh, op.existing_refresh) for op in refresh_ops]}"
            )
            assert "20 MINUTE" in refresh_ops[0].refresh.upper()
        finally:
            connection.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
