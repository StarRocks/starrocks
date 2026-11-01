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

"""End-to-end coverage for ``combine_column_alters`` against a live cluster.

Autogenerate a revision for a table with several column changes, run it through
the rewriter, execute it, and check that (a) exactly one ``ALTER TABLE`` reaches
the cluster and (b) the table ends up in the target shape. Without the rewriter
the same revision issues one statement per column, which StarRocks rejects with
"Table[...] is doing schema change" while the first job is still running.

The tables live in the plugin's alternate test schema, which each parallel worker has its own of.
"""

import logging

from alembic.autogenerate import api
from alembic.migration import MigrationContext
from alembic.operations import Operations, ops
from sqlalchemy import Column, MetaData, Table, event, testing
from sqlalchemy.testing import config, fixtures

from starrocks import INTEGER, VARCHAR
from starrocks.alembic.ops import AlterTableColumnsOp, combine_column_alters


logger = logging.getLogger(__name__)


class CombineColumnAltersTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    @testing.fixture
    def table_in_test_schema(self, connection):
        created = []

        def create(name: str, properties: str = "", distribution: str = "HASH(id)") -> str:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS `{config.test_schema}`.`{name}`")
            connection.exec_driver_sql(
                f"CREATE TABLE `{config.test_schema}`.`{name}` "
                "(id INT, old_a INT, old_b INT, keep INT) "
                f"DUPLICATE KEY(id) DISTRIBUTED BY {distribution} BUCKETS 1 "
                f"PROPERTIES ('replication_num' = '1'{properties})"
            )
            created.append(name)
            return name

        yield create
        for name in created:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS `{config.test_schema}`.`{name}`")

    def _columns(self, connection, table: str) -> set:
        return {
            row[0]
            for row in connection.exec_driver_sql(
                "SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{config.test_schema}' AND table_name = '{table}'"
            )
        }

    def _target_metadata(self, table: str) -> MetaData:
        """Target shape: drop old_a/old_b, add new_x/new_y, keep id/keep."""
        meta = MetaData()
        Table(
            table,
            meta,
            Column("id", INTEGER),
            Column("keep", INTEGER),
            Column("new_x", INTEGER),
            Column("new_y", VARCHAR(32)),
            schema=config.test_schema,
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id)",
            starrocks_buckets=1,
            starrocks_properties={"replication_num": "1"},
        )
        return meta

    @testing.requires.sqlalchemy_2
    def test_autogenerate_plus_rewriter_emits_one_alter_statement(self, connection, table_in_test_schema):
        """Four column changes must reach the cluster as a single ALTER TABLE.

        Autogenerate only looks at the test schema; other schemas may belong to parallel workers. The rewriter
        mutates the directives in place, exactly as ``process_revision_directives=combine_column_alters`` does
        in env.py. Only the column changes are executed: autogenerate may also report table-option diffs
        (distribution/properties) that are irrelevant here and would start a separate, long-running OPTIMIZE job.
        """
        table = table_in_test_schema("t_combine_exec")
        statements: list[str] = []

        ctx = MigrationContext.configure(
            connection=connection,
            opts={
                "include_schemas": True,
                "include_name": lambda name, type_, parent_names: type_ != "schema" or name == config.test_schema,
                "starrocks_wait_for_schema_change": True,
                "starrocks_schema_change_timeout": 300,
                "starrocks_schema_change_poll_interval": 1.0,
            },
        )
        migrations = api.produce_migrations(ctx, self._target_metadata(table))

        modify_ops = [
            o for o in migrations.upgrade_ops.ops if isinstance(o, ops.ModifyTableOps) and o.table_name == table
        ]
        assert modify_ops, "autogenerate produced no column changes for the table"
        column_ops = [o for o in modify_ops[0].ops if isinstance(o, (ops.AddColumnOp, ops.DropColumnOp))]
        assert len(column_ops) == 4, [type(o).__name__ for o in column_ops]

        combine_column_alters.process_revision_directives(ctx, None, [migrations])

        table_ops = [
            o for o in migrations.upgrade_ops.ops if isinstance(o, ops.ModifyTableOps) and o.table_name == table
        ]
        inner = table_ops[0].ops
        combined = [o for o in inner if isinstance(o, AlterTableColumnsOp)]
        assert len(combined) == 1, [type(o).__name__ for o in inner]
        assert sorted(c.name for c in combined[0].adds) == ["new_x", "new_y"]
        assert sorted(c.name for c in combined[0].drops) == ["old_a", "old_b"]

        @event.listens_for(connection, "before_cursor_execute")
        def _record(conn_, cursor, statement, params, context, executemany):
            normalized = statement.strip().upper()
            if normalized.startswith("ALTER TABLE") and "COLUMN" in normalized:
                statements.append(statement)

        operations = Operations(ctx)
        for inner_op in inner:
            if isinstance(inner_op, (AlterTableColumnsOp, ops.AddColumnOp, ops.DropColumnOp)):
                operations.invoke(inner_op)

        event.remove(connection, "before_cursor_execute", _record)

        assert len(statements) == 1, statements
        assert statements[0].count("ADD COLUMN") == 2
        assert statements[0].count("DROP COLUMN") == 2
        assert self._columns(connection, table) == {"id", "keep", "new_x", "new_y"}

    @testing.requires.sqlalchemy_2
    def test_a_bloom_filter_column_is_dropped_after_the_property_change(self, connection, table_in_test_schema):
        """StarRocks takes a dropped column out of ``bloom_filter_columns`` itself, and then refuses the SET that does.

        So the combined drop comes after the SET, and in the downgrade the combined add comes before it.
        """
        table = table_in_test_schema("t_combine_bloom", ", 'bloom_filter_columns' = 'old_b, keep'")
        target = MetaData()
        Table(
            table,
            target,
            Column("id", INTEGER),
            Column("keep", INTEGER),
            Column("new_x", INTEGER),
            schema=config.test_schema,
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1", "bloom_filter_columns": "keep"},
        )
        ctx = MigrationContext.configure(
            connection=connection,
            opts={
                "include_schemas": True,
                "include_name": lambda name, type_, parent_names: type_ != "schema" or name == config.test_schema,
                "starrocks_wait_for_schema_change": True,
            },
        )
        migrations = api.produce_migrations(ctx, target)
        combine_column_alters.process_revision_directives(ctx, None, [migrations])
        (table_ops,) = [
            o for o in migrations.upgrade_ops.ops if isinstance(o, ops.ModifyTableOps) and o.table_name == table
        ]
        assert [type(o).__name__ for o in table_ops.ops] == [
            "AlterTableColumnsOp", "AlterTablePropertiesOp", "AlterTableColumnsOp"
        ]

        operations = Operations(ctx)
        for inner_op in table_ops.ops:
            operations.invoke(inner_op)
        assert self._columns(connection, table) == {"id", "keep", "new_x"}
        for inner_op in table_ops.reverse().ops:
            operations.invoke(inner_op)
        assert self._columns(connection, table) == {"id", "old_a", "old_b", "keep"}

    @testing.requires.sqlalchemy_2
    def test_a_distribution_column_is_dropped_after_the_distribution_change(self, connection, table_in_test_schema):
        """StarRocks refuses to drop a column the distribution still names, so the combined drop comes after the
        distribution change, and in the downgrade the combined add comes before it."""
        table = table_in_test_schema("t_combine_dist", distribution="HASH(old_b)")
        target = MetaData()
        Table(
            table,
            target,
            Column("id", INTEGER),
            Column("keep", INTEGER),
            Column("new_x", INTEGER),
            schema=config.test_schema,
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(new_x) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        ctx = MigrationContext.configure(
            connection=connection,
            opts={
                "include_schemas": True,
                "include_name": lambda name, type_, parent_names: type_ != "schema" or name == config.test_schema,
                "starrocks_wait_for_schema_change": True,
            },
        )
        migrations = api.produce_migrations(ctx, target)
        combine_column_alters.process_revision_directives(ctx, None, [migrations])
        (table_ops,) = [
            o for o in migrations.upgrade_ops.ops if isinstance(o, ops.ModifyTableOps) and o.table_name == table
        ]
        assert [type(o).__name__ for o in table_ops.ops] == [
            "AlterTableColumnsOp", "AlterTableDistributionOp", "AlterTableColumnsOp"
        ]

        operations = Operations(ctx)
        for inner_op in table_ops.ops:
            operations.invoke(inner_op)
        assert self._columns(connection, table) == {"id", "keep", "new_x"}
        for inner_op in table_ops.reverse().ops:
            operations.invoke(inner_op)
        assert self._columns(connection, table) == {"id", "old_a", "old_b", "keep"}

    def test_uncombined_second_statement_hits_the_in_flight_job_error(self, connection, table_in_test_schema):
        """Why the feature exists: back-to-back ALTERs can be refused.

        StarRocks allows one in-flight column schema-change job per table. This
        documents the failure mode the rewriter avoids; on a fast-schema-
        evolution table the first job may already be finished, in which case the
        second statement succeeds and the test simply records that.
        """
        table = table_in_test_schema("t_uncombined")
        connection.exec_driver_sql(f"ALTER TABLE `{config.test_schema}`.`{table}` DROP COLUMN old_a")
        try:
            connection.exec_driver_sql(f"ALTER TABLE `{config.test_schema}`.`{table}` DROP COLUMN old_b")
        except Exception as exc:  # noqa: BLE001 - message is the assertion
            assert "schema change" in str(exc).lower(), str(exc)
            logger.info("second ALTER refused as expected: %s", exc)
            return
        logger.info("second ALTER succeeded (fast schema evolution finished the first job)")
        assert "old_b" not in self._columns(connection, table)
