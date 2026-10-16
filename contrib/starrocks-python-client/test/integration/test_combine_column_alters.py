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
"""

from __future__ import annotations

import logging
from typing import Union

from alembic.autogenerate import api
from alembic.migration import MigrationContext
from alembic.operations import Operations, ops
import pytest
from sqlalchemy import Column, MetaData, Table, event
from sqlalchemy.engine import Engine

from starrocks import INTEGER, VARCHAR
import starrocks.alembic  # noqa: F401  (registers the impl, ops and renderers)
from starrocks.alembic.ops import AlterTableColumnsOp, combine_column_alters
from test.conftest_sr import create_test_engine


logger = logging.getLogger(__name__)

SCHEMA = "sr_combine_alters_test"


@pytest.mark.integration
class TestCombineColumnAltersIntegration:
    engine: Engine
    schema: Union[str, None]

    @classmethod
    def setup_class(cls) -> None:
        cls.engine = create_test_engine()
        cls.schema = SCHEMA
        with cls.engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS `{cls.schema}`")

    @classmethod
    def teardown_class(cls) -> None:
        with cls.engine.begin() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS `{cls.schema}`")
        cls.engine.dispose()

    def _create_table(self, conn, name: str) -> None:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS `{self.schema}`.`{name}`")
        conn.exec_driver_sql(
            f"CREATE TABLE `{self.schema}`.`{name}` "
            "(id INT, old_a INT, old_b INT, keep INT) "
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
            "PROPERTIES ('replication_num' = '1')"
        )

    def _columns(self, table: str) -> set:
        with self.engine.connect() as conn:
            return {
                row[0]
                for row in conn.exec_driver_sql(
                    "SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema = '{self.schema}' AND table_name = '{table}'"
                )
            }

    def _target_metadata(self, table: str) -> MetaData:
        """Target shape: drop old_a/old_b, add new_x/new_y, keep id/keep."""
        meta = MetaData()
        Table(
            table, meta,
            Column("id", INTEGER),
            Column("keep", INTEGER),
            Column("new_x", INTEGER),
            Column("new_y", VARCHAR(32)),
            schema=self.schema,
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id)",
            starrocks_buckets=1,
            starrocks_properties={"replication_num": "1"},
        )
        return meta

    def test_autogenerate_plus_rewriter_emits_one_alter_statement(self):
        """Four column changes must reach the cluster as a single ALTER TABLE."""
        table = "t_combine_exec"
        statements: list[str] = []

        with self.engine.begin() as conn:
            self._create_table(conn, table)

            ctx = MigrationContext.configure(
                connection=conn,
                opts={
                    "include_schemas": True,
                    "starrocks_wait_for_schema_change": True,
                    "starrocks_schema_change_timeout": 300,
                    "starrocks_schema_change_poll_interval": 1.0,
                },
            )
            migrations = api.produce_migrations(ctx, self._target_metadata(table))

            modify_ops = [
                o for o in migrations.upgrade_ops.ops
                if isinstance(o, ops.ModifyTableOps) and o.table_name == table
            ]
            assert modify_ops, "autogenerate produced no column changes for the table"
            column_ops = [
                o for o in modify_ops[0].ops
                if isinstance(o, (ops.AddColumnOp, ops.DropColumnOp))
            ]
            assert len(column_ops) == 4, [type(o).__name__ for o in column_ops]

            # The rewriter mutates the directives in place, exactly as
            # ``process_revision_directives=combine_column_alters`` does in env.py.
            combine_column_alters.process_revision_directives(ctx, None, [migrations])

            table_ops = [
                o for o in migrations.upgrade_ops.ops
                if isinstance(o, ops.ModifyTableOps) and o.table_name == table
            ]
            inner = table_ops[0].ops
            combined = [o for o in inner if isinstance(o, AlterTableColumnsOp)]
            assert len(combined) == 1, [type(o).__name__ for o in inner]
            assert sorted(c.name for c in combined[0].adds) == ["new_x", "new_y"]
            assert sorted(c.name for c in combined[0].drops) == ["old_a", "old_b"]

            @event.listens_for(conn, "before_cursor_execute")
            def _record(conn_, cursor, statement, params, context, executemany):
                normalized = statement.strip().upper()
                if normalized.startswith("ALTER TABLE") and "COLUMN" in normalized:
                    statements.append(statement)

            # Execute only the column changes: autogenerate may also report
            # table-option diffs (distribution/properties) that are irrelevant
            # here and would start a separate, long-running OPTIMIZE job.
            operations = Operations(ctx)
            for inner_op in inner:
                if isinstance(inner_op, (AlterTableColumnsOp, ops.AddColumnOp, ops.DropColumnOp)):
                    operations.invoke(inner_op)

            event.remove(conn, "before_cursor_execute", _record)

        assert len(statements) == 1, statements
        assert statements[0].count("ADD COLUMN") == 2
        assert statements[0].count("DROP COLUMN") == 2
        assert self._columns(table) == {"id", "keep", "new_x", "new_y"}

    def test_uncombined_second_statement_hits_the_in_flight_job_error(self):
        """Why the feature exists: back-to-back ALTERs can be refused.

        StarRocks allows one in-flight column schema-change job per table. This
        documents the failure mode the rewriter avoids; on a fast-schema-
        evolution table the first job may already be finished, in which case the
        second statement succeeds and the test simply records that.
        """
        table = "t_uncombined"
        with self.engine.begin() as conn:
            self._create_table(conn, table)
            conn.exec_driver_sql(
                f"ALTER TABLE `{self.schema}`.`{table}` DROP COLUMN old_a"
            )
            try:
                conn.exec_driver_sql(
                    f"ALTER TABLE `{self.schema}`.`{table}` DROP COLUMN old_b"
                )
            except Exception as exc:  # noqa: BLE001 - message is the assertion
                assert "schema change" in str(exc).lower(), str(exc)
                logger.info("second ALTER refused as expected: %s", exc)
                return
        logger.info("second ALTER succeeded (fast schema evolution finished the first job)")
        assert "old_b" not in self._columns(table)
