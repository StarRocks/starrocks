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

"""``combine_column_alters`` vs. StarRocks' range-distribution restrictions.

On a shared-data table with RANGE distribution, the FE routes a change that
shifts the range sort key to a dedicated rewrite job and refuses to run it in a
batch (``SchemaChangeHandler``):

* ``buildRoutedAddKeyColumnJob``: "ADD COLUMN that changes the range sort key on
  a range-distribution table can not be combined with other alter operations";
* the routed DROP path: "DROP COLUMN that changes the range sort key on a
  range-distribution table can not be combined with other alter operations".

``_combine_column_alters`` folds every ADD/DROP for a table into one multi-clause
``ALTER TABLE``, so such a revision becomes unexecutable even though the same
changes work as separate statements.

These tests require a shared-data cluster whose default distribution is RANGE, and skip elsewhere. A
shared-nothing StarRocks 4.1 can make RANGE tables too, by default or with ``SET enable_range_distribution =
true``, but there it refuses a DROP of a sort-key column outright, on its own as in a batch ("DROP COLUMN on a
sort-key column is not supported on tables with range distribution"), so batching makes no difference.
The tables live in the plugin's alternate test schema, which each parallel worker has its own of.
"""

import logging

from alembic.migration import MigrationContext
from alembic.operations import Operations, ops
from sqlalchemy import Column, testing
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DatabaseError
from sqlalchemy.testing import config, expect_raises_message, fixtures

from starrocks import INTEGER
from starrocks.alembic.ops import AlterTableColumnsOp, _combine_column_alters
from starrocks.common.types import SystemRunMode


logger = logging.getLogger(__name__)

COALESCES_RANGE_SORT_KEY_CHANGES = testing.fails_on(
    "starrocks",
    "combine_column_alters coalesces unconditionally; on a shared-data RANGE table, range sort-key changes must "
    "stay standalone, because the FE rejects them in a multi-clause ALTER",
)


def _range_distribution_available(connection: Connection) -> bool:
    """Whether this cluster can host a RANGE-distributed cloud-native table.

    A table created without ``DISTRIBUTED BY`` gets RANGE distribution on a
    shared-data cluster; a shared-nothing cluster reports RANDOM.
    """
    probe = f"`{config.test_schema}`.`probe_range`"
    connection.exec_driver_sql(f"DROP TABLE IF EXISTS {probe}")
    try:
        connection.exec_driver_sql(f"CREATE TABLE {probe} (k1 INT, k2 INT, v INT) DUPLICATE KEY(k1, k2)")
    except DatabaseError as exc:
        logger.info("range probe table not creatable: %s", exc)
        return False
    try:
        dist = connection.exec_driver_sql(
            "SELECT DISTRIBUTE_TYPE FROM information_schema.tables_config "
            f"WHERE TABLE_SCHEMA = '{config.test_schema}' AND TABLE_NAME = 'probe_range'"
        ).scalar()
    finally:
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {probe}")
    logger.info("probe table distribution: %s", dist)
    return str(dist).upper() == "RANGE"


class CombineColumnAltersOnRangeTableTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    @classmethod
    def setup_test_class(cls):
        with config.db.begin() as connection:
            if connection.dialect.run_mode != SystemRunMode.SHARED_DATA:
                config.skip_test(
                    "the cluster is not shared-data; the FE routes range sort-key changes to single-clause rewrite "
                    "jobs only on shared-data RANGE tables"
                )
            if not _range_distribution_available(connection):
                config.skip_test(
                    "the cluster's default distribution is not RANGE; the FE routes range sort-key changes to "
                    "single-clause rewrite jobs only on shared-data RANGE tables"
                )

    @testing.fixture
    def range_table(self, connection):
        """Creates a RANGE-distributed table, whose sort key is its key columns, and drops it afterwards."""
        created = []

        def create(name: str) -> str:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS `{config.test_schema}`.`{name}`")
            connection.exec_driver_sql(
                f"CREATE TABLE `{config.test_schema}`.`{name}` (k1 INT, k2 INT, v INT) DUPLICATE KEY(k1, k2)"
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

    def _drop_k2_and_add_v2(self, table: str) -> ops.ModifyTableOps:
        return ops.ModifyTableOps(
            table,
            ops=[
                ops.DropColumnOp(table, "k2", schema=config.test_schema),
                ops.AddColumnOp(table, Column("v2", INTEGER), schema=config.test_schema),
            ],
            schema=config.test_schema,
        )

    def test_fe_rejects_a_batched_range_sort_key_drop(self, connection, range_table):
        """The premise: the FE refuses the multi-clause statement outright."""
        table = range_table("t_fe_reject")
        with expect_raises_message(DatabaseError, "can not be combined with other alter operations"):
            connection.exec_driver_sql(
                f"ALTER TABLE `{config.test_schema}`.`{table}` DROP COLUMN k2, ADD COLUMN v2 INT"
            )

    def test_fe_accepts_the_same_changes_as_separate_statements(self, connection, range_table):
        """...while the pre-PR, one-statement-per-column shape works."""
        table = range_table("t_fe_separate")
        connection.exec_driver_sql(f"ALTER TABLE `{config.test_schema}`.`{table}` DROP COLUMN k2")
        connection.exec_driver_sql(f"ALTER TABLE `{config.test_schema}`.`{table}` ADD COLUMN v2 INT")
        cols = self._columns(connection, table)
        assert "k2" not in cols and "v2" in cols

    @COALESCES_RANGE_SORT_KEY_CHANGES
    def test_rewriter_keeps_a_range_sort_key_drop_standalone(self, connection, range_table):
        """A sort-key DROP must not be folded in with another column change."""
        table = range_table("t_rewriter_drop")
        ctx = MigrationContext.configure(connection=connection)
        res = _combine_column_alters(ctx, None, self._drop_k2_and_add_v2(table))

        combined = [o for o in res.ops if isinstance(o, AlterTableColumnsOp)]
        assert not any(
            any(c.name == "k2" for c in o.drops) and o.adds for o in combined
        ), "range sort-key DROP was coalesced with another column change"

    @COALESCES_RANGE_SORT_KEY_CHANGES
    def test_rewritten_migration_executes_on_a_range_table(self, connection, range_table):
        """End to end: the rewritten ops must actually run against the cluster."""
        table = range_table("t_rewriter_exec")
        ctx = MigrationContext.configure(
            connection=connection,
            opts={
                "starrocks_wait_for_schema_change": True,
                "starrocks_schema_change_timeout": 300,
                "starrocks_schema_change_poll_interval": 1.0,
            },
        )
        rewritten = _combine_column_alters(ctx, None, self._drop_k2_and_add_v2(table))

        operations = Operations(ctx)
        for inner in rewritten.ops:
            operations.invoke(inner)

        cols = self._columns(connection, table)
        assert "k2" not in cols and "v2" in cols
