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

These tests require a shared-data cluster whose default distribution is RANGE;
they skip on a shared-nothing cluster (where ``needsRangeRewriteSchemaChange``
returns false immediately and the restriction cannot trigger).
"""

from __future__ import annotations

import logging
from typing import Union

from alembic.migration import MigrationContext
from alembic.operations import Operations, ops
import pytest
from sqlalchemy import Column
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError

from starrocks import INTEGER
from starrocks.alembic.ops import AlterTableColumnsOp, _combine_column_alters
from test.conftest_sr import create_test_engine


logger = logging.getLogger(__name__)

SCHEMA = "sr_range_key_test"


def _range_distribution_available(engine: Engine) -> bool:
    """Whether this cluster can host a RANGE-distributed cloud-native table.

    A table created without ``DISTRIBUTED BY`` gets RANGE distribution on a
    shared-data cluster; a shared-nothing cluster reports RANDOM.
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS `{SCHEMA}`")
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS `{SCHEMA}`.`probe_range`")
        try:
            conn.exec_driver_sql(
                f"CREATE TABLE `{SCHEMA}`.`probe_range` (k1 INT, k2 INT, v INT) "
                "DUPLICATE KEY(k1, k2)"
            )
        except DatabaseError as exc:
            logger.info("range probe table not creatable: %s", exc)
            return False
        dist = conn.exec_driver_sql(
            "SELECT DISTRIBUTE_TYPE FROM information_schema.tables_config "
            f"WHERE TABLE_SCHEMA = '{SCHEMA}' AND TABLE_NAME = 'probe_range'"
        ).scalar()
    logger.info("probe table distribution: %s", dist)
    return str(dist).upper() == "RANGE"


@pytest.mark.integration
class TestCombineColumnAltersOnRangeTable:
    engine: Engine
    schema: Union[str, None]

    @classmethod
    def setup_class(cls) -> None:
        cls.engine = create_test_engine()
        cls.schema = SCHEMA
        if not _range_distribution_available(cls.engine):
            with cls.engine.begin() as conn:
                conn.exec_driver_sql(f"DROP DATABASE IF EXISTS `{SCHEMA}`")
            cls.engine.dispose()
            pytest.skip(
                "cluster has no RANGE-distributed tables (shared-nothing run_mode); "
                "the FE's single-clause routing restriction cannot trigger here"
            )

    @classmethod
    def teardown_class(cls) -> None:
        with cls.engine.begin() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS `{cls.schema}`")
        cls.engine.dispose()

    def _create_range_table(self, conn, name: str) -> None:
        """A RANGE-distributed table whose sort key is its key columns."""
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS `{self.schema}`.`{name}`")
        conn.exec_driver_sql(
            f"CREATE TABLE `{self.schema}`.`{name}` (k1 INT, k2 INT, v INT) "
            "DUPLICATE KEY(k1, k2)"
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

    def test_fe_rejects_a_batched_range_sort_key_drop(self):
        """The premise: the FE refuses the multi-clause statement outright."""
        table = "t_fe_reject"
        with self.engine.begin() as conn:
            self._create_range_table(conn, table)
            with pytest.raises(DatabaseError) as excinfo:
                conn.exec_driver_sql(
                    f"ALTER TABLE `{self.schema}`.`{table}` "
                    "DROP COLUMN k2, ADD COLUMN v2 INT"
                )
        assert "can not be combined with other alter operations" in str(excinfo.value)

    def test_fe_accepts_the_same_changes_as_separate_statements(self):
        """...while the pre-PR, one-statement-per-column shape works."""
        table = "t_fe_separate"
        with self.engine.begin() as conn:
            self._create_range_table(conn, table)
            conn.exec_driver_sql(
                f"ALTER TABLE `{self.schema}`.`{table}` DROP COLUMN k2"
            )
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                f"ALTER TABLE `{self.schema}`.`{table}` ADD COLUMN v2 INT"
            )
        cols = self._columns(table)
        assert "k2" not in cols and "v2" in cols

    @pytest.mark.xfail(
        reason="combine_column_alters coalesces unconditionally; range sort-key "
               "changes must stay standalone",
        strict=False,
    )
    def test_rewriter_keeps_a_range_sort_key_drop_standalone(self):
        """A sort-key DROP must not be folded in with another column change."""
        table = "t_rewriter_drop"
        with self.engine.begin() as conn:
            self._create_range_table(conn, table)
            ctx = MigrationContext.configure(connection=conn)

            mto = ops.ModifyTableOps(table, ops=[
                ops.DropColumnOp(table, "k2", schema=self.schema),
                ops.AddColumnOp(table, Column("v2", INTEGER), schema=self.schema),
            ], schema=self.schema)

            res = _combine_column_alters(ctx, None, mto)

        combined = [o for o in res.ops if isinstance(o, AlterTableColumnsOp)]
        # The sort-key drop must not share a statement with the add.
        assert not any(
            any(c.name == "k2" for c in o.drops) and o.adds for o in combined
        ), "range sort-key DROP was coalesced with another column change"

    @pytest.mark.xfail(
        reason="combine_column_alters coalesces unconditionally; the resulting "
               "multi-clause ALTER is rejected by the FE",
        strict=False,
    )
    def test_rewritten_migration_executes_on_a_range_table(self):
        """End to end: the rewritten ops must actually run against the cluster."""
        table = "t_rewriter_exec"
        with self.engine.begin() as conn:
            self._create_range_table(conn, table)
            ctx = MigrationContext.configure(
                connection=conn,
                opts={
                    "starrocks_wait_for_schema_change": True,
                    "starrocks_schema_change_timeout": 300,
                    "starrocks_schema_change_poll_interval": 1.0,
                },
            )

            mto = ops.ModifyTableOps(table, ops=[
                ops.DropColumnOp(table, "k2", schema=self.schema),
                ops.AddColumnOp(table, Column("v2", INTEGER), schema=self.schema),
            ], schema=self.schema)
            rewritten = _combine_column_alters(ctx, None, mto)

            operations = Operations(ctx)
            for inner in rewritten.ops:
                operations.invoke(inner)

        cols = self._columns(table)
        assert "k2" not in cols and "v2" in cols
