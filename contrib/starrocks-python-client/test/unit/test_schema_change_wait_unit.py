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

"""Offline tests for the schema-change wait plumbing.

Covers the pieces that need no cluster: the state enum, the shared
``SHOW ALTER TABLE`` statement builder in the dialect, and the construct
classification used to decide whether to poll at all.
"""

from unittest.mock import Mock

from alembic.ddl import base as alembic_base
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column, testing
from sqlalchemy.testing import expect_raises_message, fixtures

from starrocks import INTEGER
from starrocks.alembic.starrocks import SchemaChangeState, StarRocksImpl
from starrocks.dialect import StarRocksDialect
from starrocks.sql.ddl import AlterTableColumns, AlterTableDistribution, AlterTableOrder, AlterTableProperties


class SchemaChangeStateTest(fixtures.TestBase):
    def test_known_states_parse_case_insensitively(self):
        assert SchemaChangeState.parse("finished") is SchemaChangeState.FINISHED
        assert SchemaChangeState.parse("CANCELLED") is SchemaChangeState.CANCELLED
        assert SchemaChangeState.parse("Running") is SchemaChangeState.RUNNING
        assert SchemaChangeState.parse("waiting_txn") is SchemaChangeState.WAITING_TXN
        assert SchemaChangeState.parse("PENDING") is SchemaChangeState.PENDING

    def test_unknown_state_is_not_terminal(self):
        # A state added by a future StarRocks version must keep the caller
        # waiting rather than be mistaken for FINISHED or CANCELLED.
        state = SchemaChangeState.parse("SOME_NEW_STATE")
        assert state is None
        assert state is not SchemaChangeState.FINISHED
        assert state is not SchemaChangeState.CANCELLED


class GenShowAlterTableStatementTest(fixtures.TestBase):
    def test_default_shape_is_unchanged(self):
        # Existing callers (e.g. test/system/test_table_lifecycle.py) rely on this.
        stmt = StarRocksDialect.gen_show_alter_table_statement("t", "OPTIMIZE")
        assert stmt == "SHOW ALTER TABLE OPTIMIZE WHERE TableName='t' AND State='RUNNING'"

    def test_schema_is_quoted(self):
        stmt = StarRocksDialect.gen_show_alter_table_statement("t", "COLUMN", schema="my-db")
        assert "FROM `my-db` " in stmt

    def test_poll_shape_used_by_the_wait_feature(self):
        stmt = StarRocksDialect.gen_show_alter_table_statement(
            "t", "COLUMN", schema="db", state=None,
            order_by="JobId DESC", limit=1, bind_table_name=True,
        )
        assert stmt == (
            "SHOW ALTER TABLE COLUMN FROM `db` WHERE TableName=:table_name "
            "ORDER BY JobId DESC LIMIT 1"
        )
        # No state filter: the newest job is wanted whatever state it is in.
        assert "State=" not in stmt

    def test_limit_is_coerced_to_an_int(self):
        stmt = StarRocksDialect.gen_show_alter_table_statement("t", "COLUMN", limit="5")
        assert stmt.endswith("LIMIT 5")


class SchemaChangeTargetTest(fixtures.TestBase):
    def test_column_constructs_are_targets(self):
        col = Column("a", INTEGER)
        for construct in (
            AlterTableColumns("t", adds=[col], schema="db"),
            alembic_base.AddColumn("t", col, schema="db"),
            alembic_base.DropColumn("t", col, schema="db"),
        ):
            assert StarRocksImpl._schema_change_target(construct) == ("t", "db")

    def test_unrelated_construct_is_not_a_target(self):
        assert StarRocksImpl._schema_change_target(object()) is None

    def test_missing_table_name_is_not_a_target(self):
        # The declared return type promises a str table name; a construct
        # without one must not reach the poll.
        construct = AlterTableColumns("t", adds=[Column("a", INTEGER)])
        construct.table_name = None
        assert StarRocksImpl._schema_change_target(construct) is None


class OptimizeJobTargetTest(fixtures.TestBase):
    """A distribution change starts an OPTIMIZE job; sort key and property changes and column changes start COLUMN ones."""

    @testing.combinations(
        ("distribution", lambda: AlterTableDistribution("t", "HASH(id)", schema="db"), "OPTIMIZE"),
        ("order", lambda: AlterTableOrder("t", "id", schema="db"), "COLUMN"),
        ("properties", lambda: AlterTableProperties("t", {"bloom_filter_columns": "id"}, schema="db"), "COLUMN"),
        ("columns", lambda: AlterTableColumns("t", adds=[Column("a", INTEGER)], schema="db"), "COLUMN"),
        argnames="make_construct, kind",
        id_="iaa",
    )
    def test_targets_and_job_kind(self, make_construct, kind):
        construct = make_construct()
        assert StarRocksImpl._schema_change_target(construct) == ("t", "db")
        assert StarRocksImpl._schema_change_job_kind(construct) == kind


class WaitForNewJobTest(fixtures.TestBase):
    """The wait looks only at a job newer than the one the table had before the statement."""

    @staticmethod
    def _impl_with_jobs(rows):
        context = MigrationContext.configure(
            dialect=StarRocksDialect(), opts={"starrocks_schema_change_poll_interval": 0}
        )
        connection = Mock()
        connection.execute.return_value.mappings.return_value.first.side_effect = list(rows)
        context.impl.connection = connection
        return context.impl, connection

    def test_an_older_cancelled_job_is_not_the_statements_job(self):
        impl, _ = self._impl_with_jobs([{"JobId": "5", "State": "CANCELLED", "Msg": "Table 1 does not exist"}])
        impl._wait_for_schema_change("t", None, alter_type="COLUMN", after_job_id=5)

    def test_a_newer_cancelled_job_raises(self):
        impl, _ = self._impl_with_jobs([{"JobId": "6", "State": "CANCELLED", "Msg": "boom"}])
        with expect_raises_message(RuntimeError, "CANCELLED: boom"):
            impl._wait_for_schema_change("t", None, alter_type="COLUMN", after_job_id=5)

    def test_polls_until_the_newer_job_finishes(self):
        impl, connection = self._impl_with_jobs(
            [{"JobId": "6", "State": "RUNNING"}, {"JobId": "6", "State": "FINISHED"}]
        )
        impl._wait_for_schema_change("t", "db", alter_type="OPTIMIZE", after_job_id=5)
        assert connection.execute.call_count == 2
        assert "SHOW ALTER TABLE OPTIMIZE FROM `db`" in str(connection.execute.call_args[0][0])

    def test_no_job_at_all_is_not_waited_for(self):
        impl, _ = self._impl_with_jobs([None])
        impl._wait_for_schema_change("t", None, alter_type="COLUMN", after_job_id=0)

    def test_latest_job_id_is_zero_for_a_table_without_jobs(self):
        impl, _ = self._impl_with_jobs([None])
        assert impl._latest_job_id("t", None, "COLUMN") == 0

    def test_latest_job_id_reads_the_newest_job(self):
        impl, _ = self._impl_with_jobs([{"JobId": "71510", "State": "FINISHED"}])
        assert impl._latest_job_id("t", None, "OPTIMIZE") == 71510
