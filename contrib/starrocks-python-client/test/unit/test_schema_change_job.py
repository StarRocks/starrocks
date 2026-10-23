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

"""The schema-change job, the wait for it to end, and what each waiter makes of the job the wait stops on."""

import time
from unittest import mock

from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column, Integer, MetaData, Table, testing
from sqlalchemy.testing import eq_, expect_raises_message, fixtures, is_, is_false, is_true

from starrocks import provision
from starrocks.dialect import StarRocksDialect
from starrocks.schema_change import SchemaChangeJob, wait_for_job_to_end


def _jobs_in_turn(*states):
    """A reader that returns a job in each state in turn, then None once they run out."""
    jobs = iter([SchemaChangeJob("7", state, "") for state in states])
    return lambda: next(jobs, None)


class SchemaChangeJobTest(fixtures.TestBase):
    def test_reads_a_row(self):
        eq_(
            SchemaChangeJob.from_row({"JobId": 7, "State": "CANCELLED", "Msg": "boom"}),
            SchemaChangeJob("7", "CANCELLED", "boom"),
        )

    def test_a_missing_message_is_empty(self):
        eq_(SchemaChangeJob.from_row({"JobId": 7, "State": "FINISHED", "Msg": None}).message, "")

    @testing.combinations(
        ("no_row", None),
        ("no_state", {"JobId": 7}),
        ("null_state", {"JobId": 7, "State": None}),
        id_="ia",
        argnames="row",
    )
    def test_no_job_without_a_state(self, row):
        is_(SchemaChangeJob.from_row(row), None)

    @testing.combinations(
        ("finished", "FINISHED", True, False),
        ("cancelled", "cancelled", True, True),
        ("running", "RUNNING", False, False),
        ("waiting_txn", "WAITING_TXN", False, False),
        ("unknown", "SOME_NEW_STATE", False, False),
        id_="iaaa",
        argnames="state, ended, cancelled",
    )
    def test_ended_and_cancelled(self, state, ended, cancelled):
        job = SchemaChangeJob("7", state, "")
        eq_((job.has_ended, job.was_cancelled), (ended, cancelled))


class WaitForJobToEndTest(fixtures.TestBase):
    def test_no_job_ends_the_wait(self):
        with mock.patch("starrocks.schema_change.time.sleep") as sleep:
            is_(wait_for_job_to_end(lambda: None, "COLUMN", "t", 1.0, None), None)
        is_false(sleep.called)

    def test_polls_until_the_job_ends(self):
        with mock.patch("starrocks.schema_change.time.sleep") as sleep:
            job = wait_for_job_to_end(_jobs_in_turn("PENDING", "RUNNING", "CANCELLED"), "COLUMN", "t", 1.0, None)
        eq_(job, SchemaChangeJob("7", "CANCELLED", ""))
        eq_(sleep.call_args_list, [mock.call(1.0), mock.call(1.0)])

    def test_returns_the_running_job_at_the_deadline(self):
        with mock.patch("starrocks.schema_change.time.sleep") as sleep:
            job = wait_for_job_to_end(_jobs_in_turn("RUNNING", "FINISHED"), "COLUMN", "t", 1.0, time.monotonic() - 1)
        eq_(job.state, "RUNNING")
        is_false(sleep.called)

    def test_sleeps_no_longer_than_the_deadline_allows(self):
        with mock.patch("starrocks.schema_change.time.sleep") as sleep:
            wait_for_job_to_end(_jobs_in_turn("RUNNING", "FINISHED"), "COLUMN", "t", 60.0, time.monotonic() + 5)
        (seconds,), _ = sleep.call_args
        is_true(0 < seconds <= 5)


def _connection_reading(row):
    """A connection whose SHOW ALTER TABLE query always returns the row."""
    connection = mock.Mock()
    connection.execute.return_value.mappings.return_value.first.return_value = row
    return connection


class WaitersTest(fixtures.TestBase):
    """The Alembic impl raises on a cancelled job and at its timeout; the test provisioning goes on after a cancelled
    job, since it leaves the table free, and raises at its deadline.
    """

    @staticmethod
    def _impl(connection, **opts):
        context = MigrationContext.configure(
            dialect=StarRocksDialect(), opts={"starrocks_schema_change_poll_interval": 0.005, **opts}
        )
        context.impl.connection = connection
        return context.impl

    def test_the_alembic_wait_ends_on_a_finished_job(self):
        self._impl(_connection_reading({"JobId": 6, "State": "FINISHED"}))._wait_for_schema_change("t", None)

    def test_the_alembic_wait_raises_on_a_cancelled_job(self):
        impl = self._impl(_connection_reading({"JobId": 6, "State": "CANCELLED", "Msg": "boom"}))
        with expect_raises_message(RuntimeError, "CANCELLED: boom"):
            impl._wait_for_schema_change("t", None)

    def test_the_alembic_wait_raises_at_its_timeout(self):
        impl = self._impl(_connection_reading({"JobId": 6, "State": "RUNNING"}), starrocks_schema_change_timeout=0.01)
        with expect_raises_message(RuntimeError, "Timed out after 0.01s .*last state: RUNNING"):
            impl._wait_for_schema_change("t", None)

    def test_provision_goes_on_after_a_cancelled_job(self):
        provision.wait_for_schema_changes(_connection_reading({"JobId": 6, "State": "CANCELLED", "Msg": "boom"}), "t")

    def test_provision_raises_at_its_deadline(self):
        connection = _connection_reading({"JobId": 6, "State": "RUNNING"})
        with mock.patch.object(provision, "SCHEMA_CHANGE_WAIT_SECONDS", 0):
            with expect_raises_message(TimeoutError, "schema change on t still RUNNING"):
                provision.wait_for_schema_changes(connection, "t")

    @testing.skip_if(
        lambda config: provision.delete_from_all_tables is None, "SQLAlchemy 1.4 has no delete_from_all_tables hook"
    )
    def test_the_delete_hook_empties_each_table_that_exists(self):
        metadata = MetaData()
        kept = Table("kept", metadata, Column("id", Integer))
        Table("never_created", metadata, Column("id", Integer))
        connection = mock.Mock()
        inspector = mock.Mock()
        inspector.get_table_names.return_value = ["kept"]
        with mock.patch.object(provision, "inspect", return_value=inspector):
            with mock.patch.object(provision, "wait_for_schema_changes") as wait:
                provision.delete_from_all_tables.fns["starrocks"](connection, None, metadata)
        eq_(wait.call_args_list, [mock.call(connection, "kept", None)])
        (statement,), _ = connection.execute.call_args
        eq_((connection.execute.call_count, str(statement)), (1, str(kept.delete())))
