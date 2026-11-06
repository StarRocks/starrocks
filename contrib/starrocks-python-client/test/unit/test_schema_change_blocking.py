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

"""Which statements the dialect holds back until the schema change running on their table has ended, and for how long."""

from contextlib import contextmanager
import logging
import re
from types import SimpleNamespace
from unittest import mock

from alembic.ddl import base as alembic_base
import pymysql
from sqlalchemy import DDL, Column, Index, MetaData, Table, create_engine, engine_from_config, exc, testing
from sqlalchemy.schema import CreateIndex, CreateTable, DropIndex, SetTableComment
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_, expect_raises_message

from starrocks import INTEGER
from starrocks.dialect import SchemaChangeJob, SchemaChangeTimeoutError, StarRocksDialect
from starrocks.sql.ddl import AlterTableProperties


class TableAlteredByTest(fixtures.TestBase):
    @staticmethod
    def _table():
        return Table("events", MetaData(), Column("id", INTEGER), Column("city", INTEGER), schema="analytics")

    @staticmethod
    def _altered(ddl, statement=None, schema_translate_map=None):
        dialect = StarRocksDialect()
        statement = statement if statement is not None else str(ddl.compile(dialect=dialect))
        context = SimpleNamespace(
            isddl=True,
            compiled=SimpleNamespace(statement=ddl),
            execution_options={"schema_translate_map": schema_translate_map} if schema_translate_map else {},
        )
        return StarRocksDialect._table_altered_by(statement, context)

    def test_create_and_drop_index_wait(self):
        index = Index("ix_city", self._table().c.city)
        assert self._altered(CreateIndex(index)) == ("events", "analytics")
        assert self._altered(DropIndex(index)) == ("events", "analytics")

    def test_the_schema_translate_map_names_the_schema_waited_on(self):
        index = Index("ix_city", self._table().c.city)
        assert self._altered(CreateIndex(index), schema_translate_map={"analytics": "staging"}) == ("events", "staging")
        table = self._table()
        table.comment = "events"
        assert self._altered(SetTableComment(table), schema_translate_map={"analytics": "staging"}) == (
            "events",
            "staging",
        )

    def test_an_alembic_alter_waits_on_the_schema_it_names(self):
        """Alembic writes a string schema into its ALTER as given, so the ``schema_translate_map`` doesn't reach it."""
        add = alembic_base.AddColumn("events", Column("note", INTEGER))
        assert self._altered(add, "ALTER TABLE events ADD COLUMN note INTEGER", {None: "staging"}) == ("events", None)

    def test_a_starrocks_alter_waits_on_the_schema_it_names(self):
        alter = AlterTableProperties("events", {"replication_num": "1"})
        statement = str(alter.compile(dialect=StarRocksDialect(), schema_translate_map={None: "staging"}))
        assert "staging" not in statement, statement
        assert self._altered(alter, statement, {None: "staging"}) == ("events", None)

    def test_alembic_alter_table_waits(self):
        add = alembic_base.AddColumn("events", Column("note", INTEGER), schema="analytics")
        assert self._altered(add, "ALTER TABLE analytics.events ADD COLUMN note INTEGER") == ("events", "analytics")

    def test_starrocks_alter_table_waits(self):
        alter = AlterTableProperties("events", {"replication_num": "1"}, schema="analytics")
        assert self._altered(alter) == ("events", "analytics")

    def test_alter_table_on_a_table_element_waits(self):
        table = self._table()
        table.comment = "events"
        assert self._altered(SetTableComment(table)) == ("events", "analytics")

    def test_create_table_does_not_wait(self):
        assert self._altered(CreateTable(self._table())) is None

    def test_a_ddl_construct_holding_text_does_not_wait(self):
        """``DDL()`` is text, so it names no table the dialect reads, as text sent any other way doesn't."""
        assert self._altered(DDL("ALTER TABLE analytics.events MODIFY COLUMN city BIGINT")) is None

    def test_statements_that_are_not_ddl_do_not_wait(self):
        assert StarRocksDialect._table_altered_by("ALTER TABLE t ADD COLUMN c INT", None) is None
        assert StarRocksDialect._table_altered_by(
            "ALTER TABLE t ADD COLUMN c INT", SimpleNamespace(isddl=False, compiled=None)
        ) is None


class _JobsCursor:
    """A cursor answering SHOW ALTER TABLE COLUMN with a job in the next state of ``states`` at each poll.

    The job is on the table the statement names. The last state repeats, and SHOW ALTER TABLE OPTIMIZE and ROLLUP
    list no job.
    """

    description = (("JobId",), ("TableName",), ("State",), ("Msg",))

    def __init__(self, *states):
        self.states = list(states)
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)

    def fetchall(self):
        if "ALTER TABLE COLUMN" not in self.statements[-1]:
            return []
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        table_name = re.search(r"TableName='([^']*)'", self.statements[-1]).group(1)
        return [("12345", table_name, state, "boom" if state == "CANCELLED" else "")]

    def close(self):
        pass


class _PollRecorder(logging.Handler):
    def __init__(self):
        super().__init__(logging.INFO)
        self.levels = []
        self.warnings = []

    def emit(self, record):
        if record.getMessage().startswith("Waiting for the COLUMN job 12345"):
            self.levels.append(record.levelno)
        elif record.levelno == logging.WARNING:
            self.warnings.append(record.getMessage())


@contextmanager
def _recording_dialect_logs():
    recorder = _PollRecorder()
    dialect_logger = logging.getLogger("starrocks.dialect")
    dialect_logger.addHandler(recorder)
    level = dialect_logger.level
    dialect_logger.setLevel(logging.INFO)
    try:
        yield recorder
    finally:
        dialect_logger.removeHandler(recorder)
        dialect_logger.setLevel(level)


class SchemaChangeWaitTest(fixtures.TestBase):
    """The dialect's wait for a running schema change is bounded, can be turned off, and warns when it lasts."""

    @staticmethod
    def _without_pauses(dialect):
        return mock.patch.object(dialect, "_pause_between_polls")

    def test_a_job_still_running_at_the_timeout_raises_naming_it(self):
        dialect = StarRocksDialect(starrocks_schema_change_timeout=0.05)
        cursor = _JobsCursor("WAITING_TXN")
        with expect_raises_message(
            SchemaChangeTimeoutError,
            r"COLUMN job 12345 on table 'db.t' is still WAITING_TXN after 0.05 seconds.* holds open on another "
            r"connection.* starrocks_schema_change_timeout.* starrocks_wait_for_schema_change=False",
        ):
            dialect.wait_while_schema_change_runs(cursor, "t", "db")

    def test_without_a_limit_the_wait_lasts_until_the_job_ends(self):
        dialect = StarRocksDialect(starrocks_schema_change_timeout=None)
        cursor = _JobsCursor("PENDING", "RUNNING", "RUNNING", "FINISHED")
        with self._without_pauses(dialect) as pause:
            dialect.wait_while_schema_change_runs(cursor, "t", None)
        eq_(pause.call_count, 3)
        eq_(len(cursor.statements), 6)

    def test_the_wait_can_be_turned_off(self):
        dialect = StarRocksDialect(starrocks_wait_for_schema_change=False)
        cursor = _JobsCursor("RUNNING")
        dialect.wait_while_schema_change_runs(cursor, "t", None)
        eq_(cursor.statements, [])

    def test_polls_log_at_warning_once_the_wait_lasts(self):
        dialect = StarRocksDialect()
        cursor = _JobsCursor(*["RUNNING"] * 7, "FINISHED")
        with _recording_dialect_logs() as recorder, self._without_pauses(dialect):
            dialect.wait_while_schema_change_runs(cursor, "t", None)
        eq_(recorder.levels, [logging.INFO] * 4 + [logging.WARNING] + [logging.INFO] * 2)

    def test_the_wait_returns_the_jobs_it_saw_end(self):
        dialect = StarRocksDialect()
        with self._without_pauses(dialect):
            jobs = dialect.wait_while_schema_change_runs(_JobsCursor("RUNNING", "CANCELLED"), "t", None)
        eq_(jobs, {"COLUMN": SchemaChangeJob("12345", "CANCELLED", "boom"), "OPTIMIZE": None, "ROLLUP": None})
        eq_(StarRocksDialect(starrocks_wait_for_schema_change=False).wait_while_schema_change_runs(None, "t", None), {})

    def test_a_rollup_job_of_the_table_is_waited_for_among_every_tables_jobs(self):
        """StarRocks before 4.1.6 lists the ROLLUP jobs of every table in the database, oldest first, whatever the
        WHERE, ORDER BY and LIMIT say (https://github.com/StarRocks/starrocks/pull/79281)."""
        polls = iter(["RUNNING", "FINISHED"])

        class RollupJobsCursor(_JobsCursor):
            def fetchall(self):
                if "ALTER TABLE ROLLUP" not in self.statements[-1]:
                    return []
                return [
                    ("100", "t", "FINISHED", ""),
                    ("101", "other", "RUNNING", ""),
                    ("102", "t", next(polls), ""),
                    ("103", "T", "RUNNING", ""),
                ]

        dialect = StarRocksDialect()
        cursor = RollupJobsCursor()
        with self._without_pauses(dialect) as pause:
            jobs = dialect.wait_while_schema_change_runs(cursor, "t", "db")
        eq_(pause.call_count, 1)
        eq_(jobs["ROLLUP"], SchemaChangeJob("102", "FINISHED", ""))
        assert "SHOW ALTER TABLE ROLLUP FROM `db` WHERE TableName='t'" in cursor.statements[-1], cursor.statements

    @testing.combinations(
        ("default", {}, 300.0, True),
        ("seconds", {"starrocks_schema_change_timeout": 60}, 60.0, True),
        ("text", {"starrocks_schema_change_timeout": "600", "starrocks_wait_for_schema_change": "false"}, 600.0, False),
        ("none", {"starrocks_schema_change_timeout": None}, None, True),
        ("none_text", {"starrocks_schema_change_timeout": "None"}, None, True),
        ("zero", {"starrocks_schema_change_timeout": 0}, None, True),
        argnames="arguments,timeout,waits",
        id_="iaaa",
    )
    def test_create_engine_passes_the_options_to_the_dialect(self, arguments, timeout, waits):
        dialect = create_engine("starrocks://root@localhost:9030/db", **arguments).dialect
        eq_((dialect.schema_change_timeout, dialect.wait_for_schema_change), (timeout, waits))

    @testing.combinations(
        ("timeout", "?starrocks_schema_change_timeout=7", 7.0, True),
        ("off", "?starrocks_wait_for_schema_change=false", 300.0, False),
        (
            "with_a_driver_argument",
            "?charset=utf8mb4&starrocks_schema_change_timeout=None&starrocks_wait_for_schema_change=true",
            None,
            True,
        ),
        ("given_twice", "?starrocks_schema_change_timeout=7&starrocks_schema_change_timeout=9", 9.0, True),
        argnames="query,timeout,waits",
        id_="iaaa",
    )
    def test_a_url_passes_the_options_to_the_dialect_and_not_to_the_driver(self, query, timeout, waits):
        """PyMySQL refuses a keyword argument it doesn't know, so the options must not reach it."""
        engine = create_engine(f"starrocks://root@localhost:9030/db{query}")
        dialect = engine.dialect
        eq_((dialect.schema_change_timeout, dialect.wait_for_schema_change), (timeout, waits))
        _, connect_args = dialect.create_connect_args(engine.url)
        eq_([name for name in connect_args if name.startswith("starrocks_")], [])

    def test_engine_from_config_passes_the_options_as_text(self):
        configuration = {
            "sqlalchemy.url": "starrocks://root@localhost:9030/db",
            "sqlalchemy.starrocks_schema_change_timeout": "45",
            "sqlalchemy.starrocks_wait_for_schema_change": "true",
        }
        dialect = engine_from_config(configuration).dialect
        eq_((dialect.schema_change_timeout, dialect.wait_for_schema_change), (45.0, True))

    @testing.combinations(
        ("negative", -1),
        ("not_a_number", float("nan")),
        ("not_a_number_text", "nan"),
        ("words", "abc"),
        ("negative_text", "-5"),
        argnames="timeout",
        id_="ia",
    )
    def test_a_timeout_that_is_not_a_length_of_time_is_refused(self, timeout):
        with expect_raises_message(exc.ArgumentError, "starrocks_schema_change_timeout must be 0 or more seconds"):
            StarRocksDialect(starrocks_schema_change_timeout=timeout)

    @testing.combinations(("nan", "nan"), ("words", "abc"), argnames="text", id_="ia")
    def test_a_url_timeout_that_is_not_a_length_of_time_is_refused(self, text):
        with expect_raises_message(exc.ArgumentError, "starrocks_schema_change_timeout must be 0 or more seconds"):
            create_engine(f"starrocks://root@localhost:9030/db?starrocks_schema_change_timeout={text}")


class _RollupFinishGapCursor:
    """A cursor whose table's newest ROLLUP job shows FINISHED, and which refuses the first ``refusals`` statements.

    StarRocks sets a ROLLUP job FINISHED before it sets the job's table back to NORMAL, and refuses changes to the
    table with ``refusal`` in between. SHOW ALTER TABLE COLUMN and OPTIMIZE list no job, and ROLLUP lists
    ``rollup_state``, or no job when it is None.
    """

    _ROLLUP_REFUSAL = (
        "A materialized view or rollup index is being created on the table events. Please wait until the current "
        "operation completes."
    )

    def __init__(self, refusals=1, refusal=_ROLLUP_REFUSAL, rollup_state="FINISHED"):
        self.refusals = refusals
        self.refusal = refusal
        self.rollup_state = rollup_state
        self.statements = []
        self.description = None

    def execute(self, statement, parameters=None):
        self.statements.append(statement)
        self.description = (("JobId",), ("TableName",), ("State",), ("Msg",)) if statement.startswith("SHOW") else None
        if not statement.startswith("SHOW") and self.refusals:
            self.refusals -= 1
            raise pymysql.err.ProgrammingError(1064, self.refusal)

    def fetchall(self):
        if "ALTER TABLE ROLLUP" not in self.statements[-1] or self.rollup_state is None:
            return []
        return [("200", "events", self.rollup_state, "")]

    def nextset(self):
        return None

    def close(self):
        pass

    def sent(self):
        return [statement for statement in self.statements if not statement.startswith("SHOW")]


class RollupFinishGapTest(fixtures.TestBase):
    """A statement StarRocks refuses straight after the wait saw its table's ROLLUP job end is sent again."""

    @staticmethod
    def _execute(cursor, path="do_execute", **options):
        """Run a CREATE INDEX through the dialect, as SQLAlchemy runs a construct, with pauses recorded, not slept."""
        dialect = create_engine("starrocks://root@localhost:9030/db", **options).dialect
        table = Table("events", MetaData(), Column("id", INTEGER), Column("city", INTEGER), schema="analytics")
        ddl = CreateIndex(Index("ix_city", table.c.city))
        statement = str(ddl.compile(dialect=dialect))
        context = SimpleNamespace(
            isddl=True,
            compiled=SimpleNamespace(statement=ddl),
            execution_options={},
            root_connection=SimpleNamespace(connection=SimpleNamespace(cursor=lambda: cursor)),
        )
        with mock.patch.object(dialect, "_pause_between_polls") as pause:
            if path == "do_execute":
                dialect.do_execute(cursor, statement, (), context)
            else:
                dialect.do_execute_no_params(cursor, statement, context)
        return [call.args[1] for call in pause.call_args_list]

    @testing.combinations(("do_execute",), ("do_execute_no_params",), argnames="path")
    def test_the_rollup_refusal_after_the_job_ended_is_resent(self, path):
        cursor = _RollupFinishGapCursor(refusals=2)
        pauses = self._execute(cursor, path)
        eq_(len(cursor.sent()), 3)
        eq_(pauses, [0.05, 0.1])

    def test_a_cancelled_rollup_job_leaves_the_same_gap(self):
        cursor = _RollupFinishGapCursor(rollup_state="CANCELLED")
        eq_(self._execute(cursor), [0.05])
        eq_(len(cursor.sent()), 2)

    def test_pauses_double_up_to_the_poll_interval(self):
        cursor = _RollupFinishGapCursor(refusals=7)
        eq_(self._execute(cursor), [0.05, 0.1, 0.2, 0.4, 0.8, 1.0, 1.0])

    @testing.combinations(
        ("no_rollup_job", {"rollup_state": None}, {}),
        ("another_refusal", {"refusal": "A schema change operation is in progress on the table events."}, {}),
        ("wait_off", {}, {"starrocks_wait_for_schema_change": False}),
        ("past_the_timeout", {}, {"starrocks_schema_change_timeout": 0.01}),
        argnames="cursor_arguments,options",
        id_="iaa",
    )
    def test_the_statement_is_not_resent(self, cursor_arguments, options):
        """Without a ROLLUP job seen on the table, the refusal is StarRocks' answer, and so is any other error."""
        cursor = _RollupFinishGapCursor(**cursor_arguments)
        with expect_raises_message(pymysql.err.ProgrammingError, cursor.refusal):
            self._execute(cursor, **options)
        eq_(len(cursor.sent()), 1)

    def test_a_statement_the_wait_does_not_cover_is_not_resent(self):
        dialect = create_engine("starrocks://root@localhost:9030/db").dialect
        cursor = _RollupFinishGapCursor()
        with expect_raises_message(pymysql.err.ProgrammingError, cursor.refusal):
            dialect.do_execute(cursor, "ALTER TABLE events ADD COLUMN c INT", (), None)
        eq_(cursor.statements, ["ALTER TABLE events ADD COLUMN c INT"])


class CancelledIndexJobTest(fixtures.TestBase):
    """A CREATE INDEX or DROP INDEX whose job StarRocks cancelled did nothing, which the dialect logs as a warning."""

    @staticmethod
    def _wait_after(ddl, cursor, jobs_before):
        context = SimpleNamespace(
            isddl=True,
            compiled=SimpleNamespace(statement=ddl),
            execution_options={},
            root_connection=SimpleNamespace(connection=SimpleNamespace(cursor=lambda: cursor)),
        )
        dialect = StarRocksDialect()
        with _recording_dialect_logs() as recorder, mock.patch.object(dialect, "_pause_between_polls"):
            dialect._wait_for_index_job(str(ddl.compile(dialect=dialect)), context, jobs_before)
        return recorder.warnings

    @staticmethod
    def _index():
        table = Table("events", MetaData(), Column("id", INTEGER), Column("city", INTEGER), schema="analytics")
        return Index("ix_city", table.c.city)

    @testing.combinations((CreateIndex, "was not built"), (DropIndex, "was not dropped"), argnames="ddl,outcome")
    def test_a_cancelled_index_job_is_logged_with_the_index_the_table_and_the_job(self, ddl, outcome):
        warnings = self._wait_after(
            ddl(self._index()), _JobsCursor("RUNNING", "CANCELLED"), {"COLUMN": SchemaChangeJob("12344", "FINISHED", "")}
        )
        eq_(
            warnings,
            [
                "StarRocks cancelled the COLUMN job 12345 of index 'ix_city' on table 'analytics.events', "
                f"so the index {outcome}: boom"
            ],
        )

    def test_a_table_without_a_job_before_counts_the_cancelled_job_as_the_statements(self):
        eq_(len(self._wait_after(CreateIndex(self._index()), _JobsCursor("CANCELLED"), {"COLUMN": None})), 1)

    def test_an_older_cancelled_job_is_not_the_statements(self):
        earlier = {"COLUMN": SchemaChangeJob("12345", "CANCELLED", "boom")}
        eq_(self._wait_after(CreateIndex(self._index()), _JobsCursor("CANCELLED"), earlier), [])

    def test_a_finished_index_job_is_not_logged(self):
        eq_(self._wait_after(CreateIndex(self._index()), _JobsCursor("RUNNING", "FINISHED"), {"COLUMN": None}), [])
