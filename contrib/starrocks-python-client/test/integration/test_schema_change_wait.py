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

"""Integration tests for ``starrocks_wait_for_schema_change`` polling.

These cover the identifier handling of the ``SHOW ALTER TABLE COLUMN FROM
<schema>`` polling query built in :meth:`StarRocksImpl._wait_for_schema_change`
(``starrocks/alembic/starrocks.py``):

* a database name containing the special characters StarRocks *does* allow must
  round-trip through the polling query;
* a database name containing a backtick -- the case the hand-built ``FROM
  `...` `` fragment would mis-render -- cannot be created in the first place,
  because ``FeNameFormat.DB_NAME_REGEX`` only admits ``[A-Za-z_]``, digits,
  underscore and ``- ~ ! @ # $ % ^ & < > = +``.

Database names include the test database's name, so parallel workers each get their own.

``DialectWaitTest`` covers the dialect's own wait before each ALTER TABLE, CREATE INDEX and DROP INDEX, and after
each CREATE INDEX and DROP INDEX: its ``starrocks_schema_change_timeout`` and ``starrocks_wait_for_schema_change``
engine options.
"""

import logging
import threading
import time
from unittest.mock import patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Column, Index, String, Table, create_engine, inspect, testing
from sqlalchemy.exc import DatabaseError
from sqlalchemy.testing import config, engines, expect_warnings, fixtures
from sqlalchemy.testing.assertions import eq_, expect_raises_message

from starrocks import INTEGER
from starrocks.alembic.starrocks import StarRocksImpl
from starrocks.dialect import SchemaChangeTimeoutError, StarRocksDialect
from test.util import TRANSACTIONS_NEED_3_5


WAIT_OPTIONS = {
    "starrocks_wait_for_schema_change": True,
    "starrocks_schema_change_timeout": 120,
    "starrocks_schema_change_poll_interval": 1.0,
}


def _special_schema() -> str:
    """Legal under DB_NAME_REGEX (starts with a letter; '-', '!' and '$' are in
    SPECIAL_CHARACTERS_IN_DB_NAME), and all three require identifier quoting."""
    return f"sr_wait-{config.db.url.database}!db$x"


def _backtick_schema() -> str:
    """Illegal: a backtick is not in SPECIAL_CHARACTERS_IN_DB_NAME. This is the name
    shape the review comment assumes is reachable."""
    return f"sr_bt`{config.db.url.database}"


class SchemaChangeWaitQuotingTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    @classmethod
    def setup_test_class(cls):
        cls.schema = _special_schema()
        with config.db.begin() as conn:
            conn.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS `{cls.schema}`")

    @classmethod
    def teardown_test_class(cls):
        with config.db.begin() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS `{cls.schema}` FORCE")

    def _create_table(self, conn, name: str) -> None:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS `{self.schema}`.`{name}`")
        conn.exec_driver_sql(
            f"CREATE TABLE `{self.schema}`.`{name}` (k INT, v INT) "
            "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
            "PROPERTIES ('replication_num' = '1')"
        )

    def _column_names(self, conn, table: str) -> list[str]:
        return [
            row[0]
            for row in conn.exec_driver_sql(
                "SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{self.schema}' AND table_name = '{table}'"
            )
        ]

    def test_wait_polls_a_schema_that_needs_quoting(self, connection):
        """The polling query must work for a legal db name that needs quoting.

        With ``starrocks_wait_for_schema_change=True`` the ADD COLUMN is
        followed by ``SHOW ALTER TABLE COLUMN FROM <schema> ...``; if that
        fragment is mis-rendered the migration raises after the DDL has already
        been applied. Afterwards the wait has returned and the column is really there.
        """
        table = "t_special_schema"
        self._create_table(connection, table)

        op = Operations(MigrationContext.configure(connection=connection, opts=WAIT_OPTIONS))
        op.add_column(table, Column("added", INTEGER), schema=self.schema)

        assert "added" in self._column_names(connection, table)

    def test_wait_polls_repeated_changes_on_a_quoted_schema(self, connection):
        """Two consecutive column changes: the second poll must also resolve.

        Exercises the ORDER BY JobId path (both jobs live under the same
        quoted schema) in addition to the identifier rendering.
        """
        table = "t_special_schema_twice"
        self._create_table(connection, table)

        op = Operations(MigrationContext.configure(connection=connection, opts=WAIT_OPTIONS))
        op.add_column(table, Column("a1", INTEGER), schema=self.schema)
        op.add_column(table, Column("a2", INTEGER), schema=self.schema)

        assert {"a1", "a2"} <= set(self._column_names(connection, table))

    def test_polling_query_renders_for_a_schema_needing_quotes(self, connection):
        """The exact polling statement the impl builds must parse and run.

        Guards the ``FROM <schema>`` fragment directly, independently of whether
        a schema change happens to be in flight. The test adds a column first, so a
        job row exists whichever worker runs it and in whatever order.
        """
        table = "t_special_schema_query"
        self._create_table(connection, table)
        Operations(MigrationContext.configure(connection=connection, opts=WAIT_OPTIONS)).add_column(
            table, Column("added", INTEGER), schema=self.schema
        )

        query = (
            f"SHOW ALTER TABLE COLUMN FROM `{self.schema}` "
            f"WHERE TableName = '{table}' ORDER BY JobId DESC LIMIT 1"
        )
        rows = connection.exec_driver_sql(query).mappings().all()
        assert rows, "expected a schema-change job row for the table"
        assert str(rows[0]["State"]).upper() == "FINISHED"

    def test_wait_path_is_actually_exercised(self, connection):
        """Sanity check: the impl is StarRocks' and the wait hook really fires.

        Without this the quoting tests above could pass while never reaching
        ``_wait_for_schema_change`` at all.
        """
        table = "t_wait_hook"
        calls: list = []
        self._create_table(connection, table)
        ctx = MigrationContext.configure(connection=connection, opts=WAIT_OPTIONS)
        assert isinstance(ctx.impl, StarRocksImpl)

        real = ctx.impl._wait_for_schema_change

        def spy(table_name, schema, **kwargs):
            calls.append((table_name, schema))
            return real(table_name, schema, **kwargs)

        with patch.object(ctx.impl, "_wait_for_schema_change", spy):
            Operations(ctx).add_column(table, Column("added", INTEGER), schema=self.schema)

        assert calls == [(table, self.schema)]

    def test_backtick_database_name_cannot_exist(self, connection):
        """A db name containing a backtick is unreachable on StarRocks.

        Either ``CREATE DATABASE`` errors, or the backtick is dropped from the
        stored name -- so ``_wait_for_schema_change`` can never be handed a
        schema whose backticks would break its ``FROM `...` `` fragment.
        """
        backtick_schema = _backtick_schema()
        try:
            connection.exec_driver_sql(f"CREATE DATABASE `{backtick_schema.replace('`', '``')}`")
        except DatabaseError:
            pass

        stored_prefix, _, stored_suffix = backtick_schema.partition("`")
        found = [
            row[0]
            for row in connection.exec_driver_sql("SHOW DATABASES")
            if row[0].startswith(stored_prefix) and row[0].endswith(stored_suffix)
        ]
        try:
            assert all("`" not in name for name in found), found
        finally:
            for name in found:
                connection.exec_driver_sql(f"DROP DATABASE IF EXISTS `{name}` FORCE")


class SequentialSchemaChangeTest(fixtures.TestBase):
    """Operations that start schema-change jobs run one after another, each waiting for the job before it."""

    __backend__ = True
    __only_on__ = "starrocks"

    @staticmethod
    def _operations(connection) -> Operations:
        return Operations(MigrationContext.configure(connection=connection, opts=WAIT_OPTIONS))

    @staticmethod
    def _create_table(connection, name: str) -> None:
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
        connection.exec_driver_sql(
            f"CREATE TABLE {name} (k INT, v VARCHAR(10)) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
            "PROPERTIES ('replication_num' = '1')"
        )

    @testing.combinations(
        ("bloom_first", {"bloom_filter_columns": "v", "in_memory": "false"}),
        ("bloom_last", {"in_memory": "false", "bloom_filter_columns": "v"}),
        argnames="properties",
        id_="ia",
    )
    def test_properties_that_start_a_job_do_not_fail_the_next(self, connection, properties) -> None:
        """One SET per property: a packet of several fails with "A schema change operation is in progress"."""
        table = "t_wait_properties"
        self._create_table(connection, table)
        try:
            self._operations(connection).alter_table_properties(table, properties)

            ddl = connection.exec_driver_sql(f"SHOW CREATE TABLE {table}").fetchone()[1]
            assert '"bloom_filter_columns" = "v"' in ddl
        finally:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {table}")

    def test_distribution_then_order_wait_for_their_jobs(self, connection) -> None:
        """A distribution change is an OPTIMIZE job and a sort key change a COLUMN one; the next ALTER waits for each."""
        table = "t_wait_optimize"
        self._create_table(connection, table)
        try:
            operations = self._operations(connection)
            operations.alter_table_distribution(table, "HASH(k)", buckets=2)
            operations.alter_table_distribution(table, "HASH(k)", buckets=3)
            operations.alter_table_order(table, "v")

            ddl = connection.exec_driver_sql(f"SHOW CREATE TABLE {table}").fetchone()[1]
            assert "BUCKETS 3" in ddl
            assert "ORDER BY(`v`)" in ddl
        finally:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {table}")


class DialectWaitTest(fixtures.TestBase):
    """The dialect waits for a running schema change, and for an index's job, within a bound, unless turned off."""

    __backend__ = True
    __only_on__ = "starrocks"

    @staticmethod
    def _table(metadata, name):
        return Table(
            name,
            metadata,
            Column("id", INTEGER),
            Column("v", INTEGER),
            Column("city", String(20)),
            Column("country", String(20)),
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
        )

    @TRANSACTIONS_NEED_3_5
    def test_a_job_waiting_on_an_open_transaction_times_out_naming_it(self, metadata):
        """At the default the CREATE INDEX would wait 300 seconds, and without a limit as long as the transaction."""
        table = self._table(metadata, "t_wait_txn")
        metadata.create_all(config.db)
        holder_engine = engines.testing_engine(options={"isolation_level": "READ COMMITTED"})
        engine = engines.testing_engine(options={"starrocks_schema_change_timeout": 3})
        with holder_engine.connect() as holder, engine.connect() as conn:
            transaction = holder.begin()
            try:
                holder.execute(table.insert(), {"id": 1, "v": 1, "city": "a", "country": "b"})
                with conn.begin():
                    conn.exec_driver_sql("ALTER TABLE t_wait_txn MODIFY COLUMN v BIGINT")
                    with expect_raises_message(
                        SchemaChangeTimeoutError,
                        r"COLUMN job \d+ on table '.*t_wait_txn' is still (PENDING|WAITING_TXN) after 3 seconds",
                    ):
                        Index("ix_wait_txn_city", table.c.city, starrocks_using="BITMAP").create(conn)
            finally:
                transaction.rollback()
            eq_(conn.exec_driver_sql("SHOW INDEX FROM t_wait_txn").all(), [])
            conn.exec_driver_sql("CANCEL ALTER TABLE COLUMN FROM t_wait_txn")

    def test_the_options_can_be_given_in_the_url(self):
        """As in ``alembic.ini``'s ``sqlalchemy.url``; the driver refuses a keyword argument it doesn't know."""
        url = config.db.url.update_query_dict(
            {"starrocks_schema_change_timeout": "7", "starrocks_wait_for_schema_change": "true"}
        )
        engine = engines.testing_engine(url=url)
        with engine.connect() as conn:
            eq_(conn.exec_driver_sql("SELECT 1").scalar(), 1)
        eq_((engine.dialect.schema_change_timeout, engine.dialect.wait_for_schema_change), (7.0, True))

    def test_without_the_wait_starrocks_refuses_the_second_index(self, metadata):
        """As before the dialect waited: StarRocks builds the first index while the second CREATE INDEX arrives."""
        table = self._table(metadata, "t_wait_off")
        Index("ix_wait_off_city", table.c.city, starrocks_using="BITMAP")
        Index("ix_wait_off_country", table.c.country, starrocks_using="BITMAP")
        engine = engines.testing_engine(options={"starrocks_wait_for_schema_change": False})
        with expect_raises_message(DatabaseError, "A schema change operation is in progress"):
            metadata.create_all(engine)

    def test_create_all_returns_once_the_index_is_built(self, metadata):
        """StarRocks lists an index only once its job has ended; an inspector straight after used to miss it."""
        table = self._table(metadata, "t_wait_index")
        Index("ix_wait_index_city", table.c.city, starrocks_using="BITMAP")
        with config.db.begin() as conn:
            metadata.create_all(conn)
            eq_([index["name"] for index in inspect(conn).get_indexes("t_wait_index")], ["ix_wait_index_city"])

    def test_create_index_waits_for_the_rollup_job_of_its_table(self, metadata):
        """StarRocks refuses an index while a ROLLUP job builds on the table.

        Before 4.1.6, SHOW ALTER TABLE ROLLUP lists the jobs of every table in the database, oldest first, so the
        ended job of another table comes first.
        """
        self._table(metadata, "t_wait_rollup_other")
        table = self._table(metadata, "t_wait_rollup")
        metadata.create_all(config.db)
        with config.db.begin() as conn:
            conn.exec_driver_sql("ALTER TABLE t_wait_rollup_other ADD ROLLUP r_wait_rollup_other (id, v)")
            cursor = conn.connection.cursor()
            config.db.dialect.wait_while_schema_change_runs(cursor, "t_wait_rollup_other", None)
            cursor.close()
            conn.exec_driver_sql("ALTER TABLE t_wait_rollup ADD ROLLUP r_wait_rollup (id, v)")
            Index("ix_wait_rollup_city", table.c.city, starrocks_using="BITMAP").create(conn)
            eq_([row[2] for row in conn.exec_driver_sql("SHOW INDEX FROM t_wait_rollup")], ["ix_wait_rollup_city"])

    def test_add_column_waits_for_a_synchronous_materialized_view(self, metadata):
        """StarRocks builds a synchronous materialized view in a ROLLUP job of its base table."""
        self._table(metadata, "t_wait_sync_mv")
        metadata.create_all(config.db)
        with config.db.begin() as conn:
            conn.exec_driver_sql("CREATE MATERIALIZED VIEW mv_wait_sync AS SELECT id, SUM(v) FROM t_wait_sync_mv GROUP BY id")
            Operations(MigrationContext.configure(connection=conn)).add_column("t_wait_sync_mv", Column("w", INTEGER))
            columns = [row[0] for row in conn.exec_driver_sql("SHOW COLUMNS FROM t_wait_sync_mv")]
        eq_(columns, ["id", "v", "city", "country", "w"])

    @testing.combinations(("index",), ("add_column",), argnames="change")
    def test_a_change_straight_after_a_rollup_job_ends_waits_until_starrocks_frees_the_table(self, metadata, change):
        """StarRocks shows a ROLLUP job FINISHED up to about 60 ms before it sets the table back to NORMAL, and
        refuses changes to the table in between. Polling every 5 ms, the wait ends inside that gap nearly every time.
        """
        name = f"t_wait_gap_{change}"
        table = self._table(metadata, name)
        metadata.create_all(config.db)
        with patch.object(StarRocksDialect, "_SCHEMA_CHANGE_POLL_SECONDS", 0.005), config.db.begin() as conn:
            conn.exec_driver_sql(f"ALTER TABLE {name} ADD ROLLUP r_{name} (id, v)")
            if change == "index":
                Index(f"ix_{name}_city", table.c.city, starrocks_using="BITMAP").create(conn)
            else:
                Operations(MigrationContext.configure(connection=conn)).add_column(name, Column("w", INTEGER))
            indexes = [row[2] for row in conn.exec_driver_sql(f"SHOW INDEX FROM {name}")]
            columns = [row[0] for row in conn.exec_driver_sql(f"SHOW COLUMNS FROM {name}")]
        eq_((indexes, columns[-1]), ([f"ix_{name}_city"], "country") if change == "index" else ([], "w"))

    def test_drop_index_returns_once_the_index_is_gone(self, metadata):
        table = self._table(metadata, "t_wait_drop_index")
        index = Index("ix_wait_drop_index_city", table.c.city, starrocks_using="BITMAP")
        with config.db.begin() as conn:
            metadata.create_all(conn)
            index.drop(conn)
            eq_(conn.exec_driver_sql("SHOW INDEX FROM t_wait_drop_index").all(), [])

    @TRANSACTIONS_NEED_3_5
    def test_an_index_job_still_waiting_at_the_timeout_warns(self, metadata):
        """The CREATE INDEX has run, so its job goes on, and builds the index once the transaction ends."""
        table = self._table(metadata, "t_wait_index_txn")
        metadata.create_all(config.db)
        holder_engine = engines.testing_engine(options={"isolation_level": "READ COMMITTED"})
        engine = engines.testing_engine(options={"starrocks_schema_change_timeout": 3})
        with holder_engine.connect() as holder, engine.connect() as conn:
            transaction = holder.begin()
            try:
                holder.execute(table.insert(), {"id": 1, "v": 1, "city": "a", "country": "b"})
                with conn.begin():
                    with expect_warnings(
                        r"StarRocks' COLUMN job \d+ on table '.*t_wait_index_txn' is still (PENDING|WAITING_TXN) after 3 "
                        r"seconds.* The statement has run"
                    ):
                        Index("ix_wait_index_txn_city", table.c.city, starrocks_using="BITMAP").create(conn)
            finally:
                transaction.rollback()
        with config.db.connect() as conn:
            cursor = conn.connection.cursor()
            config.db.dialect.wait_while_schema_change_runs(cursor, "t_wait_index_txn", None)
            cursor.close()
            eq_([row[2] for row in conn.exec_driver_sql("SHOW INDEX FROM t_wait_index_txn")], ["ix_wait_index_txn_city"])

    def test_an_alembic_alter_under_a_schema_translate_map_waits_on_the_table_it_names(self):
        """Alembic writes a string schema into its ALTER as given, so the map doesn't reach it, nor the wait."""
        with config.db.begin() as conn:
            conn.exec_driver_sql("DROP TABLE IF EXISTS t_wait_translated")
            conn.exec_driver_sql(
                "CREATE TABLE t_wait_translated (k INT, v INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                "PROPERTIES ('replication_num' = '1')"
            )
        try:
            with config.db.begin() as conn:
                conn.exec_driver_sql("ALTER TABLE t_wait_translated MODIFY COLUMN v BIGINT")
                translated = conn.execution_options(schema_translate_map={None: config.test_schema})
                Operations(MigrationContext.configure(connection=translated)).add_column(
                    "t_wait_translated", Column("w", INTEGER)
                )
            with config.db.connect() as conn:
                columns = conn.exec_driver_sql("SHOW COLUMNS FROM t_wait_translated").all()
            eq_([row[0] for row in columns], ["k", "v", "w"])
        finally:
            with config.db.begin() as conn:
                conn.exec_driver_sql("DROP TABLE IF EXISTS t_wait_translated")

    @TRANSACTIONS_NEED_3_5
    def test_a_cancelled_index_job_is_logged(self, metadata):
        """The index job waits on a transaction writing the table, and another connection cancels it meanwhile."""
        table = self._table(metadata, "t_wait_cancelled")
        metadata.create_all(config.db)
        canceller_engine = create_engine(config.db.url.set(drivername="starrocks"))
        holder_engine = engines.testing_engine(options={"isolation_level": "READ COMMITTED"})

        def cancel_the_index_job():
            time.sleep(2)
            with canceller_engine.connect() as conn:
                conn.exec_driver_sql("CANCEL ALTER TABLE COLUMN FROM t_wait_cancelled")

        warnings = []
        recorder = logging.Handler(logging.WARNING)
        recorder.emit = lambda record: warnings.append(record.getMessage())
        dialect_logger = logging.getLogger("starrocks.dialect")
        dialect_logger.addHandler(recorder)
        try:
            with holder_engine.connect() as holder:
                transaction = holder.begin()
                try:
                    holder.execute(table.insert(), {"id": 1, "v": 1, "city": "a", "country": "b"})
                    canceller = threading.Thread(target=cancel_the_index_job)
                    canceller.start()
                    with config.db.begin() as conn:
                        Index("ix_wait_cancelled_city", table.c.city, starrocks_using="BITMAP").create(conn)
                    canceller.join()
                finally:
                    transaction.rollback()
        finally:
            dialect_logger.removeHandler(recorder)
            canceller_engine.dispose()
        cancelled = [w for w in warnings if w.startswith("StarRocks cancelled the COLUMN job")]
        eq_(len(cancelled), 1, warnings)
        assert "of index 'ix_wait_cancelled_city' on table" in cancelled[0], cancelled
        assert "t_wait_cancelled', so the index was not built" in cancelled[0], cancelled
        with config.db.connect() as conn:
            eq_(conn.exec_driver_sql("SHOW INDEX FROM t_wait_cancelled").all(), [])
