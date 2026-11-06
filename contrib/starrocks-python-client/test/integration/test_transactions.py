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

"""Transactions: connections autocommit by default, and ``READ COMMITTED`` runs DML in StarRocks transactions."""

import os
import uuid
import warnings

import pymysql
from sqlalchemy import MetaData, Table, delete, exc, insert, literal, select, testing, update
from sqlalchemy.testing import config, engines, expect_warnings, fixtures
from sqlalchemy.testing.assertions import eq_, expect_raises, expect_raises_message
from sqlalchemy.testing.schema import Column

from starrocks import INTEGER
from test.util import TRANSACTIONS_NEED_3_5, run_coroutine


KEYED_DELETE_IN_TRANSACTION_NEEDS_4_1 = testing.skip_if(
    "starrocks<4.1",
    "StarRocks 3.5 refuses UPDATE and DELETE inside a transaction, and documents them for shared-data clusters "
    "from 4.0; 4.1 shared-nothing allows them too",
)
TRANSACTIONS_REFUSED_BEFORE_3_5 = testing.only_if(
    "starrocks<3.5", "StarRocks has transactions from 3.5, so they are refused only before it"
)
SQL_TRANSACTION_SWITCH_NEEDS_4_0_7 = testing.skip_if(
    "starrocks<4.0.7",
    "enable_sql_transaction is a session variable from StarRocks 4.0.7 and 4.1.0; 3.5 and 4.0.0 to 4.0.6 have no such "
    "variable and always run transactions",
)


class _RollBack(Exception):
    """Raised inside a transaction block to roll it back."""


class TransactionTest(fixtures.TablesTest):
    __backend__ = True
    __only_on__ = "starrocks"

    run_deletes = "each"

    @classmethod
    def define_tables(cls, metadata):
        Table("txn_keyed", metadata, Column("id", INTEGER, primary_key=True, autoincrement=False), Column("v", INTEGER))
        Table("txn_logged", metadata, Column("id", INTEGER), Column("v", INTEGER), starrocks_duplicate_key="id")
        Table(
            "txn_translated",
            metadata,
            Column("id", INTEGER),
            Column("v", INTEGER),
            starrocks_duplicate_key="id",
            schema=config.test_schema,
        )

    @staticmethod
    def _read_committed_engine():
        return engines.testing_engine(options={"isolation_level": "READ COMMITTED"})

    def _ids(self, table_name):
        with config.db.connect() as conn:
            return sorted(row.id for row in conn.execute(select(self.tables[table_name].c.id)))

    def _insert_logged_rows(self):
        with config.db.begin() as conn:
            conn.execute(insert(self.tables.txn_logged), [{"id": 1, "v": 1}, {"id": 2, "v": 2}])

    def test_connections_autocommit_by_default(self):
        with config.db.connect() as conn:
            eq_(conn.get_isolation_level(), "AUTOCOMMIT")
        eq_(config.db.dialect.default_isolation_level, "AUTOCOMMIT")

    def test_rollback_under_autocommit_keeps_the_rows(self):
        with expect_raises(_RollBack):
            with config.db.begin() as conn:
                conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
                raise _RollBack()
        eq_(self._ids("txn_keyed"), [1])

    @TRANSACTIONS_NEED_3_5
    def test_read_committed_execution_option_is_reset_when_the_connection_returns(self):
        engine = engines.testing_engine()
        with engine.connect() as conn:
            conn.execution_options(isolation_level="READ COMMITTED")
            eq_(conn.get_isolation_level(), "READ COMMITTED")
            dbapi_connection = conn.connection.dbapi_connection
        with engine.connect() as conn:
            assert conn.connection.dbapi_connection is dbapi_connection
            eq_(conn.get_isolation_level(), "AUTOCOMMIT")

    def _rollback_keeps_the_insert(self, engine):
        with expect_raises(_RollBack):
            with engine.begin() as conn:
                eq_(conn.get_isolation_level(), "AUTOCOMMIT")
                conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
                raise _RollBack()
        eq_(self._ids("txn_keyed"), [1])

    @testing.combinations(("SERIALIZABLE",), ("REPEATABLE READ",), ("READ UNCOMMITTED",), argnames="level")
    def test_a_mysql_isolation_level_runs_as_autocommit_with_a_future_warning(self, level):
        """Earlier versions of the dialect accepted these levels, and StarRocks committed each statement under them."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self._rollback_keeps_the_insert(engines.testing_engine(options={"isolation_level": level}))
        assert any(w.category is FutureWarning and repr(level) in str(w.message) for w in caught), caught

    @testing.combinations(("SERIALIZABLE",), ("REPEATABLE READ",), ("READ UNCOMMITTED",), argnames="level")
    def test_a_connection_level_change_on_a_mysql_level_engine_returns_to_the_pool(self, level):
        """SQLAlchemy 2.0 checks the engine's level when the pool resets a connection whose level was changed."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            engine = engines.testing_engine(options={"isolation_level": level, "pool_size": 1})
            for _ in range(2):
                with engine.connect() as conn:
                    conn.execution_options(isolation_level="AUTOCOMMIT")
                    conn.exec_driver_sql("SELECT 1")
                eq_(engine.pool.checkedout(), 0)
        eq_(len([w for w in caught if w.category is FutureWarning]), 1, caught)

    @TRANSACTIONS_NEED_3_5
    def test_a_read_committed_connection_on_a_mysql_level_engine_returns_to_the_pool(self):
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            engine = engines.testing_engine(options={"isolation_level": "REPEATABLE READ", "pool_size": 1})
        with expect_raises(_RollBack):
            with engine.connect() as conn:
                conn.execution_options(isolation_level="READ COMMITTED")
                with conn.begin():
                    conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
                    raise _RollBack()
        eq_(engine.pool.checkedout(), 0)
        eq_(self._ids("txn_keyed"), [])
        with engine.connect() as conn:
            eq_(conn.get_isolation_level(), "AUTOCOMMIT")

    def test_alembic_autocommit_block_on_a_mysql_level_engine_returns_the_connection(self):
        from alembic.migration import MigrationContext

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            engine = engines.testing_engine(options={"isolation_level": "REPEATABLE READ", "pool_size": 1})
        with engine.connect() as conn:
            context = MigrationContext.configure(conn)
            with context.begin_transaction():
                with context.autocommit_block():
                    context.connection.exec_driver_sql("SELECT 1")
        eq_(engine.pool.checkedout(), 0)

    def test_a_mysql_isolation_level_execution_option_runs_as_autocommit(self):
        """The FutureWarning names the line that gave the level, so Python shows it once for each such line."""
        with config.db.connect() as conn:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                conn.execution_options(isolation_level="SERIALIZABLE")
            eq_(conn.get_isolation_level(), "AUTOCOMMIT")
        eq_([os.path.normcase(w.filename) for w in caught if w.category is FutureWarning], [os.path.normcase(__file__)])

    @testing.combinations(("create_engine",), ("engine_execution_options",), argnames="given_to")
    def test_a_mysql_isolation_level_among_an_engines_execution_options_warns_once(self, given_to):
        """SQLAlchemy sets such a level on each Connection the engine makes, so the dialect replaces it once."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            if given_to == "create_engine":
                engine = engines.testing_engine(options={"execution_options": {"isolation_level": "REPEATABLE READ"}})
            else:
                engine = engines.testing_engine().execution_options(isolation_level="REPEATABLE READ")
            for _ in range(3):
                with engine.connect() as conn:
                    eq_(conn.get_isolation_level(), "AUTOCOMMIT")
        eq_([os.path.normcase(w.filename) for w in caught if w.category is FutureWarning], [os.path.normcase(__file__)])
        self._rollback_keeps_the_insert(engine)

    def test_a_url_turning_autocommit_off_connects_as_autocommit(self):
        """The URL's text reaches the driver, which takes any text as on, as before the dialect had transactions."""
        self._rollback_keeps_the_insert(engines.testing_engine(url=config.db.url.update_query_dict({"autocommit": "false"})))

    @staticmethod
    def _connect_with_pymysql():
        """Connect as ``creator`` does, with PyMySQL's default autocommit, which is off."""
        url = config.db.url
        return pymysql.connect(
            host=url.host, port=url.port, user=url.username, password=url.password or "", database=url.database
        )

    def _engine_made_by(self, made_by, **options):
        if made_by == "creator":
            return engines.testing_engine(
                url=config.db.url.set(drivername="starrocks"), options={"creator": self._connect_with_pymysql, **options}
            )
        return engines.testing_engine(options=options)

    @testing.combinations(({"autocommit": False},), ({"autocommit": 0},), argnames="connect_args")
    def test_connect_args_turning_autocommit_off_connect_as_autocommit(self, connect_args):
        """The driver's autocommit off once meant REPEATABLE READ, which StarRocks ran as AUTOCOMMIT."""
        self._rollback_keeps_the_insert(engines.testing_engine(options={"connect_args": connect_args}))

    def test_a_creator_connection_autocommits(self):
        """PyMySQL starts a connection with autocommit off, which must not make it READ COMMITTED."""
        self._rollback_keeps_the_insert(self._engine_made_by("creator"))

    @testing.requires.sqlalchemy_2
    def test_an_async_creator_connection_autocommits(self):
        """asyncmy starts a connection with autocommit off, which must not make it READ COMMITTED."""
        import asyncmy
        from sqlalchemy.ext.asyncio import create_async_engine

        url = config.db.url
        engine = create_async_engine(
            url.set(drivername="starrocks+asyncmy"),
            async_creator=lambda: asyncmy.connect(
                host=url.host, port=url.port, user=url.username, password=url.password or "", database=url.database
            ),
        )

        async def insert_and_roll_back():
            try:
                async with engine.connect() as conn:
                    level = await conn.get_isolation_level()
                    await conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
                    await conn.rollback()
                return level
            finally:
                await engine.dispose()

        eq_(run_coroutine(insert_and_roll_back()), "AUTOCOMMIT")
        eq_(self._ids("txn_keyed"), [1])

    @TRANSACTIONS_NEED_3_5
    @testing.combinations(("url",), ("creator",), argnames="made_by")
    @testing.combinations(("engine",), ("engine_execution_options",), ("connection",), argnames="level_set_on")
    def test_read_committed_rolls_back_wherever_it_is_set(self, made_by, level_set_on):
        """Each new connection starts in AUTOCOMMIT, and READ COMMITTED, set after that, turns transactions on."""
        if level_set_on == "engine":
            engine = self._engine_made_by(made_by, isolation_level="READ COMMITTED")
        else:
            engine = self._engine_made_by(made_by)
            if level_set_on == "engine_execution_options":
                engine = engine.execution_options(isolation_level="READ COMMITTED")
        with engine.connect() as conn:
            if level_set_on == "connection":
                conn.execution_options(isolation_level="READ COMMITTED")
            eq_(conn.get_isolation_level(), "READ COMMITTED")
            transaction = conn.begin()
            conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
            transaction.rollback()
        eq_(self._ids("txn_keyed"), [])

    @TRANSACTIONS_NEED_3_5
    @testing.combinations(("connection",), ("engine_execution_options",), argnames="level_set_on")
    def test_a_read_committed_connection_that_reconnects_warns_that_each_statement_commits(self, level_set_on):
        """SQLAlchemy doesn't set the Connection's level on the DBAPI connection that replaces one after a
        disconnect or ``invalidate()``, and a new DBAPI connection starts in AUTOCOMMIT, so StarRocks keeps the row.
        """
        engine = engines.testing_engine()
        if level_set_on == "engine_execution_options":
            engine = engine.execution_options(isolation_level="READ COMMITTED")
        with engine.connect() as conn:
            if level_set_on == "connection":
                conn = conn.execution_options(isolation_level="READ COMMITTED")
            conn.invalidate()
            transaction = conn.begin()
            with expect_warnings(
                r"This Connection's isolation_level is 'READ COMMITTED', but its DBAPI connection runs as AUTOCOMMIT, "
                r"so StarRocks commits each statement at once"
            ):
                conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
                conn.execute(insert(self.tables.txn_keyed), {"id": 2, "v": 2})
            transaction.rollback()
        eq_(self._ids("txn_keyed"), [1, 2])

    @TRANSACTIONS_NEED_3_5
    def test_an_autocommit_connection_on_a_read_committed_engine_that_reconnects_warns_that_a_rollback_undoes(self):
        """The new DBAPI connection gets the engine's READ COMMITTED, so StarRocks rolls the row back."""
        with self._read_committed_engine().connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.invalidate()
            transaction = conn.begin()
            with expect_warnings(
                r"This Connection's isolation_level is 'AUTOCOMMIT', but its DBAPI connection runs as READ COMMITTED"
            ):
                conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
            transaction.rollback()
        eq_(self._ids("txn_keyed"), [])

    @TRANSACTIONS_NEED_3_5
    def test_a_read_committed_engine_keeps_its_level_when_a_connection_reconnects(self):
        with self._read_committed_engine().connect() as conn:
            conn.invalidate()
            transaction = conn.begin()
            conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
            transaction.rollback()
        eq_(self._ids("txn_keyed"), [])

    @(TRANSACTIONS_NEED_3_5 + SQL_TRANSACTION_SWITCH_NEEDS_4_0_7)
    def test_a_read_committed_engine_is_refused_when_sql_transactions_are_off(self):
        """StarRocks answers BEGIN WORK, COMMIT and ROLLBACK with OK and commits each statement at once."""
        engine = engines.testing_engine(
            options={
                "isolation_level": "READ COMMITTED",
                "connect_args": {"init_command": "SET enable_sql_transaction = false"},
            }
        )
        with expect_raises_message(exc.InvalidRequestError, "needs the session variable enable_sql_transaction on"):
            with engine.connect():
                pass

    @(TRANSACTIONS_NEED_3_5 + SQL_TRANSACTION_SWITCH_NEEDS_4_0_7)
    def test_a_read_committed_execution_option_is_refused_when_sql_transactions_are_off(self):
        with engines.testing_engine().connect() as conn:
            with conn.begin():
                conn.exec_driver_sql("SET enable_sql_transaction = false")
            with expect_raises_message(exc.InvalidRequestError, "needs the session variable enable_sql_transaction on"):
                conn.execution_options(isolation_level="READ COMMITTED")
            eq_(conn.get_isolation_level(), "AUTOCOMMIT")

    @(TRANSACTIONS_NEED_3_5 + SQL_TRANSACTION_SWITCH_NEEDS_4_0_7)
    def test_a_text_begin_is_refused_when_sql_transactions_are_off(self):
        """StarRocks answers BEGIN with OK and ignores it, so a rollback would keep the rows."""
        engine = engines.testing_engine(
            options={"connect_args": {"init_command": "SET enable_sql_transaction = false"}}
        )
        with engine.connect() as conn:
            with expect_raises_message(exc.InvalidRequestError, "needs the session variable enable_sql_transaction on"):
                conn.exec_driver_sql("BEGIN")
            assert not conn.connection.info.get("starrocks_transaction_open")

    @TRANSACTIONS_NEED_3_5
    def test_read_committed_rolls_back_the_inserts_of_several_tables(self):
        with expect_raises(_RollBack):
            with self._read_committed_engine().begin() as conn:
                conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
                conn.execute(insert(self.tables.txn_logged), {"id": 1, "v": 1})
                raise _RollBack()
        eq_(self._ids("txn_keyed"), [])
        eq_(self._ids("txn_logged"), [])

    @TRANSACTIONS_NEED_3_5
    def test_read_committed_commits_the_inserts_of_several_tables(self):
        with self._read_committed_engine().begin() as conn:
            conn.execute(insert(self.tables.txn_keyed), [{"id": 1, "v": 1}, {"id": 2, "v": 2}])
            conn.execute(insert(self.tables.txn_logged), {"id": 1, "v": 1})
        eq_(self._ids("txn_keyed"), [1, 2])
        eq_(self._ids("txn_logged"), [1])

    @TRANSACTIONS_NEED_3_5
    def test_uncommitted_rows_are_invisible_to_other_connections(self):
        with self._read_committed_engine().begin() as conn:
            conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
            eq_(self._ids("txn_keyed"), [])
        eq_(self._ids("txn_keyed"), [1])

    @TRANSACTIONS_NEED_3_5
    def test_a_transaction_that_only_reads_opens_none(self):
        with self._read_committed_engine().connect() as conn:
            with conn.begin():
                conn.execute(select(self.tables.txn_keyed))
                assert not conn.connection.info.get("starrocks_transaction_open")
                conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
                assert conn.connection.info.get("starrocks_transaction_open")
            assert not conn.connection.info.get("starrocks_transaction_open")

    @TRANSACTIONS_NEED_3_5
    def test_ddl_commits_the_open_transaction(self):
        created = Table("txn_created", MetaData(), Column("id", INTEGER, primary_key=True, autoincrement=False))
        try:
            with expect_raises(_RollBack):
                with self._read_committed_engine().begin() as conn:
                    conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
                    created.create(conn)
                    conn.execute(insert(self.tables.txn_logged), {"id": 1, "v": 1})
                    raise _RollBack()
            eq_(self._ids("txn_keyed"), [1])
            eq_(self._ids("txn_logged"), [])
        finally:
            created.drop(config.db, checkfirst=True)

    @TRANSACTIONS_NEED_3_5
    def test_a_transaction_begun_as_text_is_ended_by_the_rollback(self):
        with config.db.connect() as conn:
            transaction = conn.begin()
            conn.exec_driver_sql("BEGIN WORK")
            conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
            transaction.rollback()
        eq_(self._ids("txn_keyed"), [])

    @TRANSACTIONS_NEED_3_5
    @testing.combinations(
        ("construct",), ("hinted_construct",), ("text",), ("qualified_text",), ("hinted_text",), ("catalog_text",),
        ("hint_without_a_space",), ("line_comment",), ("cte",),
        argnames="form",
    )
    def test_delete_on_a_duplicate_key_table_is_refused(self, form):
        self._insert_logged_rows()
        logged = self.tables.txn_logged
        database = config.db.url.database
        texts = {
            "text": "DELETE FROM txn_logged WHERE id = 2",
            "qualified_text": f"DELETE FROM `{database}`.`txn_logged` WHERE id = 2",
            "hinted_text": "DELETE /*+ SET_VAR(query_timeout=10) */ FROM txn_logged WHERE id = 2",
            "catalog_text": f"DELETE FROM default_catalog.`{database}`.txn_logged WHERE id = 2",
            "hint_without_a_space": "DELETE/*+ SET_VAR(query_timeout=10) */ FROM txn_logged WHERE id = 2",
            "line_comment": "DELETE -- purge\nFROM txn_logged WHERE id = 2",
            "cte": "WITH c AS (SELECT 2 AS id) DELETE FROM txn_logged WHERE id IN (SELECT id FROM c)",
        }
        with expect_raises_message(exc.InvalidRequestError, "at once, outside the transaction"):
            with self._read_committed_engine().begin() as conn:
                if form == "construct":
                    conn.execute(delete(logged).where(logged.c.id == 2))
                elif form == "hinted_construct":
                    conn.execute(delete(logged).where(logged.c.id == 2).prefix_with("/*+ SET_VAR(query_timeout=10) */"))
                else:
                    conn.exec_driver_sql(texts[form])
        eq_(self._ids("txn_logged"), [1, 2])

    @TRANSACTIONS_NEED_3_5
    def test_a_delete_through_a_schema_translate_map_is_refused(self):
        with config.db.begin() as conn:
            conn.execute(insert(self.tables[f"{config.test_schema}.txn_translated"]), {"id": 1, "v": 1})
        untranslated = Table("txn_translated", MetaData(), Column("id", INTEGER), Column("v", INTEGER))
        engine = self._read_committed_engine().execution_options(schema_translate_map={None: config.test_schema})
        with expect_raises_message(exc.InvalidRequestError, "at once, outside the transaction"):
            with engine.begin() as conn:
                conn.execute(delete(untranslated).where(untranslated.c.id == 1))
        with config.db.connect() as conn:
            eq_(conn.execute(select(self.tables[f"{config.test_schema}.txn_translated"].c.id)).scalars().all(), [1])

    def test_delete_on_a_duplicate_key_table_under_autocommit_runs(self):
        self._insert_logged_rows()
        with config.db.begin() as conn:
            conn.execute(delete(self.tables.txn_logged).where(self.tables.txn_logged.c.id == 2))
        eq_(self._ids("txn_logged"), [1])

    @(TRANSACTIONS_NEED_3_5 + KEYED_DELETE_IN_TRANSACTION_NEEDS_4_1)
    def test_delete_on_a_primary_key_table_rolls_back(self):
        with config.db.begin() as conn:
            conn.execute(insert(self.tables.txn_keyed), [{"id": 1, "v": 1}, {"id": 2, "v": 2}])
        keyed = self.tables.txn_keyed
        with expect_raises(_RollBack):
            with self._read_committed_engine().begin() as conn:
                conn.execute(delete(keyed).where(keyed.c.id == 2))
                raise _RollBack()
        eq_(self._ids("txn_keyed"), [1, 2])

    @TRANSACTIONS_NEED_3_5
    def test_read_committed_rolls_back_an_executemany(self):
        with expect_raises(_RollBack):
            with self._read_committed_engine().begin() as conn:
                conn.execute(insert(self.tables.txn_keyed), [{"id": i, "v": i} for i in range(1, 6)])
                raise _RollBack()
        eq_(self._ids("txn_keyed"), [])

    @TRANSACTIONS_NEED_3_5
    def test_commented_text_dml_is_transactional(self):
        with expect_raises(_RollBack):
            with self._read_committed_engine().begin() as conn:
                conn.exec_driver_sql("/* note */ INSERT INTO txn_keyed VALUES (1, 1)")
                raise _RollBack()
        eq_(self._ids("txn_keyed"), [])

    @(TRANSACTIONS_NEED_3_5 + KEYED_DELETE_IN_TRANSACTION_NEEDS_4_1)
    def test_an_update_with_a_cte_is_transactional(self):
        keyed = self.tables.txn_keyed
        with config.db.begin() as conn:
            conn.execute(insert(keyed), {"id": 1, "v": 1})
        source = select(literal(100).label("v")).cte("source")
        with expect_raises(_RollBack):
            with self._read_committed_engine().begin() as conn:
                conn.execute(
                    update(keyed).where(keyed.c.id == 1).values(v=select(source.c.v).scalar_subquery()).add_cte(source)
                )
                raise _RollBack()
        with config.db.connect() as conn:
            eq_(conn.execute(select(keyed.c.v)).all(), [(1,)])

    @TRANSACTIONS_NEED_3_5
    def test_text_ddl_commits_the_open_transaction(self):
        try:
            with expect_raises(_RollBack):
                with self._read_committed_engine().begin() as conn:
                    conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
                    conn.exec_driver_sql(
                        "CREATE TABLE txn_text_created (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                        "PROPERTIES ('replication_num' = '1')"
                    )
                    raise _RollBack()
            eq_(self._ids("txn_keyed"), [1])
        finally:
            with config.db.begin() as conn:
                conn.exec_driver_sql("DROP TABLE IF EXISTS txn_text_created")

    @TRANSACTIONS_NEED_3_5
    @testing.combinations(
        ("read_first", "SELECT 1; INSERT INTO txn_keyed VALUES (5, 5)"),
        ("write_first", "INSERT INTO txn_keyed VALUES (5, 5); INSERT INTO txn_logged VALUES (5, 5)"),
        argnames="statements",
        id_="ia",
    )
    def test_text_holding_several_statements_is_refused_under_read_committed(self, statements):
        with expect_raises_message(exc.InvalidRequestError, "Text holding several statements"):
            with self._read_committed_engine().begin() as conn:
                conn.exec_driver_sql(statements)
        eq_(self._ids("txn_keyed"), [])

    def test_text_holding_several_statements_runs_under_autocommit(self):
        with config.db.begin() as conn:
            conn.exec_driver_sql("INSERT INTO txn_keyed VALUES (1, 1); INSERT INTO txn_keyed VALUES (2, 2)")
        eq_(self._ids("txn_keyed"), [1, 2])

    @staticmethod
    def _insert_into_files(hint=""):
        path = f"file:///tmp/sqla_txn_{uuid.uuid4().hex[:12]}/"
        return f"INSERT {hint}INTO FILES('path' = '{path}', 'format' = 'csv', 'single' = 'true') SELECT 1 AS a"

    @TRANSACTIONS_NEED_3_5
    @testing.combinations(("plain", ""), ("hinted", "/*+ SET_VAR(query_timeout=100) */ "), argnames="hint", id_="ia")
    def test_insert_into_files_opens_no_transaction(self, hint):
        with self._read_committed_engine().connect() as conn:
            with conn.begin():
                conn.exec_driver_sql(self._insert_into_files(hint))
                assert not conn.connection.info.get("starrocks_transaction_open")

    @TRANSACTIONS_NEED_3_5
    def test_insert_into_files_inside_a_transaction_is_refused(self):
        with expect_raises_message(exc.InvalidRequestError, "can't run INSERT INTO FILES inside a transaction"):
            with self._read_committed_engine().begin() as conn:
                conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
                conn.exec_driver_sql(self._insert_into_files())
        eq_(self._ids("txn_keyed"), [])

    @TRANSACTIONS_NEED_3_5
    @testing.combinations(("construct",), ("text",), argnames="form")
    def test_insert_overwrite_opens_no_transaction(self, form):
        """StarRocks can't run INSERT OVERWRITE inside a transaction, so it runs on its own, as INSERT INTO FILES does."""
        keyed = self.tables.txn_keyed
        with config.db.begin() as conn:
            conn.execute(insert(keyed), {"id": 1, "v": 1})
        with self._read_committed_engine().connect() as conn:
            with conn.begin():
                if form == "construct":
                    conn.execute(insert(keyed).with_dialect_options(starrocks_overwrite=True), {"id": 2, "v": 2})
                else:
                    conn.exec_driver_sql("INSERT /*+ SET_VAR(query_timeout=100) */ OVERWRITE txn_keyed VALUES (2, 2)")
                assert not conn.connection.info.get("starrocks_transaction_open")
        eq_(self._ids("txn_keyed"), [2])

    @TRANSACTIONS_NEED_3_5
    def test_insert_overwrite_inside_a_transaction_is_refused(self):
        with expect_raises_message(exc.InvalidRequestError, "can't run INSERT OVERWRITE inside a transaction"):
            with self._read_committed_engine().begin() as conn:
                conn.execute(insert(self.tables.txn_logged), {"id": 1, "v": 1})
                conn.execute(insert(self.tables.txn_keyed).with_dialect_options(starrocks_overwrite=True), {"id": 2, "v": 2})
        eq_(self._ids("txn_logged"), [])
        eq_(self._ids("txn_keyed"), [])

    @TRANSACTIONS_NEED_3_5
    @testing.combinations(("analyze", "ANALYZE TABLE txn_keyed"), ("use", "USE `{database}`"), argnames="statement", id_="ia")
    def test_a_statement_starrocks_refuses_in_a_transaction_is_refused_and_the_transaction_kept(self, statement):
        with self._read_committed_engine().connect() as conn:
            with expect_raises(_RollBack):
                with conn.begin():
                    conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
                    with expect_raises_message(exc.InvalidRequestError, "can't run in the open one"):
                        conn.exec_driver_sql(statement.format(database=config.db.url.database))
                    assert conn.connection.info.get("starrocks_transaction_open")
                    raise _RollBack()
        eq_(self._ids("txn_keyed"), [])

    @TRANSACTIONS_NEED_3_5
    def test_queries_show_describe_and_set_run_in_a_transaction(self):
        with expect_raises(_RollBack):
            with self._read_committed_engine().begin() as conn:
                conn.execute(insert(self.tables.txn_keyed), {"id": 1, "v": 1})
                conn.exec_driver_sql("SHOW TABLES").all()
                conn.exec_driver_sql("DESC txn_logged").all()
                conn.exec_driver_sql("SET query_timeout = 300")
                eq_(conn.exec_driver_sql("(SELECT 1) UNION ALL (SELECT 2)").scalars().all(), [1, 2])
                assert conn.connection.info.get("starrocks_transaction_open")
                raise _RollBack()
        eq_(self._ids("txn_keyed"), [])

    @TRANSACTIONS_NEED_3_5
    def test_a_text_commit_ends_the_transaction_and_the_next_dml_opens_another(self):
        keyed = self.tables.txn_keyed
        with expect_raises(_RollBack):
            with self._read_committed_engine().begin() as conn:
                conn.execute(insert(keyed), {"id": 1, "v": 1})
                conn.exec_driver_sql("COMMIT")
                conn.execute(insert(keyed), {"id": 2, "v": 2})
                raise _RollBack()
        eq_(self._ids("txn_keyed"), [1])

    @TRANSACTIONS_NEED_3_5
    def test_a_begin_starrocks_refuses_opens_no_transaction(self):
        """The dialect notes a text BEGIN only once StarRocks has accepted it."""
        with config.db.connect() as conn:
            with expect_raises(exc.DBAPIError):
                conn.exec_driver_sql("START TRANSACTION READ ONLY")
            assert not conn.connection.info.get("starrocks_transaction_open")

    @TRANSACTIONS_NEED_3_5
    def test_a_delete_on_a_view_is_left_to_starrocks(self):
        """A view has no table model to refuse a DELETE for, so StarRocks' own error comes back."""
        with config.db.begin() as conn:
            conn.exec_driver_sql("CREATE VIEW txn_logged_view AS SELECT id, v FROM txn_logged")
        try:
            with expect_raises(exc.DBAPIError):
                with self._read_committed_engine().begin() as conn:
                    conn.exec_driver_sql("DELETE FROM txn_logged_view WHERE id = 2")
        finally:
            with config.db.begin() as conn:
                conn.exec_driver_sql("DROP VIEW IF EXISTS txn_logged_view")

    @TRANSACTIONS_NEED_3_5
    def test_a_delete_without_where_is_refused(self):
        self._insert_logged_rows()
        with expect_raises_message(exc.InvalidRequestError, "compiles to TRUNCATE TABLE"):
            with self._read_committed_engine().begin() as conn:
                conn.execute(delete(self.tables.txn_logged))
        eq_(self._ids("txn_logged"), [1, 2])

    @TRANSACTIONS_NEED_3_5
    def test_a_raw_cursor_transaction_is_rolled_back_when_the_connection_returns(self):
        engine = engines.testing_engine(options={"pool_size": 1, "max_overflow": 0})
        with engine.connect() as conn:
            cursor = conn.connection.cursor()
            cursor.execute("BEGIN WORK")
            cursor.execute("INSERT INTO txn_keyed VALUES (1, 1)")
        with engine.begin() as conn:
            conn.execute(insert(self.tables.txn_keyed), {"id": 2, "v": 2})
        eq_(self._ids("txn_keyed"), [2])

    @TRANSACTIONS_REFUSED_BEFORE_3_5
    def test_a_text_begin_is_refused_before_3_5(self):
        with config.db.connect() as conn:
            with expect_raises_message(exc.InvalidRequestError, "needs StarRocks 3.5 or later"):
                conn.exec_driver_sql("BEGIN WORK")

    @TRANSACTIONS_REFUSED_BEFORE_3_5
    def test_a_read_committed_engine_is_refused_before_3_5(self):
        """Refused at the first connection, before a migration's first statement can run."""
        with expect_raises_message(exc.InvalidRequestError, "needs StarRocks 3.5 or later"):
            with self._read_committed_engine().connect():
                pass

    @TRANSACTIONS_REFUSED_BEFORE_3_5
    def test_a_read_committed_execution_option_is_refused_before_3_5(self):
        with config.db.connect() as conn:
            with expect_raises_message(exc.InvalidRequestError, "needs StarRocks 3.5 or later"):
                conn.execution_options(isolation_level="READ COMMITTED")
            eq_(conn.get_isolation_level(), "AUTOCOMMIT")
