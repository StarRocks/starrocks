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

"""How the dialect maps isolation levels onto the driver's autocommit, and which DELETE target it reads."""

import os
from types import SimpleNamespace
from unittest import mock
import warnings

from sqlalchemy import Column, MetaData, Table, create_engine, delete, exc, testing
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
from sqlalchemy.engine import make_url
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_, expect_raises_message

from starrocks import INTEGER
from starrocks.dialect import StarRocksDialect


class _VariablesCursor:
    """A cursor answering SHOW VARIABLES with the rows it is given."""

    def __init__(self, rows):
        self.rows = rows
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class _DriverConnection:
    def __init__(self, autocommit_mode, sql_transaction="true"):
        self.autocommit_mode = autocommit_mode
        self.commits = 0
        self.rollbacks = 0
        rows = [] if sql_transaction is None else [("enable_sql_transaction", sql_transaction)]
        self.variables = _VariablesCursor(rows)

    def autocommit(self, value):
        self.autocommit_mode = bool(value)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def cursor(self):
        return self.variables


class IsolationLevelTest(fixtures.TestBase):
    @testing.combinations(
        ("default", "", True),
        ("off", "?autocommit=false", "false"),
        ("on", "?autocommit=true", "true"),
        argnames="query,autocommit",
        id_="iaa",
    )
    def test_connect_args_turn_autocommit_on_and_pass_a_url_value_on_as_text(self, query, autocommit):
        """The driver takes any text as autocommit on, as it did before the dialect turned autocommit on itself."""
        _, connect_args = StarRocksDialect().create_connect_args(make_url(f"starrocks://root@host:9030/db{query}"))
        eq_(connect_args["autocommit"], autocommit)

    def test_levels(self):
        eq_(
            StarRocksDialect().get_isolation_level_values(None),
            ("AUTOCOMMIT", "READ COMMITTED", "READ UNCOMMITTED", "REPEATABLE READ", "SERIALIZABLE"),
        )

    @testing.combinations(
        ("READ COMMITTED", False, "READ COMMITTED"),
        ("read_committed", False, "READ COMMITTED"),
        ("AUTOCOMMIT", True, "AUTOCOMMIT"),
        argnames="level,autocommit_mode,reported",
    )
    def test_level_sets_the_driver_autocommit(self, level, autocommit_mode, reported):
        dialect = StarRocksDialect()
        connection = _DriverConnection(not autocommit_mode)
        dialect.set_isolation_level(connection, level)
        eq_(connection.autocommit_mode, autocommit_mode)
        eq_(dialect.get_isolation_level(connection), reported)

    def test_read_committed_is_refused_before_3_5(self):
        dialect = StarRocksDialect()
        dialect.server_version_info = (3, 3, 22)
        connection = _DriverConnection(True)
        with expect_raises_message(exc.InvalidRequestError, "needs StarRocks 3.5 or later; the server is 3.3.22"):
            dialect.set_isolation_level(connection, "READ COMMITTED")
        eq_(connection.autocommit_mode, True)
        dialect.set_isolation_level(connection, "AUTOCOMMIT")

    def test_a_pooled_connection_is_unwrapped(self):
        dialect = StarRocksDialect()
        connection = _DriverConnection(True)
        dialect.set_isolation_level(SimpleNamespace(dbapi_connection=connection), "READ COMMITTED")
        eq_(connection.autocommit_mode, False)

    def test_the_server_default_autocommits(self):
        eq_(StarRocksDialect().get_isolation_level(_DriverConnection(None)), "AUTOCOMMIT")

    @testing.combinations(
        ("SERIALIZABLE",), ("REPEATABLE READ",), ("repeatable_read",), ("READ UNCOMMITTED",), argnames="level"
    )
    def test_a_mysql_level_runs_as_autocommit_with_a_future_warning(self, level):
        dialect = StarRocksDialect()
        connection = _DriverConnection(False)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            dialect.set_isolation_level(connection, level)
        eq_([(w.category, "is deprecated for StarRocks" in str(w.message)) for w in caught], [(FutureWarning, True)])
        eq_(connection.autocommit_mode, True)
        eq_(dialect.get_isolation_level(connection), "AUTOCOMMIT")

    @testing.combinations(
        ("create_engine", lambda url: create_engine(url, isolation_level="REPEATABLE READ")),
        (
            "create_engine_execution_options",
            lambda url: create_engine(url, execution_options={"isolation_level": "REPEATABLE READ"}),
        ),
        ("engine_execution_options", lambda url: create_engine(url).execution_options(isolation_level="SERIALIZABLE")),
        argnames="give_the_level",
        id_="ia",
    )
    def test_a_mysql_level_given_to_an_engine_warns_once_naming_the_line_that_gave_it(self, give_the_level):
        """Python shows a FutureWarning once for each line it names, so it must name the caller's line."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            give_the_level("starrocks://root@localhost:9030/db")
        eq_([(w.category, os.path.normcase(w.filename)) for w in caught], [(FutureWarning, os.path.normcase(__file__))])

    @testing.combinations(("SNAPSHOT",), ("READ_ONLY",), argnames="level")
    def test_other_levels_are_refused(self, level):
        with expect_raises_message(exc.ArgumentError, f"Invalid value '{level.replace('_', ' ')}' for isolation_level"):
            StarRocksDialect().set_isolation_level(_DriverConnection(True), level)

    @testing.combinations(("false",), ("0",), ("FALSE",), argnames="value")
    def test_read_committed_is_refused_when_sql_transactions_are_off(self, value):
        dialect = StarRocksDialect()
        dialect.server_version_info = (4, 1, 6)
        connection = _DriverConnection(True, sql_transaction=value)
        with expect_raises_message(exc.InvalidRequestError, "needs the session variable enable_sql_transaction on"):
            dialect.set_isolation_level(connection, "READ COMMITTED")
        eq_(connection.autocommit_mode, True)
        eq_(connection.variables.statements, ["SHOW VARIABLES LIKE 'enable_sql_transaction'"])

    @testing.combinations(("on", "true"), ("missing", None), argnames="sql_transaction", id_="ia")
    def test_read_committed_runs_where_sql_transactions_are_on_or_have_no_switch(self, sql_transaction):
        """StarRocks 3.5 has no enable_sql_transaction, and SHOW VARIABLES lists nothing for it."""
        dialect = StarRocksDialect()
        dialect.server_version_info = (3, 5, 21) if sql_transaction is None else (4, 1, 6)
        connection = _DriverConnection(True, sql_transaction=sql_transaction)
        dialect.set_isolation_level(connection, "READ COMMITTED")
        eq_(connection.autocommit_mode, False)

    def test_a_first_read_committed_connection_is_checked_when_the_dialect_initializes(self):
        """SQLAlchemy sets the first connection's level before the server version is read."""
        dialect = StarRocksDialect()
        dialect.server_version_info = (4, 1, 6)
        connection = SimpleNamespace(connection=SimpleNamespace(dbapi_connection=_DriverConnection(False, "false")))
        with mock.patch.object(MySQLDialect_pymysql, "initialize"), mock.patch.object(dialect, "_init_run_mode"):
            with expect_raises_message(exc.InvalidRequestError, "needs the session variable enable_sql_transaction"):
                dialect.initialize(connection)


class LostIsolationLevelTest(fixtures.TestBase):
    """A DBAPI connection that doesn't run at its Connection's level, as after a reconnect, warns, once."""

    @staticmethod
    def _execute_twice(level, autocommit_mode):
        info = {}
        connection = SimpleNamespace(info=info, dbapi_connection=_DriverConnection(autocommit_mode))
        context = SimpleNamespace(
            execution_options={"isolation_level": level} if level else {},
            root_connection=SimpleNamespace(connection=connection),
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            for _ in range(2):
                StarRocksDialect()._warn_of_a_lost_isolation_level(context, info)
        return [str(w.message) for w in caught]

    @testing.combinations(
        (
            "read_committed_lost",
            "READ COMMITTED",
            True,
            "'READ COMMITTED', but its DBAPI connection runs as AUTOCOMMIT",
        ),
        ("autocommit_lost", "AUTOCOMMIT", False, "'AUTOCOMMIT', but its DBAPI connection runs as READ COMMITTED"),
        ("mysql_level_lost", "serializable", False, "'serializable', but its DBAPI connection runs as READ COMMITTED"),
        argnames="level,autocommit_mode,warning",
        id_="iaaa",
    )
    def test_a_lost_level_warns_once(self, level, autocommit_mode, warning):
        messages = self._execute_twice(level, autocommit_mode)
        eq_(len(messages), 1, messages)
        assert messages[0].startswith(f"This Connection's isolation_level is {warning}, so StarRocks"), messages

    @testing.combinations(
        ("read_committed", "read_committed", False),
        ("autocommit", "AUTOCOMMIT", True),
        ("mysql_level", "REPEATABLE READ", True),
        ("engine_level_autocommit", None, True),
        ("engine_level_read_committed", None, False),
        argnames="level,autocommit_mode",
        id_="iaa",
    )
    def test_a_kept_level_does_not_warn(self, level, autocommit_mode):
        eq_(self._execute_twice(level, autocommit_mode), [])


class TransactionEndTest(fixtures.TestBase):
    @testing.combinations((True,), (False,), argnames="open_transaction")
    def test_commit_and_rollback_always_reach_the_driver_and_clear_the_flag(self, open_transaction):
        dialect = StarRocksDialect()
        driver = _DriverConnection(False)
        info = {"starrocks_transaction_open": True} if open_transaction else {}
        pooled = SimpleNamespace(info=info, commit=driver.commit, rollback=driver.rollback)
        dialect.do_commit(pooled)
        eq_(pooled.info, {})
        pooled.info["starrocks_transaction_open"] = open_transaction
        dialect.do_rollback(pooled)
        eq_((driver.commits, driver.rollbacks), (1, 1))
        eq_(pooled.info, {})

    def test_an_untracked_connection_is_committed(self):
        driver = _DriverConnection(False)
        StarRocksDialect().do_commit(driver)
        StarRocksDialect().do_rollback(driver)
        eq_((driver.commits, driver.rollbacks), (1, 1))


class StatementKeywordTest(fixtures.TestBase):
    @testing.combinations(
        ("plain", "INSERT INTO t VALUES (1)", "INSERT"),
        ("block_comment", "/* note */ INSERT INTO t VALUES (1)", "INSERT"),
        ("hint", "/*+ SET_VAR(query_timeout=10) */ UPDATE t SET a = 1", "UPDATE"),
        ("line_comment", "-- note\nupdate t set a = 1", "UPDATE"),
        ("hash_comment", "# note\ndelete from t where 1", "DELETE"),
        ("cte_update", "WITH c AS (SELECT 1 AS a) UPDATE t SET a = (SELECT a FROM c)", "UPDATE"),
        ("cte_quoted", 'with c as (select "x)" as a) delete from t where 1', "DELETE"),
        ("cte_select", "WITH RECURSIVE c(a) AS (SELECT (1)) SELECT * FROM c", "SELECT"),
        ("truncate", "TRUNCATE TABLE t", "TRUNCATE"),
        ("empty", "", ""),
        id_="iaa",
        argnames="statement,keyword",
    )
    def test_keyword(self, statement, keyword):
        eq_(StarRocksDialect._statement_keyword(statement), keyword)

    @testing.combinations(
        ("BEGIN WORK", True, False),
        ("/* c */ START TRANSACTION", True, False),
        ("COMMIT WORK", False, True),
        ("ROLLBACK", False, True),
        ("ROLLBACK TO SAVEPOINT s", False, False),
        ("rollback work to s", False, False),
        argnames="statement,starts,ends",
    )
    def test_transaction_control(self, statement, starts, ends):
        position = StarRocksDialect._LEADING_COMMENTS.match(statement).end()
        eq_(bool(StarRocksDialect._TRANSACTION_START.match(statement, position)), starts)
        eq_(bool(StarRocksDialect._TRANSACTION_END.match(statement, position)), ends)


class SeveralStatementsTest(fixtures.TestBase):
    @testing.combinations(
        ("one", "SELECT 1", False),
        ("terminated", "INSERT INTO t VALUES (1);", False),
        ("terminated_and_commented", "INSERT INTO t VALUES (1); -- done\n", False),
        ("semicolon_in_literal", "SELECT ';' AS a; ", False),
        ("semicolon_in_name", "SELECT 1 AS `a;b`", False),
        ("semicolon_in_comment", "/* a; b */ SELECT 1", False),
        ("two", "SELECT 1; INSERT INTO t VALUES (1)", True),
        ("two_after_a_comment", "SET query_timeout = 300; /* c */ DELETE FROM t WHERE id = 1", True),
        id_="iaa",
        argnames="statement,several",
    )
    def test_several(self, statement, several):
        eq_(StarRocksDialect._holds_several_statements(statement), several)

    @testing.combinations(
        ("autocommit", True, {}, False),
        ("read_committed", False, {}, True),
        ("transaction_begun_as_text", True, {"starrocks_transaction_open": True}, True),
        argnames="autocommit_mode,info,scanned",
        id_="iaaa",
    )
    def test_text_is_scanned_only_where_several_statements_are_refused(self, autocommit_mode, info, scanned):
        """A long INSERT on an AUTOCOMMIT connection, the default, is not read token by token."""
        connection = SimpleNamespace(dbapi_connection=_DriverConnection(autocommit_mode))
        context = SimpleNamespace(isddl=False, root_connection=SimpleNamespace(connection=connection))
        with mock.patch.object(StarRocksDialect, "_holds_several_statements", return_value=False) as scan:
            StarRocksDialect()._refuse_several_statements_in_transaction("INSERT INTO t VALUES (1)", context, info)
        eq_(scan.called, scanned)


class StatementInsideTransactionTest(fixtures.TestBase):
    """Which statements may run inside an open StarRocks transaction, and which INSERTs run outside one."""

    @staticmethod
    def _context(info):
        connection = SimpleNamespace(info=info, dbapi_connection=_DriverConnection(False))
        return SimpleNamespace(
            isinsert=False,
            isupdate=False,
            isdelete=False,
            isddl=False,
            execution_options={},
            root_connection=SimpleNamespace(connection=connection),
        )

    @testing.combinations(
        ("query", "SELECT 1"),
        ("cte", "WITH c AS (SELECT 1 AS a) SELECT a FROM c"),
        ("parenthesized", "(SELECT 1) UNION ALL (SELECT 2)"),
        ("show", "SHOW TABLES"),
        ("describe", "DESC t"),
        ("explain", "/* plan */ EXPLAIN SELECT 1"),
        ("set", "SET query_timeout = 600"),
        ("commit", "COMMIT"),
        ("rollback", "ROLLBACK WORK"),
        ("comment_only", "-- nothing\n"),
        argnames="statement",
        id_="ia",
    )
    def test_statements_starrocks_runs_in_a_transaction_are_sent(self, statement):
        info = {"starrocks_transaction_open": True}
        StarRocksDialect()._match_transaction_to(None, statement, self._context(info))
        eq_(info, {"starrocks_transaction_open": True})

    @testing.combinations(
        ("analyze", "ANALYZE TABLE t", "ANALYZE"),
        ("use", "use other_db", "USE"),
        ("kill", "KILL QUERY 7", "KILL"),
        ("cancel", "CANCEL ALTER TABLE COLUMN FROM t", "CANCEL"),
        ("recover", "RECOVER TABLE t", "RECOVER"),
        ("submit", "SUBMIT TASK AS INSERT INTO t SELECT 1", "SUBMIT"),
        ("savepoint", "SAVEPOINT s", "SAVEPOINT"),
        argnames="statement,keyword",
        id_="iaa",
    )
    def test_other_statements_are_refused_and_the_transaction_stays_open(self, statement, keyword):
        info = {"starrocks_transaction_open": True}
        with expect_raises_message(exc.InvalidRequestError, f"so this {keyword} statement can't run in the open one"):
            StarRocksDialect()._match_transaction_to(None, statement, self._context(info))
        eq_(info, {"starrocks_transaction_open": True})

    def test_other_statements_run_when_no_transaction_is_open(self):
        StarRocksDialect()._match_transaction_to(None, "ANALYZE TABLE t", self._context({}))

    @testing.combinations(("BEGIN",), ("/* c */ start transaction",), argnames="statement")
    def test_a_text_begin_is_refused_when_sql_transactions_are_off(self, statement):
        """StarRocks answers it with OK and ignores it, so no transaction would open."""
        dialect = StarRocksDialect()
        dialect.server_version_info = (4, 1, 6)
        driver = _DriverConnection(True, sql_transaction="false")
        context = self._context({})
        context.root_connection.connection.dbapi_connection = driver
        with expect_raises_message(
            exc.InvalidRequestError, r"A transaction \(BEGIN\) needs the session variable enable_sql_transaction on"
        ):
            dialect._match_transaction_to(None, statement, context)
        eq_(driver.variables.statements, ["SHOW VARIABLES LIKE 'enable_sql_transaction'"])

    @testing.combinations(("on", "true"), ("missing", None), argnames="sql_transaction", id_="ia")
    def test_a_text_begin_runs_where_sql_transactions_are_on_or_have_no_switch(self, sql_transaction):
        dialect = StarRocksDialect()
        dialect.server_version_info = (3, 5, 21) if sql_transaction is None else (4, 1, 6)
        context = self._context({})
        context.root_connection.connection.dbapi_connection = _DriverConnection(True, sql_transaction=sql_transaction)
        dialect._match_transaction_to(None, "BEGIN WORK", context)

    @testing.combinations(
        ("overwrite", "INSERT OVERWRITE t SELECT 1", "INSERT OVERWRITE"),
        ("hinted_overwrite", "insert /*+ SET_VAR(query_timeout=10) */ overwrite t VALUES (1)", "INSERT OVERWRITE"),
        ("overwrite_partition", "INSERT OVERWRITE t PARTITION (p1) VALUES (1)", "INSERT OVERWRITE"),
        ("files", "INSERT INTO FILES('path' = 'file:///tmp/x/') SELECT 1", "INSERT INTO FILES"),
        ("into_a_table_named_overwrite", "INSERT INTO overwrite VALUES (1)", None),
        ("into_a_table", "INSERT INTO t VALUES (1)", None),
        ("query", "SELECT 1", None),
        argnames="statement,kind",
        id_="iaa",
    )
    def test_inserts_run_outside_transactions(self, statement, kind):
        eq_(StarRocksDialect._insert_run_outside_transactions(statement), kind)


class DeleteTargetTest(fixtures.TestBase):
    @testing.combinations(
        ("DELETE FROM t WHERE id = 1", ("t", None)),
        ("delete  from `s`.`t` where 1", ("t", "s")),
        ("DELETE FROM `a``b` WHERE x = 1", ("a`b", None)),
        ("DELETE FROM s . t USING u WHERE t.id = u.id", ("t", "s")),
        ("\n  DELETE FROM `my-db`.events", ("events", "my-db")),
        ("DELETE /*+ SET_VAR(query_timeout=10) */ FROM dup WHERE id = 2", ("dup", None)),
        ("/* note */ DELETE FROM dup WHERE id = 2", ("dup", None)),
        ("DELETE FROM default_catalog.db.dup WHERE id = 2", ("dup", "db")),
        ("DELETE/*+ SET_VAR(query_timeout=10) */ FROM dup WHERE id = 2", ("dup", None)),
        ("DELETE -- purge\nFROM dup WHERE id = 2", ("dup", None)),
        ("DELETE # purge\nFROM /* t */ s . `dup`", ("dup", "s")),
        ("WITH c AS (SELECT 2 AS id) DELETE FROM dup WHERE id IN (SELECT id FROM c)", ("dup", None)),
        ("SELECT * FROM t", None),
        ("INSERT INTO t VALUES (1)", None),
        ("WITH c AS (SELECT 1) SELECT * FROM c", None),
        argnames="statement,target",
    )
    def test_target(self, statement, target):
        eq_(StarRocksDialect._delete_target(statement), target)

    def test_a_delete_whose_table_cannot_be_read_is_refused(self):
        with expect_raises_message(exc.InvalidRequestError, "can't read which table this DELETE deletes from"):
            StarRocksDialect._delete_target("DELETE FROM (SELECT 1) AS t WHERE 1")

    @testing.combinations(
        ("untranslated", None, {}, ("t", None)),
        ("default_schema", None, {None: "other"}, ("t", "other")),
        ("named_schema", "s", {"s": "other"}, ("t", "other")),
        ("unmapped_schema", "s", {None: "other"}, ("t", "s")),
        id_="iaaa",
        argnames="schema,schema_translate_map,target",
    )
    def test_a_construct_target_is_in_the_translated_schema(self, schema, schema_translate_map, target):
        table = Table("t", MetaData(), Column("id", INTEGER), schema=schema)
        context = SimpleNamespace(
            isdelete=True,
            compiled=SimpleNamespace(statement=delete(table)),
            execution_options={"schema_translate_map": schema_translate_map} if schema_translate_map else {},
        )
        eq_(StarRocksDialect._delete_target("DELETE FROM t", context), target)
