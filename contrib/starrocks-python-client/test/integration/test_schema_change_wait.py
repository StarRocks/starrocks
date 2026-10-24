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
"""

from unittest.mock import patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Column, testing
from sqlalchemy.exc import DatabaseError
from sqlalchemy.testing import config, fixtures

from starrocks import INTEGER
from starrocks.alembic.starrocks import StarRocksImpl


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
