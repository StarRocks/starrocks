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
"""

from __future__ import annotations

import logging
from typing import Union
from unittest.mock import patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
from sqlalchemy import Column
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError

from starrocks import INTEGER
from starrocks.alembic.starrocks import StarRocksImpl
from test.conftest_sr import create_test_engine


logger = logging.getLogger(__name__)

# Legal under DB_NAME_REGEX (starts with a letter; '-', '!' and '$' are in
# SPECIAL_CHARACTERS_IN_DB_NAME), and all three require identifier quoting.
SPECIAL_SCHEMA = "sr_wait-test!db$x"

# Illegal: a backtick is not in SPECIAL_CHARACTERS_IN_DB_NAME. This is the name
# shape the review comment assumes is reachable.
BACKTICK_SCHEMA = "sr_wait`test"


@pytest.mark.integration
class TestSchemaChangeWaitQuoting:
    engine: Engine
    schema: Union[str, None]

    @classmethod
    def setup_class(cls) -> None:
        cls.engine = create_test_engine()
        cls.schema = SPECIAL_SCHEMA
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
            f"CREATE TABLE `{self.schema}`.`{name}` (k INT, v INT) "
            "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
            "PROPERTIES ('replication_num' = '1')"
        )

    def test_wait_polls_a_schema_that_needs_quoting(self):
        """The polling query must work for a legal db name that needs quoting.

        With ``starrocks_wait_for_schema_change=True`` the ADD COLUMN is
        followed by ``SHOW ALTER TABLE COLUMN FROM <schema> ...``; if that
        fragment is mis-rendered the migration raises after the DDL has already
        been applied.
        """
        table = "t_special_schema"
        with self.engine.begin() as conn:
            self._create_table(conn, table)

            ctx = MigrationContext.configure(
                connection=conn,
                opts={
                    "starrocks_wait_for_schema_change": True,
                    "starrocks_schema_change_timeout": 120,
                    "starrocks_schema_change_poll_interval": 1.0,
                },
            )
            op = Operations(ctx)
            op.add_column(table, Column("added", INTEGER), schema=self.schema)

        # The wait returned normally and the column is really there.
        with self.engine.connect() as conn:
            cols = [
                row[0]
                for row in conn.exec_driver_sql(
                    "SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema = '{self.schema}' AND table_name = '{table}'"
                )
            ]
        assert "added" in cols

    def test_wait_polls_repeated_changes_on_a_quoted_schema(self):
        """Two consecutive column changes: the second poll must also resolve.

        Exercises the ORDER BY JobId path (both jobs live under the same
        quoted schema) in addition to the identifier rendering.
        """
        table = "t_special_schema_twice"
        with self.engine.begin() as conn:
            self._create_table(conn, table)

            ctx = MigrationContext.configure(
                connection=conn,
                opts={
                    "starrocks_wait_for_schema_change": True,
                    "starrocks_schema_change_timeout": 120,
                    "starrocks_schema_change_poll_interval": 1.0,
                },
            )
            op = Operations(ctx)
            op.add_column(table, Column("a1", INTEGER), schema=self.schema)
            op.add_column(table, Column("a2", INTEGER), schema=self.schema)

        with self.engine.connect() as conn:
            cols = [
                row[0]
                for row in conn.exec_driver_sql(
                    "SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema = '{self.schema}' AND table_name = '{table}'"
                )
            ]
        assert {"a1", "a2"} <= set(cols)

    def test_polling_query_renders_for_a_schema_needing_quotes(self):
        """The exact polling statement the impl builds must parse and run.

        Guards the ``FROM <schema>`` fragment directly, independently of whether
        a schema change happens to be in flight.
        """
        table = "t_special_schema"
        impl_fragment = f"FROM `{self.schema}` "
        query = (
            f"SHOW ALTER TABLE COLUMN {impl_fragment}"
            f"WHERE TableName = '{table}' ORDER BY JobId DESC LIMIT 1"
        )
        with self.engine.connect() as conn:
            rows = conn.exec_driver_sql(query).mappings().all()
        # A prior test added a column to this table, so a job row exists.
        assert rows, "expected a schema-change job row for the table"
        assert str(rows[0]["State"]).upper() == "FINISHED"

    def test_wait_path_is_actually_exercised(self):
        """Sanity check: the impl is StarRocks' and the wait hook really fires.

        Without this the quoting tests above could pass while never reaching
        ``_wait_for_schema_change`` at all.
        """
        table = "t_wait_hook"
        calls: list = []
        with self.engine.begin() as conn:
            self._create_table(conn, table)
            ctx = MigrationContext.configure(
                connection=conn,
                opts={
                    "starrocks_wait_for_schema_change": True,
                    "starrocks_schema_change_timeout": 120,
                    "starrocks_schema_change_poll_interval": 1.0,
                },
            )
            assert isinstance(ctx.impl, StarRocksImpl)

            real = ctx.impl._wait_for_schema_change

            def spy(table_name, schema):
                calls.append((table_name, schema))
                return real(table_name, schema)

            with patch.object(ctx.impl, "_wait_for_schema_change", spy):
                Operations(ctx).add_column(
                    table, Column("added", INTEGER), schema=self.schema
                )

        assert calls == [(table, self.schema)]

    def test_backtick_database_name_cannot_exist(self):
        """A db name containing a backtick is unreachable on StarRocks.

        Either ``CREATE DATABASE`` errors, or the backtick is dropped from the
        stored name -- so ``_wait_for_schema_change`` can never be handed a
        schema whose backticks would break its ``FROM `...` `` fragment.
        """
        escaped = BACKTICK_SCHEMA.replace("`", "``")
        with self.engine.connect() as conn:
            try:
                conn.exec_driver_sql(f"CREATE DATABASE `{escaped}`")
            except DatabaseError:
                pass  # rejected outright: nothing to clean up

            found = [
                row[0]
                for row in conn.exec_driver_sql("SHOW DATABASES")
                if "sr_wait" in row[0]
            ]
            try:
                assert all("`" not in name for name in found), found
            finally:
                for name in found:
                    if name != self.schema:
                        conn.exec_driver_sql(f"DROP DATABASE IF EXISTS `{name}`")
