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

"""Each statement a DDL construct compiles to raises its own error, through PyMySQL and through asyncmy.

With ``async_fallback=true``, asyncmy's synchronous engine reads the results and waits for schema changes too,
and asyncmy's own engine waits for a schema change without blocking the event loop.
"""

import asyncio
from unittest import mock

from sqlalchemy import Index, String, Table, create_engine, exc, testing
from sqlalchemy.testing import config, fixtures
from sqlalchemy.testing.assertions import eq_, expect_raises_message
from sqlalchemy.testing.schema import Column

from starrocks import INTEGER
from starrocks.sql.ddl import AlterTableProperties
from test.util import run_coroutine


def _properties_the_third_of_which_fails():
    """Properties StarRocks takes one at a time, so they compile to three ALTER TABLE SET statements."""
    return {"replication_num": "1", "in_memory": "false", "primary_index_cache_expire_sec": "abc"}


class DdlResultsTest(fixtures.TablesTest):
    __backend__ = True
    __only_on__ = "starrocks"

    @classmethod
    def define_tables(cls, metadata):
        Table("ddl_results", metadata, Column("id", INTEGER, primary_key=True, autoincrement=False))

    def test_the_error_of_a_third_statement_raises(self, connection):
        with expect_raises_message(exc.DBAPIError, "primary_index_cache_expire_sec"):
            connection.execute(AlterTableProperties("ddl_results", _properties_the_third_of_which_fails()))

    @testing.requires.sqlalchemy_2
    def test_the_error_of_a_third_statement_raises_through_asyncmy(self):
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine(config.db.url.set(drivername="starrocks+asyncmy"))

        async def alter():
            try:
                async with engine.begin() as conn:
                    await conn.execute(AlterTableProperties("ddl_results", _properties_the_third_of_which_fails()))
            finally:
                await engine.dispose()

        with expect_raises_message(exc.DBAPIError, "primary_index_cache_expire_sec"):
            run_coroutine(alter())


class AsyncFallbackDdlTest(fixtures.TestBase):
    """``async_fallback=true`` makes a synchronous engine whose cursors await with ``await_fallback``.

    ``await_only`` raises MissingGreenlet there, after the statement has run.
    """

    __backend__ = True
    __only_on__ = "starrocks"
    __requires__ = ("sqlalchemy_2",)

    @staticmethod
    def _fallback_engine():
        return create_engine(
            config.db.url.set(drivername="starrocks+asyncmy").update_query_dict({"async_fallback": "true"})
        )

    def test_every_result_of_a_ddl_construct_is_read(self, metadata):
        """Two properties StarRocks takes one at a time compile to two ALTER TABLE SET statements."""
        Table("ddl_fallback_results", metadata, Column("id", INTEGER, primary_key=True, autoincrement=False))
        metadata.create_all(config.db)
        engine = self._fallback_engine()
        try:
            with engine.begin() as conn:
                conn.execute(
                    AlterTableProperties("ddl_fallback_results", {"replication_num": "1", "enable_persistent_index": "false"})
                )
        finally:
            engine.dispose()
        with config.db.connect() as conn:
            ddl = conn.exec_driver_sql("SHOW CREATE TABLE ddl_fallback_results").one()[1]
        assert '"enable_persistent_index" = "false"' in ddl, ddl

    def test_the_second_index_waits_for_the_first(self, metadata):
        table = Table(
            "ddl_fallback_indexes",
            metadata,
            Column("id", INTEGER),
            Column("city", String(20)),
            Column("country", String(20)),
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
        )
        Index("ix_city", table.c.city, starrocks_using="BITMAP")
        Index("ix_country", table.c.country, starrocks_using="BITMAP")
        engine = self._fallback_engine()
        try:
            with engine.begin() as conn:
                metadata.create_all(conn)
        finally:
            engine.dispose()
        with config.db.connect() as conn:
            eq_(len(conn.exec_driver_sql("SHOW ALTER TABLE COLUMN WHERE TableName = 'ddl_fallback_indexes'").all()), 2)


def _table_with_two_indexes(name, metadata):
    """A table whose second CREATE INDEX waits for the build of the first."""
    table = Table(
        name,
        metadata,
        Column("id", INTEGER),
        Column("city", String(20)),
        Column("country", String(20)),
        starrocks_duplicate_key="id",
        starrocks_distributed_by="HASH(id) BUCKETS 1",
    )
    Index(f"ix_{name}_city", table.c.city, starrocks_using="BITMAP")
    Index(f"ix_{name}_country", table.c.country, starrocks_using="BITMAP")
    return table


class AsyncmyWaitTest(fixtures.TestBase):
    """asyncmy pauses between schema-change polls with ``asyncio.sleep``, so other tasks run meanwhile."""

    __backend__ = True
    __only_on__ = "starrocks"
    __requires__ = ("sqlalchemy_2",)

    def test_a_task_runs_while_the_second_index_waits(self, metadata):
        from sqlalchemy.ext.asyncio import create_async_engine

        from starrocks.asyncmy import StarRocksDialect_asyncmy

        _table_with_two_indexes("ddl_async_wait", metadata)
        engine = create_async_engine(config.db.url.set(drivername="starrocks+asyncmy"))
        pauses = []
        pause_between_polls = StarRocksDialect_asyncmy._pause_between_polls

        def counted_pause(dialect, cursor, seconds):
            pauses.append(seconds)
            pause_between_polls(dialect, cursor, seconds)

        async def create_all_and_count_ticks():
            ticks = 0
            created = asyncio.Event()

            async def tick():
                nonlocal ticks
                while not created.is_set():
                    ticks += 1
                    await asyncio.sleep(0.05)

            ticker = asyncio.ensure_future(tick())
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(metadata.create_all)
            finally:
                created.set()
                await ticker
                await engine.dispose()
            return ticks

        with mock.patch.object(StarRocksDialect_asyncmy, "_pause_between_polls", counted_pause):
            ticks = run_coroutine(create_all_and_count_ticks())
        assert pauses, "the second CREATE INDEX did not wait for the first"
        assert ticks >= 10 * sum(pauses), f"{ticks} ticks of 50 ms while paused for {sum(pauses)} s"
