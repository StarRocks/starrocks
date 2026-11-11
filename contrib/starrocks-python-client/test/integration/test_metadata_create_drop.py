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

"""``MetaData.create_all()`` and ``drop_all()`` with views and materialized views next to tables."""

import logging

from sqlalchemy import Column, MetaData, Table, inspect, literal, select, testing, text
from sqlalchemy.schema import CreateTable
from sqlalchemy.testing import fixtures

from starrocks.datatype import INTEGER, VARCHAR
from starrocks.sql.schema import MaterializedView, View


class MetaDataCreateDropTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    @staticmethod
    def _names(connection):
        inspector = inspect(connection)
        return (
            set(inspector.get_table_names())
            | set(inspector.get_view_names())
            | set(inspector.get_materialized_view_names())
        )

    @testing.requires.sqlalchemy_2
    def test_create_all_and_drop_all(self, connection):
        metadata = MetaData()
        Table(
            "cd_base",
            metadata,
            Column("id", INTEGER),
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
        )
        View("cd_view", metadata, definition="SELECT id FROM cd_base")
        MaterializedView("cd_mv", metadata, definition="SELECT id FROM cd_base", starrocks_refresh="MANUAL")

        metadata.create_all(connection)
        assert {"cd_base", "cd_view", "cd_mv"} <= self._names(connection)

        metadata.drop_all(connection)
        assert not {"cd_base", "cd_view", "cd_mv"} & self._names(connection)

    @testing.requires.sqlalchemy_2
    def test_views_are_created_after_the_tables_they_read(self, connection):
        """The views and MV are defined before their base table, and ``create_all()`` keeps definition order."""
        metadata = MetaData()
        View("aa_view", metadata, definition="SELECT id FROM zz_base")
        View("ac_text_view", metadata, definition=text("SELECT id FROM zz_base"))
        MaterializedView("ab_mv", metadata, definition="SELECT id FROM zz_base", starrocks_refresh="MANUAL")
        Table(
            "zz_base",
            metadata,
            Column("id", INTEGER),
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
        )

        metadata.create_all(connection)
        assert {"zz_base", "aa_view", "ac_text_view", "ab_mv"} <= self._names(connection)

        metadata.drop_all(connection)
        assert not {"zz_base", "aa_view", "ac_text_view", "ab_mv"} & self._names(connection)

    @testing.requires.sqlalchemy_2
    def test_a_view_of_a_select_reads_its_columns(self, connection):
        """A ``select()`` definition names a mixed-case and a reserved column as StarRocks reads them, and keeps a
        literal's ``%``."""
        metadata = MetaData()
        base = Table(
            "cd_select_base",
            metadata,
            Column("Id", INTEGER),
            Column("select", VARCHAR(10)),
            starrocks_duplicate_key="Id",
            starrocks_distributed_by="HASH(Id) BUCKETS 1",
        )
        View("cd_select_view", metadata, definition=select(base.c.Id, base.c.select, literal("50%").label("pct")))
        metadata.create_all(connection)
        try:
            connection.execute(base.insert(), [{"Id": 1, "select": "s"}])
            assert connection.execute(text("SELECT * FROM cd_select_view")).all() == [(1, "s", "50%")]
        finally:
            metadata.drop_all(connection)


    @testing.requires.sqlalchemy_2
    def test_a_materialized_view_without_a_refresh_is_built_synchronously(self, connection, caplog):
        """StarRocks builds a rollup of its base table, which reflection can't see, so the dialect logs a warning.

        DROP MATERIALIZED VIEW can't find the rollup while StarRocks builds it, so the base table is dropped with it.
        StarRocks before 4.1.6 lists the ROLLUP jobs of every table in the database, dropped tables' included,
        whatever the WHERE says, so the rows are kept to the table's here.
        """
        metadata = MetaData()
        Table(
            "sync_base",
            metadata,
            Column("k", INTEGER),
            Column("v", INTEGER),
            starrocks_duplicate_key="k",
            starrocks_distributed_by="HASH(k) BUCKETS 1",
        )
        MaterializedView("sync_mv", metadata, definition="SELECT k, max(v) AS max_v FROM sync_base GROUP BY k")
        try:
            with caplog.at_level(logging.WARNING, logger="starrocks.dialect"):
                metadata.create_all(connection)
            assert "Materialized view 'sync_mv' has neither starrocks_refresh" in caplog.text
            rollups = connection.exec_driver_sql("SHOW ALTER TABLE ROLLUP WHERE TableName = 'sync_base'").mappings()
            assert [row["RollupIndexName"] for row in rollups if row["TableName"] == "sync_base"] == ["sync_mv"]
        finally:
            connection.exec_driver_sql("DROP TABLE IF EXISTS sync_base")


class ReflectedTableRecreateTest(fixtures.TestBase):
    """A reflected table's CREATE TABLE runs again on every version, with its partition, distribution and sort key."""

    __backend__ = True
    __only_on__ = "starrocks"

    def test_a_reflected_table_is_created_again(self, connection):
        connection.exec_driver_sql(
            "CREATE TABLE reflected_to_recreate (k INT, d DATE) DUPLICATE KEY(k) PARTITION BY date_trunc('day', d) "
            "DISTRIBUTED BY HASH(k) BUCKETS 2 ORDER BY (k) PROPERTIES('replication_num' = '1')"
        )
        try:
            reflected = Table("reflected_to_recreate", MetaData(), autoload_with=connection)
            connection.exec_driver_sql(
                str(CreateTable(reflected).compile(connection)).replace("reflected_to_recreate", "recreated", 1)
            )
            options = inspect(connection).get_table_options("recreated")
            assert "date_trunc" in str(options["starrocks_partition_by"]), options
        finally:
            connection.exec_driver_sql("DROP TABLE IF EXISTS recreated")
            connection.exec_driver_sql("DROP TABLE IF EXISTS reflected_to_recreate")

    @testing.combinations(("two_columns", "k, s"), ("one_column", "s"), argnames="partition_by", id_="ia")
    def test_a_reflected_column_list_partition_is_created_again(self, metadata, connection, partition_by):
        """StarRocks 3.3 reflects a column partition without its parentheses, and takes it back only with them."""
        Table(
            "partitioned_by_columns",
            metadata,
            Column("k", INTEGER),
            Column("s", VARCHAR(10)),
            starrocks_partition_by=partition_by,
            starrocks_distributed_by="HASH(k)",
            starrocks_properties={"replication_num": "1"},
        )
        metadata.create_all(connection)
        reflected = Table("partitioned_by_columns", MetaData(), autoload_with=connection)
        recreate = str(CreateTable(reflected).compile(connection)).replace(
            "partitioned_by_columns", "partitioned_by_columns_again", 1
        )
        connection.exec_driver_sql(recreate)
        try:
            options = inspect(connection).get_table_options("partitioned_by_columns_again")
            assert "`s`" in str(options["starrocks_partition_by"]), options
        finally:
            connection.exec_driver_sql("DROP TABLE IF EXISTS partitioned_by_columns_again")
