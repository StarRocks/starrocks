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

"""Secondary indexes: creating them from metadata, reflecting them, and migrations that drop indexed columns.

StarRocks drops a column's index together with the column, and runs each index or column change as an
asynchronous job that blocks the next one on the same table. Autogenerate emits both a ``drop_index`` and a
``drop_column`` for a removed indexed column; the dialect waits for the running job before each statement, and
the Alembic impl skips a ``drop_index`` whose index StarRocks dropped with a column earlier in the migration.
"""

import time

from alembic.autogenerate import api
from alembic.operations import Operations, ops as alembic_ops
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column, Index, MetaData, String, Table, exc, inspect, testing
from sqlalchemy.testing import config, expect_raises_message, fixtures

from starrocks import INTEGER
from starrocks.alembic.compare import combine_include_object
from starrocks.provision import wait_for_schema_changes


INDEXED_TABLE = "idx_reflect_t"


class SecondaryIndexReflectionTest(fixtures.TestBase):
    """BITMAP and other secondary indexes are reflected, and autogenerate handles indexed columns."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.fixture
    def indexed_table(self, connection):
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {INDEXED_TABLE}")
        connection.exec_driver_sql(
            f"""
            CREATE TABLE {INDEXED_TABLE} (
                id INT,
                old INT,
                keep INT,
                INDEX old_idx (old) USING BITMAP COMMENT 'old values'
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            """
        )
        yield INDEXED_TABLE
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {INDEXED_TABLE}")

    @staticmethod
    def _model(columns, indexes=()):
        metadata = MetaData()
        table = Table(
            INDEXED_TABLE,
            metadata,
            *[Column(name, INTEGER) for name in columns],
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        for name, column, kwargs in indexes:
            Index(name, table.c[column], **kwargs)
        return metadata

    @staticmethod
    def _migration(connection, metadata):
        def only_this_table(obj, name, type_, reflected, compare_to):
            return not (type_ == "table" and name != INDEXED_TABLE)

        context = MigrationContext.configure(
            connection=connection,
            opts={"target_metadata": metadata, "include_object": combine_include_object(only_this_table)},
        )
        return context, api.produce_migrations(context, metadata)

    @staticmethod
    def _run(context, container):
        operations = Operations(context)
        for outer in container.ops:
            for op in outer.ops if isinstance(outer, alembic_ops.ModifyTableOps) else [outer]:
                operations.invoke(op)

    @staticmethod
    def _state(connection):
        columns = [column["name"] for column in inspect(connection).get_columns(INDEXED_TABLE)]
        deadline = time.monotonic() + 60
        while True:
            indexes = [row[2] for row in connection.exec_driver_sql(f"SHOW INDEX FROM {INDEXED_TABLE}")]
            jobs = connection.exec_driver_sql(
                f"SHOW ALTER TABLE COLUMN WHERE TableName = '{INDEXED_TABLE}' ORDER BY JobId DESC LIMIT 1"
            ).mappings().all()
            if not jobs or jobs[0]["State"] in ("FINISHED", "CANCELLED") or time.monotonic() > deadline:
                return columns, indexes
            time.sleep(1)

    def test_bitmap_index_is_reflected(self, connection, indexed_table):
        reflected = [ix for ix in inspect(connection).get_indexes(indexed_table) if ix["name"] == "old_idx"]
        assert reflected == [
            {
                "name": "old_idx",
                "column_names": ["old"],
                "unique": False,
                "dialect_options": {"starrocks_using": "BITMAP", "starrocks_comment": "old values"},
            }
        ]

    def test_indexes_are_those_show_index_lists(self, connection):
        """SHOW CREATE TABLE prints comments as they are: a column comment holding a line that reads like an index,
        and an index comment holding a newline."""
        connection.exec_driver_sql(
            "CREATE TABLE idx_comment_lines_t (k INT NOT NULL, "
            "note VARCHAR(100) COMMENT 'first line\nINDEX ghost (`k`) USING BITMAP\nlast line', "
            "INDEX ix_note (note) USING BITMAP COMMENT 'index\ncomment') "
            "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')"
        )
        try:
            indexes = inspect(connection).get_indexes("idx_comment_lines_t")
        finally:
            connection.exec_driver_sql("DROP TABLE IF EXISTS idx_comment_lines_t")

        assert indexes == [
            {
                "name": "ix_note",
                "column_names": ["note"],
                "unique": False,
                "dialect_options": {"starrocks_using": "BITMAP", "starrocks_comment": "index\ncomment"},
            }
        ]

    @testing.requires.sqlalchemy_2
    def test_model_with_the_index_has_no_diff(self, connection, indexed_table):
        """Before indexes were reflected, every model Index came back as a new create_index on each run."""
        metadata = self._model(["id", "old", "keep"], [("old_idx", "old", {"starrocks_using": "BITMAP"})])
        _, script = self._migration(connection, metadata)
        assert script.upgrade_ops.is_empty()

    @testing.requires.sqlalchemy_2
    def test_dropping_an_indexed_column_applies_and_reverses(self, connection, indexed_table):
        """The upgrade drops the column and its index; the downgrade adds both back."""
        context, script = self._migration(connection, self._model(["id", "keep"]))
        kinds = {type(op).__name__ for outer in script.upgrade_ops.ops for op in getattr(outer, "ops", [outer])}
        assert {"DropColumnOp", "DropIndexOp"} <= kinds

        self._run(context, script.upgrade_ops)
        assert self._state(connection) == (["id", "keep"], [])

        self._run(context, script.downgrade_ops)
        columns, indexes = self._state(connection)
        assert sorted(columns) == ["id", "keep", "old"]
        assert indexes == ["old_idx"]

    @testing.combinations((False,), (True,), argnames="translated")
    def test_drop_index_after_its_column_was_dropped(self, connection, indexed_table, translated):
        """A hand-written migration may drop the column first; StarRocks has then dropped the index with it.

        Under a ``schema_translate_map`` of None to the connection's database, DROP COLUMN still names the table
        without a schema and DROP INDEX names it with the database, and the two are known to be one table.
        """
        if translated:
            database = connection.exec_driver_sql("SELECT DATABASE()").scalar()
            connection = connection.execution_options(schema_translate_map={None: database})
        operations = Operations(MigrationContext.configure(connection=connection))
        operations.drop_column(INDEXED_TABLE, "old")
        operations.drop_index("old_idx", table_name=INDEXED_TABLE)
        assert self._state(connection) == (["id", "keep"], [])

    def test_drop_index_if_exists_drops_an_index_that_exists(self, connection, indexed_table):
        """StarRocks has no DROP INDEX IF EXISTS; online, the impl checks first and drops without it."""
        operations = Operations(MigrationContext.configure(connection=connection))
        operations.drop_index("old_idx", table_name=INDEXED_TABLE, if_exists=True)
        assert self._state(connection)[1] == []
        operations.drop_index("old_idx", table_name=INDEXED_TABLE, if_exists=True)

    def test_drop_index_names_the_index_without_regard_to_case(self, connection, indexed_table):
        """StarRocks matches index names without regard to case, so a drop of OLD_IDX drops old_idx.

        With ``if_exists``, the impl looks for the index first, so it must match the name as StarRocks does too.
        """
        Operations(MigrationContext.configure(connection=connection)).drop_index(
            "OLD_IDX", table_name=INDEXED_TABLE, if_exists=True
        )
        assert self._state(connection)[1] == []

    def test_drop_index_of_a_missing_index_raises(self, connection, indexed_table):
        """Without if_exists, an index no column took with it is StarRocks' to refuse, as a misspelt name is."""
        operations = Operations(MigrationContext.configure(connection=connection))
        with expect_raises_message(exc.DBAPIError, "does not exist"):
            operations.drop_index("old_idx_misspelt", table_name=INDEXED_TABLE)
        assert self._state(connection)[1] == ["old_idx"]

    def test_drop_index_through_a_schema_translate_map(self, connection):
        """DROP INDEX reaches the schema the connection's schema_translate_map names, so the impl reads it there."""
        schema = config.test_schema
        connection.exec_driver_sql(
            f"CREATE TABLE {schema}.{INDEXED_TABLE} (id INT, old INT, INDEX old_idx (old) USING BITMAP) "
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')"
        )
        try:
            with config.db.begin() as translated:
                translated = translated.execution_options(schema_translate_map={None: schema})
                Operations(MigrationContext.configure(connection=translated)).drop_index(
                    "old_idx", table_name=INDEXED_TABLE
                )
            wait_for_schema_changes(connection, INDEXED_TABLE, schema)
            assert connection.exec_driver_sql(f"SHOW INDEX FROM {schema}.{INDEXED_TABLE}").all() == []
        finally:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {schema}.{INDEXED_TABLE}")


class SecondaryIndexCreateTest(fixtures.TestBase):
    """Indexes declared on a model are created with the StarRocks index type the model names."""

    __backend__ = True
    __only_on__ = "starrocks"

    def test_create_bitmap_index(self, connection, metadata):
        table = Table(
            "idx_create_t",
            metadata,
            Column("id", INTEGER),
            Column("city", String(20)),
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
        )
        Index("ix_city", table.c.city, starrocks_using="BITMAP")
        metadata.create_all(connection)

        assert self._indexes_once_built(connection, "idx_create_t") == [("ix_city", "city", "BITMAP")]

    def test_create_all_with_two_indexes_on_one_table(self, connection, metadata):
        """The second CREATE INDEX waits for the first index's build, which StarRocks runs as a schema change."""
        table = Table(
            "idx_two_t",
            metadata,
            Column("id", INTEGER),
            Column("city", String(20)),
            Column("country", String(20)),
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
        )
        Index("ix_city", table.c.city, starrocks_using="BITMAP")
        Index("ix_country", table.c.country, starrocks_using="BITMAP")
        metadata.create_all(connection)

        deadline = time.monotonic() + 60
        while len(self._indexes_once_built(connection, "idx_two_t")) < 2 and time.monotonic() < deadline:
            time.sleep(1)
        assert sorted(self._indexes_once_built(connection, "idx_two_t")) == [
            ("ix_city", "city", "BITMAP"),
            ("ix_country", "country", "BITMAP"),
        ]

    def test_create_ngrambf_index_with_properties_and_comment(self, connection, metadata):
        table = Table(
            "idx_ngram_t",
            metadata,
            Column("id", INTEGER),
            Column("name", String(50)),
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
        )
        Index(
            "ix_name",
            table.c.name,
            starrocks_using="NGRAMBF",
            starrocks_properties={"gram_num": "3", "bloom_filter_fpp": "0.01"},
            starrocks_comment="name's n-grams",
        )
        metadata.create_all(connection)

        self._indexes_once_built(connection, "idx_ngram_t")
        row = connection.exec_driver_sql("SHOW INDEX FROM idx_ngram_t").mappings().one()
        assert row["Index_type"].startswith("NGRAMBF(")
        assert '"gram_num" = "3"' in row["Index_type"]
        assert '"bloom_filter_fpp" = "0.01"' in row["Index_type"]
        assert row["Comment"] == "name's n-grams"

    @staticmethod
    def _indexes_once_built(connection, table_name, timeout=60.0):
        """Return the table's indexes as (name, column, type), waiting for the asynchronous index build."""
        deadline = time.monotonic() + timeout
        while True:
            rows = connection.exec_driver_sql(f"SHOW INDEX FROM {table_name}").mappings().all()
            if rows or time.monotonic() > deadline:
                return [(row["Key_name"], row["Column_name"], row["Index_type"]) for row in rows]
            time.sleep(1)
