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

"""Canary tests for StarRocks secondary-index reflection.

The Alembic column-coalescing rewriter (``starrocks.alembic.ops._combine_column_alters``)
folds every ADD/DROP COLUMN for a table into one ``ALTER TABLE`` and assumes it
never has to order a ``DROP INDEX`` against a column drop. That assumption holds
ONLY because the dialect does not reflect secondary indexes (bitmap, GIN, vector,
...): ``get_indexes`` returns just the table KEY, so autogenerate never emits a
``DropIndexOp`` for a secondary index and there is nothing to reorder.

If secondary-index reflection is added later, these tests fail on purpose — that
is the signal to revisit the rewriter, because Alembic could then emit
``AddColumnOp, DropIndexOp, DropColumnOp`` and the rewriter would need to keep
the ``DROP INDEX`` -> ``DROP COLUMN`` dependency (StarRocks drops a column's
index together with the column, so a standalone ``DROP INDEX`` afterward fails).
"""

import time

from alembic.autogenerate import api
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column, Index, MetaData, String, Table, inspect, testing
from sqlalchemy.testing import fixtures

from starrocks import INTEGER
from starrocks.alembic.compare import combine_include_object


INDEXED_TABLE = "idx_reflect_t"


class SecondaryIndexReflectionTest(fixtures.TestBase):
    """Guards the rewriter's assumption that secondary indexes are not reflected."""

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
                INDEX old_idx (old) USING BITMAP
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            """
        )
        yield INDEXED_TABLE
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {INDEXED_TABLE}")

    def test_bitmap_secondary_index_is_not_reflected(self, connection, indexed_table):
        """get_indexes must NOT surface the bitmap secondary index.

        Canary: when this starts failing, secondary-index reflection has been
        added and the coalescing rewriter must be revisited (see module docstring).
        """
        raw = [r[2] for r in connection.exec_driver_sql(f"SHOW INDEX FROM {indexed_table}")]
        assert "old_idx" in raw, "precondition: the bitmap index should exist in StarRocks"

        reflected_names = {ix.get("name") for ix in inspect(connection).get_indexes(indexed_table)}
        assert "old_idx" not in reflected_names, (
            "Secondary-index reflection appears to have been added: get_indexes now "
            "returns the bitmap index 'old_idx'. Revisit "
            "starrocks.alembic.ops._combine_column_alters — autogenerate can now emit "
            "a DropIndexOp that must not be reordered across a column drop."
        )

    @testing.requires.sqlalchemy_2
    def test_dropping_indexed_column_emits_no_drop_index(self, connection, indexed_table):
        """Autogenerate must emit no DropIndexOp when removing an indexed column.

        Because the bitmap index is not reflected, removing column ``old`` yields
        only a column drop. If a DropIndexOp appears here, the rewriter's
        no-reordering assumption is broken. The model has neither column ``old``
        nor any index.
        """
        md = MetaData()
        Table(
            indexed_table,
            md,
            Column("id", INTEGER),
            Column("keep", INTEGER),
            starrocks_duplicate_key="id",
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )

        def _only_this_table(obj, name, type_, reflected, compare_to):
            return not (type_ == "table" and name != indexed_table)

        mc = MigrationContext.configure(
            connection=connection,
            opts={"target_metadata": md, "include_object": combine_include_object(_only_this_table)},
        )
        script = api.produce_migrations(mc, md)

        all_ops = []
        for outer in script.upgrade_ops.ops:
            all_ops.append(outer)
            all_ops.extend(getattr(outer, "ops", []))

        drop_index_ops = [o for o in all_ops if type(o).__name__ == "DropIndexOp"]
        assert not drop_index_ops, (
            "autogenerate emitted a DropIndexOp for a secondary index; the "
            "coalescing rewriter must be updated to preserve DROP INDEX -> DROP "
            "COLUMN ordering (see starrocks.alembic.ops._combine_column_alters)."
        )


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
