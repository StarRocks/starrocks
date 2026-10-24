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

"""Autogenerate against tables whose metadata differs from the database in ways ALTER TABLE cannot express."""

import re
import warnings

from alembic.autogenerate import compare_metadata, produce_migrations
from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy
from sqlalchemy import Column, Computed, Float, LargeBinary, MetaData, Numeric, Table, exc, testing
from sqlalchemy.schema import CreateTable
from sqlalchemy.testing import engines, expect_raises_message, expect_warnings, fixtures

import starrocks.alembic  # noqa: F401
from starrocks.datatype import BIGINT, CHAR, DATE, DATETIME, INTEGER, STRING, VARCHAR
from test.util import (
    BUCKET_SIZE_NEEDS_RANDOM_DISTRIBUTION_FROM_4_1,
    GENERATED_COLUMN_IN_THE_DEFAULT_KEY_BEFORE_3_4,
    IN_MEMORY_PRIMARY_KEY_INDEX_REFUSED_FROM_3_5,
    PRIMARY_KEY_TABLE_SORTED_BY_BINARY_FROM_4_0,
    TIME_SLICE_CEIL_PARTITION_NEEDS_3_4_1,
)


class OmittedAttributesTest(fixtures.TestBase):
    """An attribute the metadata leaves out is left as the database has it, without aborting the run."""

    __backend__ = True
    __only_on__ = "starrocks"

    @staticmethod
    def _diffs(connection, metadata, prefix):
        context = MigrationContext.configure(
            connection,
            opts={"include_name": lambda name, type_, parents: type_ != "table" or name.startswith(prefix)},
        )
        return compare_metadata(context, metadata)

    @testing.requires.sqlalchemy_2
    def test_omitted_distribution_on_a_hash_table(self, connection):
        connection.exec_driver_sql(
            "CREATE TABLE unexpressible_hash (id INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
        )
        try:
            metadata = MetaData()
            Table("unexpressible_hash", metadata, Column("id", INTEGER), starrocks_duplicate_key="id")
            assert self._diffs(connection, metadata, "unexpressible_") == []
        finally:
            connection.exec_driver_sql("DROP TABLE unexpressible_hash")


class UnalterableChangesTest(fixtures.TestBase):
    """A change StarRocks cannot ALTER is warned about and skipped; the rest of the migration is still generated."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.requires.sqlalchemy_2
    def test_partition_change_does_not_stop_other_tables(self, connection, caplog):
        connection.exec_driver_sql(
            "CREATE TABLE unalterable_part (id INT, dt DATETIME) DUPLICATE KEY(id) "
            "PARTITION BY date_trunc('day', dt) DISTRIBUTED BY HASH(id) BUCKETS 1"
        )
        connection.exec_driver_sql(
            "CREATE TABLE unalterable_plain (id INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
        )
        try:
            metadata = MetaData()
            Table(
                "unalterable_part",
                metadata,
                Column("id", INTEGER),
                Column("dt", DATETIME),
                starrocks_duplicate_key="id",
                starrocks_partition_by="date_trunc('month', dt)",
                starrocks_distributed_by="HASH(id) BUCKETS 1",
            )
            Table(
                "unalterable_plain",
                metadata,
                Column("id", INTEGER),
                Column("added", INTEGER),
                starrocks_duplicate_key="id",
                starrocks_distributed_by="HASH(id) BUCKETS 1",
            )
            diffs = OmittedAttributesTest._diffs(connection, metadata, "unalterable_")
            assert [(diff[0], diff[2], diff[3].name) for diff in diffs] == [("add_column", "unalterable_plain", "added")]
            assert "StarRocks does not support 'ALTER TABLE PARTITION BY'" in caplog.text
        finally:
            connection.exec_driver_sql("DROP TABLE unalterable_part")
            connection.exec_driver_sql("DROP TABLE unalterable_plain")

    @testing.requires.sqlalchemy_2
    def test_the_generated_property_changes_apply(self, connection, caplog):
        """A property StarRocks refuses to SET on this server is warned about and skipped; the others are set."""
        connection.exec_driver_sql(
            "CREATE TABLE unalterable_props (id INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
            "PROPERTIES ('replication_num' = '1')"
        )
        try:
            metadata = MetaData()
            Table(
                "unalterable_props",
                metadata,
                Column("id", INTEGER),
                starrocks_duplicate_key="id",
                starrocks_distributed_by="HASH(id) BUCKETS 1",
                starrocks_properties={
                    "replication_num": "1",
                    "compression": "ZSTD",
                    "fast_schema_evolution": "false",
                    "enable_statistic_collect_on_first_load": "false",
                    "write_quorum": "ALL",
                },
            )
            context = MigrationContext.configure(
                connection,
                opts={"include_name": lambda name, type_, parents: type_ != "table" or name == "unalterable_props"},
            )
            operations = Operations(context)
            for op in produce_migrations(context, metadata).upgrade_ops.ops:
                for table_op in getattr(op, "ops", [op]):
                    operations.invoke(table_op)

            ddl = connection.exec_driver_sql("SHOW CREATE TABLE unalterable_props").fetchone()[1]
            assert '"write_quorum" = "ALL"' in ddl, ddl
            assert "would set property 'compression' to 'ZSTD'" in caplog.text
            assert "would set property 'fast_schema_evolution' to 'false'" in caplog.text
        finally:
            connection.exec_driver_sql("DROP TABLE unalterable_props")


class ColumnDefaultChangeTest(fixtures.TestBase):
    """A changed column default is warned about and skipped, since StarRocks cannot change a default."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.requires.sqlalchemy_2
    def test_default_change_is_skipped_and_other_changes_are_generated(self, connection, caplog):
        connection.exec_driver_sql(
            "CREATE TABLE default_change (id INT, s VARCHAR(10) DEFAULT 'a') DUPLICATE KEY(id) "
            "DISTRIBUTED BY HASH(id) BUCKETS 1"
        )
        try:
            metadata = MetaData()
            Table(
                "default_change",
                metadata,
                Column("id", INTEGER),
                Column("s", VARCHAR(10), server_default="b"),
                Column("added", INTEGER),
                starrocks_duplicate_key="id",
                starrocks_distributed_by="HASH(id) BUCKETS 1",
            )
            context = MigrationContext.configure(
                connection,
                opts={
                    "compare_server_default": True,
                    "include_name": lambda name, type_, parents: type_ != "table" or name == "default_change",
                },
            )
            diffs = compare_metadata(context, metadata)

            assert [(diff[0], diff[3].name) for diff in diffs] == [("add_column", "added")]
            assert "Detected DEFAULT change on column 'default_change.s'" in caplog.text
        finally:
            connection.exec_driver_sql("DROP TABLE default_change")


class TimeSlicePartitionTest(fixtures.TestBase):
    """A time_slice partition reflects in a different spelling from the one it was created with, and still compares equal."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.requires.sqlalchemy_2
    @testing.combinations(
        ("time_slice(dt, INTERVAL 7 DAY)", "time_slice(dt, INTERVAL 7 DAY)"),
        ("time_slice(dt, INTERVAL 7 DAY, floor)", "time_slice(dt, INTERVAL 7 DAY)"),
        (
            "time_slice(dt, INTERVAL 1 MONTH, ceil)",
            "time_slice(dt, INTERVAL 1 MONTH, ceil)",
            TIME_SLICE_CEIL_PARTITION_NEEDS_3_4_1,
        ),
        argnames="partition_by, model_partition_by",
    )
    def test_no_diff_after_create(self, connection, caplog, partition_by, model_partition_by):
        connection.exec_driver_sql(
            f"CREATE TABLE time_slice_part (id INT, dt DATETIME) DUPLICATE KEY(id) "
            f"PARTITION BY {partition_by} DISTRIBUTED BY HASH(id) BUCKETS 1"
        )
        try:
            metadata = MetaData()
            Table(
                "time_slice_part",
                metadata,
                Column("id", INTEGER),
                Column("dt", DATETIME),
                starrocks_duplicate_key="id",
                starrocks_partition_by=model_partition_by,
                starrocks_distributed_by="HASH(id) BUCKETS 1",
            )
            assert OmittedAttributesTest._diffs(connection, metadata, "time_slice_part") == []
            assert "does not support 'ALTER TABLE PARTITION BY'" not in caplog.text
        finally:
            connection.exec_driver_sql("DROP TABLE time_slice_part")


class PrimaryKeyInferenceOffTest(fixtures.TestBase):
    """By default ``primary_key=True`` columns leave a table with no ``starrocks_*_key`` a DUPLICATE KEY table, as 1.3
    did, and autogenerate leaves the key of any table alone.
    """

    __backend__ = True
    __only_on__ = "starrocks"

    @classmethod
    def setup_test_class(cls):
        cls.bind = engines.testing_engine(options={"starrocks_infer_primary_key": False, "scope": "class"})

    @testing.fixture
    def shape_table(self, connection):
        yield "pk_shape"
        connection.exec_driver_sql("DROP TABLE IF EXISTS pk_shape FORCE")

    @testing.combinations(("create_all",), ("create_table_op",), argnames="path")
    def test_a_table_the_inference_would_key_is_a_duplicate_key_table(self, connection, shape_table, path):
        columns = [Column("id", INTEGER, primary_key=True), Column("v", INTEGER)]
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            InferredPrimaryKeyTest._create(connection, path, columns, {})
        assert InferredPrimaryKeyTest._key_of(connection, shape_table) == "DUPLICATE KEY"
        connection.exec_driver_sql("INSERT INTO pk_shape VALUES (1, 1), (1, 2)")
        assert connection.exec_driver_sql("SELECT count(*) FROM pk_shape").scalar() == 2

    @testing.requires.sqlalchemy_2
    def test_implied_key_is_not_compared_with_a_primary_key_table(self, connection, caplog):
        """The model implies (id, v), and CREATE TABLE would give it no key, so the table's key (id) is left alone."""
        connection.exec_driver_sql(
            "CREATE TABLE inferred_pk (id INT NOT NULL, v INT NOT NULL, w INT) PRIMARY KEY(id) "
            "DISTRIBUTED BY HASH(id) BUCKETS 1"
        )
        try:
            metadata = MetaData()
            Table(
                "inferred_pk",
                metadata,
                Column("id", INTEGER, primary_key=True),
                Column("v", INTEGER, primary_key=True),
                Column("w", INTEGER),
                starrocks_distributed_by="HASH(id) BUCKETS 1",
            )
            assert OmittedAttributesTest._diffs(connection, metadata, "inferred_pk") == []
            assert "different key" not in caplog.text
        finally:
            connection.exec_driver_sql("DROP TABLE inferred_pk")


class InferredPrimaryKeyTest(fixtures.TestBase):
    """With ``starrocks_infer_primary_key`` on, ``primary_key=True`` columns make a table with no ``starrocks_*_key``
    a PRIMARY KEY table where StarRocks can.

    Each shape StarRocks refuses as a PRIMARY KEY table is created with no key clause, as a DUPLICATE KEY table.
    Autogenerate compares the key only with a PRIMARY KEY table; any other key model is left as the database has it.
    """

    __backend__ = True
    __only_on__ = "starrocks"

    @classmethod
    def setup_test_class(cls):
        cls.bind = engines.testing_engine(options={"starrocks_infer_primary_key": True, "scope": "class"})

    @staticmethod
    def _model(metadata):
        return Table("inferred_pk", metadata, Column("id", INTEGER, primary_key=True), Column("v", INTEGER))

    @testing.requires.sqlalchemy_2
    def test_created_as_primary_key_table_with_no_diff(self, connection, metadata):
        self._model(metadata)
        metadata.create_all(connection)

        ddl = connection.exec_driver_sql("SHOW CREATE TABLE inferred_pk").fetchone()[1]
        assert "PRIMARY KEY(`id`)" in ddl
        assert OmittedAttributesTest._diffs(connection, metadata, "inferred_pk") == []

    @testing.requires.sqlalchemy_2
    def test_key_columns_that_do_not_lead_make_a_duplicate_key_table(self, connection, metadata, caplog):
        Table("inferred_pk", metadata, Column("v", INTEGER), Column("id", INTEGER, primary_key=True))
        with expect_warnings(r"Table 'inferred_pk' has the primary key \(id\)"):
            metadata.create_all(connection)

        ddl = connection.exec_driver_sql("SHOW CREATE TABLE inferred_pk").fetchone()[1]
        assert "DUPLICATE KEY(`v`" in ddl
        assert OmittedAttributesTest._diffs(connection, metadata, "inferred_pk") == []
        assert "different key" not in caplog.text

    @testing.requires.sqlalchemy_2
    def test_existing_duplicate_key_table_is_left_alone(self, connection, caplog):
        """A table an older dialect created as DUPLICATE KEY from the same model gives no warning and no diff."""
        connection.exec_driver_sql("CREATE TABLE inferred_pk (id INT NOT NULL, v INT) DUPLICATE KEY(id, v)")
        try:
            metadata = MetaData()
            self._model(metadata)
            assert OmittedAttributesTest._diffs(connection, metadata, "inferred_pk") == []
            assert "different key" not in caplog.text
        finally:
            connection.exec_driver_sql("DROP TABLE inferred_pk")

    @testing.fixture
    def shape_table(self, connection):
        yield "pk_shape"
        connection.exec_driver_sql("DROP TABLE IF EXISTS pk_shape FORCE")

    @staticmethod
    def _create(connection, path, columns, table_kwargs):
        if path == "create_all":
            metadata = MetaData()
            Table("pk_shape", metadata, *columns, **table_kwargs)
            metadata.create_all(connection)
            return metadata
        Operations(MigrationContext.configure(connection)).create_table("pk_shape", *columns, **table_kwargs)
        return None

    @staticmethod
    def _key_of(connection, table_name):
        ddl = connection.exec_driver_sql(f"SHOW CREATE TABLE {table_name}").fetchone()[1]
        return re.search(r"^(\w+ KEY)\(", ddl, re.MULTILINE).group(1)

    @testing.combinations(
        ("char_key", lambda: [Column("id", CHAR(36), primary_key=True), Column("v", INTEGER)], {}, "key column id is CHAR"),
        (
            "uuid_key",
            lambda: [Column("id", sqlalchemy.Uuid, primary_key=True), Column("v", INTEGER)],
            {},
            "key column id is CHAR",
            testing.requires.sqlalchemy_2,
        ),
        (
            "decimal_key",
            lambda: [Column("amount", Numeric(10, 2), primary_key=True), Column("v", INTEGER)],
            {},
            "key column amount is NUMERIC",
        ),
        (
            "binary_key_column",
            lambda: [Column("id", INTEGER, primary_key=True), Column("b", LargeBinary, primary_key=True)],
            {},
            "key column b is BINARY",
        ),
        (
            "generated_key_column",
            lambda: [
                Column("id", INTEGER, primary_key=True),
                Column("g", BIGINT, Computed("id + 1"), primary_key=True, nullable=True),
            ],
            {},
            "key column g is a generated column",
            GENERATED_COLUMN_IN_THE_DEFAULT_KEY_BEFORE_3_4,
        ),
        (
            "expression_partition",
            lambda: [Column("id", INTEGER, primary_key=True), Column("dt", DATE, nullable=False)],
            {"starrocks_partition_by": "date_trunc('day', dt)"},
            "PARTITION BY uses dt, not a key column",
        ),
        (
            "range_partition",
            lambda: [Column("id", INTEGER, primary_key=True), Column("dt", DATE, nullable=False)],
            {"starrocks_partition_by": "RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2026-01-01'))"},
            "PARTITION BY uses dt, not a key column",
        ),
        (
            "column_partition",
            lambda: [Column("id", INTEGER, primary_key=True), Column("region", VARCHAR(20), nullable=False)],
            {"starrocks_partition_by": "region"},
            "PARTITION BY uses region, not a key column",
        ),
        (
            "hash_distribution",
            lambda: [Column("id", INTEGER, primary_key=True), Column("user_id", INTEGER)],
            {"starrocks_distributed_by": "HASH(user_id)"},
            "DISTRIBUTED BY HASH uses user_id, not a key column",
        ),
        (
            "random_distribution",
            lambda: [Column("id", INTEGER, primary_key=True), Column("v", INTEGER)],
            {"starrocks_distributed_by": "RANDOM"},
            "it is distributed by RANDOM",
        ),
        (
            "sort_key_type",
            lambda: [Column("id", INTEGER, primary_key=True), Column("amount", Numeric(10, 2))],
            {"starrocks_order_by": "amount"},
            "ORDER BY uses amount, which is NUMERIC",
        ),
        (
            "cooldown_property",
            lambda: [Column("id", INTEGER, primary_key=True), Column("v", INTEGER)],
            {"starrocks_properties": {"storage_medium": "SSD", "storage_cooldown_ttl": "1 day"}},
            "StarRocks refuses the storage_cooldown_ttl property",
        ),
        (
            "compaction_property",
            lambda: [Column("id", INTEGER, primary_key=True), Column("v", INTEGER)],
            {"starrocks_properties": {"base_compaction_forbidden_time_ranges": "* * * * *"}},
            "StarRocks refuses the base_compaction_forbidden_time_ranges property",
        ),
        (
            "in_memory_index",
            lambda: [Column("id", INTEGER, primary_key=True), Column("v", INTEGER)],
            {"starrocks_properties": {"enable_persistent_index": "false"}},
            "enable_persistent_index is false",
        ),
        (
            "bucket_size_with_the_default_distribution",
            lambda: [Column("id", INTEGER, primary_key=True), Column("v", INTEGER)],
            {"starrocks_properties": {"bucket_size": "1073741824"}},
            "StarRocks 4.1 allows bucket_size only with RANDOM distribution",
        ),
        (
            "key_wider_than_the_size_limit",
            lambda: [Column("name", VARCHAR(255), primary_key=True), Column("v", INTEGER)],
            {},
            "its key columns can take 255 bytes a row",
        ),
        (
            "string_key",
            lambda: [Column("name", STRING, primary_key=True), Column("v", INTEGER)],
            {},
            "key column name is STRING, which has no length",
        ),
        (
            "nullable_key",
            lambda: [
                Column("tenant", INTEGER, primary_key=True),
                Column("ref", INTEGER, primary_key=True, nullable=True),
                Column("v", INTEGER),
            ],
            {},
            "key column ref is nullable",
        ),
        argnames="columns, table_kwargs, obstacle",
        id_="iaaa",
    )
    @testing.combinations(("create_all",), ("create_table_op",), argnames="path")
    def test_shape_starrocks_cannot_hold_as_a_primary_key_table_is_a_duplicate_key_table(
        self, connection, shape_table, path, columns, table_kwargs, obstacle
    ):
        """StarRocks refuses each shape as a PRIMARY KEY table, so the dialect warns and sends no key clause."""
        with expect_warnings(
            r"Table 'pk_shape' has the primary key \(.*\) and names no starrocks_\*_key, but it can't be made a "
            r"PRIMARY KEY table: " + re.escape(obstacle)
        ):
            self._create(connection, path, columns(), table_kwargs)
        assert self._key_of(connection, shape_table) == "DUPLICATE KEY"

    @testing.combinations(
        (
            "key_not_first_put_first",
            lambda: [Column("name", VARCHAR(20)), Column("id", INTEGER, primary_key=True)],
            {},
            "put_first",
            "PRIMARY KEY",
        ),
        (
            "key_not_first_filtered",
            lambda: [Column("name", VARCHAR(20)), Column("id", INTEGER, primary_key=True)],
            {},
            "filter",
            "DUPLICATE KEY",
        ),
        (
            "char_key",
            lambda: [Column("id", CHAR(36), primary_key=True), Column("v", INTEGER)],
            {},
            "name",
            "DUPLICATE KEY",
        ),
        (
            "nullable_key",
            lambda: [Column("t", INTEGER, primary_key=True), Column("r", INTEGER, primary_key=True, nullable=True)],
            {},
            "name",
            "DUPLICATE KEY",
        ),
        (
            "partition_outside_the_key",
            lambda: [Column("id", INTEGER, primary_key=True), Column("dt", DATE, nullable=False)],
            {"starrocks_partition_by": "date_trunc('day', dt)"},
            "name",
            "DUPLICATE KEY",
        ),
        (
            "hash_outside_the_key",
            lambda: [Column("id", INTEGER, primary_key=True), Column("user_id", INTEGER)],
            {"starrocks_distributed_by": "HASH(user_id)"},
            "name",
            "DUPLICATE KEY",
        ),
        (
            "random_distribution",
            lambda: [Column("id", INTEGER, primary_key=True), Column("v", INTEGER)],
            {"starrocks_distributed_by": "RANDOM"},
            "name",
            "DUPLICATE KEY",
        ),
        (
            "key_wider_than_the_size_limit",
            lambda: [Column("name", VARCHAR(255), primary_key=True), Column("v", INTEGER)],
            {},
            "name",
            "DUPLICATE KEY",
        ),
        (
            "float_key_column",
            lambda: [Column("id", INTEGER, primary_key=True), Column("f", Float, primary_key=True)],
            {},
            "filter",
            "DUPLICATE KEY",
        ),
        (
            "binary_key_column",
            lambda: [Column("id", INTEGER, primary_key=True), Column("b", LargeBinary, primary_key=True)],
            {},
            "filter",
            "DUPLICATE KEY",
        ),
        argnames="columns, table_kwargs, remedy, key",
        id_="iaaaa",
    )
    def test_the_warning_advice_is_ddl_starrocks_takes(
        self, connection, shape_table, columns, table_kwargs, remedy, key
    ):
        """The advice the warning gives makes the table a PRIMARY KEY table, or keeps it as it is without the warning.

        StarRocks needs any key's columns first, so for key columns that don't lead, naming starrocks_duplicate_key
        would be refused too. It refuses a FLOAT key column in any key model, and 3.3 a BINARY one.
        """
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            CreateTable(Table("pk_shape", MetaData(), *columns(), **table_kwargs)).compile(dialect=connection.dialect)
        message = str(caught[0].message)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            if remedy == "put_first":
                assert "Put the key columns first, in key order" in message, message
                created = sorted(columns(), key=lambda column: not column.primary_key)
                self._create(connection, "create_all", created, table_kwargs)
            elif remedy == "name":
                advised_key = re.search(r'starrocks_duplicate_key="([^"]*)"', message).group(1)
                key_model = {"starrocks_duplicate_key": advised_key}
                self._create(connection, "create_all", columns(), {**table_kwargs, **key_model})
            else:
                eval(re.search(r"warnings\.filterwarnings\(.*\)$", message).group(0))
                self._create(connection, "create_all", columns(), table_kwargs)
        assert self._key_of(connection, shape_table) == key

    @testing.combinations(
        (
            "key_not_first",
            lambda: [Column("w", INTEGER), Column("id", INTEGER, primary_key=True)],
            {},
            "Key columns must be the first few columns",
        ),
        (
            "char_key",
            lambda: [Column("id", CHAR(36), primary_key=True)],
            {},
            r"primary key column\[id\] type not supported",
        ),
        (
            "uuid_key",
            lambda: [Column("id", sqlalchemy.Uuid, primary_key=True)],
            {},
            r"primary key column\[id\] type not supported",
            testing.requires.sqlalchemy_2,
        ),
        (
            "decimal_key",
            lambda: [Column("id", Numeric(10, 2), primary_key=True)],
            {},
            r"primary key column\[id\] type not supported",
        ),
        (
            "binary_key_column",
            lambda: [Column("id", INTEGER, primary_key=True), Column("b", LargeBinary, primary_key=True)],
            {},
            r"primary key column\[b\] type not supported",
        ),
        (
            "generated_key_column",
            lambda: [
                Column("id", INTEGER, primary_key=True),
                Column("g", BIGINT, Computed("id + 1"), primary_key=True, nullable=True),
            ],
            {},
            "Generated Column .*can not be KEY",
        ),
        (
            "expression_partition",
            lambda: [Column("id", INTEGER, primary_key=True), Column("dt", DATE, nullable=False)],
            {"starrocks_partition_by": "date_trunc('day', dt)"},
            "partition column must be key column",
        ),
        (
            "range_partition",
            lambda: [Column("id", INTEGER, primary_key=True), Column("dt", DATE, nullable=False)],
            {"starrocks_partition_by": "RANGE(dt) (PARTITION p1 VALUES LESS THAN ('2026-01-01'))"},
            "partition column must be key column",
        ),
        (
            "column_partition",
            lambda: [Column("id", INTEGER, primary_key=True), Column("region", VARCHAR(20), nullable=False)],
            {"starrocks_partition_by": "region"},
            "partition column must be key column",
        ),
        (
            "hash_distribution",
            lambda: [Column("id", INTEGER, primary_key=True), Column("user_id", INTEGER)],
            {"starrocks_distributed_by": "HASH(user_id)"},
            r"Distribution column\[user_id\] is not key column",
        ),
        (
            "random_distribution",
            lambda: [Column("id", INTEGER, primary_key=True)],
            {"starrocks_distributed_by": "RANDOM"},
            "PRIMARY KEY (cannot use random|must use hash) distribution",
        ),
        (
            "sort_key_type",
            lambda: [Column("id", INTEGER, primary_key=True), Column("amount", Numeric(10, 2))],
            {"starrocks_order_by": "amount"},
            r"sort key column\[amount\] type not supported",
        ),
        (
            "binary_sort_key",
            lambda: [Column("id", INTEGER, primary_key=True), Column("b", LargeBinary)],
            {"starrocks_order_by": "b"},
            r"sort key column\[b\] type not supported",
            PRIMARY_KEY_TABLE_SORTED_BY_BINARY_FROM_4_0,
        ),
        (
            "cooldown_property",
            lambda: [Column("id", INTEGER, primary_key=True)],
            {"starrocks_properties": {"storage_medium": "SSD", "storage_cooldown_ttl": "1 day"}},
            "does not support storage medium cool down",
        ),
        (
            "compaction_property",
            lambda: [Column("id", INTEGER, primary_key=True)],
            {"starrocks_properties": {"base_compaction_forbidden_time_ranges": "* * * * *"}},
            "base_compaction_forbidden_time_ranges not support primary keys table",
        ),
        (
            "in_memory_index",
            lambda: [Column("id", INTEGER, primary_key=True)],
            {"starrocks_properties": {"enable_persistent_index": "false"}},
            "In-Memory index is not supported",
            IN_MEMORY_PRIMARY_KEY_INDEX_REFUSED_FROM_3_5,
        ),
        (
            "bucket_size_with_the_default_distribution",
            lambda: [Column("id", INTEGER, primary_key=True)],
            {"starrocks_properties": {"bucket_size": "1073741824"}},
            "Setting bucket size is not allowed",
            BUCKET_SIZE_NEEDS_RANDOM_DISTRIBUTION_FROM_4_1,
        ),
        argnames="columns, table_kwargs, refusal",
        id_="iaaa",
    )
    def test_starrocks_refuses_the_shape_as_a_primary_key_table(
        self, connection, shape_table, columns, table_kwargs, refusal
    ):
        """Each shape the dialect leaves a DUPLICATE KEY table, named a PRIMARY KEY table, is refused by StarRocks.

        The shapes StarRocks refuses only at load are in the next test.
        """
        created = columns()
        key = ", ".join(column.name for column in created if column.primary_key)
        with expect_raises_message(exc.DBAPIError, refusal):
            self._create(connection, "create_all", created, {**table_kwargs, "starrocks_primary_key": key})

    @testing.combinations(
        (
            "nullable_key",
            lambda: [Column("t", INTEGER, primary_key=True), Column("r", INTEGER, primary_key=True, nullable=True)],
            {"t": 1, "r": None},
        ),
        ("key_wider_than_the_size_limit", lambda: [Column("s", VARCHAR(255), primary_key=True)], {"s": "x" * 129}),
        ("string_key", lambda: [Column("s", STRING, primary_key=True)], {"s": "x" * 129}),
        (
            "auto_increment_and_varchar_over_the_size_limit",
            lambda: [
                Column("id", INTEGER, primary_key=True, autoincrement=True),
                Column("s", VARCHAR(122), primary_key=True),
            ],
            {"s": "x" * 122},
        ),
        argnames="columns, row",
        id_="iaa",
    )
    def test_starrocks_refuses_rows_of_the_shape_as_a_primary_key_table(self, connection, shape_table, columns, row):
        """StarRocks makes a PRIMARY KEY column NOT NULL, and loads no row whose key takes more than 128 bytes."""
        created = [*columns(), Column("v", INTEGER)]
        key = ", ".join(column.name for column in created if column.primary_key)
        metadata = self._create(connection, "create_all", created, {"starrocks_primary_key": key})
        with expect_raises_message(exc.DBAPIError, "filtered data|exceed the limit"):
            connection.execute(metadata.tables["pk_shape"].insert(), [{**row, "v": 1}])

    @testing.combinations(
        (
            "partition_on_the_key",
            lambda: [
                Column("id", INTEGER, primary_key=True),
                Column("dt", DATE, primary_key=True),
                Column("v", INTEGER),
            ],
            {"starrocks_partition_by": "date_trunc('day', dt)"},
        ),
        (
            "hash_on_the_key_in_another_case",
            lambda: [Column("id", INTEGER, primary_key=True), Column("v", INTEGER)],
            {"starrocks_distributed_by": "HASH(`ID`) BUCKETS 2"},
        ),
        (
            "sort_key_of_a_value_column",
            lambda: [Column("id", INTEGER, primary_key=True), Column("name", VARCHAR(20))],
            {"starrocks_order_by": "name"},
        ),
        (
            "interval_unit_named_like_a_column",
            lambda: [
                Column("id", INTEGER, primary_key=True),
                Column("dt", DATETIME, primary_key=True),
                Column("day", INTEGER),
            ],
            {"starrocks_partition_by": "time_slice(dt, INTERVAL 1 day)"},
        ),
        (
            "interval_unit_and_boundary_named_like_columns",
            lambda: [
                Column("id", INTEGER, primary_key=True),
                Column("dt", DATETIME, primary_key=True),
                Column("week", INTEGER),
                Column("ceil", INTEGER),
            ],
            {"starrocks_partition_by": "time_slice(dt, INTERVAL 1 week, ceil)"},
            TIME_SLICE_CEIL_PARTITION_NEEDS_3_4_1,
        ),
        (
            "nullable_auto_increment_key",
            lambda: [Column("id", INTEGER, primary_key=True, autoincrement=True, nullable=True), Column("v", INTEGER)],
            {},
        ),
        argnames="columns, table_kwargs",
        id_="iaa",
    )
    @testing.combinations(("create_all",), ("create_table_op",), argnames="path")
    def test_shape_starrocks_holds_as_a_primary_key_table(self, connection, shape_table, path, columns, table_kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            self._create(connection, path, columns(), table_kwargs)
        assert self._key_of(connection, shape_table) == "PRIMARY KEY"

    @testing.combinations(
        (
            "varchar_and_int",
            lambda: [Column("s", VARCHAR(100), primary_key=True), Column("id", INTEGER, primary_key=True)],
            {"s": "x" * 100, "id": 1},
        ),
        ("varchar_alone", lambda: [Column("s", VARCHAR(128), primary_key=True)], {"s": "x" * 128}),
        ("multibyte_characters", lambda: [Column("s", VARCHAR(128), primary_key=True)], {"s": "\u20ac" * 42 + "xx"}),
        (
            "varchar_before_int",
            lambda: [Column("s", VARCHAR(122), primary_key=True), Column("id", INTEGER, primary_key=True)],
            {"s": "x" * 122, "id": 1},
        ),
        (
            "int_then_varchar",
            lambda: [Column("id", INTEGER, primary_key=True), Column("s", VARCHAR(124), primary_key=True)],
            {"id": 1, "s": "x" * 124},
        ),
        argnames="columns, widest_row",
        id_="iaa",
    )
    def test_a_key_that_fits_the_size_limit_loads_its_widest_row(self, connection, shape_table, columns, widest_row):
        """StarRocks loads no row whose primary key takes more than 128 bytes; a VARCHAR(n) holds n bytes at most.

        A VARCHAR before another key column counts 2 bytes more. Each table's widest row takes 128 bytes or fewer.
        """
        metadata = MetaData()
        table = Table("pk_shape", metadata, *columns(), Column("v", INTEGER))
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            metadata.create_all(connection)
        assert self._key_of(connection, shape_table) == "PRIMARY KEY"
        connection.execute(table.insert(), [{**widest_row, "v": 1}])
        assert connection.execute(table.select()).all() == [tuple({**widest_row, "v": 1}[c.name] for c in table.c)]

    @testing.requires.sqlalchemy_2
    @testing.combinations(
        ("refused", {"starrocks_distributed_by": "HASH(v)"}, "DUPLICATE KEY"),
        ("held", {"starrocks_distributed_by": "HASH(id)"}, "PRIMARY KEY"),
        argnames="table_kwargs, key",
        id_="iaa",
    )
    def test_autogenerate_agrees_with_the_table_create_all_made(
        self, connection, shape_table, caplog, table_kwargs, key
    ):
        """The comparison infers the key just where CREATE TABLE does, so neither table gives a diff."""
        columns = [Column("id", INTEGER, primary_key=True), Column("v", INTEGER, nullable=False)]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            metadata = self._create(connection, "create_all", columns, table_kwargs)
        assert self._key_of(connection, shape_table) == key
        assert OmittedAttributesTest._diffs(connection, metadata, shape_table) == []
        assert "different key" not in caplog.text

    @testing.requires.sqlalchemy_2
    def test_a_nullable_key_loads_its_null_rows_and_gives_no_diff(self, connection, shape_table):
        """StarRocks would make the key column of a PRIMARY KEY table NOT NULL and refuse the row whose ref is NULL."""
        columns = [
            Column("tenant", INTEGER, primary_key=True),
            Column("ref", INTEGER, primary_key=True, nullable=True),
            Column("v", INTEGER),
        ]
        with expect_warnings(r"Table 'pk_shape' .* key column ref is nullable"):
            metadata = self._create(connection, "create_all", columns, {})
        assert self._key_of(connection, shape_table) == "DUPLICATE KEY"
        connection.exec_driver_sql("INSERT INTO pk_shape VALUES (1, NULL, 1)")
        assert connection.exec_driver_sql("SELECT tenant, ref, v FROM pk_shape").all() == [(1, None, 1)]
        assert OmittedAttributesTest._diffs(connection, metadata, shape_table) == []

    @testing.requires.sqlalchemy_2
    def test_implied_key_is_compared_with_a_primary_key_table(self, connection, caplog):
        """The model implies the key (id, v) and the table has (id); StarRocks cannot change a key, so this warns."""
        connection.exec_driver_sql(
            "CREATE TABLE inferred_pk (id INT NOT NULL, v INT NOT NULL, w INT) PRIMARY KEY(id) "
            "DISTRIBUTED BY HASH(id) BUCKETS 1"
        )
        try:
            metadata = MetaData()
            Table(
                "inferred_pk",
                metadata,
                Column("id", INTEGER, primary_key=True),
                Column("v", INTEGER, primary_key=True),
                Column("w", INTEGER),
                starrocks_distributed_by="HASH(id) BUCKETS 1",
            )
            assert OmittedAttributesTest._diffs(connection, metadata, "inferred_pk") == []
            assert "has different key columns: (id) to (id, v)" in caplog.text
        finally:
            connection.exec_driver_sql("DROP TABLE inferred_pk")
