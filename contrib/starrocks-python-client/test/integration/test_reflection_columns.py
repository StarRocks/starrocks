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

from sqlalchemy import Column, Integer, MetaData, String, Table, inspect, testing
from sqlalchemy.testing import expect_warnings, fixtures

from starrocks.common.params import ColumnAggInfoKey, DialectName, TableInfoKeyWithPrefix
from starrocks.common.types import ColumnAggType
from starrocks.datatype import BITMAP, HLL, PERCENTILE
from test.util import normalize_sql


class ReflectionColumnsAggTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    def test_reflect_aggregate_key_table(self, connection, metadata):
        Table(
            "test_reflect_columns",
            metadata,
            Column("id", Integer),
            Column("key1", String(32), starrocks_IS_AGG_KEY=True),
            Column("val_sum", Integer, starrocks_agg_type="sum"),
            Column("val_min", Integer, starrocks_agg_type="Min"),
            Column("val_max", Integer, starrocks_agg_type=ColumnAggType.MAX),
            Column("val_replace", String(32), starrocks_agg_type=ColumnAggType.REPLACE),
            Column("val_replace_if_not_null", Integer, starrocks_agg_type=ColumnAggType.REPLACE_IF_NOT_NULL),
            Column("val_hll", HLL, starrocks_agg_type=ColumnAggType.HLL_UNION),
            Column("val_bitmap", BITMAP, starrocks_agg_type=ColumnAggType.BITMAP_UNION),
            Column("val_percentile", PERCENTILE, starrocks_agg_type=ColumnAggType.PERCENTILE_UNION),
            starrocks_engine="OLAP",
            starrocks_aggregate_key="id, key1",
            starrocks_distributed_by="HASH(id)",
            starrocks_properties={"replication_num": "1"},
        )
        metadata.create_all(connection)

        reflected = Table("test_reflect_columns", MetaData(), autoload_with=connection)
        agg_types = {
            column.name: column.dialect_options[DialectName].get(ColumnAggInfoKey.AGG_TYPE)
            for column in reflected.columns
        }

        assert agg_types == {
            "id": None,
            "key1": None,
            "val_sum": ColumnAggType.SUM,
            "val_min": ColumnAggType.MIN,
            "val_max": ColumnAggType.MAX,
            "val_replace": ColumnAggType.REPLACE,
            "val_replace_if_not_null": ColumnAggType.REPLACE_IF_NOT_NULL,
            "val_hll": ColumnAggType.HLL_UNION,
            "val_bitmap": ColumnAggType.BITMAP_UNION,
            "val_percentile": ColumnAggType.PERCENTILE_UNION,
        }


class ReflectionColumnAutoIncrementTest(fixtures.TestBase):
    """The tables are created with plain DDL, so reflection is checked against what the server reports."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.fixture
    def table_from_ddl(self, connection):
        created = []

        def create(name: str, id_column: str) -> str:
            connection.exec_driver_sql(
                f"CREATE TABLE {name} ({id_column}, v INT) DISTRIBUTED BY RANDOM PROPERTIES(\"replication_num\"=\"1\")"
            )
            created.append(name)
            return name

        yield create
        for name in created:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

    @testing.combinations(
        ("autoincrement", "id BIGINT NOT NULL AUTO_INCREMENT", True),
        ("no_autoincrement", "id BIGINT NOT NULL", False),
        ("named_in_the_comment", "id INT NULL COMMENT 'not AUTO_INCREMENT'", False),
        ("named_in_the_default", "id VARCHAR(20) NULL DEFAULT 'AUTO_INCREMENT'", False),
        argnames="id_column, expected",
        id_="iaa",
    )
    def test_reflects_autoincrement(self, connection, table_from_ddl, id_column, expected):
        table_name = table_from_ddl("reflect_columns_ai", id_column)
        id_col = next(c for c in inspect(connection).get_columns(table_name) if c["name"].lower() == "id")
        assert id_col["autoincrement"] is expected

    def test_a_struct_field_named_auto_increment(self, connection, table_from_ddl):
        """StarRocks prints the field's name in the column's type, backquoted from 4.1 and bare in 3.3."""
        table_name = table_from_ddl("reflect_columns_ai_struct", "k INT, s STRUCT<auto_increment INT, b INT> NULL")
        s_col = next(c for c in inspect(connection).get_columns(table_name) if c["name"] == "s")
        assert s_col["autoincrement"] is False

    def test_columns_after_a_comment_holding_a_quote(self, connection):
        """StarRocks 3.3 leaves the ``"`` of a PRIMARY KEY table's column comment unescaped in SHOW CREATE TABLE."""
        connection.exec_driver_sql(
            "CREATE TABLE reflect_columns_quote ("
            "id BIGINT NOT NULL COMMENT '12\" pizza', "
            "v BIGINT NOT NULL AUTO_INCREMENT, "
            "s VARCHAR(10) DEFAULT 'NULL', "
            "w VARCHAR(10) DEFAULT '  x  ', "
            "k BIGINT NULL COMMENT 'not \"AUTO_INCREMENT\" here', "
            "z VARCHAR(10) COMMENT 'closing\"'"
            ") PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")"
        )
        try:
            columns = {c["name"]: c for c in inspect(connection).get_columns("reflect_columns_quote")}
        finally:
            connection.exec_driver_sql("DROP TABLE IF EXISTS reflect_columns_quote")

        assert {name: column["autoincrement"] for name, column in columns.items()} == {
            "id": False, "v": True, "s": False, "w": False, "k": False, "z": False
        }
        assert (columns["s"]["default"], columns["w"]["default"]) == ("'NULL'", "'  x  '")


class ReflectionStructFieldNameTest(fixtures.TestBase):
    """A quote or a keyword in a STRUCT field name changes nothing else that reflects from SHOW CREATE TABLE.

    StarRocks 3.3 prints STRUCT field names bare, and can't read such a type back (see
    ``STRUCT_FIELD_NAMES_UNQUOTED``), so the type's warning is let through here.
    """

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.combinations(
        ("single_quote", "`owner's`"),
        ("double_quote", '`a"b`'),
        ("backquote", "`a``b`"),
        ("comment_clause", '`x COMMENT "y`'),
        ("null_auto_increment", "`null auto_increment`"),
        argnames="field",
        id_="ia",
    )
    def test_the_later_columns_and_the_partition(self, connection, field):
        name = "reflect_struct_field"
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
        connection.exec_driver_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL, dt DATETIME NOT NULL, s STRUCT<{field} INT> NULL, "
            "w VARCHAR(10) NULL DEFAULT 'NULL', ai BIGINT NOT NULL AUTO_INCREMENT) DUPLICATE KEY(id, dt) "
            "PARTITION BY date_trunc('day', dt) DISTRIBUTED BY HASH(id) BUCKETS 1 "
            'PROPERTIES ("replication_num" = "1")'
        )
        try:
            with expect_warnings("Did not recognize type", assert_=False):
                inspector = inspect(connection)
                columns = {c["name"]: (c["default"], c["autoincrement"]) for c in inspector.get_columns(name)}
                partition = inspector.get_table_options(name).get(TableInfoKeyWithPrefix.PARTITION_BY)
        finally:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

        assert columns == {
            "id": (None, False), "dt": (None, False), "s": (None, False), "w": ("'NULL'", False), "ai": (None, True)
        }
        assert normalize_sql(partition.partition_method) == normalize_sql("date_trunc('day', dt)")

    def test_a_generated_column_reading_a_column_named_null_auto_increment(self, connection):
        name = "reflect_generated_name"
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
        connection.exec_driver_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL, `null auto_increment` BIGINT NULL, "
            "ai BIGINT NOT NULL AUTO_INCREMENT, g BIGINT AS `null auto_increment` + 1) DUPLICATE KEY(id) "
            'DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")'
        )
        try:
            columns = {c["name"]: c["autoincrement"] for c in inspect(connection).get_columns(name)}
        finally:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

        assert columns == {"id": False, "null auto_increment": False, "ai": True, "g": False}


class ReflectionMultiLineCommentTest(fixtures.TestBase):
    """A comment line that starts with the next column's backquoted name doesn't hide that column's definition.

    StarRocks prints a column comment's newlines as they are, on every key model, and 3.3 leaves the ``"`` of a
    PRIMARY KEY table's column comment unescaped.
    """

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.fixture
    def table_from_ddl(self, connection):
        created = []

        def create(name: str, ddl: str) -> str:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            connection.exec_driver_sql(ddl)
            created.append(name)
            return name

        yield create
        for name in created:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")

    @testing.combinations(("duplicate_key", "DUPLICATE"), ("primary_key", "PRIMARY"), argnames="model", id_="ia")
    def test_a_doc_comment_naming_the_next_column(self, connection, table_from_ddl, model):
        name = table_from_ddl(
            "reflect_doc_comment",
            "CREATE TABLE reflect_doc_comment (id BIGINT NOT NULL, "
            "dt DATE NOT NULL COMMENT 'Event day, in \"UTC\",\n`currency` is next.', "
            f"currency VARCHAR(3) NULL DEFAULT 'EUR') {model} KEY(id, dt) PARTITION BY date_trunc('day', dt) "
            "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")",
        )

        inspector = inspect(connection)
        columns = {c["name"]: c for c in inspector.get_columns(name)}
        partition = inspector.get_table_options(name)[TableInfoKeyWithPrefix.PARTITION_BY]
        reflected = Table(name, MetaData(), autoload_with=connection)

        assert columns["currency"]["default"] == "'EUR'"
        assert normalize_sql(partition.partition_method) == normalize_sql("date_trunc('day', dt)")
        assert reflected.c.currency.server_default.arg.text == "'EUR'"
        assert TableInfoKeyWithPrefix.PARTITION_BY in reflected.dialect_kwargs

    @testing.combinations(("duplicate_key", "DUPLICATE"), ("primary_key", "PRIMARY"), argnames="model", id_="ia")
    def test_a_comment_holding_the_next_column_line(self, connection, table_from_ddl, model):
        name = table_from_ddl(
            "reflect_next_line_comment",
            "CREATE TABLE reflect_next_line_comment ("
            "a INT NOT NULL COMMENT 'one,\n  `b` bigint(20) NOT NULL AUTO_INCREMENT COMMENT \"', "
            f"b BIGINT NULL DEFAULT '7' COMMENT 'b''s own') {model} KEY(a) DISTRIBUTED BY HASH(a) BUCKETS 1 "
            "PROPERTIES (\"replication_num\" = \"1\")",
        )

        b = next(c for c in inspect(connection).get_columns(name) if c["name"] == "b")

        assert (b["default"], b["autoincrement"]) == ("'7'", False)
