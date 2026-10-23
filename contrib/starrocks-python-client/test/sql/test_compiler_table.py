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

"""CREATE TABLE compilation, with StarRocks key, partition, distribution, ORDER BY and PROPERTIES clauses.

The dialect puts each column on its own indented line and each table clause on its own line; ``assert_compile``
drops the line breaks, so each expected string literal below is one line of the generated DDL.
"""


from sqlalchemy import BigInteger, Column, Computed, Date, DateTime, Integer, MetaData, String, Table, exc, testing
from sqlalchemy.schema import CreateTable
from sqlalchemy.testing import expect_raises_message, fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

from starrocks.common.params import ColumnAggInfoKeyWithPrefix
from starrocks.common.types import ColumnAggType
from starrocks.datatype import DOUBLE
from starrocks.dialect import StarRocksDialect


class CreateTableCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()

    def test_engine_and_comment(self):
        """``ENGINE = OLAP`` is StarRocks' default, so the dialect leaves it out."""
        tbl = Table(
            "engine_comment_tbl",
            MetaData(),
            Column("k1", Integer),
            starrocks_engine="OLAP",
            comment="A simple table comment.",
            starrocks_duplicate_key="k1",
            starrocks_distributed_by="HASH(k1)",
        )
        self.assert_compile(
            CreateTable(tbl),
            "CREATE TABLE engine_comment_tbl ("
            "    k1 INTEGER"
            ")"
            "DUPLICATE KEY (k1)"
            "COMMENT 'A simple table comment.'"
            "DISTRIBUTED BY HASH(k1)",
        )

    @testing.combinations(
        ("starrocks_duplicate_key", "DUPLICATE KEY (k1)"),
        ("starrocks_aggregate_key", "AGGREGATE KEY (k1)"),
        ("starrocks_unique_key", "UNIQUE KEY (k1)"),
        ("starrocks_primary_key", "PRIMARY KEY (k1)"),
        argnames="key_kwarg, key_clause",
        id_="sa",
    )
    def test_key_description(self, key_kwarg, key_clause):
        tbl = Table(
            "key_tbl", MetaData(), Column("k1", Integer), starrocks_distributed_by="HASH(k1)", **{key_kwarg: "k1"}
        )
        self.assert_compile(
            CreateTable(tbl),
            "CREATE TABLE key_tbl ("
            "    k1 INTEGER"
            ")"
            f"{key_clause}"
            "DISTRIBUTED BY HASH(k1)",
        )

    def test_column_comment(self):
        tbl = Table(
            "col_comment_tbl",
            MetaData(),
            Column("k1", Integer, comment="This is a column comment."),
            starrocks_distributed_by="HASH(k1)",
        )
        self.assert_compile(
            CreateTable(tbl),
            "CREATE TABLE col_comment_tbl ("
            "    k1 INTEGER COMMENT 'This is a column comment.'"
            ")"
            "DISTRIBUTED BY HASH(k1)",
        )

    def test_column_attributes(self):
        tbl = Table(
            "col_attr_tbl",
            MetaData(),
            Column("k1", Integer, primary_key=True, autoincrement=False),
            Column("k2", BigInteger, autoincrement=True),
            Column("k3", String(50), nullable=True),
            starrocks_distributed_by="HASH(k1)",
            starrocks_primary_key="k1",
        )
        self.assert_compile(
            CreateTable(tbl),
            "CREATE TABLE col_attr_tbl ("
            "    k1 INTEGER NOT NULL, "
            "    k2 BIGINT NOT NULL AUTO_INCREMENT, "
            "    k3 VARCHAR(50)"
            ")"
            "PRIMARY KEY (k1)"
            "DISTRIBUTED BY HASH(k1)",
        )

    def test_aggregate_key_table(self):
        tbl = Table(
            "agg_tbl",
            MetaData(),
            Column("k1", Integer),
            Column("v1", Integer, **{ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.SUM}),
            Column("v2", String(50), **{ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.REPLACE}),
            starrocks_AGGREGATE_KEY="k1",
        )
        self.assert_compile(
            CreateTable(tbl),
            "CREATE TABLE agg_tbl ("
            "    k1 INTEGER, "
            "    v1 INTEGER SUM, "
            "    v2 VARCHAR(50) REPLACE"
            ")"
            "AGGREGATE KEY (k1)",
        )

    def test_aggregate_key_column_cannot_be_aggregated(self):
        tbl = Table(
            "invalid_agg_tbl1",
            MetaData(),
            Column(
                "k1",
                Integer,
                **{ColumnAggInfoKeyWithPrefix.IS_AGG_KEY: True, ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.SUM},
            ),
            starrocks_aggregate_key="k1",
        )
        with expect_raises_message(exc.CompileError, "cannot be both KEY and aggregated"):
            CreateTable(tbl).compile(dialect=self.__dialect__)

    def test_aggregate_type_needs_aggregate_key_table(self):
        tbl = Table(
            "invalid_agg_tbl2",
            MetaData(),
            Column("k1", Integer),
            Column("v1", Integer, **{ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.SUM}),
            starrocks_duplicate_key="k1",
        )
        with expect_raises_message(exc.CompileError, "only valid for AGGREGATE KEY tables"):
            CreateTable(tbl).compile(dialect=self.__dialect__)

    def test_column_default_value(self):
        tbl = Table(
            "default_val_tbl",
            MetaData(),
            Column("k1", Integer),
            Column("k2", String(50), server_default="foo"),
            starrocks_distributed_by="HASH(k1)",
        )
        self.assert_compile(
            CreateTable(tbl),
            "CREATE TABLE default_val_tbl ("
            "    k1 INTEGER, "
            "    k2 VARCHAR(50) DEFAULT 'foo'"
            ")"
            "DISTRIBUTED BY HASH(k1)",
        )

    def test_generated_column(self):
        tbl = Table(
            "generated_col_tbl",
            MetaData(),
            Column("k1", Integer),
            Column("k2", String(50)),
            Column("k3", Integer, Computed("left(k2, 10)")),
            starrocks_distributed_by="HASH(k1)",
        )
        self.assert_compile(
            CreateTable(tbl),
            "CREATE TABLE generated_col_tbl ("
            "    k1 INTEGER, "
            "    k2 VARCHAR(50), "
            "    k3 INTEGER AS (left(k2, 10))"
            ")"
            "DISTRIBUTED BY HASH(k1)",
        )

    @testing.combinations(
        (
            "range",
            [Column("event_date", Date)],
            'RANGE(event_date) (PARTITION p2023 VALUES LESS THAN ("2024-01-01"))',
            "    event_date DATE",
        ),
        (
            "range_multi_column",
            [Column("k1", Integer), Column("k2", Integer)],
            'RANGE(k1, k2) (PARTITION p1 VALUES LESS THAN ("100", "200"))',
            "    k1 INTEGER,     k2 INTEGER",
        ),
        (
            "list",
            [Column("city", String(50))],
            'LIST(city) (PARTITION p_north VALUES IN ("Beihai", "Dalian"))',
            "    city VARCHAR(50)",
        ),
        (
            "date_trunc_recognised_as_range",
            [Column("event_time", DateTime)],
            "date_trunc('month', event_time)",
            "    event_time DATETIME",
        ),
        (
            "expression",
            [Column("event_time", DateTime)],
            "to_date(event_time)",
            "    event_time DATETIME",
        ),
        (
            "expression_multi",
            [Column("s", String(20)), Column("id", Integer)],
            "str2date(s, '%Y-%m-%d'), id",
            "    s VARCHAR(20),     id INTEGER",
        ),
        (
            "batch",
            [Column("datekey", Integer)],
            'RANGE(datekey) (START ("1") END ("5") EVERY (1))',
            "    datekey INTEGER",
        ),
        argnames="columns, partition_by, column_lines",
        id_="iaaa",
    )
    def test_partition_description(self, columns, partition_by, column_lines):
        """The partition clause is passed through as given.

        Keep the ``date_trunc`` case: StarRocks recognises it as a RANGE partition, unlike other expressions.
        """
        tbl = Table("p_tbl", MetaData(), *columns, starrocks_partition_by=partition_by)
        self.assert_compile(
            CreateTable(tbl),
            f"CREATE TABLE p_tbl ({column_lines})PARTITION BY {partition_by}",
        )

    @testing.combinations(
        ("hash", Column("user_id", Integer), "HASH(user_id) BUCKETS 10", "    user_id INTEGER"),
        ("random", Column("log_id", BigInteger), "RANDOM BUCKETS 4", "    log_id BIGINT"),
        argnames="column, distributed_by, column_line",
        id_="iaaa",
    )
    def test_distribution_description(self, column, distributed_by, column_line):
        tbl = Table("dist_tbl", MetaData(), column, starrocks_distributed_by=distributed_by)
        self.assert_compile(
            CreateTable(tbl),
            f"CREATE TABLE dist_tbl ({column_line})DISTRIBUTED BY {distributed_by}",
        )

    def test_order_by(self):
        tbl = Table(
            "orderby_tbl",
            MetaData(),
            Column("k1", Integer),
            Column("k2", String(50)),
            starrocks_order_by="k1, k2",
            starrocks_distributed_by="HASH(k1)",
        )
        self.assert_compile(
            CreateTable(tbl),
            "CREATE TABLE orderby_tbl ("
            "    k1 INTEGER, "
            "    k2 VARCHAR(50)"
            ")"
            "DISTRIBUTED BY HASH(k1)"
            "ORDER BY (k1, k2)",
        )

    def test_properties(self):
        tbl = Table(
            "props_tbl",
            MetaData(),
            Column("k1", Integer),
            starrocks_properties={"replication_num": "1", "storage_medium": "SSD"},
            starrocks_distributed_by="HASH(k1)",
        )
        self.assert_compile(
            CreateTable(tbl),
            "CREATE TABLE props_tbl ("
            "    k1 INTEGER"
            ")"
            "DISTRIBUTED BY HASH(k1)"
            "PROPERTIES("
            '    "replication_num"="1",'
            '    "storage_medium"="SSD"'
            ")",
        )

    def test_comprehensive_table(self):
        tbl = Table(
            "comprehensive_table",
            MetaData(),
            Column("user_id", BigInteger, nullable=False, comment="User ID"),
            Column("event_date", Date, nullable=False, comment="Event date"),
            Column("city", String(50), server_default="Unknown"),
            Column("revenue", DOUBLE, server_default="0.0"),
            starrocks_engine="OLAP",
            starrocks_aggregate_key="user_id, event_date",
            comment="A comprehensive table",
            starrocks_partition_by=(
                'RANGE(event_date) (START ("2023-01-01") END ("2024-01-01") EVERY (INTERVAL 1 MONTH))'
            ),
            starrocks_distributed_by="HASH(user_id) BUCKETS 32",
            starrocks_order_by="event_date, user_id",
            starrocks_properties={
                "replication_num": "1",
                "storage_medium": "SSD",
                "bloom_filter_columns": "city",
                "dynamic_partition.enable": "true",
                "dynamic_partition.time_unit": "MONTH",
                "dynamic_partition.end": "3",
                "dynamic_partition.prefix": "p",
            },
        )
        self.assert_compile(
            CreateTable(tbl),
            "CREATE TABLE comprehensive_table ("
            "    user_id BIGINT NOT NULL COMMENT 'User ID', "
            "    event_date DATE NOT NULL COMMENT 'Event date', "
            "    city VARCHAR(50) DEFAULT 'Unknown', "
            "    revenue DOUBLE DEFAULT '0.0'"
            ")"
            "AGGREGATE KEY (user_id, event_date)"
            "COMMENT 'A comprehensive table'"
            'PARTITION BY RANGE(event_date) (START ("2023-01-01") END ("2024-01-01") EVERY (INTERVAL 1 MONTH))'
            "DISTRIBUTED BY HASH(user_id) BUCKETS 32"
            "ORDER BY (event_date, user_id)"
            "PROPERTIES("
            '    "replication_num"="1",'
            '    "storage_medium"="SSD",'
            '    "bloom_filter_columns"="city",'
            '    "dynamic_partition.enable"="true",'
            '    "dynamic_partition.time_unit"="MONTH",'
            '    "dynamic_partition.end"="3",'
            '    "dynamic_partition.prefix"="p"'
            ")",
        )
