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

import logging

import pytest
from sqlalchemy import BigInteger, Column, Date, DateTime, Double, Integer, MetaData, String, Table
from sqlalchemy.dialects import registry
from sqlalchemy.schema import CreateTable

from starrocks.common.params import ColumnAggInfoKeyWithPrefix
from starrocks.common.types import ColumnAggType
from test.unit.test_utils import normalize_sql


class TestCreateTableCompiler:
    @classmethod
    def setup_class(cls):
        registry.register("starrocks", "starrocks.dialect", "StarRocksDialect")
        cls.logger = logging.getLogger(__name__)
        cls.dialect = registry.load("starrocks")()
        cls.metadata = MetaData()

    def _compile_table(self, table: Table) -> str:
        return str(CreateTable(table).compile(dialect=self.dialect))

    def test_engine_and_comment(self):
        self.logger.info("Testing ENGINE and COMMENT clauses")
        tbl = Table('engine_comment_tbl', self.metadata, Column('k1', Integer),
                    starrocks_engine='OLAP',
                    comment='A simple table comment.',
                    starrocks_duplicate_key='k1',
                    starrocks_distributed_by='HASH(k1)')
        sql = self._compile_table(tbl)
        expected = """
            CREATE TABLE engine_comment_tbl(k1 INTEGER)
            ENGINE=OLAP
            DUPLICATE KEY(k1)
            COMMENT 'A simple table comment.'
            DISTRIBUTED BY HASH(k1)
        """
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_key_descriptions(self):
        self.logger.info("Testing Key Description clauses")
        # DUPLICATE KEY
        tbl_dup = Table('key_duplicate_tbl', self.metadata, Column('k1', Integer),
                        starrocks_duplicate_key='k1', starrocks_distributed_by='HASH(k1)')
        sql = self._compile_table(tbl_dup)
        expected_dup = """
            CREATE TABLE key_duplicate_tbl(k1 INTEGER)
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1)
        """
        assert normalize_sql(sql) == normalize_sql(expected_dup)

        # AGGREGATE KEY
        tbl_agg = Table('key_aggregate_tbl', self.metadata, Column('k1', Integer),
                        starrocks_aggregate_key='k1', starrocks_distributed_by='HASH(k1)')
        sql = self._compile_table(tbl_agg)
        expected_agg = """
            CREATE TABLE key_aggregate_tbl(k1 INTEGER)
            AGGREGATE KEY(k1)
            DISTRIBUTED BY HASH(k1)
        """
        assert normalize_sql(sql) == normalize_sql(expected_agg)

        # UNIQUE KEY
        tbl_unique = Table('key_unique_tbl', self.metadata, Column('k1', Integer),
                           starrocks_unique_key='k1', starrocks_distributed_by='HASH(k1)')
        sql = self._compile_table(tbl_unique)
        expected_unique = """
            CREATE TABLE key_unique_tbl(k1 INTEGER)
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1)
        """
        assert normalize_sql(sql) == normalize_sql(expected_unique)

        # PRIMARY KEY
        tbl_primary = Table('key_primary_tbl', self.metadata, Column('k1', Integer),
                            starrocks_primary_key='k1', starrocks_distributed_by='HASH(k1)')
        sql = self._compile_table(tbl_primary)
        expected_primary = """
            CREATE TABLE key_primary_tbl(k1 INTEGER)
            PRIMARY KEY(k1)
            DISTRIBUTED BY HASH(k1)
        """
        assert normalize_sql(sql) == normalize_sql(expected_primary)

    def test_column_comment(self):
        self.logger.info("Testing Column Comment clause")
        tbl = Table('col_comment_tbl', self.metadata,
                    Column('k1', Integer, comment='This is a column comment.'),
                    starrocks_distributed_by='HASH(k1)')
        sql = self._compile_table(tbl)
        expected = "CREATE TABLE col_comment_tbl(k1 INTEGER COMMENT 'This is a column comment.') DISTRIBUTED BY HASH(k1)"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_column_attributes(self):
        self.logger.info("Testing various column attributes")
        tbl = Table('col_attr_tbl', self.metadata,
                    Column('k1', Integer, primary_key=True),
                    Column('k2', BigInteger, autoincrement=True),
                    Column('k3', String(50), nullable=True),
                    starrocks_distributed_by='HASH(k1)',
                    starrocks_primary_key='k1')
        sql = self._compile_table(tbl)
        expected = """
            CREATE TABLE col_attr_tbl(
                k1 INTEGER NOT NULL,
                k2 BIGINT NOT NULL AUTO_INCREMENT,
                k3 VARCHAR(50)
            )
            PRIMARY KEY(k1)
            DISTRIBUTED BY HASH(k1)
        """
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_aggregate_key_table(self):
        self.logger.info("Testing Aggregate Key Table DDL")
        tbl = Table('agg_tbl', self.metadata,
                    Column('k1', Integer),
                    Column('v1', Integer, **{ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.SUM}),
                    Column('v2', String(50), **{ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.REPLACE}),
                    starrocks_aggregate_key='k1')
        sql = self._compile_table(tbl)
        expected = """
            CREATE TABLE agg_tbl(
                k1 INTEGER,
                v1 INTEGER SUM,
                v2 VARCHAR(50) REPLACE
            )
            AGGREGATE KEY(k1)
        """
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_aggregate_key_validation(self):
        self.logger.info("Testing AGGREGATE KEY validation")
        from sqlalchemy import exc

        # Test case 1: Column marked as both key and aggregate
        with pytest.raises(exc.CompileError, match="cannot be both KEY and aggregated"):
            tbl1 = Table('invalid_agg_tbl1', self.metadata,
                         Column('k1', Integer, **{
                             ColumnAggInfoKeyWithPrefix.IS_AGG_KEY: True,
                             ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.SUM
                         }),
                         starrocks_aggregate_key='k1')
            self._compile_table(tbl1)

        # Test case 2: Column with aggregate marker on a non-aggregate table
        with pytest.raises(exc.CompileError, match="only valid for AGGREGATE KEY tables"):
            tbl2 = Table('invalid_agg_tbl2', self.metadata,
                         Column('k1', Integer),
                         Column('v1', Integer, **{ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.SUM}),
                         starrocks_duplicate_key='k1')
            self._compile_table(tbl2)

    def test_aggregate_key_ordering(self):
        self.logger.info("Testing AGGREGATE KEY column ordering validation")

        # Test case 1: Value column before key column
        # Don't check it now. leave it to SR.
        # with pytest.raises(exc.CompileError, match="all key columns must be defined before any value columns"):
        #     tbl1 = Table('invalid_order_tbl1', self.metadata,
        #                  Column('v1', Integer, **{ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.SUM}),
        #                  Column('k1', Integer),
        #                  **{TableInfoKeyWithPrefix.AGGREGATE_KEY: 'k1'})
        #     self._compile_table(tbl1)

        # Test case 2: Key columns in wrong order compared to starrocks_aggregate_key
        # Don't check it now. leave it to SR.
        # with pytest.raises(exc.CompileError, match="order of key columns in the table definition must match"):
        #     tbl2 = Table('invalid_order_tbl2', self.metadata,
        #                  Column('k2', Integer),
        #                  Column('k1', Integer),
        #                  Column('v1', Integer, **{ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.SUM}),
        #                  **{TableInfoKeyWithPrefix.AGGREGATE_KEY: 'k1,k2'})
        #     self._compile_table(tbl2)

    def test_column_default_value(self):
        self.logger.info("Testing column server_default value")
        tbl = Table('default_val_tbl', self.metadata,
                    Column('k1', Integer),
                    Column('k2', String(50), server_default="foo"),
                    starrocks_distributed_by='HASH(k1)')
        sql = self._compile_table(tbl)
        expected = """
            CREATE TABLE default_val_tbl(
                k1 INTEGER,
                k2 VARCHAR(50) DEFAULT 'foo'
            )
            DISTRIBUTED BY HASH(k1)
        """
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_generated_column(self):
        self.logger.info("Testing generated column DDL")
        from sqlalchemy import Computed
        tbl = Table('generated_col_tbl', self.metadata,
                    Column('k1', Integer),
                    Column('k2', String(50)),
                    Column('k3', Integer, Computed("left(k2, 10)")),
                    starrocks_distributed_by='HASH(k1)')
        sql = self._compile_table(tbl)
        expected = """
            CREATE TABLE generated_col_tbl(
                k1 INTEGER,
                k2 VARCHAR(50),
                k3 INTEGER AS left(k2, 10)
            )
            DISTRIBUTED BY HASH(k1)
        """
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_partition_descriptions(self):
        self.logger.info("Testing Partition Description clauses")

        # Single-column RANGE partition
        tbl_range = Table('p_range_tbl', self.metadata, Column('event_date', Date),
                          starrocks_partition_by='RANGE(event_date) (PARTITION p2023 VALUES LESS THAN ("2024-01-01"))')
        sql = self._compile_table(tbl_range)
        expected = "CREATE TABLE p_range_tbl(event_date DATE) PARTITION BY RANGE(event_date)(PARTITION p2023 VALUES LESS THAN(\"2024-01-01\"))"
        assert normalize_sql(sql) == normalize_sql(expected)

        # Multi-column RANGE partition
        tbl_range_multi = Table('p_range_multi_tbl', self.metadata, Column('k1', Integer), Column('k2', Integer),
                                starrocks_partition_by='RANGE(k1, k2) (PARTITION p1 VALUES LESS THAN (100, 200))')
        sql = self._compile_table(tbl_range_multi)
        expected = "CREATE TABLE p_range_multi_tbl(k1 INTEGER,k2 INTEGER) PARTITION BY RANGE(k1,k2)(PARTITION p1 VALUES LESS THAN(100,200))"
        assert normalize_sql(sql) == normalize_sql(expected)

        # LIST partition
        tbl_list = Table('p_list_tbl', self.metadata, Column('city', String(50)),
                         starrocks_partition_by='LIST(city) (PARTITION p_north VALUES IN ("Beihai", "Dalian"))')
        sql = self._compile_table(tbl_list)
        expected = "CREATE TABLE p_list_tbl(city VARCHAR(50)) PARTITION BY LIST(city)(PARTITION p_north VALUES IN(\"Beihai\",\"Dalian\"))"
        assert normalize_sql(sql) == normalize_sql(expected)

        # Please keep it. It is a special case that is recoganized as a RANGE partition.
        tbl_expr_single = Table('p_range_date_trunc_tbl', self.metadata, Column('event_time', DateTime),
                                starrocks_partition_by="date_trunc('month', event_time)")
        sql = self._compile_table(tbl_expr_single)
        expected = "CREATE TABLE p_range_date_trunc_tbl(event_time DATETIME) PARTITION BY date_trunc('month',event_time)"
        assert normalize_sql(sql) == normalize_sql(expected)

        # Expression partition (single function)
        tbl_expr_single = Table('p_expr_single_tbl', self.metadata, Column('event_time', DateTime),
                                starrocks_partition_by="to_date(event_time)")
        sql = self._compile_table(tbl_expr_single)
        expected = "CREATE TABLE p_expr_single_tbl(event_time DATETIME) PARTITION BY to_date(event_time)"
        assert normalize_sql(sql) == normalize_sql(expected)

        # Expression partition (multiple functions and columns)
        tbl_expr_multi = Table('p_expr_multi_tbl', self.metadata, Column('event_time', DateTime), Column('id', Integer),
                               starrocks_partition_by="date_trunc('day', event_time), id % 10")
        sql = self._compile_table(tbl_expr_multi)
        expected = "CREATE TABLE p_expr_multi_tbl(event_time DATETIME,id INTEGER) PARTITION BY date_trunc('day',event_time),id % 10"
        assert normalize_sql(sql) == normalize_sql(expected)

        # Batch partition
        tbl_batch = Table('p_batch_tbl', self.metadata, Column('datekey', Integer),
                          starrocks_partition_by='RANGE(datekey) (START ("1") END ("5") EVERY (1))')
        sql = self._compile_table(tbl_batch)
        expected = "CREATE TABLE p_batch_tbl(datekey INTEGER) PARTITION BY RANGE(datekey)(START(\"1\")END(\"5\")EVERY(1))"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_distribution_descriptions(self):
        self.logger.info("Testing Distribution Description clauses")

        # HASH distribution
        tbl_hash = Table('dist_hash_tbl', self.metadata, Column('user_id', Integer),
                         starrocks_distributed_by='HASH(user_id) BUCKETS 10')
        sql = self._compile_table(tbl_hash)
        expected = "CREATE TABLE dist_hash_tbl(user_id INTEGER) DISTRIBUTED BY HASH(user_id) BUCKETS 10"
        assert normalize_sql(sql) == normalize_sql(expected)

        # RANDOM distribution
        tbl_random = Table('dist_random_tbl', self.metadata, Column('log_id', BigInteger),
                           starrocks_distributed_by='RANDOM BUCKETS 4')
        sql = self._compile_table(tbl_random)
        expected = "CREATE TABLE dist_random_tbl(log_id BIGINT) DISTRIBUTED BY RANDOM BUCKETS 4"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_order_by(self):
        self.logger.info("Testing ORDER BY clause")
        tbl = Table('orderby_tbl', self.metadata, Column('k1', Integer), Column('k2', String(50)),
                    starrocks_order_by='k1, k2', starrocks_distributed_by='HASH(k1)')
        sql = self._compile_table(tbl)
        expected = "CREATE TABLE orderby_tbl(k1 INTEGER, k2 VARCHAR(50)) DISTRIBUTED BY HASH(k1) ORDER BY(k1, k2)"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_properties(self):
        self.logger.info("Testing PROPERTIES clause")
        tbl = Table('props_tbl', self.metadata, Column('k1', Integer),
                    starrocks_properties={'replication_num': '1', 'storage_medium': 'SSD'},
                    starrocks_distributed_by='HASH(k1)')
        sql = self._compile_table(tbl)
        expected = """
            PROPERTIES(
                "replication_num" = "1",
                "storage_medium" = "SSD"
            )
        """
        assert normalize_sql(expected) in normalize_sql(sql)

    def test_comprehensive_table(self):
        self.logger.info("Testing comprehensive CREATE TABLE statement")
        tbl = Table(
            'comprehensive_table', self.metadata,
            Column('user_id', BigInteger, nullable=False, comment='User ID'),
            Column('event_date', Date, nullable=False, comment='Event date'),
            Column('city', String(50), server_default='Unknown'),
            Column('revenue', Double, server_default='0.0'),
            starrocks_engine='OLAP',
            starrocks_aggregate_key='user_id, event_date',
            comment='A comprehensive table',
            starrocks_partition_by='RANGE(event_date) (START ("2023-01-01") END ("2024-01-01") EVERY (INTERVAL 1 MONTH))',
            starrocks_distributed_by='HASH(user_id) BUCKETS 32',
            starrocks_order_by='event_date, user_id',
            starrocks_properties={
                "replication_num": "1",
                "storage_medium": "SSD",
                "bloom_filter_columns": "city",
                "dynamic_partition.enable": "true",
                "dynamic_partition.time_unit": "MONTH",
                "dynamic_partition.end": "3",
                "dynamic_partition.prefix": "p"
            }
        )
        sql = self._compile_table(tbl)
        expected = """
            CREATE TABLE comprehensive_table (
                user_id BIGINT NOT NULL COMMENT 'User ID',
                event_date DATE NOT NULL COMMENT 'Event date',
                city VARCHAR(50) DEFAULT 'Unknown',
                revenue DOUBLE DEFAULT '0.0'
            )
            ENGINE=OLAP
            AGGREGATE KEY(user_id, event_date)
            COMMENT 'A comprehensive table'
            PARTITION BY RANGE(event_date) (START ("2023-01-01") END ("2024-01-01") EVERY (INTERVAL 1 MONTH))
            DISTRIBUTED BY HASH(user_id) BUCKETS 32
            ORDER BY(event_date, user_id)
            PROPERTIES(
                "replication_num" = "1",
                "storage_medium" = "SSD",
                "bloom_filter_columns" = "city",
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "MONTH",
                "dynamic_partition.end" = "3",
                "dynamic_partition.prefix" = "p"
            )
        """
        assert normalize_sql(sql) == normalize_sql(expected)

