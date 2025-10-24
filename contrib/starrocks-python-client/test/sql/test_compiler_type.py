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
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.dialects import registry
from sqlalchemy.schema import CreateTable

from starrocks.datatype import (
    ARRAY,
    BIGINT,
    BINARY,
    BITMAP,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    DECIMAL,
    DOUBLE,
    FLOAT,
    HLL,
    INTEGER,
    JSON,
    LARGEINT,
    MAP,
    SMALLINT,
    STRING,
    STRUCT,
    TINYINT,
    VARBINARY,
    VARCHAR,
)
from test.unit.test_utils import normalize_sql


# Test data for different type categories
BASIC_TYPE_TEST_CASES = [
    # (type_instance, expected_sql)
    (TINYINT(), "col TINYINT"),
    (TINYINT(1), "col TINYINT"),  # StarRocks TINYINT doesn't preserve display_width in compilation
    (SMALLINT(), "col SMALLINT"),
    (INTEGER(), "col INTEGER"),
    (BIGINT(), "col BIGINT"),
    (LARGEINT(), "col LARGEINT"),
    (BOOLEAN(), "col BOOLEAN"),
    (DECIMAL(), "col DECIMAL"),
    (DECIMAL(10), "col DECIMAL(10)"),
    (DECIMAL(10, 2), "col DECIMAL(10,2)"),
    (FLOAT(), "col FLOAT"),
    (FLOAT(10), "col FLOAT"),  # StarRocks FLOAT might not preserve precision in compilation
    (DOUBLE(), "col DOUBLE"),
    (CHAR(10), "col CHAR(10)"),
    (VARCHAR(255), "col VARCHAR(255)"),
    (STRING(), "col STRING"),
    (BINARY(10), "col BINARY"),  # StarRocks BINARY might not preserve length in compilation
    (VARBINARY(255), "col VARBINARY"),  # StarRocks VARBINARY might not preserve length in compilation
    (DATE(), "col DATE"),
    (DATETIME(), "col DATETIME"),
    (HLL(), "col HLL"),
    (BITMAP(), "col BITMAP"),
    # (PERCENTILE(), "col PERCENTILE"),  # PERCENTILE type compiler not implemented yet
    (JSON(), "col JSON"),
]

COMPLEX_TYPE_TEST_CASES = [
    # ARRAY types
    (ARRAY(INTEGER), "col ARRAY<INTEGER>"),
    (ARRAY(VARCHAR(50)), "col ARRAY<VARCHAR(50)>"),
    (ARRAY(ARRAY(INTEGER)), "col ARRAY<ARRAY<INTEGER>>"),

    # MAP types
    (MAP(STRING, INTEGER), "col MAP<STRING,INTEGER>"),
    (MAP(VARCHAR(50), DOUBLE), "col MAP<VARCHAR(50),DOUBLE>"),
    (MAP(STRING, MAP(INTEGER, STRING)), "col MAP<STRING,MAP<INTEGER,STRING>>"),

    # STRUCT types
    (STRUCT(name=STRING, age=INTEGER), "col STRUCT<name STRING,age INTEGER>"),
    (STRUCT(id=INTEGER, name=VARCHAR(100), active=BOOLEAN),
     "col STRUCT<id INTEGER,name VARCHAR(100),active BOOLEAN>"),
    (STRUCT(user=STRUCT(id=INTEGER, name=STRING), metadata=MAP(STRING, STRING)),
     "col STRUCT<user STRUCT<id INTEGER,name STRING>,metadata MAP<STRING,STRING>>"),
]

CONSTRAINT_TEST_CASES = [
    # (type_instance, nullable, comment, server_default, expected_sql)
    (INTEGER(), False, None, None, "col INTEGER NOT NULL"),
    (VARCHAR(100), None, 'A test column', None, "col VARCHAR(100) COMMENT 'A test column'"),
    (INTEGER(), None, None, '0', "col INTEGER DEFAULT '0'"),
    (VARCHAR(50), False, 'Full test column', 'test',
     "col VARCHAR(50) NOT NULL DEFAULT 'test' COMMENT 'Full test column'"),
]

EDGE_CASE_TEST_CASES = [
    (VARCHAR(65533), "col VARCHAR(65533)"),
    (DECIMAL(38, 18), "col DECIMAL(38,18)"),
    (ARRAY(MAP(STRING, STRUCT(level1=ARRAY(MAP(INTEGER, STRUCT(level2=STRING, level2_array=ARRAY(INTEGER))))))),
     "col ARRAY<MAP<STRING,STRUCT<level1 ARRAY<MAP<INTEGER,STRUCT<level2 STRING,level2_array ARRAY<INTEGER>>>>>>>"),
]


class TestDataTypeCompiler:
    @classmethod
    def setup_class(cls):
        registry.register("starrocks", "starrocks.dialect", "StarRocksDialect")
        cls.logger = logging.getLogger(__name__)
        cls.dialect = registry.load("starrocks")()

    def _compile_column_type(self, column: Column) -> str:
        """Helper method to compile a single column type"""
        # Use a fresh MetaData for each test to avoid table name conflicts
        metadata = MetaData()
        table = Table('test_table', metadata, column)
        create_sql = str(CreateTable(table).compile(dialect=self.dialect))
        # Extract just the column definition from the CREATE TABLE statement
        # Format: CREATE TABLE test_table(column_definition)
        start = create_sql.find('(') + 1
        end = create_sql.rfind(')')
        return create_sql[start:end].strip()

    @pytest.mark.parametrize("type_instance, expected_sql", BASIC_TYPE_TEST_CASES)
    def test_basic_types(self, type_instance, expected_sql):
        """Test compilation of basic data types"""
        col = Column('col', type_instance)
        result = self._compile_column_type(col)
        assert normalize_sql(result) == normalize_sql(expected_sql)

    @pytest.mark.parametrize("type_instance, expected_sql", COMPLEX_TYPE_TEST_CASES)
    def test_complex_types(self, type_instance, expected_sql):
        """Test compilation of complex data types"""
        col = Column('col', type_instance)
        result = self._compile_column_type(col)
        assert normalize_sql(result) == normalize_sql(expected_sql)

    @pytest.mark.parametrize("type_instance, nullable, comment, server_default, expected_sql", CONSTRAINT_TEST_CASES)
    def test_type_constraints(self, type_instance, nullable, comment, server_default, expected_sql):
        """Test compilation of data types with constraints"""
        kwargs = {}
        if nullable is not None:
            kwargs['nullable'] = nullable
        if comment is not None:
            kwargs['comment'] = comment
        if server_default is not None:
            kwargs['server_default'] = server_default

        col = Column('col', type_instance, **kwargs)
        result = self._compile_column_type(col)
        assert normalize_sql(result) == normalize_sql(expected_sql)

    @pytest.mark.parametrize("type_instance, expected_sql", EDGE_CASE_TEST_CASES)
    def test_edge_cases(self, type_instance, expected_sql):
        """Test edge cases and boundary conditions for type compilation"""
        col = Column('col', type_instance)
        result = self._compile_column_type(col)
        assert normalize_sql(result) == normalize_sql(expected_sql)
