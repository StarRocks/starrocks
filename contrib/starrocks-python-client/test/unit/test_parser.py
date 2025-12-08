#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import contextlib
import logging
from types import MethodType, SimpleNamespace
from typing import Optional

from lark import LarkError
import pytest
from sqlalchemy import MetaData, Table
from sqlalchemy.engine.reflection import _ReflectionInfo

from starrocks import datatype as datatype
from starrocks.dialect import StarRocksDialect
from starrocks.drivers.parsers import parse_data_type, parse_mv_refresh_clause
from starrocks.engine.interfaces import ReflectedTableKeyInfo
from starrocks.reflection import StarRocksInspector, StarRocksTableDefinitionParser


logger = logging.getLogger(__name__)


class TestDataTypeParser:
    @pytest.mark.parametrize(
        "type_str, expected_type",
        [
            ("boolean", datatype.BOOLEAN),
            ("tinyint", datatype.TINYINT),
            ("tinyint(1)", datatype.BOOLEAN),  # TINYINT(1) should be converted to BOOLEAN during reflection
            ("smallint", datatype.SMALLINT),
            ("int", datatype.INTEGER),
            ("integer", datatype.INTEGER),
            ("bigint", datatype.BIGINT),
            ("largeint", datatype.LARGEINT),
            ("decimal(10, 2)", datatype.DECIMAL(10, 2)),
            ("double", datatype.DOUBLE),
            ("float", datatype.FLOAT),
            ("char(10)", datatype.CHAR(10)),
            ("varchar(100)", datatype.VARCHAR(100)),
            ("string", datatype.STRING),
            ("binary", datatype.BINARY),
            ("varbinary", datatype.VARBINARY),
            ("date", datatype.DATE),
            ("datetime", datatype.DATETIME),
            ("hll", datatype.HLL),
            ("bitmap", datatype.BITMAP),
            ("percentile", datatype.PERCENTILE),
            ("json", datatype.JSON),
            # Case-insensitivity
            ("VARCHAR(50)", datatype.VARCHAR(50)),
            ("Integer", datatype.INTEGER),
            # Unsigned
            ("bigint unsigned", datatype.LARGEINT),
            ("bigint(20) unsigned", datatype.LARGEINT),
            # ("int unsigned", datatype.INTEGER),
            # Whitespace
            ("  varchar  (  100  )  ", datatype.VARCHAR(100)),
            ("array < int >", datatype.ARRAY(datatype.INTEGER)),
        ],
    )
    def test_simple_types(self, type_str, expected_type):
        # logger.debug(f"type_str: {type_str}, expected_type: {expected_type}")
        assert repr(parse_data_type(type_str)) == repr(expected_type)

    @pytest.mark.parametrize(
        "type_str, expected_type",
        [
            ("array<int>", datatype.ARRAY(datatype.INTEGER)),
            ("array<varchar(20)>", datatype.ARRAY(datatype.VARCHAR(20))),
            ("map<string, int>", datatype.MAP(datatype.STRING, datatype.INTEGER)),
            (
                "struct<name varchar(100), age int>",
                datatype.STRUCT(
                    ("name", datatype.VARCHAR(100)), ("age", datatype.INTEGER)
                ),
            ),
        ],
    )
    def test_complex_types(self, type_str, expected_type):
        assert repr(parse_data_type(type_str)) == repr(expected_type)

    @pytest.mark.parametrize(
        "type_str, expected_type",
        [
            (
                "array<array<int>>",
                datatype.ARRAY(datatype.ARRAY(datatype.INTEGER)),
            ),
            (
                "map<string, array<int>>",
                datatype.MAP(datatype.STRING, datatype.ARRAY(datatype.INTEGER)),
            ),
            (
                "struct<name varchar(100), details map<string, array<struct<item_id int, price decimal(10, 2)>>>>",
                datatype.STRUCT(
                    ("name", datatype.VARCHAR(100)),
                    (
                        "details",
                        datatype.MAP(
                            datatype.STRING,
                            datatype.ARRAY(
                                datatype.STRUCT(
                                    ("item_id", datatype.INTEGER),
                                    ("price", datatype.DECIMAL(10, 2)),
                                )
                            ),
                        ),
                    ),
                ),
            ),
            (
                "STRUCT<name VARCHAR(100), details MAP<VARCHAR(65533), ARRAY<STRUCT<item_id INT(11), price DECIMAL(10, 2)>>>>",
                datatype.STRUCT(
                    ("name", datatype.VARCHAR(100)),
                    (
                        "details",
                        datatype.MAP(
                            datatype.VARCHAR(65533),
                            datatype.ARRAY(
                                datatype.STRUCT(
                                    ("item_id", datatype.INTEGER(11)),
                                    ("price", datatype.DECIMAL(10, 2)),
                                )
                            ),
                        ),
                    ),
                ),
            ),
        ],
    )
    def test_nested_complex_types(self, type_str, expected_type):
        assert repr(parse_data_type(type_str)) == repr(expected_type)

    @pytest.mark.parametrize(
        "type_str",
        [
            "invalid_type",
            # "varchar(10, 2)",
        ],
    )
    def test_unsupported_types(self, type_str):
        with pytest.raises(TypeError):
            parse_data_type(type_str)

    @pytest.mark.parametrize(
        "type_str",
        [
            "array<int",
            "array<ARRAY>",
            "map<int,>",
            "MAP<MAP>",
            "struct<name int",
            "xxxx<Array<int>>"
            "varchar(10, 5, 3)",
        ],
    )
    def test_invalid_syntax(self, type_str):
        with pytest.raises(LarkError):
            parse_data_type(type_str)


class TestMVRefreshParser:
    @pytest.mark.parametrize(
        "clause, expected",
        [
            # Correct cases
            ("REFRESH IMMEDIATE", {"refresh_moment": "IMMEDIATE", "refresh_type": None}),
            ("REFRESH DEFERRED", {"refresh_moment": "DEFERRED", "refresh_type": None}),
            ("REFRESH ASYNC", {"refresh_moment": None, "refresh_type": "ASYNC"}),
            ("REFRESH MANUAL", {"refresh_moment": None, "refresh_type": "MANUAL"}),
            ("REFRESH INCREMENTAL", {"refresh_moment": None, "refresh_type": "INCREMENTAL"}),
            ("REFRESH DEFERRED ASYNC", {"refresh_moment": "DEFERRED", "refresh_type": "ASYNC"}),
            ("REFRESH IMMEDIATE MANUAL", {"refresh_moment": "IMMEDIATE", "refresh_type": "MANUAL"}),
            ("REFRESH ASYNC EVERY (INTERVAL 1 DAY)", {"refresh_moment": None, "refresh_type": "ASYNC EVERY (INTERVAL 1 DAY)"}),
            ("REFRESH ASYNC START ('2025-01-01 12:00:00') EVERY (INTERVAL 1 HOUR)", {"refresh_moment": None, "refresh_type": "ASYNC START ('2025-01-01 12:00:00') EVERY (INTERVAL 1 HOUR)"}),
            ("refresh deferred async", {"refresh_moment": "DEFERRED", "refresh_type": "ASYNC"}), # Case-insensitivity
        ]
    )
    def test_correct_clauses(self, clause, expected):
        """Tests that various correct refresh clauses are parsed as expected."""
        result = parse_mv_refresh_clause(clause)
        assert result == expected

    @pytest.mark.parametrize(
        "clause",
        [
            # Incorrect cases
            "REFRESH",
            "REFRESH FOO",
            # "REFRESH ASYNC EVERY",
            # "REFRESH ASYNC EVERY (INTERVAL 1 MONTHS)", # Invalid time unit
            "REFRESH IMMEDIATE DEFERRED",
            # "REFRESH ASYNC MANUAL",
        ]
    )
    def test_incorrect_clauses(self, clause):
        """Tests that various incorrect refresh clauses raise a LarkError."""
        from lark.exceptions import LarkError
        with pytest.raises(LarkError):
            parse_mv_refresh_clause(clause)


class TestKeyClauseParser:
    @pytest.mark.parametrize(
        "clause, expected",
        [
            ("PRIMARY KEY(id)", ReflectedTableKeyInfo(type="PRIMARY KEY", columns="id")),
            ("PRIMARY KEY ( id )", ReflectedTableKeyInfo(type="PRIMARY KEY", columns="id")),
            ("primary key(id, name)", ReflectedTableKeyInfo(type="PRIMARY KEY", columns="id, name")),
            ("PRIMARY KEY ( id, name )", ReflectedTableKeyInfo(type="PRIMARY KEY", columns="id, name")),
            ("DUPLICATE KEY (k)", ReflectedTableKeyInfo(type="DUPLICATE KEY", columns="k")),
            ("duplicate key ( k1, k2 )", ReflectedTableKeyInfo(type="DUPLICATE KEY", columns="k1, k2")),
            ("AGGREGATE KEY(k)", ReflectedTableKeyInfo(type="AGGREGATE KEY", columns="k")),
            ("aggregate key ( k1, k2 )", ReflectedTableKeyInfo(type="AGGREGATE KEY", columns="k1, k2")),
        ],
    )
    def test_valid_clauses(self, clause, expected):
        got = StarRocksTableDefinitionParser.parse_key_clause(clause)
        assert isinstance(got, ReflectedTableKeyInfo)
        assert str(got) == str(expected)

    @pytest.mark.parametrize(
        "clause",
        [
            None,
            "",
            "   ",
            "KEY (id)",
            "PRIMARY(id)",
            "PRIMARY KEY",  # missing columns
            "PRIMARY KEY (",  # bad paren
            "PRIMARY KEY )",
        ],
    )
    def test_invalid_clauses(self, clause):
        got = StarRocksTableDefinitionParser.parse_key_clause(clause)
        assert got is None


class TestColumnDefaultReflection:
    def setup_method(self):
        self.parser = StarRocksTableDefinitionParser(dialect=None, preparer=None)

    @staticmethod
    def _make_column(
        *,
        name: str,
        column_type: str = "int",
        default: Optional[str] = None,
    ):
        return SimpleNamespace(
            COLUMN_NAME=name,
            COLUMN_TYPE=column_type,
            IS_NULLABLE="YES",
            COLUMN_DEFAULT=default,
            COLUMN_COMMENT=None,
            COLUMN_KEY="",
            ORDINAL_POSITION=1,
            GENERATION_EXPRESSION=None,
            EXTRA=None,
        )

    def test_literal_defaults_are_double_quoted(self):
        cases = [
            ("c_str", "varchar(20)", "plain", "'plain'"),
            ("c_int", "int", "123", "'123'"),
            ("c_keyword", "varchar(10)", "CURRENT_TIMESTAMP", "'CURRENT_TIMESTAMP'"),
        ]
        for name, column_type, default, expected in cases:
            column = self._make_column(name=name, column_type=column_type, default=default)
            parsed = self.parser._parse_column(column)
            assert parsed["default"] == expected

    def test_expression_defaults_use_raw_sql(self):
        cases = [
            ("c_uuid", "varchar(36)", "uuid()", "(uuid())"),
            ("c_uuid_numeric", "varchar(36)", "uuid_numeric()", "(uuid_numeric())"),
            ("c_now", "datetime", "NOW(6)", "(NOW(6))"),
            ("c_current_ts", "datetime", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"),
            ("c_current_ts_precise", "datetime", "CURRENT_TIMESTAMP(3)", "CURRENT_TIMESTAMP(3)"),
        ]
        for name, column_type, default, expected in cases:
            column = self._make_column(name=name, column_type=column_type, default=default)
            parsed = self.parser._parse_column(column)
            assert parsed["default"] == expected

    def test_null_default_remains_null_keyword(self):
        column = self._make_column(name="c_null", default="NULL")
        parsed = self.parser._parse_column(column)
        assert parsed["default"] is None or parsed["default"] == "NULL"


class TestColumnFlagReflection:
    def test_marks_primary_key_columns_and_nullable(self):
        parser = StarRocksTableDefinitionParser(dialect=None, preparer=None)
        table_row = SimpleNamespace(
            TABLE_NAME="t_flags",
            TABLE_SCHEMA="test_db",
            TABLE_COMMENT="OLAP",
        )
        columns = [
            SimpleNamespace(
                COLUMN_NAME="k_nullable",
                COLUMN_TYPE="int",
                IS_NULLABLE="YES",
                COLUMN_DEFAULT=None,
                COLUMN_COMMENT=None,
                COLUMN_KEY="PRI",
                ORDINAL_POSITION=1,
                GENERATION_EXPRESSION=None,
            ),
            SimpleNamespace(
                COLUMN_NAME="k_not_null",
                COLUMN_TYPE="int",
                IS_NULLABLE="NO",
                COLUMN_DEFAULT=None,
                COLUMN_COMMENT=None,
                COLUMN_KEY="PRI",
                ORDINAL_POSITION=2,
                GENERATION_EXPRESSION=None,
            ),
            SimpleNamespace(
                COLUMN_NAME="v",
                COLUMN_TYPE="int",
                IS_NULLABLE="YES",
                COLUMN_DEFAULT=None,
                COLUMN_COMMENT=None,
                COLUMN_KEY="",
                ORDINAL_POSITION=3,
                GENERATION_EXPRESSION=None,
            ),
        ]
        state = parser.parse_table(
            table=table_row,
            table_config={},
            columns=columns,
        )
        metadata = MetaData()
        table = Table(table_row.TABLE_NAME, metadata, schema=table_row.TABLE_SCHEMA)

        reflection_info = _ReflectionInfo(
            columns={},
            pk_constraint={},
            foreign_keys={},
            indexes={},
            unique_constraints={},
            table_comment={},
            check_constraints={},
            table_options={},
            unreflectable={},
        )
        table_key = (table_row.TABLE_SCHEMA, table_row.TABLE_NAME)
        reflection_info.columns[table_key] = state.columns
        reflection_info.pk_constraint[table_key] = {
            "constrained_columns": [col_name for col_name, *_ in state.keys[0]["columns"]],
            "name": None,
            "comment": None,
            "dialect_options": None,
        }
        reflection_info.foreign_keys[table_key] = []
        reflection_info.indexes[table_key] = []
        reflection_info.unique_constraints[table_key] = []
        reflection_info.check_constraints[table_key] = []
        reflection_info.table_comment[table_key] = None
        reflection_info.table_options[table_key] = {}

        dialect = StarRocksDialect()
        dialect._parsed_state_or_create = lambda *args, **kwargs: state  # type: ignore[assignment]

        inspector = StarRocksInspector.__new__(StarRocksInspector)
        inspector.bind = SimpleNamespace(dialect=dialect)
        inspector.engine = None
        inspector.dialect = dialect
        inspector.info_cache = {}
        inspector._op_context_requires_connect = False

        @contextlib.contextmanager
        def dummy_operation_context():
            class DummyConn:
                @staticmethod
                def schema_for_object(table_obj):
                    return table_obj.schema

            yield DummyConn()

        inspector._operation_context = MethodType(lambda self: dummy_operation_context(), inspector)

        def fake_get_reflection_info(self, schema, filter_names, kind, scope, _reflect_info=None, **kw):
            return reflection_info

        inspector._get_reflection_info = MethodType(fake_get_reflection_info, inspector)
        inspector.has_table = MethodType(lambda self, table_name, schema=None: True, inspector)

        inspector.reflect_table(table, include_columns=None)

        assert table.c.k_nullable.primary_key is True
        assert table.c.k_nullable.nullable is True
        assert table.c.k_not_null.primary_key is True
        assert table.c.k_not_null.nullable is False
        assert table.c.v.primary_key is False
        assert table.c.v.nullable is True
