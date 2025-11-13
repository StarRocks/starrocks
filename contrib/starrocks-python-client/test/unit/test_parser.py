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

import logging

from lark import LarkError
import pytest

from starrocks import datatype as datatype
from starrocks.drivers.parsers import parse_data_type, parse_mv_refresh_clause
from starrocks.engine.interfaces import ReflectedTableKeyInfo
from starrocks.reflection import StarRocksTableDefinitionParser


logger = logging.getLogger(__name__)


class TestDataTypeParser:
    @pytest.mark.parametrize(
        "type_str, expected_type",
        [
            ("boolean", datatype.BOOLEAN),
            ("tinyint", datatype.TINYINT),
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
