#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import date, datetime
from decimal import Decimal

import pytest

from starrocks import datatype
from starrocks.dialect import StarRocksDialect

_DIALECT = StarRocksDialect()


def _sub_type_reprs(structured_type):
    return {repr(sub_type) for sub_type in structured_type.get_sub_item_types()}


def _col_spec(structured_type):
    return structured_type.get_col_spec()


class TestStructuredTypeSubItems:
    def test_array_with_scalar_items(self):
        array_type = datatype.ARRAY(datatype.INTEGER)

        assert _sub_type_reprs(array_type) == {repr(datatype.INTEGER())}

    def test_map_with_struct_and_json(self):
        map_type = datatype.MAP(
            datatype.STRING,
            datatype.ARRAY(
                datatype.STRUCT(
                    ("flag", datatype.BOOLEAN),
                    ("payload", datatype.JSON()),
                )
            ),
        )

        assert _sub_type_reprs(map_type) == {
            repr(datatype.STRING()),
            repr(
                datatype.ARRAY(
                    datatype.STRUCT(
                        ("flag", datatype.BOOLEAN),
                        ("payload", datatype.JSON()),
                    )
                )
            ),
            repr(
                datatype.STRUCT(
                    ("flag", datatype.BOOLEAN),
                    ("payload", datatype.JSON()),
                )
            ),
            repr(datatype.BOOLEAN()),
            repr(datatype.JSON()),
        }

    def test_struct_with_mixed_nested_types(self):
        struct_type = datatype.STRUCT(
            ("id", datatype.BIGINT),
            ("tags", datatype.ARRAY(datatype.STRING)),
            (
                "props",
                datatype.MAP(
                    datatype.VARCHAR(5),
                    datatype.STRUCT(
                        ("k", datatype.CHAR(3)),
                        ("v", datatype.DECIMAL(9, 3)),
                        ("extra", datatype.ARRAY(datatype.JSON())),
                    ),
                ),
            ),
        )

        assert _sub_type_reprs(struct_type) == {
            repr(datatype.BIGINT()),
            repr(datatype.ARRAY(datatype.STRING)),
            repr(datatype.STRING()),
            repr(
                datatype.MAP(
                    datatype.VARCHAR(5),
                    datatype.STRUCT(
                        ("k", datatype.CHAR(3)),
                        ("v", datatype.DECIMAL(9, 3)),
                        ("extra", datatype.ARRAY(datatype.JSON())),
                    ),
                )
            ),
            repr(datatype.VARCHAR(5)),
            repr(
                datatype.STRUCT(
                    ("k", datatype.CHAR(3)),
                    ("v", datatype.DECIMAL(9, 3)),
                    ("extra", datatype.ARRAY(datatype.JSON())),
                )
            ),
            repr(datatype.CHAR(3)),
            repr(datatype.DECIMAL(9, 3)),
            repr(datatype.ARRAY(datatype.JSON())),
            repr(datatype.JSON()),
        }


class TestStructuredTypeColSpec:
    def test_array_simple_col_spec(self):
        array_type = datatype.ARRAY(datatype.INTEGER)

        assert _col_spec(array_type) == "ARRAY<INTEGER>"

    def test_array_struct_col_spec(self):
        array_type = datatype.ARRAY(
            datatype.STRUCT(
                ("flag", datatype.BOOLEAN),
                ("payload", datatype.JSON()),
            )
        )

        assert _col_spec(array_type) == "ARRAY<STRUCT<flag BOOLEAN, payload JSON>>"

    def test_map_nested_col_spec(self):
        map_type = datatype.MAP(
            datatype.STRING,
            datatype.ARRAY(
                datatype.STRUCT(
                    ("flag", datatype.BOOLEAN),
                    ("payload", datatype.JSON()),
                )
            ),
        )

        assert (
            _col_spec(map_type)
            == "MAP<STRING, ARRAY<STRUCT<flag BOOLEAN, payload JSON>>>"
        )

    def test_struct_mixed_col_spec(self):
        struct_type = datatype.STRUCT(
            ("id", datatype.BIGINT),
            ("tags", datatype.ARRAY(datatype.STRING)),
            (
                "props",
                datatype.MAP(
                    datatype.VARCHAR(5),
                    datatype.STRUCT(
                        ("k", datatype.CHAR(3)),
                        ("v", datatype.DECIMAL(9, 3)),
                        ("extra", datatype.ARRAY(datatype.JSON())),
                    ),
                ),
            ),
        )

        assert (
            _col_spec(struct_type)
            == "STRUCT<id BIGINT, tags ARRAY<STRING>, props MAP<VARCHAR(5), STRUCT<k CHAR(3), v DECIMAL(9, 3), extra ARRAY<JSON>>>>"
        )


def _process(type_obj, value):
    """Call result_processor and apply it to value."""
    proc = type_obj.result_processor(dialect=_DIALECT, coltype=None)
    if proc is None:
        return value
    return proc(value)


class TestArrayResultProcessor:
    def test_array_of_ints(self):
        assert _process(datatype.ARRAY(datatype.INTEGER), "[1,2,3]") == [1, 2, 3]

    def test_array_of_strings(self):
        assert _process(datatype.ARRAY(datatype.VARCHAR(50)), '["a","b"]') == ["a", "b"]

    def test_array_null_returns_none(self):
        assert _process(datatype.ARRAY(datatype.INTEGER), None) is None

    def test_array_already_list(self):
        assert _process(datatype.ARRAY(datatype.INTEGER), [4, 5]) == [4, 5]

    def test_nested_array(self):
        assert _process(datatype.ARRAY(datatype.ARRAY(datatype.INTEGER)), "[[1,2],[3]]") == [[1, 2], [3]]

    def test_null_element_preserved(self):
        assert _process(datatype.ARRAY(datatype.INTEGER), "[1,null,3]") == [1, None, 3]

    def test_array_of_dates_applies_element_processor(self):
        result = _process(datatype.ARRAY(datatype.DATE()), '["2024-01-15","2024-06-30"]')
        assert result == [date(2024, 1, 15), date(2024, 6, 30)]

    def test_array_of_datetimes_applies_element_processor(self):
        result = _process(datatype.ARRAY(datatype.DATETIME()), '["2024-01-15 10:30:00"]')
        assert result == [datetime(2024, 1, 15, 10, 30, 0)]

    def test_array_of_decimal_preserves_precision(self):
        result = _process(datatype.ARRAY(datatype.DECIMAL(18, 18)), '[0.123456789012345678]')
        assert isinstance(result[0], Decimal)
        assert result[0] == Decimal('0.123456789012345678')

    def test_array_of_json_returns_raw_string(self):
        # StarRocks sends ARRAY<JSON> using single-quoted elements (not valid JSON).
        # We fall back to returning the raw string rather than crashing.
        raw = """['{"a":1}','{"b":2}']"""
        assert _process(datatype.ARRAY(datatype.JSON()), raw) == raw

    @pytest.mark.parametrize("json_str,expected", [
        ("[1]", [1]),
        ("[]", []),
        ('["x","y","z"]', ["x", "y", "z"]),
    ])
    def test_parametrized_cases(self, json_str, expected):
        item_type = datatype.INTEGER if expected and isinstance(expected[0], int) else datatype.VARCHAR(10)
        assert _process(datatype.ARRAY(item_type), json_str) == expected


class TestMapResultProcessor:
    def test_simple_map(self):
        assert _process(datatype.MAP(datatype.VARCHAR(10), datatype.INTEGER), '{"a":1,"b":2}') == {"a": 1, "b": 2}

    def test_map_null_returns_none(self):
        assert _process(datatype.MAP(datatype.VARCHAR(10), datatype.INTEGER), None) is None

    def test_map_already_dict(self):
        assert _process(datatype.MAP(datatype.VARCHAR(10), datatype.INTEGER), {"x": 9}) == {"x": 9}

    def test_null_value_in_map_preserved(self):
        result = _process(datatype.MAP(datatype.VARCHAR(10), datatype.INTEGER), '{"a":null}')
        assert result == {"a": None}

    def test_map_with_array_values(self):
        t = datatype.MAP(datatype.VARCHAR(10), datatype.ARRAY(datatype.INTEGER))
        assert _process(t, '{"x":[1,2],"y":[3]}') == {"x": [1, 2], "y": [3]}

    def test_nested_map(self):
        t = datatype.MAP(datatype.VARCHAR(10), datatype.MAP(datatype.VARCHAR(10), datatype.INTEGER))
        assert _process(t, '{"outer":{"inner":42}}') == {"outer": {"inner": 42}}

    def test_map_decimal_value_preserves_precision(self):
        result = _process(
            datatype.MAP(datatype.VARCHAR(10), datatype.DECIMAL(18, 18)),
            '{"k":0.123456789012345678}'
        )
        assert isinstance(result["k"], Decimal)
        assert result["k"] == Decimal('0.123456789012345678')


class TestStructResultProcessor:
    def test_simple_struct(self):
        t = datatype.STRUCT(name=datatype.VARCHAR(50), age=datatype.INTEGER)
        assert _process(t, '{"name":"Alice","age":30}') == {"name": "Alice", "age": 30}

    def test_struct_null_returns_none(self):
        t = datatype.STRUCT(x=datatype.INTEGER)
        assert _process(t, None) is None

    def test_struct_already_dict(self):
        t = datatype.STRUCT(x=datatype.INTEGER)
        assert _process(t, {"x": 7}) == {"x": 7}

    def test_null_field_preserved(self):
        t = datatype.STRUCT(name=datatype.VARCHAR(50), age=datatype.INTEGER)
        assert _process(t, '{"name":null,"age":30}') == {"name": None, "age": 30}

    def test_struct_with_array_field(self):
        t = datatype.STRUCT(tags=datatype.ARRAY(datatype.VARCHAR(20)), score=datatype.INTEGER)
        result = _process(t, '{"tags":["c++","python"],"score":9}')
        assert result == {"tags": ["c++", "python"], "score": 9}
        assert isinstance(result["tags"], list)

    def test_struct_applies_field_processor_for_dates(self):
        t = datatype.STRUCT(created=datatype.DATE())
        result = _process(t, '{"created":"2024-03-01"}')
        assert result == {"created": date(2024, 3, 1)}

    def test_struct_unknown_field_passed_through(self):
        """Extra keys from DB not in the type definition are passed through unchanged."""
        t = datatype.STRUCT(name=datatype.VARCHAR(50))
        result = _process(t, '{"name":"Bob","extra":"ignored"}')
        assert result["name"] == "Bob"
        assert result["extra"] == "ignored"

    def test_struct_decimal_field_preserves_precision(self):
        result = _process(
            datatype.STRUCT(price=datatype.DECIMAL(18, 18)),
            '{"price":0.123456789012345678}'
        )
        assert isinstance(result["price"], Decimal)
        assert result["price"] == Decimal('0.123456789012345678')


class TestMapNonStringKeys:
    """MAP keys that are non-string types must be cast from JSON string keys."""

    def test_integer_keys(self):
        result = _process(datatype.MAP(datatype.INTEGER, datatype.VARCHAR(10)), '{"1":"a","2":"b"}')
        assert list(result.keys()) == [1, 2]
        assert all(isinstance(k, int) for k in result.keys())

    def test_bigint_keys(self):
        result = _process(datatype.MAP(datatype.BIGINT, datatype.INTEGER), '{"100":1}')
        assert list(result.keys()) == [100]
        assert isinstance(list(result.keys())[0], int)

    def test_string_keys_unchanged(self):
        result = _process(datatype.MAP(datatype.VARCHAR(10), datatype.INTEGER), '{"a":1}')
        assert list(result.keys()) == ["a"]
        assert isinstance(list(result.keys())[0], str)
