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

"""Decoding of the server's complex-value rendering and related reflection.

The raw strings are what StarRocks 4.1.4 returns over the MySQL protocol.
"""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from starrocks import datatype
from starrocks.dialect import StarRocksDialect
from starrocks.drivers.parsers import parse_data_type

_DIALECT = StarRocksDialect()


def _process(type_, raw):
    return type_._cached_result_processor(_DIALECT, None)(raw)


class TestComplexValueDecoding:
    def test_map_with_unquoted_integer_keys(self):
        assert _process(datatype.MAP(datatype.INTEGER, datatype.VARCHAR(20)),
                        '{1:"a\\"b",2:null}') == {1: 'a"b', 2: None}

    def test_array_of_maps_with_integer_keys(self):
        assert _process(datatype.ARRAY(datatype.MAP(datatype.INTEGER, datatype.INTEGER)),
                        "[{3:4},null]") == [{3: 4}, None]

    def test_raw_control_characters_in_strings(self):
        assert _process(datatype.ARRAY(datatype.VARCHAR(9)),
                        '["nl\nx","tab\ty"]') == ["nl\nx", "tab\ty"]

    def test_decimal_keys_and_largeint_values(self):
        big = 170141183460469231731687303715884105727
        assert _process(datatype.MAP(datatype.DECIMAL(10, 2), datatype.LARGEINT),
                        "{1.25:%d}" % big) == {Decimal("1.25"): big}

    def test_struct_members_are_converted(self):
        struct = datatype.STRUCT(x=datatype.DATETIME, d=datatype.DATE,
                                 k=datatype.MAP(datatype.BOOLEAN, datatype.DOUBLE))
        got = _process(struct, '{"x":"2026-01-02 03:04:05.000007","d":"2026-02-03","k":{1:1.5}}')
        assert got == {"x": datetime(2026, 1, 2, 3, 4, 5, 7), "d": date(2026, 2, 3),
                       "k": {True: 1.5}}
        assert type(got["k"][True]) is float

    def test_struct_copy_keeps_fields(self):
        struct = datatype.STRUCT(a=datatype.INTEGER, b=datatype.DATE)
        assert [n for n, _ in struct.copy().field_tuples] == ["a", "b"]
        assert [n for n, _ in struct.dialect_impl(_DIALECT).field_tuples] == ["a", "b"]

    def test_nested_binary_is_hex(self):
        assert _process(datatype.ARRAY(datatype.VARBINARY), '["616263"]') == [b"abc"]

    def test_malformed_value_raises(self):
        with pytest.raises(ValueError, match="could not parse complex value"):
            _process(datatype.ARRAY(datatype.INTEGER), "[1,2")

    def test_single_quote_escapes(self):
        assert datatype.parse_complex_value(r"['it\'s \\ é']") == ["it's \\ é"]


class TestScalarProcessors:
    def test_datetime_fraction_is_scaled_to_microseconds(self):
        proc = datatype.DATETIME().result_processor(_DIALECT, None)
        assert proc("2026-01-02 03:04:05.5") == datetime(2026, 1, 2, 3, 4, 5, 500000)

    def test_datetime_rejects_aware_bind(self):
        bind = datatype.DATETIME().bind_processor(_DIALECT)
        with pytest.raises(ValueError, match="no time zone"):
            bind(datetime(2026, 1, 1, tzinfo=timezone.utc))

    def test_largeint_text_becomes_int(self):
        proc = datatype.LARGEINT()._cached_result_processor(_DIALECT, None)
        assert proc("-170141183460469231731687303715884105728") == -(2 ** 127)


class TestReflection:
    def test_struct_with_backquoted_field_names(self):
        t = parse_data_type("struct<`k` map<boolean,double>, `a b` varchar(50), `c``d` int(11)>")
        assert [n for n, _ in t.field_tuples] == ["k", "a b", "c`d"]
        assert t.get_col_spec().startswith("STRUCT<k MAP<BOOLEAN, DOUBLE>, `a b` VARCHAR(50)")

    def test_nested_largeint_display_width(self):
        t = parse_data_type("map<decimal(10, 2),largeint(40)>")
        assert isinstance(t, datatype.MAP)

    def test_double_returns_float(self):
        t = parse_data_type("double")
        assert t._cached_result_processor(_DIALECT, None) is None or \
            t._cached_result_processor(_DIALECT, None)(1.2345678901234567) == 1.2345678901234567
        assert t.asdecimal is False
