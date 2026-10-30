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

"""Reading ARRAY, MAP and STRUCT values from the text StarRocks sends, without a server.

The texts are what StarRocks 4.1.6 and 3.3.22 send over the MySQL protocol, unless a case says otherwise.
"""

from datetime import date, datetime
from decimal import Decimal
import math
from unittest import mock

from sqlalchemy import testing
from sqlalchemy.testing import eq_, expect_raises, expect_raises_message, fixtures

from starrocks import datatype
from starrocks.dialect import StarRocksDialect


_LARGEST = 2**127 - 1


def _dialect(version=(4, 1, 6)):
    dialect = StarRocksDialect()
    dialect.server_version_info = version
    return dialect


def _read(type_, text, version=(4, 1, 6)):
    return type_.result_processor(_dialect(version), None)(text)


class _RefusingDecoder:
    """Stands in for the JSON decoder, so that every value is read by the scanner."""

    def decode(self, text):
        raise ValueError("JSON is switched off")


class CollectionTextDecodingTest(fixtures.TestBase):
    """``json`` and the scanner decode a collection's text to the same values, whichever reads it."""

    @testing.combinations(
        ("decimals_keep_their_digits", "[1.50,2.25,0.00]"),
        ("decimals_at_full_scale", "[12345.6780000000,0.0000010000]"),
        ("largeint", "[%d,-%d]" % (_LARGEST, _LARGEST + 1)),
        ("doubles", "[1.5,1e+308,0,1e-07,1.2345678901234568e+17,1E+2,-0.0]"),
        ("nested_binary_as_hex", '["616263",null]'),
        ("nested_binary_as_bytes_on_3_3", '["abc"]'),
        ("binary_selected_as_hex", '{"hex":["616263","FFFE",null]}'),
        ("escaped_quote_and_backslash", r'["a\"b","c\\d"]'),
        ("raw_control_characters", '["e\nf","tab\tx","x\x01"," "]'),
        ("non_ascii", '["é中\U0001f600"]'),
        ("json_escapes", r'["é😀\/\b\f\r"]'),
        ("unicode_escapes", r'["\u00e9\u0041"]'),
        ("escaped_surrogate_pair", r'["\ud83d\ude00","a\ud83d\ude00b"]'),
        ("escaped_lone_surrogate", r'["\ud83d"]'),
        ("struct", '{"a":1,"b":"x","c":"2024-01-02 03:04:05"}'),
        ("map_of_string_keys", '{"key0":1,"key1":2}'),
        ("map_of_date_keys", '{"2024-01-02":1}'),
        ("map_of_datetime_keys", '{"2024-01-02 03:04:05.123456":1}'),
        ("nested", '[[1,2],[3]]'),
        ("map_of_arrays", '{"k":[1,2]}'),
        ("array_of_structs", '[{"a":1,"b":"x"},null]'),
        ("nulls", "[null,1]"),
        ("empty_array", "[]"),
        ("empty_map", "{}"),
        ("booleans", "[1,0]"),
        ("whitespace", ' [ 1 , {"a" : 2} ] '),
        ("repeated_key", '{"a":1,"a":2}'),
        argnames="text",
        id_="ia",
    )
    def test_json_text_decodes_alike(self, text):
        """``repr`` tells ``1`` from ``True``, ``Decimal('1.0')`` from ``Decimal('1.00')``, and a list from a tuple."""
        decoded = datatype._COLLECTION_JSON.decode(text)
        eq_(repr(datatype.scan_collection_text(text)), repr(decoded))
        eq_(repr(datatype.decode_collection_text(text)), repr(decoded))

    @testing.combinations(
        ("map_of_integer_keys", '{1:"a",2:"b"}', ((1, "a"), (2, "b"))),
        ("map_of_an_escaped_surrogate_pair", r'{1:"\ud83d\ude00"}', ((1, "\U0001f600"),)),
        ("map_of_decimal_keys", "{1.25:1}", ((Decimal("1.25"), 1),)),
        ("map_of_boolean_keys", "{1:1,0:2}", ((1, 1), (0, 2))),
        ("map_inside_an_array", "[{1:2}]", [((1, 2),)]),
        ("map_inside_a_struct", '{"x":{1:"a"}}', (("x", ((1, "a"),)),)),
        ("json_elements", """['{"a": 1}','"1"','"null"',null]""", ['{"a": 1}', '"1"', '"null"', None]),
        ("json_element_escapes", r"""['{"a": "x\\"y", "b": "it\'s"}']""", ['{"a": "x\\"y", "b": "it\'s"}']),
        ("json_in_a_map", """{"k":'[1, 2]'}""", (("k", "[1, 2]"),)),
        ("non_finite_doubles", "[inf,-inf,1.5]", [math.inf, -math.inf, Decimal("1.5")]),
        argnames="text,decoded",
        id_="iaa",
    )
    def test_text_json_cannot_read_is_scanned(self, text, decoded):
        with expect_raises(ValueError):
            datatype._COLLECTION_JSON.decode(text)
        eq_(repr(datatype.decode_collection_text(text)), repr(decoded))

    def test_nan_is_scanned(self):
        assert math.isnan(datatype.decode_collection_text("[nan]")[0])

    @testing.combinations(
        ("unterminated_array", "[1,2"),
        ("trailing_comma", "[1,]"),
        ("missing_comma", "[1 2]"),
        ("missing_value", "{1:}"),
        ("unknown_word", "[x]"),
        ("unterminated_string", '["a]'),
        ("trailing_text", "[1]x"),
        ("mismatched_bracket", "[1}"),
        ("key_that_is_an_array", "{[1]:2}"),
        ("json_nan", "[NaN]"),
        ("json_infinity", "[Infinity]"),
        ("unknown_escape", r'["\q"]'),
        ("empty", ""),
        argnames="text",
        id_="ia",
    )
    def test_malformed_text_is_refused(self, text):
        with expect_raises_message(ValueError, "could not read"):
            datatype.decode_collection_text(text)

    def test_bytes_are_decoded_as_utf8(self):
        eq_(datatype.decode_collection_text('["é"]'.encode("utf-8")), ["é"])


class CollectionColumnReadingTest(fixtures.TestBase):
    """Each element is read as its declared type, from what the server writes."""

    @testing.combinations(
        (
            "map_of_integer_keys",
            datatype.MAP(datatype.INTEGER(), datatype.VARCHAR(20)),
            r'{1:"a\"b",2:null}',
            {1: 'a"b', 2: None},
        ),
        (
            "array_of_maps_of_integer_keys",
            datatype.ARRAY(datatype.MAP(datatype.INTEGER(), datatype.INTEGER())),
            "[{3:4},null,{}]",
            [{3: 4}, None, {}],
        ),
        (
            "decimal_keys_and_largeint_values",
            datatype.MAP(datatype.DECIMAL(10, 2), datatype.LARGEINT()),
            "{1.25:%d,-0.50:null}" % _LARGEST,
            {Decimal("1.25"): _LARGEST, Decimal("-0.50"): None},
        ),
        (
            "boolean_keys",
            datatype.MAP(datatype.BOOLEAN(), datatype.DOUBLE()),
            "{1:1.5,0:inf}",
            {True: 1.5, False: math.inf},
        ),
        (
            "struct_fields",
            datatype.STRUCT(x=datatype.DATETIME(), d=datatype.DATE(), n=datatype.DECIMAL(5, 2), s=datatype.VARCHAR(9)),
            '{"x":"2026-01-02 03:04:05.000007","d":"2026-02-03","n":1.50,"s":"nl\nx"}',
            {"x": datetime(2026, 1, 2, 3, 4, 5, 7), "d": date(2026, 2, 3), "n": Decimal("1.50"), "s": "nl\nx"},
        ),
        (
            "struct_field_names_in_another_case",
            datatype.STRUCT(Mixed=datatype.DATE()),
            '{"mixed":"2026-02-03"}',
            {"mixed": date(2026, 2, 3)},
        ),
        (
            "array_of_json",
            datatype.ARRAY(datatype.JSON()),
            r"""['{"a": 1}','[2, "x\'s"]',null]""",
            [{"a": 1}, [2, "x's"], None],
        ),
        (
            "single_quoted_strings",
            datatype.ARRAY(datatype.VARCHAR(20)),
            r"['it\'s \\ ok']",
            ["it's \\ ok"],
        ),
        ("binary_as_hex", datatype.ARRAY(datatype.VARBINARY()), '["616263",null]', [b"abc", None]),
        argnames="type_,text,expected",
        id_="iaaa",
    )
    def test_read(self, type_, text, expected):
        read = _read(type_, text)
        eq_(read, expected)
        eq_([type(value) for value in _values(read)], [type(value) for value in _values(expected)])

    def test_a_malformed_value_raises(self):
        with expect_raises_message(ValueError, "could not read"):
            _read(datatype.ARRAY(datatype.INTEGER()), "[1,2")

    @testing.combinations(
        ("array_of_struct", datatype.ARRAY(datatype.STRUCT(("a", datatype.INTEGER()), ("c", datatype.DATETIME()))),
         '[{"a":1,"c":"2024-01-02 03:04:05.500000"},null]', (4, 1, 6)),
        ("decimals", datatype.ARRAY(datatype.DECIMAL(20, 10)), "[12345.6780000000,0.0000010000]", (4, 1, 6)),
        ("doubles", datatype.ARRAY(datatype.DOUBLE()), "[1.5,1e+308,0,1e-07]", (4, 1, 6)),
        ("largeint", datatype.ARRAY(datatype.LARGEINT()), "[%d]" % _LARGEST, (4, 1, 6)),
        ("map_of_string_keys", datatype.MAP(datatype.VARCHAR(10), datatype.INTEGER()), '{"a":1,"b":null}', (4, 1, 6)),
        ("map_of_date_keys", datatype.MAP(datatype.DATE(), datatype.BOOLEAN()), '{"2024-01-02":1}', (4, 1, 6)),
        ("binary_as_hex", datatype.ARRAY(datatype.VARBINARY()), '["616263"]', (4, 1, 6)),
        ("binary_as_bytes_on_3_3", datatype.ARRAY(datatype.VARBINARY()), '["abc"]', (3, 3, 22)),
        ("binary_selected_as_hex_on_3_3", datatype.ARRAY(datatype.VARBINARY()), '{"hex":["FFFE",null]}', (3, 3, 22)),
        argnames="type_,text,version",
        id_="iaaa",
    )
    def test_json_and_the_scanner_read_a_column_alike(self, type_, text, version):
        read_with_json = _read(type_, text, version)
        with mock.patch.object(datatype, "_COLLECTION_JSON", _RefusingDecoder()):
            read_with_the_scanner = _read(type_, text, version)
        eq_(repr(read_with_the_scanner), repr(read_with_json))


def _values(value):
    """Flatten a read value into its leaves, keys included, to compare their types."""
    if isinstance(value, dict):
        return [leaf for pair in value.items() for item in pair for leaf in _values(item)]
    if isinstance(value, list):
        return [leaf for item in value for leaf in _values(item)]
    return [value]


class ConvertersBuiltOnceTest(fixtures.TestBase):
    """A column's element conversions are built with its result processor, not again for every value."""

    _TYPE = datatype.ARRAY(
        datatype.STRUCT(("a", datatype.INTEGER()), ("b", datatype.VARCHAR(10)), ("c", datatype.DATETIME()))
    )
    _TEXT = '[{"a":1,"b":"x","c":"2024-01-02 03:04:05"},{"a":2,"b":"y","c":"2024-01-03 03:04:05"}]'

    def test_reading_values_builds_no_converter(self):
        read = self._TYPE.result_processor(_dialect(), None)
        with mock.patch.object(datatype, "_element_converter", side_effect=AssertionError("rebuilt")), \
                mock.patch.object(datatype.DATETIME, "result_processor", side_effect=AssertionError("rebuilt")):
            values = [read(self._TEXT) for _ in range(3)]
        eq_(values[2][1], {"a": 2, "b": "y", "c": datetime(2024, 1, 3, 3, 4, 5)})

    def test_each_element_type_is_set_up_once(self):
        with mock.patch.object(
            datatype.DATETIME, "result_processor", autospec=True, side_effect=datatype.DATETIME.result_processor
        ) as datetime_setup:
            read = self._TYPE.result_processor(_dialect(), None)
            for _ in range(5):
                read(self._TEXT)
        eq_(datetime_setup.call_count, 1)
