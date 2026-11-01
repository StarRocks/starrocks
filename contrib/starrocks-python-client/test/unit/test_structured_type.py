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

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import enum
import json
import math
import pickle
import warnings

import sqlalchemy as sa
from sqlalchemy import Column, Enum, Float, MetaData, PickleType, Table, TypeDecorator, insert, testing
from sqlalchemy.testing import AssertsCompiledSQL, fixtures

from starrocks import datatype
from starrocks.dialect import StarRocksDialect


_DIALECT = StarRocksDialect()


def _sub_type_reprs(structured_type):
    return {repr(sub_type) for sub_type in structured_type.get_sub_item_types()}


def _col_spec(structured_type):
    return structured_type.get_col_spec()


class StructuredTypeSubItemsTest(fixtures.TestBase):
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


class StructuredTypeColSpecTest(fixtures.TestBase):
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

    def test_struct_field_names_that_are_not_plain_are_backquoted(self):
        struct_type = datatype.STRUCT(("a b", datatype.INTEGER), ("日本", datatype.INTEGER), ("_c1", datatype.DATE))
        assert _col_spec(struct_type) == "STRUCT<`a b` INTEGER, `日本` INTEGER, _c1 DATE>"

    def test_a_backquote_in_a_struct_field_name_is_refused(self):
        with testing.expect_raises_message(sa.exc.CompileError, "drops the backquote"):
            _col_spec(datatype.STRUCT(("a`b", datatype.INTEGER)))


class StructFieldsCopyTest(fixtures.TestBase):
    """A STRUCT's fields are positional, so its copies must be built with them."""

    def test_copy_keeps_the_fields(self):
        struct_type = datatype.STRUCT(a=datatype.INTEGER, b=datatype.DATE)
        assert [name for name, _ in struct_type.copy().field_tuples] == ["a", "b"]

    def test_dialect_impl_keeps_the_fields(self):
        struct_type = datatype.STRUCT(("b", datatype.DATE), ("a", datatype.INTEGER))
        assert [name for name, _ in struct_type.dialect_impl(_DIALECT).field_tuples] == ["b", "a"]


def _process(type_obj, value):
    """Call result_processor and apply it to value."""
    proc = type_obj.result_processor(dialect=_DIALECT, coltype=None)
    if proc is None:
        return value
    return proc(value)


class ArrayResultProcessorTest(fixtures.TestBase):
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

    @testing.combinations(
        ("unknown", None, '["616263"]'),
        ("hex_4_1", (4, 1, 6), '["616263"]'),
        ("hex_4_0", (4, 0, 10), '["616263"]'),
        ("raw_4_1_0", (4, 1, 0), '["abc"]'),
        ("raw_4_0_9", (4, 0, 9), '["abc"]'),
        ("raw_3_5", (3, 5, 21), '["abc"]'),
        id_="iaa",
        argnames="version,text",
    )
    def test_nested_binary_is_read_as_the_server_writes_it(self, version, text):
        dialect = StarRocksDialect()
        dialect.server_version_info = version
        processor = datatype.ARRAY(datatype.VARBINARY()).result_processor(dialect, None)
        assert processor(text) == [b"abc"]

    def test_non_finite_doubles_are_read(self):
        """StarRocks writes a non-finite DOUBLE inside a collection as a bare inf, -inf or nan."""
        values = _process(datatype.ARRAY(datatype.DOUBLE()), "[inf,-inf,nan,1.5]")
        assert values[:2] == [math.inf, -math.inf] and math.isnan(values[2]) and values[3] == 1.5
        assert _process(datatype.MAP(datatype.VARCHAR(5), datatype.DOUBLE()), '{"k":inf}') == {"k": math.inf}

    def test_array_of_decimal_preserves_precision(self):
        result = _process(datatype.ARRAY(datatype.DECIMAL(18, 18)), '[0.123456789012345678]')
        assert isinstance(result[0], Decimal)
        assert result[0] == Decimal('0.123456789012345678')

    def test_array_of_json_reads_each_document(self):
        """StarRocks writes each element of an ARRAY<JSON> as a single-quoted JSON document, which is not JSON."""
        text = """['{"a": 1}','[2, "x\\'s"]',null]"""
        assert _process(datatype.ARRAY(datatype.JSON()), text) == [{"a": 1}, [2, "x's"], None]

    @testing.combinations(
        ("[1]", [1]),
        ("[]", []),
        ('["x","y","z"]', ["x", "y", "z"]),
        argnames="json_str,expected",
    )
    def test_parametrized_cases(self, json_str, expected):
        item_type = datatype.INTEGER if expected and isinstance(expected[0], int) else datatype.VARCHAR(10)
        assert _process(datatype.ARRAY(item_type), json_str) == expected


class MapResultProcessorTest(fixtures.TestBase):
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


class StructResultProcessorTest(fixtures.TestBase):
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


class MapNonStringKeysTest(fixtures.TestBase):
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


def _bound_document(structured_type, value):
    """Return the text the type's bind processor sends to the driver for ``value``."""
    return structured_type.bind_processor(_DIALECT)(value)


def _insert_sql(type_obj, **compile_kwargs):
    table = Table("t", MetaData(), Column("v", type_obj))
    return str(insert(table).compile(dialect=_DIALECT, **compile_kwargs))


class StructuredTypeBindProcessorTest(fixtures.TestBase):
    """A value is sent as one JSON document, which the bind expression rebuilds exactly.

    The driver can't send a list or a dict: it would write one as a SQL tuple such as ``('a', 'b')``, which
    StarRocks refuses. Elements travel as text, so no number or date is cast into NULL.
    """

    @testing.combinations(
        (datatype.ARRAY(datatype.VARCHAR(10)),),
        (datatype.MAP(datatype.VARCHAR(10), datatype.INTEGER()),),
        (datatype.STRUCT(name=datatype.VARCHAR(10)),),
        argnames="structured_type",
    )
    def test_none_is_sent_as_sql_null(self, structured_type):
        assert _bound_document(structured_type, None) is None

    def test_a_list_is_a_json_array_of_its_elements(self):
        assert _bound_document(datatype.ARRAY(datatype.VARCHAR(10)), ["a", None]) == '["a", null]'

    def test_a_nested_list_is_a_nested_json_array(self):
        assert _bound_document(datatype.ARRAY(datatype.ARRAY(datatype.INTEGER)), [[1, 2], [3]]) == '[["1", "2"], ["3"]]'

    @testing.combinations(
        (datatype.ARRAY(datatype.INTEGER()), "[1,2,3]", '["1", "2", "3"]'),
        (datatype.ARRAY(datatype.ARRAY(datatype.INTEGER())), " [[1], [], null] ", '[["1"], [], null]'),
        (datatype.ARRAY(datatype.DECIMAL(30, 20)), "[1.12345678901234567890]", '["1.12345678901234567890"]'),
        (datatype.ARRAY(datatype.VARCHAR(10)), '["a", "b\\"c"]', '["a", "b\\"c"]'),
        argnames="array_type,text,document",
    )
    def test_the_text_of_an_array_is_sent_as_its_elements(self, array_type, text, document):
        """Text was how an array was bound before lists were; it is read as JSON, keeping a decimal's digits."""
        assert _bound_document(array_type, text) == document

    @testing.combinations(
        (datatype.ARRAY(datatype.VARCHAR(10)), "1"),
        (datatype.ARRAY(datatype.VARCHAR(10)), "[a,b]"),
        (datatype.ARRAY(datatype.VARCHAR(10)), '{"a": 1}'),
        (datatype.ARRAY(datatype.VARCHAR(10)), ""),
        (datatype.ARRAY(datatype.INTEGER()), "[1.5, 2]"),
        (datatype.ARRAY(datatype.INTEGER()), '["x"]'),
        (datatype.ARRAY(datatype.ARRAY(datatype.VARCHAR(10))), "[[a]]"),
        argnames="array_type,text",
    )
    def test_other_text_is_left_to_the_servers_cast(self, array_type, text):
        """Text that is not a JSON array its elements' types take was cast by the server, and still is."""
        assert _bound_document(array_type, text) == datatype.SERVER_CAST_MARK + text

    @testing.combinations(("[a]",), ('[{"a": "x"}]',), argnames="text")
    def test_an_array_of_structs_takes_only_json_text(self, text):
        """StarRocks has no cast from text to an array of MAP or STRUCT values."""
        with testing.expect_raises((TypeError, ValueError)):
            _bound_document(datatype.ARRAY(datatype.STRUCT(a=datatype.INTEGER())), text)

    def test_an_element_given_as_text_is_refused(self):
        """Only a whole value was ever bound as text; inside a list, text is an element, not an array."""
        with testing.expect_raises_message(ValueError, "expected an ARRAY value as a list"):
            _bound_document(datatype.ARRAY(datatype.ARRAY(datatype.INTEGER())), ["[1]"])

    def test_map_encodes_keys_and_values_as_two_lists(self):
        assert _bound_document(datatype.MAP(datatype.VARCHAR(10), datatype.INTEGER), {"a": 1}) == '[["a"], ["1"]]'

    def test_struct_encodes_fields_in_declared_order(self):
        person = datatype.STRUCT(name=datatype.VARCHAR(10), age=datatype.INTEGER())
        assert _bound_document(person, {"age": 30, "name": "Alice"}) == '["Alice", "30"]'

    def test_struct_missing_field_is_null(self):
        person = datatype.STRUCT(name=datatype.VARCHAR(10), age=datatype.INTEGER())
        assert _bound_document(person, {"name": "Alice"}) == '["Alice", null]'

    def test_struct_field_names_match_case_insensitively(self):
        t = datatype.STRUCT(Name=datatype.VARCHAR(10))
        assert _bound_document(t, {"name": "Alice"}) == '["Alice"]'

    def test_struct_unknown_field_is_rejected(self):
        named = datatype.STRUCT(name=datatype.VARCHAR(10))
        with testing.expect_raises_message(ValueError, "'nmae' is not a field"):
            _bound_document(named, {"nmae": "Alice"})

    @testing.combinations(
        (datatype.ARRAY(datatype.DECIMAL(9, 3)), [Decimal("1.500"), Decimal("1E+2"), None], ["1.500", "100", None]),
        (datatype.ARRAY(datatype.DATE()), [date(2024, 2, 29)], ["2024-02-29"]),
        (
            datatype.ARRAY(datatype.DATETIME()),
            [datetime(2024, 2, 29, 10, 11, 12, 123456), datetime(2024, 1, 1)],
            ["2024-02-29 10:11:12.123456", "2024-01-01 00:00:00"],
        ),
        (datatype.ARRAY(datatype.BOOLEAN()), [True, False], ["true", "false"]),
        (datatype.ARRAY(datatype.LARGEINT()), [2**127 - 1], [str(2**127 - 1)]),
        (datatype.ARRAY(datatype.DOUBLE()), [0.1, 1e-07], ["0.1", "1e-07"]),
        (datatype.ARRAY(datatype.VARBINARY()), [b"\x00\xff"], ["00ff"]),
        (datatype.ARRAY(datatype.JSON()), [{"a": [1, None]}, 1, "1"], [{"a": [1, None]}, 1, "1"]),
        (datatype.ARRAY(datatype.VARCHAR(20)), ["b'c", 'q"r', "s\\t", "\u00e9\u4e2d"], ["b'c", 'q"r', "s\\t", "\u00e9\u4e2d"]),
        argnames="type_obj,value,encoded",
    )
    def test_leaves_are_encoded_by_declared_type(self, type_obj, value, encoded):
        assert json.loads(_bound_document(type_obj, value)) == encoded

    @testing.combinations(
        ("datetime", datatype.ARRAY(datatype.DATETIME())),
        ("date", datatype.ARRAY(sa.Date())),
        ("map_key", datatype.MAP(datatype.DATETIME(), datatype.INTEGER())),
        argnames="type_obj",
        id_="ia",
    )
    def test_aware_datetime_is_bound_as_its_wall_time(self, type_obj):
        aware = datetime(2024, 1, 1, 3, tzinfo=timezone(timedelta(hours=5)))
        value = {aware: 1} if isinstance(type_obj, datatype.MAP) else [aware]
        encoded = json.loads(_bound_document(type_obj, value))
        assert (encoded[0] if isinstance(type_obj, datatype.MAP) else encoded) == ["2024-01-01 03:00:00"]

    @testing.combinations(
        (datatype.ARRAY(datatype.DOUBLE()), [float("nan")]),
        (datatype.ARRAY(datatype.DECIMAL(9, 3)), [Decimal("Infinity")]),
        argnames="type_obj,value",
    )
    def test_non_finite_number_is_rejected(self, type_obj, value):
        with testing.expect_raises_message(ValueError, "cannot store"):
            _bound_document(type_obj, value)

    @testing.combinations(
        (datatype.ARRAY(datatype.INTEGER()), {"a": 1}),
        (datatype.MAP(datatype.INTEGER(), datatype.INTEGER()), [1, 2]),
        (datatype.STRUCT(a=datatype.INTEGER()), 5),
        argnames="type_obj,value",
    )
    def test_value_of_the_wrong_shape_is_rejected(self, type_obj, value):
        with testing.expect_raises_message(ValueError, "expected"):
            _bound_document(type_obj, value)

    @testing.combinations(
        ("list_in_integer", datatype.ARRAY(datatype.INTEGER()), [[1, 2]]),
        ("list_in_string", datatype.ARRAY(datatype.VARCHAR(10)), [["a"]]),
        ("tuple_in_string", datatype.ARRAY(datatype.VARCHAR(10)), [("a",)]),
        ("dict_in_string", datatype.ARRAY(datatype.VARCHAR(10)), [{"a": 1}]),
        ("list_in_struct_field", datatype.STRUCT(a=datatype.INTEGER()), {"a": [1]}),
        ("dict_in_map_value", datatype.MAP(datatype.VARCHAR(10), datatype.DATE()), {"k": {"x": 1}}),
        id_="iaa",
        argnames="type_obj,value",
    )
    def test_a_collection_in_a_scalar_element_is_refused(self, type_obj, value):
        """A collection in a scalar element would be stored as its repr, or as NULL."""
        with testing.expect_raises_message(TypeError, "cannot store the"):
            _bound_document(type_obj, value)

    @testing.combinations(
        ("array", lambda: datatype.ARRAY(sa.ARRAY(sa.Integer)).item_type, "ARRAY(Integer())"),
        ("map", lambda: datatype.MAP(sa.String(5), sa.ARRAY(sa.Date, dimensions=2)).value_type, "ARRAY(ARRAY(Date()))"),
        ("struct", lambda: datatype.STRUCT(a=sa.ARRAY(sa.Integer)).field_tuples[0][1], "ARRAY(Integer())"),
        id_="iaa",
        argnames="element_type_factory,expected",
    )
    def test_a_generic_array_element_is_this_dialects_array(self, element_type_factory, expected):
        """A generic ARRAY element is otherwise cast from its JSON text, which stores NULLs."""
        element_type = element_type_factory()
        assert type(element_type) is datatype.ARRAY and repr(element_type) == expected

    def test_generic_array_dimensions_nest(self):
        """The generic ARRAY declares an array of arrays by ``dimensions``; a copy of the adapted type keeps the depth."""
        adapted = sa.ARRAY(sa.Integer, dimensions=3).dialect_impl(_DIALECT)
        assert repr(adapted) == "ARRAY(ARRAY(ARRAY(Integer())))"
        assert repr(adapted.copy()) == repr(adapted)
        assert _bound_document(adapted, [[[1]], None]) == '[[["1"]], null]'


class _Reversed(TypeDecorator):
    impl = datatype.VARCHAR(10)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return value[::-1]


class _Cents(TypeDecorator):
    impl = datatype.INTEGER()
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return round(value * 100)


class _Wrapped(TypeDecorator):
    """A JSON decorator that stores a value inside an object, and reads it back out."""

    impl = datatype.JSON()
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return {"v": value}

    def process_result_value(self, value, dialect):
        return value["v"]


class _NoneToEmpty(TypeDecorator):
    impl = datatype.VARCHAR(10)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return "" if value is None else value


class _Color(enum.Enum):
    RED = 1
    GREEN = 2


class _Bytes(TypeDecorator):
    impl = datatype.VARBINARY()
    cache_ok = True


class _EmptyForNone(TypeDecorator):
    impl = datatype.VARCHAR(10)
    cache_ok = True

    def process_result_value(self, value, dialect):
        return "" if value is None else value


def _read(type_obj, text, version=(4, 1, 6)):
    dialect = StarRocksDialect()
    dialect.server_version_info = version
    return type_obj.result_processor(dialect, None)(text)


class ElementResultProcessingTest(fixtures.TestBase):
    """An element is read through its declared type's result processing, as a plain column value is."""

    @testing.combinations(
        ("enum", datatype.ARRAY(Enum(_Color)), '["GREEN","RED"]', [_Color.GREEN, _Color.RED]),
        ("enum_in_struct", datatype.STRUCT(c=Enum(_Color)), '{"c":"RED"}', {"c": _Color.RED}),
        ("enum_in_map", datatype.MAP(Enum(_Color), datatype.INTEGER()), '{"GREEN":1}', {_Color.GREEN: 1}),
        ("binary_decorator", datatype.ARRAY(_Bytes()), '["616263"]', [b"abc"]),
        ("pickle", datatype.ARRAY(PickleType()), '["%s"]' % pickle.dumps({"x": 1}).hex(), [{"x": 1}]),
        ("decorator_reads_none", datatype.ARRAY(_EmptyForNone()), '[null,"x"]', ["", "x"]),
        id_="iaaa",
        argnames="type_obj,text,expected",
    )
    def test_read(self, type_obj, text, expected):
        assert _read(type_obj, text) == expected

    def test_binary_decorator_on_a_server_that_nests_raw_bytes(self):
        assert _read(datatype.ARRAY(_Bytes()), '["abc"]', version=(3, 5, 21)) == [b"abc"]

    def test_float_with_asdecimal_reads_a_decimal(self):
        value = _read(datatype.ARRAY(Float(asdecimal=True)), "[0.5]")
        assert value == [Decimal("0.5")] and isinstance(value[0], Decimal)


def _select_sql(type_obj, version):
    dialect = StarRocksDialect()
    dialect.server_version_info = version
    table = Table("t", MetaData(), Column("v", type_obj))
    return str(sa.select(table.c.v).compile(dialect=dialect)).replace("\n", "")


class BinaryElementsSelectedAsHexTest(fixtures.TestBase):
    """A server that writes nested VARBINARY as its bytes is asked for hex, inside a STRUCT that marks it."""

    @testing.combinations(
        (
            "array",
            datatype.ARRAY(datatype.VARBINARY()),
            "SELECT named_struct('hex', array_map(h1 -> hex(h1), t.v)) AS v FROM t",
        ),
        (
            "nested_array",
            datatype.ARRAY(datatype.ARRAY(datatype.VARBINARY())),
            "SELECT named_struct('hex', array_map(h1 -> array_map(h2 -> hex(h2), h1), t.v)) AS v FROM t",
        ),
        (
            "generic_array",
            sa.ARRAY(sa.LargeBinary()),
            "SELECT named_struct('hex', array_map(h1 -> hex(h1), t.v)) AS v FROM t",
        ),
        ("pickle", datatype.ARRAY(PickleType()), "SELECT named_struct('hex', array_map(h1 -> hex(h1), t.v)) AS v FROM t"),
        ("decorator", datatype.ARRAY(_Bytes()), "SELECT named_struct('hex', array_map(h1 -> hex(h1), t.v)) AS v FROM t"),
        ("strings", datatype.ARRAY(datatype.VARCHAR(5)), "SELECT t.v FROM t"),
        ("array_of_struct", datatype.ARRAY(datatype.STRUCT(b=datatype.VARBINARY())), "SELECT t.v FROM t"),
        ("map", datatype.MAP(datatype.VARCHAR(5), datatype.VARBINARY()), "SELECT t.v FROM t"),
        id_="iaa",
        argnames="type_obj,expected",
    )
    def test_select_on_a_server_that_writes_raw_bytes(self, type_obj, expected):
        assert _select_sql(type_obj, (3, 5, 21)) == expected

    @testing.combinations(("unknown", None), ("hex_4_1", (4, 1, 6)), ("hex_4_0", (4, 0, 10)), id_="ia", argnames="version")
    def test_select_on_a_server_that_writes_hex(self, version):
        assert _select_sql(datatype.ARRAY(datatype.VARBINARY()), version) == "SELECT t.v AS v FROM t"

    @testing.combinations(
        ("array", datatype.ARRAY(datatype.VARBINARY()), '{"hex":["616263","FFFE",null]}', [b"abc", b"\xff\xfe", None]),
        ("nested", datatype.ARRAY(datatype.ARRAY(datatype.VARBINARY())), '{"hex":[["80"],null]}', [[b"\x80"], None]),
        ("null", datatype.ARRAY(datatype.VARBINARY()), '{"hex":null}', None),
        ("raw_without_the_expression", datatype.ARRAY(datatype.VARBINARY()), '["616263"]', [b"616263"]),
        id_="iaaa",
        argnames="type_obj,text,expected",
    )
    def test_read_on_a_server_that_writes_raw_bytes(self, type_obj, text, expected):
        assert _read(type_obj, text, version=(3, 5, 21)) == expected

    def test_a_struct_with_a_field_named_hex_is_read_as_itself(self):
        assert _read(datatype.STRUCT(hex=datatype.VARCHAR(5)), '{"hex":"abc"}', version=(3, 5, 21)) == {"hex": "abc"}


class _Float(float):
    """A float subclass whose repr is not a float literal, as numpy's float64 is under numpy 2."""

    def __repr__(self):
        return f"_Float({float(self)!r})"


class StructuredLeafBindProcessingTest(fixtures.TestBase):
    """An element goes through its type's own bind processing, as a plain column value does."""

    @testing.combinations(
        ("whole_float", datatype.ARRAY(datatype.INTEGER()), [2.0], ["2"]),
        ("whole_decimal", datatype.ARRAY(datatype.BIGINT()), [Decimal("2.0"), Decimal("2E+1")], ["2", "20"]),
        ("float_subclass", datatype.ARRAY(datatype.DOUBLE()), [_Float(0.5)], ["0.5"]),
        ("type_decorator", datatype.ARRAY(_Reversed()), ["abc"], ["cba"]),
        ("type_decorator_number", datatype.STRUCT(m=_Cents()), {"m": 5.5}, ["550"]),
        ("enum", datatype.ARRAY(Enum(_Color)), [_Color.GREEN], ["GREEN"]),
        ("json_decorator", datatype.ARRAY(_Wrapped()), [{"a": 1}, None], [{"v": {"a": 1}}, {"v": None}]),
        ("json_decorator_in_map", datatype.MAP(datatype.VARCHAR(5), _Wrapped()), {"k": 1}, [["k"], [{"v": 1}]]),
        ("none_through_decorator", datatype.ARRAY(_NoneToEmpty()), [None, "x"], ["", "x"]),
        ("none_through_struct_decorator", datatype.STRUCT(s=_NoneToEmpty()), {"s": None}, [""]),
        id_="iaaa",
        argnames="type_obj,value,encoded",
    )
    def test_encoded(self, type_obj, value, encoded):
        assert json.loads(_bound_document(type_obj, value)) == encoded

    def test_a_json_element_uses_the_engine_json_serializer(self):
        """A plain JSON column serialises with the engine's ``json_serializer``; so does a JSON element."""
        dialect = StarRocksDialect(json_serializer=lambda value: json.dumps(value, default=str))
        process = datatype.ARRAY(datatype.JSON()).bind_processor(dialect)
        assert json.loads(process([{"day": date(2024, 1, 2)}])) == [{"day": "2024-01-02"}]

    def test_a_fraction_in_an_integer_element_is_refused(self):
        with testing.expect_raises_message(ValueError, "cannot store 1.7 in an integer element exactly"):
            _bound_document(datatype.ARRAY(datatype.INTEGER()), [1.7])

    @testing.combinations(
        ("integer", datatype.ARRAY(datatype.INTEGER()), ["7", " -3 ", "2.0", "1E+2"], ["7", "-3", "2", "100"]),
        ("decimal", datatype.ARRAY(datatype.DECIMAL(10, 2)), ["1.25", "1e-2"], ["1.25", "0.01"]),
        ("double", datatype.STRUCT(d=datatype.DOUBLE()), {"d": "1e-07"}, ["0.0000001"]),
        id_="iaaa",
        argnames="type_obj,value,encoded",
    )
    def test_numeric_text_is_encoded_as_its_number(self, type_obj, value, encoded):
        assert json.loads(_bound_document(type_obj, value)) == encoded

    @testing.combinations(
        ("word", datatype.INTEGER(), "abc", ValueError, "it is not a number"),
        ("trailing_word", datatype.INTEGER(), "7 apples", ValueError, "it is not a number"),
        ("fraction_text", datatype.INTEGER(), "2.5", ValueError, "in an integer element exactly"),
        ("decimal_comma", datatype.DECIMAL(10, 2), "1,5", ValueError, "it is not a number"),
        ("double_suffix", datatype.DOUBLE(), "1.5x", ValueError, "it is not a number"),
        ("nan_text", datatype.DOUBLE(), "nan", ValueError, "cannot store"),
        ("date", datatype.INTEGER(), date(2024, 1, 1), TypeError, "cannot store the date"),
        ("bytes", datatype.DECIMAL(10, 2), b"1", TypeError, "cannot store the bytes"),
        id_="iaaaa",
        argnames="element_type,value,error,message",
    )
    def test_a_numeric_element_refuses_what_is_not_a_number(self, element_type, value, error, message):
        """The element's CAST would turn the text into NULL, where a plain column of the type refuses it."""
        with testing.expect_raises_message(error, message):
            _bound_document(datatype.ARRAY(element_type), [value])

    @testing.combinations(("false",), ("0",), (2,), argnames="value")
    def test_a_boolean_element_is_as_strict_as_a_boolean_column(self, value):
        with testing.expect_raises((TypeError, ValueError)):
            _bound_document(datatype.ARRAY(datatype.BOOLEAN()), [value])


class StructuredTypeBindExpressionTest(fixtures.TestBase, AssertsCompiledSQL):
    """The bind parameter must become the declared type through casts the server evaluates correctly.

    The server's text-to-ARRAY cast is not a JSON parser, and its JSON casts to ARRAY<DECIMAL/DATE/DATETIME>,
    MAP and STRUCT are refused or depend on the version, so every element is cast on its own from the parsed
    document.
    """

    __dialect__ = _DIALECT

    @staticmethod
    def _array_from_bound_text(array_sql, rebuilt_sql):
        """The INSERT of an ARRAY, which rebuilds a bound JSON document and has the server cast marked text."""
        return f"INSERT INTO t (v) VALUES (IF(left(%s, 1) = '~', CAST(substr(%s, 2) AS {array_sql}), {rebuilt_sql}))"

    def test_array_casts_each_element(self):
        assert _insert_sql(datatype.ARRAY(datatype.VARCHAR(50))) == self._array_from_bound_text(
            "ARRAY<VARCHAR(50)>", "array_map(e1 -> CAST(e1 AS VARCHAR), CAST(parse_json(%s) AS ARRAY<JSON>))"
        )

    def test_array_of_decimal_casts_through_text(self):
        assert _insert_sql(datatype.ARRAY(datatype.DECIMAL(10, 2))) == self._array_from_bound_text(
            "ARRAY<DECIMAL(10, 2)>",
            "array_map(e1 -> CAST(CAST(e1 AS VARCHAR) AS DECIMAL(10, 2)), CAST(parse_json(%s) AS ARRAY<JSON>))",
        )

    def test_array_of_json_keeps_the_parsed_elements(self):
        assert _insert_sql(datatype.ARRAY(datatype.JSON())) == self._array_from_bound_text(
            "ARRAY<JSON>", "CAST(parse_json(%s) AS ARRAY<JSON>)"
        )

    def test_array_of_binary_decodes_hex(self):
        assert _insert_sql(datatype.ARRAY(datatype.VARBINARY())) == self._array_from_bound_text(
            "ARRAY<VARBINARY>",
            "array_map(e1 -> to_binary(CAST(e1 AS VARCHAR), 'hex'), CAST(parse_json(%s) AS ARRAY<JSON>))",
        )

    def test_array_of_maps_rebuilds_its_document_only(self):
        """StarRocks has no cast from text to an array of MAP values, so none is rendered."""
        assert "substr(" not in _insert_sql(datatype.ARRAY(datatype.MAP(datatype.INTEGER(), datatype.INTEGER())))

    def test_map_builds_from_key_and_value_arrays(self):
        assert _insert_sql(datatype.MAP(datatype.INTEGER(), datatype.DECIMAL(10, 2))) == (
            "INSERT INTO t (v) VALUES (element_at(array_map(e1 -> map_from_arrays("
            "array_map(e3 -> CAST(CAST(e3 AS VARCHAR) AS INTEGER), CAST(CAST(e1 AS ARRAY<JSON>)[1] AS ARRAY<JSON>)), "
            "array_map(e4 -> CAST(CAST(e4 AS VARCHAR) AS DECIMAL(10, 2)), CAST(CAST(e1 AS ARRAY<JSON>)[2] AS ARRAY<JSON>))), "
            "array_filter(e2 -> e2 IS NOT NULL, [parse_json(%s)])), 1))"
        )

    def test_struct_builds_with_named_struct_and_null_stays_null(self):
        assert _insert_sql(datatype.STRUCT(name=datatype.VARCHAR(10), age=datatype.INTEGER())) == (
            "INSERT INTO t (v) VALUES (element_at(array_map(e1 -> named_struct("
            "'name', CAST(CAST(e1 AS ARRAY<JSON>)[1] AS VARCHAR), "
            "'age', CAST(CAST(CAST(e1 AS ARRAY<JSON>)[2] AS VARCHAR) AS INTEGER)), "
            "array_filter(e2 -> e2 IS NOT NULL, [parse_json(%s)])), 1))"
        )

    def test_struct_field_name_is_an_escaped_literal(self):
        sql = _insert_sql(datatype.STRUCT(**{"it's": datatype.INTEGER()}))
        assert "named_struct('it''s', " in sql

    def test_array_of_struct_nests_lambdas_without_reusing_variables(self):
        sql = _insert_sql(datatype.ARRAY(datatype.STRUCT(a=datatype.INTEGER(), b=datatype.ARRAY(datatype.DATE()))))
        assert sql == (
            "INSERT INTO t (v) VALUES (array_map(e1 -> element_at(array_map(e2 -> named_struct("
            "'a', CAST(CAST(CAST(e2 AS ARRAY<JSON>)[1] AS VARCHAR) AS INTEGER), "
            "'b', array_map(e4 -> CAST(CAST(e4 AS VARCHAR) AS DATE), CAST(CAST(e2 AS ARRAY<JSON>)[2] AS ARRAY<JSON>))), "
            "array_filter(e3 -> e3 IS NOT NULL, [e1])), 1), CAST(parse_json(%s) AS ARRAY<JSON>)))"
        )

    @testing.combinations(
        (datatype.ARRAY(datatype.VARCHAR(5)), ["it's", "100%", "back\\slash"]),
        (datatype.MAP(datatype.VARCHAR(5), datatype.INTEGER()), {"it's": 1}),
        (datatype.STRUCT(a=datatype.VARCHAR(5)), {"a": "it's"}),
        argnames="type_obj,value",
    )
    def test_literal_binds_renders_the_escaped_document(self, type_obj, value):
        table = Table("t", MetaData(), Column("v", type_obj))
        sql = str(insert(table).values(v=value).compile(dialect=_DIALECT, compile_kwargs={"literal_binds": True}))
        document = json.dumps(datatype.encode_structured_value(type_obj, value, _DIALECT))
        escaped = document.replace("\\", "\\\\").replace("'", "''").replace("%", "%%")
        assert "parse_json('%s')" % escaped in sql
        assert "%s" not in sql.replace("%%", "")

    def test_literal_binds_renders_null(self):
        table = Table("t", MetaData(), Column("v", datatype.ARRAY(datatype.INTEGER())))
        sql = str(insert(table).values(v=None).compile(dialect=_DIALECT, compile_kwargs={"literal_binds": True}))
        assert "CAST(parse_json(NULL) AS ARRAY<JSON>)" in sql

    @testing.combinations(
        (datatype.ARRAY(datatype.INTEGER()), "ARRAY<INTEGER>"),
        (datatype.MAP(datatype.VARCHAR(10), datatype.INTEGER()), "MAP<VARCHAR(10), INTEGER>"),
        (datatype.STRUCT(a=datatype.INTEGER()), "STRUCT<a INTEGER>"),
        argnames="structured_type,type_sql",
    )
    def test_a_cast_to_a_structured_type_names_it(self, structured_type, type_sql):
        """MySQL's compiler has no CAST to these types, and would drop the CAST with a warning."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            self.assert_compile(sa.cast(sa.column("x", structured_type), structured_type), f"CAST(x AS {type_sql})")
