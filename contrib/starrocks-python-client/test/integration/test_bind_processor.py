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

"""Python values bound to ARRAY, MAP and STRUCT columns are stored as those values.

The driver cannot send a list or a dict, so each value is bound as one JSON document and rebuilt by the server.
"""
from datetime import date, datetime
from decimal import Decimal
import enum

import sqlalchemy as sa
from sqlalchemy import Column, Enum, Integer, PickleType, Table, TypeDecorator, select, testing, type_coerce
from sqlalchemy.testing import eq_, fixtures

from starrocks.datatype import (
    ARRAY,
    BOOLEAN,
    DATE,
    DATETIME,
    DECIMAL,
    DOUBLE,
    INTEGER,
    JSON,
    LARGEINT,
    MAP,
    STRUCT,
    VARBINARY,
    VARCHAR,
)


_LISTS_AND_DICTS = {
    1: {
        "numbers": [1, 2, 3],
        "words": ["a", "b"],
        "grid": [[1, 2], [3]],
        "counts": {"a": 1, "b": 2},
        "groups": {"x": [1, 2], "y": [3]},
        "person": {"name": "Alice", "age": 30},
        "scored": {"tags": ["go", "python"], "score": 9},
    },
    2: {
        "numbers": [9],
        "words": ["x"],
        "grid": [[5]],
        "counts": {"z": 99},
        "groups": {"m": []},
        "person": {"name": "Bob", "age": None},
        "scored": {"tags": [], "score": 0},
    },
    3: dict.fromkeys(("numbers", "words", "grid", "counts", "groups", "person", "scored")),
}


class ListAndDictBindTest(fixtures.TablesTest):
    """A list binds to an ARRAY column, and a dict to a MAP or a STRUCT one, through an ``insert()`` of the rows.

    The rows hold one-element, nested and empty collections and NULLs, and are inserted once, as an executemany.
    """

    __backend__ = True
    __only_on__ = "starrocks"

    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "bound_lists_and_dicts",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("numbers", ARRAY(INTEGER())),
            Column("words", ARRAY(VARCHAR(50))),
            Column("grid", ARRAY(ARRAY(INTEGER()))),
            Column("counts", MAP(VARCHAR(20), INTEGER())),
            Column("groups", MAP(VARCHAR(20), ARRAY(INTEGER()))),
            Column("person", STRUCT(name=VARCHAR(50), age=INTEGER())),
            Column("scored", STRUCT(tags=ARRAY(VARCHAR(20)), score=INTEGER())),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )

    @classmethod
    def insert_data(cls, connection):
        rows = [{"id": row_id, **values} for row_id, values in _LISTS_AND_DICTS.items()]
        connection.execute(cls.tables.bound_lists_and_dicts.insert(), rows)

    @testing.combinations(
        ("numbers",), ("words",), ("grid",), ("counts",), ("groups",), ("person",), ("scored",), argnames="column"
    )
    def test_each_row_reads_back_as_bound(self, connection, column):
        table = self.tables.bound_lists_and_dicts
        stored = dict(connection.execute(select(table.c.id, table.c[column])).all())
        eq_(stored, {row_id: values[column] for row_id, values in _LISTS_AND_DICTS.items()})


_BIG = 170141183460469231731687303715884105727


class _Color(enum.Enum):
    RED = 1
    GREEN = 2


class _Bytes(TypeDecorator):
    impl = VARBINARY()
    cache_ok = True


class ComplexValueRoundTripTest(fixtures.TestBase):
    """Every value bound to an ARRAY, MAP or STRUCT column must come back exactly, whatever its element types.

    The server's text-to-ARRAY cast mis-splits a string holding a quote and turns quoted decimals and dates
    into NULL, and its JSON casts to MAP and STRUCT are refused or missing on older versions, so each value
    is rebuilt element by element on the server. Each case is bound once through ``insert().values`` and
    twice through ``executemany``.
    """

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.combinations(
        (
            "array_of_awkward_strings",
            ARRAY(VARCHAR(50)),
            ["a", "b'c", None, 'q"r', "back\\slash", "uni \u00e9\u4e2d \U0001f600", "null", "", "100%", "[x]"],
        ),
        ("array_of_decimal", ARRAY(DECIMAL(10, 2)), [Decimal("1.25"), None, Decimal("-0.01")]),
        ("array_of_date", ARRAY(DATE()), [date(2024, 2, 29), None]),
        (
            "array_of_datetime",
            ARRAY(DATETIME()),
            [datetime(2024, 2, 29, 10, 11, 12, 123456), datetime(2024, 1, 1)],
        ),
        ("array_of_boolean", ARRAY(BOOLEAN()), [True, False, None]),
        ("array_of_json", ARRAY(JSON()), [{"a": [1, None]}, 1, "1", None, [2]]),
        ("array_of_largeint", ARRAY(LARGEINT()), [_BIG, -_BIG - 1, 0]),
        ("array_of_double", ARRAY(DOUBLE()), [0.1, 1e-07, -3.5e20]),
        ("empty_array", ARRAY(INTEGER()), []),
        ("array_of_nulls", ARRAY(INTEGER()), [None, None]),
        ("array_of_array_of_decimal", ARRAY(ARRAY(DECIMAL(10, 2))), [[Decimal("1.10")], [], None]),
        ("map_of_decimal", MAP(INTEGER(), DECIMAL(10, 2)), {1: Decimal("1.25"), 2: None}),
        ("map_of_array_of_date", MAP(VARCHAR(20), ARRAY(DATE())), {"k'1": [date(2024, 1, 2)], "k2": []}),
        ("empty_map", MAP(VARCHAR(20), INTEGER()), {}),
        (
            "struct_with_nested_struct_and_array",
            STRUCT(a=INTEGER(), b=STRUCT(c=VARCHAR(10), d=ARRAY(DATE()))),
            {"a": 1, "b": {"c": "x'y", "d": [date(2024, 1, 2), None]}},
        ),
        ("struct_of_nulls", STRUCT(a=INTEGER(), b=VARCHAR(10)), {"a": None, "b": None}),
        (
            "array_of_struct",
            ARRAY(STRUCT(a=INTEGER(), s=VARCHAR(10))),
            [{"a": 1, "s": "x"}, None, {"a": 2, "s": None}],
        ),
        (
            "map_of_struct_with_map",
            MAP(VARCHAR(10), STRUCT(a=INTEGER(), m=MAP(INTEGER(), VARCHAR(5)))),
            {"q": {"a": 1, "m": {1: "z"}}},
        ),
        ("array_of_enum", ARRAY(Enum(_Color)), [_Color.GREEN, None, _Color.RED]),
        ("struct_of_enum", STRUCT(c=Enum(_Color)), {"c": _Color.RED}),
        ("map_of_enum", MAP(Enum(_Color), Enum(_Color)), {_Color.GREEN: _Color.RED}),
        ("array_of_binary_decorator", ARRAY(_Bytes()), [b"abc", None]),
        ("struct_of_binary_decorator", STRUCT(b=_Bytes()), {"b": b"abc"}),
        ("array_of_pickle", ARRAY(PickleType(protocol=0)), [{"x": [1, 2]}, None]),
        ("array_of_binary_pickle", ARRAY(PickleType()), [{"x": [1, 2]}]),
        ("array_of_non_utf8_binary", ARRAY(VARBINARY()), [b"\xff\xfe", None, b"abc"]),
        ("map_of_generic_array", MAP(VARCHAR(5), sa.ARRAY(sa.String(10))), {"k": ["a,b", 'c"d', None]}),
        ("struct_of_generic_array", STRUCT(a=sa.ARRAY(sa.Numeric(10, 2))), {"a": [Decimal("1.25"), None]}),
        ("array_of_generic_array", ARRAY(sa.ARRAY(sa.Date, dimensions=2)), [[[date(2024, 1, 2)]], None]),
        ("array_of_array_of_non_utf8_binary", ARRAY(ARRAY(VARBINARY())), [[b"\x80", None], None, []]),
        ("map_of_pickle", MAP(VARCHAR(5), PickleType(protocol=0)), {"k": ("t", 1)}),
        argnames="type_,value",
        id_="iaa",
    )
    def test_value_round_trips(self, connection, metadata, type_, value):
        table = Table(
            "test_bind_round_trip",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("v", type_),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        table.create(connection)
        connection.execute(table.insert().values(id=1, v=value))
        connection.execute(table.insert(), [{"id": 2, "v": value}, {"id": 3, "v": value}])
        connection.execute(table.insert().values(id=4, v=None))

        rows = connection.execute(select(table.c.v).order_by(table.c.id)).scalars().all()

        assert rows == [value, value, value, None]

    @testing.combinations(
        ("integer", ARRAY(INTEGER()), ["2.0", "-3", "1E+2"], [2, -3, 100]),
        ("decimal", ARRAY(DECIMAL(10, 2)), ["1.25", " 7 "], [Decimal("1.25"), Decimal("7.00")]),
        ("double", MAP(VARCHAR(5), DOUBLE()), {"k": "1e-07"}, {"k": 1e-07}),
        argnames="type_,value,stored",
        id_="iaaa",
    )
    def test_numeric_text_is_stored_as_its_number(self, connection, metadata, type_, value, stored):
        """A number given as text is cast from its exact digits, not turned into NULL."""
        table = Table(
            "test_bind_numeric_text",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("v", type_),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        table.create(connection)
        connection.execute(table.insert().values(id=1, v=value))

        assert connection.execute(select(table.c.v)).scalar_one() == stored

    @testing.combinations(
        ("integers", ARRAY(INTEGER()), "[1,2,3]", [1, 2, 3]),
        ("spaced_with_null", ARRAY(INTEGER()), " [1, 2, null] ", [1, 2, None]),
        ("strings", ARRAY(VARCHAR(20)), '["a", "b\\"c", "it\'s 100%"]', ["a", 'b"c', "it's 100%"]),
        ("empty", ARRAY(INTEGER()), "[]", []),
        ("nested", ARRAY(ARRAY(INTEGER())), "[[1,2],[3]]", [[1, 2], [3]]),
        ("dates", ARRAY(DATE()), '["2024-01-02"]', [date(2024, 1, 2)]),
        ("exact_decimal", ARRAY(DECIMAL(30, 20)), "[1.12345678901234567890]", [Decimal("1.12345678901234567890")]),
        ("booleans", ARRAY(BOOLEAN()), "[true, false]", [True, False]),
        ("unquoted_words", ARRAY(VARCHAR(20)), "[a,b]", ["a", "b"]),
        ("nested_unquoted_words", ARRAY(ARRAY(VARCHAR(20))), "[[a,b],[c]]", [["a", "b"], ["c"]]),
        ("fraction_in_an_integer", ARRAY(INTEGER()), "[1.5, 2]", [None, 2]),
        ("not_an_array", ARRAY(INTEGER()), "abc", None),
        argnames="type_,text,stored",
        id_="iaaa",
    )
    def test_array_text_is_stored_as_its_array(self, connection, metadata, type_, text, stored):
        """The text of an array binds to an ARRAY column, through ``values()``, executemany and multi-row values.

        JSON array text is read exactly. Other text goes to StarRocks' own cast, as it did before lists were bound:
        unquoted words are strings, an element the type can't hold is NULL, and text that is not an array is NULL.
        """
        table = Table(
            "test_bind_array_text",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("v", type_),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        table.create(connection)
        connection.execute(table.insert().values(id=1, v=text))
        connection.execute(table.insert(), [{"id": 2, "v": text}, {"id": 3, "v": text}])
        connection.execute(table.insert().values([{"id": 4, "v": text}, {"id": 5, "v": text}]))

        eq_(connection.execute(select(table.c.v).order_by(table.c.id)).scalars().all(), [stored] * 5)

    def test_literal_binds_value_round_trips(self, connection, metadata):
        type_ = ARRAY(STRUCT(s=VARCHAR(20), d=ARRAY(DECIMAL(10, 2))))
        value = [{"s": "it's 100%", "d": [Decimal("1.25"), None]}, None]
        table = Table(
            "test_bind_literal",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("v", type_),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        table.create(connection)
        statement = table.insert().values(id=1, v=value)
        connection.exec_driver_sql(str(statement.compile(connection, compile_kwargs={"literal_binds": True})))

        assert connection.execute(select(table.c.v)).scalar_one() == value


class _Wrapped(TypeDecorator):
    """A JSON decorator that stores a value inside an object, and reads it back out."""

    impl = JSON()
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return {"v": value}

    def process_result_value(self, value, dialect):
        return value["v"]


class _NoneToEmpty(TypeDecorator):
    impl = VARCHAR(10)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return "" if value is None else value


class _IntegerList(TypeDecorator):
    impl = ARRAY(INTEGER())
    cache_ok = True


class _Point(TypeDecorator):
    impl = STRUCT(x=INTEGER(), y=VARCHAR(5))
    cache_ok = True


class _Doubled(TypeDecorator):
    """An ARRAY decorator that doubles each number it stores."""

    impl = ARRAY(INTEGER())
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return None if value is None else [None if item is None else item * 2 for item in value]


class _PlusOneThenDoubled(TypeDecorator):
    """A decorator over :class:`_Doubled`, which adds one to each number first."""

    impl = _Doubled()
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return None if value is None else [item + 1 for item in value]


_POINTS = [{"x": 1, "y": "a"}, None]


class TypeDecoratorElementTest(fixtures.TestBase):
    """An element of a decorated type is written and read through the decorator, as a plain column is.

    Each value is read back twice: through the decorated type, and through the same type without the decorator,
    which shows what was stored. A decorator over an ARRAY, MAP or STRUCT is that collection inside another one.
    """

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.combinations(
        ("array", ARRAY(_Wrapped()), ARRAY(JSON()), [{"a": 1}, [2]], [{"v": {"a": 1}}, {"v": [2]}], None),
        ("struct", STRUCT(w=_Wrapped()), STRUCT(w=JSON()), {"w": {"a": 1}}, {"w": {"v": {"a": 1}}}, None),
        ("map", MAP(VARCHAR(5), _Wrapped()), MAP(VARCHAR(5), JSON()), {"k": "x"}, {"k": {"v": "x"}}, None),
        ("none_to_empty", ARRAY(_NoneToEmpty()), ARRAY(VARCHAR(10)), [None, "x"], ["", "x"], ["", "x"]),
        (
            "map_of_decorated_arrays",
            MAP(VARCHAR(5), _IntegerList()),
            MAP(VARCHAR(5), ARRAY(INTEGER())),
            {"k": [1, 2], "q": [3, None]},
            {"k": [1, 2], "q": [3, None]},
            None,
        ),
        ("array_of_decorated_structs", ARRAY(_Point()), ARRAY(STRUCT(x=INTEGER(), y=VARCHAR(5))), _POINTS, _POINTS, None),
        ("array_of_decorated_arrays", ARRAY(_Doubled()), ARRAY(ARRAY(INTEGER())), [[1, 2], None], [[2, 4], None], [[2, 4], None]),
        (
            "map_of_nested_decorators",
            MAP(VARCHAR(5), _PlusOneThenDoubled()),
            MAP(VARCHAR(5), ARRAY(INTEGER())),
            {"a": [1, 2]},
            {"a": [4, 6]},
            {"a": [4, 6]},
        ),
        argnames="type_,plain_type,value,stored,read",
        id_="iaaaaa",
    )
    def test_value_round_trips_through_the_decorator(self, connection, metadata, type_, plain_type, value, stored, read):
        table = Table(
            "test_bind_decorated_element",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("v", type_),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"replication_num": "1"},
        )
        table.create(connection)
        connection.execute(table.insert().values(id=1, v=value))

        eq_(connection.scalar(select(type_coerce(table.c.v, plain_type))), stored)
        eq_(connection.scalar(select(table.c.v)), value if read is None else read)
