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

"""JSON index and path access: the SQL it compiles to, and what the server returns for it."""

import decimal

from sqlalchemy import (
    JSON as GenericJSON,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    cast,
    exc,
    insert,
    null,
    select,
    testing,
)
from sqlalchemy.testing import eq_, fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL

from starrocks.datatype import JSON, JSONIndexType, JSONPathType
from starrocks.dialect import StarRocksDialect


METADATA = MetaData()
DOCUMENTS = Table("documents", METADATA, Column("id", Integer), Column("data", JSON))
DATA = DOCUMENTS.c.data


class JsonAccessCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()

    @testing.combinations(
        ("index", lambda: DATA["k"], "json_query(documents.data, %s)"),
        ("path", lambda: DATA[("a", "b")], "json_query(documents.data, %s)"),
        ("as_json", lambda: DATA["k"].as_json(), "json_query(documents.data, %s)"),
        ("as_integer", lambda: DATA["k"].as_integer(), "get_json_int(documents.data, %s)"),
        ("as_string", lambda: DATA["k"].as_string(), "get_json_string(documents.data, %s)"),
        ("as_float", lambda: DATA["k"].as_float(), "get_json_double(documents.data, %s)"),
        ("as_boolean", lambda: DATA["k"].as_boolean(), "get_json_bool(documents.data, %s)"),
        (
            "as_numeric_unscaled",
            lambda: DATA["k"].as_numeric(12, 0),
            "COALESCE(CAST(get_json_string(documents.data, %s) AS DECIMAL(12, 0)), "
            "CAST(CAST(json_query(documents.data, %s) AS DOUBLE) AS DECIMAL(12, 0)))",
        ),
        (
            "as_numeric",
            lambda: DATA["k"].as_numeric(10, 2),
            "COALESCE(CAST(get_json_string(documents.data, %s) AS DECIMAL(10, 2)), "
            "CAST(CAST(json_query(documents.data, %s) AS DOUBLE) AS DECIMAL(10, 2)))",
        ),
        ("path_as_integer", lambda: DATA[("a", 1)].as_integer(), "get_json_int(documents.data, %s)"),
        ("chained", lambda: DATA["a"][1], "json_query(documents.data, concat(%s, substring(%s, 2)))"),
        (
            "chained_three",
            lambda: DATA["a"]["b"]["c"].as_string(),
            "get_json_string(documents.data, concat(%s, substring(%s, 2), substring(%s, 2)))",
        ),
        ("path_then_index", lambda: DATA[("a", 2)][1], "json_query(documents.data, concat(%s, substring(%s, 2)))"),
        ("index_then_path", lambda: DATA["a"][(2, 1)], "json_query(documents.data, concat(%s, substring(%s, 2)))"),
        id_="iaa",
        argnames="expression_factory, expected",
    )
    def test_access_renders_starrocks_function(self, expression_factory, expected):
        self.assert_compile(select(expression_factory()), f"SELECT {expected} AS anon_1 FROM documents")

    def test_comparison_in_where(self):
        self.assert_compile(
            select(DOCUMENTS.c.id).where(DATA["k"].as_integer() == 5),
            "SELECT documents.id FROM documents WHERE get_json_int(documents.data, %s) = %s",
        )

    @testing.combinations(
        ("index", JSONIndexType, "C:\\temp", '$."C:\\\\temp"'),
        ("index_of_an_escape", JSONIndexType, "a\\n", '$."a\\\\n"'),
        ("path", JSONPathType, ("a\\n", 1, "b\\"), '$."a\\\\n"[1]."b\\\\"'),
        ("plain", JSONPathType, ("a", 1), '$."a"[1]'),
        id_="iaaa",
        argnames="type_class, value, path",
    )
    def test_a_backslash_in_a_key_is_doubled(self, type_class, value, path):
        """StarRocks reads ``\\n`` in a quoted path key as a newline, and ``\\\\`` as one backslash."""
        eq_(type_class()._cached_bind_processor(StarRocksDialect())(value), path)


class JsonBindCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = StarRocksDialect()

    def test_insert_wraps_bind_to_keep_scalar_type(self):
        self.assert_compile(
            insert(DOCUMENTS).values(id=1, data={"a": 1}),
            "INSERT INTO documents (id, data) VALUES (%s, json_query(parse_json(concat('[', %s, ']')), '$[0]'))",
        )

    def test_generic_json_type_is_adapted(self):
        generic = Table("g", MetaData(), Column("data", GenericJSON))
        self.assert_compile(
            insert(generic).values(data=1),
            "INSERT INTO g (data) VALUES (json_query(parse_json(concat('[', %s, ']')), '$[0]'))",
        )

    def test_cast_json_to_string_renders_json_text(self):
        self.assert_compile(
            select(cast(DATA["k"], String)),
            "SELECT json_string(json_query(documents.data, %s)) AS anon_1 FROM documents",
        )

    def test_cast_json_column_to_string(self):
        self.assert_compile(select(cast(DATA, String(20))), "SELECT json_string(documents.data) AS data FROM documents")

    def test_cast_of_other_types_is_unchanged(self):
        self.assert_compile(
            select(cast(DOCUMENTS.c.id, String(20))), "SELECT CAST(documents.id AS CHAR(20)) AS id FROM documents"
        )


class JsonAccessTest(fixtures.TablesTest):
    __backend__ = True
    __only_on__ = "starrocks"

    DOCUMENT = {
        "text": "x y",
        "unicode": "réve",
        "number": 5,
        "wide": 5000000000,
        "fraction": 1.5,
        "flag": True,
        "nothing": None,
        "nested": {"deep": {"leaf": 7}},
        "items": [10, 20, 30],
        "grid": [[1, 2], [3, {"deep": 4}]],
        "k\\n": "backslash and n",
        "k\n": "newline",
        "C:\\temp": "path",
        "ends\\": "trailing backslash",
        "two\\\\": "two backslashes",
        "escapes": {"k\\n": "nested backslash and n", "k\n": "nested newline"},
        "past_2_53": 9007199254740993,
        "past_2_63": 12345678901234567890,
        "seventeen_digits": 12345678901234567,
        "off": False,
        "true_text": "true",
        "twenty_digits": "1234567890.0123456789",
    }

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "json_access",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", JSON),
            starrocks_primary_key="id",
            starrocks_distributed_by="HASH(id)",
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.json_access.insert(),
            [{"id": 1, "data": cls.DOCUMENT}, {"id": 2, "data": {}}],
        )

    def _scalar(self, connection, expression, row_id=1):
        table = self.tables.json_access
        return connection.scalar(select(expression).where(table.c.id == row_id))

    @testing.combinations(
        ("text", "as_string", "x y"),
        ("unicode", "as_string", "réve"),
        ("number", "as_integer", 5),
        ("fraction", "as_float", 1.5),
        ("flag", "as_boolean", True),
        ("number", "as_string", "5"),
        argnames="key, accessor, expected",
    )
    def test_typed_accessor(self, connection, key, accessor, expected):
        data = self.tables.json_access.c.data
        value = self._scalar(connection, getattr(data[key], accessor)())
        eq_(value, expected)
        eq_(type(value), type(expected))

    def test_integer_cast_keeps_a_wide_value(self, connection):
        data = self.tables.json_access.c.data
        eq_(self._scalar(connection, cast(data["wide"], Integer)), 5000000000)

    def test_numeric_accessor(self, connection):
        data = self.tables.json_access.c.data
        eq_(self._scalar(connection, data["fraction"].as_numeric(10, 2)), decimal.Decimal("1.50"))

    @testing.combinations(
        ("past_2_53", "past_2_53", 20, 0, "9007199254740993"),
        ("past_2_63", "past_2_63", 20, 0, "12345678901234567890"),
        ("twenty_digit_text", "twenty_digits", 20, 10, "1234567890.0123456789"),
        ("fraction", "fraction", 10, 2, "1.50"),
        ("integer", "number", 12, 0, "5"),
        ("seventeen_digits", "seventeen_digits", 17, 0, "12345678901234567"),
        id_="iaaaa",
        argnames="key, precision, scale, expected",
    )
    def test_numeric_accessor_keeps_every_digit(self, connection, key, precision, scale, expected):
        """A double holds about 16 digits; ``as_numeric`` reads the value's text instead, which holds them all."""
        data = self.tables.json_access.c.data
        eq_(self._scalar(connection, data[key].as_numeric(precision, scale)), decimal.Decimal(expected))

    @testing.combinations(
        ("true", "flag", "1.00"),
        ("false", "off", "0.00"),
        ("text_true", "true_text", None),
        ("text", "text", None),
        ("json_null", "nothing", None),
        ("missing", "absent", None),
        id_="iaa",
        argnames="key, expected",
    )
    def test_numeric_accessor_reads_a_boolean_as_one_or_zero(self, connection, key, expected):
        """A JSON boolean reads as 1 or 0, as ``as_float`` reads it; text that is not a number reads as NULL."""
        data = self.tables.json_access.c.data
        read = self._scalar(connection, data[key].as_numeric(10, 2))
        eq_(read, None if expected is None else decimal.Decimal(expected))

    def test_numeric_accessor_of_a_chain_and_in_where(self, connection):
        table = self.tables.json_access
        data = table.c.data
        eq_(self._scalar(connection, data["nested"]["deep"]["leaf"].as_numeric(5, 0)), decimal.Decimal("7"))
        eq_(self._scalar(connection, data[("grid", 1, 1, "deep")].as_numeric(5, 1)), decimal.Decimal("4.0"))
        rows = connection.execute(select(table.c.id).where(data["flag"].as_numeric(3, 0) == 1)).all()
        eq_(rows, [(1,)])

    @testing.combinations(
        ("as_string",), ("as_integer",), ("as_float",), ("as_boolean",), ("as_json",), argnames="accessor"
    )
    def test_missing_key_is_null(self, connection, accessor):
        data = self.tables.json_access.c.data
        eq_(self._scalar(connection, getattr(data["absent"], accessor)()), None)
        eq_(self._scalar(connection, getattr(data["text"], accessor)(), row_id=2), None)

    @testing.combinations(
        ("as_string",), ("as_integer",), ("as_float",), ("as_boolean",), ("as_json",), argnames="accessor"
    )
    def test_json_null_reads_as_none(self, connection, accessor):
        data = self.tables.json_access.c.data
        eq_(self._scalar(connection, getattr(data["nothing"], accessor)()), None)

    def test_untyped_index_returns_json_value(self, connection):
        data = self.tables.json_access.c.data
        eq_(self._scalar(connection, data["nested"]), {"deep": {"leaf": 7}})
        eq_(self._scalar(connection, data["items"]), [10, 20, 30])
        eq_(self._scalar(connection, data["text"]), "x y")

    def test_path_access(self, connection):
        data = self.tables.json_access.c.data
        eq_(self._scalar(connection, data[("nested", "deep", "leaf")].as_integer()), 7)
        eq_(self._scalar(connection, data[("items", 1)].as_integer()), 20)
        eq_(self._scalar(connection, data[("nested", "deep")]), {"leaf": 7})

    def test_chained_access(self, connection):
        """Each level is joined into one path; nested json_query calls would read NULL from an array."""
        data = self.tables.json_access.c.data
        eq_(self._scalar(connection, data["items"][1]), 20)
        eq_(self._scalar(connection, data["nested"]["deep"]["leaf"]), 7)
        eq_(self._scalar(connection, data["items"][2].as_integer()), 30)

    def test_chained_access_mixing_paths_and_indexes(self, connection):
        """A chain that mixes a path with an index is joined too; nested calls read another element on 4.1."""
        data = self.tables.json_access.c.data
        eq_(self._scalar(connection, data[("grid", 1)][0]), 3)
        eq_(self._scalar(connection, data[("grid", 1)][1]["deep"]), 4)
        eq_(self._scalar(connection, data["grid"][(1, 1, "deep")].as_integer()), 4)
        eq_(self._scalar(connection, data[("nested",)][("deep", "leaf")]), 7)

    @testing.combinations(
        ("empty", ""),
        ("double_quote", 'q"uote'),
        ("dollar", "$"),
        ("index_suffix", "a[0]"),
        ("index_only", "[0]"),
        ("close_bracket", "x]"),
        ("close_bracket_only", "]"),
        id_="ia",
        argnames="key",
    )
    def test_a_key_a_path_cannot_name_is_refused(self, connection, key):
        """StarRocks would read another value, or refuse the path as invalid, as it does a key holding ``]``."""
        data = self.tables.json_access.c.data
        with testing.expect_raises_message(exc.StatementError, "cannot name the key"):
            self._scalar(connection, data[key])
        with testing.expect_raises_message(exc.StatementError, "cannot name the key"):
            self._scalar(connection, data[("nested", key)])

    @testing.combinations(
        ("backslash_n", "k\\n", "backslash and n"),
        ("newline", "k\n", "newline"),
        ("windows_path", "C:\\temp", "path"),
        ("trailing_backslash", "ends\\", "trailing backslash"),
        ("two_backslashes", "two\\\\", "two backslashes"),
        id_="iaa",
        argnames="key, expected",
    )
    def test_a_key_holding_a_backslash_is_named_exactly(self, connection, key, expected):
        """A backslash in a key is doubled, so StarRocks' path reads it as itself and not as an escape."""
        data = self.tables.json_access.c.data
        eq_(self._scalar(connection, data[key].as_string()), expected)
        eq_(self._scalar(connection, data[key]), expected)

    @testing.combinations(
        ("backslash_n", "k\\n", "nested backslash and n"),
        ("newline", "k\n", "nested newline"),
        id_="iaa",
        argnames="key, expected",
    )
    def test_a_nested_key_holding_a_backslash_is_named_exactly(self, connection, key, expected):
        data = self.tables.json_access.c.data
        eq_(self._scalar(connection, data[("escapes", key)].as_string()), expected)
        eq_(self._scalar(connection, data["escapes"][key].as_string()), expected)

    def test_index_in_where(self, connection):
        table = self.tables.json_access
        rows = connection.execute(select(table.c.id).where(table.c.data["number"].as_integer() == 5)).all()
        eq_(rows, [(1,)])


class JsonScalarRoundTripTest(fixtures.TablesTest):
    __backend__ = True
    __only_on__ = "starrocks"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "json_scalars",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", JSON),
            Column("nulldata", JSON(none_as_null=True)),
            starrocks_primary_key="id",
            starrocks_distributed_by="HASH(id)",
        )

    @testing.combinations(
        (1,),
        (0,),
        (-7,),
        (1.5,),
        (-0.25,),
        (True,),
        (False,),
        ("s",),
        ("",),
        ("1",),
        ("true",),
        ("null",),
        ('it\'s "quoted" \\ back',),
        ("réve",),
        ({"a": 1, "b": [1, 2, {"c": None}]},),
        ([1, "two", 3.5, None, True],),
        ({},),
        ([],),
        argnames="value",
    )
    def test_value_round_trips_with_its_type(self, connection, value):
        table = self.tables.json_scalars
        connection.execute(table.insert(), {"id": 1, "data": value})
        result = connection.scalar(select(table.c.data))
        eq_(result, value)
        eq_(type(result), type(value))

    def test_executemany_round_trips_each_scalar(self, connection):
        table = self.tables.json_scalars
        values = [1, "1", True, 2.5, {"k": "v"}, [1, 2]]
        connection.execute(table.insert(), [{"id": i, "data": v} for i, v in enumerate(values)])
        rows = connection.execute(select(table.c.data).order_by(table.c.id)).all()
        eq_([row[0] for row in rows], values)
        eq_([type(row[0]) for row in rows], [type(v) for v in values])

    def test_update_round_trips_scalar(self, connection):
        table = self.tables.json_scalars
        connection.execute(table.insert(), {"id": 1, "data": "x"})
        connection.execute(table.update().where(table.c.id == 1).values(data=42))
        eq_(connection.scalar(select(table.c.data)), 42)

    def _stored_text(self, connection, column):
        return connection.exec_driver_sql(f"SELECT {column} FROM json_scalars").scalar()

    def test_none_is_stored_as_json_null_by_default(self, connection):
        table = self.tables.json_scalars
        connection.execute(table.insert(), {"id": 1, "data": None, "nulldata": None})
        eq_(self._stored_text(connection, "data"), "null")
        eq_(self._stored_text(connection, "nulldata"), None)

    def test_json_null_constant_is_stored_as_json_null(self, connection):
        table = self.tables.json_scalars
        connection.execute(table.insert(), {"id": 1, "data": JSON.NULL, "nulldata": JSON.NULL})
        eq_(self._stored_text(connection, "data"), "null")
        eq_(self._stored_text(connection, "nulldata"), "null")

    def test_null_construct_is_stored_as_sql_null(self, connection):
        table = self.tables.json_scalars
        connection.execute(table.insert(), {"id": 1, "data": null(), "nulldata": null()})
        eq_(self._stored_text(connection, "data"), None)
        eq_(self._stored_text(connection, "nulldata"), None)

    def test_cast_to_string_gives_json_text(self, connection):
        table = self.tables.json_scalars
        connection.execute(
            table.insert(),
            [{"id": 1, "data": "text"}, {"id": 2, "data": None}, {"id": 3, "data": 5}, {"id": 4, "data": {"a": [1]}}],
        )
        rows = connection.execute(select(table.c.id, cast(table.c.data, String)).order_by(table.c.id)).all()
        eq_(rows, [(1, '"text"'), (2, "null"), (3, "5"), (4, '{"a": [1]}')])
