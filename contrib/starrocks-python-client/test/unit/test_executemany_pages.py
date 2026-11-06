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

"""Which executemany INSERTs the dialect sends as multi-row INSERTs itself, and how it splits them into pages."""

from sqlalchemy import JSON, Column, Integer, MetaData, String, Table, func, insert, testing, text
from sqlalchemy.testing import eq_, fixtures

from starrocks.dialect import StarRocksDialect


_WITH_JSON = Table("t", MetaData(), Column("id", Integer), Column("v", String(10)), Column("j", JSON))
_PLAIN = Table("p", MetaData(), Column("id", Integer), Column("v", String(10)))


class _RenderingCursor:
    """A cursor that renders a row as the driver's ``mogrify`` does, with each value written as its repr."""

    max_stmt_length = 1024000

    def mogrify(self, query, args):
        if isinstance(args, dict):
            return query % {key: repr(value) for key, value in args.items()}
        return query % tuple(repr(value) for value in args)


def _compiled_for_executemany(statement, paramstyle):
    dialect = StarRocksDialect(paramstyle=paramstyle)
    column_keys = [column.key for column in statement.table.columns]
    return dialect.statement_compiler(dialect, statement, column_keys=column_keys, for_executemany=True)


class DriverRowByRowTest(fixtures.TestBase):
    """The dialect sends the rows only of an INSERT the driver would send once per row."""

    @testing.combinations(
        ("json_column", insert(_WITH_JSON), True),
        ("plain_columns", insert(_PLAIN), False),
        ("sql_expression", insert(_PLAIN).values(v=func.upper("x")), True),
        argnames="statement,paged",
        id_="iaa",
    )
    @testing.combinations(("pyformat",), ("format",), argnames="paramstyle")
    def test_an_executemany_is_paged_when_its_row_holds_more_than_placeholders(self, statement, paged, paramstyle):
        eq_(_compiled_for_executemany(statement, paramstyle)._rows_paged_by_dialect, paged)

    @testing.combinations(
        ("bare_placeholders_at_the_limit", "INSERT INTO t (a) VALUES (%s)", 10000, False),
        ("bare_placeholders_past_the_limit", "INSERT INTO t (a) VALUES (%(a)s)", 10001, True),
        ("expression_text_past_the_limit", "INSERT INTO t (a) VALUES (f(%s))", 10001, False),
        ("update_past_the_limit", "UPDATE t SET a = %s", 10001, False),
        argnames="statement,rows,paged",
        id_="iaaa",
    )
    def test_the_dialect_pages_bare_placeholders_only_past_starrocks_row_limit(self, statement, rows, paged):
        eq_(StarRocksDialect._driver_passes_the_row_limit(statement, [(1,)] * rows), paged)

    def test_a_single_execution_is_not_marked(self):
        eq_(insert(_WITH_JSON).compile(dialect=StarRocksDialect())._rows_paged_by_dialect, False)

    @testing.combinations(
        ("INSERT INTO t (a) VALUES (%s)", ("INSERT INTO t (a) VALUES ", "(%s)")),
        ("INSERT INTO t VALUES (%(a)s, parse_json(%(b)s)) ;\n", ("INSERT INTO t VALUES ", "(%(a)s, parse_json(%(b)s))")),
        ("/* x */ INSERT INTO `values` (`(`) VALUES (')', %s)", ("/* x */ INSERT INTO `values` (`(`) VALUES ", "(')', %s)")),
        ("INSERT INTO t VALUES (%s), (%s)", None),
        ("INSERT INTO t SELECT * FROM (VALUES (1)) v", None),
        ("INSERT INTO t VALUES (%s) -- trailing", None),
        ("UPDATE t SET a = %s", None),
        argnames="statement,split",
    )
    def test_the_values_row_is_split_off_its_insert(self, statement, split):
        eq_(StarRocksDialect._split_off_values_row(statement), split)


class ValuesPagesTest(fixtures.TestBase):
    """Rows go in as few multi-row INSERTs as StarRocks' row limit and the driver's statement length allow."""

    def test_rows_are_joined_after_the_head(self):
        pages = StarRocksDialect()._values_pages(
            _RenderingCursor(), "INSERT INTO t (a) VALUES (%s, f(%s))", [(1, "x"), (2, "y")]
        )
        eq_(pages, ["INSERT INTO t (a) VALUES (1, f('x')),(2, f('y'))"])

    def test_percent_in_the_head_is_unescaped(self):
        pages = StarRocksDialect()._values_pages(_RenderingCursor(), "INSERT INTO `a%%b` VALUES (f(%s))", [(1,)])
        eq_(pages, ["INSERT INTO `a%b` VALUES (f(1))"])

    def test_a_page_holds_at_most_the_row_limit(self):
        dialect = StarRocksDialect()
        with testing.mock.patch.object(StarRocksDialect, "_VALUES_ROWS_PER_STATEMENT", 2):
            pages = dialect._values_pages(_RenderingCursor(), "INSERT INTO t VALUES (f(%s))", [(n,) for n in range(5)])
        eq_(pages, ["INSERT INTO t VALUES (f(0)),(f(1))", "INSERT INTO t VALUES (f(2)),(f(3))", "INSERT INTO t VALUES (f(4))"])

    def test_a_page_is_at_most_the_drivers_statement_length_in_utf8(self):
        cursor = _RenderingCursor()
        head = "INSERT INTO t VALUES "
        row = "(f(%s))"
        one = "(f('é'))"
        cursor.max_stmt_length = len(head) + 2 * len(one.encode("utf-8")) + 1
        pages = StarRocksDialect()._values_pages(cursor, head + row, [("é",)] * 5)
        eq_([page.count("(f(") for page in pages], [2, 2, 1])
        assert all(len(page.encode("utf-8")) <= cursor.max_stmt_length for page in pages)

    def test_a_row_longer_than_the_limit_goes_alone(self):
        cursor = _RenderingCursor()
        cursor.max_stmt_length = 10
        pages = StarRocksDialect()._values_pages(cursor, "INSERT INTO t VALUES (f(%s))", [("long text",), (1,)])
        eq_(pages, ["INSERT INTO t VALUES (f('long text'))", "INSERT INTO t VALUES (f(1))"])

    @testing.combinations(
        ("INSERT INTO t SELECT f(%s)",),
        ("WITH c AS (SELECT %s) INSERT INTO t VALUES (f(%s))",),
        argnames="statement",
    )
    def test_a_statement_without_one_row_to_repeat_is_left_to_the_driver(self, statement):
        eq_(StarRocksDialect()._values_pages(_RenderingCursor(), statement, [(1,), (2,)]), None)

    def test_a_cursor_that_cannot_render_a_row_is_left_to_the_driver(self):
        eq_(StarRocksDialect()._values_pages(object(), "INSERT INTO t VALUES (f(%s))", [(1,)]), None)

    def test_text_is_not_marked(self):
        dialect = StarRocksDialect()
        compiled = text("INSERT INTO t VALUES (:a, parse_json(:b))").compile(dialect=dialect)
        eq_(getattr(compiled, "_rows_paged_by_dialect", False), False)
