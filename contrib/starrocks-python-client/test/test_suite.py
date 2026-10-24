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

import decimal
import logging

from sqlalchemy import (
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    and_,
    cast,
    column,
    func,
    literal,
    literal_column,
    schema,
    select,
    table,
    testing,
    text,
    type_coerce,
    union,
)
from sqlalchemy.sql.sqltypes import CHAR, Float
from sqlalchemy.testing import config, fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL, eq_
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.suite import *  # noqa: F403, I001
from sqlalchemy.testing.suite import (
    ComponentReflectionTest as _ComponentReflectionTest,
    CompositeKeyReflectionTest as _CompositeKeyReflectionTest,
    CTETest as _CTETest,
    FetchLimitOffsetTest as _FetchLimitOffsetTest,
    JSONLegacyStringCastIndexTest as _JSONLegacyStringCastIndexTest,
    JSONTest as _JSONTest,
    LongNameBlowoutTest as _LongNameBlowoutTest,
    NumericTest as _NumericTest,
    ServerSideCursorsTest as _ServerSideCursorsTest,
    StringTest as _StringTest,
)

from starrocks.datatype import INTEGER
from starrocks.requirements import IS_SQLALCHEMY_2
from test.util import normalize_sql


if IS_SQLALCHEMY_2:
    from sqlalchemy.testing.suite import TrueDivTest as _TrueDivTest
else:
    from sqlalchemy.testing.suite import (
        HasIndexTest,
        LikeFunctionsTest as _LikeFunctionsTest,
        QuotedNameArgumentTest as _QuotedNameArgumentTest,
    )


logger = logging.getLogger(__name__)


class SRAssertsCompiledSQLMixin:
    def assert_compile_normalized(self, clause, expected, **kw):
        """Compile and compare SQL using normalize_sql for format-independent comparison."""

        dialect = kw.get('dialect')
        if dialect is None:
            dialect = getattr(self, "__dialect__", None)
        if dialect is None:
            dialect = config.db.dialect
        # logger.debug(f"dialect: {dialect}")

        compiled = clause.compile(dialect=dialect, **kw.get('compile_kwargs', {}))
        actual_sql = str(compiled)

        normalized_actual = normalize_sql(actual_sql)
        normalized_expected = normalize_sql(expected)

        assert normalized_actual == normalized_expected, (
            f"\nActual (normalized):   {normalized_actual}\n"
            f"Expected (normalized): {normalized_expected}\n"
            f"Actual (raw):\n{actual_sql}"
        )


class CompileTest(fixtures.TestBase, AssertsCompiledSQL, SRAssertsCompiledSQLMixin):

    __only_on__ = "starrocks"

    def test_create_table_with_properties(self):
        m = MetaData()
        tbl = Table(
            'atable', m, Column("id", INTEGER),
            starrocks_properties=(
                ("storage_medium", "SSD"),
                ("storage_cooldown_time", "2015-06-04 00:00:00"),
                ("replication_num", "1"),
            ))
        self.assert_compile_normalized(
            schema.CreateTable(tbl),
            'CREATE TABLE atable (id INTEGER) PROPERTIES("storage_medium"="SSD", "storage_cooldown_time"="2015-06-04 00:00:00", "replication_num"="1")')

    def test_create_primary_key_table(self):
        m = MetaData()
        tbl = Table(
            'btable', m, Column("id", INTEGER, primary_key=True, autoincrement=False), # primary key must be specified autoincrement=False if not wanted to be auto generated
            starrocks_primary_key="id",
            starrocks_distributed_by="HASH(id)",
            starrocks_properties={"replication_num": "3"}
        )
        self.assert_compile_normalized(
            schema.CreateTable(tbl),
            'CREATE TABLE btable (id INTEGER NOT NULL)PRIMARY KEY(id) DISTRIBUTED BY HASH(id)PROPERTIES("replication_num"="3")')

    def test_offset_without_limit_gets_a_limit(self):
        """StarRocks takes OFFSET only after LIMIT; 2**62 - 1 rows leaves the sum within 64 bits."""
        t = table("t", column("id"))
        self.assert_compile(
            select(t.c.id).order_by(t.c.id).offset(5),
            "SELECT t.id FROM t ORDER BY t.id LIMIT 4611686018427387903 OFFSET 5",
            literal_binds=True,
        )


class StarrocksMogrifyTest(fixtures.TablesTest, AssertsCompiledSQL):
    """The driver's ``mogrify`` renders a compiled statement with its parameters as literals.

    pymysql compiles named ``%(name)s`` placeholders and asyncmy positional ``%s`` ones, so the
    expected placeholder and the parameters given to ``mogrify`` follow the dialect's paramstyle.
    """

    def test_mogrify_query_with_parameters(self, connection):
        t = table('t1', column('c1'), column('c2'), column('test_param'))

        sel = select(
            cast(t.c.c1, CHAR(4000)),
            cast(t.c.c2, CHAR(4000)),
        ).where(
            and_(
                t.c.test_param == 'Y',
                ),
        )

        positional = connection.dialect.positional
        placeholder = "%s" if positional else "%(test_param_1)s"
        self.assert_compile(
            sel,
            result=f"SELECT CAST(t1.c1 AS CHAR(4000)) AS c1, CAST(t1.c2 AS CHAR(4000)) AS c2 FROM t1 WHERE t1.test_param = {placeholder}",
            params={'test_param_1':'Y'},
            render_postcompile=True,
        )
        compiled = sel.compile(connection, compile_kwargs={"render_postcompile": True})
        params = tuple(compiled.params[name] for name in compiled.positiontup) if positional else compiled.params
        cursor = connection.connection.driver_connection.cursor()
        mog = cursor.mogrify(str(compiled).replace('\n', ''), params)
        assert mog == "SELECT CAST(t1.c1 AS CHAR(4000)) AS c1, CAST(t1.c2 AS CHAR(4000)) AS c2 FROM t1 WHERE t1.test_param = 'Y'"

    # def test_select_nonrecursive_round_trip(self, connection):
    #     some_table = self.tables.some_table
    #
    #     cte = (
    #         select(some_table)
    #         .where(some_table.c.data.in_(["d2", "d3", "d4"]))
    #         .cte("some_cte")
    #     )
    #     result = connection.execute(
    #         select(cte.c.data).where(cte.c.data.in_(["d4", "d5"]))
    #     )
    #     eq_(result.fetchall(), [("d4",)])

class ComponentReflectionTest(_ComponentReflectionTest):
    """On SQLAlchemy 1.4 ``test_get_indexes`` is gated on ``index_reflection``, as 2.0 already does."""

    # Updated to allow tests to run when Starrocks does not support default autoincrement on single primary key columns
    @classmethod
    def define_tables(cls, metadata):
        super().define_tables(metadata)
        for t_name, t in metadata.tables.items():
            autoincrement_column = t.autoincrement_column if IS_SQLALCHEMY_2 else t._autoincrement_column
            if autoincrement_column is not None:
                autoincrement_column.autoincrement = True

    if not IS_SQLALCHEMY_2:
        test_get_indexes = testing.requires.index_reflection(_ComponentReflectionTest.test_get_indexes)


if not IS_SQLALCHEMY_2:

    class QuotedNameArgumentTest(_QuotedNameArgumentTest):
        """SQLAlchemy 2.0 gates the foreign key and index tests on requirements; 1.4 does not."""

        test_get_foreign_keys = testing.requires.foreign_key_constraint_reflection(
            _QuotedNameArgumentTest.test_get_foreign_keys
        )
        test_get_indexes = testing.requires.index_reflection(_QuotedNameArgumentTest.test_get_indexes)


class FetchLimitOffsetTest(_FetchLimitOffsetTest):

    # Fixed by adding order_by
    def test_limit_render_multiple_times(self, connection):
        table = self.tables.some_table
        stmt = select(table.c.id).order_by(table.c.id).limit(1).scalar_subquery()

        u = union(select(stmt), select(stmt)).subquery().select()

        self._assert_result(
            connection,
            u,
            [
                (1,),
            ],
        )



class NumericTest(_NumericTest,):
    # Changed because Starrocks does not support Float as first column

    @testing.fixture
    def do_numeric_test(self, metadata, connection):
        def run(type_, input_, output, filter_=None, check_scale=False):
            # Fix table so the first column is not float
            t = Table("t", metadata, Column("a", Integer), Column("x", type_))
            t.create(connection)
            connection.execute(t.insert(), [{"a": 1, "x": x} for x in input_])

            result = {row[0] for row in connection.execute(select(t.c.x))}
            output = set(output)
            if filter_:
                result = {filter_(x) for x in result}
                output = {filter_(x) for x in output}
            eq_(result, output)
            if check_scale:
                eq_([str(x) for x in result], [str(x) for x in output])

            connection.execute(t.delete())

            # test that this is actually a number!
            # note we have tiny scale here as we have tests with very
            # small scale Numeric types.  PostgreSQL will raise an error
            # if you use values outside the available scale.
            if type_.asdecimal:
                test_value = decimal.Decimal("2.9")
                add_value = decimal.Decimal("37.12")
            else:
                test_value = 2.9
                add_value = 37.12

            connection.execute(t.insert(), {"x": test_value})
            assert_we_are_a_number = connection.scalar(
                select(type_coerce(t.c.x + add_value, type_))
            )
            eq_(
                round(assert_we_are_a_number, 3),
                round(test_value + add_value, 3),
            )

        return run

    if IS_SQLALCHEMY_2:
        @testing.fixture
        def literal_round_trip(self, metadata, connection):
            """test literal rendering"""

            # for literal, we test the literal render in an INSERT
            # into a typed column.  we can then SELECT it back as its
            # official type; ideally we'd be able to use CAST here
            # but MySQL in particular can't CAST fully

            def run(
                    type_,
                    input_,
                    output,
                    filter_=None,
                    compare=None,
                    support_whereclause=True,
            ):
                if isinstance(type_, Float):
                    t = Table("t", metadata, Column("a", Integer), Column("x", type_))
                else:
                    t = Table("t", metadata, Column("x", type_))
                t.create(connection)

                for value in input_:
                    ins = t.insert().values(
                        x=literal(value, type_, literal_execute=True)
                    )
                    connection.execute(ins)

                ins = t.insert().values(
                    x=literal(None, type_, literal_execute=True)
                )
                connection.execute(ins)

                if support_whereclause and self.supports_whereclause:
                    if compare:
                        stmt = select(t.c.x).where(
                            t.c.x
                            == literal(
                                compare,
                                type_,
                                literal_execute=True,
                            ),
                            t.c.x
                            == literal(
                                input_[0],
                                type_,
                                literal_execute=True,
                            ),
                            )
                    else:
                        stmt = select(t.c.x).where(
                            t.c.x
                            == literal(
                                compare if compare is not None else input_[0],
                                type_,
                                literal_execute=True,
                            )
                        )
                else:
                    stmt = select(t.c.x).where(t.c.x.is_not(None))

                rows = connection.execute(stmt).all()
                assert rows, "No rows returned"
                for row in rows:
                    value = row[0]
                    if filter_ is not None:
                        value = filter_(value)
                    assert value in output

                stmt = select(t.c.x).where(t.c.x.is_(None))
                rows = connection.execute(stmt).all()
                eq_(rows, [(None,)])

            return run
    else:
        @testing.fixture
        def literal_round_trip(self, metadata, connection):
            """SQLAlchemy 1.4's literal round trip, keeping a FLOAT out of the first column"""

            def run(type_, input_, output, filter_=None):
                if isinstance(type_, Float):
                    t = Table("t", metadata, Column("a", Integer), Column("x", type_))
                else:
                    t = Table("t", metadata, Column("x", type_))
                t.create(connection)

                for value in input_:
                    ins = t.insert().values(x=literal(value, type_)).compile(
                        dialect=testing.db.dialect, compile_kwargs=dict(literal_binds=True)
                    )
                    connection.execute(ins)

                stmt = select(t.c.x)
                if self.supports_whereclause:
                    stmt = stmt.where(t.c.x == literal(value))
                stmt = stmt.compile(dialect=testing.db.dialect, compile_kwargs=dict(literal_binds=True))
                for row in connection.execute(stmt):
                    value = row[0]
                    if filter_ is not None:
                        value = filter_(value)
                    assert value in output

            return run


DECIMAL_SCALE_ABOVE_38 = testing.skip(
    "starrocks", "the quotient of two DECIMALs of scale 20 needs scale 40; StarRocks allows 38"
)
NULLIF_DECIMAL_OVERFLOW = testing.skip(
    "starrocks", "StarRocks overflows the DECIMAL(38, 0) quotient to NULL once the divisor is wrapped in nullif()"
)
TINY_DIVIDEND, TINY_DIVISOR = "0.00000000000049382716", "0.00000000000012345679"
BIG_DIVIDEND, BIG_DIVISOR = "30000000000000000000000000000000000000", "10000000000000000000000000000000000000"
MANY_DIGITS_DIVIDEND, MANY_DIGITS_DIVISOR = "5.02797279921331664759", "0.71828182845904523537"


if IS_SQLALCHEMY_2:

    class TrueDivTest(_TrueDivTest):
        """Upstream's pair of combinations, flattened so StarRocks' DECIMAL limits can be marked per case."""

        @testing.combinations(
            ("float-typed", "7.0", "3.5", Float(), "2", False),
            ("float-untyped_function", "7.0", "3.5", Float(), "2", True),
            ("numeric_whole_number-typed", "1", "2.00", Numeric(10, 2), "0.5", False),
            ("numeric_whole_number-untyped_function", "1", "2.00", Numeric(10, 2), "0.5", True),
            ("numeric_many_decimals-typed", MANY_DIGITS_DIVIDEND, MANY_DIGITS_DIVISOR, Numeric(22, 20), "7", False,
             DECIMAL_SCALE_ABOVE_38),
            ("numeric_many_decimals-untyped_function", MANY_DIGITS_DIVIDEND, MANY_DIGITS_DIVISOR, Numeric(22, 20), "7",
             True, DECIMAL_SCALE_ABOVE_38),
            ("numeric_tiny_divisor-typed", TINY_DIVIDEND, TINY_DIVISOR, Numeric(38, 20), "4", False,
             DECIMAL_SCALE_ABOVE_38),
            ("numeric_tiny_divisor-untyped_function", TINY_DIVIDEND, TINY_DIVISOR, Numeric(38, 20), "4", True,
             DECIMAL_SCALE_ABOVE_38),
            ("numeric_38_digit_divisor-typed", BIG_DIVIDEND, BIG_DIVISOR, Numeric(38, 0), "3", False),
            ("numeric_38_digit_divisor-untyped_function", BIG_DIVIDEND, BIG_DIVISOR, Numeric(38, 0), "3", True,
             NULLIF_DECIMAL_OVERFLOW),
            id_="iaaaaa",
            argnames="left, right, type_, expected, hide_divisor_type",
        )
        def test_truediv_divisor(self, connection, left, right, type_, expected, hide_divisor_type):
            divisor = cast(literal_column(right), type_)
            if hide_divisor_type:
                divisor = func.nullif(divisor, 0)
            result = connection.scalar(select(cast(literal_column(left), type_) / divisor))

            eq_(decimal.Decimal(str(result)), decimal.Decimal(expected))


class StringTest(_StringTest):
    # Fixed by adding order_by
    @testing.combinations(
        ("%B%", ["AB", "BC"]),
        ("A%C", ["AC"]),
        ("A%C%Z", []),
        argnames="expr, expected",
    )
    def test_dont_truncate_rightside(
            self, metadata, connection, expr, expected
    ):
        t = Table("t", metadata, Column("x", String(2)))
        t.create(connection)

        connection.execute(t.insert(), [{"x": "AB"}, {"x": "BC"}, {"x": "AC"}])

        eq_(
            connection.scalars(select(t.c.x).where(t.c.x.like(expr)).order_by(t.c.x)).all(),
            expected,
        )

class CTETest(_CTETest):
    """The suite's tables become PRIMARY KEY tables, the only kind StarRocks lets UPDATE and DELETE target."""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "some_table",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(50)),
            Column("parent_id", Integer),
            starrocks_primary_key="id",
            starrocks_distributed_by="HASH(id)",
        )

        Table(
            "some_other_table",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(50)),
            Column("parent_id", Integer),
            starrocks_primary_key="id",
            starrocks_distributed_by="HASH(id)",
        )

    @testing.skip('starrocks', 'Does not support recursive CTE')
    def test_select_recursive_round_trip(self, connection):
        pass

class JSONTest(_JSONTest):
    # Override define_tables to specify autoincrement=True explicitly for test on Starrocks
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data_table",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(30), nullable=False),
            Column("data", cls.datatype, nullable=False),
            Column("nulldata", cls.datatype(none_as_null=True)),
        )


class JSONLegacyStringCastIndexTest(_JSONLegacyStringCastIndexTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data_table",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(30), nullable=False),
            Column("data", cls.datatype),
            Column("nulldata", cls.datatype(none_as_null=True)),
        )


FOR_UPDATE = testing.skip("starrocks", "StarRocks does not support SELECT ... FOR UPDATE")
class CompositeKeyReflectionTest(_CompositeKeyReflectionTest):
    """Upstream's tables, with tb1's columns in its key's order.

    StarRocks needs a PRIMARY KEY table's key columns first, in key order. Upstream's tb1 lists its key (name, id,
    attr) in another order than its columns, so the dialect would create it as a DUPLICATE KEY table, with a warning,
    and a DUPLICATE KEY table reflects no primary key.
    """

    @classmethod
    def define_tables(cls, metadata):
        tb1 = Table(
            "tb1",
            metadata,
            Column("name", String(20)),
            Column("id", Integer),
            Column("attr", Integer),
            schema.PrimaryKeyConstraint("name", "id", "attr", name="pk_tb1"),
            test_needs_fk=True,
        )
        Table(
            "tb2",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("pid", Integer),
            Column("pattr", Integer),
            Column("pname", String(20)),
            schema.ForeignKeyConstraint(
                ["pname", "pid", "pattr"], [tb1.c.name, tb1.c.id, tb1.c.attr], name="fk_tb1_name_id_attr"
            ),
            test_needs_fk=True,
        )


NO_AUTOINCREMENT_IN_THE_1_4_FIXTURE = testing.skip(
    "starrocks",
    "SQLAlchemy 1.4's fixture leaves out test_needs_autoincrement (2.0 adds it), and the dialect renders "
    "AUTO_INCREMENT only when it is asked for, so the inserted rows get no id",
)


class ServerSideCursorsTest(_ServerSideCursorsTest):
    """Upstream's cursor status test, with its FOR UPDATE combinations skipped.

    The body is copied rather than delegated: the upstream combinations carry an exclusion, which adds an
    ``_exclusions_0`` parameter to the upstream function.
    """

    if IS_SQLALCHEMY_2:

        @testing.combinations(
            ("global_string", True, lambda stringify: stringify("select 1"), True),
            ("global_text", True, lambda stringify: text(stringify("select 1")), True),
            ("global_expr", True, select(1), True),
            ("global_off_explicit", False, lambda stringify: text(stringify("select 1")), False),
            ("stmt_option", False, select(1).execution_options(stream_results=True), True),
            ("stmt_option_disabled", True, select(1).execution_options(stream_results=False), False),
            ("for_update_expr", True, select(1).with_for_update(), True, FOR_UPDATE),
            ("for_update_string", True, lambda stringify: stringify("SELECT 1 FOR UPDATE"), True, FOR_UPDATE),
            ("text_no_ss", False, lambda stringify: text(stringify("select 42")), False),
            (
                "text_ss_option",
                False,
                lambda stringify: text(stringify("select 42")).execution_options(stream_results=True),
                True,
            ),
            id_="iaaa",
            argnames="engine_ss_arg, statement, cursor_ss_status",
        )
        def test_ss_cursor_status(self, engine_ss_arg, statement, cursor_ss_status):
            engine = self._fixture(engine_ss_arg)
            with engine.begin() as conn:
                if callable(statement):
                    statement = testing.resolve_lambda(statement, stringify=self.stringify)
                if isinstance(statement, str):
                    result = conn.exec_driver_sql(statement)
                else:
                    result = conn.execute(statement)
                eq_(self._is_server_side(result.cursor), cursor_ss_status)
                result.close()
    else:

        @testing.combinations(
            ("global_string", True, "select 1", True),
            ("global_text", True, text("select 1"), True),
            ("global_expr", True, select(1), True),
            ("global_off_explicit", False, text("select 1"), False),
            ("stmt_option", False, select(1).execution_options(stream_results=True), True),
            ("stmt_option_disabled", True, select(1).execution_options(stream_results=False), False),
            ("for_update_expr", True, select(1).with_for_update(), True, FOR_UPDATE),
            ("for_update_string", True, "SELECT 1 FOR UPDATE", True, FOR_UPDATE),
            ("text_no_ss", False, text("select 42"), False),
            ("text_ss_option", False, text("select 42").execution_options(stream_results=True), True),
            id_="iaaa",
            argnames="engine_ss_arg, statement, cursor_ss_status",
        )
        def test_ss_cursor_status(self, engine_ss_arg, statement, cursor_ss_status):
            engine = self._fixture(engine_ss_arg)
            with engine.begin() as conn:
                if isinstance(statement, str):
                    result = conn.exec_driver_sql(statement)
                else:
                    result = conn.execute(statement)
                eq_(self._is_server_side(result.cursor), cursor_ss_status)
                result.close()

        @NO_AUTOINCREMENT_IN_THE_1_4_FIXTURE
        def test_roundtrip_fetchmany(self, metadata):
            pass

        @NO_AUTOINCREMENT_IN_THE_1_4_FIXTURE
        def test_roundtrip_fetchall(self, metadata):
            pass

SINGLE_COLUMN_INDEXES = testing.skip("starrocks", "a StarRocks index covers one column, and this one has two")


class LongNameBlowoutTest(_LongNameBlowoutTest):
    """Upstream's long constraint name test, with its two-column index case skipped.

    The skip goes on that case rather than the class: SQLAlchemy 2.0 drops a ``__backend__`` class whose
    requirements fail without reporting its tests as skipped. The body is copied rather than delegated, since the
    upstream combinations carry an exclusion, which adds an ``_exclusions_0`` parameter to the upstream function.
    """

    @testing.combinations(
        ("fk",),
        ("pk",),
        ("ix", SINGLE_COLUMN_INDEXES),
        ("ck", testing.requires.check_constraint_reflection.as_skips()),
        ("uq", testing.requires.unique_constraint_reflection.as_skips()),
        argnames="type_",
    )
    def test_long_convention_name(self, type_, metadata, connection):
        actual_name, reflected_name = getattr(self, type_)(metadata, connection)

        assert len(actual_name) > 255

        if reflected_name is not None:
            overlap = actual_name[0 : len(reflected_name)]
            if len(overlap) < len(actual_name):
                eq_(overlap[0:-5], reflected_name[0 : len(overlap) - 5])
            else:
                eq_(overlap, reflected_name)


if not IS_SQLALCHEMY_2:
    HasIndexTest.__requires__ = ('index_reflection',)

    class LikeFunctionsTest(_LikeFunctionsTest):
        """SQLAlchemy 2.0 gates the escape tests on ``like_escapes``; 1.4 does not."""

        test_startswith_autoescape = testing.requires.like_escapes(_LikeFunctionsTest.test_startswith_autoescape)
        test_startswith_escape = testing.requires.like_escapes(_LikeFunctionsTest.test_startswith_escape)
        test_startswith_autoescape_escape = testing.requires.like_escapes(
            _LikeFunctionsTest.test_startswith_autoescape_escape
        )
        test_endswith_autoescape = testing.requires.like_escapes(_LikeFunctionsTest.test_endswith_autoescape)
        test_endswith_escape = testing.requires.like_escapes(_LikeFunctionsTest.test_endswith_escape)
        test_endswith_autoescape_escape = testing.requires.like_escapes(
            _LikeFunctionsTest.test_endswith_autoescape_escape
        )
        test_contains_autoescape = testing.requires.like_escapes(_LikeFunctionsTest.test_contains_autoescape)
        test_contains_escape = testing.requires.like_escapes(_LikeFunctionsTest.test_contains_escape)
        test_contains_autoescape_escape = testing.requires.like_escapes(
            _LikeFunctionsTest.test_contains_autoescape_escape
        )
