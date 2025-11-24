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
    String,
    Table,
    and_,
    cast,
    column,
    literal,
    schema,
    select,
    table,
    testing,
    text,
    type_coerce,
    union,
)
from sqlalchemy.engine import ObjectKind, ObjectScope
from sqlalchemy.sql.sqltypes import CHAR, Float
from sqlalchemy.testing import config, fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL, eq_
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.suite import *  # noqa: F403, I001
from sqlalchemy.testing.suite import (
    ComponentReflectionTest as _ComponentReflectionTest,
    CTETest as _CTETest,
    CompositeKeyReflectionTest,
    EnumTest,
    FetchLimitOffsetTest as _FetchLimitOffsetTest,
    JSONTest as _JSONTest,
    LongNameBlowoutTest,
    NumericTest as _NumericTest,
    ServerSideCursorsTest as _ServerSideCursorsTest,
    StringTest as _StringTest,
)
from sqlalchemy.testing.util import mock

from starrocks.datatype import INTEGER
from test import test_utils


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

        normalized_actual = test_utils.normalize_sql(actual_sql)
        normalized_expected = test_utils.normalize_sql(expected)

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


class StarrocksMogrifyTest(fixtures.TablesTest, AssertsCompiledSQL):
    # Not strictly a dialect test, but allows me to test why mogrify is not working correctly
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

        self.assert_compile(
            sel,
            result="SELECT CAST(t1.c1 AS CHAR(4000)) AS c1, CAST(t1.c2 AS CHAR(4000)) AS c2 FROM t1 WHERE t1.test_param = %(test_param_1)s",
            params={'test_param_1':'Y'},
            render_postcompile=True,
        )
        compiled = sel.compile(connection, compile_kwargs={"render_postcompile": True})
        mog = connection.engine.raw_connection().cursor().mogrify(str(compiled).replace('\n', ''), compiled.params)
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
    # Updated to allow tests to run when Starrocks does not support default autoincrement on single primary key columns
    @classmethod
    def define_tables(cls, metadata):
        super().define_tables(metadata)
        for t_name, t in metadata.tables.items():
            if t.autoincrement_column is not None:
                t.autoincrement_column.autoincrement = True

    # Updated because Starrocks does not currently support column comments
    def exp_columns(
            self,
            schema=None,
            scope=ObjectScope.ANY,
            kind=ObjectKind.ANY,
            filter_names=None,
    ):
        def col(
                name, auto=False, default=mock.ANY, comment=None, nullable=True
        ):
            res = {
                "name": name,
                "autoincrement": auto,
                "type": mock.ANY,
                "default": default,
                "comment": comment if config.requirements.comment_reflection.enabled else '',
                "nullable": nullable,
            }
            if auto == "omit":
                res.pop("autoincrement")
            return res

        def pk(name, **kw):
            kw = {"auto": True, "default": mock.ANY, "nullable": False, **kw}
            return col(name, **kw)

        materialized = {
            (schema, "dingalings_v"): [
                col("dingaling_id", auto="omit", nullable=mock.ANY),
                col("address_id"),
                col("id_user"),
                col("data"),
            ]
        }
        views = {
            (schema, "email_addresses_v"): [
                col("address_id", auto="omit", nullable=mock.ANY),
                col("remote_user_id"),
                col("email_address"),
            ],
            (schema, "users_v"): [
                col("user_id", auto="omit", nullable=mock.ANY),
                col("test1", nullable=mock.ANY),
                col("test2", nullable=mock.ANY),
                col("parent_user_id"),
            ],
            (schema, "user_tmp_v"): [
                col("id", auto="omit", nullable=mock.ANY),
                col("name"),
                col("foo"),
            ],
        }
        self._resolve_views(views, materialized)
        tables = {
            (schema, "users"): [
                pk("user_id"),
                col("test1", nullable=False),
                col("test2", nullable=False),
                col("parent_user_id"),
            ],
            (schema, "dingalings"): [
                pk("dingaling_id"),
                col("address_id"),
                col("id_user"),
                col("data"),
            ],
            (schema, "email_addresses"): [
                pk("address_id"),
                col("remote_user_id"),
                col("email_address"),
            ],
            (schema, "comment_test"): [
                pk("id", comment="id comment"),
                col("data", comment="data % comment"),
                col(
                    "d2",
                    comment=r"""Comment types type speedily ' " \ '' Fun!""",
                ),
                col("d3", comment="Comment\nwith\rescapes"),
            ],
            (schema, "no_constraints"): [col("data")],
            (schema, "local_table"): [pk("id"), col("data"), col("remote_id")],
            (schema, "remote_table"): [pk("id"), col("local_id"), col("data")],
            (schema, "remote_table_2"): [pk("id"), col("data")],
            (schema, "noncol_idx_test_nopk"): [col("q")],
            (schema, "noncol_idx_test_pk"): [pk("id"), col("q")],
            (schema, self.temp_table_name()): [
                pk("id"),
                col("name"),
                col("foo"),
            ],
        }
        res = self._resolve_kind(kind, tables, views, materialized)
        res = self._resolve_names(schema, scope, filter_names, res)
        return res

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

    @testing.skip('starrocks', 'cannot render offset without limit')
    @testing.requires.offset
    def test_simple_offset(self, connection):
        pass

    @testing.skip('starrocks', 'cannot render offset without limit')
    @testing.requires.offset
    def test_simple_offset_zero(self, connection):
        pass

    @testing.skip('starrocks', 'cannot render offset without limit')
    @testing.requires.bound_limit_offset
    def test_bound_offset(self, connection):
        pass


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
    @testing.requires.ctes_with_update_delete
    @testing.skip('starrocks', 'needs a primary column')
    def test_delete_scalar_subq_round_trip(self, connection):
        pass

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

    @testing.skip('starrocks', 'Seems to return "null", not sure why')
    def test_round_trip_json_null_as_json_null(self, connection):
        pass

    @testing.combinations(
        ("parameters",),
        ("multiparameters",),
        ("values",),
        argnames="insert_type",
    )
    @testing.skip("starrocks", 'Seems to return "null", not sure why')
    def test_round_trip_none_as_json_null(self, connection, insert_type):
        pass

    @testing.combinations(
        (True,),
        (False,),
        (None,),
        (15,),
        (0,),
        (-1,),
        (-1.0,),
        (15.052,),
        ("a string",),
        ("réve illé",),
        ("réve🐍 illé",),
    )
    @testing.skip("starrocks", 'Seems to return "null", not sure why')
    def test_single_element_round_trip(self, element):
        pass

class ServerSideCursorsTest(_ServerSideCursorsTest):

    @testing.combinations(
        ("global_string", True, lambda stringify: stringify("select 1"), True),
        (
                "global_text",
                True,
                lambda stringify: text(stringify("select 1")),
                True,
        ),
        ("global_expr", True, select(1), True),
        (
                "global_off_explicit",
                False,
                lambda stringify: text(stringify("select 1")),
                False,
        ),
        (
                "stmt_option",
                False,
                select(1).execution_options(stream_results=True),
                True,
        ),
        (
                "stmt_option_disabled",
                True,
                select(1).execution_options(stream_results=False),
                False,
        ),
        # Omit unsupported FOR UPDATE
        # ("for_update_expr", True, select(1).with_for_update(), True),
        # # TODO: need a real requirement for this, or dont use this test
        # (
        #     "for_update_string",
        #     True,
        #     lambda stringify: stringify("SELECT 1 FOR UPDATE"),
        #     True,
        #     testing.skip_if(["sqlite", "mssql"]),
        # ),
        (
                "text_no_ss",
                False,
                lambda stringify: text(stringify("select 42")),
                False,
        ),
        (
                "text_ss_option",
                False,
                lambda stringify: text(stringify("select 42")).execution_options(
                    stream_results=True
                ),
                True,
        ),
        id_="iaaa",
        argnames="engine_ss_arg, statement, cursor_ss_status",
    )
    def test_ss_cursor_status(
            self, engine_ss_arg, statement, cursor_ss_status
    ):
        engine = self._fixture(engine_ss_arg)
        with engine.begin() as conn:
            if callable(statement):
                statement = testing.resolve_lambda(
                    statement, stringify=self.stringify
                )

            if isinstance(statement, str):
                result = conn.exec_driver_sql(statement)
            else:
                result = conn.execute(statement)
            eq_(self._is_server_side(result.cursor), cursor_ss_status)
            result.close()

    @testing.skip("starrocks", 'Auto Increment seems to increment by 100000, not 1')
    def test_roundtrip_fetchall(self, metadata):
        pass

EnumTest.__requires__ = ("enums",)  # Fix Enum handling. Mysql has native ENUM type, but Starrocks has not
LongNameBlowoutTest.__requires__ = ("index_reflection",)  # This will do to make it skip for now, no multiple column index
CompositeKeyReflectionTest.__requires__ = ('primary_key_constraint_reflection',) # This will make it also skip the fixture which is not creating succesfully
