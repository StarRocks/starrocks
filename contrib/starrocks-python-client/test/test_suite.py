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

import decimal
import logging
from textwrap import dedent

from sqlalchemy import Column, MetaData, Table, literal, schema, testing, text
from sqlalchemy.sql.sqltypes import Float
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL, eq_
# from sqlalchemy.testing.suite import *  # noqa: F403, I001

from starrocks.datatype import INTEGER, VARCHAR
from test.unit import test_utils


logger = logging.getLogger(__name__)


class SRAssertsCompiledSQLMixin():
    def assert_compile_normalized(self, clause, expected, **kw):
        """Compile and compare SQL using normalize_sql for format-independent comparison."""
        from sqlalchemy.testing import config

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
            'btable', m, Column("id", INTEGER, primary_key=True),
            starrocks_primary_key="id",
            starrocks_distributed_by="HASH(id)",
            starrocks_properties={"replication_num": "3"}
            )
        self.assert_compile_normalized(
            schema.CreateTable(tbl),
            'CREATE TABLE btable (id INTEGER NOT NULL)PRIMARY KEY(id) DISTRIBUTED BY HASH(id)PROPERTIES("replication_num"="3")')


# Float test fixes below for "Data type of first column cannot be FLOAT" error given by starrocks
class _LiteralRoundTripFixture2(object):
    supports_whereclause = True

    @testing.fixture
    def literal_round_trip(self, metadata, connection):
        """test literal rendering"""

        # for literal, we test the literal render in an INSERT
        # into a typed column.  we can then SELECT it back as its
        # official type; ideally we'd be able to use CAST here
        # but MySQL in particular can't CAST fully

        def run(type_, input_, output, filter_=None):
            t = Table("t", metadata, Column("a", INTEGER), Column("x", type_))
            t.create(connection)

            for value in input_:
                ins = (
                    t.insert()
                    .values(x=literal(value, type_))
                    .compile(
                        dialect=testing.db.dialect,
                        compile_kwargs=dict(literal_binds=True),
                    )
                )
                connection.execute(ins)

            if self.supports_whereclause:
                stmt = t.select().where(t.c.x == literal(value))
            else:
                stmt = t.select()

            stmt = stmt.compile(
                dialect=testing.db.dialect,
                compile_kwargs=dict(literal_binds=True),
            )
            for row in connection.execute(stmt):
                value = row[0]
                if filter_ is not None:
                    value = filter_(value)
                assert value in output

        return run


class FloatTest(_LiteralRoundTripFixture2, fixtures.TestBase):
    __backend__ = True

    @testing.fixture
    def do_numeric_test(self, metadata, connection):
        @testing.emits_warning(
            r".*does \*not\* support Decimal objects natively"
        )
        def run(type_, input_, output, filter_=None, check_scale=False):
            t = Table("t", metadata, Column("a", INTEGER), Column("x", type_))
            t.create(connection)
            connection.execute(t.insert(), [{"x": x} for x in input_])

            result = {row[1] for row in connection.execute(t.select())}
            output = set(output)
            if filter_:
                result = set(filter_(x) for x in result)
                output = set(filter_(x) for x in output)
            eq_(result, output)
            if check_scale:
                eq_([str(x) for x in result], [str(x) for x in output])

        return run

    @testing.requires.floats_to_four_decimals
    def test_float_as_decimal(self, do_numeric_test):
        do_numeric_test(
            Float(precision=8, asdecimal=True),
            [15.7563, decimal.Decimal("15.7563"), None],
            [decimal.Decimal("15.7563"), None],
            filter_=lambda n: n is not None and round(n, 4) or None,
        )

    def test_float_as_float(self, do_numeric_test):
        do_numeric_test(
            Float(precision=8),
            [15.7563, decimal.Decimal("15.7563")],
            [15.7563],
            filter_=lambda n: n is not None and round(n, 5) or None,
        )

    @testing.requires.precision_generic_float_type
    def test_float_custom_scale(self, do_numeric_test):
        do_numeric_test(
            Float(None, decimal_return_scale=6, asdecimal=True),
            [15.756382, decimal.Decimal("15.756382")],
            [decimal.Decimal("15.756382")],
            check_scale=True,
        )

    def test_render_literal_float(self, literal_round_trip):
        literal_round_trip(
            Float(4),
            [15.7563, decimal.Decimal("15.7563")],
            [15.7563],
            filter_=lambda n: n is not None and round(n, 5) or None,
        )


class StarRocksReflectionTest(fixtures.TestBase):
    __only_on__ = "starrocks"

    # @testing.combinations(True, False, argnames="primary_key")
    def test_default_reflection(self, connection, metadata):
        tn = "test_starrocks_reflection"
        Table(
            tn,
            metadata,
            Column("id", INTEGER),
            Column("data", VARCHAR(10)),
        )
        metadata.create_all(connection)

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]
        logger.debug(f"reflected table: {reflected_table!r}")

        eq_(reflected_table.name, tn)
        eq_(reflected_table.schema, None)
        eq_(len(reflected_table.columns), 2)
        eq_(reflected_table.comment, None)
        eq_(set(reflected_table.columns.keys()), {"id", "data"})
        assert isinstance(reflected_table.columns["id"].type, INTEGER)
        assert isinstance(reflected_table.columns["data"].type, VARCHAR)
        eq_(reflected_table.columns["data"].type.length, 10)
        for c in ["id", "data"]:
            eq_(reflected_table.columns[c].comment, None)
            eq_(reflected_table.columns[c].nullable, True)
            eq_(reflected_table.columns[c].default, None)
            eq_(reflected_table.columns[c].autoincrement, None)
        logger.debug(f"reflected 'COMMENT': {reflected_table.dialect_options['starrocks']['COMMENT']}")
        eq_(reflected_table.dialect_options["starrocks"]["COMMENT"], None)
        eq_(reflected_table.comment, None)
        logger.debug(f"reflected 'DISTRIBUTED_BY': {reflected_table.dialect_options['starrocks']['DISTRIBUTED_BY']}")
        # eq_(reflected_table.dialect_options["starrocks"]["DISTRIBUTED_BY"], "RANDOM")
        logger.debug(f"reflected 'ENGINE': {reflected_table.dialect_options['starrocks']['ENGINE']}")
        eq_(reflected_table.dialect_options["starrocks"]["ENGINE"], "OLAP")
        # eq_(reflected_table.dialect_options["starrocks"]["key_desc"], "DUPLICATE KEY(`id`, `data`)")
        logger.debug(f"reflected 'ORDER_BY': {reflected_table.dialect_options['starrocks']['ORDER_BY']}")
        eq_(reflected_table.dialect_options["starrocks"]["ORDER_BY"], "`id`, `data`")
        logger.debug(f"reflected 'PARTITION_BY': {reflected_table.dialect_options['starrocks']['PARTITION_BY']}")
        eq_(reflected_table.dialect_options["starrocks"]["PARTITION_BY"], None)
        logger.debug(f"reflected 'PROPERTIES': {reflected_table.dialect_options['starrocks']['PROPERTIES']}")
        assert reflected_table.dialect_options["starrocks"]["PROPERTIES"]["compression"] == "LZ4"

    def test_comment_reflection(self, connection, metadata):
        tn = "test_starrocks_reflection_comment"

        connection.execute(text(f"DROP TABLE IF EXISTS {tn}"))
        create_table_sql = dedent(f"""
            CREATE TABLE {tn} (
                id INT COMMENT 'This is id column',
                data VARCHAR(10) COMMENT 'This is data column'
            )
            COMMENT 'This is a test table'
            PROPERTIES ("replication_num"="1")
        """).strip()
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]

        eq_(reflected_table.comment, "This is a test table")
        eq_(reflected_table.columns["id"].comment, "This is id column")
        eq_(reflected_table.columns["data"].comment, "This is data column")

    def test_view_comment_reflection(self, connection, metadata):
        vn = "test_starrocks_reflection_view_comment"

        connection.execute(text(f"DROP VIEW IF EXISTS {vn}"))
        create_table_sql = dedent(f"""
            CREATE VIEW {vn} (
                id COMMENT 'This is id column',
                data COMMENT 'This is data column'
            )
            COMMENT 'This is a test view'
            AS SELECT 1 AS id, 'data' AS data
        """).strip()
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[vn], views=True)
        reflected_table = reflected_meta.tables[vn]

        eq_(reflected_table.comment, "This is a test view")
        eq_(reflected_table.columns["id"].comment, "This is id column")
        eq_(reflected_table.columns["data"].comment, "This is data column")

    @testing.combinations(["id"], ["id", "data"], argnames="key_columns")
    @testing.combinations("PRIMARY", "DUPLICATE", "UNIQUE", argnames="key_type")
    def test_key_and_distribution_reflection(self, connection, metadata, key_type, key_columns):
        tn = "test_starrocks_reflection_key"
        connection.execute(text(f"DROP TABLE IF EXISTS {tn}"))
        create_table_sql = dedent(f"""
            CREATE TABLE {tn} (
                id INT,
                data VARCHAR(10),
                something_else DATETIME
            )
            {key_type} KEY ({', '.join(key_columns)})
            DISTRIBUTED BY HASH(id)
            PROPERTIES ("replication_num"="1")
        """).strip()
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]

        if key_type == "PRIMARY":
            for c in key_columns:
                eq_(reflected_table.columns[c].nullable, False)
        eq_(reflected_table.dialect_options["starrocks"][f"{key_type.upper()}_KEY"], f"{', '.join([f'{c}' for c in key_columns])}")
        eq_(reflected_table.dialect_options["starrocks"]["DISTRIBUTED_BY"], "HASH(`id`)")

    @testing.combinations(["id"], ["id", "data"], argnames="partition_by")
    def test_partition_by_columns_reflection(self, connection, metadata, partition_by):
        tn = "test_starrocks_reflection_partition_by_columns"
        connection.execute(text(f"DROP TABLE IF EXISTS {tn}"))
        create_table_sql = dedent(f"""
            CREATE TABLE {tn} (
                id INT,
                data VARCHAR(10),
                something_else DATETIME
            )
            PARTITION BY ({', '.join(partition_by)})
            PROPERTIES ("replication_num"="1")
        """).strip()
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]

        eq_(str(reflected_table.dialect_options["starrocks"]["PARTITION_BY"]), "(%s)" % ",".join([f"`{c}`" for c in partition_by]))

    def test_expression_partitioning_reflection(self, connection, metadata):
        tn = "test_starrocks_reflection_expression_partitioning"
        connection.execute(text(f"DROP TABLE IF EXISTS {tn}"))
        create_table_sql = dedent(f"""
            CREATE TABLE {tn} (
                event_day DATETIME NOT NULL,
                data VARCHAR(10)
            )
            PARTITION BY date_trunc('month', event_day)
            PROPERTIES ("replication_num"="1")
        """).strip()
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]

        eq_(str(reflected_table.dialect_options["starrocks"]["PARTITION_BY"]), "date_trunc('month', event_day)")

    @testing.combinations(["id"], ["id", "data"], argnames="order_by")
    def test_order_by_reflection(self, connection, metadata, order_by):
        tn = "test_starrocks_reflection_order_by"
        connection.execute(text(f"DROP TABLE IF EXISTS {tn}"))
        create_table_sql = dedent(f"""
            CREATE TABLE {tn} (
                id INT,
                data VARCHAR(10),
                something_else DATETIME
            )
            ORDER BY ({', '.join(order_by)})
            PROPERTIES ("replication_num"="1")
        """).strip()
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]

        eq_(reflected_table.dialect_options["starrocks"]["ORDER_BY"], ", ".join([f"`{c}`" for c in order_by]))

    def test_nullable_column_reflection(self, connection, metadata):
        tn = "test_starrocks_reflection_nullable"
        connection.execute(text(f"DROP TABLE IF EXISTS {tn}"))
        create_table_sql = dedent(f"""
            CREATE TABLE {tn} (
                id INT NOT NULL,
                data VARCHAR(10) NULL,
                something_else DATETIME
            )
            PROPERTIES ("replication_num"="1")
        """).strip()
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]

        eq_(reflected_table.columns["id"].nullable, False)
        eq_(reflected_table.columns["data"].nullable, True)

    def test_generated_columns_reflection(self, connection, metadata):
        tn = "test_starrocks_reflection_generated"
        connection.execute(text(f"DROP TABLE IF EXISTS {tn}"))
        create_table_sql = dedent(f"""
            CREATE TABLE {tn} (
                id INT,
                data JSON,
                generated_column BIGINT AS (id + 1),
                generated_json TEXT AS get_json_string(`data`, '$.some_key') COMMENT 'generated from json'
            )
            PROPERTIES ("replication_num"="1")
        """).strip()
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]

        eq_(reflected_table.columns["generated_column"].computed.sqltext.text, "id + 1")
        eq_(reflected_table.columns["generated_json"].computed.sqltext.text, "get_json_string(`data`, '$.some_key')")

    def test_reflect_server_default_not_as_computed(self, connection, metadata):
        tn = "test_server_default_reflection"
        connection.execute(text(f"DROP TABLE IF EXISTS {tn}"))
        create_table_sql = dedent(f"""
            CREATE TABLE {tn} (
                id INT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                generated_col BIGINT AS (id + 1)
            )
            PRIMARY KEY(id)
            DISTRIBUTED BY HASH(id);
        """)
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(bind=connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]

        created_at_col = reflected_table.columns["created_at"]
        eq_(created_at_col.server_default.arg.text, "CURRENT_TIMESTAMP")
        eq_(created_at_col.computed, None)

        generated_col = reflected_table.columns["generated_col"]
        eq_(generated_col.computed.sqltext.text, "id + 1")