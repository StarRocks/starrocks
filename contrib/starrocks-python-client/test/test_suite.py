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
from textwrap import dedent

from sqlalchemy.testing.suite import *

from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy import VARCHAR, Table, Column, Integer, MetaData
from sqlalchemy import schema

from sqlalchemy.testing import fixtures
from sqlalchemy import testing, literal
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.sql.sqltypes import Float
from sqlalchemy.sql import elements


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):

    __only_on__ = "starrocks"

    def test_create_table_with_properties(self):
        m = MetaData()
        tbl = Table(
            'atable', m, Column("id", Integer),
            starrocks_properties=(
                ("storage_medium", "SSD"),
                ("storage_cooldown_time", "2015-06-04 00:00:00"),
            ))
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER)PROPERTIES(\"storage_medium\"=\"SSD\",\"storage_cooldown_time\"=\"2015-06-04 00:00:00\")")

    def test_create_primary_key_table(self):
        m = MetaData()
        tbl = Table(
            'btable', m, Column("id", Integer, primary_key=True),
            starrocks_primary_key="id",
            starrocks_distributed_by="id"
            )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE btable (id BIGINT NOT NULL AUTO_INCREMENT)PRIMARY KEY(id) DISTRIBUTED BY HASH(id)")


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
            t = Table("t", metadata, Column("a", Integer), Column("x", type_))
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
            t = Table("t", metadata, Column("a", Integer), Column("x", type_))
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
            Column("id", Integer),
            Column("data", VARCHAR(10)),
        )
        metadata.create_all(connection)

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]
        
        eq_(reflected_table.name, tn)
        eq_(reflected_table.schema, None)
        eq_(len(reflected_table.columns), 2)
        eq_(reflected_table.comment, "")
        eq_(set(reflected_table.columns.keys()), {"id", "data"})
        assert isinstance(reflected_table.columns["id"].type, Integer)
        assert isinstance(reflected_table.columns["data"].type, VARCHAR)
        eq_(reflected_table.columns["data"].type.length, 10)
        for c in ["id", "data"]:
            eq_(reflected_table.columns[c].comment, "")
            eq_(reflected_table.columns[c].nullable, True)
            eq_(reflected_table.columns[c].default, None)
            eq_(reflected_table.columns[c].autoincrement, None)
        eq_(reflected_table.dialect_options["starrocks"]["comment"], "")
        eq_(reflected_table.dialect_options["starrocks"]["distribution"], "RANDOM")
        eq_(reflected_table.dialect_options["starrocks"]["engine"], "OLAP")
        eq_(reflected_table.dialect_options["starrocks"]["key_desc"], "DUPLICATE KEY(`id`, `data`)")
        eq_(reflected_table.dialect_options["starrocks"]["order_by"], "`id`, `data`")
        eq_(reflected_table.dialect_options["starrocks"]["partition_by"], "")
        assert ("compression", "LZ4") in reflected_table.dialect_options["starrocks"]["properties"]

    def test_comment_reflection(self, connection, metadata):
        tn = "test_starrocks_reflection_comment"

        connection.execute(text(f"DROP TABLE IF EXISTS {tn}"))
        create_table_sql = dedent(f"""
            CREATE TABLE {tn} (
                id INT COMMENT 'This is id column',
                data VARCHAR(10) COMMENT 'This is data column'
            )
            COMMENT 'This is a test table'
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
        """).strip()
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]

        if key_type == "PRIMARY":
            for c in key_columns:
                eq_(reflected_table.columns[c].nullable, False)
        eq_(reflected_table.dialect_options["starrocks"]["key_desc"], f"{key_type} KEY({', '.join([f'`{c}`' for c in key_columns])})")
        eq_(reflected_table.dialect_options["starrocks"]["distribution"], "HASH(`id`)")

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
        """).strip()
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]

        eq_(reflected_table.dialect_options["starrocks"]["partition_by"], ', '.join([f'`{c}`' for c in partition_by]))

    def test_expression_partitioning_reflection(self, connection, metadata):
        tn = "test_starrocks_reflection_expression_partitioning"
        connection.execute(text(f"DROP TABLE IF EXISTS {tn}"))
        create_table_sql = dedent(f"""
            CREATE TABLE {tn} (
                event_day DATETIME NOT NULL,
                data VARCHAR(10)
            )
            PARTITION BY date_trunc('month', event_day)
        """).strip()
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]

        eq_(reflected_table.dialect_options["starrocks"]["partition_by"], "`event_day`")

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
        """).strip()
        connection.execute(text(create_table_sql))

        reflected_meta = MetaData()
        reflected_meta.reflect(connection, only=[tn])
        reflected_table = reflected_meta.tables[tn]

        eq_(reflected_table.dialect_options["starrocks"]["order_by"], ', '.join([f'`{c}`' for c in order_by]))

    def test_nullable_column_reflection(self, connection, metadata):
        tn = "test_starrocks_reflection_nullable"
        connection.execute(text(f"DROP TABLE IF EXISTS {tn}"))
        create_table_sql = dedent(f"""
            CREATE TABLE {tn} (
                id INT NOT NULL,
                data VARCHAR(10) NULL,
                something_else DATETIME
            )
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