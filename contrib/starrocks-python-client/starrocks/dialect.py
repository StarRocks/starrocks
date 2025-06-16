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
import re

from sqlalchemy import exc, schema as sa_schema
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
from sqlalchemy.dialects.mysql.base import MySQLDDLCompiler, MySQLTypeCompiler, MySQLCompiler, MySQLIdentifierPreparer
from sqlalchemy.sql import sqltypes
from sqlalchemy.util import topological
from sqlalchemy import util
from sqlalchemy import log
from sqlalchemy.engine import reflection
from sqlalchemy.dialects.mysql.types import TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, DOUBLE, FLOAT, CHAR, VARCHAR, DATETIME
from sqlalchemy.dialects.mysql.json import JSON
from .datatype import (
    LARGEINT, HLL, BITMAP, PERCENTILE, ARRAY, MAP, STRUCT,
    DATE
)

from . import reflection as _reflection


##############################################################################################
## NOTES - INCOMPLETE/UNFINISHED
# There are a number of items in here marked as ToDo
# In terms of table creation, the Partition, Distribution and OrderBy clauses need to be addressed from table options
# Tests `test_has_index` and `test_has_index_schema` are failing, this is because the CREATE INDEX statement appears to work async
#  and only when it's finished does it appear in the table definition
# Other tests are failing, need to fix or figure out how to suppress
# Review some skipped test suite requirements
#
#
#
##############################################################################################

# starrocks supported data types
ischema_names = {
    # === Boolean ===
    "boolean": sqltypes.BOOLEAN,
    # === Integer ===
    "tinyint": TINYINT,
    "smallint": SMALLINT,
    "int": INTEGER,
    "bigint": BIGINT,
    "largeint": LARGEINT,
    # === Floating-point ===
    "float": FLOAT,
    "double": DOUBLE,
    # === Fixed-precision ===
    "decimal": DECIMAL,
    "decimal64": DECIMAL,
    # === String ===
    "varchar": VARCHAR,
    "char": CHAR,
    "json": JSON,
    # === Date and time ===
    "date": DATE,
    "datetime": DATETIME,
    "timestamp": sqltypes.DATETIME,
    # == binary ==
    "binary": sqltypes.BINARY,
    "varbinary": sqltypes.VARBINARY,
    # === Structural ===
    "array": ARRAY,
    "map": MAP,
    "struct": STRUCT,
    "hll": HLL,
    "percentile": PERCENTILE,
    "bitmap": BITMAP,
}


class StarRocksTypeCompiler(MySQLTypeCompiler):

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_FLOAT(self, type_, **kw):
        return "FLOAT"

    def visit_TINYINT(self, type_, **kw):
        return "TINYINT"

    def visit_SMALLINT(self, type_, **kw):
        return "SMALLINT"

    def visit_INTEGER(self, type_, **kw):
        return "INTEGER"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"

    def visit_LARGEINT(self, type_, **kw):
        return "LARGEINT"

    def visit_ARRAY(self, type_, **kw):
        return "ARRAY<type>"

    def visit_MAP(self, type_, **kw):
        return "MAP<keytype,valuetype>"

    def visit_STRUCT(self, type_, **kw):
        return "STRUCT<name, type>"

    def visit_HLL(self, type_, **kw):
        return "HLL"

    def visit_BITMAP(self, type_, **kw):
        return "BITMAP"


class StarRocksSQLCompiler(MySQLCompiler):
    def visit_delete(self, delete_stmt, **kw):
        result = super().visit_delete(delete_stmt, **kw)
        compile_state = delete_stmt._compile_state_factory(
            delete_stmt, self, **kw
        )
        delete_stmt = compile_state.statement
        table = self.delete_table_clause(
            delete_stmt, delete_stmt.table, False
        )
        if not delete_stmt._where_criteria:
            return "TRUNCATE TABLE " + table
        return result

    def limit_clause(self, select, **kw):
        # StarRocks supports:
        #   LIMIT <limit>
        #   LIMIT <limit> OFFSET <offset>
        text = ""
        if select._limit_clause is not None:
            text += "\n LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            # if select._limit_clause is None:
            #     text += "\n LIMIT -1"
            text += " OFFSET " + self.process(select._offset_clause, **kw)
        return text


class StarRocksDDLCompiler(MySQLDDLCompiler):

    def visit_create_table(self, create, **kw):
        table = create.element
        preparer = self.preparer

        text = "\nCREATE "
        if table._prefixes:
            text += " ".join(table._prefixes) + " "

        text += "TABLE "
        if create.if_not_exists:
            text += "IF NOT EXISTS "

        text += preparer.format_table(table) + " "

        create_table_suffix = self.create_table_suffix(table)
        if create_table_suffix:
            text += create_table_suffix + " "

        text += "("

        separator = "\n"

        # if only one primary key, specify it along with the column
        first_pk = False
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(
                    create_column, first_pk=column.primary_key and not first_pk
                )
                if processed is not None:
                    text += separator
                    separator = ", \n"
                    text += "\t" + processed
                if column.primary_key:
                    first_pk = True
            except exc.CompileError as ce:
                raise exc.CompileError(
                    "(in table '%s', column '%s'): %s"
                    % (table.description, column.name, ce.args[0])
                ) from ce

        # N.B. Primary Key is specified in post_create_table
        #  Indexes are created by SQLA after the creation of the table using CREATE INDEX
        # const = self.create_table_constraints(
        #     table,
        #     _include_foreign_key_constraints=create.include_foreign_key_constraints,  # noqa
        # )
        # if const:
        #     text += separator + "\t" + const

        text += "\n)%s\n\n" % self.post_create_table(table)
        return text

    def post_create_table(self, table):
        """Build table-level CREATE options like ENGINE and COLLATE."""

        table_opts = []

        opts = dict(
            (k[len(self.dialect.name) + 1 :].upper(), v)
            for k, v in table.kwargs.items()
            if k.startswith("%s_" % self.dialect.name)
        )

        if table.comment is not None:
            opts["COMMENT"] = table.comment


        if 'ENGINE' in opts:
            table_opts.append(f'ENGINE={opts["ENGINE"]}')

        if 'PRIMARY_KEY' in opts:
            table_opts.append(f'PRIMARY KEY({opts["PRIMARY_KEY"]})')

        if 'DISTRIBUTED_BY' in opts:
            table_opts.append(f'DISTRIBUTED BY HASH({opts["DISTRIBUTED_BY"]})')

        if "COMMENT" in opts:
            comment = self.sql_compiler.render_literal_value(
                opts["COMMENT"], sqltypes.String()
            )
            table_opts.append(f"COMMENT {comment}")

        # ToDo - Partition
        # ToDo - Distribution
        # ToDo - Order by

        if "PROPERTIES" in opts:
            props = ",\n".join([f'\t"{k}"="{v}"' for k, v in opts["PROPERTIES"]])
            table_opts.append(f"PROPERTIES(\n{props}\n)")

        return " ".join(table_opts)

    def get_column_specification(self, column, **kw):
        """Builds column DDL."""

        colspec = [
            self.preparer.format_column(column),
            self.dialect.type_compiler.process(
                column.type, type_expression=column
            ),
        ]

        # ToDo: Support aggregation type
        #  agg_type: aggregation type.If not specified, this column is key column.If specified, it is value
        #  column.The aggregation types supported are as follows:
        #  SUM, MAX, MIN, REPLACE
        #  HLL_UNION(only for HLL type)
        #  BITMAP_UNION(only for BITMAP)
        #  REPLACE_IF_NOT_NULL

        # if column.computed is not None:
        #     colspec.append(self.process(column.computed))

        # is_timestamp = isinstance(
        #     column.type._unwrapped_dialect_impl(self.dialect),
        #     sqltypes.TIMESTAMP,
        # )

        if not column.nullable:
            colspec.append("NOT NULL")

        # see: https://docs.sqlalchemy.org/en/latest/dialects/mysql.html#mysql_timestamp_null  # noqa
        # elif column.nullable and is_timestamp:
        #     colspec.append("NULL")

        # Column comment is not supported in Starrocks
        # comment = column.comment
        # if comment is not None:
        #     literal = self.sql_compiler.render_literal_value(
        #         comment, sqltypes.String()
        #     )
        #     colspec.append("COMMENT " + literal)

        # ToDo >= version 3.0
        if (
            column.table is not None
            and column is column.table._autoincrement_column
            and (
                column.server_default is None
                or isinstance(column.server_default, sa_schema.Identity)
            )
            and not (
                self.dialect.supports_sequences
                and isinstance(column.default, sa_schema.Sequence)
                and not column.default.optional
            )
        ):
            colspec[1] = "BIGINT" # ToDo - remove this, find way to fix the test
            colspec.append("AUTO_INCREMENT")
        else:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec.append("DEFAULT " + default)
        return " ".join(colspec)

    def visit_computed_column(self, generated, **kw):
        #ToDo >= version 3.1
        text = "AS (%s)" % self.sql_compiler.process(
            generated.sqltext, include_table=False, literal_binds=True
        )
        return text

    def visit_primary_key_constraint(self, constraint, **kw):
        if len(constraint) == 0:
            return ""
        text = ""
        # if constraint.name is not None:
        #     formatted_name = self.preparer.format_constraint(constraint)
        #     if formatted_name is not None:
        #         text += "CONSTRAINT %s " % formatted_name
        text += "PRIMARY KEY "
        text += "(%s)" % ", ".join(
            self.preparer.quote(c.name)
            for c in (
                constraint.columns_autoinc_first
                if constraint._implicit_generated
                else constraint.columns
            )
        )
        text += self.define_constraint_deferrability(constraint)
        return text

    def visit_set_table_comment(self, create):
        return "ALTER TABLE %s COMMENT=%s" % (
            self.preparer.format_table(create.element),
            self.sql_compiler.render_literal_value(
                create.element.comment, sqltypes.String()
            ),
        )

    def visit_drop_table_comment(self, create):
        return "ALTER TABLE %s COMMENT=''" % (
            self.preparer.format_table(create.element)
        )


class StarRocksIdentifierPreparer(MySQLIdentifierPreparer):
    # reserved_words = RESERVED_WORDS
    pass


@log.class_logger
class StarRocksDialect(MySQLDialect_pymysql):
    name = "starrocks"
    # Caching
    # Warnings are generated by SQLAlchemy if this flag is not explicitly set
    # and tests are needed before being enabled
    supports_statement_cache = True
    supports_server_side_cursors = False
    supports_empty_insert = False

    ischema_names = ischema_names


    statement_compiler = StarRocksSQLCompiler
    ddl_compiler = StarRocksDDLCompiler
    type_compiler = StarRocksTypeCompiler
    preparer = StarRocksIdentifierPreparer

    def __init__(self, *args, **kw):
        super(StarRocksDialect, self).__init__(*args, **kw)

    def _get_server_version_info(self, connection):
        # get database server version info explicitly over the wire
        # to avoid proxy servers like MaxScale getting in the
        # way with their own values, see #4205
        dbapi_con = connection.connection
        cursor = dbapi_con.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        val = cursor.fetchone()[0]
        cursor.close()
        if isinstance(val, bytes):
            val = val.decode()

        return self._parse_server_version(val)

    def _parse_server_version(self, val):
        server_version_info = tuple()
        m = re.match(r"(\d+)\.?(\d+)?(?:\.(\d+))?(?:\.\d+)?(?:[-\s])?(?P<commit>.*)?", val)
        if m is not None:
            server_version_info = tuple([int(x) for x in m.group(1, 2, 3) if x is not None])

        # setting it here to help w the test suite
        self.server_version_info = server_version_info
        return server_version_info

    @util.memoized_property
    def _tabledef_parser(self):
        """return the MySQLTableDefinitionParser, generate if needed.

        The deferred creation ensures that the dialect has
        retrieved server version information first.

        """
        preparer = self.identifier_preparer
        return _reflection.StarRocksTableDefinitionParser(self, preparer)

    def _show_table_indexes(
        self, connection, table, charset=None, full_name=None
    ):
        """Run SHOW INDEX FROM for a ``Table``."""

        if full_name is None:
            full_name = self.identifier_preparer.format_table(table)
        st = "SHOW INDEX FROM %s" % full_name

        rp = None
        try:
            rp = connection.execution_options(
                skip_user_error_events=True
            ).exec_driver_sql(st)
        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) == 1146:
                raise exc.NoSuchTableError(full_name) from e
            else:
                raise
        index_results = self._compat_fetchall(rp, charset=charset)
        return index_results

    # This was to get indexes, but the indexes are created just Starrocks takes a while to admit to them
    # @reflection.cache
    # def _setup_parser(self, connection, table_name, schema=None, **kw):
    #     charset = self._connection_charset
    #     parser = self._tabledef_parser
    #     full_name = ".".join(
    #         self.identifier_preparer._quote_free_identifiers(
    #             schema, table_name
    #         )
    #     )
    #     sql = self._show_create_table(
    #         connection, None, charset, full_name=full_name
    #     )
    #     indexes = []
    #     if parser._check_view(sql):
    #         # Adapt views to something table-like.
    #         columns = self._describe_table(
    #             connection, None, charset, full_name=full_name
    #         )
    #         sql = parser._describe_to_create(table_name, columns)
    #     else:
    #         indexes = self._show_table_indexes(
    #             connection, None, charset, full_name=full_name
    #         )
    #     return parser.parse(sql, indexes, charset)

    # @reflection.cache
    # def get_table_comment(self, connection, table_name, schema=None, **kw):
    #     parsed_state = self._parsed_state_or_create(
    #         connection, table_name, schema, **kw
    #     )
    #     return {
    #         "text": parsed_state.table_options.get(
    #             "%s_comment" % self.name, None
    #         )
    #     }

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):

        parsed_state = self._parsed_state_or_create(
            connection, table_name, schema, **kw
        )

        indexes = []

        for spec in parsed_state.keys:
            dialect_options = {}
            unique = False
            flavor = spec["type"]
            if flavor == "PRIMARY":
                continue
            if flavor == "DUPLICATE":
                continue
            if flavor == "UNIQUE":
                unique = True
            elif flavor in ("FULLTEXT", "SPATIAL"):
                dialect_options["%s_prefix" % self.name] = flavor
            elif flavor is None:
                pass
            else:
                self.logger.info(
                    "Converting unknown KEY type %s to a plain KEY", flavor
                )
                pass

            if spec["parser"]:
                dialect_options["%s_with_parser" % (self.name)] = spec[
                    "parser"
                ]

            index_d = {}

            index_d["name"] = spec["name"]
            index_d["column_names"] = [s[0] for s in spec["columns"]]
            mysql_length = {
                s[0]: s[1] for s in spec["columns"] if s[1] is not None
            }
            if mysql_length:
                dialect_options["%s_length" % self.name] = mysql_length

            index_d["unique"] = unique
            if flavor:
                index_d["type"] = flavor

            if dialect_options:
                index_d["dialect_options"] = dialect_options

            indexes.append(index_d)
        return indexes

    def has_table(self, connection, table_name, schema=None, **kw):
        try:
            return super().has_table(connection, table_name, schema, **kw)
        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) in (5501, 5502):
                return False
            raise
