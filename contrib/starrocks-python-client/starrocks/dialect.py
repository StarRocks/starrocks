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
import logging
import re
from textwrap import dedent
from typing import Any, Dict, Final, List, Optional, Tuple, Union

from alembic.ddl.base import alter_table, format_column_name, format_server_default, format_table_name
from alembic.ddl.mysql import (
    MySQLAlterDefault,
    MySQLChangeColumn,
    MySQLModifyColumn,
)
from alembic.operations.ops import AlterColumnOp
from alembic.util.sqla_compat import compiles
from sqlalchemy import (
    Column,
    Table,
    exc,
    log,
    schema as sa_schema,
    text,
    types as sqltypes,
    util,
)
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.dialects.mysql.base import (
    MySQLCompiler,
    MySQLDDLCompiler,
    MySQLIdentifierPreparer,
    MySQLTypeCompiler,
    _DecodingRow,
    colspecs as base_colspecs,
)
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
from sqlalchemy.engine import reflection
#from sqlalchemy.engine.interfaces import ReflectedTableComment
from sqlalchemy.engine.row import Row
from sqlalchemy.sql import sqltypes, bindparam
from sqlalchemy.sql.expression import Delete, Select, Update, TableClause

from starrocks.common import utils
from starrocks.common.defaults import ReflectionMVDefaults, ReflectionTableDefaults, ReflectionViewDefaults
from starrocks.common.params import (
    ColumnAggInfoKey,
    ColumnAggInfoKeyWithPrefix,
    DialectName,
    TableInfoKey,
    TableInfoKeyWithPrefix,
    TableKind,
    TableObjectInfoKey,
)
from starrocks.common.types import ColumnAggType, SystemRunMode, TableEngine, TableType
from starrocks.common.utils import TableAttributeNormalizer
from starrocks.sql.schema import View

from . import reflection as _reflection
from .datatype import (
    ARRAY,
    BIGINT,
    BINARY,
    BITMAP,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    DECIMAL,
    DOUBLE,
    FLOAT,
    HLL,
    INTEGER,
    JSON,
    LARGEINT,
    MAP,
    PERCENTILE,
    SMALLINT,
    STRING,
    STRUCT,
    TINYINT,
    VARBINARY,
    VARCHAR,
)
from .engine.interfaces import ReflectedMVState, ReflectedState, ReflectedViewState
from .reflection import StarRocksInspector, StarRocksTableDefinitionParser
from .sql.ddl import (
    AlterMaterializedView,
    AlterTableDistribution,
    AlterTableEngine,
    AlterTableKey,
    AlterTableOrder,
    AlterTablePartition,
    AlterTableProperties,
    AlterView,
    CreateMaterializedView,
    CreateView,
    DropMaterializedView,
    DropView,
)


# Register the compiler methods
# The @compiles decorator is the public API for registering new SQL constructs.
# However, we are now using the internal `__visit_name__` attribute on the
# DDLElement classes themselves to hook into the visitor pattern, which is
# consistent with how SQLAlchemy's own constructs are implemented.
# compiles(CreateView)(StarRocksDDLCompiler.visit_create_view)
# compiles(DropView)(StarRocksDDLCompiler.visit_drop_view)
# compiles(CreateMaterializedView)(StarRocksDDLCompiler.visit_create_materialized_view)
# compiles(DropMaterializedView)(StarRocksDDLCompiler.visit_drop_materialized_view)


##############################################################################################
# NOTES - INCOMPLETE/UNFINISHED
# There are a number of items in here marked as ToDo
# In terms of table creation, the Partition, Distribution and OrderBy clauses need to be addressed from table options
# Tests `test_has_index` and `test_has_index_schema` are failing, this is because the CREATE INDEX statement appears to
# work async and only when it's finished does it appear in the table definition
# Other tests are failing, need to fix or figure out how to suppress
# Review some skipped test suite requirements
#
#
#
##############################################################################################

logger = logging.getLogger(__name__)

# starrocks supported data types
ischema_names = {
    # === Boolean ===
    "boolean": BOOLEAN,
    # === Integer ===
    "tinyint": TINYINT,
    "smallint": SMALLINT,
    "int": INTEGER,
    "integer": INTEGER,
    "bigint": BIGINT,
    "largeint": LARGEINT,
    # === Floating-point ===
    "float": FLOAT,
    "double": DOUBLE,
    # === Fixed-precision ===
    "decimal": DECIMAL,
    "decimal32": DECIMAL,
    "decimal64": DECIMAL,
    "decimal128": DECIMAL,
    # === String ===
    "varchar": VARCHAR,
    "char": CHAR,
    "string": STRING,
    "json": JSON,
    # === Date and time ===
    "date": DATE,
    "datetime": DATETIME,
    "timestamp": DATETIME,
    # == binary ==
    "binary": BINARY,
    "varbinary": VARBINARY,
    # === Structural ===
    "array": ARRAY,
    "map": MAP,
    "struct": STRUCT,
    "hll": HLL,
    "percentile": PERCENTILE,
    "bitmap": BITMAP,
}

colspecs = base_colspecs | {
    sqltypes.Date: DATE,
    sqltypes.DateTime: DATETIME,
    sqltypes.DECIMAL: DECIMAL,
}

class StarRocksTypeCompiler(MySQLTypeCompiler):
    """
    Compile a datatype to StarRocks' SQL type string.
    For special types only, use the default implementation in MySQLTypeCompiler for other types.

    NOTE: StarRocks will ignore the length of Integer types, so we need to care about the
    comparison of Integer types.
    NOTE: StarRocks does not support scale of Float types.
    """

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_FLOAT(self, type_, **kw):
        return "FLOAT"

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_LARGEINT(self, type_, **kw):
        if getattr(type_, "display_width", None) is not None:
            return f"LARGEINT({type_.display_width})"
        return "LARGEINT"

    def visit_NVARCHAR(self, type_, **kw):
        return self.visit_VARCHAR(type_, **kw)

    def visit_STRING(self, type_, **kw):
        return "STRING"

    def visit_VARCHAR(self, type_, **kw):
        # StarRocks supports unbounded strings via STRING. When no length is
        # provided (e.g. from SQLAlchemy String() without length), render
        # STRING instead of requiring a VARCHAR length.
        if getattr(type_, "length", None) is None or type_.length == 65533:
            return "STRING"
        return f"VARCHAR({type_.length})"

    def visit_BINARY(self, type_, **kw):
        return "BINARY"

    def visit_VARBINARY(self, type_, **kw):
        return "VARBINARY"

    def visit_ARRAY(self, type_: ARRAY, **kw):
        """Compiles the ARRAY type into the correct StarRocks syntax."""
        # logger.debug("visit_ARRAY: type_: %r, kw: %s", type_, kw)
        inner_type_sql = self.process(type_.item_type, **kw)
        return f"ARRAY<{inner_type_sql}>"

    def visit_MAP(self, type_: MAP, **kw):
        # logger.debug("visit_MAP: type_: %r, kw: %s", type_, kw)
        key_type_sql = self.process(type_.key_type, **kw)
        value_type_sql = self.process(type_.value_type, **kw)
        return f"MAP<{key_type_sql}, {value_type_sql}>"

    def visit_STRUCT(self, type_: STRUCT, **kw):
        # logger.debug("visit_STRUCT: type_: %r, kw: %s", type_, kw)
        fields_sql = []
        for name, type_ in type_.field_tuples:
            name_sql = self.process(name, **kw) if isinstance(name, sqltypes.TypeEngine) else name
            type_sql = self.process(type_, **kw)
            fields_sql.append(f"{name_sql} {type_sql}")
        return f"STRUCT<{', '.join(fields_sql)}>"

    def visit_HLL(self, type_, **kw):
        return "HLL"

    def visit_BITMAP(self, type_, **kw):
        return "BITMAP"

    def visit_BLOB(self, type_, **kw):
        return "BINARY"


class StarRocksSQLCompiler(MySQLCompiler):
    def visit_delete(self, delete_stmt: Delete, **kw: Any) -> str:
        result: str = super().visit_delete(delete_stmt, **kw)
        compile_state: Any = delete_stmt._compile_state_factory(
            delete_stmt, self, **kw
        )
        delete_stmt = compile_state.statement
        table: str = self.delete_table_clause(
            delete_stmt, delete_stmt.table, False
        )
        if not delete_stmt._where_criteria:
            return "TRUNCATE TABLE " + table
        return result

    def limit_clause(self, select: Select, **kw: Any) -> str:
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

    def visit_typeclause(
        self,
        typeclause,
        type_=None,
        **kw: Any,
    ):
        if type_ is None:
            type_ = typeclause.type.dialect_impl(self.dialect)
        if isinstance(type_, sqltypes.Boolean):
            return self.dialect.type_compiler_instance.process(type_)
        return super().visit_typeclause(typeclause, type_, **kw)

    def visit_insert_into_files(self, insert_into, **kw):
        return (
            f"INSERT INTO {self.process(insert_into.target, **kw)}\n"
            f" {self.process(insert_into.from_, **kw)}"
        )

    def visit_files_target(self, files, **kw):
        target_items = []
        target_items.append(self.process(files.storage, **kw))
        target_items.append(self.process(files.format, **kw))
        if files.options is not None:
            target_items.append(self.process(files.options, **kw))
        files_str = ",\n".join(target_items)
        return f"FILES(\n{files_str}\n)"

    def _key_value_format(self, items: dict):
        return ',\n'.join([
            f'{repr(k)} = {repr(v)}'
            for k, v in items.items()
        ])

    def visit_cloud_storage(self, storage, **kw):
        return self._key_value_format(storage.options)

    def visit_files_format(self, files_format, **kw):
        return self._key_value_format(files_format.options)

    def visit_files_options(self, files_options, **kw):
        return self._key_value_format(files_options.options)

    def visit_insert_from_files(self, insert_from, **kw):
        target = (
            self.preparer.format_table(insert_from.target)
            if isinstance(insert_from.target, (TableClause,))
            else self.process(insert_from.target, **kw)
        )

        if isinstance(insert_from.columns, str):
            select_str = insert_from.columns
        else:
            select_str = ",".join(
                [
                    self.process(col, **kw)
                    for col in insert_from.columns
                ]
            )

        return (
            f"INSERT INTO {target}\n"
            f" SELECT {select_str}\n"
            f" FROM {self.process(insert_from.from_, **kw)}"
        )

    def visit_files_source(self, files, **kw):
        source_items = []
        source_items.append(self.process(files.storage, **kw))
        source_items.append(self.process(files.format, **kw))
        if files.options is not None:
            source_items.append(self.process(files.options, **kw))
        files_str = ",\n".join(source_items)
        return f"FILES(\n{files_str}\n)"

class StarRocksDDLCompiler(MySQLDDLCompiler):
    def __init__(self, *args, **kwargs):
        self.indent: Final[str] = "    "
        super().__init__(*args, **kwargs)

    def visit_alter_column(self, alter: AlterColumnOp, **kw):
        """
        Compile ALTER COLUMN statements.

        StarRocks does not support altering the aggregation type of a column.
        This method checks for such attempts and raises a NotImplementedError.
        """
        # The `starrocks_agg_type` is added to kwargs only when a change is detected
        # by the comparator. See `compare_starrocks_column_agg_type`.
        if ColumnAggInfoKeyWithPrefix.AGG_TYPE in alter.kwargs:
            raise NotImplementedError(
                f"StarRocks does not support changing the aggregation type of column '{alter.column_name}'."
            )
        return super().visit_alter_column(alter, **kw)

    def visit_create_table(self, create: sa_schema.CreateTable, **kw: Any) -> str:
        table: sa_schema.Table = create.element
        table_kind: str = table.info.get(TableObjectInfoKey.TABLE_KIND, TableKind.TABLE).upper()

        if table_kind == TableKind.VIEW:
            return self._compile_create_view_from_table(table, create, **kw)
        elif table_kind == TableKind.MATERIALIZED_VIEW:
            return self._compile_create_mv_from_table(table, create, **kw)
        else:
            return self._compile_create_table_original(table, create, **kw)

    def _compile_create_table_original(
            self,
            table: sa_schema.Table,
            create: sa_schema.CreateTable,
            **kw: Any) -> str:
        preparer = self.preparer

        text = "\nCREATE "
        if table._prefixes:
            text += " ".join(table._prefixes) + " "

        text += "TABLE "
        if create.if_not_exists:
            text += "IF NOT EXISTS "

        text += preparer.format_table(table) + " "

        # StarRocks-specific validation for all key types
        self._validate_key_definitions(table)

        if create_table_suffix := self.create_table_suffix(table):
            text += create_table_suffix + " "

        text += "(\n"

        column_text_list = list()
        # if only one primary key, specify it along with the column
        primary_keys: List[str] = list()
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(create_column)
                # logger.debug("column desc for column: %r is '%s'", column.name, processed)
                if processed is not None:
                    column_text_list.append(f"{self.indent}{processed}")
                if column.primary_key:
                    primary_keys.append(column.name)
            except exc.CompileError as ce:
                raise exc.CompileError(
                    "(in table %r, column %r): %s"
                    % (self._get_simple_full_table_name(table.name, table.schema), column.name, ce.args[0])
                ) from ce
        text += ", \n".join(column_text_list)

        # N.B. Primary Key is specified in post_create_table
        #  Indexes are created by SQLA after the creation of the table using CREATE INDEX
        # const = self.create_table_constraints(
        #     table,
        #     _include_foreign_key_constraints=create.include_foreign_key_constraints,  # noqa
        # )
        # if const:
        #     text += separator + self.indent + const

        text += "\n)\n%s\n" % self.post_create_table(table, primary_keys=primary_keys, **kw)
        logger.debug("create table text for table: %s, schema: %s, text: %s", table.name, table.schema, text)

        return text

    def _get_simple_full_table_name(self, table_name: str, schema: Optional[str] = None) -> str:
        """Directly get the full table name without adding any quotes."""
        return f"{schema}.{table_name}" if schema else table_name

    def _validate_key_definitions(self, table: sa_schema.Table) -> None:
        """
        Validates key definitions for all StarRocks table types.

        This performs two checks:
        1. (All key types) Ensures that all columns specified in the table's key
           (e.g., `starrocks_primary_key`) are actually defined in the table.
        2. (AGGREGATE KEY only) Enforces StarRocks' strict column ordering rules.

        Args:
            table: The SQLAlchemy Table object to validate.

        Raises:
            CompileError: If any validation rule is violated.
        """
        key_kwarg_map = TableInfoKeyWithPrefix.KEY_KWARG_MAP

        key_str = None
        key_type = None

        for kwarg, name in key_kwarg_map.items():
            if kwarg in table.kwargs:
                key_str = table.kwargs[kwarg]
                key_type = name
                break  # Found the key, no need to check for others

        if not key_str:
            return  # No key defined, nothing to validate

        key_column_names = [k.strip() for k in key_str.split(',')]
        table_column_names = {c.name for c in table.columns}

        # 1. Generic Check: Ensure all key columns exist in the table definition.
        missing_keys = set(key_column_names) - table_column_names
        if missing_keys:
            raise exc.CompileError(
                f"Columns specified in {key_type} ('{key_str}') not found in table: {', '.join(missing_keys)}"
            )

        # 2. Specific Check: For AGGREGATE KEY tables, validate column order.
        if key_type == TableType.AGGREGATE_KEY:
            self._validate_aggregate_key_order(table, key_column_names)

    def _validate_aggregate_key_order(self, table: sa_schema.Table, key_column_names: List[str]) -> None:
        """
        Validates column order for AGGREGATE KEY tables.

        In StarRocks, for an AGGREGATE KEY table:
        1. All key columns must be defined before any value (aggregate) columns.
        2. The order of key columns in the table definition must match the order
           specified in the `starrocks_aggregate_key` argument.

        Args:
            table: The SQLAlchemy Table object to validate.
            key_column_names: The list of key column names from the kwarg.
        """
        key_cols_from_table = []

        # Separate table columns into key and value lists
        for col in table.columns:
            # Note: value columns are any columns not in the key list.
            if col.name in key_column_names:
                key_cols_from_table.append(col.name)

        # Rule 2: The order of key columns in the table definition must match
        if key_cols_from_table != key_column_names:
            raise exc.CompileError(
                "For AGGREGATE KEY tables, the order of key columns in the table definition "
                f"must match the order in starrocks_aggregate_key. "
                f"Expected order: {key_column_names}, Actual order: {key_cols_from_table}"
            )

        # Rule 1: All key columns must be defined before any value columns.
        last_key_col_index = -1
        col_list = list(table.columns)
        for i, col in enumerate(col_list):
            if col.name in key_column_names:
                last_key_col_index = i

        for i in range(last_key_col_index):
            if col_list[i].name not in key_column_names:
                raise exc.CompileError(
                    "For AGGREGATE KEY tables, all key columns must be defined before any value columns. "
                    f"Value column '{col_list[i].name}' appears before "
                    f"key column '{col_list[last_key_col_index].name}'."
                )

    def post_create_table(self, table: sa_schema.Table, **kw: Any) -> str:
        """
        Appends StarRocks-specific clauses to a CREATE TABLE or CREATE MATERIALIZED VIEW statement.

        This method compiles StarRocks-specific table options provided as `starrocks_`
        kwargs on the `Table` object. It is responsible for constructing clauses
        that appear after the column definitions, such as:
        - `ENGINE`
        - `KEY`: `PRIMARY KEY`, `DUPLICATE KEY`, `AGGREGATE KEY`, `UNIQUE KEY`
        - `COMMENT`
        - `PARTITION BY`
        - `DISTRIBUTED BY`
        - `ORDER BY`
        - `PROPERTIES`
        - `REFRESH`  // for MV
        This method is called after the main CREATE TABLE statement is formed and
        appends clauses like PARTITION BY, DISTRIBUTED BY, PROPERTIES, etc.

        Args:
            table: The `sqlalchemy.schema.Table` object being compiled.
            **kw: Additional keyword arguments from the compiler.
                primary_keys: The list of primary key column names. We need to check it with a table's KEY attribute.

        Returns:
            A string containing all the compiled table-level DDL clauses.
        """
        import warnings
        table_opts: List[str] = []

        opts = utils.extract_dialect_options_as_case_insensitive(table)
        logger.debug("table original opts for table: %s, schema: %s, opts: %r", table.name, table.schema, opts)

        # ENGINE
        engine = opts.get(TableInfoKey.ENGINE)
        if engine and engine.upper() != TableEngine.OLAP:
            table_opts.append(f'ENGINE={engine}')

        # Key / Table Type (Primary key, Duplicate key, Aggregate key, Unique key)
        primary_keys = kw.get("primary_keys", [])
        if key_desc := self._get_create_table_key_desc(primary_keys, opts):
            table_opts.append(key_desc)

        # Comment
        table_comment = table.comment
        if table_comment is None:
            # For backward compatibility, check for 'starrocks_comment'
            if starrocks_comment := opts.get(TableInfoKey.COMMENT):
                warnings.warn(
                    f"The 'starrocks_comment' dialect argument is deprecated for table '{table.name}'. "
                    "Please use the standard 'comment' argument on the Table object instead.",
                    DeprecationWarning,
                    stacklevel=3,
                )
                table_comment = starrocks_comment

        if table_comment is not None:
            comment = self.sql_compiler.render_literal_value(table_comment, sqltypes.String())
            table_opts.append(f"COMMENT {comment}")

        # Partition
        if partition_by := opts.get(TableInfoKey.PARTITION_BY):
            table_opts.append(f'PARTITION BY {partition_by}')

        # Distribution
        distributed_by = opts.get(TableInfoKey.DISTRIBUTED_BY)
        if distributed_by is None:
            # For backward compatibility, check for 'distribution'
            if starrocks_distribution := opts.get("DISTRIBUTION"):
                warnings.warn(
                    f"The 'starrocks_distribution' dialect argument is deprecated for table '{table.name}'. "
                    "Please use 'starrocks_distributed_by' instead.",
                    DeprecationWarning,
                    stacklevel=3,
                )
                distributed_by = starrocks_distribution

        if distributed_by:
            table_opts.append(f'DISTRIBUTED BY {distributed_by}')

        # Order By
        if order_by := opts.get(TableInfoKey.ORDER_BY):
            order_by = TableAttributeNormalizer.remove_outer_parentheses(order_by)
            table_opts.append(f'ORDER BY ({order_by})')

        # Handle MV-specific REFRESH clause
        if refresh := opts.get(TableInfoKey.REFRESH):
            table_opts.append(f"REFRESH {str(refresh)}")

        # Properties
        properties = opts.get(TableInfoKey.PROPERTIES)
        props_dict = {}
        if properties is not None:
            if isinstance(properties, (dict, list, tuple)):
                props_dict = dict(properties)
            else:
                raise exc.CompileError(
                    f"Unsupported type for PROPERTIES: {type(properties)}"
                )
        # if self.dialect.test_replication_num and "replication_num" not in props_dict:
        #     props_dict.setdefault("replication_num", str(self.dialect.test_replication_num))
        if props_dict:
            props = ",\n".join([f'{self.indent}"{k}"="{v}"' for k, v in props_dict.items()])
            table_opts.append(f"PROPERTIES(\n{props}\n)")

        logger.debug("table opts for table: %s, schema: %s, processed opts: %r", table.name, table.schema, table_opts)

        return "\n".join(table_opts)

    def _get_create_table_key_desc(self, primary_keys: List[str], opts: Dict[str, Any]) -> Optional[str]:
        """Visit create table key description.
        Args:
            primary_keys: The list of primary key column names. We need to check it with a talbe's KEY attribute.
            opts: The table options.

        Returns:
            The table key description. like "PRIMARY KEY (id, name)"
        """
        # Key / Table Type (Primary key, Duplicate key, Aggregate key, Unique key)
        key_type = None
        key_desc = None
        for tbl_kind_key_str, table_type in TableInfoKey.KEY_KWARG_MAP.items():
            kwarg_upper = tbl_kind_key_str.upper()
            if kwarg_upper in opts:
                if key_type:
                    raise exc.CompileError(f"Multiple key types found: {tbl_kind_key_str}, first_key_type: {key_type}")
                key_type = table_type
                key_columns_str: str = TableAttributeNormalizer.remove_outer_parentheses(opts[kwarg_upper])
                logger.debug("get table key info: key_type: %s, key_columns: %s", key_type, key_columns_str)
                # check if the key columns are valid
                if primary_keys:
                    key_columns_set = set(k.strip().strip('`') for k in key_columns_str.split(','))
                    primary_keys_set = set(k.strip().strip('`') for k in primary_keys)
                    logger.debug("check constraint keys. primary_key_set: %s, key_columns_set: [%s]", primary_keys, key_columns_str)
                    if primary_keys_set != key_columns_set:
                        raise exc.CompileError(f"Primary key columns doesn't equal to the table KEY columns. "
                                            f"primary_keys: {primary_keys}, SR's key_columns: ({key_columns_str})")
                key_desc = f"{key_type} ({key_columns_str})"
        return key_desc


    def _has_column_info_key(self, column: sa_schema.Column, key: str) -> bool:
        """Check if column has a specific info key (case-insensitive)."""
        return any(k.lower() == key.lower() for k in column.info.keys())

    def _get_column_info_value(self, column: sa_schema.Column, key: str, default: Any = None) -> Any:
        """Get column info value by key (case-insensitive)."""
        for k, v in column.info.items():
            if k.lower() == key.lower():
                return v
        return default

    def get_column_specification(self, column: sa_schema.Column, **kw: Any) -> str:
        """Builds column DDL for StarRocks, handling StarRocks-specific features.

        This method extends the base MySQL compiler to support:
        - **KEY specifier**: For AGGREGATE KEY tables, key columns can be marked
          with `info={'starrocks_is_agg_key': True}`. The compiler validates that
          a column is not both a key and an aggregate.
        - **Aggregate Functions**: For AGGREGATE KEY tables, value columns can have
          an aggregate function (e.g., 'SUM', 'REPLACE') specified via the
          `info={'starrocks_agg': '...'}` dictionary on a Column.
        - **AUTO_INCREMENT**: Automatically renders `AUTO_INCREMENT` for columns
          with `autoincrement=True`. It also ensures these columns are `BIGINT`
          and `NOT NULL` as required by StarRocks.
        - **Generated Columns**: Compiles `sqlalchemy.Computed` constructs into
          StarRocks' `AS (...)` syntax.

        Args:
            column: The `sqlalchemy.schema.Column` object to process.
            **kw: Additional keyword arguments from the compiler.

        Returns:
            The full DDL string for the column definition.
        """
        # Name, type, others of a column for the output colspec
        _, idx_type = 0, 1

        # Get name and type first
        colspec: List[str] = [
            self.preparer.format_column(column),
            self.dialect.type_compiler.process(
                column.type, type_expression=column
            ),
        ]

        # Get and set column-level aggregate information
        if agg_info := self._get_column_agg_info(column):
            # logger.debug("agg info for column: %r is '%s'", column.name, agg_info)
            colspec.append(agg_info)

        # NULL or NOT NULL.
        if not column.nullable:
            colspec.append("NOT NULL")
        # else: omit explicit NULL (default)

        # AUTO_INCREMENT or default value or computed column
        # AUTO_INCREMENT columns must be NOT NULL
        if (
            column.table is not None
            and (
                # NOTE: we should not use column.table._autoincrement_column here.
                # because the column will be auto-incremented when it's set with `primary_key=True`.
                # column is column.table._autoincrement_column or
                column.autoincrement is True
            )
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
            colspec[idx_type] = "BIGINT"  # AUTO_INCREMENT column must be BIGINT
            if column.nullable:
                colspec.append("NOT NULL")
            colspec.append("AUTO_INCREMENT")
        else:
            default = self.get_column_default_string(column)
            if default == "AUTO_INCREMENT":
                colspec[1] = "BIGINT"
                if column.nullable:
                    colspec.append("NOT NULL")
                colspec.append("AUTO_INCREMENT")

            elif default is not None:
                colspec.append("DEFAULT " + default)

        # Computed
        if column.computed is not None:
            colspec.append(self.process(column.computed))

        # Comment
        if column.comment is not None:
            literal = self.sql_compiler.render_literal_value(
                column.comment, sqltypes.String()
            )
            colspec.append("COMMENT " + literal)

        column_spec_str = " ".join(colspec)
        logger.debug("column spec for column: %s is [%s]", column.name, column_spec_str)
        return column_spec_str

    def get_column_spec_for_alter_column(self,
            name: str,
            nullable: Optional[bool],
            default: Optional[Any],
            type_: sqltypes.TypeEngine,
            autoincrement: Optional[bool],
            comment: Optional[Any],
             **kw: Any) -> str:
        """Builds ALTER COLUMN DDL for StarRocks, handling StarRocks-specific features.

        This method extends the base MySQL compiler to support:
        - **KEY specifier**: For AGGREGATE KEY tables, key columns can be marked
          with `info={'starrocks_is_agg_key': True}`. The compiler validates that
          a column is not both a key and an aggregate.
        - **Aggregate Functions**: For AGGREGATE KEY tables, value columns can have
          an aggregate function (e.g., 'SUM', 'REPLACE') specified via the
          `info={'starrocks_agg': '...'}` dictionary on a Column.
        - **AUTO_INCREMENT**: Automatically renders `AUTO_INCREMENT` for columns
          with `autoincrement=True`. It also ensures these columns are `BIGINT`
          and `NOT NULL` as required by StarRocks.
        - **Generated Columns**: Compiles `sqlalchemy.Computed` constructs into
          StarRocks' `AS (...)` syntax.

        Args:
            column: The `sqlalchemy.schema.Column` object to process.
            **kw: Additional keyword arguments from the compiler.
                - aggregate info should be stored here. But, it's not passed now.

        Returns:
            The DDL string for the alter column definition, without name.
        """
        colspec: List[str] = []

        # type if set
        if type_:
            type_: str = self.dialect.type_compiler.process(type_)
            colspec.append(type_)

            # Get and set column-level aggregate information for aggregate key tables.
            # TODO: But currently it's not supported. we need to implement it.
            # if agg_info := self._get_column_agg_info(column):
            #     colspec.append(agg_info)

        # NULL or NOT NULL.
        if nullable is not None:
            colspec.append("NULL" if nullable else "NOT NULL")

        if autoincrement:
            colspec.append("AUTO_INCREMENT")
        # DEFAULT: include if provided (even if 0 or empty string), but not when False
        elif default is not False and default is not None:
            default = format_server_default(self, default)
            if default == "AUTO_INCREMENT":
                colspec.append("AUTO_INCREMENT")
            elif default is not None:
                colspec.append("DEFAULT " + default)

        # Computed is not supported in ALTER COLUMN
        # if computed is not None:
        #     colspec.append(self.process(computed))

        # Comment
        if comment:
            literal = self.sql_compiler.render_literal_value(
                comment, sqltypes.String()
            )
            colspec.append("COMMENT " + literal)

        return " ".join(colspec)

    def _get_table_agg_info(self, table: sa_schema.Table) -> Optional[bool]:
        """
        Get aggregate information for a table from its dialect_option.
        It will use cache to store, in `table.info`.
        Args:
            table: The `sqlalchemy.schema.Table` object to process.

        Returns:
            whether it's AGGREGATE table or not (None if unknown).
        """
        # In CREATE TABLE, table.dialect_options will contain StarRocks options.
        # In ALTER TABLE ADD/MODIFY COLUMN, Alembic/SQLAlchemy often provides a lightweight
        # Table placeholder without dialect options; in that case we treat the table type
        # as unknown and avoid raising on presence of KEY/agg markers so users can specify
        # them explicitly in ADD/MODIFY statements.
        if TableInfoKeyWithPrefix.AGGREGATE_KEY in table.info:
            is_agg_table: Optional[bool] = table.info.get(TableInfoKeyWithPrefix.AGGREGATE_KEY)
            # logger.debug("Cached is_agg_table for table: %s is '%s'", table.name, is_agg_table)
        else:
            # logger.debug("is_agg_table is not stored in cache for table: %s.", table.name)
            try:
                # Remove items with value being None, because the `defaults` has all the keys.
                dialect_options = utils.extract_dialect_options_as_case_insensitive(table)
                logger.debug("Extract dialect options to get aggr info fot table: %s, options=%s", table.name, dialect_options)
                if dialect_options:
                    is_agg_table = TableInfoKey.AGGREGATE_KEY in dialect_options
                    table.info[TableInfoKeyWithPrefix.AGGREGATE_KEY] = is_agg_table
                    # logger.debug("Cache is_agg_table for table: %s to '%s'", table.name, is_agg_table)
                else:
                    # Unknown table options in ALTER context; leave as None
                    is_agg_table = None
            except Exception:
                # No dialect options available; leave as unknown
                is_agg_table = None

        return is_agg_table

    def _get_column_agg_info(self, column: sa_schema.Column) -> Union[str, None]:
        """Get aggregate information for a column.
        Args:
            column: The `sqlalchemy.schema.Column` object to process.

        Returns:
            The aggregate information for the column (`KEY` or `agg_type`, such as `SUM`, or None).
        """
        is_agg_table = self._get_table_agg_info(column.table)

        # check agg key/type in the column
        opt_dict = utils.extract_dialect_options_as_case_insensitive(column)
        has_is_agg_key = ColumnAggInfoKey.IS_AGG_KEY in opt_dict
        has_agg_type = ColumnAggInfoKey.AGG_TYPE in opt_dict

        # If we can determine the table is NOT AGGREGATE KEY, disallow column-level
        # KEY/agg markers. If unknown (ALTER context), allow rendering markers.
        if is_agg_table is False and (has_is_agg_key or has_agg_type):
            raise exc.CompileError(
                "Column-level KEY/aggregate markers are only valid for AGGREGATE KEY tables; "
                "declare starrocks_aggregate_key at table level first."
            )

        # Disallow specifying both KEY and agg_type simultaneously.
        if has_is_agg_key and has_agg_type:
            raise exc.CompileError(
                f"Column '{column.name}' cannot be both KEY and aggregated "
                f"(has {ColumnAggInfoKey.AGG_TYPE})."
            )

        if has_is_agg_key:
            return ColumnAggType.KEY
        elif has_agg_type:
            agg_val = str(opt_dict[ColumnAggInfoKey.AGG_TYPE])
            if agg_val.upper() not in ColumnAggType.ALLOWED_ITEMS:
                raise exc.CompileError(
                    f"Unsupported aggregate type for column '{column.name}': {agg_val}"
                )
            return agg_val
        return None

    def visit_computed_column(self, generated: sa_schema.Computed, **kw: Any) -> str:
        text = "AS (%s)" % self.sql_compiler.process(
            generated.sqltext, include_table=False, literal_binds=True
        )
        return text

    def visit_set_table_comment(self, create: sa_schema.SetTableComment, **kw: Any) -> str:
        return "ALTER TABLE %s COMMENT=%s" % (
            self.preparer.format_table(create.element),
            self.sql_compiler.render_literal_value(
                create.element.comment, sqltypes.String()
            ),
        )

    def visit_drop_table_comment(self, drop: sa_schema.DropTableComment, **kw: Any) -> str:
        return "ALTER TABLE %s COMMENT=''" % (
            self.preparer.format_table(drop.element)
        )

    def visit_alter_view(self, alter: AlterView, **kw: Any) -> str:
        view = alter.element
        text = f"ALTER VIEW {self.preparer.format_table(view)}\n"

        if view.columns:
            text += self._get_view_column_clauses(view)

        # StarRocks does not support altering COMMENT or SECURITY via ALTER VIEW.
        # TODO: we can optimize it when StarRocks supports it in the future
        # Only redefine the SELECT statement.
        text += f"AS\n{view.definition}"

        logger.debug("Compiled SQL for AlterView: \n%s", text)
        return text

    def _get_view_column_clauses(self, view: View) -> str:
        """
        Helper method to format column clauses for a CREATE VIEW statement.

        StarRocks VIEW columns only support name and comment.

        Args:
            view: View object with columns

        Returns:
            Formatted column clauses string, e.g., " (\n    id,\n    name COMMENT 'User name'\n)"
        """
        column_clauses: List[str] = []
        for col in view.columns:
            col_name = self.preparer.quote(col.name)
            if col.comment:
                comment = self.sql_compiler.render_literal_value(
                    col.comment, sqltypes.String()
                )
                column_clauses.append(f'{self.indent}{col_name} COMMENT {comment}')
            else:
                column_clauses.append(f'{self.indent}{col_name}')
        return " (\n%s\n)" % ",\n".join(column_clauses)

    def _compile_create_view_from_table(self, table: Table, create: CreateView, **kw: Any) -> str:
        """
        Helper to compile CREATE VIEW from a CreateView DDL element or CreateTable for View.

        Args:
            create: CreateView DDL element or CreateTable element where table_kind='VIEW'
                the table may be not a View object, so we treat it as a normal Table.
            **kw: Additional compilation kwargs

        Returns:
            Compiled SQL string for CREATE VIEW statement
        """
        preparer = self.preparer

        view_name = preparer.format_table(table)
        definition = table.info.get(TableObjectInfoKey.DEFINITION)
        if not definition:
            raise exc.CompileError("View definition is required")
        security = utils.get_dialect_option(table, TableInfoKey.SECURITY)

        or_replace_clause = "OR REPLACE " if create.or_replace else ""
        if_not_exists_clause = "IF NOT EXISTS " if create.if_not_exists else ""

        text = f"CREATE {or_replace_clause}VIEW {if_not_exists_clause}{view_name}"

        # Add column definitions if present (only name and comment are supported)
        if table.columns:
            text += self._get_view_column_clauses(table)

        if table.comment:
            text += f"\nCOMMENT '{table.comment}'"
        if security:
            text += f"\nSECURITY {security}"
        text += f"\nAS\n{definition}"

        logger.debug("Compiled SQL for CreateView: \n%s", text)
        return text

    def visit_create_view(self, create: CreateView, **kw: Any) -> str:
        """
        CREATE VIEW is handled by `visit_create_table` dispatcher
        based on `table.info['table_type']`.
        But, this is still needed for alembic's `CreateViewOp`.
        """
        return self._compile_create_view_from_table(create.element, create, **kw)

    def visit_drop_view(self, drop: DropView, **kw: Any) -> str:
        view = drop.element
        text = "DROP VIEW "
        if drop.if_exists:
            text += "IF EXISTS "
        text += self.preparer.format_table(view)
        return text

    def visit_alter_materialized_view(self, alter: AlterMaterializedView, **kw: Any) -> str:
        """
        Compile ALTER MATERIALIZED VIEW statement.
        Each attribute will be generated as a separate statement.

        Only supports altering mutable attributes:
        - refresh: ALTER MATERIALIZED VIEW ... REFRESH <new_scheme>
        - properties: ALTER MATERIALIZED VIEW ... SET ("<key>" = "<value>")
        """
        preparer = self.preparer

        # Format MV name with schema
        if alter.schema:
            mv_name = f"{preparer.quote(alter.schema)}.{preparer.quote(alter.mv_name)}"
        else:
            mv_name = preparer.quote(alter.mv_name)

        statements = []

        # ALTER REFRESH
        if alter.refresh is not None:
            statements.append(f"ALTER MATERIALIZED VIEW {mv_name} REFRESH {alter.refresh}")

        # ALTER PROPERTIES
        if alter.properties is not None:
            props_str = ", ".join([f'"{k}" = "{v}"' for k, v in alter.properties.items()])
            statements.append(f"ALTER MATERIALIZED VIEW {mv_name} SET ({props_str})")

        if not statements:
            raise exc.CompileError("ALTER MATERIALIZED VIEW requires at least one mutable attribute (refresh or properties)")

        # Return statements joined by semicolon
        return ";\n".join(statements)

    def _compile_create_mv_from_table(self, table: Table, create: CreateMaterializedView, **kw: Any) -> str:
        """
        Helper to compile CREATE MATERIALIZED VIEW from a CreateMaterializedView DDL element or CreateTable for MV.

        Args:
            create: CreateMaterializedView DDL element or CreateTable element where table_kind='MATERIALIZED_VIEW'
            **kw: Additional compilation kwargs

        Returns:
            Compiled SQL string for CREATE MATERIALIZED VIEW statement
        """
        preparer = self.preparer

        mv_name = preparer.format_table(table)
        definition = table.info.get(TableObjectInfoKey.DEFINITION)
        if not definition:
            raise ValueError(f"Materialized view '{table.name}' requires a definition.")

        # Handle or_replace and if_not_exists flags
        or_replace_clause = "OR REPLACE " if getattr(create, 'or_replace', False) else ""
        if_not_exists_clause = "IF NOT EXISTS " if getattr(create, 'if_not_exists', False) else ""

        text = f"CREATE {or_replace_clause}MATERIALIZED VIEW {if_not_exists_clause}{mv_name}"

        # Add column definitions if present (only name and comment are supported, same as View)
        if table.columns:
            text += self._get_view_column_clauses(table)

        clauses = []

        # Handle common clauses (COMMENT, PARTITION BY, etc.)
        common_clauses: str = self.post_create_table(table, **kw)
        clauses.append(common_clauses)

        if clauses:
            text += "\n" + "\n".join(clauses)

        text += f"\nAS\n{definition}"

        logger.debug("Compiled SQL for CreateMaterializedView: \n%s", text)
        return text

    def visit_create_materialized_view(self, create: CreateMaterializedView, **kw: Any) -> str:
        """
        The dispatch for creating a materialized view is from `CreateTable` DDL element
        with `table_kind='MATERIALIZED_VIEW'`.
        But, this is still needed for alembic's `CreateMaterializedViewOp`.
        """
        return self._compile_create_mv_from_table(create.element, create, **kw)

    def visit_drop_materialized_view(self, drop: DropMaterializedView, **kw: Any) -> str:
        mv = drop.element
        text = "DROP MATERIALIZED VIEW "
        if drop.if_exists:
            text += "IF EXISTS "
        text += self.preparer.format_table(mv)
        return text

    # Visit methods ordered according to StarRocks grammar:
    # engine → key → partition → distribution → order by → properties

    def visit_alter_table_engine(self, alter: AlterTableEngine, **kw: Any) -> str:
        """Compile ALTER TABLE ENGINE DDL for StarRocks.
        Not supported in StarRocks.
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)
        return f"ALTER TABLE {table_name} ENGINE = {alter.engine}"

    def visit_alter_table_key(self, alter: AlterTableKey, **kw: Any) -> str:
        """Compile ALTER TABLE KEY DDL for StarRocks.
        Not supported in StarRocks yet.
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)
        return f"ALTER TABLE {table_name} {alter.key_type} KEY ({alter.key_columns})"

    def visit_alter_table_partition(self, alter: AlterTablePartition, **kw: Any) -> str:
        """Compile ALTER TABLE PARTITION BY DDL for StarRocks.
        Not supported in StarRocks yet.
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)
        return f"ALTER TABLE {table_name} PARTITION BY {alter.partition_by}"

    def visit_alter_table_distribution(self, alter: AlterTableDistribution, **kw: Any) -> str:
        """Compile ALTER TABLE DISTRIBUTED BY DDL for StarRocks."""
        # TODO:
        table_name = format_table_name(self, alter.table_name, alter.schema)
        distribution_clause = f"DISTRIBUTED BY {alter.distribution_method}"
        if alter.buckets is not None:
            distribution_clause += f" BUCKETS {alter.buckets}"

        # notice users about such a time consuming operation
        from_db_clause = f"FROM {alter.schema} " if alter.schema else ""
        show_clause = f"SHOW ALTER TABLE OPTIMIZE {from_db_clause}WHERE TableName='{alter.table_name}'"
        logger.info(f"You probably should use ({show_clause}) to check the execution status "
                    f"of altering DISTRIBUTION before doing another ALTER TABLE statement.")

        return f"ALTER TABLE {table_name} {distribution_clause}"

    def visit_alter_table_order(self, alter: AlterTableOrder, **kw: Any) -> str:
        """Compile ALTER TABLE ORDER BY DDL for StarRocks."""

        table_name = format_table_name(self, alter.table_name, alter.schema)
        if isinstance(alter.order_by, list):
            order_by = ", ".join(alter.order_by)
        else:
            order_by = alter.order_by

        # notice users about such a time consuming operation
        from_db_clause = f"FROM {alter.schema} " if alter.schema else ""
        show_clause = f"SHOW ALTER TABLE OPTIMIZE {from_db_clause}WHERE TableName='{alter.table_name}'"
        logger.info(f"You probably should use ({show_clause}) to check the execution status "
                    f"of altering ORDER before doing another ALTER TABLE statement.")

        return f"ALTER TABLE {table_name} ORDER BY ({order_by})"

    def visit_alter_table_properties(self, alter: AlterTableProperties, **kw: Any) -> str:
        """Compile ALTER TABLE SET (...) DDL for StarRocks.

        Note:
            Currently, SR only support one property in an ALTER TABLE SET statement.
            So, we will generate multiple ALTER TABLE SET statements if there are multiple properties.
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)
        # logger.debug("ALTER TABLE %r SET (%s)", table_name, alter.properties)

        # Escape double quotes in property values
        def escape_value(value: str) -> str:
            return value.replace('"', '\\"')

        multi_set_statement = "; ".join([f'ALTER TABLE {table_name} SET ("{k}" = "{escape_value(v)}")' for k, v in alter.properties.items()])
        logger.debug("Compiled SQL for AlterTableProperties: \n%s", multi_set_statement)
        return multi_set_statement


class StarRocksIdentifierPreparer(MySQLIdentifierPreparer):
    """
    We can add some starrocks specific identifier behavior here if needed.

    Currently, we don't force to use quote for identifier.

    for reserved words, we can use the same as MySQL, and add some starrocks specific reserved words later.
    such as: reserved_words = STARROCKS_RESERVED_WORDS | RESERVED_WORDS_MYSQL
    """

    def __init__(self, dialect, **kwargs):
        kwargs.pop("server_ansiquotes", None)
        super().__init__(dialect, server_ansiquotes=False, **kwargs)

    # We don't force to use quote for identifier. we can uncomment this if needed.
    # def _requires_quotes(self, ident):
    #     return True


@log.class_logger
class StarRocksDialect(MySQLDialect_pymysql):
    # Dialect name
    name: Final[str] = "starrocks"

    # Supported/Permitted StarRocks's dialect construct arguments for Table and Column
    # Supports both lower and upper case variants for the arguments (easier for users' usages).
    construct_arguments = [
        (Table, {variant: None for k in TableInfoKey.ALL for variant in (k.lower(), k.upper())}),
        (Column, {variant: None for k in ColumnAggInfoKey.ALL for variant in (k.lower(), k.upper())}),
        (Update, {"limit": None}),
        (Delete, {"limit": None}),
        (sa_schema.PrimaryKeyConstraint, {"using": None}),
        (
            sa_schema.Index,
            {
                "using": None,
                "length": None,
                "prefix": None,
                "with_parser": None,
            },
        ),
    ]

    # Caching
    # Warnings are generated by SQLAlchemy if this flag is not explicitly set
    # and tests are needed before being enabled
    supports_statement_cache = True
    supports_empty_insert = False

    ischema_names = ischema_names
    colspecs = colspecs
    inspector = StarRocksInspector

    statement_compiler = StarRocksSQLCompiler
    ddl_compiler = StarRocksDDLCompiler
    type_compiler = StarRocksTypeCompiler
    preparer = StarRocksIdentifierPreparer

    # Used to get the partition info from the SHOW CREATE TABLE statement
    # Use regex to find the PARTITION BY clause. It can be multi-line.
    # _PARTITION_BY_PATTERN = re.compile(r"PARTITION BY\s+(.*?)(?:\s*DISTRIBUTED BY|\s*ORDER BY|\s*PROPERTIES|AS|;|$)", re.DOTALL | re.IGNORECASE)

    def __init__(self, *args, **kwargs):
        super(StarRocksDialect, self).__init__(*args, **kwargs)
        self._run_mode: Optional[str] = None
        self._bind_engine: Optional[Engine] = None
        # It may be error to explicitly instantiate the preparer here, `initialize` method will instance it.
        # self.preparer = self.preparer(self)

        # some test parameters
        # self.test_replication_num: Optional[int] = None

    def create_connect_args(self, url, _translate_args: Optional[Dict[str, Any]] = None):
        # Allow the superclass to create the base connect arguments
        _, connect_args = super(StarRocksDialect, self).create_connect_args(url, _translate_args)
        logger.debug("connect_args: %s", connect_args)

        # Handle the test-specific replication_num parameter
        # self.test_replication_num = connect_args.pop("test_replication_num", None)
        # if self.test_replication_num is not None:
        #     logger.info(
        #         f"set replication_num={self.test_replication_num} for small test environment"
        #     )

        return [], connect_args

    def initialize(self, connection: Connection) -> None:
        super().initialize(connection)
        # Cache an engine reference for deferred run_mode lookup.
        self._bind_engine = connection.engine

    @property
    def run_mode(self) -> str:
        """Lazily get and cache StarRocks run_mode.

        Keep the public attribute name for backward compatibility.
        """
        if self._run_mode is not None:
            return self._run_mode

        if self._bind_engine is not None:
            with self._bind_engine.connect() as conn:
                self._run_mode = self._get_run_mode(conn)
                logger.debug("system run mode: %s", self._run_mode)
                return self._run_mode

        # No usable connection context: return conservative default without caching.
        logger.warning("No connection context for run_mode lookup, fallback to shared_nothing")
        return SystemRunMode.SHARED_NOTHING

    def _get_server_version_info(self, connection: Connection) -> Tuple[int, ...]:
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

    def _parse_server_version(self, val: str) -> Tuple[int, ...]:
        server_version_info: tuple[int, ...] = tuple()
        m = re.match(r"(\d+)\.?(\d+)?(?:\.(\d+))?(?:\.\d+)?(?:[-\s])?(?P<commit>.*)?", val)
        if m is not None:
            server_version_info = tuple([int(x) for x in m.group(1, 2, 3) if x is not None])

        # setting it here to help w the test suite
        self.server_version_info = server_version_info
        return server_version_info

    def _get_run_mode(self, connection: Connection) -> str:
        """Get the StarRocks system run_mode (shared_data or shared_nothing).

        Args:
            connection: The SQLAlchemy connection object.

        Returns:
            The run_mode as a string ('shared_data' or 'shared_nothing').

        Raises:
            exc.DBAPIError: If the query fails.
        """
        try:
            result = connection.execute(text("ADMIN SHOW FRONTEND CONFIG LIKE 'run_mode'"))
            rows = result.fetchall()
            if rows and len(rows) > 0:
                # The result format is: | Key | AliasNames | Value | Type | IsMutable | Comment |
                value = str(rows[0][2]).lower()
                if value in (SystemRunMode.SHARED_DATA, SystemRunMode.SHARED_NOTHING):
                    return value
                else:
                    logger.warning(f"The run_mode gotten via frontend config is incorrect. run_mode: {value}")
        except exc.DBAPIError as e:
            logger.warning("Failed to get run_mode via frontend config: %s", e)

        try:
            result = connection.execute(text("SHOW STORAGE VOLUMES"))
            rows = result.fetchall()
            # Shared-data mode exposes storage volumes.
            if rows and len(rows) > 0:
                return SystemRunMode.SHARED_DATA
            else:
                logger.warning("Nothing gotten via storage volumes.")
        except exc.DBAPIError as e:
            logger.warning("Failed to infer run_mode via storage volumes: %s", e)

        # Conservative fallback.
        logger.warning("Starrocks, set run_mode to shared_nothing, because it failed to infer run_mode.")
        return SystemRunMode.SHARED_NOTHING

    def _show_create_table(
        self,
        connection: Connection,
        table: Optional[Table],
        charset: Optional[str] = None,
        full_name: Optional[str] = None,
    ) -> str:
        try:
            return super()._show_create_table(
                connection,
                table,
                charset,
                full_name,
            )
        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) == 1064:  # type: ignore[arg-type] # noqa: E501
                raise exc.NoSuchTableError(full_name) from e
            else:
                raise

    @util.memoized_property
    def _tabledef_parser(self) -> _reflection.StarRocksTableDefinitionParser:
        """return the StarRocksTableDefinitionParser, generate if needed.

        The deferred creation ensures that the dialect has
        retrieved server version information first.

        """
        preparer = self.identifier_preparer
        return _reflection.StarRocksTableDefinitionParser(self, preparer)

    def _read_from_information_schema(
        self, connection: Connection, inf_sch_table: str, charset: Optional[str] = None, **kwargs: Any
    ) -> List[_DecodingRow]:
        st = text(dedent(
            f"""
            SELECT *
            FROM information_schema.{inf_sch_table}
            WHERE {" AND ".join([f"{k} = :{k}" for k in kwargs.keys()])}
        """
        )).bindparams(
            *[
                bindparam(k, type_=sqltypes.Unicode)
                for k in kwargs.keys()
            ]
        )
        try:
            rp = connection.execution_options(
                skip_user_error_events=False
            ).execute(st, kwargs)
            rows: list[_DecodingRow] = [
                _DecodingRow(row, charset)
                for row in rp.mappings().fetchall()
            ]
            # NOTE: We should not raise NoSuchTableError if the query returns empty rows.
            # if not rows:
            #     raise exc.NoSuchTableError(f"Empty response for query: '{st}'")
            return rows

        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) == 1146:
                raise exc.NoSuchTableError(
                    f"information_schema.{inf_sch_table}"
                ) from e
            else:
                raise

    @reflection.cache
    def _setup_parser(
        self, connection, table_name, schema = None, **kwargs
    ):
        """
        Override to return different ReflectedState subclasses based on object type.

        Key: Query table_kind only once here, leveraging @reflection.cache.
        """
        # logger.debug("setup parser for table object: %s, schema: %s", table_name, schema)
        # 1. Query object type (only once)
        table_kind = self._get_table_kind_from_db(connection, table_name, schema)

        # 2. Dispatch based on type
        if table_kind == TableKind.VIEW:
            return self._setup_view_parser(connection, table_name, schema, **kwargs)
        elif table_kind == TableKind.MATERIALIZED_VIEW:
            return self._setup_mv_parser(connection, table_name, schema, **kwargs)
        else:
            return self._setup_table_parser(connection, table_name, schema, **kwargs)

    def _get_table_kind_from_db(self, connection: Connection, table_name: str, schema: Optional[str]) -> str:
        """
        Query object type from the database (without cache).
        Only called once in _setup_parser.
        """
        if not schema:
            schema = connection.dialect.default_schema_name
        # 1. Query information_schema.tables
        table_rows = self._read_from_information_schema(
            connection, "tables", table_schema=schema, table_name=table_name
        )
        if not table_rows:
            raise exc.NoSuchTableError(table_name)

        table_type = table_rows[0].TABLE_TYPE

        # 2. BASE TABLE → "TABLE"
        if table_type == 'BASE TABLE':
            return TableKind.TABLE

        # 3. VIEW → Further Distinguish
        if table_type == 'VIEW':
            mv_rows = self._read_from_information_schema(
                connection, "materialized_views",
                table_schema=schema, table_name=table_name
            )
            return TableKind.MATERIALIZED_VIEW if mv_rows else TableKind.VIEW

        return TableKind.TABLE  # Default fallback

    @reflection.cache
    def get_table_kind(self, connection, table_name, schema = None, **kwargs):
        """
        Get the object's table_kind (from cache).
        Reuse _setup_parser's cache via _parsed_state_or_create.
        Pass kwargs with `info_cache=inspector.info_cache` if you want to use the cache.
        """
        parsed_state = self._parsed_state_or_create(connection, table_name, schema, **kwargs)
        return parsed_state.table_kind

    @reflection.cache
    def _setup_table_parser(
        self, connection, table_name, schema = None, **kwargs
    ):
        charset: Optional[str] = self._connection_charset
        parser: _reflection.StarRocksTableDefinitionParser = self._tabledef_parser
        # logger.debug("setup table parser for table: %s, schema: %s", table_name, schema)

        if not schema:
            schema = connection.dialect.default_schema_name

        table_rows: List[_DecodingRow] = self._read_from_information_schema(
            connection=connection,
            inf_sch_table="tables",
            charset=charset,
            table_schema=schema,
            table_name=table_name,
        )
        if not table_rows:
            raise exc.NoSuchTableError(table_name)
        if len(table_rows) > 1:
            raise exc.InvalidRequestError(
                f"Multiple tables found with name {table_name} in schema {schema}"
            )
        # logger.debug("reflected table row for table: %s, info: %s", table_name, dict(table_rows[0]))
        try:
            table_config_rows: List[_DecodingRow] = self._read_from_information_schema(
                connection=connection,
                inf_sch_table="tables_config",
                charset=charset,
                table_schema=schema,
                table_name=table_name,
            )
        except Exception as e:
            if 'Unknown table \'information_schema.tables_config\'' in str(e):
                table_config_rows = [{
                    'TABLE_ENGINE': table_rows[0].get('ENGINE'),
                }]
            else:
                raise
        if table_config_rows:
            if len(table_config_rows) > 1:
                raise exc.InvalidRequestError(
                    f"Multiple tables found with name {table_name} in schema {schema}"
                )
            table_config_row = table_config_rows[0]
        else:
            table_config_row = {}
        # logger.debug("reflected table config for table: %s, table_config: %s", table_name, dict(table_config_row))

        column_rows: List[_DecodingRow] = self._read_from_information_schema(
            connection=connection,
            inf_sch_table="columns",
            charset=charset,
            table_schema=schema,
            table_name=table_name,
        )

        full_name = self._get_quote_full_table_name(table_name, schema=schema)
        show_create_table = self._show_create_table(connection, None, charset, full_name)
        column_autoinc = self._get_autoinc_from_show_create_table(show_create_table)

        # Get aggregate info from `SHOW FULL COLUMNS`
        full_column_rows: List[Row] = self._get_show_full_columns(
            connection, table_name=table_name, schema=schema
        )
        column_2_agg_type: Dict[str, str] = {
            row.Field: row.Extra.upper()
            for row in full_column_rows
            if row.Extra
        }

        partition_clause = self._get_partition_clause_from_create_table(show_create_table)
        # Add the partition info into table_config row for convenience
        # But the row object is immutable, so we convert it to a dictionary to modify it.
        table_config_dict = dict(table_config_row)
        if partition_clause:
            table_config_dict['PARTITION_CLAUSE'] = partition_clause

        return parser.parse_table(
            table=table_rows[0],
            table_config=table_config_dict,
            columns=column_rows,
            column_2_agg_type=column_2_agg_type,
            column_autoinc=column_autoinc,
            charset=charset,
        )

    def _get_quote_full_table_name(
        self, table_name: str, schema: Optional[str] = None
    ) -> str:
        return ".".join(
            self.identifier_preparer._quote_free_identifiers(
                schema, table_name
            )
        )

    def _get_autoinc_from_show_create_table(self, create_table: str) -> Dict[str, Any]:
        """
        Get the auto increment info from the SHOW CREATE TABLE statement.

        Args:
            create_table: The SHOW CREATE TABLE statement.

        Returns:
            A dictionary of column names and their auto increment info.
            The key is the column name, the value is True/False whether the column has auto increment
            Example: `{"col1": True, "col2": False, ...}`
        """
        if create_table.lstrip().startswith("CREATE VIEW"):
            return dict()
        only_create = create_table.split('ENGINE=')[0]
        only_columns = only_create.split("\n")[1:-1]
        only_columns = [c.strip() for c in only_columns if c.strip().startswith("`")]
        col_autoinc = {
            c.split(' ')[0].strip('`'): 'AUTO_INCREMENT' in c
            for c in only_columns
        }
        # logger.debug("get auto increment info from show create table: %s", col_autoinc)
        return col_autoinc

    def _get_partition_clause_from_create_table(self, create_table: str) -> Optional[str]:
        """
        Get the PARTITION BY clause from the SHOW CREATE TABLE statement.
        Because we can't get the partition info from any information_schema views.
        """
        # Use regex to find the PARTITION BY clause. It can be multi-line.
        # match = self._PARTITION_BY_PATTERN.search(create_table_str)
        match = StarRocksTableDefinitionParser._PARTITION_BY_PATTERN.search(create_table)
        if match:
            partition_clause = match.group(1).strip()
            return partition_clause
        return None

    def _get_show_full_columns(
        self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> List[Row]:
        """Run SHOW FULL COLUMNS to get detailed column information.
        Currently, it's only used to get aggregate type of columns.
        Other column info are still mainly extracted from information_schema.columns.
        """
        full_table_name = self._get_quote_full_table_name(table_name, schema)
        try:
            st: str = "SHOW FULL COLUMNS FROM %s" % full_table_name
            # logger.debug(f"query special column info by using: {st}")
            return list(connection.exec_driver_sql(st).fetchall())
        except exc.DBAPIError as e:
            # 1146: Table ... doesn't exist
            if e.orig and e.orig.args[0] == 1146:
                raise exc.NoSuchTableError(table_name) from e
            raise
        except Exception as e:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            logger.warning(f"Could not get SHOW FULL COLUMNS for table {full_table_name}: {e}")
            return []

    @staticmethod
    def gen_show_alter_table_statement(table_name: str, alter_type: str,
            schema: Optional[str] = None, state: str = 'RUNNING') -> str:
        """Generate the SHOW ALTER TABLE OPTIMIZE statement for a given table."""
        from_db_clause = f"FROM `{schema}` " if schema else ""
        state_clause = f" AND State='{state}'" if state else ""
        stmt = f"SHOW ALTER TABLE {alter_type} {from_db_clause}WHERE TableName='{table_name}'{state_clause}"
        # logger.debug("generate show alter table statement: %s", stmt)
        return stmt

    @staticmethod
    def get_show_alter_table(connection: Connection, table_name: str, alter_type: str,
            schema: Optional[str] = None, state: str = 'RUNNING') -> Optional[Row]:
        """Get the SHOW ALTER TABLE OPTIMIZE statement for a given table."""
        if alter_type.upper() not in ["COLUMN", "OPTIMIZE"]:
            raise exc.NotSupportedError("You can only SHOW ALTER TABLE [ COLUMN | OPTIMIZE ].")
        st: str = StarRocksDialect.gen_show_alter_table_statement(table_name, alter_type, schema, state)
        try:
            return connection.execute(text(st)).fetchone()
        except exc.DBAPIError as e:
            # 1146: Table ... doesn't exist
            if e.orig and e.orig.args[0] == 1146:
                raise exc.NoSuchTableError(table_name) from e
            raise
        except Exception as e:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            logger.warning(f"Could not get SHOW ALTER TABLE OPTIMIZE for table {full_table_name}: {e}")
            return None

    @reflection.cache
    def get_table_comment(
            self,
            connection,
            table_name,
            schema = None,
            **kw,
    ):
        """Get the table comment from the parsed state.
        Overrides the mysql's implementation, which will use 'mysql_comment' as the key,
        here the `comment` supports lower case only.
        """
        parsed_state = self._parsed_state_or_create(connection, table_name, schema, **kw)
        comment = parsed_state.table_options.get(TableInfoKeyWithPrefix.COMMENT, None)
        if comment is not None:
            return {"text": comment}
        else:
            return ReflectionTableDefaults.table_comment()

    @reflection.cache
    def get_indexes(
        self, connection, table_name, schema = None, **kwargs
    ):

        parsed_state: Any = self._parsed_state_or_create(
            connection, table_name, schema, **kwargs
        )

        indexes: List[Dict[str, Any]] = []

        # TODO: same logic as MySQL?
        for spec in parsed_state.keys:

            dialect_options: Dict[str, Any] = {}
            unique = False
            flavor: Optional[str] = spec["type"]
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

            index_d: Dict[str, Any] = {}

            index_d["name"] = spec["name"]
            index_d["column_names"] = [s[0] for s in spec["columns"]]
            mysql_length: Dict[str, Any] = {
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

    @reflection.cache
    def has_table(
        self, connection, table_name, schema = None, **kwargs
    ):
        try:
            return super().has_table(connection, table_name, schema, **kwargs)
        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) in (5501, 5502):
                return False
            if 'not exists' in str(e.orig).lower():
                return False
            raise

    def get_view_names(self, connection: Connection, schema: Optional[str] = None, **kwargs: Any) -> List[str]:
        """Return all view names in a given schema."""
        if schema is None:
            schema = self.default_schema_name
        try:
            rows = self._read_from_information_schema(
                connection,
                "views",
                table_schema=schema,
            )
            return [row.TABLE_NAME for row in rows]
        except Exception:
            return []

    @reflection.cache
    def _setup_view_parser(
        self, connection, view_name, schema = None, **kwargs
    ):
        """
        Fetches raw data for a view and passes it to the parser.
        """
        if schema is None:
            schema = self.default_schema_name

        view_rows = self._read_from_information_schema(
            connection, "views", table_schema=schema, table_name=view_name
        )
        if not view_rows:
            raise exc.NoSuchTableError(view_name)
        view_row = view_rows[0]

        table_row = None
        try:
            table_rows = self._read_from_information_schema(
                connection, "tables", table_schema=schema, table_name=view_name
            )
            if table_rows:
                table_row = table_rows[0]
        except Exception as e:
            self.logger.info(f"Could not retrieve comment for View '{schema}.{view_name}': {e}")

        # Reflect columns for view (only name and comment are meaningful for views)
        column_rows: List[_DecodingRow] = self._read_from_information_schema(
            connection=connection,
            inf_sch_table="columns",
            table_schema=schema,
            table_name=view_name,
        )

        # Get SECURITY from SHOW CREATE VIEW
        # Note: information_schema.views.SECURITY_TYPE is always empty in StarRocks
        create_view_sql = None
        try:
            # Execute SHOW CREATE VIEW to get the full CREATE VIEW statement
            qualified_name = f"`{schema}`.`{view_name}`" if schema else f"`{view_name}`"
            result = connection.execute(text(f"SHOW CREATE VIEW {qualified_name}"))
            rows = result.fetchone()
            if rows:
                # SHOW CREATE VIEW returns: (View, Create View, character_set_client, collation_connection)
                create_view_sql = rows[1] if len(rows) > 1 else None
        except Exception as e:
            self.logger.warning(f"Could not retrieve CREATE VIEW statement for '{schema}.{view_name}': {e}")

        parser = self._tabledef_parser
        return parser.parse_view(view_row, table_row, column_rows, create_view_sql)

    def get_view(self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> ReflectedViewState:
        """
        Return a ReflectedViewState object for a single view.
        Pass kwargs with `info_cache=inspector.info_cache` if you want to use the cache.

        Raises NoSuchTableError if the view does not exist.
        """
        view_info = self._setup_view_parser(connection, view_name, schema=schema, **kwargs)
        logger.debug(
            "get_view normalized: schema=%s, name=%s, security=%s, definition=(%s)",
            schema, view_info.name, view_info.security, view_info.definition
        )
        return ReflectionViewDefaults.apply_info(view_info)

    def get_view_definition(
        self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> Optional[str]:
        """Return the definition of a view.
        Pass kwargs with `info_cache=inspector.info_cache` if you want to use the cache.
        """
        view_state = self._setup_view_parser(connection, view_name, schema=schema, **kwargs)
        return view_state.definition if view_state else None

    def get_materialized_view_names(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        """Return all materialized view names in a given schema."""
        if schema is None:
            schema = self.default_schema_name
        try:
            rows: List[_DecodingRow] = self._read_from_information_schema(
                connection,
                "materialized_views",
                table_schema=schema,
            )
            return [row.TABLE_NAME for row in rows]
        except Exception:
            return []

    @reflection.cache
    def _setup_mv_parser(
        self, connection, view_name, schema = None, **kwargs
    ):
        """
        Fetches all raw data for a Materialized View and passes it to the parser.
        """
        if schema is None:
            schema = self.default_schema_name

        try:
            # 1. Get MV row (contains DDL) from information_schema.materialized_views
            mv_rows = self._read_from_information_schema(
                connection, "materialized_views", table_schema=schema, table_name=view_name
            )
            if not mv_rows:
                raise exc.NoSuchTableError(view_name)
            mv_row = mv_rows[0]

            # 2. Get table row (for comment) from information_schema.tables
            table_row = None
            try:
                table_rows = self._read_from_information_schema(
                    connection, "tables", table_schema=schema, table_name=view_name
                )
                if table_rows:
                    table_row = table_rows[0]
            except Exception as e:
                self.logger.info(f"Could not retrieve comment for MV '{schema}.{view_name}': {e}")

            # 3. Get config row (for distribution, order_by) from information_schema.tables_config
            config_row = None
            try:
                config_rows = self._read_from_information_schema(
                    connection, "tables_config", table_schema=schema, table_name=view_name
                )
                if config_rows:
                    config_row = config_rows[0]
            except Exception as e:
                self.logger.info(f"Could not retrieve config for MV '{schema}.{view_name}': {e}")

            # 4. Pass all raw data to the parser
            parser = self._tabledef_parser
            return parser.parse_mv(mv_row, table_row, config_row)

        except Exception as e:
            self.logger.warning(f"Failed to get materialized view info for '{schema}.{view_name}': {e}")
            raise

    def get_materialized_view(
        self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> ReflectedMVState:
        """Return all information about a materialized view.
        Pass kwargs with `info_cache=inspector.info_cache` if you want to use the cache.
        """
        mv_info = self._setup_mv_parser(connection, view_name, schema=schema, **kwargs)
        logger.debug("get_materialized_view normalized: schema=%s, name=%s, definition=(%s)",
                     schema, view_name, mv_info.definition)
        return ReflectionMVDefaults.apply_info(mv_info)

    def get_materialized_view_options(
        self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> Dict[str, str]:
        """Return the physical properties of a materialized view.
        Pass kwargs with `info_cache=inspector.info_cache` if you want to use the cache.
        """
        mv_info = self._setup_mv_parser(connection, view_name, schema=schema, **kwargs)
        return mv_info.table_options


# --- Alembic alter column compilers for StarRocks ---
"""
For MySQLModifyColumn, MySQLChangeColumn, MySQLAlterDefault,
We should register the 'starrocks' compiler for them.
In the future, we may implement StarRocks's alter_table in StarRocksImpl to override MySQL's alter_table.
TODO: Then, we can add more StarRocks specific attributes, such as KEY/agg_type.
"""


@compiles(MySQLModifyColumn, DialectName)
def _starrocks_modify_column(element: MySQLModifyColumn, compiler: StarRocksDDLCompiler, **kw: Any) -> str:
    return "%s MODIFY COLUMN %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_column_name(compiler, element.column_name),
        compiler.get_column_spec_for_alter_column(
            name=element.column_name,
            nullable=element.nullable,
            default=element.default,
            type_=element.type_,
            autoincrement=element.autoincrement,
            comment=element.comment,
        ),
    )


@compiles(MySQLChangeColumn, DialectName)
def _starrocks_change_column(element: MySQLChangeColumn, compiler: StarRocksDDLCompiler, **kw: Any) -> str:
    """
    It's a must for RENAMEing a column, because MODIFY COLUMN does not support changing the name.
    And in StarRocks, there should be two alter clauses if both RENAME and MODIFY

    NOTE: Currently, MySQL will pass column_type even for RENAME COLUMN. SO, it will also generate
    an MODIFY COLUMN clause, because we don't know whether the column_type is changed, and StarRocks
    doesn't support CHANGE COLUMN.
    """
    rename_clause = "RENAME COLUMN %s TO %s" % (
        format_column_name(compiler, element.column_name),
        format_column_name(compiler, element.newname),
    ) if element.newname else None

    modify_clause = "MODIFY COLUMN %s %s" % (
        format_column_name(compiler, element.column_name),
        compiler.get_column_spec_for_alter_column(
            name=element.column_name,
            nullable=element.nullable,
            default=element.default,
            type_=element.type_,
            autoincrement=element.autoincrement,
            comment=element.comment,
        ),
    ) if (element.nullable is not None
            or element.type_ is not None
            or element.autoincrement is not None
            or element.comment is not None
    ) else None

    alter_claus_header: str = alter_table(compiler, element.table_name, element.schema)
    if rename_clause and modify_clause:
        return "%s %s, %s" % (alter_claus_header, modify_clause, rename_clause)
    elif rename_clause:
        return "%s %s" % (alter_claus_header, rename_clause)
    else:
        return "%s %s" % (alter_claus_header, modify_clause)


@compiles(MySQLAlterDefault, DialectName)
def _starrocks_alter_default(element: MySQLAlterDefault, compiler: StarRocksDDLCompiler, **kw: Any) -> str:  # type: ignore[name-defined]
    """
    StarRocks only supports MODIFY DEFAULT, no DROP DEFAULT now.
    """
    return "%s MODIFY COLUMN %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        format_column_name(compiler, element.column_name),
        (
            "DEFAULT %s" % format_server_default(compiler, element.default)
        ),
    )
