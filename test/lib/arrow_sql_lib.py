#!/usr/bin/env python
# -- coding: utf-8 --
###########################################################################
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
###########################################################################
"""
arrow_sql_lib.py

@Time : 2024/11/13
@Author : liubotao
"""
import json
import math
import warnings
from decimal import Decimal
from typing import Tuple, Any

from cup import log

import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql
import pyarrow
import pyarrow.compute as pc
from dbutils.pooled_db import PooledDB
from pyarrow import types as T

from lib.connection_base_lib import BaseConnectionLib, SQLRawResult


class ArrowSqlLib(BaseConnectionLib):
    def __init__(self, conn=None):
        self.connector: flight_sql.Connection = conn

    def connect(self, query_dict):
        # Filter the autocommit warning from ADBC driver
        warnings.filterwarnings('ignore', message='Cannot disable autocommit.*')
        self.connector = flight_sql.connect(
            uri=f"grpc://{query_dict['host']}:{int(query_dict['arrow_port'])}",
            db_kwargs={
                adbc_driver_manager.DatabaseOptions.USERNAME.value: query_dict["user"],
                adbc_driver_manager.DatabaseOptions.PASSWORD.value: query_dict["password"],
            })

    def close(self):
        if self.connector:
            self.connector.close()
            self.connector = None

    def execute(self, sql: str) -> SQLRawResult:
        sql_statements = self._split_sql(sql)

        try:
            if len(sql_statements) == 1:
                return self._execute_single(sql_statements[0])

            # If multiple SQL statements, execute each and return query result
            results = []
            for stmt in sql_statements:
                result = self._execute_single(stmt)
                if not result.status:
                    return result  # Return immediately on error
                results.append(result)

            # Select the result to return (prioritize query results)
            return self._select_result(results)
        except adbc_driver_manager.DatabaseError as e:
            return SQLRawResult(status=False, result=(), msg=str(e), desc=None)

    def wrapper(self, conn):
        return ArrowSqlLib(conn)

    def create_pool(self, host: str, mysql_port: int, arrow_port: int, user: str, password: str) -> PooledDB:
        # Filter the autocommit warning from ADBC driver
        warnings.filterwarnings('ignore', message='Cannot disable autocommit.*')
        return PooledDB(
            creator=flight_sql,
            mincached=3,
            blocking=True,
            uri=f"grpc://{host}:{arrow_port}",
            db_kwargs={
                adbc_driver_manager.DatabaseOptions.USERNAME.value: user,
                adbc_driver_manager.DatabaseOptions.PASSWORD.value: password,
            },
        )

    def _split_sql(self, sql: str) -> list:
        """
        Split SQL string into multiple SQL statements.
        A statement ends with ';' that is not inside a string literal.
        Handles single quotes ('), double quotes ("), and backslash escaping.
        """
        statements = []
        current_stmt = []
        in_single_quote = False
        in_double_quote = False
        escaped = False

        for char in sql:
            if escaped:
                # Previous character was a backslash, current character is escaped
                current_stmt.append(char)
                escaped = False
                continue

            if char == '\\':
                # Backslash escapes the next character
                current_stmt.append(char)
                escaped = True
                continue

            if char == "'" and not in_double_quote:
                # Toggle single quote state (only if not in double quote)
                in_single_quote = not in_single_quote
                current_stmt.append(char)
            elif char == '"' and not in_single_quote:
                # Toggle double quote state (only if not in single quote)
                in_double_quote = not in_double_quote
                current_stmt.append(char)
            elif char == ';' and not in_single_quote and not in_double_quote:
                # Statement terminator (only if not in any string)
                current_stmt.append(char)
                stmt = ''.join(current_stmt)
                if stmt and stmt != ';':  # Ignore empty statements
                    statements.append(stmt)
                current_stmt = []
            else:
                # Regular character
                current_stmt.append(char)

        # Handle remaining statement (if any)
        if current_stmt:
            stmt = ''.join(current_stmt)
            if stmt:  # Ignore empty statements
                statements.append(stmt)

        for i, raw_statement in enumerate(statements):
            statement = raw_statement.strip()
            if statement.startswith('--') and '\n' not in statement:
                statements[i - 1] += raw_statement
                statements[i] = ''
        statements = [s for s in statements if s.strip() and s.strip() != ';']

        return statements if statements else [sql]  # Return original if no split occurred

    def _execute_single(self, sql: str) -> SQLRawResult:
        """Execute a single SQL statement."""
        with self.connector.cursor() as cursor:
            cursor.execute(sql)
            arrow_table = cursor.fetch_arrow_table()
            res = convert_arrow_table_to_mysql_rows(arrow_table)
            return SQLRawResult(status=True, result=res, msg="OK", desc=cursor.description)

    def _is_query_result(self, result: SQLRawResult) -> bool:
        """
        Check if the result is a query result.
        Non-query result: cursor.description has only one column named 'StatusResult' with string type.
        """
        if not result.desc:
            return False

        if len(result.desc) != 1:
            return True  # More than one column, it's a query

        # Check if it's StatusResult column
        col_name = result.desc[0][0]
        col_type = str(result.desc[0][1])  # Convert to string for comparison

        # Check if it's a StatusResult (non-query result)
        if col_name == 'StatusResult' and 'string' in col_type.lower():
            return False

        return True

    def _select_result(self, results: list) -> SQLRawResult:
        """
        Select which result to return from multiple results.
        Prioritize query results over non-query results.
        """
        # Try to find a query result
        for result in results:
            if self._is_query_result(result):
                return result

        # If no query result found, return the last result
        return results[-1] if results else SQLRawResult(status=True, result=(), msg="OK")


def convert_arrow_table_to_mysql_rows(arrow_table) -> Tuple[Tuple[Any, ...], ...]:
    if arrow_table.num_rows == 0:
        return tuple()

    rows = arrow_table_to_py(arrow_table)
    new_rows = []
    for row in rows:
        new_row = []
        for col in row:
            if isinstance(col, (dict, list)):
                new_col = serialize_to_json(col)
            elif isinstance(col, bytes):
                try:
                    new_col = col.decode()
                except UnicodeDecodeError as e:
                    log.info("decode sql result by utf-8 error, try str")
                    new_col = str(col)
            else:
                new_col = col
            new_row.append(new_col)
        new_rows.append(tuple(new_row))

    return tuple(new_rows)


def serialize_to_json(item, deep=0) -> str:
    """
    Convert the data into the same format as MySQL's result set. The main focus is on handling Array, Map, and Struct
    types (which correspond to Python's list/tuple and dict).
    Note that when a key is an int, its serialized form must not be wrapped in quotes.
    For example: {1: "value"} -> {1:"value"} instead of {"1":"value"}
    """
    if item is None:
        return "null"
    elif isinstance(item, bool):
        return "true" if item else "false"
    elif isinstance(item, (int, Decimal)):
        return str(item)
    elif isinstance(item, float):
        # In array/map/struct, MySQL displays integer-valued floats without .0
        # e.g., 6.0 -> "6", but 1.1 -> "1.1"
        if deep != 0 and not math.isnan(item) and item == int(item):  # Check it's an integer value and not NaN
            return str(int(item))
        return str(item)
    elif isinstance(item, str):
        escaped = json.dumps(item, ensure_ascii=False)
        return escaped
    elif isinstance(item, dict):
        if not item:
            return "{}"
        items = []
        for k, v in item.items():
            # For integer keys, don't add quotes
            if isinstance(k, int):
                key_str = str(k)
            else:
                key_str = json.dumps(str(k), ensure_ascii=False)
            value_str = serialize_to_json(v, deep + 1)
            items.append(f"{key_str}:{value_str}")
        return "{" + ",".join(items) + "}"
    elif isinstance(item, (list, tuple)):
        if not item:
            return "[]"
        items = [serialize_to_json(item, deep + 1) for item in item]
        return "[" + ",".join(items) + "]"
    else:
        # Fallback to standard JSON serialization
        return json.dumps(item, ensure_ascii=False, separators=(',', ':'))


def arrow_table_to_py(table: pyarrow.Table):
    """
    Given a pyarrow.Table, return a List[List[...]].
    """
    num_cols = table.num_columns
    num_rows = table.num_rows

    col_converters = []
    columns = []
    for col_i in range(num_cols):
        col = table.column(col_i)
        columns.append(col.combine_chunks())
        col_converters.append(_build_arrow_to_py_converter(col.type))

    rows = []
    for row_i in range(num_rows):
        new_columns = []
        for col_i in range(num_cols):
            item = columns[col_i][row_i]
            try:
                py_value = item.as_py()  # may contain nested list/map/struct
            except UnicodeDecodeError:
                # Handle non-UTF8 bytes in string columns
                # Get the raw bytes and represent them as bytes object
                if hasattr(item, 'as_buffer'):
                    raw_bytes = item.as_buffer().to_pybytes()
                    py_value = raw_bytes
                else:
                    # Fallback: try to get bytes representation
                    py_value = bytes(item)
            except OverflowError:
                col_type = columns[col_i].type
                if T.is_date32(col_type):
                    # Some dates (e.g. year 0000) cannot be converted to Python date; format via Arrow.
                    py_value = pc.strftime(item, format="%Y-%m-%d").as_py()
                elif T.is_timestamp(col_type):
                    # Use Arrow formatting to avoid Python datetime limits.
                    py_value = pc.strftime(item, format="%Y-%m-%d %H:%M:%S.%f").as_py()
                else:
                    raise
            new_columns.append(col_converters[col_i](py_value))
        rows.append(new_columns)

    return rows


def _build_arrow_to_py_converter(dtype: pyarrow.DataType):
    """
    Build a Python-side converter function based on an Arrow DataType.
    The converter will recursively turn any map-typed values (including nested)
    from list-of-tuples form into Python dict.
    """
    # 1. MapType: value is [(key, value), ...]
    if T.is_map(dtype):
        key_conv = _build_arrow_to_py_converter(dtype.key_type)
        value_conv = _build_arrow_to_py_converter(dtype.item_type)

        def conv_map(value):
            if value is None:
                return None
            # Default Python representation of map is [(k, v), ...]
            return {key_conv(k): value_conv(v) for (k, v) in value}

        return conv_map

    # 2. List / LargeList / FixedSizeList: recursively convert each element
    if T.is_list(dtype) or T.is_large_list(dtype) or T.is_fixed_size_list(dtype):
        elem_conv = _build_arrow_to_py_converter(dtype.value_type)

        def conv_list(value):
            if value is None:
                return None
            return [elem_conv(v) for v in value]

        return conv_list

    # 3. Struct: value is a dict {field_name: field_value, ...}
    if T.is_struct(dtype):
        field_convs = {
            field.name: _build_arrow_to_py_converter(field.type)
            for field in dtype
        }

        def conv_struct(value):
            if value is None:
                return None
            # value is a dict: field name -> Python value
            return {
                name: field_convs[name](value.get(name))
                for name in field_convs
            }

        return conv_struct

    # 4. Date32: convert to YYYY-MM-DD string
    if T.is_date32(dtype):
        def conv_date32(value):
            if value is None:
                return None
            if isinstance(value, str):
                return value
            return f"{value.year:04d}-{value.month:02d}-{value.day:02d}"

        return conv_date32

    # 5. Timestamp (datetime): convert to string, append .uuuuuu when microsecond > 0
    if T.is_timestamp(dtype):
        def conv_timestamp(value):
            if value is None:
                return None
            if isinstance(value, str):
                return value
            base = (f"{value.year:04d}-{value.month:02d}-{value.day:02d} "
                    f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}")
            if value.microsecond > 0:
                return f"{base}.{value.microsecond:06d}"
            return base

        return conv_timestamp

    # 6. Boolean type: convert True/False to 1/0
    if T.is_boolean(dtype):
        def conv_bool(value):
            if value is None:
                return None
            return 1 if value else 0

        return conv_bool

    # 7. Float32 type: handle precision to match MySQL/Ryu behavior
    if T.is_float32(dtype):
        import struct

        def conv_float32(value):
            if value is None:
                return None

            # Round-trip through float32 to preserve only float32 precision
            float32_bytes = struct.pack('f', value)
            float32_value = struct.unpack('f', float32_bytes)[0]

            # Implement Ryu-like algorithm: find shortest representation that round-trips
            # Try from FLT_DIG (6) to FLT_DIG + 2 (8) significant digits
            for digits in [6, 7, 8]:
                formatted = f'{float32_value:.{digits}g}'
                try:
                    reparsed = struct.unpack('f', struct.pack('f', float(formatted)))[0]
                    if reparsed == float32_value:
                        return float(formatted)
                except (ValueError, struct.error):
                    pass

            # Fallback: use 8 digits if nothing worked
            formatted = f'{float32_value:.8g}'
            return float(formatted)

        return conv_float32

    # 6. Other complex types (dictionary/union/extension/etc.) can be added here.
    #    By default, return the Python value as-is.
    return lambda v: v
