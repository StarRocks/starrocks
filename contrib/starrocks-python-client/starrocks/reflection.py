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

from __future__ import annotations

import json
import logging
import re
from typing import Any, Collection, Dict, List, Optional, Set, Union

from sqlalchemy import log, types as sqltypes, util
from sqlalchemy.dialects.mysql.base import _DecodingRow
from sqlalchemy.dialects.mysql.reflection import _re_compile
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.schema import Table

from starrocks.common.consts import TableConfigKey
from starrocks.common.params import (
    ColumnAggInfoKeyWithPrefix,
    SRKwargsPrefix,
    TableInfoKey,
    TableInfoKeyWithPrefix,
    TableKind,
    TableObjectInfoKey,
)
from starrocks.common.types import PartitionType
from starrocks.common.utils import SQLParseError, TableAttributeNormalizer

from .common import utils
from .drivers.parsers import parse_data_type, parse_mv_refresh_clause
from .engine.interfaces import (
    MySQLKeyType,
    ReflectedDistributionInfo,
    ReflectedMVState,
    ReflectedPartitionInfo,
    ReflectedRefreshInfo,
    ReflectedState,
    ReflectedTableKeyInfo,
    ReflectedViewState,
)


logger = logging.getLogger(__name__)


class StarRocksInspector(Inspector):
    """
    The StarRocksInspector provides a custom inspector for the StarRocks dialect,
    allowing for reflection of StarRocks-specific database objects like views.
    """
    def __init__(self, bind):
        super().__init__(bind)

    def get_view(self, view_name: str, schema: Optional[str] = None, **kwargs: Any) -> ReflectedViewState:
        """
        Retrieves information about a specific view.

        :param view_name: The name of the view to inspect.
        :param schema: The schema of the view; defaults to the default schema name if None.
        :param kwargs: Additional arguments passed to the dialect's get_view method.
        :return: A ReflectedViewState object.
        :raises: NoSuchTableError if the view does not exist.
        """
        return self.dialect.get_view(self.bind, view_name, schema=schema, **kwargs)

    def get_materialized_view(self, view_name: str, schema: Optional[str] = None, **kwargs: Any) -> Optional[ReflectedMVState]:
        """
        Retrieves information about a specific materialized view.

        :param view_name: The name of the materialized view to inspect.
        :param schema: The schema of the materialized view; defaults to the default schema name if None.
        :param kwargs: Additional arguments passed to the dialect's get_materialized_view method.
        :return: A ReflectedMVState object, or None if the materialized view does not exist.
        """
        return self.dialect.get_materialized_view(self.bind, view_name, schema=schema, **kwargs)

    def get_materialized_view_definition(self, view_name: str, schema: Optional[str] = None, **kwargs: Any) -> Optional[str]:
        """
        Retrieves the definition of a specific materialized view.

        :param view_name: The name of the materialized view to inspect.
        :param schema: The schema of the materialized view; defaults to the default schema name if None.
        :param kwargs: Additional arguments passed to the dialect's get_materialized_view_definition method.
        :return: The materialized view definition as a string, or None if the view does not exist.
        """
        mv_state = self.get_materialized_view(view_name, schema=schema, **kwargs)
        return mv_state.definition if mv_state else None

    def reflect_table(
        self,
        table: Table,
        include_columns: Optional[Collection[str]] = None,
        exclude_columns: Collection[str] = (),
        resolve_fks: bool = True,
        _extend_on: Optional[Set[Table]] = None,
        _reflect_info: Optional[Any] = None
    ) -> None:
        """
        Override to set VIEW/MV specific attributes.
        """

        # 1. Call parent class (will call get_pk_constraints, etc., which will trigger _setup_parser)
        #    And, it will set all dialect options from parsed_state, including for a View or a Mv or a Table.
        super().reflect_table(table, include_columns, exclude_columns, resolve_fks, _extend_on, _reflect_info)
        # comment is already set to Table.comment, the starrocks_comment is not used again.
        self._delete_comment_from_dialect_options(table)

        # 1. Get table_kind and parsed_state, which will use the cached parsed_state)
        parsed_state = self.dialect._parsed_state_or_create(self.bind, table.name, table.schema, info_cache=self.info_cache)
        table_kind = parsed_state.table_kind
        logger.debug("reflect %s: %s, parsed_state: %s.", table_kind.lower(), table.name, parsed_state)

        # 3. Set info['table_kind']
        table.info[TableObjectInfoKey.TABLE_KIND] = table_kind

        # 4. Set specific attributes based on type
        if table_kind == TableKind.VIEW:
            self._reflect_view_attributes(table, parsed_state)
        elif table_kind == TableKind.MATERIALIZED_VIEW:
            self._reflect_mv_attributes(table, parsed_state)

    @staticmethod
    def _delete_comment_from_dialect_options(table: Table):
        try:
            del table.dialect_kwargs[TableInfoKeyWithPrefix.COMMENT]
        except KeyError:
            # starrocks_comment does not exist in dialect_kwargs
            pass

    def _reflect_view_attributes(self, table: Table, view_state: ReflectedViewState) -> None:
        """Set View specific attributes from ReflectedViewState.

        Note: Column information (including comments) is already handled by
        SQLAlchemy's standard reflection flow via get_columns(), which returns
        ReflectedColumn dictionaries from view_state.columns.
        """
        table.info[TableObjectInfoKey.DEFINITION] = view_state.definition
        # if view_state.security:
        #     table.dialect_options.setdefault(DialectName, {})[TableInfoKey.SECURITY] = view_state.security

    def _reflect_mv_attributes(self, table, mv_state: ReflectedMVState):
        """Set MV specific attributes from ReflectedMVState"""
        table.info[TableObjectInfoKey.DEFINITION] = mv_state.definition
        # table.dialect_options.setdefault(DialectName, {}).update(mv_state.table_options)


@log.class_logger
class StarRocksTableDefinitionParser(object):
    """
    This parser is responsible for interpreting the raw data returned from
    StarRocks' `information_schema` and `SHOW` commands.

    For columns, the base attributes (name, type, nullable, default) are
    parsed here, leveraging the underlying MySQL dialect where possible.
    This dialect-specific implementation adds logic to parse StarRocks-specific
    attributes that are not present in standard MySQL, such as the aggregation
    type on a column (e.g., 'SUM', 'REPLACE', 'KEY'). This is achieved by
    querying `SHOW FULL COLUMNS` and processing the 'Extra' field.

    Other standard column attributes are assumed to be handled correctly by
    the base MySQL dialect's reflection mechanisms.

    MySQLTableDefinitionParser uses regex to parse information, so it's not
    used here.
    """

    _COLUMN_TYPE_PATTERN = re.compile(r"^(?P<type>\w+)(?:\s*\((?P<args>.*?)\))?\s*(?:(?P<attr>unsigned))?$")
    _TABLE_KEY_PATTERN = re.compile(r'\s*(\w+\s+KEY)\s*\((.*)\)\s*', re.IGNORECASE)
    _BUCKETS_PATTERN = re.compile(r'\sBUCKETS\s+(\d+)', re.IGNORECASE)
    _BUCKETS_REPLACE_PATTERN = re.compile(r'\s+BUCKETS\s+\d+', re.IGNORECASE)
    _PARTITION_BY_PATTERN = re.compile(r"PARTITION BY\s*(.+?)(?=\s*(?:DISTRIBUTED BY|ORDER BY|REFRESH|PROPERTIES|AS|\Z))", re.IGNORECASE | re.DOTALL)

    _VIEW_SECURITY_PATTERN = re.compile(r'\s+SECURITY\s+(INVOKER|DEFINER|NONE)\b', re.IGNORECASE)

    # Patterns to parse CREATE MATERIALIZED VIEW statement
    _MV_REFRESH_PATTERN = re.compile(r"\s*REFRESH\s+(.+?)(?=\s*(?:PARTITION BY|DISTRIBUTED BY|ORDER BY|PROPERTIES|AS|\Z))", re.IGNORECASE | re.DOTALL)
    _MV_PROPERTIES_PATTERN = re.compile(r"\s*PROPERTIES\s*\((.+?)\)(?=\s*(?:PARTITION BY|DISTRIBUTED BY|ORDER BY|REFRESH|AS|\Z))", re.IGNORECASE | re.DOTALL)
    _MV_AS_DEFINITION_PATTERN = re.compile(r"\s*AS\s*((?:WITH|SELECT)\s*.+)", re.IGNORECASE | re.DOTALL)

    def __init__(self, dialect, preparer):
        self.dialect = dialect
        self.preparer = preparer
        self._re_csv_int = _re_compile(r"\d+")
        self._re_csv_str = _re_compile(r"\x27(?:\x27\x27|[^\x27])*\x27")

    def parse(
        self,
        table: _DecodingRow,
        table_config: Dict[str, Any],
        columns: List[_DecodingRow],
        column_2_agg_type: Dict[str, str],
        column_autoinc: dict[str, bool],
        charset: str,
    ) -> ReflectedState:
        """
        Parses the raw reflection data into a structured ReflectedState object.

        :param table: A row from `information_schema.tables`.
        :param table_config: A dictionary representing a row from `information_schema.tables_config`,
                             augmented with the 'PARTITION_CLAUSE'.
        :param columns: A list of rows from `information_schema.columns`.
        :param column_2_agg_type: A dictionary mapping column names to their aggregation types.
        :param charset: The character set of the table.
        :return: A ReflectedState object containing the parsed table information.
        """
        # logger.debug("parsing table: %s", table.TABLE_NAME)
        reflected_table_info = ReflectedState(
            table_name=table.TABLE_NAME,
            columns=[
                self._parse_column(
                    column=column,
                    col_2_autoinc=column_autoinc,
                    **{ColumnAggInfoKeyWithPrefix.AGG_TYPE: column_2_agg_type.get(column.COLUMN_NAME)},
                )
                for column in columns
            ],
            table_options=self._parse_table_options(
                table.TABLE_NAME, schema=table.TABLE_SCHEMA,
                table=table, table_config=table_config, columns=columns
            ),
            keys=[{
                "type": self._get_mysql_key_type(table_config=table_config),
                "columns": [(c, None, None) for c in self._get_key_columns(columns=columns)],
                "parser": None,
                "name": None,
            }],
        )
        logger.debug("reflected table info for table: %r, info: '%s'", table.TABLE_NAME, reflected_table_info)
        return reflected_table_info

    def _parse_column(self, column: _DecodingRow, col_2_autoinc: dict | None = None, **kwargs: Any) -> dict:
        """
        Parse column from information_schema.columns table.
        It returns dictionary with column informations expected by sqlalchemy.

        Args:
            column: A row from `information_schema.columns`.
            col_2_autoinc: A dictionary of column names and their auto increment info.
                The key is the column name, the value is True/False whether the column has auto increment.
                Example: `{"col1": True, "col2": False, ...}`
            kwargs: Additional keyword arguments, with prefix `starrocks_`, passed to the dialect.
                currently only support:
                    - starrocks_is_agg_key: Whether the column is a key column.
                    - starrocks_agg_type: The aggregate type of the column.

        Returns:
            A dictionary with column information expected by sqlalchemy.
            It's the same as the `ReflectedColumn` object.
        """
        col_data = {
            "name": column.COLUMN_NAME,
            "type": self._parse_column_type(column=column),
            "nullable": column.IS_NULLABLE == "YES",
            "default": column.COLUMN_DEFAULT or None,
            "autoincrement": col_2_autoinc.get(column.COLUMN_NAME, False) if col_2_autoinc else False,
            "comment": column.COLUMN_COMMENT or None,
            "dialect_options": {
                k: v for k, v in kwargs.items() if v is not None
            }
        }
        if column.GENERATION_EXPRESSION:
            col_data["computed"] = {
                "sqltext": column.GENERATION_EXPRESSION
            }

        # logger.debug("parsed column data for column: %s is %s", column.COLUMN_NAME, col_data)

        return col_data

    def _parse_column_type(self, column: _DecodingRow) -> Any:
        """
        Parse column type from information_schema.columns table.
        It splits column type into type and arguments.
        After that it creates instance of column type.

        Some special cases:
            - LARGEINT: treat 'bigint(20) unsigned' as 'LARGEINT'
        """
        try:
            return parse_data_type(column.COLUMN_TYPE)
        except Exception as e:
            logger.warning(f"Could not parse type string '{column.COLUMN_TYPE}' for column '{column.COLUMN_NAME}'. Error: {e}")
            match = self._COLUMN_TYPE_PATTERN.match(column.COLUMN_TYPE)
            if match:
                type_ = match.group("type")
            else:
                type_ = column.COLUMN_TYPE

            util.warn(
                "Did not recognize type '%s' of column %r" % (type_, column.COLUMN_NAME)
            )
            return sqltypes.NullType

    def _get_mysql_key_type(self, table_config: Dict[str, Any]) -> str:
        """
        Get key type from information_schema.tables_config table.
        And return the MySQL's key type, as MySQLKeyType
        But, directly return the MySQLKeyType.PRIMARY, for check only
        """
        # table_model_to_key_type_map: Dict[str, MySQLKeyType] = {
        #     TableModel.DUP_KEYS: MySQLKeyType.UNIQUE,
        #     TableModel.DUP_KEYS2: MySQLKeyType.UNIQUE,
        #     TableModel.AGG_KEYS: MySQLKeyType.UNIQUE,
        #     TableModel.AGG_KEYS2: MySQLKeyType.UNIQUE,
        #     TableModel.PRI_KEYS: MySQLKeyType.PRIMARY,
        #     TableModel.PRI_KEYS2: MySQLKeyType.PRIMARY,
        #     TableModel.UNQ_KEYS: MySQLKeyType.UNIQUE,
        #     TableModel.UNQ_KEYS2: MySQLKeyType.UNIQUE,
        # }
        # return str(table_model_to_key_type_map.get(table_config.get(TableConfigKey.TABLE_MODEL), "").value)
        return str(MySQLKeyType.PRIMARY.value)

    def _get_key_columns(self, columns: List[_DecodingRow]) -> List[str]:
        """
        Get list of key columns (COLUMN_KEY) from information_schema.columns table.
        It returns list of column names that are part of key.

        Currently, we can't extract the key columns from information_schema.tables_config.
        """
        sorted_columns = sorted(columns, key=lambda col: col.ORDINAL_POSITION)
        return [c.COLUMN_NAME for c in sorted_columns if c.COLUMN_KEY]

    @staticmethod
    def parse_key_clause(table_key: str) -> Optional[ReflectedTableKeyInfo]:
        """
        Parses a raw TABLE KEY clause string into a structured ReflectedTableKeyInfo object.
        It's not used now.
        """
        if not table_key or table_key.strip() == "":
            return None
        key_match = StarRocksTableDefinitionParser._TABLE_KEY_PATTERN.search(table_key)
        if key_match:
            type = key_match.group(1)
            columns = key_match.group(2)
            return ReflectedTableKeyInfo(type=type, columns=columns)
        else:
            logger.error(f"Invalid table key clause: '{table_key}'")
            return None

    @staticmethod
    def parse_partition_clause(partition_clause: str) -> Optional[ReflectedPartitionInfo]:
        """
        Parses a raw PARTITION BY clause string into a structured ReflectedPartitionInfo object.

        This method handles RANGE, LIST, and expression partitioning schemes. It
        extracts the partition type, or expression used for partitioning,
        and any pre-defined partition clauses
        (e.g., `(PARTITION p1 VALUES LESS THAN ('100'), PARTITION p2 VALUES LESS THAN ('200'))`).

        Args:
            partition_clause: The raw string of the PARTITION BY clause from a
                `SHOW CREATE TABLE` statement.

        Returns:
            A `ReflectedPartitionInfo` object containing the parsed details.
        """
        if not partition_clause:
            return None

        clause_upper = partition_clause.strip().upper()
        partition_method: str
        pre_created_partitions: Optional[str] = None

        # Check for RANGE or LIST partitioning
        if clause_upper.startswith(PartitionType.RANGE) or clause_upper.startswith(PartitionType.LIST):
            partition_type = PartitionType.RANGE if clause_upper.startswith(PartitionType.RANGE) else PartitionType.LIST

            # Find the end of the RANGE/LIST(...) part using robust parenthesis matching
            open_paren_index = partition_clause.find('(')
            if open_paren_index != -1:
                close_paren_index = utils.find_matching_parenthesis(partition_clause, open_paren_index)
                if close_paren_index != -1:
                    partition_method = partition_clause[:close_paren_index + 1].strip()
                    rest = partition_clause[close_paren_index + 1:].strip()
                    if rest:
                        pre_created_partitions = rest
                else:  # Fallback for mismatched parentheses
                    raise SQLParseError(f"Invalid partition clause, mismatched parentheses: {partition_clause}")
            else:  # Fallback for no parentheses
                raise SQLParseError(f"Invalid partition clause, no columns specified: {partition_clause}")
        else:
            # If not RANGE or LIST, it's an expression-based partition
            # normalize partition method to be without outer parentheses
            partition_type = PartitionType.EXPRESSION
            partition_method = TableAttributeNormalizer.remove_outer_parentheses(partition_clause)

        return ReflectedPartitionInfo(
            type=partition_type,
            partition_method=partition_method,
            pre_created_partitions=pre_created_partitions
        )

    @staticmethod
    def parse_distribution_clause(distribution: str) -> Union[ReflectedDistributionInfo, None]:
        """Parse DISTRIBUTED BY string to extract distribution method and buckets.
        Args:
            distribution: String like "HASH(id) BUCKETS 8" or "HASH(id)"
        Returns:
            ReflectedDistributionInfo object
        """
        if not distribution:
            return None
        buckets_match = StarRocksTableDefinitionParser._BUCKETS_PATTERN.search(distribution)

        if buckets_match:
            buckets = int(buckets_match.group(1))
            # Remove BUCKETS part to get pure distribution
            distribution_method = StarRocksTableDefinitionParser._BUCKETS_REPLACE_PATTERN.sub('', distribution).strip()
        else:
            buckets = None
            distribution_method = distribution

        return ReflectedDistributionInfo(
            type=None,
            columns=None,
            distribution_method=distribution_method,
            buckets=buckets,
        )

    @staticmethod
    def parse_distribution(distribution: Optional[Union[ReflectedDistributionInfo, str]]
                           ) -> Union[ReflectedDistributionInfo, None]:
        if not distribution or isinstance(distribution, ReflectedDistributionInfo):
            return distribution
        return StarRocksTableDefinitionParser.parse_distribution_clause(distribution)

    def _get_distribution_info(self, table_config: dict[str, Any]) -> ReflectedDistributionInfo:
        """
        Get distribution from information_schema.tables_config table.
        It returns ReflectedDistributionInfo representation of distribution option.
        """
        return ReflectedDistributionInfo(
            type=table_config.get(TableConfigKey.DISTRIBUTE_TYPE),
            columns=table_config.get(TableConfigKey.DISTRIBUTE_KEY),
            distribution_method=None,
            buckets=table_config.get(TableConfigKey.DISTRIBUTE_BUCKET),
        )

    def _parse_common_table_options(self, table_row: Optional[_DecodingRow]) -> Dict[str, Any]:
        """
        Parses common options from an information_schema.tables row.
        """
        opts = {}
        if not table_row:
            return opts
        if table_row.TABLE_COMMENT and table_row.TABLE_COMMENT != 'OLAP':
            # logger.debug("table.TABLE_COMMENT: %s", table_row.TABLE_COMMENT)
            opts[TableInfoKeyWithPrefix.COMMENT] = table_row.TABLE_COMMENT
        return opts

    def _parse_general_table_options(self, table_name: str, schema: Optional[str] = None, table_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Parses general table options from a `tables_config` dict (not the original _DecodingRow object).
        This logic is shared between table and materialized view reflection.
        """
        table_fqn = utils.gen_simple_qualified_name(table_name, schema)
        logger.debug("parse general table options for table: %r.", table_fqn)

        opts = {}
        if partition_clause := table_config.get(TableConfigKey.PARTITION_CLAUSE):
            # logger.debug("table_config.%s: %s", TableConfigKey.PARTITION_CLAUSE, partition_clause)
            opts[TableInfoKeyWithPrefix.PARTITION_BY] = self.parse_partition_clause(partition_clause)

        distribute_key = table_config.get(TableConfigKey.DISTRIBUTE_KEY)
        distribute_type = table_config.get(TableConfigKey.DISTRIBUTE_TYPE)
        if distribute_key or distribute_type:
            # logger.debug("table_config.%s: %s", TableConfigKey.DISTRIBUTE_KEY, distribute_key)
            # logger.debug("table_config.%s: %s", TableConfigKey.DISTRIBUTE_TYPE, distribute_type)
            opts[TableInfoKeyWithPrefix.DISTRIBUTED_BY] = str(self._get_distribution_info(table_config))

        if sort_key := table_config.get(TableConfigKey.SORT_KEY):
            # logger.debug("table_config.%s: %s", TableConfigKey.SORT_KEY, sort_key)
            opts[TableInfoKeyWithPrefix.ORDER_BY] = sort_key

        if properties := table_config.get(TableConfigKey.PROPERTIES):
            # logger.debug("table_config.%s: %s", TableConfigKey.PROPERTIES, properties)
            try:
                opts[TableInfoKeyWithPrefix.PROPERTIES] = dict(json.loads(properties or "{}").items())
            except json.JSONDecodeError:
                logger.info(f"properties are not valid JSON: {properties}")
        return opts

    def _parse_table_options(self, table_name: str, schema: Optional[str],
            table: _DecodingRow, table_config: Dict[str, Any], columns: List[_DecodingRow]
        ) -> Dict:
        """
        Parse table options from `information_schema` views,
        and generate the table options with `starrocks_` prefix, which will be used to reflect a Table().
        Then, these options will be exactly the same as the options of a sqlalchemy.Table()
        which is created by users manually, for both sqlalchemy.Table() or ORM styles.

        NOTE: partition info is extracted from SHOW CREATE TABLE rather than directly from information_schema.tables_config.
            But, here we can directly use table_config, bucause they are already filled into the table_config dict.

        Args:
            table: A row from `information_schema.tables`.
            table_config: A dictionary representing a row from `information_schema.tables_config`,
                             augmented with the 'PARTITION_CLAUSE'.
            columns: A list of rows from `information_schema.columns`.

        Returns:
            A dictionary of StarRocks-specific table options with the 'starrocks_' prefix.
        """
        opts = self._parse_common_table_options(table)
        table_fqn = utils.gen_simple_qualified_name(table_name, schema)
        logger.debug("parse table options for table: %r.", table_fqn)

        # Set partition, distribution, sort key, properties
        opts.update(self._parse_general_table_options(table_name, schema, table_config))

        if table_engine := table_config.get(TableConfigKey.TABLE_ENGINE):
            # logger.debug("table_config.%s: %s", TableConfigKey.TABLE_ENGINE, table_engine)
            # if table_engine.upper() != TableEngine.OLAP:
            #     raise NotImplementedError(f"Table engine {table_engine} is not supported now.")
            opts[TableInfoKeyWithPrefix.ENGINE] = table_engine.upper()

        # Get key type from information_schema.tables_config.TABLE_MODEL,
        # and key columns from information_schema.columns.COLUMN_KEY
        if table_model := table_config.get(TableConfigKey.TABLE_MODEL):
            # logger.debug("table_config.%s: %s", TableConfigKey.TABLE_MODEL, table_model)
            # convert to key string, such as "PRIMARY_KEY", not PRIMARY KEY"
            key_str = TableInfoKey.MODEL_TO_KEY_MAP.get(table_model)
            if key_str:
                key_columns_str = ", ".join(self._get_key_columns(columns))
                prefixed_key = f"{SRKwargsPrefix}{key_str}"
                opts[prefixed_key] = key_columns_str

        return opts

    def parse_view(
        self,
        view_row: _DecodingRow,
        table_row: Optional[_DecodingRow],
        column_rows: List[_DecodingRow],
        create_view_sql: Optional[str] = None
    ) -> ReflectedViewState:
        """
        Parses raw reflection data into a structured ReflectedViewState object.

        Args:
            view_row: Row from information_schema.views
            table_row: Optional row from information_schema.tables (for comment)
            column_rows: Rows from information_schema.columns (for column names, types, and comments)
            create_view_sql: Optional CREATE VIEW statement from SHOW CREATE VIEW (to parse SECURITY)

        Returns:
            A ReflectedViewState object with parsed view information
        """
        state = ReflectedViewState(
            table_name=view_row.TABLE_NAME,
            definition=view_row.VIEW_DEFINITION,
        )

        # Parse columns using standard _parse_column method
        # For views, we care about name, type, and comment
        # Type and nullable are inferred from the SELECT statement
        if column_rows:
            state.columns = [
                self._parse_column(col)
                for col in column_rows
            ]

        table_options = self._parse_common_table_options(table_row)

        # table_options[TableInfoKeyWithPrefix.SECURITY] = view_row.SECURITY_TYPE.upper()
        # Parse SECURITY from SHOW CREATE VIEW output
        # Note: information_schema.views.SECURITY_TYPE is always empty in StarRocks (v3.5)
        if security := self._parse_sql_security_from_create_view(create_view_sql):
            table_options[TableInfoKeyWithPrefix.SECURITY] = security

        state.table_options = table_options
        return state

    def _parse_sql_security_from_create_view(self, create_view_sql: Optional[str]) -> Optional[str]:
        """
        Parse SECURITY clause from CREATE VIEW statement.

        Args:
            create_view_sql: CREATE VIEW statement from SHOW CREATE VIEW

        Returns:
            'INVOKER' or 'DEFINER' if found, None otherwise

        Example:
            CREATE VIEW v1 SECURITY INVOKER AS SELECT ...
            -> Returns 'INVOKER'
        """
        if not create_view_sql:
            return None

        # Match: SECURITY {INVOKER|DEFINER|NONE}
        match = self._VIEW_SECURITY_PATTERN.search(create_view_sql)

        if match:
            security_type = match.group(1).upper()
            # logger.debug("Parsed SECURITY: %s from CREATE VIEW: %s", security_type, create_view_sql[:200])
            return security_type
        else:
            logger.debug("No SECURITY match in CREATE VIEW: %s", create_view_sql[:200] if create_view_sql else 'None')

        return None

    def parse_mv(
        self,
        mv_row: _DecodingRow,
        table_row: Optional[_DecodingRow],
        config_row: Optional[_DecodingRow],
    ) -> ReflectedMVState:
        """
        Parses all raw reflection data for a Materialized View into a ReflectedMVState.
        This is the main entry point for MV reflection parsing.

        Args:
            mv_row: A row from `information_schema.materialized_views`.
            table_row: An optional row from `information_schema.tables` for the same mv
            config_row: An optional row from `information_schema.tables_config` for the same mv
        Returns:
            A ReflectedMVState object.
        """
        ddl = mv_row.MATERIALIZED_VIEW_DEFINITION.strip()
        mv_fqn = utils.gen_simple_qualified_name(mv_row.TABLE_NAME, mv_row.TABLE_SCHEMA)
        # logger.debug("mv create ddl for %r: %s", mv_fqn, ddl)

        mv_name, schema = mv_row.TABLE_NAME, mv_row.TABLE_SCHEMA
        # 1. Parse the DDL to get properties that are only available there.
        #   Includes partition, refresh, and properties.
        try:
            parsed_state = self._parse_mv_ddl(mv_name, ddl, schema)
        except Exception as e:
            logger.warning(f"Failed to parse DDL for MV '{mv_row.TABLE_SCHEMA}.{mv_row.TABLE_NAME}', reflection may be incomplete: {e}")
            parsed_state = ReflectedMVState(table_name=mv_row.TABLE_NAME, definition=ddl)
        # logger.debug("partial parsed mv state. mv: %s, state: %s", mv_fqn, parsed_state)

        # 2. Augment/overwrite with more reliable info from other sources.
        parsed_state.table_options.update(self._parse_common_table_options(table_row))

        if config_row:
            general_options = self._parse_general_table_options(mv_name, schema, table_config=config_row)
            logger.debug("parsed general table options for mv: %s, options: %s", mv_fqn, general_options)
            parsed_state.table_options.update(general_options)

        logger.debug("parsed mv state. mv: %s, state: %s", mv_fqn, parsed_state)
        return parsed_state

    def _parse_mv_ddl(
        self,
        mv_name: str,
        create_mv_ddl: str,
        schema: Optional[str] = None
    ) -> ReflectedMVState:
        """
        Parses the DDL from SHOW CREATE MATERIALIZED VIEW.
        This is the main entry point for MV reflection.
        """
        # Extract AS SELECT definition first
        mv_definition = None
        definition_match = self._MV_AS_DEFINITION_PATTERN.search(create_mv_ddl)
        if definition_match:
            mv_definition = definition_match.group(1).strip()
            clauses_str = create_mv_ddl[:definition_match.start()]
        else:
            raise SQLParseError(f"Could not find 'AS SELECT' in CREATE MATERIALIZED VIEW statement for {mv_name}", create_mv_ddl)

        state = ReflectedMVState(
            table_name=mv_name,
            definition=mv_definition,
        )

        partition_match = self._PARTITION_BY_PATTERN.search(clauses_str)
        if partition_match:
            state.table_options[TableInfoKeyWithPrefix.PARTITION_BY] = self.parse_partition_clause(partition_match.group(1).strip())

        # Use Lark parser for the refresh clause
        try:
            refresh_text_match = self._MV_REFRESH_PATTERN.search(clauses_str)
            if refresh_text_match:
                refresh_clause_str = refresh_text_match.group(0).strip()
                logger.debug("mv: %r, refresh_clause: %r", mv_name, refresh_clause_str)
                parsed_refresh = parse_mv_refresh_clause(refresh_clause_str)
                state.table_options[TableInfoKeyWithPrefix.REFRESH] = ReflectedRefreshInfo(
                    moment=parsed_refresh.get("refresh_moment"),
                    type=parsed_refresh.get("refresh_type")
                )
        except Exception as e:
            logger.warning(f"Failed to parse refresh clause for MV {mv_name}, falling back to regex: {e}")
            # Fallback to simple regex if lark parsing fails
            self._parse_mv_refresh_with_regex(clauses_str, state)

        # NOTE: currently, it uses properties from information_schema.tables_config, not from the DDL.
        # properties_match = self._MV_PROPERTIES_PATTERN.search(clauses_str)
        # if properties_match:
        #     # Use string instead of dictionary now.
        #     # state.mv_options.properties = self._parse_properties(properties_match.group(1))
        #     state.table_options[TableInfoKeyWithPrefix.PROPERTIES] = properties_match.group(1).strip()

        return state

    def _parse_mv_refresh_with_regex(self, ddl_part: str, state: ReflectedMVState):
        """Fallback refresh clause parser using regex."""
        refresh_match = re.search(r"REFRESH\s+(.+?)(?=\s*(?:PROPERTIES|AS))", ddl_part, re.IGNORECASE | re.DOTALL)
        if refresh_match:
            refresh_text = refresh_match.group(1).strip().upper()
            moment, type = None, None
            parts = refresh_text.split()
            if len(parts) > 0:
                if parts[0] in ("IMMEDIATE", "DEFERRED"):
                    moment = parts.pop(0)
                if parts:
                    type = " ".join(parts)
            state.table_options[TableInfoKeyWithPrefix.REFRESH] = ReflectedRefreshInfo(moment=moment, type=type)

    def _parse_properties(self, props_str: str) -> Dict[str, str]:
        """
        Parses the content of a PROPERTIES clause into a dictionary.
        This implementation uses regex to correctly handle commas and escaped quotes within property values.
        """
        # Regex to find all key-value pairs, respecting quotes.
        # It captures the key in group 1 and the value in group 2.
        # The value part `((?:\\"|[^"])*)` handles escaped quotes `\"` inside the value string.
        pattern = re.compile(r'"([^"]+)"\s*=\s*"((?:\\"|[^"])*)"')
        matches = pattern.findall(props_str)
        if matches:
            properties = {key: value.replace('\\"', '"') for key, value in matches}
        else:
            properties = {}
        return properties
