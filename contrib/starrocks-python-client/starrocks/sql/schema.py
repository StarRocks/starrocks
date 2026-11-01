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

import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from sqlalchemy import Column, Table, event
from sqlalchemy.schema import MetaData
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy.sql.selectable import Selectable
from sqlalchemy.sql.util import find_tables

from starrocks.common.params import DialectName, TableInfoKey, TableInfoKeyWithPrefix, TableKind, TableObjectInfoKey
from starrocks.datatype import STRING


logger = logging.getLogger(__name__)


DefinitionType = Union[str, "Selectable"]
ColumnDefinition = Union[Column, str, Dict[str, Any]]


def extract_view_columns(table: Table) -> Union[List[Dict[str, Any]], None]:
    """
    Extract column information from a View/Table object for serialization.

    This function works with both:
    - View objects (user-defined in metadata)
    - Table objects representing views (reflected from database)

    This is the inverse operation of View._normalize_columns():
    - _normalize_columns: dict/str -> Column objects (for View creation)
    - extract_view_columns: Column objects -> dict (for Alembic operations)

    Args:
        table: A View or Table object with columns

    Returns:
        List of dicts (``{"name": str, "comment": str}``), or None if no columns

    Note:
        StarRocks VIEW columns only support name and comment (not type/nullable).
        This is used by Alembic operations to serialize view columns for migration scripts.

    Example:
        >>> view = View('v1', metadata, Column('id', STRING(), comment='ID'))
        >>> extract_view_columns(view)
        [{'name': 'id', 'comment': 'ID'}]

        >>> # Also works with reflected Table objects
        >>> reflected_table = metadata.tables['my_view']
        >>> extract_view_columns(reflected_table)
        [{'name': 'col1', 'comment': 'Comment'}]
    """
    if not table.columns:
        return None
    return [
        {'name': col.name, 'comment': col.comment}
        for col in table.columns
    ]


TableReference = Tuple[Optional[str], str]

_SQL_TOKEN = re.compile(
    r"'(?:[^'\\]|\\.|'')*'|\"(?:[^\"\\]|\\.)*\"|/\*.*?\*/|(?:--|#)[^\n]*|\[[^\]]*\]"
    r"|`((?:[^`]|``)+)`"
    r"|((?:[A-Za-z_$\u0080-\uffff]|[0-9]+[A-Za-z_$\u0080-\uffff])[A-Za-z0-9_$\u0080-\uffff]*)"
    r"|([(),.])",
    re.DOTALL,
)
"""A query token: a backquoted name, a word or punctuation; literals, comments and join hints match uncaptured.

A word is a name as StarRocks' lexer reads one: letters, digits, ``_`` and ``$``, any character from U+0080 up
counting as a letter, starting with a digit only when a letter follows the digits, as in ``2024_sales``.
"""
_TABLE_REFERENCE_KEYWORDS = frozenset({"FROM", "JOIN"})
_FUNCTIONS_TAKING_FROM = frozenset({"EXTRACT", "TRIM"})
_QUERY_KEYWORDS = frozenset({"SELECT", "WITH", "VALUES"})
_JOIN_CONDITION_KEYWORDS = frozenset({"ON", "USING"})
_FROM_LIST_END_KEYWORDS = frozenset(
    {"WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "UNION", "EXCEPT", "INTERSECT", "WINDOW", "QUALIFY"}
)
_QueryToken = Tuple[Optional[str], Optional[str], Optional[str]]


def tables_read_by_query(sql: str) -> Set[TableReference]:
    """Return the ``(schema, name)`` of each table a query reads, lower-cased, with None for an unqualified name.

    A table reference is a name after FROM or JOIN, or after a comma of a FROM list, in subqueries and
    parenthesised joins too; a comma after a join's ON or USING condition continues the list.
    ``catalog.db.table`` gives ``db`` as the schema. Columns, column aliases, the FROM of ``EXTRACT(... FROM d)``
    and ``TRIM(... FROM s)``, table functions, names a WITH clause defines, comments, string literals and join
    hints such as ``[shuffle]`` are not table references.
    """
    tokens = _query_tokens(sql)
    common_table_expressions = _common_table_expression_names(tokens)
    return {
        (schema, name)
        for schema, name in _from_clause_references(tokens)
        if schema is not None or name not in common_table_expressions
    }


EVERY_COLUMN = "*"
"""What :func:`names_in_query` holds for a query that may read every column of a table it reads."""


def names_in_query(sql: str) -> Set[str]:
    """Return every name a query holds, lower-cased, and :data:`EVERY_COLUMN` when it holds a ``*``.

    A name is a word or a backquoted name outside literals, comments and join hints: a column, table, alias,
    function or keyword. So a column the query reads is among them, whatever qualifies it. A ``*`` outside
    them, as in ``SELECT *`` or ``t.*``, may read every column; so, as far as this reading tells, may the one of
    ``COUNT(*)`` or of a multiplication.
    """
    names: Set[str] = set()
    end = 0
    for match in _SQL_TOKEN.finditer(sql):
        if "*" in sql[end:match.start()]:
            names.add(EVERY_COLUMN)
        end = match.end()
        name = _token_name(match.groups())
        if name is not None:
            names.add(name.lower())
    if "*" in sql[end:]:
        names.add(EVERY_COLUMN)
    return names


def _query_tokens(sql: str) -> List[_QueryToken]:
    """Split a query into (backquoted name, word, punctuation) tokens, leaving out literals, comments and hints."""
    return [match.groups() for match in _SQL_TOKEN.finditer(sql) if any(match.groups())]


def _token_name(token: _QueryToken) -> Optional[str]:
    """Return a name token's name, with a backquoted name's doubled backquotes undone, or None for punctuation."""
    quoted, word, _ = token
    if quoted is not None:
        return quoted.replace("``", "`")
    return word


def _from_clause_references(tokens: List[_QueryToken]) -> Set[TableReference]:
    """Return the ``(schema, name)`` of every name in table position, CTE names included.

    A parenthesis opened where a table is expected holds a parenthesised join, whose first name is a table, or a
    subquery, which starts with a query keyword and has a FROM list of its own. A parenthesis opened by EXTRACT
    or TRIM holds a FROM that reads a column.
    """
    references: Set[TableReference] = set()
    from_list_depths: List[int] = []
    function_from_depths: Set[int] = set()
    depth = 0
    expect_table = False
    index = 0
    while index < len(tokens):
        _, word, punctuation = tokens[index]
        previous_word = tokens[index - 1][1] if index > 0 else None
        index += 1
        keyword = word.upper() if word else None
        if punctuation == "(":
            depth += 1
            if expect_table:
                from_list_depths.append(depth)
            if previous_word is not None and previous_word.upper() in _FUNCTIONS_TAKING_FROM:
                function_from_depths.add(depth)
        elif punctuation == ")":
            function_from_depths.discard(depth)
            depth -= 1
            while from_list_depths and from_list_depths[-1] > depth:
                from_list_depths.pop()
        elif punctuation == ",":
            expect_table = bool(from_list_depths) and from_list_depths[-1] == depth
        elif punctuation == ".":
            continue
        elif keyword == "FROM" and depth in function_from_depths:
            continue
        elif keyword in _TABLE_REFERENCE_KEYWORDS:
            expect_table = True
            if not from_list_depths or from_list_depths[-1] != depth:
                from_list_depths.append(depth)
        elif keyword in _JOIN_CONDITION_KEYWORDS:
            expect_table = False
        elif keyword in _FROM_LIST_END_KEYWORDS:
            if from_list_depths and from_list_depths[-1] == depth:
                from_list_depths.pop()
            expect_table = False
        elif keyword == "LATERAL" and expect_table:
            continue
        elif keyword in _QUERY_KEYWORDS and expect_table:
            if tokens[index - 2][2] == "(" and from_list_depths and from_list_depths[-1] == depth:
                from_list_depths.pop()
            expect_table = False
        elif expect_table:
            parts = [_token_name(tokens[index - 1])]
            while index + 1 < len(tokens) and tokens[index][2] == "." and not tokens[index + 1][2]:
                parts.append(_token_name(tokens[index + 1]))
                index += 2
            is_table_function = index < len(tokens) and tokens[index][2] == "("
            if not is_table_function:
                references.add((parts[-2].lower() if len(parts) > 1 else None, parts[-1].lower()))
            expect_table = False
    return references


def _common_table_expression_names(tokens: List[_QueryToken]) -> Set[str]:
    """Return the lower-cased names the query's WITH clauses define, in subqueries too."""
    names: Set[str] = set()
    for start, (_, word, _) in enumerate(tokens):
        if word is None or word.upper() != "WITH":
            continue
        expect_name = True
        depth = 0
        for token in tokens[start + 1:]:
            _, word, punctuation = token
            if expect_name:
                if word is not None and word.upper() == "RECURSIVE":
                    continue
                name = _token_name(token)
                if name is None:
                    break
                names.add(name.lower())
                expect_name = False
            elif punctuation == "(":
                depth += 1
            elif punctuation == ")":
                depth -= 1
            elif depth == 0 and punctuation == ",":
                expect_name = True
            elif depth == 0 and word is not None and word.upper() != "AS":
                break
    return names


class View(Table):
    """Represents a View object."""

    _explicit_dependencies: Set[Table]

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args,
        definition: Optional[DefinitionType] = None,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        columns: Optional[List[ColumnDefinition]] = None,
        keep_existing: bool = False,
        extend_existing: bool = False,
        _no_init: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        Create a View object.

        Args:
            name: Name of the view
            metadata: MetaData object to bind the view to
            *args: Column objects (optional). If provided, these columns will be used
                   to define the view's schema explicitly. This is useful for
                   Alembic autogenerate to compare column changes.
            definition: SQL string or SQLAlchemy Selectable object defining the view query (required)
                Even for reflection, it should be explicitly set to a placeholder value, such as '<placeholder_definition>'.
                Otherwise, it will raise Exception.
            schema: Schema name (optional)
            comment: View comment (optional)
            columns: List of column definitions (optional). Can be:
                - Column objects: Column('id', Integer) as standard SQLAlchemy Column objects
                - Strings: 'id' (just column name)
                - Dicts: {'name': 'id', 'comment': 'User ID'}
            keep_existing: When True, if the view already exists in the MetaData,
                ignore further arguments and return the existing View object.
                This allows defining a view that may already be defined elsewhere.
            extend_existing: When True, if the view already exists in the MetaData,
                apply further arguments to update the existing View object.
                This allows modifying an existing view definition.
            **kwargs: Additional keyword arguments, including:
                - starrocks_security: Security mode (INVOKER or NONE). Note: DEFINER is not supported by StarRocks.
                - Other dialect-specific parameters with starrocks_ prefix for future use

        Examples:
            # String definition without explicit columns
            View('v1', metadata, definition='SELECT * FROM users')

            # With Column objects (standard way)
            View('v1', metadata,
                 Column('id', STRING()),
                 Column('name', STRING(), comment='User name'),
                 definition='SELECT id, name FROM users')

            # With simplified column definitions
            View('v1', metadata,
                 definition='SELECT id, name FROM users',
                 columns=['id', {'name': 'name', 'comment': 'User name'}])

            # Selectable (type-safe) definition
            stmt = select(users.c.id, users.c.name)
            View('v1', metadata, definition=stmt)

            # With security and comment
            View('v1', metadata,
                 definition='SELECT * FROM users',
                 comment='User view',
                 starrocks_security='INVOKER')

            # Update existing view definition
            View('v1', metadata,
                 definition='SELECT id, name FROM users',
                 comment='Updated view',
                 extend_existing=True)

            # Get existing view reference (or create if not exists)
            View('v1', metadata,
                 definition='SELECT * FROM users',
                 keep_existing=True)

        Notes:
            - It must not raise error if `definition` is not valid when checking, because it will cause RecursionError.
              use `after_parent_attach` event to validate definition.
              The same to other attributes, such as `columns`
            - StarRocks VIEW columns only support name and comment (not type/nullable)
            - Column types are automatically inferred from the SELECT statement, but useless now
            - Type parameter in Column() is a placeholder for SQLAlchemy compatibility
        """
        # Follow Table's pattern: skip initialization if _no_init=True
        # This happens when:
        # 1. Python automatically calls __init__ after __new__ returns (for existing tables)
        # 2. Getting an existing view without extend_existing
        if _no_init:
            return

        # Prepare view-specific info
        view_info = {TableObjectInfoKey.TABLE_KIND: TableKind.VIEW}
        # Process definition (handles both str and Selectable)
        view_info.update(self._process_definition(definition))

        # Prepare view info for Table.__init__
        object_info = kwargs.setdefault("info", {})
        object_info.update(view_info)

        # Convert simplified column definitions to Column objects
        normalized_columns = []
        if columns:
            normalized_columns.extend(self._normalize_columns(columns, object_info))
        # Merge with *args columns
        args = list(args) + normalized_columns

        # Let Table to handle comment, columns, and starrocks_* parameters
        # Pass keep_existing and extend_existing to Table for proper singleton behavior
        # logger.debug(f"View.__init__('{name}'): view_info={view_info}, kwargs['info'] id={id(kwargs.get('info'))}")
        super().__init__(name, metadata, *args, schema=schema, comment=comment,
                        keep_existing=keep_existing, extend_existing=extend_existing,
                        _no_init=False, **kwargs)
        # logger.debug(f"  After Table.__init__: self.info id={id(self.info)}, self.info={self.info}")

    def _init_existing(self, *args, **kwargs):
        """
        Override Table._init_existing to handle View-specific parameters.

        This is called when extend_existing=True and the view already exists in metadata.
        We need to extract View-specific parameters before passing to Table._init_existing.
        """
        # Extract View-specific parameters
        definition = kwargs.pop('definition', None)
        columns = kwargs.pop('columns', None)

        # Update view definition if provided
        # if definition is not None:
        view_info = self._process_definition(definition)
        self.info.update(view_info)

        # Handle columns parameter (View-specific simplified syntax)
        # Convert to Column objects and prepend to args so Table._init_existing can process them
        if columns:
            normalized_columns = self._normalize_columns(columns, self.info)
            args = args + tuple(normalized_columns)

        super()._init_existing(*args, **kwargs)

    @staticmethod
    def _process_definition(definition: Optional[DefinitionType]) -> Dict[str, Any]:
        """
        Process view definition and return view_info dict.

        This is a helper method to avoid code duplication between __init__ and _init_existing.

        Args:
            definition: SQL string or SQLAlchemy Selectable object

        Returns:
            Dict containing DEFINITION and optionally SELECTABLE keys

        Raises:
            TypeError: If definition is not str or Selectable
        """
        view_info = {}

        if isinstance(definition, str):
            view_info[TableObjectInfoKey.DEFINITION] = definition
        else:
            # Compile Selectable to SQL string
            from sqlalchemy.sql import ClauseElement

            from starrocks.dialect import stored_sql_text

            if isinstance(definition, ClauseElement):
                view_info[TableObjectInfoKey.DEFINITION] = stored_sql_text(definition)
                view_info[TableObjectInfoKey.SELECTABLE] = definition
            else:
                # raise TypeError(f"definition must be str or Selectable, got {type(definition)}")
                view_info[TableObjectInfoKey.DEFINITION] = definition
        # logger.debug("View._process_definition: view_info=%r", view_info)

        return view_info

    @staticmethod
    def _normalize_columns(columns: List[ColumnDefinition], info: Dict[str, Any]) -> List[Column]:
        """
        Convert simplified column definitions to Column objects.

        Args:
            columns: List of column definitions (Column, str, or dict)
            info: Keyword arguments passed to View constructor

        Returns:
            List of Column objects

        Raises:
            TypeError: If column definition is invalid
        """
        result = []
        for col_def in columns:
            if isinstance(col_def, Column):
                # Already a Column object
                result.append(col_def)
            elif isinstance(col_def, str):
                # String: just column name
                result.append(Column(col_def, STRING()))
            elif isinstance(col_def, dict):
                # Dict: name + optional comment
                if 'name' not in col_def:
                    info.setdefault('invalid_columns', []).append(col_def)
                    # raise ValueError(f"Column dict must have 'name' key: {col_def}")
                else:
                    result.append(Column(
                        col_def['name'],
                        STRING(),
                        comment=col_def.get('comment')
                    ))
            else:
                info.setdefault('invalid_columns', []).append(col_def)
                # raise TypeError(
                #     f"Invalid column definition: {col_def}. "
                #     f"Expected Column object, str, or dict with 'name' key."
                # )
                # It must not raise error in View constructor, because it will cause RecursionError.
        return result

    @property
    def _extra_dependencies(self) -> Set[Table]:
        """The tables to create before this view: those added with ``add_is_dependent_on()`` and those it reads.

        ``MetaData.create_all()`` creates unrelated tables in the order they were defined, and
        ``MetaData.sorted_tables`` sorts them by name, so without these a view defined, or named, before its base
        table would come first, and fail to create. A Selectable definition names the tables it reads; a string
        definition reads the tables :func:`tables_read_by_query` finds in it, matched as :meth:`_reads_table`
        describes.
        """
        return self._explicit_dependencies | self._tables_read_by_definition()

    @_extra_dependencies.setter
    def _extra_dependencies(self, tables: Set[Table]) -> None:
        self._explicit_dependencies = tables

    def add_is_dependent_on(self, table: Table) -> None:
        """Create this view after ``table``, as ``Table.add_is_dependent_on`` does."""
        self._explicit_dependencies.add(table)

    def _tables_read_by_definition(self) -> Set[Table]:
        """Return the other tables of this view's MetaData that its definition reads.

        A Selectable names the tables it reads. A ``text()`` definition names none to SQLAlchemy, nor does a
        Selectable that reads only ``text()`` or ``table()`` FROMs, so its compiled SQL is read as a string
        definition is.
        """
        if self.selectable is not None:
            tables = {table for table in find_tables(self.selectable) if isinstance(table, Table) and table is not self}
            if tables:
                return tables
        if not isinstance(self.definition, str):
            return set()
        references = tables_read_by_query(self.definition)
        tables = [table for table in self.metadata.tables.values() if table is not self]
        return {table for table in tables if self._reads_table(table, references, tables)}

    def _reads_table(self, table: Table, references: Set[TableReference], tables: List[Table]) -> bool:
        """Return whether the definition's references name ``table``.

        An unqualified name is a table of this view's schema or of the default one. A qualified ``db.name`` is the
        table in schema ``db``, or the default schema's table of that name when the MetaData has no ``db.name``,
        since the default schema may be ``db``.
        """
        name = table.name.lower()
        schema = table.schema.lower() if table.schema else None
        own_schema = self.schema.lower() if self.schema else None
        for reference_schema, reference_name in references:
            if reference_name != name:
                continue
            if reference_schema is None and schema in (None, own_schema):
                return True
            if reference_schema is not None and schema == reference_schema:
                return True
            if reference_schema is not None and schema is None and not any(
                other.name.lower() == name and other.schema and other.schema.lower() == reference_schema
                for other in tables
            ):
                return True
        return False

    @property
    def definition(self) -> str:
        return self.info.get(TableObjectInfoKey.DEFINITION, "")

    @property
    def selectable(self) -> Optional["Selectable"]:
        """Get original Selectable object if created from one"""
        return self.info.get(TableObjectInfoKey.SELECTABLE, None)

    @property
    def security(self) -> Optional[str]:
        from starrocks.common.params import DialectName, TableInfoKey
        return self.dialect_options.get(DialectName, {}).get(TableInfoKey.SECURITY)


@event.listens_for(View, "after_parent_attach")
def validate_definition(view: View, connection):
    if not view.definition:
        raise ValueError("View/MV definition is required. Use definition='SELECT ...' parameter.")
    if not isinstance(view.definition, (str, ClauseElement)):
        raise TypeError("View/MV definition must be a string or a Selectable object")
    if 'invalid_columns' in view.info:
        raise ValueError(f"Invalid column definitions: {view.info['invalid_columns']!r}. "
                         f"Each Column should be an object, str, or dict with 'name' key.")


class MaterializedView(View):
    """Represents a Materialized View object in Python."""

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args,
        definition: Optional[DefinitionType] = None,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        columns: Optional[List[ColumnDefinition]] = None,
        keep_existing: bool = False,
        extend_existing: bool = False,
        _no_init: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        Create a Materialized View object.

        Args:
            name: Name of the materialized view
            metadata: MetaData object to bind the materialized view to
            *args: Column objects (optional)
            definition: SQL string or SQLAlchemy Selectable object defining the MV query (required)
            schema: Schema name (optional)
            comment: Materialized view comment (optional)
            columns: List of column definitions (optional), same format as View
            keep_existing: When True, if the materialized view already exists, ignore further
                arguments and return the existing object.
            extend_existing: When True, if the materialized view already exists, apply further
                arguments to update the existing object.
            **kwargs: Additional keyword arguments, including:
                - starrocks_partition_by: Partition expression (e.g., 'date_trunc("day", created_at)')
                - starrocks_distributed_by: Distribution method (e.g., 'HASH(user_id) BUCKETS 10')
                - starrocks_order_by: Order by columns (e.g., 'user_id, created_at')
                - starrocks_refresh: Refresh mode (e.g., 'ASYNC', 'MANUAL', 'IMMEDIATE ASYNC')
                    Format: [IMMEDIATE|DEFERRED] [ASYNC|MANUAL]
                    With neither it nor starrocks_distributed_by, StarRocks builds a synchronous materialized
                    view, which reflection and autogenerate can't see, and the dialect warns.
                - starrocks_properties: Additional properties dict (e.g., {'replication_num': '3'})
                - Other dialect-specific parameters with starrocks_ prefix

        Examples:
            # Basic materialized view, refreshed when its base tables change
            MaterializedView('mv1', metadata, definition='SELECT * FROM users', starrocks_refresh='ASYNC')

            # With partition and refresh
            MaterializedView('user_stats_mv', metadata,
                           definition='SELECT user_id, COUNT(*) FROM orders GROUP BY user_id',
                           starrocks_partition_by='user_id',
                           starrocks_refresh='ASYNC')

            # With all options
            MaterializedView('order_mv', metadata,
                           Column('user_id', INTEGER),
                           Column('total', INTEGER, comment='Total orders'),
                           definition='SELECT user_id, COUNT(*) as total FROM orders GROUP BY user_id',
                           comment='User order statistics',
                           starrocks_partition_by='user_id',
                           starrocks_distributed_by='HASH(user_id) BUCKETS 10',
                           starrocks_order_by='user_id',
                           starrocks_refresh='IMMEDIATE ASYNC',
                           starrocks_properties={'replication_num': '3'})

            # With columns (simplified syntax)
            MaterializedView('mv1', metadata,
                           definition='SELECT id, name FROM users',
                           columns=['id', {'name': 'name', 'comment': 'User name'}])

            # Update existing MV definition
            MaterializedView('mv1', metadata,
                           definition='SELECT id, name, email FROM users',
                           starrocks_refresh='MANUAL',
                           extend_existing=True)
        """
        # First, call the parent View's __init__ to handle the definition
        # and other common parameters.
        super().__init__(name, metadata, *args, definition=definition, schema=schema,
                        comment=comment, columns=columns,
                        keep_existing=keep_existing, extend_existing=extend_existing,
                        _no_init=_no_init, **kwargs)

        # Then, override the table_kind to be specific to MaterializedView.
        # Only do this if we're actually initializing (not skipping due to _no_init)
        if not _no_init:
            self.info[TableObjectInfoKey.TABLE_KIND] = TableKind.MATERIALIZED_VIEW

    def _init_existing(self, *args, **kwargs):
        """
        Override View._init_existing to handle MV-specific parameters.

        This is called when extend_existing=True and the MV already exists in metadata.
        We need to extract MV-specific parameters before passing to View._init_existing.
        """
        # Extract MV-specific parameters from kwargs
        partition_by = kwargs.pop(TableInfoKeyWithPrefix.PARTITION_BY, None)
        distributed_by = kwargs.pop(TableInfoKeyWithPrefix.DISTRIBUTED_BY, None)
        order_by = kwargs.pop(TableInfoKeyWithPrefix.ORDER_BY, None)
        refresh = kwargs.pop(TableInfoKeyWithPrefix.REFRESH, None)
        properties = kwargs.pop(TableInfoKeyWithPrefix.PROPERTIES, None)

        # Update dialect_options if provided
        dialect_opts = self.dialect_options.setdefault(DialectName, {})
        if partition_by is not None:
            dialect_opts[TableInfoKey.PARTITION_BY] = partition_by
        if distributed_by is not None:
            dialect_opts[TableInfoKey.DISTRIBUTED_BY] = distributed_by
        if order_by is not None:
            dialect_opts[TableInfoKey.ORDER_BY] = order_by
        if refresh is not None:
            dialect_opts[TableInfoKey.REFRESH] = refresh
        if properties is not None:
            dialect_opts[TableInfoKey.PROPERTIES] = properties

        # Call parent to handle View-specific parameters (definition, columns, comment, security)
        super()._init_existing(*args, **kwargs)

    @property
    def partition_by(self) -> Optional[str]:
        return self.dialect_options.get(DialectName, {}).get(TableInfoKey.PARTITION_BY)

    @property
    def distributed_by(self) -> Optional[str]:
        return self.dialect_options.get(DialectName, {}).get(TableInfoKey.DISTRIBUTED_BY)

    @property
    def order_by(self) -> Optional[str]:
        return self.dialect_options.get(DialectName, {}).get(TableInfoKey.ORDER_BY)

    @property
    def refresh(self) -> Optional[str]:
        return self.dialect_options.get(DialectName, {}).get(TableInfoKey.REFRESH)

    @property
    def properties(self) -> Optional[Dict[str, str]]:
        return self.dialect_options.get(DialectName, {}).get(TableInfoKey.PROPERTIES)
