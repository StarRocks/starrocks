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
from typing import Any, Collection, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from sqlalchemy import log, types as sqltypes, util
from sqlalchemy.dialects.mysql.base import _DecodingRow
from sqlalchemy.dialects.mysql.reflection import _re_compile
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.schema import Table

from starrocks.common.consts import TableConfigKey
from starrocks.common.defaults import ReflectionMVDefaults
from starrocks.common.params import (
    ColumnAggInfoKeyWithPrefix,
    DialectName,
    SRKwargsPrefix,
    TableInfoKey,
    TableInfoKeyWithPrefix,
    TableKind,
    TableObjectInfoKey,
)
from starrocks.common.types import PartitionType, TableEngine
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


class hashdict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))


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
        with self._operation_context() as conn:
            return self.dialect.get_view(conn, view_name, schema=schema, **kwargs)

    def get_materialized_view_names(self, schema: Optional[str] = None, **kwargs: Any) -> List[str]:
        """Return the names of the materialized views in ``schema``, which SQLAlchemy 1.4's inspector cannot.

        Autogenerate compares materialized views on every run, so without it autogenerate fails on 1.4 even for
        a metadata that has none.
        """
        with self._operation_context() as conn:
            return self.dialect.get_materialized_view_names(conn, schema, info_cache=self.info_cache, **kwargs)

    def get_materialized_view(self, view_name: str, schema: Optional[str] = None, **kwargs: Any) -> Optional[ReflectedMVState]:
        """
        Retrieves information about a specific materialized view.

        :param view_name: The name of the materialized view to inspect.
        :param schema: The schema of the materialized view; defaults to the default schema name if None.
        :param kwargs: Additional arguments passed to the dialect's get_materialized_view method.
        :return: A ReflectedMVState object, or None if the materialized view does not exist.
        """
        with self._operation_context() as conn:
            return self.dialect.get_materialized_view(conn, view_name, schema=schema, **kwargs)

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
        if _reflect_info: # SQLAlchemy 2.0
            super().reflect_table(table, include_columns, exclude_columns, resolve_fks, _extend_on, _reflect_info)
        else: # SQLAlchemy 1.4
            super().reflect_table(table, include_columns, exclude_columns, resolve_fks, _extend_on)
        # comment is already set to Table.comment, the starrocks_comment is not used again.
        self._delete_comment_from_dialect_options(table)

        # 1. Get table_kind and parsed_state, which will use the cached parsed_state)
        with self._operation_context() as conn:
            parsed_state = self.dialect._parsed_state_or_create(conn, table.name, table.schema, info_cache=self.info_cache)
        table_kind = parsed_state.table_kind
        logger.debug("reflect %s: %s, parsed_state: %s.", table_kind, table.name, parsed_state)

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


_SHOW_CREATE_QUOTED = re.compile(r'`(?:[^`]|``)*`|"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'', re.DOTALL)
_SHOW_CREATE_DEFINITION_END = re.compile(_SHOW_CREATE_QUOTED.pattern + r'|\sCOMMENT\s+"|\n', re.DOTALL)
_SHOW_CREATE_LINE_START_NAME = re.compile(r"\n[ \t]*`((?:[^`]|``)+)`[ \t]")
_SHOW_CREATE_COLUMN_LIST_END = re.compile(r"\n\)\s*ENGINE\s*=", re.IGNORECASE)
_SHOW_CREATE_TABLE_CLAUSES_START = re.compile(
    r"\n\)[ \t]*ENGINE[ \t]*=[ \t]*\w+[ \t]*\n"
    r"(?=(?:DUPLICATE|PRIMARY|UNIQUE|AGGREGATE) KEY\(|COMMENT \"|PARTITION BY |DISTRIBUTED BY |PROPERTIES \()",
    re.IGNORECASE,
)
_SHOW_CREATE_INDEX_LINE_END = re.compile(r",\n|\n\)")
_SHOW_CREATE_COMMENT_OPENING = re.compile(r'\sCOMMENT\s+"')
_SHOW_CREATE_COMMENT_CLOSING = re.compile(r'"(?=,\n|\n\))')
_SHOW_CREATE_DEFAULT = re.compile(r"\sDEFAULT\s+(\S)")
_SHOW_CREATE_AUTO_INCREMENT = re.compile(r"\bNULL\s+AUTO_INCREMENT\b", re.IGNORECASE)

ColumnComments = Optional[Mapping[str, Optional[str]]]
"""Each column's COLUMN_COMMENT from information_schema.columns, by column name."""

ColumnTypes = Optional[Mapping[str, Optional[str]]]
"""Each column's COLUMN_TYPE from information_schema.columns, by column name."""


def show_create_column_specs(
    create_table: str,
    column_names: Optional[Sequence[str]] = None,
    column_comments: ColumnComments = None,
    column_types: ColumnTypes = None,
) -> Dict[str, str]:
    """Return each column's definition from SHOW CREATE TABLE, after its type and up to its comment, with literals
    and backquoted names emptied.

    ``column_names`` are the table's columns in ORDINAL_POSITION order, and ``column_comments`` and
    ``column_types`` their comments and types, as information_schema lists them. Each definition is on the line
    that starts with the column's backquoted name, and it ends at its COMMENT clause, which StarRocks prints last,
    or at the end of the line.

    StarRocks prints a comment's newlines as they are, and 3.3 leaves a ``"`` unescaped in the column comments of a
    PRIMARY KEY table, so where a comment ends can't be told from its quotes. StarRocks 3.3 also prints a STRUCT's
    field names bare, so a ``'`` or ``"`` in a field name pairs with no quote of its own. Each column's type is
    stepped over by its COLUMN_TYPE instead, which is how SHOW CREATE TABLE prints it, and each comment by its text,
    escaped or as it is, and the next column's line must follow the comment. The definition is the text between
    them: so no field name and no line of a multi-line comment is read as part of it.

    Where a type is not given, or printed otherwise, the definition starts at the type, and it is read only when it
    ends on its own line: a quote in the type may otherwise pair with one on a later column's line. A definition
    after a known type may run onto a later line, as a generated column's literal can, only when its comment then
    follows it. Where a comment is not given, or is printed as neither, the next column's line is searched for, and
    a line is read only if it looks like a column's: it follows a ``,`` line end, and its definition ends at a
    COMMENT clause on the same line. A line whose own comment then follows it is preferred. A column without such a
    line is left out, as is a column SHOW CREATE TABLE does not list: reflection then renders its default from
    information_schema alone, and reports no AUTO_INCREMENT. Without ``column_names``, the columns are the
    backquoted names that start a line.

    An emptied literal or name keeps its quotes, so ``DEFAULT "AUTO_INCREMENT"`` reads as ``DEFAULT ""``; the
    DEFAULT literal is escaped on every version. A view's DDL lists no columns.
    """
    if create_table.lstrip().upper().startswith("CREATE VIEW"):
        return {}
    spans, _ = _column_definition_spans(create_table, column_names, column_comments, column_types)
    return {name: _SHOW_CREATE_QUOTED.sub(_emptied_literal, create_table[start:end]) for name, start, end in spans}


def show_create_partition_clause(
    create_table: str,
    column_names: Optional[Sequence[str]] = None,
    column_comments: ColumnComments = None,
    column_types: ColumnTypes = None,
    index_comments: Optional[Sequence[Optional[str]]] = None,
) -> Optional[str]:
    """Return the clause after ``PARTITION BY`` in SHOW CREATE TABLE, or None when the table is not partitioned.

    The clause is looked for after the column list, outside the literals there. The column list ends after the last
    column's comment (see :func:`show_create_column_specs` for ``column_names``, ``column_comments`` and
    ``column_types``) and the index lines (see :func:`_index_lines_end` for ``index_comments``), at the
    ``) ENGINE=`` line that a table clause follows. So neither a column or index comment, the table's comment nor a
    partition value that holds ``PARTITION BY`` or the keyword of a later clause is read as part of it. The one
    exception is a LIST partition value that holds a quote: StarRocks prints the values as they are, so the clause
    may then run on into the clauses after it, though its LIST method still reads right. The clause is returned as
    SHOW CREATE TABLE prints it.
    """
    _, columns_end = _column_definition_spans(create_table, column_names, column_comments, column_types)
    clauses = create_table[_column_list_end(create_table, _index_lines_end(create_table, columns_end, index_comments)):]
    bounds = StarRocksTableDefinitionParser.partition_clause_bounds(_SHOW_CREATE_QUOTED.sub(_blanked_literal, clauses))
    return (clauses[bounds[0]:bounds[1]].strip() or None) if bounds else None


def _index_lines_end(create_table: str, position: int, index_comments: Optional[Sequence[Optional[str]]]) -> int:
    """Return where the index lines after ``position`` end, each stepped over by its comment.

    ``index_comments`` are the table's index comments, in the order SHOW INDEX lists the indexes, which is the
    order of their lines. StarRocks prints an index's comment last on its line, in single quotes and as it is, so a
    comment may hold a line such as ``) ENGINE=OLAP``. Each comment is stepped over by its text, up to the end of
    its line; an index without a comment is passed over. Where a comment's clause isn't found, the lines are not
    stepped any further.
    """
    for comment in index_comments or ():
        if not comment:
            continue
        clause = f" COMMENT '{comment}'"
        found = create_table.find(clause, position)
        while found != -1 and not _SHOW_CREATE_INDEX_LINE_END.match(create_table, found + len(clause)):
            found = create_table.find(clause, found + 1)
        if found == -1:
            break
        position = found + len(clause)
    return position


def _column_list_end(create_table: str, position: int) -> int:
    """Return where the column list ends: at the first ``) ENGINE=`` line after ``position`` that a table clause
    follows, such as the key, COMMENT, PARTITION BY or PROPERTIES; failing that, at the first ``) ENGINE=`` line;
    failing that, at ``position``. An external catalog's table has no ``ENGINE=``."""
    for pattern in (_SHOW_CREATE_TABLE_CLAUSES_START, _SHOW_CREATE_COLUMN_LIST_END):
        match = pattern.search(create_table, position)
        if match:
            return match.start()
    return position


def _column_definition_spans(
    create_table: str, column_names: Optional[Sequence[str]], column_comments: ColumnComments, column_types: ColumnTypes
) -> Tuple[List[Tuple[str, int, int]], int]:
    """Return each column's name and where its definition starts and ends, and where the columns' text ends.

    The columns are read as :func:`show_create_column_specs` describes. The columns' text ends after the last
    column's comment, or at the end of its definition when that comment could not be stepped over.
    """
    if column_names is None:
        column_names = list(dict.fromkeys(_line_start_column_names(create_table)))
    comments = column_comments or {}
    types = column_types or {}
    spans: List[Tuple[str, int, int]] = []
    position = max(create_table.find("\n"), 0)
    line_follows = True
    for name in column_names:
        comment, column_type = comments.get(name), types.get(name)
        definition = _column_line_at(create_table, name, position, comment, column_type) if line_follows else None
        if definition is None:
            definition = _column_line_searched(create_table, name, position, comment, column_type)
        if definition is None:
            line_follows = False
            continue
        start, end = definition
        spans.append((name, start, end))
        comment_end = _known_comment_end(create_table, end, comment)
        line_follows = comment_end is not None
        position = end if comment_end is None else comment_end
    return spans, position


def _line_start_column_names(create_table: str) -> List[str]:
    """Return the backquoted names that start a line of the statement, in order: its columns' names."""
    return [match.group(1).replace("``", "`") for match in _SHOW_CREATE_LINE_START_NAME.finditer(create_table)]


def _column_line_pattern(column_name: str, line_end_before: str) -> re.Pattern[str]:
    """Match the start of the column's line, after ``line_end_before``, up to its definition."""
    name = re.escape("`" + column_name.replace("`", "``") + "`")
    return re.compile(line_end_before + r"[ \t]*" + name + r"[ \t]")


def _column_line_at(
    create_table: str, column_name: str, position: int, comment: Optional[str], column_type: Optional[str]
) -> Optional[Tuple[int, int]]:
    """Return where the column's definition starts and ends, when the column's line starts right at ``position``.

    None when the definition runs onto a later line without what :func:`_definition_is_bounded` asks for.
    """
    match = _column_line_pattern(column_name, r",?\n").match(create_table, position)
    if match is None:
        return None
    start, end, after_known_type = _definition_bounds(create_table, match.end(), column_type)
    return (start, end) if _definition_is_bounded(create_table, start, end, after_known_type, comment) else None


def _column_line_searched(
    create_table: str, column_name: str, position: int, comment: Optional[str], column_type: Optional[str]
) -> Optional[Tuple[int, int]]:
    """Return where the column's definition starts and ends on the first line after ``position`` that looks like its.

    A line looks like the column's when it follows a ``,`` (or the first column's ``(``) line end and its definition
    ends at a COMMENT clause on the same line, after the column's type when it is ``column_type``. A line whose
    comment is ``comment`` is taken over an earlier one.
    """
    first_found: Optional[Tuple[int, int]] = None
    for match in _column_line_pattern(column_name, r"[,(]\n").finditer(create_table, position):
        start, end, _ = _definition_bounds(create_table, match.end(), column_type)
        if "\n" in create_table[start:end] or not _SHOW_CREATE_COMMENT_OPENING.match(create_table, end):
            continue
        if _known_comment_end(create_table, end, comment) is not None:
            return start, end
        first_found = first_found or (start, end)
    return first_found


def _definition_bounds(create_table: str, name_end: int, column_type: Optional[str]) -> Tuple[int, int, bool]:
    """Return where the column definition after the name that ends at ``name_end`` starts and ends, and whether it
    starts after the type, as it does when the type is ``column_type``."""
    type_end = _known_type_end(create_table, name_end, column_type)
    start = name_end if type_end is None else type_end
    return start, _column_definition_end(create_table, start), type_end is not None


def _known_type_end(create_table: str, definition_start: int, column_type: Optional[str]) -> Optional[int]:
    """Return where the column's type ends, when the definition at ``definition_start`` starts with ``column_type``.

    SHOW CREATE TABLE prints a type as information_schema's COLUMN_TYPE gives it, nested types included, except
    BOOLEAN and LARGEINT, whose COLUMN_TYPE is ``tinyint(1)`` and ``bigint(20) unsigned``: their definitions start
    at the type, which holds no quote. None when the type is not known, or is printed otherwise.
    """
    if column_type and create_table.startswith(column_type + " ", definition_start):
        return definition_start + len(column_type)
    return None


def _definition_is_bounded(
    create_table: str, start: int, end: int, after_known_type: bool, comment: Optional[str]
) -> bool:
    """Whether the definition between ``start`` and ``end`` can't have run over a later column's line.

    It can't when it ends on its own line. Past a known type, the literals of the definition pair with each other, so
    it may run onto a later line, as a generated column's literal can, when its own comment, stepped over by its
    text, then follows it.
    """
    if "\n" not in create_table[start:end]:
        return True
    return (
        after_known_type
        and _SHOW_CREATE_COMMENT_OPENING.match(create_table, end) is not None
        and _known_comment_end(create_table, end, comment) is not None
    )


def _column_definition_end(create_table: str, start: int) -> int:
    """Return where the column definition starting at ``start`` ends: at its COMMENT clause or line end."""
    for match in _SHOW_CREATE_DEFINITION_END.finditer(create_table, start):
        if match.group(0)[0] not in "`\"'":
            return match.start()
    return len(create_table)


def _known_comment_end(create_table: str, definition_end: int, comment: Optional[str]) -> Optional[int]:
    """Return where the COMMENT clause at ``definition_end`` ends, after its closing quote, when it holds ``comment``.

    StarRocks prints a column comment with ``\\`` and ``"`` escaped, except in the column comments of a 3.3 PRIMARY
    KEY table, which it prints as they are; both spellings are tried. The closing quote must end the column's line.
    An empty comment may have no clause at all, as in an external catalog's table: the line then ends at
    ``definition_end``. None when the clause holds neither, or when the comment is not known.
    """
    opening = _SHOW_CREATE_COMMENT_OPENING.match(create_table, definition_end)
    if opening is None:
        return definition_end if comment == "" and create_table.startswith("\n", definition_end) else None
    if comment is None:
        return None
    text_start = opening.end()
    for printed in dict.fromkeys((comment.replace("\\", "\\\\").replace('"', '\\"'), comment)):
        closing = text_start + len(printed)
        if create_table.startswith(printed, text_start) and _SHOW_CREATE_COMMENT_CLOSING.match(create_table, closing):
            return closing + 1
    return None


def _emptied_literal(match: re.Match[str]) -> str:
    """Empty a string literal or a backquoted name down to its quotes."""
    return match.group(0)[0] * 2


def _blanked_literal(match: re.Match[str]) -> str:
    """Blank out a literal or backquoted name between its quotes, keeping its length, so positions still match."""
    literal = match.group(0)
    return literal[0] + " " * (len(literal) - 2) + literal[-1]


def column_default_kinds(
    create_table: str,
    column_names: Optional[Sequence[str]] = None,
    column_comments: ColumnComments = None,
    column_types: ColumnTypes = None,
) -> Dict[str, str]:
    """Return how SHOW CREATE TABLE spells each column's default: ``literal``, ``expression``, ``keyword`` or ``none``.

    A quoted default is a literal, a parenthesized one an expression such as ``(uuid())``, and a bare one a keyword
    such as CURRENT_TIMESTAMP. See :func:`show_create_column_specs` for ``column_names``, ``column_comments`` and
    ``column_types``.
    """
    kinds: Dict[str, str] = {}
    for name, spec in show_create_column_specs(create_table, column_names, column_comments, column_types).items():
        default = _SHOW_CREATE_DEFAULT.search(" " + spec)
        if default is None:
            kinds[name] = "none"
        else:
            kinds[name] = {'"': "literal", "'": "literal", "(": "expression"}.get(default.group(1), "keyword")
    return kinds


def column_auto_increments(
    create_table: str,
    column_names: Optional[Sequence[str]] = None,
    column_comments: ColumnComments = None,
    column_types: ColumnTypes = None,
) -> Dict[str, bool]:
    """Return whether SHOW CREATE TABLE gives each column AUTO_INCREMENT, outside its comment and default.

    StarRocks prints AUTO_INCREMENT right after the column's NULL or NOT NULL, which follow its type, so it is read
    only there, after the type and outside backquoted names: a STRUCT field named ``auto_increment`` or
    ``null auto_increment``, bare in 3.3, is not taken for it, nor a column of that name in a generated column's
    expression. See :func:`show_create_column_specs` for ``column_names``, ``column_comments`` and ``column_types``.
    """
    return {
        name: bool(_SHOW_CREATE_AUTO_INCREMENT.search(spec))
        for name, spec in show_create_column_specs(create_table, column_names, column_comments, column_types).items()
    }


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
    _PARTITION_BY_START = re.compile(r"\bPARTITION BY\s*", re.IGNORECASE)
    _PARTITION_BY_END = re.compile(r"[(\[)\]]|\b(?:DISTRIBUTED BY|ORDER BY|REFRESH|PROPERTIES|AS)\b", re.IGNORECASE)

    _ROW_PER_KEY_TABLE_KEYS = (TableInfoKey.PRIMARY_KEY, TableInfoKey.UNIQUE_KEY, TableInfoKey.AGGREGATE_KEY)

    _VIEW_SECURITY_PATTERN = re.compile(r'\s+SECURITY\s+(INVOKER|DEFINER|NONE)\b', re.IGNORECASE)

    # Patterns to parse CREATE MATERIALIZED VIEW statement
    _MV_REFRESH_PATTERN = re.compile(
        r"\s*\bREFRESH\s+(.+?)(?=\s*(?:\b(?:PARTITION BY|DISTRIBUTED BY|ORDER BY|PROPERTIES|AS)\b|\Z))",
        re.IGNORECASE | re.DOTALL,
    )
    _MV_PROPERTIES_PATTERN = re.compile(
        r"\s*\bPROPERTIES\s*\((.+?)\)(?=\s*(?:\b(?:PARTITION BY|DISTRIBUTED BY|ORDER BY|REFRESH|AS)\b|\Z))",
        re.IGNORECASE | re.DOTALL,
    )
    _MV_AS_DEFINITION_PATTERN = re.compile(r"\s*\bAS\s*((?:WITH|SELECT)\s*.+)", re.IGNORECASE | re.DOTALL)
    _MV_PROPERTIES_BLOCK_START = re.compile(r"\nPROPERTIES\s*\(\n", re.IGNORECASE)
    _MV_PROPERTIES_BLOCK_END = re.compile(r"\n\)[ \t]*(?=\n\s*AS\s*(?:WITH|SELECT)\b)", re.IGNORECASE)

    def __init__(self, dialect, preparer):
        self.dialect = dialect
        self.preparer = preparer
        self._re_csv_int = _re_compile(r"\d+")
        self._re_csv_str = _re_compile(r"\x27(?:\x27\x27|[^\x27])*\x27")

    def parse_table(
        self,
        table: _DecodingRow,
        table_config: Dict[str, Any],
        columns: List[_DecodingRow],
        column_2_agg_type: Dict[str, str] = None,
        column_autoinc: dict[str, bool] = None,
        charset: str = None,
        column_default_kinds: Optional[Dict[str, str]] = None,
        secondary_indexes: Optional[List[Dict[str, Any]]] = None,
    ) -> ReflectedState:
        """
        Parses the raw reflection data into a structured ReflectedState object.

        :param table: A row from `information_schema.tables`.
        :param table_config: A dictionary representing a row from `information_schema.tables_config`,
                             augmented with the 'PARTITION_CLAUSE'.
        :param columns: A list of rows from `information_schema.columns`.
        :param column_2_agg_type: A dictionary mapping column names to their aggregation types.
        :param charset: The character set of the table.
        :param column_default_kinds: How SHOW CREATE TABLE spells each column's default; see column_default_kinds.
        :return: A ReflectedState object containing the parsed table information.
        """
        # logger.debug("parsing table: %s", table.TABLE_NAME)
        sorted_columns = self._sort_columns(columns)
        reflected_table_info = ReflectedState(
            table_name=table.TABLE_NAME,
            columns=[
                self._parse_column(
                    column=column,
                    col_2_autoinc=column_autoinc,
                    default_kinds=column_default_kinds,
                    **{ColumnAggInfoKeyWithPrefix.AGG_TYPE: column_2_agg_type.get(column.COLUMN_NAME)} if column_2_agg_type else {},
                )
                for column in sorted_columns
            ],
            table_options=self._parse_table_options(
                table.TABLE_NAME, schema=table.TABLE_SCHEMA,
                table=table, table_config=table_config, sorted_columns=sorted_columns
            ),
            keys=[{
                "type": self._get_mysql_key_type(table_config=table_config),
                "columns": [(c, None, None) for c in self._get_key_columns(sorted_columns=sorted_columns)],
                "parser": None,
                "name": None,
            }],
            secondary_indexes=list(secondary_indexes or []),
        )
        logger.debug("reflected table info for table: %r, info: '%s'", table.TABLE_NAME, reflected_table_info)
        return reflected_table_info

    _INDEX_TYPE = re.compile(r"\s*(?P<type>\w+)\s*(?:\((?P<properties>.*)\))?\s*$", re.DOTALL)
    _INDEX_PROPERTY = re.compile(r'"((?:[^"\\]|\\.)*)"\s*=\s*"((?:[^"\\]|\\.)*)"')

    @classmethod
    def parse_show_index_rows(cls, rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
        """Turn the rows of ``SHOW INDEX FROM`` a table into SQLAlchemy reflected indexes.

        StarRocks lists every BITMAP, NGRAMBF, GIN or VECTOR index, one row per column, with its type and the
        properties it defaulted as well as those given in ``Index_type`` (``NGRAMBF("gram_num" = "4", ...)``).
        Each index has its type, properties and comment as ``starrocks_using``, ``starrocks_properties`` and
        ``starrocks_comment``.
        """
        indexes: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            index = indexes.get(row["Key_name"])
            if index is None:
                index = indexes[row["Key_name"]] = {
                    "name": row["Key_name"],
                    "column_names": [],
                    "unique": False,
                    "dialect_options": cls._index_dialect_options(row["Index_type"] or "", row["Comment"]),
                }
            index["column_names"].append(row["Column_name"])
        return list(indexes.values())

    @classmethod
    def _index_dialect_options(cls, index_type: str, comment: Optional[str]) -> Dict[str, Any]:
        """Return the ``starrocks_*`` options of an index from SHOW INDEX's ``Index_type`` and ``Comment``."""
        type_match = cls._INDEX_TYPE.match(index_type)
        dialect_options: Dict[str, Any] = {
            f"{DialectName}_using": (type_match.group("type") if type_match else index_type.strip()).upper()
        }
        if type_match and type_match.group("properties"):
            dialect_options[f"{DialectName}_properties"] = dict(cls._INDEX_PROPERTY.findall(type_match.group("properties")))
        if comment:
            dialect_options[f"{DialectName}_comment"] = comment
        return dialect_options

    def _parse_column(
        self,
        column: _DecodingRow,
        col_2_autoinc: dict | None = None,
        default_kinds: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> dict:
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
            "nullable": column.IS_NULLABLE != "NO",
            "default": self._ColumnDefaultValueRenderer().build(column, (default_kinds or {}).get(column.COLUMN_NAME)),
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

    class _ColumnDefaultValueRenderer:
        """
        Apply StarRocks-friendly default value rendering:
        1. DATETIME CURRENT_TIMESTAMP defaults are emitted verbatim.
        2. Function-looking defaults become `(func_expr)`.
        3. Remaining literals are single quoted SQL string literals.
        4. The NUL placeholder StarRocks reports for HLL, BITMAP and PERCENTILE columns is no default.
        """
        _SKETCH_COLUMN_TYPES = ("HLL", "BITMAP", "PERCENTILE")
        _SKETCH_PLACEHOLDER_DEFAULT = "\x00"

        def build(self, column: _DecodingRow, kind: Optional[str] = None) -> Optional[str]:
            """Render the column's default, as SHOW CREATE TABLE spells it when ``kind`` says how.

            information_schema reports the string ``'uuid()'`` and the expression ``(uuid())`` alike, and the
            string ``'NULL'`` as ``NULL``. SHOW CREATE TABLE tells them apart: ``kind`` is ``literal`` (quoted),
            ``expression`` (parenthesized), ``keyword`` (bare, such as CURRENT_TIMESTAMP) or ``none`` (no DEFAULT
            clause), from :func:`column_default_kinds`. Without it, the default is guessed from its text.
            """
            raw_default = getattr(column, "COLUMN_DEFAULT", None)
            if raw_default is None or kind == "none":
                return None
            default = str(raw_default)
            if self._is_sketch_placeholder(column, default):
                return None
            if kind == "literal":
                return utils.quote_sql_string_literal(default)
            if kind == "expression":
                return self._wrap_function_default(default)
            if kind == "keyword":
                return default.strip()
            return self._guessed_default(column, default.strip())

        def _guessed_default(self, column: _DecodingRow, default: str) -> Optional[str]:
            """Render a default from its text alone: NULL is none, and a call is taken for an expression."""
            if default.upper() == "NULL":
                return None
            if self._is_datetime_column(column) and default.upper().startswith("CURRENT_TIMESTAMP"):
                return default
            if self._looks_like_function(default):
                return self._wrap_function_default(default)
            return utils.quote_sql_string_literal(default)

        @classmethod
        def _is_sketch_placeholder(cls, column: _DecodingRow, default: str) -> bool:
            column_type = (getattr(column, "COLUMN_TYPE", "") or "").upper()
            return column_type in cls._SKETCH_COLUMN_TYPES and default == cls._SKETCH_PLACEHOLDER_DEFAULT

        @staticmethod
        def _is_datetime_column(column: _DecodingRow) -> bool:
            column_type = getattr(column, "COLUMN_TYPE", "") or ""
            return column_type.upper().startswith("DATETIME")

        @staticmethod
        def _looks_like_function(default: str) -> bool:
            return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*\(.*\)$", default))

        @staticmethod
        def _wrap_function_default(default: str) -> str:
            trimmed = default.strip()
            if trimmed.startswith("(") and trimmed.endswith(")"):
                return trimmed
            return f"({trimmed})"

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
        Map the table model in information_schema.tables_config to the MySQL key type
        that the inherited ``get_pk_constraint`` and ``get_indexes`` understand.

        PRIMARY KEY, UNIQUE KEY and AGGREGATE KEY tables hold one row per key, so their
        key columns are reported as the primary key. A DUPLICATE KEY table (or one whose
        model is unknown) is only sorted by its key columns, so it has no primary key.
        """
        table_model = table_config.get(TableConfigKey.TABLE_MODEL)
        if TableInfoKey.MODEL_TO_KEY_MAP.get(table_model) in self._ROW_PER_KEY_TABLE_KEYS:
            return str(MySQLKeyType.PRIMARY.value)
        return str(MySQLKeyType.DUPLICATE.value)

    def _sort_columns(self, columns: List[_DecodingRow]) -> List[_DecodingRow]:
        """
        Sort columns by ORDINAL_POSITION.
        """
        return sorted(columns, key=lambda col: col.ORDINAL_POSITION)

    def _get_key_columns(self, sorted_columns: List[_DecodingRow]) -> List[str]:
        """
        Get list of key columns (COLUMN_KEY) from information_schema.columns table.
        It returns list of column names that are part of key.

        Currently, we can't extract the key columns from information_schema.tables_config.
        """
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

    @classmethod
    def partition_clause_bounds(cls, masked_ddl: str) -> Optional[Tuple[int, int]]:
        """Return where the clause after ``PARTITION BY`` starts and ends, in DDL whose literals are blanked out.

        The clause ends at the next clause's keyword, as a word and outside parentheses, or at the end of the DDL.
        StarRocks prints partition names bare inside the partition list, so a partition named ``p_texas``, or even
        ``as``, is not taken for the AS keyword. A range partition's ``[a, b)`` counts as one pair. When the
        parentheses don't close, the clause ends at the first keyword. None when there is no PARTITION BY.
        """
        start = cls._PARTITION_BY_START.search(masked_ddl)
        if start is None:
            return None
        depth, first_nested_keyword = 0, None
        for token in cls._PARTITION_BY_END.finditer(masked_ddl, start.end()):
            if token.group(0) in ("(", "["):
                depth += 1
            elif token.group(0) in (")", "]"):
                depth -= 1
            elif depth <= 0:
                return start.end(), token.start()
            elif first_nested_keyword is None:
                first_nested_keyword = token.start()
        if depth > 0 and first_nested_keyword is not None:
            return start.end(), first_nested_keyword
        return start.end(), len(masked_ddl)

    @staticmethod
    def parse_partition_clause(partition_clause: str) -> Optional[ReflectedPartitionInfo]:
        """
        Parses a raw PARTITION BY clause string into a structured ReflectedPartitionInfo object.

        This method handles RANGE, LIST, and expression partitioning schemes. It
        extracts the partition type, or expression used for partitioning,
        and any pre-defined partition clauses
        (e.g., `(PARTITION p1 VALUES LESS THAN ('100'), PARTITION p2 VALUES LESS THAN ('200'))`).

        The parentheses of a RANGE or LIST method are matched outside literals, so a parenthesis in a string, such
        as the format of ``RANGE(str2date(s, '(%Y-%m-%d'))``, doesn't end the method early or late.

        Args:
            partition_clause: The raw string of the PARTITION BY clause from a
                `SHOW CREATE TABLE` statement.

        Returns:
            A `ReflectedPartitionInfo` object containing the parsed details.

        Raises:
            SQLParseError: for a RANGE or LIST clause whose parentheses don't match.
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
            masked_clause = _SHOW_CREATE_QUOTED.sub(_blanked_literal, partition_clause)
            open_paren_index = masked_clause.find('(')
            if open_paren_index != -1:
                close_paren_index = utils.find_matching_parenthesis(masked_clause, open_paren_index)
                if close_paren_index != -1:
                    partition_method = partition_clause[:close_paren_index + 1].strip()
                    rest = partition_clause[close_paren_index + 1:].strip()
                    if rest:
                        pre_created_partitions = rest
                else:  # Fallback for mismatched parentheses
                    raise SQLParseError("Invalid partition clause, mismatched parentheses", partition_clause)
            else:  # Fallback for no parentheses
                raise SQLParseError("Invalid partition clause, no columns specified", partition_clause)
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

    def _parse_common_table_options(self, table_kind: str, table_row: Optional[_DecodingRow]) -> Dict[str, Any]:
        """
        Parses common options from an information_schema.tables row.
        Args:
            table_kind: Table kind, such as 'TABLE' or 'MATERIALIZED VIEW'.
            table_row: A row from `information_schema.tables`.
        """
        opts = {}
        if not table_row:
            return opts
        if table_row.TABLE_COMMENT and table_row.TABLE_COMMENT != 'OLAP':
            # logger.debug("table.TABLE_COMMENT: %s", table_row.TABLE_COMMENT)
            if table_kind == TableKind.MATERIALIZED_VIEW and table_row.TABLE_COMMENT == 'MATERIALIZED_VIEW':
                pass  # SR will automatically set the COMMENT `MATERIALIZED_VIEW` for MV if not provided
            elif table_kind == TableKind.VIEW and table_row.TABLE_COMMENT == 'VIEW':
                pass  # SR will automatically set the COMMENT `VIEW` for VIEW if not provided
            else:
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
                opts[TableInfoKeyWithPrefix.PROPERTIES] = hashdict(json.loads(properties or "{}").items())
            except json.JSONDecodeError:
                logger.info(f"properties are not valid JSON: {properties}")
        return opts

    def _parse_table_options(self, table_name: str, schema: Optional[str],
            table: _DecodingRow, table_config: Dict[str, Any], sorted_columns: List[_DecodingRow]
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
            sorted_columns: A list of rows from `information_schema.columns`, sorted by ORDINAL_POSITION.

        Returns:
            A dictionary of StarRocks-specific table options with the 'starrocks_' prefix.
        """
        opts = self._parse_common_table_options(TableKind.TABLE, table)
        table_fqn = utils.gen_simple_qualified_name(table_name, schema)
        logger.debug("parse table options for table: %r.", table_fqn)

        # Set partition, distribution, sort key, properties
        opts.update(self._parse_general_table_options(table_name, schema, table_config))

        if table_engine := table_config.get(TableConfigKey.TABLE_ENGINE):
            # logger.debug("table_config.%s: %s", TableConfigKey.TABLE_ENGINE, table_engine)
            # change CLOUD_NATIVE to OLAP for shared-data mode.
            if table_engine.upper() == TableEngine.CLOUD_NATIVE:
                table_engine = TableEngine.OLAP
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
                key_columns_str = ", ".join(self._get_key_columns(sorted_columns))
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
            sorted_columns = self._sort_columns(column_rows)
            state.columns = [self._parse_column(col) for col in sorted_columns]

        table_options = self._parse_common_table_options(TableKind.VIEW, table_row)

        # table_options[TableInfoKeyWithPrefix.SECURITY] = view_row.SECURITY_TYPE.upper()
        # Parse SECURITY from SHOW CREATE VIEW output
        # Note: information_schema.views.SECURITY_TYPE is always empty in StarRocks (v3.5)
        if security := self._parse_sql_security_from_create_view(create_view_sql):
            table_options[TableInfoKeyWithPrefix.SECURITY] = security

        state.table_options = table_options
        return state

    @staticmethod
    def _view_clauses_with_literals_blanked(create_view_sql: str) -> str:
        """Return the text of a CREATE VIEW statement before its ``AS``, with its literals and names blanked out.

        The view's comment and column comments are quoted, and the definition after ``AS`` holds anything, so a
        comment that says `` SECURITY NONE`` is not read as the view's SECURITY clause.
        """
        masked = _SHOW_CREATE_QUOTED.sub(_blanked_literal, create_view_sql)
        definition_start = re.search(r"\bAS\b", masked, re.IGNORECASE)
        return masked[:definition_start.start()] if definition_start else masked

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

        match = self._VIEW_SECURITY_PATTERN.search(self._view_clauses_with_literals_blanked(create_view_sql))

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
        column_rows: Optional[List[_DecodingRow]] = None,
    ) -> ReflectedMVState:
        """
        Parses all raw reflection data for a Materialized View into a ReflectedMVState.
        This is the main entry point for MV reflection parsing.

        Args:
            mv_row: A row from `information_schema.materialized_views`.
            table_row: An optional row from `information_schema.tables` for the same mv
            config_row: An optional row from `information_schema.tables_config` for the same mv
            column_rows: Optional rows from `information_schema.columns` for the same mv
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

        if column_rows:
            ordered_rows = self._order_like_mv_column_list(self._sort_columns(column_rows), ddl)
            parsed_state.columns = [self._parse_column(col) for col in ordered_rows]

        # 2. Augment/overwrite with more reliable info from other sources.
        self._merge_table_options(
            parsed_state.table_options,
            self._parse_common_table_options(TableKind.MATERIALIZED_VIEW, table_row),
        )

        if config_row:
            general_options = self._parse_general_table_options(mv_name, schema, table_config=config_row)
            logger.debug("parsed general table options for mv: %s, options: %s", mv_fqn, general_options)
            self._merge_table_options(parsed_state.table_options, general_options)

        logger.debug("parsed mv state. mv: %s, state: %s", mv_fqn, parsed_state)
        return parsed_state

    @classmethod
    def _order_like_mv_column_list(cls, column_rows: List[_DecodingRow], create_mv_ddl: str) -> List[_DecodingRow]:
        """Order an MV's information_schema columns as the column list of its CREATE statement names them.

        information_schema lists an MV's columns in storage order, sort key first, while the column list in its
        DDL follows the SELECT. A CREATE MATERIALIZED VIEW built from the reflection applies its column names to
        the SELECT items by position, so it needs the DDL's order.
        """
        names = cls._mv_column_list(create_mv_ddl)
        if not names:
            return column_rows
        position = {name: index for index, name in enumerate(names)}
        return sorted(column_rows, key=lambda row: position.get(row.COLUMN_NAME, len(names)))

    @staticmethod
    def _mv_column_list(create_mv_ddl: str) -> List[str]:
        """Return the column names in the parenthesised list after the MV's name, or [] when there is none."""
        header = re.match(r"\s*CREATE\s+MATERIALIZED\s+VIEW\s+(?:`(?:[^`]|``)+`|[\w.]+)(?:\.`(?:[^`]|``)+`)?\s*\(",
                          create_mv_ddl, re.IGNORECASE)
        if header is None:
            return []
        names: List[str] = []
        depth, quote, escaped, item_start = 1, None, False, header.end()
        for position in range(header.end(), len(create_mv_ddl)):
            char = create_mv_ddl[position]
            if escaped:
                escaped = False
            elif quote:
                escaped = char == "\\" and quote != "`"
                quote = None if char == quote else quote
            elif char in "`'\"":
                quote = char
            elif char == "(":
                depth += 1
            elif char in ",)" and depth == 1:
                item = re.match(r"\s*(`(?:[^`]|``)+`|\w+)", create_mv_ddl[item_start:position])
                if item:
                    name = item.group(1)
                    names.append(name[1:-1].replace("``", "`") if name.startswith("`") else name)
                item_start = position + 1
                if char == ")":
                    return names
            elif char == ")":
                depth -= 1
        return []

    def _merge_table_options(self, table_options: Dict[str, Any], new_options: Dict[str, Any]) -> None:
        """
        Merges `new_options` into `table_options` in place, with `new_options` taking precedence.

        This is `dict.update()` except for PROPERTIES, whose value is itself a dict: those are
        merged key by key instead of being replaced wholesale, so a property reported by only
        one source survives a source that does not report it. Callers are expected to pass
        options in increasing order of reliability.

        This matters because no single source reports every property of a materialized view:
        information_schema.tables_config is authoritative for what it does report, but omits
        some properties (see ReflectionMVDefaults._DDL_ONLY_PROPERTY_KEYS) that are only available
        from the CREATE MATERIALIZED VIEW ddl. Which properties are trusted from which source is
        decided when they are parsed, not here.
        """
        for key, value in new_options.items():
            if key == TableInfoKeyWithPrefix.PROPERTIES and table_options.get(key):
                value = hashdict({**table_options[key], **value}.items())
            table_options[key] = value

    @classmethod
    def _mask_mv_ddl(cls, create_mv_ddl: str) -> str:
        """Blank out the literals and backquoted names of a CREATE MATERIALIZED VIEW statement, keeping its length.

        StarRocks 4.1.6 prints a property value's ``"`` as it is, so ``"partition_retention_condition" = "city !=
        "O'Brien""`` leaves a ``'`` that pairs with one in the SELECT, and the quotes after it are then read the wrong
        way round. The PROPERTIES block is therefore found by the lines it is printed on, and blanked whole, so the
        quotes inside it never pair with the quotes of the SELECT. A statement that isn't printed that way, with no
        PROPERTIES block or on one line, is masked by quote pairing alone.
        """
        start = cls._MV_PROPERTIES_BLOCK_START.search(_SHOW_CREATE_QUOTED.sub(_blanked_literal, create_mv_ddl))
        end = cls._MV_PROPERTIES_BLOCK_END.search(create_mv_ddl, start.end()) if start else None
        if start is None or end is None:
            return _SHOW_CREATE_QUOTED.sub(_blanked_literal, create_mv_ddl)
        head = _SHOW_CREATE_QUOTED.sub(_blanked_literal, create_mv_ddl[:start.end()])
        properties = re.sub(r"[^\n]", " ", create_mv_ddl[start.end():end.start()])
        tail = _SHOW_CREATE_QUOTED.sub(_blanked_literal, create_mv_ddl[end.start():])
        return head + properties + tail

    def _parse_mv_ddl(
        self,
        mv_name: str,
        create_mv_ddl: str,
        schema: Optional[str] = None
    ) -> ReflectedMVState:
        """
        Parses the DDL from SHOW CREATE MATERIALIZED VIEW.
        This is the main entry point for MV reflection.

        StarRocks ends the statement with ``;``, which is not part of the definition, so it is left out: a
        migration script that recreates the MV from the reflected definition would otherwise end in ``;;``.

        The clauses are found with the literals and backquoted names blanked out, and read from the DDL as it is,
        so a column or MV comment that says ``PARTITION BY``, ``REFRESH`` or ``AS SELECT`` is not taken for one.
        """
        # Extract AS SELECT definition first
        mv_definition = None
        masked_ddl = self._mask_mv_ddl(create_mv_ddl)
        definition_match = self._MV_AS_DEFINITION_PATTERN.search(masked_ddl)
        if definition_match:
            definition_text = create_mv_ddl[definition_match.start(1):definition_match.end(1)]
            mv_definition = definition_text.strip().removesuffix(";").rstrip()
            clauses_str = create_mv_ddl[:definition_match.start()]
            masked_clauses = masked_ddl[:definition_match.start()]
        else:
            raise SQLParseError(f"Could not find 'AS SELECT' in CREATE MATERIALIZED VIEW statement for {mv_name}", create_mv_ddl)

        state = ReflectedMVState(
            table_name=mv_name,
            definition=mv_definition,
        )

        partition_bounds = self.partition_clause_bounds(masked_clauses)
        partition_clause = clauses_str[partition_bounds[0]:partition_bounds[1]].strip() if partition_bounds else ""
        if partition_clause:
            state.table_options[TableInfoKeyWithPrefix.PARTITION_BY] = self.parse_partition_clause(partition_clause)

        # Use Lark parser for the refresh clause
        try:
            refresh_text_match = self._MV_REFRESH_PATTERN.search(masked_clauses)
            if refresh_text_match:
                refresh_clause_str = clauses_str[refresh_text_match.start():refresh_text_match.end()].strip()
                logger.debug("mv: %r, refresh_clause: %r", mv_name, refresh_clause_str)
                parsed_refresh = parse_mv_refresh_clause(refresh_clause_str)
                state.table_options[TableInfoKeyWithPrefix.REFRESH] = ReflectedRefreshInfo(
                    moment=parsed_refresh.get("refresh_moment"),
                    type=parsed_refresh.get("refresh_type")
                )
        except Exception as e:
            logger.warning(f"Failed to parse refresh clause for MV {mv_name}, falling back to regex: {e}")
            # Fallback to simple regex if lark parsing fails
            self._parse_mv_refresh_with_regex(clauses_str, state, masked_clauses)

        # NOTE: properties come from information_schema.tables_config, except for the few that
        # tables_config does not report for MVs. Only those are taken from the DDL here;
        # parse_mv() merges tables_config on top. See ReflectionMVDefaults._DDL_ONLY_PROPERTY_KEYS.
        properties_match = self._MV_PROPERTIES_PATTERN.search(masked_clauses)
        if properties_match:
            ddl_properties = self._parse_properties(clauses_str[properties_match.start(1):properties_match.end(1)])
            ddl_only_properties = {
                key: value for key, value in ddl_properties.items()
                if ReflectionMVDefaults.is_ddl_only_property(key)
            }
            if ddl_only_properties:
                logger.debug("mv: %r, ddl-only properties: %s", mv_name, ddl_only_properties)
                state.table_options[TableInfoKeyWithPrefix.PROPERTIES] = hashdict(ddl_only_properties.items())

        return state

    def _parse_mv_refresh_with_regex(
        self, ddl_part: str, state: ReflectedMVState, masked_ddl_part: Optional[str] = None
    ) -> None:
        """Fallback refresh clause parser using regex.

        ``masked_ddl_part`` is ``ddl_part`` with its literals blanked out, as :meth:`_parse_mv_ddl` searches it. The
        clause ends at a PROPERTIES or AS keyword as a word, so the ASYNC of ``DEFERRED ASYNC`` doesn't end it.
        """
        searched = ddl_part if masked_ddl_part is None else masked_ddl_part
        refresh_match = re.search(
            r"\bREFRESH\s+(.+?)(?=\s*\b(?:PROPERTIES|AS)\b)", searched, re.IGNORECASE | re.DOTALL
        )
        if refresh_match:
            refresh_text = ddl_part[refresh_match.start(1):refresh_match.end(1)].strip().upper()
            moment, type = None, None
            parts = refresh_text.split()
            if len(parts) > 0:
                if parts[0] in ("IMMEDIATE", "DEFERRED"):
                    moment = parts.pop(0)
                if parts:
                    type = " ".join(parts)
            state.table_options[TableInfoKeyWithPrefix.REFRESH] = ReflectedRefreshInfo(moment=moment, type=type)

    _MV_PROPERTY = re.compile(r'"([^"\n]+)"\s*=\s*"(.*?)"(?=\s*,\s*"[^"\n]+"\s*=|\s*\Z)', re.DOTALL)

    def _parse_properties(self, props_str: str) -> Dict[str, str]:
        """Parse the content of a PROPERTIES clause into a dictionary.

        A value ends at the ``"`` that the next ``"key" =`` or the end of the clause follows, so it may hold commas
        and quotes: StarRocks 4.1.6 prints a ``"`` in a value as it is, as in
        ``"partition_retention_condition" = "dt >= "2020-01-01""``. An escaped ``\\"`` reads as ``"``.
        """
        matches = self._MV_PROPERTY.findall(props_str)
        if matches:
            properties = {key: value.replace('\\"', '"') for key, value in matches}
        else:
            properties = {}
        return properties
