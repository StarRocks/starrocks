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

import re
from typing import Any, Callable, Dict, Final, Iterator, List, Mapping, Optional, Tuple, Union

from sqlalchemy import exc, schema as sa_schema
from sqlalchemy.engine import Dialect
from sqlalchemy.exc import StatementError
from sqlalchemy.sql import sqltypes

from starrocks.common.params import DialectName, TableInfoKey
from starrocks.common.types import TableEngine
from starrocks.engine.interfaces import ReflectedDistributionInfo, ReflectedPartitionInfo, ReflectedTableKeyInfo


class SQLParseError(StatementError):
    """A parse error occurred during execution of a SQL statement.

    ``statement`` is the SQL text that could not be parsed. It has no parameters, and no DBAPI error behind it.
    """

    def __init__(self, message: str, statement: Optional[str]) -> None:
        super().__init__(message, statement, None, None)


class CaseInsensitiveDict(dict):
    """A dictionary that enables case insensitive searching while preserving case sensitivity when keys are set."""

    def __init__(self, data: Optional[Union[Dict[str, Any], Mapping[str, Any]]] = None) -> None:
        super().__init__()
        self._lower_keys: Dict[str, str] = {}  # Maps lowercase keys to actual keys
        if data:
            self.update(data)

    def __setitem__(self, key: str, value: Any) -> None:
        lower_key: str = key.lower()
        if lower_key in self._lower_keys:
            # Remove old entry if exists
            old_key: str = self._lower_keys[lower_key]
            super().__delitem__(old_key)
        self._lower_keys[lower_key] = key
        super().__setitem__(key, value)

    def __getitem__(self, key: str) -> Any:
        lower_key: str = key.lower()
        if lower_key in self._lower_keys:
            actual_key: str = self._lower_keys[lower_key]
            return super().__getitem__(actual_key)
        raise KeyError(key)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: str) -> bool:
        return key.lower() in self._lower_keys

    def update(self, other: Union['CaseInsensitiveDict', Mapping[str, Any], Iterator[Tuple[str, Any]]]) -> None:
        if hasattr(other, 'items'):
            for key, value in other.items():
                self[key] = value
        else:
            for key, value in other:
                self[key] = value


def extract_dialect_options_as_case_insensitive(
    table: Union[sa_schema.Table, sa_schema.Column]
) -> CaseInsensitiveDict:
    """
    Extract StarRocks dialect-specific options and return as a CaseInsensitiveDict.

    This function is useful when extracting options from Table.dialect_options or similar,
    filtering out None values and enabling case-insensitive key lookups.

    Args:
        dialect_options: The dialect_options dict (e.g., Table.dialect_options)

    Returns:
        CaseInsensitiveDict with non-None values

    Example:
        >>> opts = extract_dialect_options_as_case_insensitive(table)
        >>> security = opts.get('SECURITY')  # Works with any case
    """
    raw_opts = table.dialect_options.get(DialectName, {})
    return CaseInsensitiveDict({k: v for k, v in raw_opts.items() if v is not None})


def implied_primary_key_columns(table: sa_schema.Table) -> Optional[List[str]]:
    """Return the primary key columns, in key order, of a table that names no StarRocks key model.

    The key comes from ``primary_key=True`` columns or a ``PrimaryKeyConstraint``. Returns None when the table names
    ``starrocks_primary_key`` or another ``starrocks_*_key``, or has no primary key.
    """
    opts = extract_dialect_options_as_case_insensitive(table)
    if any(key_kwarg in opts for key_kwarg in TableInfoKey.KEY_KWARG_MAP):
        return None
    return [column.name for column in table.primary_key.columns] or None


def infers_primary_keys(dialect: Dialect) -> bool:
    """Whether the dialect was created with ``starrocks_infer_primary_key`` on, which is off by default."""
    return bool(getattr(dialect, "infer_primary_key", False))


def inferred_primary_key_columns(table: sa_schema.Table, dialect: Dialect) -> Optional[List[str]]:
    """Return the key columns of a table that is created as a PRIMARY KEY table without naming a key model.

    With ``starrocks_infer_primary_key`` on (see :func:`infers_primary_keys`), a table whose primary key is only
    implied (see :func:`implied_primary_key_columns`) becomes a PRIMARY KEY table on those columns when StarRocks
    can hold it as one (see :func:`primary_key_table_obstacle`). Otherwise CREATE TABLE sends no key clause, as
    earlier versions of the dialect did, and StarRocks makes its default DUPLICATE KEY table. CREATE TABLE and the
    autogenerate comparison read the key from here, so they agree on it. Returns None when no PRIMARY KEY is
    inferred.
    """
    if not infers_primary_keys(dialect):
        return None
    key_columns = implied_primary_key_columns(table)
    if not key_columns or primary_key_table_obstacle(table, key_columns, dialect) is not None:
        return None
    return key_columns


PRIMARY_KEY_COLUMN_TYPES: Final[frozenset] = frozenset(
    {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "LARGEINT", "DATE", "DATETIME", "VARCHAR", "STRING"}
)
"""The types StarRocks takes for a primary key column, and for a sort key column of a PRIMARY KEY table.

StarRocks 3.3 to 4.1 refuse the others, among them CHAR (so ``sa.Uuid()``, which is CHAR(32)), DECIMAL, FLOAT,
DOUBLE, BINARY, JSON and TIME. From 4.0 StarRocks also sorts by BINARY, which 3.3 refuses.
"""

DUPLICATE_KEY_COLUMN_TYPES: Final[frozenset] = PRIMARY_KEY_COLUMN_TYPES | {"CHAR", "DECIMAL", "NUMERIC"}
"""The types StarRocks 3.3 to 4.1 all take for a DUPLICATE KEY column.

They refuse FLOAT, DOUBLE, JSON and TIME key columns in any key model, and 3.3 refuses BINARY ones.
"""

PRIMARY_KEY_SIZE_LIMIT: Final[int] = 128
"""The bytes a row's primary key may take, StarRocks' BE ``primary_key_limit_size`` (128 in 3.3 to 4.1).

A PRIMARY KEY table refuses to load a row whose key takes more ("primary key size exceed the limit").
"""

PRIMARY_KEY_COLUMN_WIDTHS: Final[Dict[str, int]] = {
    "BOOLEAN": 1,
    "TINYINT": 1,
    "SMALLINT": 2,
    "INT": 4,
    "INTEGER": 4,
    "BIGINT": 8,
    "LARGEINT": 16,
    "DATE": 4,
    "DATETIME": 8,
}
"""The bytes StarRocks counts for a fixed-width key column against :data:`PRIMARY_KEY_SIZE_LIMIT`."""

PRIMARY_KEY_TABLE_REFUSED_PROPERTIES: Final[Tuple[str, ...]] = (
    "storage_cooldown_ttl",
    "storage_cooldown_time",
    "base_compaction_forbidden_time_ranges",
)
"""Table properties StarRocks refuses for a PRIMARY KEY table, and takes for a DUPLICATE KEY one."""

_CLAUSE_TOKEN: Final[re.Pattern[str]] = re.compile(
    r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|`(?P<quoted>(?:[^`]|``)*)`"
    r"|(?i:\bINTERVAL\s+[-+]?\d+\s+[^\W\d]\w*(?:\s*,\s*(?:FLOOR|CEIL)\b)?)"
    r"|(?P<bare>[^\W\d]\w*)"
)
_RANDOM_DISTRIBUTION: Final[re.Pattern[str]] = re.compile(r"\s*RANDOM\b", re.IGNORECASE)
_HASH_DISTRIBUTION: Final[re.Pattern[str]] = re.compile(r"\s*HASH\s*\((?P<columns>[^()]*)\)", re.IGNORECASE)
_RANGE_OR_LIST_PARTITION: Final[re.Pattern[str]] = re.compile(r"\s*(?:RANGE|LIST)\s*\(", re.IGNORECASE)
_VARCHAR_LENGTH: Final[re.Pattern[str]] = re.compile(r"\s*VARCHAR\s*\(\s*(?P<length>\d+)\s*\)\s*", re.IGNORECASE)


def primary_key_table_obstacle(table: sa_schema.Table, key_columns: List[str], dialect: Dialect) -> Optional[str]:
    """Return why StarRocks can't hold the table as a PRIMARY KEY table on ``key_columns``, or None when it can.

    StarRocks 3.3 to 4.1 make a PRIMARY KEY table only of an OLAP table whose key columns come first, in key
    order, each of a type in :data:`PRIMARY_KEY_COLUMN_TYPES` and none a generated column. The table is
    distributed by HASH of key columns, or by default, never by RANDOM, and partitioned by key columns only. It
    sorts only by columns of those types, and has none of :data:`PRIMARY_KEY_TABLE_REFUSED_PROPERTIES`; from
    3.5, it keeps a persistent index too. So that it loads every row the DUPLICATE KEY table of earlier versions
    of the dialect took, no key column may be nullable (see :func:`_key_columns_nullable`), and the key's
    declared width must fit :data:`PRIMARY_KEY_SIZE_LIMIT` (see :func:`_key_wider_than_the_limit`). A PARTITION
    BY, DISTRIBUTED BY or ORDER BY clause is free text: the table's column names are read in it, backquoted or
    bare, outside string literals, and a clause in which none is found can't be checked, so the dialect makes no
    PRIMARY KEY table of it.
    """
    checks = (
        _key_columns_out_of_order,
        _engine_without_keys,
        _key_column_types_refused,
        _key_columns_nullable,
        _key_wider_than_the_limit,
        _distribution_outside_the_key,
        _partition_outside_the_key,
        _sort_key_types_refused,
        _properties_refused,
    )
    for check in checks:
        if (obstacle := check(table, key_columns, dialect)) is not None:
            return obstacle
    return None


def table_engine_is_olap(table: sa_schema.Table) -> bool:
    """Whether the table is an OLAP table, the only kind that has a key: ``starrocks_engine`` is unset or OLAP."""
    engine = get_dialect_option(table, TableInfoKey.ENGINE)
    return not engine or str(engine).strip().upper() == TableEngine.OLAP


def column_names_in_clause(clause: str, table: sa_schema.Table) -> List[str]:
    """Return the names of the table's columns that a clause given as free text names, in the table's spelling.

    A name counts backquoted or bare, in any case, as StarRocks matches column names. Text inside a string
    literal does not count, nor does an interval's unit, as ``day`` in ``time_slice(dt, INTERVAL 1 day)``, or the
    FLOOR or CEIL that time_slice takes after it.
    """
    names_by_lower_case = {column.name.lower(): column.name for column in table.columns}
    names: List[str] = []
    for token in _CLAUSE_TOKEN.finditer(clause):
        identifier = token.group("bare") or (token.group("quoted") or "").replace("``", "`")
        name = names_by_lower_case.get(identifier.lower())
        if name is not None and name not in names:
            names.append(name)
    return names


def column_type_sql(column: sa_schema.Column, dialect: Dialect) -> str:
    """Return the type CREATE TABLE gives the column, such as ``VARCHAR(10)``.

    The dialect makes an ``autoincrement=True`` column BIGINT. A type the dialect can't compile gives "".
    """
    if column.autoincrement is True:
        return "BIGINT"
    try:
        return dialect.type_compiler.process(column.type, type_expression=column)
    except exc.CompileError:
        return ""


def column_type_keyword(column: sa_schema.Column, dialect: Dialect) -> str:
    """Return the first word of the type CREATE TABLE gives the column, such as VARCHAR for ``VARCHAR(10)``."""
    return _type_keyword(column_type_sql(column, dialect))


def _type_keyword(type_sql: str) -> str:
    keyword = re.match(r"\s*(\w*)", type_sql)
    return keyword.group(1).upper() if keyword else ""


def _names_phrase(names: List[str]) -> str:
    """Join column names for a message: ``a``, ``a and b``, ``a, b and c``."""
    return names[0] if len(names) == 1 else f"{', '.join(names[:-1])} and {names[-1]}"


def primary_key_table_remedy(table: sa_schema.Table, key_columns: List[str], dialect: Dialect) -> str:
    """Return the advice for a table StarRocks can't hold as a PRIMARY KEY table: what makes it one, or what keeps
    it a DUPLICATE KEY table without the warning.

    StarRocks needs any key's columns first, in key order, so key columns that don't lead can't be named
    ``starrocks_duplicate_key`` either: they have to be put first, which makes a PRIMARY KEY table, or the warning
    filtered. Other key columns can be named ``starrocks_duplicate_key`` when every StarRocks version takes them in
    a DUPLICATE KEY (see :data:`DUPLICATE_KEY_COLUMN_TYPES`); otherwise the warning can only be filtered.
    """
    if _key_columns_out_of_order(table, key_columns, dialect) is not None:
        return (
            "Put the key columns first, in key order, to make it a PRIMARY KEY table (on SQLAlchemy 2.0, a "
            "declarative mixin's columns come after the class's own unless mapped_column(sort_order=...) puts them "
            f"first), or keep it a DUPLICATE KEY table without this warning by filtering it: {_warning_filter(table)}"
        )
    refused = [name for name in key_columns if not _takes_in_a_duplicate_key(_column_named(table, name), dialect)]
    if not refused:
        return (
            f'Name the key model, as starrocks_duplicate_key="{", ".join(key_columns)}", to keep it so without '
            "this warning"
        )
    columns = f"key column {refused[0]}" if len(refused) == 1 else f"key columns {_names_phrase(refused)}"
    return (
        f"StarRocks 3.3 to 4.1 don't all take {columns} in a DUPLICATE KEY, so filter this warning to keep it so: "
        f"{_warning_filter(table)}"
    )


def _warning_filter(table: sa_schema.Table) -> str:
    """The ``warnings.filterwarnings`` call that silences the table's warning of a PRIMARY KEY it can't be."""
    message = f"Table {re.escape(repr(table.name))} has the primary key"
    return f'warnings.filterwarnings("ignore", message={message!r})'


def _takes_in_a_duplicate_key(column: sa_schema.Column, dialect: Dialect) -> bool:
    return column.computed is None and column_type_keyword(column, dialect) in DUPLICATE_KEY_COLUMN_TYPES


def _key_columns_out_of_order(table: sa_schema.Table, key_columns: List[str], dialect: Dialect) -> Optional[str]:
    leading_columns = [column.name for column in table.columns][: len(key_columns)]
    if leading_columns == key_columns:
        return None
    return (
        "StarRocks needs a PRIMARY KEY table's key columns first, in key order, and its first columns are "
        f"{_names_phrase(leading_columns)}"
    )


def _engine_without_keys(table: sa_schema.Table, key_columns: List[str], dialect: Dialect) -> Optional[str]:
    if table_engine_is_olap(table):
        return None
    return f"a table of ENGINE={get_dialect_option(table, TableInfoKey.ENGINE)} has no key"


def _key_column_types_refused(table: sa_schema.Table, key_columns: List[str], dialect: Dialect) -> Optional[str]:
    for name in key_columns:
        column = _column_named(table, name)
        if column.computed is not None:
            return f"key column {name} is a generated column, which StarRocks can't key on"
        type_sql = column_type_sql(column, dialect)
        if _type_keyword(type_sql) not in PRIMARY_KEY_COLUMN_TYPES:
            return (
                f"key column {name} is {type_sql or 'of an unknown type'}, and StarRocks takes BOOLEAN, integer, "
                "DATE, DATETIME, VARCHAR and STRING primary key columns only"
            )
    return None


def _key_columns_nullable(table: sa_schema.Table, key_columns: List[str], dialect: Dialect) -> Optional[str]:
    """Return why nullable key columns keep the table from being a PRIMARY KEY table, or None when none is.

    CREATE TABLE writes no NULL for a nullable column, and StarRocks then makes a PRIMARY KEY table's key column
    NOT NULL without a word, so it refuses each row whose key holds a NULL, which a DUPLICATE KEY table takes. The
    dialect makes an ``autoincrement=True`` column NOT NULL itself, so such a column counts as not nullable.
    """
    nullable = [name for name in key_columns if _created_nullable(_column_named(table, name))]
    if not nullable:
        return None
    columns = f"key column {nullable[0]} is" if len(nullable) == 1 else f"key columns {_names_phrase(nullable)} are"
    return (
        f"{columns} nullable, and StarRocks makes a PRIMARY KEY table's key columns NOT NULL, refusing the rows "
        "whose key holds a NULL"
    )


def _created_nullable(column: sa_schema.Column) -> bool:
    """Whether CREATE TABLE leaves the column nullable: it is, and it isn't AUTO_INCREMENT, which is NOT NULL."""
    return bool(column.nullable) and column.autoincrement is not True


def _key_wider_than_the_limit(table: sa_schema.Table, key_columns: List[str], dialect: Dialect) -> Optional[str]:
    """Return why the key's declared width doesn't fit :data:`PRIMARY_KEY_SIZE_LIMIT`, or None when it does.

    StarRocks counts a fixed-width column by :data:`PRIMARY_KEY_COLUMN_WIDTHS` and a VARCHAR by the bytes it
    holds, plus 2 for a VARCHAR that isn't the key's last column (and 1 more for each zero byte in it, which this
    leaves out). A ``VARCHAR(n)`` holds at most ``n`` bytes, so it counts ``n``. STRING, and so ``Text`` and a
    VARCHAR with no length, has no width the key can be held to.
    """
    width = 0
    for position, name in enumerate(key_columns):
        type_sql = column_type_sql(_column_named(table, name), dialect)
        keyword = _type_keyword(type_sql)
        if keyword in PRIMARY_KEY_COLUMN_WIDTHS:
            width += PRIMARY_KEY_COLUMN_WIDTHS[keyword]
            continue
        varchar = _VARCHAR_LENGTH.fullmatch(type_sql)
        if varchar is None:
            return (
                f"key column {name} is {type_sql}, which has no length to hold the key to the "
                f"{PRIMARY_KEY_SIZE_LIMIT} bytes StarRocks loads in a row's primary key"
            )
        width += int(varchar.group("length")) + (0 if position == len(key_columns) - 1 else 2)
    if width <= PRIMARY_KEY_SIZE_LIMIT:
        return None
    return (
        f"its key columns can take {width} bytes a row, and StarRocks loads no row whose primary key takes more "
        f"than {PRIMARY_KEY_SIZE_LIMIT}"
    )


def _column_named(table: sa_schema.Table, name: str) -> sa_schema.Column:
    """Return the table's column of this name, which may differ from the ``key`` that ``table.columns`` uses."""
    return next(column for column in table.columns if column.name == name)


def _distribution_outside_the_key(table: sa_schema.Table, key_columns: List[str], dialect: Dialect) -> Optional[str]:
    opts = extract_dialect_options_as_case_insensitive(table)
    distribution = opts.get(TableInfoKey.DISTRIBUTED_BY) or opts.get("DISTRIBUTION")
    if not distribution:
        return None
    text = str(distribution)
    if _RANDOM_DISTRIBUTION.match(text):
        return "it is distributed by RANDOM, which StarRocks refuses for a PRIMARY KEY table"
    hash_columns = _HASH_DISTRIBUTION.match(text)
    names = column_names_in_clause(hash_columns.group("columns"), table) if hash_columns else []
    if not names:
        return f"the dialect can't tell which columns DISTRIBUTED BY {text!r} uses"
    return _columns_outside_the_key("DISTRIBUTED BY HASH", "distributes", names, key_columns)


def _partition_outside_the_key(table: sa_schema.Table, key_columns: List[str], dialect: Dialect) -> Optional[str]:
    partition = get_dialect_option(table, TableInfoKey.PARTITION_BY)
    if not partition:
        return None
    text = str(partition)
    columns_text = text
    if range_or_list := _RANGE_OR_LIST_PARTITION.match(text):
        opening = range_or_list.end() - 1
        closing = find_matching_parenthesis(text, opening)
        columns_text = text[opening + 1 : closing] if closing != -1 else text
    names = column_names_in_clause(columns_text, table)
    if not names:
        return f"the dialect can't tell which columns PARTITION BY {text!r} uses"
    return _columns_outside_the_key("PARTITION BY", "partitions", names, key_columns)


def _columns_outside_the_key(clause: str, verb: str, names: List[str], key_columns: List[str]) -> Optional[str]:
    outside = [name for name in names if name not in key_columns]
    if not outside:
        return None
    what = "not a key column" if len(outside) == 1 else "not key columns"
    return f"{clause} uses {_names_phrase(outside)}, {what}, and StarRocks {verb} a PRIMARY KEY table by key columns only"


def _sort_key_types_refused(table: sa_schema.Table, key_columns: List[str], dialect: Dialect) -> Optional[str]:
    order_by = get_dialect_option(table, TableInfoKey.ORDER_BY)
    if not order_by:
        return None
    text = ", ".join(str(item) for item in order_by) if isinstance(order_by, (list, tuple)) else str(order_by)
    names = column_names_in_clause(text, table)
    if not names:
        return f"the dialect can't tell which columns ORDER BY {text!r} uses"
    for name in names:
        keyword = column_type_keyword(_column_named(table, name), dialect)
        if keyword not in PRIMARY_KEY_COLUMN_TYPES:
            return (
                f"ORDER BY uses {name}, which is {keyword or 'of an unknown type'}, and StarRocks sorts a PRIMARY KEY "
                "table by BOOLEAN, integer, DATE, DATETIME, VARCHAR and STRING columns only"
            )
    return None


def _distribution_is_named(table: sa_schema.Table) -> bool:
    opts = extract_dialect_options_as_case_insensitive(table)
    return bool(opts.get(TableInfoKey.DISTRIBUTED_BY) or opts.get("DISTRIBUTION"))


def _properties_refused(table: sa_schema.Table, key_columns: List[str], dialect: Dialect) -> Optional[str]:
    properties = get_dialect_option(table, TableInfoKey.PROPERTIES)
    if not isinstance(properties, (dict, list, tuple)):
        return None
    values = {str(name).strip().lower(): value for name, value in dict(properties).items()}
    for name in PRIMARY_KEY_TABLE_REFUSED_PROPERTIES:
        if name in values:
            return f"StarRocks refuses the {name} property for a PRIMARY KEY table"
    if "bucket_size" in values and not _distribution_is_named(table):
        return (
            "StarRocks 4.1 allows bucket_size only with RANDOM distribution, which a PRIMARY KEY table can't use, "
            "and a PRIMARY KEY table with no DISTRIBUTED BY is distributed by HASH"
        )
    if "enable_persistent_index" in values and str(values["enable_persistent_index"]).strip().lower() != "true":
        return "enable_persistent_index is false, and StarRocks 3.5 and later refuse a PRIMARY KEY table without it"
    return None


def get_dialect_option(
    table: sa_schema.Table,
    key: str,
    default: Optional[Any] = None,
) -> Any:
    """
    Get a StarRocks dialect-specific option value with case-insensitive key lookup.

    This is a convenience function that combines extraction and lookup in one call.
    Useful when you only need to retrieve a single option value.

    Args:
        Table: in which there is the dialect_options dict (e.g., Table.dialect_options)
        key: The option key to retrieve (case-insensitive)
        default: Default value if key not found

    Returns:
        The option value, or default if not found

    Example:
        >>> from starrocks.common.params import TableInfoKey
        >>> security = get_dialect_option(table, TableInfoKey.SECURITY)
    """
    opts = extract_dialect_options_as_case_insensitive(table)
    return opts.get(key, default)


class TableAttributeNormalizer:
    """A class to normalize StarRocks attributes for comparison."""

    # Pre-compiled regex patterns for better performance
    _BACKTICKS_PATTERN = re.compile(r'`([^`]+)`')
    _WHITESPACE_PATTERN = re.compile(r'\s+')
    # Matches spaces around opening parenthesis
    _OPEN_PAREN_SPACE_PATTERN = re.compile(r'\s*(\()\s*')
    # Matches spaces around closing parenthesis
    _CLOSE_PAREN_SPACE_PATTERN = re.compile(r'\s*(\)\s?)\s*')
    _COMMA_SPACE_PATTERN = re.compile(r'\s*,\s*')
    _QUALIFIERS_PATTERN = re.compile(r"((?:`[^`]+`|\d*[^\W\d]\w*)\.)+")
    _TIME_SLICE_PATTERN = re.compile(
        r"\btime_slice\(\s*(?P<column>[^,()]+?)\s*,\s*"
        r"(?:interval\s+(?P<interval>\d+)\s+(?P<unit>[a-z]+)|(?P<count>\d+)\s*,\s*'(?P<quoted_unit>[a-z]+)')"
        r"(?:\s*,\s*'?(?P<boundary>floor|ceil)'?)?\s*\)",
        re.IGNORECASE,
    )

    @staticmethod
    def strip_identifier_backticks(sql: str) -> str:
        """Remove MySQL-style identifier quotes (`) while preserving string literals."""
        result = []
        in_string = False
        in_backtick = False
        quote_char = None

        i = 0
        while i < len(sql):
            ch = sql[i]

            if in_string:
                result.append(ch)
                if ch == quote_char:
                    # ignore quote with escape character
                    if i == 0 or sql[i - 1] != '\\':
                        in_string = False

            elif in_backtick:
                if ch == '\\' and i + 1 < len(sql):
                    # support MySQL-style backtick escape in backtick, e.g., `col\`name`
                    next_ch = sql[i + 1]
                    if next_ch == '`' or next_ch == '\\':
                        result.append(next_ch)
                        i += 1
                    else:
                        result.append(ch)
                elif ch == '`':
                    in_backtick = False
                else:
                    result.append(ch)

            else:
                if ch == '`':
                    in_backtick = True
                elif ch in ("'", '"'):
                    in_string = True
                    quote_char = ch
                    result.append(ch)
                else:
                    result.append(ch)

            i += 1

        return ''.join(result)

    @staticmethod
    def _strip_line_comments(sql: str) -> str:
        """Replace ``-- ...`` line comments with a space, skipping ``--`` inside string
        literals or backtick identifiers (where it is data, not a comment)."""
        result: List[str] = []
        in_string = False
        in_backtick = False
        quote_char = None
        i = 0
        n = len(sql)
        while i < n:
            ch = sql[i]
            if in_string:
                result.append(ch)
                if ch == quote_char and sql[i - 1] != '\\':
                    in_string = False
            elif in_backtick:
                result.append(ch)
                if ch == '`':
                    in_backtick = False
            elif ch == '`':
                in_backtick = True
                result.append(ch)
            elif ch in ("'", '"'):
                in_string = True
                quote_char = ch
                result.append(ch)
            elif ch == '-' and i + 1 < n and sql[i + 1] == '-':
                # Replace "-- ... \n" (newline included) with one space.
                result.append(' ')
                i += 2
                while i < n and sql[i] != '\n':
                    i += 1
                i += 1
                continue
            else:
                result.append(ch)
            i += 1
        return ''.join(result)

    @staticmethod
    def _collapse_whitespace_outside_strings(sql: str) -> str:
        """Collapse runs of whitespace to a single space, leaving whitespace inside string
        literals untouched so that e.g. ``'a   b'`` and ``'a b'`` stay distinct."""
        result: List[str] = []
        in_string = False
        quote_char = None
        i = 0
        n = len(sql)
        while i < n:
            ch = sql[i]
            if in_string:
                result.append(ch)
                if ch == quote_char and sql[i - 1] != '\\':
                    in_string = False
            elif ch in ("'", '"'):
                in_string = True
                quote_char = ch
                result.append(ch)
            elif ch.isspace():
                result.append(' ')
                while i + 1 < n and sql[i + 1].isspace():
                    i += 1
            else:
                result.append(ch)
            i += 1
        return ''.join(result)

    @staticmethod
    def _rewrite_outside_string_literals(sql: str, rewrite: Callable[[str], str]) -> str:
        """Apply ``rewrite`` to each stretch of ``sql`` outside its string literals, and keep the literals as written.

        A literal is quoted with ``'`` or ``"``, and ends at its quote unless a backslash comes before it. A quote
        inside a backtick identifier starts no literal.
        """
        pieces: List[str] = []
        outside_from = 0
        i = 0
        n = len(sql)
        while i < n:
            ch = sql[i]
            if ch == '`':
                closing = sql.find('`', i + 1)
                i = n if closing == -1 else closing + 1
            elif ch in ("'", '"'):
                end = i + 1
                while end < n and not (sql[end] == ch and sql[end - 1] != '\\'):
                    end += 1
                pieces.append(rewrite(sql[outside_from:i]))
                pieces.append(sql[i:end + 1])
                i = outside_from = end + 1
            else:
                i += 1
        pieces.append(rewrite(sql[outside_from:]))
        return ''.join(pieces)

    @staticmethod
    def _remove_qualifiers(sql: str) -> str:
        return TableAttributeNormalizer._QUALIFIERS_PATTERN.sub("", sql)

    @staticmethod
    def normalize_sql(sql: Optional[str], lowercase: bool = True, remove_qualifiers: bool = False) -> Optional[str]:
        """Normalize an SQL string for comparison (string hygiene only, no semantic
        canonicalization — that lives in :mod:`starrocks.common.sql_canonical`).

        Lowercases, strips ``--`` comments and backtick quotes, collapses whitespace and
        trailing semicolons, and optionally removes ``schema.table.`` qualifiers, all outside
        string literals, so ``'Active'`` and ``'ACTIVE'``, or ``'a.b'`` and ``'b'``, stay different.
        """
        if sql is None:
            return None
        sql = sql.replace('\\n', '\n').replace('\\t', '\t')
        # MySQL-style escaping: 'O\'Brien' -> 'O''Brien'
        sql = sql.replace("\\'", "''")

        sql = TableAttributeNormalizer._strip_line_comments(sql)
        if lowercase:
            sql = TableAttributeNormalizer._rewrite_outside_string_literals(sql, str.lower).strip()

        if remove_qualifiers:
            sql = TableAttributeNormalizer._rewrite_outside_string_literals(
                sql, TableAttributeNormalizer._remove_qualifiers
            )

        sql = TableAttributeNormalizer.strip_identifier_backticks(sql)

        sql = TableAttributeNormalizer._collapse_whitespace_outside_strings(sql).strip()
        # Strip trailing semicolons together with any surrounding whitespace.
        sql = sql.rstrip(" ;")
        return sql

    @staticmethod
    def _simple_normalize(value: str) -> str:
        return value.upper().strip() if value else value

    @staticmethod
    def normalize_engine(engine: str) -> Optional[str]:
        return TableAttributeNormalizer._simple_normalize(engine)

    @staticmethod
    def normalize_key(table_key: Union[ReflectedTableKeyInfo, str]) -> Optional[str]:
        """Normalize table key string by removing backticks and extra spaces.
        Because there may be column names in this string, we don't simply lowercase it.
        If the table key is a ReflectedTableKeyInfo object, return the string representation only.
        """
        if not table_key:
            return table_key
        return TableAttributeNormalizer.normalize_column_identifiers(
            str(table_key) if isinstance(table_key, ReflectedTableKeyInfo) else table_key
        )

    @staticmethod
    def normalize_partition_method(partition: Union[ReflectedPartitionInfo, str]) -> Optional[str]:
        """Normalize partition string by removing backticks and extra spaces.
        Because there may be column names in this string, we don't simply lowercase it.
        If the partition is a ReflectedPartitionInfo object, return the partition_method only.
        """
        if not partition:
            return partition
        normalized: str = TableAttributeNormalizer.normalize_column_identifiers(
            partition.partition_method if isinstance(partition, ReflectedPartitionInfo) else partition
        )
        return TableAttributeNormalizer.normalize_time_slice_calls(normalized)

    @staticmethod
    def normalize_time_slice_calls(text: str) -> str:
        """Rewrite every time_slice call to the form StarRocks reflects for FLOOR.

        `time_slice(dt, INTERVAL 7 DAY)` and `time_slice(dt, 7, 'day', 'floor')` are the same
        partitioning, but SHOW CREATE TABLE returns the second for FLOOR and the first
        for CEIL, so both spellings, with or without the boundary, become
        `time_slice(dt, 7, 'day', 'floor')`.
        """
        def canonical_call(match: re.Match[str]) -> str:
            count: str = match.group('interval') or match.group('count')
            unit: str = (match.group('unit') or match.group('quoted_unit')).lower()
            boundary: str = (match.group('boundary') or 'floor').lower()
            return f"time_slice({match.group('column')}, {int(count)}, '{unit}', '{boundary}')"

        return TableAttributeNormalizer._TIME_SLICE_PATTERN.sub(canonical_call, text)

    @staticmethod
    def normalize_distribution_string(distribution: Union[ReflectedDistributionInfo, str]) -> Optional[str]:
        """Normalize distribution string by removing backticks and extra spaces.
        Because there may be column names in this string, we don't simply lowercase it.
        If the distribution is a ReflectedDistributionInfo object, return the string representation only.
        """
        if not distribution:
            return distribution
        return TableAttributeNormalizer.normalize_column_identifiers(
            str(distribution) if isinstance(distribution, ReflectedDistributionInfo) else distribution
        )

    @staticmethod
    def normalize_order_by_string(order_by: Union[str, List[str], None]) -> Optional[str]:
        """Normalize ORDER BY string by removing backticks and standardizing format.
        Because there may be column names in this string, we don't simply lowercase it.
        """
        if not order_by:
            return order_by
        elif isinstance(order_by, list):
            order_by = ', '.join(str(item) for item in order_by)
        order_by = TableAttributeNormalizer.remove_outer_parentheses(order_by)
        return TableAttributeNormalizer.normalize_column_identifiers(order_by)

    @staticmethod
    def normalize_column_identifiers(text: str) -> Optional[str]:
        """Normalize column identifiers by removing backticks, standardizing spaces, and removing spaces inside parentheses.
        Because there may be column names in this string, we don't simply lowercase it.
        """
        if not text:
            return text
        normalized: str = TableAttributeNormalizer._BACKTICKS_PATTERN.sub(r'\1', text)
        normalized = TableAttributeNormalizer._WHITESPACE_PATTERN.sub(' ', normalized).strip()
        # Remove spaces immediately around opening parenthesis
        normalized = TableAttributeNormalizer._OPEN_PAREN_SPACE_PATTERN.sub(r'\1', normalized)
        # Remove spaces immediately around closing parenthesis
        normalized = TableAttributeNormalizer._CLOSE_PAREN_SPACE_PATTERN.sub(r'\1', normalized)
        # Standardize spaces around commas within parentheses
        normalized = TableAttributeNormalizer._COMMA_SPACE_PATTERN.sub(', ', normalized)
        return normalized

    @staticmethod
    def remove_outer_parentheses(text: str) -> str:
        """Remove outer parentheses from text."""
        if not text:
            return text
        text = text.strip()
        if text.startswith('(') and text.endswith(')'):
            return text[1:-1].strip()
        return text

    @staticmethod
    def simply_normalize_quotes(text: Optional[str]) -> Optional[str]:
        """Simply normalize double quotes to single quotes."""
        if not text:
            return text
        return text.replace('"', "'")


def find_matching_parenthesis(text: str, start_index: int = 0) -> int:
    """
    Finds the index of the matching closing parenthesis for a given starting parenthesis.

    Args:
        text: The string to search within.
        start_index: The index of the opening parenthesis.

    Returns:
        The index of the matching closing parenthesis, or -1 if not found.

    Note:
        The first byte is '(', we need to skip it.
    """
    open_paren_count = 1
    for i in range(start_index + 1, len(text)):
        if text[i] == '(':
            open_paren_count += 1
        elif text[i] == ')':
            open_paren_count -= 1
            if open_paren_count == 0:
                return i
    return -1


def gen_simple_qualified_name(table_name: str, schema: Optional[str] = None) -> str:
    """Generate a simple qualified name for a table."""
    return f"{schema}.{table_name}" if schema else table_name


_SQL_STRING_LITERAL_BODY = re.compile(r"(?:[^'\\]|''|\\.)*", re.DOTALL)
_SQL_STRING_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "0": "\x00", "b": "\b", "Z": "\x1a"}
_FUNCTION_DEFAULT = re.compile(r"[A-Za-z_]\w*\s*\(.*\)", re.DOTALL)
_KEYWORD_DEFAULT = re.compile(r"[A-Za-z_]\w*")


def quote_sql_string_literal(value: str) -> str:
    """
    Render a string as the SQL literal StarRocks accepts: quotes doubled, backslashes and NUL escaped.

    This is what SQLAlchemy emits for ``server_default="..."``, so a reflected default renders back to the DDL
    that created it.
    """
    escaped = value.replace("\\", "\\\\").replace("'", "''").replace("\x00", "\\0")
    return f"'{escaped}'"


def unquote_sql_string_literal(text: str) -> Optional[str]:
    """
    Return the value of a single-quoted SQL string literal, or None if ``text`` is not one.
    """
    text = text.strip()
    if len(text) < 2 or text[0] != "'" or text[-1] != "'" or not _SQL_STRING_LITERAL_BODY.fullmatch(text, 1, len(text) - 1):
        return None
    body = text[1:-1].replace("''", "'")
    return re.sub(r"\\(.)", lambda m: _SQL_STRING_ESCAPES.get(m.group(1), m.group(1)), body, flags=re.DOTALL)


def strip_wrapping_parentheses(text: str) -> str:
    """
    Remove the parentheses that wrap the whole expression, however many, as in ``((uuid()))``.
    """
    text = text.strip()
    while text.startswith("(") and find_matching_parenthesis(text, 0) == len(text) - 1:
        text = text[1:-1].strip()
    return text


def canonical_server_default(sql: str) -> Tuple[str, str]:
    """
    Reduce a column default, as SQL text, to a ``(kind, value)`` pair that is equal for equal defaults.

    - ``("literal", value)`` for a quoted string literal, compared by its decoded value;
    - ``("expression", text)`` for a function call or keyword such as ``uuid()`` or ``CURRENT_TIMESTAMP``, with
      wrapping parentheses removed and the case and spacing normalised, as these are case insensitive;
    - ``("literal", text)`` for anything else, such as a bare number.
    """
    unwrapped = strip_wrapping_parentheses(sql)
    if (decoded := unquote_sql_string_literal(unwrapped)) is not None:
        return "literal", decoded
    if _FUNCTION_DEFAULT.fullmatch(unwrapped) or _KEYWORD_DEFAULT.fullmatch(unwrapped):
        return "expression", re.sub(r"\s+", "", unwrapped).lower()
    return "literal", unwrapped


_TIME_FUNCTION_DEFAULT = ("expression", "current_timestamp")
_TIME_FUNCTION_SPELLINGS: Final[frozenset[str]] = frozenset({"now()", "current_timestamp", "current_timestamp()"})
_BOOLEAN_DEFAULTS_KEPT_AS_DIGITS: Final[Mapping[str, str]] = {"true": "1", "false": "0"}


def canonical_default_as_kept(
    canonical: Optional[Tuple[str, str]], column_type: Any, boolean_kept_as_digits: bool
) -> Optional[Tuple[str, str]]:
    """
    Reduce a canonical default (see :func:`canonical_server_default`) to what StarRocks keeps, or ``None`` for none.

    Both the check of a restated default in ``alter_column`` and autogenerate's comparison of defaults go through
    this, so that they agree with StarRocks:

    - ``DEFAULT NULL`` is no default.
    - ``now()`` and ``CURRENT_TIMESTAMP`` are one default. StarRocks keeps them apart, comparing a restated default
      as written, but reflects both as ``CURRENT_TIMESTAMP``, so reflection cannot tell which was written. They
      are taken as equal, and a restated one that StarRocks refuses is StarRocks' to refuse.
    - From 3.5.11, 4.0.4 and 4.1.0 (StarRocks PR 66818), a BOOLEAN column's ``'true'`` and ``'false'``, in any
      case, are kept as ``'1'`` and ``'0'``; ``boolean_kept_as_digits`` says whether the server does.
    """
    if canonical is None or canonical == ("expression", "null"):
        return None
    if canonical[0] == "expression" and canonical[1] in _TIME_FUNCTION_SPELLINGS:
        return _TIME_FUNCTION_DEFAULT
    if boolean_kept_as_digits and isinstance(column_type, sqltypes.Boolean):
        digit = _BOOLEAN_DEFAULTS_KEPT_AS_DIGITS.get(canonical[1].lower())
        if digit is not None:
            return "literal", digit
    return canonical
