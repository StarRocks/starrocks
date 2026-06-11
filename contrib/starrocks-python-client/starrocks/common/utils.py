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
from typing import Any, Dict, Iterator, List, Mapping, Optional, Tuple, Union

from sqlalchemy import schema as sa_schema
from sqlalchemy.exc import StatementError

from starrocks.common.params import DialectName
from starrocks.engine.interfaces import ReflectedDistributionInfo, ReflectedPartitionInfo, ReflectedTableKeyInfo


class SQLParseError(StatementError):
    """A parse error occurred during execution of a SQL statement."""

    def __init__(self, message: str, statement: Optional[str]):
        super().__init__(message, statement)


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

    # ---------------------------------------------------------------------------
    # Patterns to canonicalize StarRocks' own rewrites of view / materialized-view
    # definitions. On clusters older than 4.0.6, StarRocks stores a view definition
    # in a canonical form that differs syntactically (but not semantically) from the
    # SQL the user wrote. These patterns reconcile both sides so that equivalent
    # definitions compare equal. See ``_canonicalize_statement``.
    # ---------------------------------------------------------------------------
    # "JOIN" is stored as "INNER JOIN"; collapse it back to a bare "JOIN".
    _INNER_JOIN_PATTERN = re.compile(r'\binner\s+join\b', re.IGNORECASE)
    # "<x> OUTER JOIN" is equivalent to "<x> JOIN"; drop the redundant OUTER.
    _OUTER_JOIN_PATTERN = re.compile(r'\b(left|right|full)\s+outer\s+join\b', re.IGNORECASE)
    # StarRocks may add or drop LATERAL before unnest / table functions.
    _LATERAL_PATTERN = re.compile(r'\blateral\s+', re.IGNORECASE)
    # The optional AS keyword (column alias "x AS y", table alias "t AS a", CAST(x AS int),
    # "WITH cte AS (...)") is removed entirely. StarRocks is inconsistent about emitting it
    # (e.g. on 3.5.x it stores "src AS a" but "unnest(x) k(c)"), and AS is never semantically
    # meaningful, so dropping it symmetrically on both sides reconciles the difference without
    # ever hiding a real change. Must run AFTER the CTE column-list rewrite below, which needs AS.
    _ALIAS_AS_PATTERN = re.compile(r'\s+as\s+', re.IGNORECASE)
    # StarRocks injects a CTE column list: "WITH cat (category, c) AS (" -> "WITH cat AS (".
    _CTE_COLUMN_LIST_PATTERN = re.compile(r'\b(\w+)\s*\([^()]*\)\s+as\s*\(', re.IGNORECASE)
    # StarRocks wraps simple predicates in redundant parentheses: "(x > 100)" -> "x > 100".
    # Restricted to a single flat comparison (no nested parens, no commas, no AND/OR) so the
    # removed parentheses are guaranteed redundant and grouping is never altered.
    _REDUNDANT_PREDICATE_PAREN_PATTERN = re.compile(r'\(\s*([^(),]*?(?:>=|<=|<>|!=|=|>|<)[^(),]*?)\s*\)')

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
    def normalize_sql(sql: Optional[str], lowercase: bool = True, remove_qualifiers: bool = False) -> Optional[str]:
        """
        Normalize an SQL string for comparison.
        - Converts to lowercase
        - Removes leading/trailing whitespace
        - Replaces multiple spaces with a single space
        - Standardizes comma spacing (``a , b`` / ``a,b`` -> ``a, b``)
        - Removes trailing semicolons
        - Removes all qualifiers (e.g., ``schema.table.``) from identifiers.
        - Canonicalizes StarRocks' own view/MV definition rewrites (INNER/OUTER JOIN,
          LATERAL, the optional AS keyword, CTE column lists, redundant predicate
          parentheses) so that semantically equivalent definitions compare equal. See
          ``_canonicalize_statement``.

        Args:
            sql: The SQL string to normalize.
            lowercase: Whether to convert the SQL string to lowercase.
            remove_qualifiers: Whether to remove all qualifiers (e.g., ``schema.table.``) from identifiers.
        """
        if sql is None:
            return None
        # string with \ in SQL statement
        sql = sql.replace('\\n', '\n').replace('\\t', '\t')
        # This is for MySQL-like escaping of single quotes in string literals
        # e.g., 'O\'Brien' becomes 'O''Brien' for standard SQL
        sql = sql.replace("\\'", "''")

        sql = re.sub(r"--.*?(?:\n|$)", " ", sql)
        if lowercase:
            sql = sql.lower().strip()

        # Removes qualifiers like `schema`. from `schema`.`table`.`column`
        # It handles multiple qualifiers.
        if remove_qualifiers:
            sql = re.sub(r"((?:`[^`]+`|\w+)\.)+", "", sql)

        # Removes backticks
        sql = TableAttributeNormalizer.strip_identifier_backticks(sql)

        sql = re.sub(r"\s+", " ", sql).strip()
        sql = TableAttributeNormalizer._canonicalize_statement(sql)
        # Strip trailing semicolons together with any surrounding whitespace.
        sql = sql.rstrip(" ;").rstrip(";")
        return sql

    @staticmethod
    def _canonicalize_statement(sql: str) -> str:
        """Reconcile StarRocks' canonical view/MV definition form with user-written SQL.

        On clusters older than 4.0.6 StarRocks stores view definitions in a canonical form
        that is semantically identical but syntactically different from the SQL the user wrote.
        This step rewrites both the stored and the model definition into a common form so that
        equivalent definitions compare equal during Alembic autogeneration. It is applied
        symmetrically to both sides, so it can only ever erase a *syntactic* difference, never
        a real schema change.

        Operates on already lower-cased, backtick-stripped, single-spaced SQL.
        """
        if not sql:
            return sql
        # Standardize comma spacing: "a , b" / "a,b" -> "a, b".
        sql = TableAttributeNormalizer._COMMA_SPACE_PATTERN.sub(', ', sql)
        # "INNER JOIN" -> "JOIN"; "<x> OUTER JOIN" -> "<x> JOIN".
        sql = TableAttributeNormalizer._INNER_JOIN_PATTERN.sub('join', sql)
        sql = TableAttributeNormalizer._OUTER_JOIN_PATTERN.sub(r'\1 join', sql)
        # Drop LATERAL before unnest / table functions.
        sql = TableAttributeNormalizer._LATERAL_PATTERN.sub('', sql)
        # Remove injected CTE column lists: "with cat (category, c) as (" -> "with cat as (".
        # Runs before AS removal because the pattern relies on the AS keyword.
        sql = TableAttributeNormalizer._CTE_COLUMN_LIST_PATTERN.sub(r'\1 as (', sql)
        # Drop the optional AS keyword for all aliases (column, table, CAST, CTE).
        sql = TableAttributeNormalizer._ALIAS_AS_PATTERN.sub(' ', sql)
        # Remove redundant parentheses around simple predicates: "(x > 100)" -> "x > 100".
        sql = TableAttributeNormalizer._strip_redundant_predicate_parens(sql)
        return sql

    @staticmethod
    def _strip_redundant_predicate_parens(sql: str) -> str:
        """Remove parentheses that merely wrap a single flat comparison predicate.

        A parenthesized group is stripped only when it contains exactly one comparison and no
        nested parentheses, commas, or AND/OR — so the parentheses are unambiguously redundant
        and removing them cannot change operator grouping. Parentheses that belong to a function
        call (``foo(x = y)``) are left untouched by requiring that the ``(`` is not preceded by
        an identifier character.
        """
        def _replace(match: 're.Match[str]') -> str:
            inner = match.group(1)
            lowered = inner.lower()
            if ' and ' in lowered or ' or ' in lowered:
                return match.group(0)
            start = match.start()
            if start > 0 and (sql[start - 1].isalnum() or sql[start - 1] == '_'):
                # Preceded by an identifier char -> this is a function call, keep the parens.
                return match.group(0)
            return inner

        return TableAttributeNormalizer._REDUNDANT_PREDICATE_PAREN_PATTERN.sub(_replace, sql)

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
        return TableAttributeNormalizer.normalize_column_identifiers(
            partition.partition_method if isinstance(partition, ReflectedPartitionInfo) else partition
        )

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
