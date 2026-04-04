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

from __future__ import annotations

import dataclasses
from enum import Enum
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, TypedDict, Union

try:
    from sqlalchemy.engine.interfaces import ReflectedColumn
except:
    class ReflectedColumn:
        pass

from starrocks.common.params import TableInfoKeyWithPrefix, TableKind


"""
Follow the mysql's ReflectedState, but with more specific types
It will be much cleaner and easier to use.
"""


def add_cached_str_clause(cls):
    """Add cached full string clause for a table attribute."""

    cls._cached_clause = None

    original_str = getattr(cls, "__str__", None)

    def __str__(self):
        if getattr(self, "_cached_clause", None) is None:
            self._cached_clause = original_str(self)
        return self._cached_clause

    cls.__str__ = __str__
    return cls


@dataclasses.dataclass
class ReflectedState:
    """Reflected information for a Table (base class). Similar to MySQL's implementation.
    comment is in table_options as the key `{dialect_name}_comment`.
    """
    table_name: Optional[str]
    columns: List[ReflectedColumn] = dataclasses.field(default_factory=list)
    table_options: Dict[str, Any] = dataclasses.field(default_factory=dict)
    keys: List[Union['ReflectedIndexInfo', 'ReflectedPKInfo', 'ReflectedUKInfo']] = dataclasses.field(default_factory=list)
    fk_constraints: List['ReflectedFKInfo'] = dataclasses.field(default_factory=list)
    ck_constraints: List['ReflectedCKInfo'] = dataclasses.field(default_factory=list)

    @property
    def name(self) -> Optional[str]:
        return self.table_name

    @name.setter
    def name(self, value: str) -> None:
        self.table_name = value

    @property
    def table_kind(self) -> str:
        return TableKind.TABLE

    @property
    def comment(self) -> Optional[str]:
        """Get the comment easily only, not able to write to it."""
        return self.table_options.get(TableInfoKeyWithPrefix.COMMENT)

    @property
    def partition_info(self) -> Optional['ReflectedPartitionInfo']:
        return self.table_options.get(TableInfoKeyWithPrefix.PARTITION_BY)

    @property
    def distribution_info(self) -> Optional['ReflectedDistributionInfo']:
        return self.table_options.get(TableInfoKeyWithPrefix.DISTRIBUTED_BY)

    @property
    def refresh_info(self) -> Optional['ReflectedRefreshInfo']:
        return self.table_options.get(TableInfoKeyWithPrefix.REFRESH)

    @property
    def order_by(self) -> Optional[str]:
        return self.table_options.get(TableInfoKeyWithPrefix.ORDER_BY)

    @property
    def properties(self) -> Optional[Union[str, Dict[str, str]]]:
        return self.table_options.get(TableInfoKeyWithPrefix.PROPERTIES)


@dataclasses.dataclass
class ReflectedViewState(ReflectedState):
    """Reflected information for a View"""
    definition: str = ''
    # security: Optional[str] = None # Moved to table_options

    # Views have no primary keys, foreign keys, or indexes
    keys: list = dataclasses.field(default_factory=list, init=False)
    fk_constraints: list = dataclasses.field(default_factory=list, init=False)

    @property
    def table_kind(self) -> str:
        return TableKind.VIEW

    @property
    def view_name(self) -> Optional[str]:
        return self.table_name

    @view_name.setter
    def view_name(self, value: str) -> None:
        self.table_name = value

    @property
    def security(self) -> Optional[str]:
        """Get the security type easily only, not able to write to it."""
        return self.table_options.get(TableInfoKeyWithPrefix.SECURITY)


@dataclasses.dataclass
class ReflectedMVState(ReflectedViewState):
    """Reflected information for a Materialized View"""

    @property
    def table_kind(self) -> str:
        return TableKind.MATERIALIZED_VIEW

    @property
    def mv_name(self) -> Optional[str]:
        return self.table_name

    @mv_name.setter
    def mv_name(self, value: str) -> None:
        self.table_name = value


@add_cached_str_clause
@dataclasses.dataclass(unsafe_hash=True)
class ReflectedRefreshInfo:
    """Stores structured reflection information about a materialized view's refresh scheme."""
    moment: Optional[str] = None
    type: Optional[str] = None

    def __str__(self) -> str:
        parts = []
        if self.moment:
            parts.append(self.moment)
        if self.type:
            parts.append(self.type)
        return " ".join(parts)


class MySQLKeyType(Enum):
    PRIMARY = "PRIMARY"
    UNIQUE = "UNIQUE"
    FULLTEXT = "FULLTEXT"
    SPATIAL = "SPATIAL"


class ReflectedIndexInfo(TypedDict):
    """In ReflectedState.keys
    And, will be used to form ReflectedIndex
    """
    name: str
    type: MySQLKeyType
    parser: Optional[Any]  # Object?
    columns: List[Tuple[str, int, Any]]  # name, length, ...
    dialect_options: Dict[str, Any]


class ReflectedFKInfo(TypedDict):
    """In ReflectedState.fk_constraints
    And, will be used to form ReflectedForeignKeyConstraint
    """
    name: str
    table: NamedTuple[Optional[str], str]  # schema, name
    local: List[str]
    foreign: List[str]

    onupdate: bool
    ondelete: bool


class ReflectedPKInfo(TypedDict):
    """In ReflectedState.keys
    And, will be used to form ReflectedPrimaryKeyConstraint
    """
    type: MySQLKeyType
    columns: List[Tuple[str, Any, Any]]


class ReflectedUKInfo(TypedDict):
    """In ReflectedState.keys
    And, will be used to form ReflectedUniqueConstraint
    """
    name: str
    type: MySQLKeyType
    columns: List[Tuple[str, Any, Any]]


class ReflectedCKInfo(TypedDict):
    """In ReflectedState.ck_constraints
    And, will be used to form ReflectedCheckConstraint
    """
    name: str
    sqltext: str


@add_cached_str_clause
@dataclasses.dataclass(**(dict(kw_only=True) if 'KW_ONLY' in dataclasses.__all__ else {}))
class ReflectedTableKeyInfo:
    """
    Stores structed reflection information about a table' key/type.
    Such as `PRIMARY KEY (id, name)`, `UNIQUE KEY (id, name)`, `DUPLICATE KEY (id, name)`.

    Attributes:
        type: The key type string (e.g., 'PRIMARY KEY', 'UNIQUE KEY', 'DUPLICATE KEY').
        columns: The key columns as a list of strings or a single string (e.g., ['id', 'name'] or 'id, name').
    """
    type: str
    columns: Optional[Union[List[str], str]]

    def __hash__(self) -> int:
        cols = tuple(self.columns) if isinstance(self.columns, list) else self.columns
        return hash((self.type, cols))

    def __str__(self) -> str:
        type_str = self.type.upper() if self.type else self.type
        columns_str = self.columns.strip() if isinstance(self.columns, str) else self.columns
        if isinstance(columns_str, list):
            return f"{type_str} ({', '.join(columns_str)})"
        return f"{type_str} ({columns_str})"

    def __repr__(self) -> str:
        return repr(str(self))


@add_cached_str_clause
@dataclasses.dataclass(unsafe_hash=True, **(dict(kw_only=True) if 'KW_ONLY' in dataclasses.__all__ else {}))
class ReflectedPartitionInfo:
    """
    Stores structured reflection information about a table's partitioning scheme.

    Attributes:
        type: The partitioning type (e.g., 'RANGE', 'LIST', 'EXPRESSION').
        partition_by: The whole partitioning expression string
            (e.g., 'RANGE(id, name)', 'date_trunc('day', dt)', 'id, col1, col2').
        pre_created_partitions: A string containing the full DDL for all
            pre-created partitions (e.g.,
            "(PARTITION p1 VALUES LESS THAN ('100'), PARTITION p2 VALUES LESS THAN ('200'))").
    """
    type: str
    partition_method: str  # includes the type and expressions
    pre_created_partitions: Optional[str] = None

    def __str__(self) -> str:
        method_str = self.partition_method.strip() if self.partition_method else self.partition_method
        if self.pre_created_partitions:
            return f"{method_str} {self.pre_created_partitions}"
        return f"{method_str}"

    def __repr__(self) -> str:
        return repr(str(self))


@add_cached_str_clause
@dataclasses.dataclass(**(dict(kw_only=True) if 'KW_ONLY' in dataclasses.__all__ else {}))
class ReflectedDistributionInfo:
    """Stores reflection information about a view."""
    type: Union[str, None]
    """The distribution type string like 'HASH' or 'RANDOM'."""
    columns: Optional[Union[List[str], str]] | None
    """The distribution columns string like 'id' or 'id, name'."""
    distribution_method: Union[str, None]
    """The distribution method string like 'HASH(id)' or 'RANDOM' without BUCKETS.
    It will be used first if it's not None."""
    buckets: Union[int, None]
    """The buckets count."""

    def __hash__(self) -> int:
        cols = tuple(self.columns) if isinstance(self.columns, list) else self.columns
        return hash((self.type, cols, self.distribution_method, self.buckets))

    def __str__(self) -> str:
        """Convert to string representation of distribution option."""
        buckets_str = f' BUCKETS {self.buckets}' if self.buckets and str(self.buckets) != "0" else ""
        method = self.distribution_method
        if not method:
            distribution_cols = ', '.join(self.columns) if isinstance(self.columns, list) else self.columns
            distribution_cols_str = f'({distribution_cols})' if distribution_cols else ""
            method = f'{self.type}{distribution_cols_str}'
        return f'{method}{buckets_str}'

    def __repr__(self) -> str:
        return repr(str(self))
