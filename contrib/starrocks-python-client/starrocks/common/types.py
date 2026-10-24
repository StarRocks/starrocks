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


class SystemRunMode:
    """`run_mode` values defined by StarRocks"""
    SHARED_DATA = "shared_data"
    SHARED_NOTHING = "shared_nothing"


class TableEngine:
    OLAP = "OLAP"
    CLOUD_NATIVE = "CLOUD_NATIVE"


class TableDistribution:
    HASH = "HASH"
    RANDOM = "RANDOM"


class TableType:
    PRIMARY_KEY = "PRIMARY KEY"
    DUPLICATE_KEY = "DUPLICATE KEY"
    AGGREGATE_KEY = "AGGREGATE KEY"
    UNIQUE_KEY = "UNIQUE KEY"

    # For validation and mapping
    ALL_KEY_TYPES = {PRIMARY_KEY, DUPLICATE_KEY, AGGREGATE_KEY, UNIQUE_KEY}


class TableKey(TableType):
    """Alias name for TableType."""
    pass


class TableModel:
    """ Table type in information_schema.tables_config.TABLE_MODEL
    """
    DUP_KEYS = "DUP_KEYS"
    DUP_KEYS2 = "DUPLICATE_KEYS"
    AGG_KEYS = "AGG_KEYS"
    AGG_KEYS2 = "AGGREGATE_KEYS"
    PRI_KEYS = "PRI_KEYS"
    PRI_KEYS2 = "PRIMARY_KEYS"
    UNQ_KEYS = "UNQ_KEYS"
    UNQ_KEYS2 = "UNIQUE_KEYS"

    # useless
    # TO_TYPE_MAP = {
    #     DUP_KEYS: TableType.DUPLICATE_KEY,
    #     DUP_KEYS2: TableType.DUPLICATE_KEY,
    #     AGG_KEYS: TableType.AGGREGATE_KEY,
    #     AGG_KEYS2: TableType.AGGREGATE_KEY,
    #     PRI_KEYS: TableType.PRIMARY_KEY,
    #     PRI_KEYS2: TableType.PRIMARY_KEY,
    #     UNQ_KEYS: TableType.UNIQUE_KEY,
    #     UNQ_KEYS2: TableType.UNIQUE_KEY,
    # }


class PartitionType:
    RANGE = "RANGE"
    LIST = "LIST"
    EXPRESSION = "EXPRESSION"


class ColumnAggType:
    """StarRocks aggregate functions for the value columns of an AGGREGATE KEY table.

    Pass one as ``Column(..., starrocks_agg_type=ColumnAggType.SUM)``, and mark key columns with
    ``Column(..., starrocks_is_agg_key=True)``. An aggregate state function such as ``"sum(BIGINT)"``, StarRocks'
    ``aggStateDesc``, is accepted too.
    """

    # Key column type, rather than aggregate value type
    KEY = "KEY"

    # Core aggregate types
    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"

    # Specialized aggregate types
    HLL_UNION = "HLL_UNION"
    BITMAP_UNION = "BITMAP_UNION"
    PERCENTILE_UNION = "PERCENTILE_UNION"
    REPLACE = "REPLACE"
    REPLACE_IF_NOT_NULL = "REPLACE_IF_NOT_NULL"

    # Allowed set for validation
    ALLOWED_ITEMS = {
        SUM,
        MIN,
        MAX,
        HLL_UNION,
        BITMAP_UNION,
        PERCENTILE_UNION,
        REPLACE,
        REPLACE_IF_NOT_NULL,
    }

    AGGREGATE_STATE_FUNCTION = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\s*\(.+\)", re.DOTALL)

    @classmethod
    def is_valid(cls, agg_type: str) -> bool:
        """Return whether StarRocks accepts ``agg_type`` as a column's aggregate: a named one or a state function."""
        return agg_type.upper() in cls.ALLOWED_ITEMS or cls.AGGREGATE_STATE_FUNCTION.fullmatch(agg_type) is not None


class ViewSecurityType:
    """StarRocks view security types, as ``View(..., starrocks_security=ViewSecurityType.INVOKER)``.

    StarRocks has ``SECURITY NONE`` and ``SECURITY INVOKER`` only; ``DEFINER`` is kept as a deprecated name.
    """

    EMPTY = ""
    NONE = "NONE"
    INVOKER = "INVOKER"
    DEFINER = "DEFINER"
    """Deprecated: StarRocks has no ``SECURITY DEFINER``, so the dialect refuses it with a CompileError.

    It is kept so that code naming it still imports, and it is not in :attr:`ALLOWED_ITEMS`.
    """

    # Allowed set for validation
    ALLOWED_ITEMS = {
        EMPTY,
        NONE,
        INVOKER,
    }


class MVRefreshMoment:
    """Supported StarRocks materialized view refresh moments.
    """
    IMMEDIATE = "IMMEDIATE"
    DEFERRED = "DEFERRED"

    ALLOWED_ITEMS = {
        IMMEDIATE,
        DEFERRED,
    }


class MVRefreshType:
    """Supported StarRocks materialized view refresh schemes.
    """
    ASYNC = "ASYNC"
    MANUAL = "MANUAL"
    INCREMENTAL = "INCREMENTAL"

    ALLOWED_ITEMS = {
        ASYNC,
        MANUAL,
        INCREMENTAL,
    }
