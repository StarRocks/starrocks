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

class SystemRunMode:
    SHARED_DATA = "shared_data"
    SHARED_NOTHING = "shared_nothing"


class TableEngine:
    OLAP = "OLAP"


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
    """Supported StarRocks aggregate functions for value columns.

    Use these constants in Column.info["starrocks_agg"].
    """

    # Key column type, rather than aggregate value type
    KEY = "KEY"

    # Core aggregate types
    SUM = "SUM"
    COUNT = "COUNT"
    MIN = "MIN"
    MAX = "MAX"

    # Specialized aggregate types
    HLL_UNION = "HLL_UNION"
    BITMAP_UNION = "BITMAP_UNION"
    REPLACE = "REPLACE"
    REPLACE_IF_NOT_NULL = "REPLACE_IF_NOT_NULL"

    # Allowed set for validation
    ALLOWED_ITEMS = {
        SUM,
        COUNT,
        MIN,
        MAX,
        HLL_UNION,
        BITMAP_UNION,
        REPLACE,
        REPLACE_IF_NOT_NULL,
    }


class ViewSecurityType:
    """Supported StarRocks view security types.

    Use these constants in View.info["security"].
    """

    EMPTY = ""
    NONE = "NONE"
    INVOKER = "INVOKER"
    DEFINER = "DEFINER"

    # Allowed set for validation
    ALLOWED_ITEMS = {
        EMPTY,
        NONE,
        INVOKER,
        DEFINER,
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
