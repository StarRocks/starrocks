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

from typing import Final

from starrocks.common.types import TableModel, TableType


DialectName: Final[str] = 'starrocks'
"""Dialect name for StarRocks."""

SRKwargsPrefix: Final[str] = 'starrocks_'
"""Prefix for StarRocks-specific kwargs."""


class AlterTableEnablement:
    """Enablement for ALTER TABLE operations.
    Only support the operations that are supported by StarRocks.
    """
    ENGINE = False
    KEY = True  # columns may be changed, but type is not supported to change
    TABLE_TYPE = KEY
    COMMENT = True
    PARTITION_BY = False
    DISTRIBUTED_BY = True
    ORDER_BY = True
    PROPERTIES = True


class TableInfoKey:
    """Centralizes starrocks_ prefixed kwargs for Table objects. Clean names without prefix."""

    # Individual key kwargs for clarity
    KEY = 'KEY'  # Not in the options, but used for comparison
    PRIMARY_KEY = 'PRIMARY_KEY'
    DUPLICATE_KEY = 'DUPLICATE_KEY'
    AGGREGATE_KEY = 'AGGREGATE_KEY'
    UNIQUE_KEY = 'UNIQUE_KEY'

    # Key type kwargs and their mapping to DDL strings
    KEY_KWARG_MAP = {
        PRIMARY_KEY: TableType.PRIMARY_KEY,
        DUPLICATE_KEY: TableType.DUPLICATE_KEY,
        AGGREGATE_KEY: TableType.AGGREGATE_KEY,
        UNIQUE_KEY: TableType.UNIQUE_KEY,
    }
    MODEL_TO_KEY_MAP = {
        TableModel.PRI_KEYS: PRIMARY_KEY,
        TableModel.PRI_KEYS2: PRIMARY_KEY,
        TableModel.UNQ_KEYS: UNIQUE_KEY,
        TableModel.UNQ_KEYS2: UNIQUE_KEY,
        TableModel.DUP_KEYS: DUPLICATE_KEY,
        TableModel.DUP_KEYS2: DUPLICATE_KEY,
        TableModel.AGG_KEYS: AGGREGATE_KEY,
        TableModel.AGG_KEYS2: AGGREGATE_KEY,
    }

    # Other table-level kwargs
    ENGINE = 'ENGINE'
    COMMENT = 'COMMENT'
    PARTITION_BY = 'PARTITION_BY'
    DISTRIBUTED_BY = 'DISTRIBUTED_BY'
    BUCKETS = 'BUCKETS'
    ORDER_BY = 'ORDER_BY'
    PROPERTIES = 'PROPERTIES'


TableInfoKey.ALL = {
    k for k, v in vars(TableInfoKey).items() if not callable(v) and not k.startswith("__")
}


class ColumnAggInfoKey:
    """StarRocks-specific Column.info keys for aggregate-model tables. Clean names without prefix.

    - IS_AGG_KEY: mark a column as a KEY column in AGGREGATE KEY tables.
    - AGG_TYPE: specify the aggregate function for value columns (see ColumnAggType).
    """

    IS_AGG_KEY = "IS_AGG_KEY"
    AGG_TYPE = "AGG_TYPE"


ColumnAggInfoKey.ALL = {
    k for k, v in vars(ColumnAggInfoKey).items() if not callable(v) and not k.startswith("__")
}


class TableInfoKeyWithPrefix:
    """Centralizes starrocks_ prefixed kwargs for Table objects. Full prefixed names."""

    # Individual key kwargs for clarity
    PRIMARY_KEY = 'starrocks_PRIMARY_KEY'
    DUPLICATE_KEY = 'starrocks_DUPLICATE_KEY'
    AGGREGATE_KEY = 'starrocks_AGGREGATE_KEY'
    UNIQUE_KEY = 'starrocks_UNIQUE_KEY'

    # Key type kwargs and their mapping to DDL strings
    KEY_KWARG_MAP = {
        PRIMARY_KEY: TableType.PRIMARY_KEY,
        DUPLICATE_KEY: TableType.DUPLICATE_KEY,
        AGGREGATE_KEY: TableType.AGGREGATE_KEY,
        UNIQUE_KEY: TableType.UNIQUE_KEY,
    }

    # Other table-level kwargs
    ENGINE = 'starrocks_ENGINE'
    COMMENT = 'starrocks_COMMENT'
    PARTITION_BY = 'starrocks_PARTITION_BY'
    DISTRIBUTED_BY = 'starrocks_DISTRIBUTED_BY'
    BUCKETS = 'starrocks_BUCKETS'
    ORDER_BY = 'starrocks_ORDER_BY'
    PROPERTIES = 'starrocks_PROPERTIES'


TableInfoKeyWithPrefix.ALL = {
    k for k, v in vars(TableInfoKeyWithPrefix).items() if not callable(v) and not k.startswith("__")
}


class TablePropertyForFuturePartitions:
    """Table properties that support change on future partition data, rather than all the data."""
    REPLICATION_NUM = "replication_num"
    STORAGE_MEDIUM = "storage_medium"

    @classmethod
    def contains(cls, property_name: str) -> bool:
        return property_name.lower() in cls.ALL if property_name else False

    @classmethod
    def wrap(cls, property_name: str) -> str:
        return f"default.{property_name}" if TablePropertyForFuturePartitions.contains(property_name) else property_name


TablePropertyForFuturePartitions.ALL = {
    v for k, v in vars(TablePropertyForFuturePartitions).items()
        if not k.startswith("__") and isinstance(v, str)
}


class ColumnAggInfoKeyWithPrefix:
    """StarRocks-specific Column.info keys for aggregate-model tables. Full prefixed names.

    - IS_AGG_KEY: mark a column as a KEY column in AGGREGATE KEY tables.
    - AGG_TYPE: specify the aggregate function for value columns (see ColumnAggType).
    """

    IS_AGG_KEY = "starrocks_IS_AGG_KEY"
    AGG_TYPE = "starrocks_AGG_TYPE"


ColumnAggInfoKeyWithPrefix.ALL = {
    k for k, v in vars(ColumnAggInfoKeyWithPrefix).items() if not callable(v) and not k.startswith("__")
}


ColumnSROptionsKey: str = "column_sr_options"
"""Column StarRocks-specific options key, used to store StarRocks-specific options in the Column object.
"""
