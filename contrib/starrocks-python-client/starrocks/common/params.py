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

import logging
from typing import Dict, Final, Optional, Tuple

from starrocks.common.types import SystemRunMode, TableModel, TableType


logger = logging.getLogger(__name__)


DialectName: Final[str] = 'starrocks'
"""Dialect name for StarRocks."""

SRKwargsPrefix: Final[str] = 'starrocks_'
"""Prefix for StarRocks-specific kwargs."""


class TableKind:
    """Table kind constants.
    Used in `table.info[TableInfoKey.TABLE_KIND]` to distinguish object types.
    """
    TABLE = "TABLE"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"


class TableObjectInfoKey:
    """Keys for the `info` dictionary on Table objects, used for storing
    cross-dialect metadata about StarRocks objects like Views and MVs.
    """
    TABLE_KIND = "table_kind"
    DEFINITION = "definition"
    SELECTABLE = "_selectable"


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


class AlterMVEnablement:
    """Enablement for ALTER MATERIALIZED VIEW operations.
    """
    RENAME = True
    KEY = False
    COMMENT = False
    PARTITION_BY = False
    DISTRIBUTED_BY = False
    ORDER_BY = False
    REFRESH = True
    PROPERTIES = True


class TableInfoKey:
    """Centralizes starrocks_ prefixed kwargs for Table objects. Clean names without prefix."""

    # Individual key kwargs for clarity
    KEY = 'key'  # Not in the options, but used for comparison
    PRIMARY_KEY = 'primary_key'
    DUPLICATE_KEY = 'duplicate_key'
    AGGREGATE_KEY = 'aggregate_key'
    UNIQUE_KEY = 'unique_key'

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
    ENGINE = 'engine'
    COMMENT = 'comment'
    PARTITION_BY = 'partition_by'
    DISTRIBUTED_BY = 'distributed_by'
    BUCKETS = 'buckets'
    ORDER_BY = 'order_by'
    PROPERTIES = 'properties'
    # for view only
    SECURITY = 'security'
    # for MV only
    REFRESH = 'refresh'


TableInfoKey.ALL = {
    k for k, v in vars(TableInfoKey).items() if not callable(v) and not k.startswith("__")
}


class ColumnAggInfoKey:
    """StarRocks-specific Column.info keys for aggregate-model tables. Clean names without prefix.

    - IS_AGG_KEY: mark a column as a KEY column in AGGREGATE KEY tables.
    - AGG_TYPE: specify the aggregate function for value columns (see ColumnAggType).
    """

    IS_AGG_KEY = "is_agg_key"
    AGG_TYPE = "agg_type"


ColumnAggInfoKey.ALL = {
    k for k, v in vars(ColumnAggInfoKey).items() if not callable(v) and not k.startswith("__")
}


class TableInfoKeyWithPrefix:
    """Centralizes starrocks_ prefixed kwargs for Table objects. Full prefixed names."""

    # Individual key kwargs for clarity
    PRIMARY_KEY = 'starrocks_primary_key'
    DUPLICATE_KEY = 'starrocks_duplicate_key'
    AGGREGATE_KEY = 'starrocks_aggregate_key'
    UNIQUE_KEY = 'starrocks_unique_key'

    # Key type kwargs and their mapping to DDL strings
    KEY_KWARG_MAP = {
        PRIMARY_KEY: TableType.PRIMARY_KEY,
        DUPLICATE_KEY: TableType.DUPLICATE_KEY,
        AGGREGATE_KEY: TableType.AGGREGATE_KEY,
        UNIQUE_KEY: TableType.UNIQUE_KEY,
    }

    # Other table-level kwargs
    ENGINE = 'starrocks_engine'
    COMMENT = 'starrocks_comment'
    PARTITION_BY = 'starrocks_partition_by'
    DISTRIBUTED_BY = 'starrocks_distributed_by'
    BUCKETS = 'starrocks_buckets'
    ORDER_BY = 'starrocks_order_by'
    PROPERTIES = 'starrocks_properties'
    # for view only
    SECURITY = 'starrocks_security'
    # for MV only
    REFRESH = 'starrocks_refresh'


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


class InvalidTableProperty:
    """
    Some properties are invalid in shared-nothing or shared-data architecture.
    So we need to ignore them for comparision.
    """
    @staticmethod
    def get_invalid_properties(run_mode: str):
        if run_mode == SystemRunMode.SHARED_DATA:
            return InvalidTablePropertyForSharedData.ALL
        else:
            return InvalidTablePropertyForSharedNothing.ALL

    @staticmethod
    def purify_dict(property_dict: dict, run_mode: str, table_name: str):
        for invalid_property in InvalidTableProperty.get_invalid_properties(run_mode):
            if invalid_property in property_dict:
                logger.warning(f"Property {invalid_property} is not valid for {run_mode} mode"
                               f"{' table: ' + table_name if table_name else ''}. It will be ignored.")
                property_dict.pop(invalid_property)
        return property_dict


class InvalidTablePropertyForSharedNothing(InvalidTableProperty):
    pass


InvalidTablePropertyForSharedNothing.ALL = {
    v for k, v in vars(InvalidTablePropertyForSharedNothing).items()
        if not k.startswith("__") and isinstance(v, str)
}


class InvalidTablePropertyForSharedData(InvalidTableProperty):
    STORAGE_MEDIUM = "storage_medium"
    STORAGE_COOLDOWN_TIME = "storage_cooldown_time"
    REPLICATED_STORAGE = "replicated_storage"


InvalidTablePropertyForSharedData.ALL = {
    v for k, v in vars(InvalidTablePropertyForSharedData).items()
        if not k.startswith("__") and isinstance(v, str)
}


class UnalterableTableProperty:
    """Table properties StarRocks refuses to change with ``ALTER TABLE ... SET``, mapped to why.

    Checked on StarRocks 3.3.22 and 4.1.6 shared-nothing clusters, and in the 3.5 and 4.0 sources for the
    properties that became alterable later.
    """

    SET_ONLY_AT_CREATE: Final[Dict[str, str]] = {
        'compression': 'StarRocks sets it only when the table is created',
        'enable_async_write_back': 'StarRocks sets it only when the table is created',
        'fast_schema_evolution': 'StarRocks sets it only when the table is created',
        'storage_cooldown_time': 'StarRocks sets it only when the table is created',
        'storage_format': 'StarRocks sets it only when the table is created',
        'storage_type': 'StarRocks sets it only when the table is created',
    }
    SHARED_DATA_ONLY: Final[Dict[str, str]] = {
        'compaction_strategy': 'StarRocks changes it only on shared-data tables',
        'persistent_index_type': 'StarRocks changes it only on shared-data tables',
    }
    FIRST_ALTERABLE_IN: Final[Dict[str, Tuple[Tuple[int, ...], ...]]] = {
        'enable_statistic_collect_on_first_load': ((3, 5, 10), (4, 0, 2)),
        'flat_json.column.max': ((4, 0, 0),),
        'flat_json.enable': ((4, 0, 0),),
        'flat_json.null.factor': ((4, 0, 0),),
        'flat_json.sparsity.factor': ((4, 0, 0),),
    }
    """For each property, the first version of each release line that can change it."""

    @classmethod
    def reason(cls, key: str, run_mode: str, server_version: Optional[Tuple[int, ...]]) -> Optional[str]:
        """Why StarRocks refuses to SET property ``key`` (lowercase) on this cluster, or None if it accepts it.

        Without a server version, as in offline mode, a property that only later versions accept is not refused.
        """
        if key in cls.SET_ONLY_AT_CREATE:
            return cls.SET_ONLY_AT_CREATE[key]
        if key in cls.SHARED_DATA_ONLY and run_mode != SystemRunMode.SHARED_DATA:
            return cls.SHARED_DATA_ONLY[key]
        first_alterable = cls.FIRST_ALTERABLE_IN.get(key)
        if first_alterable and server_version and not cls._alterable_on(tuple(server_version), first_alterable):
            return "StarRocks changes it only from " + " and ".join(
                ".".join(map(str, version)) for version in first_alterable
            )
        return None

    @staticmethod
    def _alterable_on(version: Tuple[int, ...], first_alterable: Tuple[Tuple[int, ...], ...]) -> bool:
        """Whether ``version`` is at least the first alterable version of its release line, or of a later line."""
        release_line = version[:2]
        for first in first_alterable:
            if release_line == first[:2]:
                return version >= first
        return release_line > max(first[:2] for first in first_alterable)


class UnalterableMaterializedViewProperty:
    """Materialized view properties StarRocks refuses to change with ``ALTER MATERIALIZED VIEW ... SET``.

    Checked on StarRocks 3.3.22 and 4.1.6 shared-nothing clusters, on partitioned and unpartitioned materialized
    views. StarRocks accepts the rest of what it reports, such as ``mv_rewrite_staleness_second`` and ``session.*``.
    """

    REFUSED: Final[frozenset] = frozenset({
        'bucket_size',
        'colocate_with',
        'compaction_strategy',
        'compression',
        'enable_async_write_back',
        'enable_load_profile',
        'enable_persistent_index',
        'enable_statistic_collect_on_first_load',
        'fast_schema_evolution',
        'flat_json.enable',
        'in_memory',
        'mutable_bucket_num',
        'partition_live_number',
        'persistent_index_type',
        'primary_index_cache_expire_sec',
        'replicated_storage',
        'replication_num',
        'storage_cooldown_time',
        'storage_format',
        'storage_medium',
        'storage_type',
        'write_quorum',
    })

    @classmethod
    def reason(cls, key: str) -> Optional[str]:
        """Why StarRocks refuses to SET property ``key`` (lowercase) on a materialized view, or None if it accepts it."""
        return "ALTER MATERIALIZED VIEW cannot change it" if key in cls.REFUSED else None


class ColumnAggInfoKeyWithPrefix:
    """StarRocks-specific Column.info keys for aggregate-model tables. Full prefixed names.

    - IS_AGG_KEY: mark a column as a KEY column in AGGREGATE KEY tables.
    - AGG_TYPE: specify the aggregate function for value columns (see ColumnAggType).
    """

    IS_AGG_KEY = "starrocks_is_agg_key"
    AGG_TYPE = "starrocks_agg_type"


ColumnAggInfoKeyWithPrefix.ALL = {
    k for k, v in vars(ColumnAggInfoKeyWithPrefix).items() if not callable(v) and not k.startswith("__")
}
