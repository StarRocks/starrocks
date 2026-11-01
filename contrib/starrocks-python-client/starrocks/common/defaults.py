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

import re
from typing import Collection, Optional, Tuple, Union

try:
    from sqlalchemy.engine.reflection import ReflectionDefaults
except:
    class ReflectionDefaults:
        @classmethod
        def table_comment(cls):
            return {"text": None}

from starrocks.common.params import TableInfoKeyWithPrefix
from starrocks.common.types import SystemRunMode, TableDistribution, TableEngine, TableType, ViewSecurityType
from starrocks.common.utils import TableAttributeNormalizer
from starrocks.engine.interfaces import ReflectedMVState, ReflectedTableKeyInfo, ReflectedViewState


class ReflectionTableDefaults(ReflectionDefaults):
    """StarRocks table reflection default values management."""

    # Properties StarRocks fills in when a table does not set them. Different defaults for
    # different run modes.
    #
    # Autogenerate never resets one of these when the model omits it: many depend on the
    # StarRocks version or FE config (default_replication_num, default_automatic_bucket_size,
    # ...), so a hardcoded value cannot tell a user-set value from one StarRocks filled in.
    # The values here are only used when the model sets a property the database does not
    # report, as the database's effective value and as the downgrade target.
    _DEFAULT_PROPERTIES = {
        'compression': 'LZ4',
        'fast_schema_evolution': 'true',
        'replicated_storage': 'true',
        'storage_format': 'DEFAULT',
        # FE default_automatic_bucket_size: 4294967296 before 4.0, 1073741824 from 4.0 (#63168).
        # Only reported for RANDOM-distributed tables; see _UNREPORTED_PROPERTY_REASONS.
        'bucket_size': '4294967296',
        'storage_medium': 'HDD',

        # for following properties, they won't explicitly set in the 'properties' field of the table.
        'enable_persistent_index': 'true',
        # tables_config only records this when explicitly set to false; absent means true.
        # Must be 'true' here: ALTER SET true clears the override (→ absent) → decodes back
        # to 'true', converging.
        'enable_statistic_collect_on_first_load': 'true',

        # Only reported when they differ from these values (OlapTable.getProperties).
        'write_quorum': 'MAJORITY',
        'compaction_strategy': 'DEFAULT',  # 4.0+
        'enable_load_profile': 'false',
        'mutable_bucket_num': '0',
        'primary_index_cache_expire_sec': '0',
        'storage_type': 'column',

        # Added when a dynamic_partition.* property is set, whatever it is (even enable=false).
        # time_zone is the FE time zone, so any value here only marks the key as known.
        'dynamic_partition.history_partition_num': '0',
        'dynamic_partition.time_zone': 'Etc/UTC',
        # Added when the table sets no value for them.
        'dynamic_partition.enable': 'true',
        'dynamic_partition.prefix': 'p',
        # Added when the table sets no start, and for a WEEK or MONTH time_unit when it sets no first day.
        'dynamic_partition.start': '-2147483648',
        'dynamic_partition.start_day_of_week': '1',
        'dynamic_partition.start_day_of_month': '1',

        # Added when flat_json.enable is true.
        'flat_json.null.factor': '0.3',
        'flat_json.sparsity.factor': '0.3',
        'flat_json.column.max': '100',

        # Absent until set; SET to the empty string clears them again.
        'bloom_filter_columns': '',
        'colocate_with': '',
    }
    _DEFAULT_PROPERTIES_SHARED_NOTHING = {**{
        'replication_num': '3',
    }, **_DEFAULT_PROPERTIES}

    _DEFAULT_PROPERTIES_SHARED_DATA = {**{
        'replication_num': '1',  # Different for shared-data
    }, **_DEFAULT_PROPERTIES}

    # Properties StarRocks may not report even when the CREATE set them, mapped to why. When the
    # metadata sets one to a non-default value and the database does not report it, the table
    # cannot hold it, so autogenerate warns instead of generating an ALTER that would fail or
    # repeat on every run.
    _UNREPORTED_PROPERTY_REASONS: dict = {
        'storage_format': 'StarRocks never reports it',
        'bucket_size': 'StarRocks only reports it for RANDOM-distributed tables with automatic bucketing',
        'enable_persistent_index': 'StarRocks only reports it for PRIMARY KEY tables',
    }

    # Enum-valued properties StarRocks accepts in any case but reports in its own case.
    _CASE_INSENSITIVE_PROPERTIES: frozenset = frozenset({
        'compaction_strategy',
        'compression',
        'persistent_index_type',
        'query_rewrite_consistency',
        'storage_format',
        'storage_medium',
        'storage_type',
        'write_quorum',
    })

    # Properties that hold a set of column names, which StarRocks reports in its own order.
    _COLUMN_SET_PROPERTIES: frozenset = frozenset({
        'bloom_filter_columns',
    })

    # MV properties that list tables, which StarRocks reports without the spaces after the commas.
    # Table names are case sensitive, so unlike column names they keep their case.
    _TABLE_LIST_PROPERTIES: frozenset = frozenset({
        'excluded_refresh_tables',
        'excluded_trigger_tables',
    })

    # Default table options
    # engine -> key -> comment -> partition -> distribution -> order by -> properties

    @classmethod
    def engine(cls) -> str:
        return TableEngine.OLAP

    @classmethod
    def normalize_engine(cls, engine: Optional[str]) -> str:
        """Normalize engine: None, empty, or OLAP are all treated as OLAP."""
        return TableAttributeNormalizer.normalize_engine(engine) if engine else cls.engine()

    @classmethod
    def key(cls) -> str:
        return TableType.DUPLICATE_KEY

    @classmethod
    def normalize_key(cls, key: Optional[str]) -> str:
        """Normalize key: None, empty, or DUPLICATE KEY are all treated as DUPLICATE KEY."""
        return TableAttributeNormalizer.normalize_key(key) if key else cls.key()

    @classmethod
    def reflected_key_info(cls) -> ReflectedTableKeyInfo:
        return ReflectedTableKeyInfo(type=cls.key(), columns=None)

    @classmethod
    def comment(cls) -> str:
        return None

    @classmethod
    def partition_by(cls) -> Optional[str]:
        return None

    @classmethod
    def distribution_type(cls) -> str:
        """Get default distribution method. such as HASH, RANDOM."""
        return TableDistribution.RANDOM

    @classmethod
    def buckets(cls) -> int:
        """Get default buckets count."""
        return 0

    @classmethod
    def distribution(cls) -> str:
        """Get default distribution by. such as 'HASH(id) BUCKETS 10'."""
        return TableDistribution.RANDOM

    @classmethod
    def order_by(cls) -> Optional[str]:
        """Get default order by."""
        return None

    @classmethod
    def properties(cls, run_mode: str = SystemRunMode.SHARED_NOTHING) -> dict:
        """Get default properties based on run_mode.
        Keep mind not to change the default properties, because it will affect the table creation.
        Or you need to make a copy of the default properties.
        """
        if run_mode == SystemRunMode.SHARED_DATA:
            return cls._DEFAULT_PROPERTIES_SHARED_DATA
        else:
            return cls._DEFAULT_PROPERTIES_SHARED_NOTHING

    @classmethod
    def unreported_property_reason(cls, key: str) -> Optional[str]:
        """Why StarRocks may not report property ``key`` (lowercase), or None if it always
        reports it once set. See ``_UNREPORTED_PROPERTY_REASONS``.
        """
        return cls._UNREPORTED_PROPERTY_REASONS.get(key)

    @classmethod
    def normalize_property_value(cls, key: str, value: Optional[str]) -> Optional[str]:
        """Normalize a property value for comparison: booleans and the enums in
        ``_CASE_INSENSITIVE_PROPERTIES`` compare case-insensitively, the column sets of
        ``_COLUMN_SET_PROPERTIES`` in any order and case, as StarRocks column names are, and the table lists of
        ``_TABLE_LIST_PROPERTIES`` in any order and spacing.
        """
        if value is None:
            return None
        if key in cls._COLUMN_SET_PROPERTIES:
            return ", ".join(sorted({name.strip().strip('`').lower() for name in value.split(",") if name.strip()}))
        if key in cls._TABLE_LIST_PROPERTIES:
            return ",".join(sorted({name.strip().replace("`", "") for name in value.split(",") if name.strip()}))
        if key == 'unique_constraints':
            return cls._normalize_unique_constraints(value)
        if key == 'foreign_key_constraints':
            return cls._normalize_foreign_key_constraints(value)
        if key in cls._CASE_INSENSITIVE_PROPERTIES or value.lower() in ('true', 'false'):
            return value.lower()
        return value

    @classmethod
    def downgrade_value(
        cls, key: str, database_value: Optional[str], default_value: Optional[str], database_keys: Collection[str]
    ) -> Optional[str]:
        """Return the value a downgrade sets ``key`` back to, or None when the dialect has none to set.

        That is the database's value, or else the property's default. A ``dynamic_partition.*`` default only stands
        for what StarRocks adds once the table has some such property, so on a table with none it is no value to go
        back to: ``dynamic_partition.enable`` goes back to ``false``, which leaves the table with no dynamic
        partitioning, and the other properties have no value to restore.
        """
        if database_value is not None:
            return database_value
        prefix = 'dynamic_partition.'
        if key.startswith(prefix) and not any(name.lower().startswith(prefix) for name in database_keys):
            return 'false' if key == f'{prefix}enable' else None
        return default_value

    @staticmethod
    def _normalize_unique_constraints(value: str) -> str:
        """Normalize ``unique_constraints`` for comparison, as the set of ``table.column,column`` constraints.

        StarRocks reports ``t.id,dt;u.id`` as ``default_catalog.db.t.id,dt;default_catalog.db.u.id``, so the catalog
        and database are dropped, and the order of constraints and of the columns in one does not matter.
        """
        constraints = set()
        for item in value.split(";"):
            names = [name.strip().replace("`", "").lower() for name in item.split(",") if name.strip()]
            if not names:
                continue
            table, _, first_column = names[0].rpartition(".")
            columns = {first_column, *(name.rpartition(".")[2] for name in names[1:])}
            constraints.add(f"{table.rpartition('.')[2]}.{','.join(sorted(columns))}")
        return ";".join(sorted(constraints))

    @staticmethod
    def _normalize_foreign_key_constraints(value: str) -> str:
        """Normalize ``foreign_key_constraints`` for comparison, as the set of ``t(c) REFERENCES u(c)`` constraints.

        StarRocks reports ``t(id) REFERENCES u(id)`` with both tables qualified by catalog and database, so those are
        dropped, and the order of the constraints does not matter.
        """
        qualifier = re.compile(r"(?:[\w$]+\.){1,2}(?=[\w$]+\s*\()")
        constraints = set()
        for item in value.replace("`", "").split(";"):
            if item.strip():
                constraints.add(re.sub(r"\s+", " ", qualifier.sub("", item.strip().lower())).replace(" (", "("))
        return ";".join(sorted(constraints))


class ReflectionViewDefaults(ReflectionTableDefaults):
    """Central place for view reflection default values and normalization."""

    @classmethod
    def security(cls) -> str:
        return ViewSecurityType.NONE

    @classmethod
    def apply(
        cls,
        *,
        name: str,
        definition: str,
        comment: Union[str, None] = None,
        security: Union[str, None] = None,
    ) -> ReflectedViewState:
        """Apply defaults and normalization to reflected view values.

        - comment: default empty string
        - security: default empty string, uppercase when present
        """
        normalized_comment = (comment or cls.comment())
        normalized_security = (security or cls.security()).upper()
        state = ReflectedViewState(
            table_name=name,
            definition=definition,
        )
        state.table_options[TableInfoKeyWithPrefix.COMMENT] = normalized_comment
        state.table_options[TableInfoKeyWithPrefix.SECURITY] = normalized_security
        return state

    @classmethod
    def apply_info(cls, reflection_view_info: ReflectedViewState) -> ReflectedViewState:
        """Apply defaults and normalization to reflected view values.
        Recently, there is nothing to change.
        """
        return reflection_view_info


class ReflectionMVDefaults(ReflectionViewDefaults):
    """Central place for materialized view reflection default values and normalization."""

    _DEFAULT_PROPERTIES = {
        "mv_rewrite_staleness_second": "0",
        "storage_medium": "HDD",
    }

    _DEFAULT_PROPERTIES_SHARED_NOTHING = {**{
        'replication_num': '3',
    }, **_DEFAULT_PROPERTIES}

    _DEFAULT_PROPERTIES_SHARED_DATA = {**{
        'replication_num': '1',  # Different for shared-data
    }, **_DEFAULT_PROPERTIES}

    # Properties that information_schema.tables_config omits for materialized views, and which
    # must therefore be read from the PROPERTIES clause of the CREATE MATERIALIZED VIEW ddl.
    # Without this, such a property looks absent on the reflected side while being present in
    # the metadata, so autogenerate reports the same phantom change on every run.
    #
    # Keep this an explicit allowlist rather than merging the whole ddl PROPERTIES clause:
    # SHOW CREATE MATERIALIZED VIEW also prints effective values that tables_config leaves out
    # on purpose (e.g. replicated_storage), and those have no entry in _DEFAULT_PROPERTIES, so
    # merging them in would warn about an unmanaged database value on every autogenerate.
    #
    # StarRocks 3.3.22, 3.5.21 and 4.1.6 all leave out these, and every session.* property.
    _DDL_ONLY_PROPERTY_KEYS: frozenset = frozenset({
        'colocate_with',
        'enable_query_rewrite',
        'excluded_refresh_tables',
        'force_external_table_query_rewrite',
        'foreign_key_constraints',
        'partition_retention_condition',
        'query_rewrite_consistency',
        'resource_group',
        'unique_constraints',
    })
    _DDL_ONLY_PROPERTY_PREFIXES: Tuple[str, ...] = ('session.',)

    @classmethod
    def is_ddl_only_property(cls, key: str) -> bool:
        """Whether to take the MV property from the MV's DDL, because tables_config leaves it out.

        See ``_DDL_ONLY_PROPERTY_KEYS``. StarRocks 3.3 has no ``partition_retention_condition`` for an MV, and 4.1.6
        leaves it out of tables_config, as it does not for a table.
        """
        key = key.lower()
        return key in cls._DDL_ONLY_PROPERTY_KEYS or key.startswith(cls._DDL_ONLY_PROPERTY_PREFIXES)

    @classmethod
    def apply(cls, *, name: str, definition: str, comment: Union[str, None] = None, security: Union[str, None] = None) -> ReflectedMVState:
        """Apply defaults and normalization to reflected materialized view values.
        """
        return ReflectedMVState(
            name=name,
            definition=definition,
            comment=(comment or cls.comment()),
            security=(security or cls.security()).upper(),
        )

    @classmethod
    def apply_info(cls, reflection_mv_info: ReflectedMVState) -> ReflectedMVState:
        """Apply defaults and normalization to reflected materialized view values.
        Recently, there is nothing to change.
        """
        return reflection_mv_info
