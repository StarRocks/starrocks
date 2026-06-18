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

from typing import Optional, Union

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

    # StarRocks default properties that are automatically set
    # Note: These values should match StarRocks documentation defaults
    # Different defaults for different run modes
    _DEFAULT_PROPERTIES = {
        'compression': 'LZ4',
        'fast_schema_evolution': 'true',
        'replicated_storage': 'true',
        'storage_format': 'DEFAULT',
        'bucket_size': '4294967296',
        'storage_medium': 'HDD',

        # for following properties, they won't explicitly set in the 'properties' field of the table.
        'enable_persistent_index': 'true',
        # `enable_statistic_collect_on_first_load` is recorded in information_schema.tables_config
        # ONLY when the table has an explicit `false` override; a `true` override or an unset
        # value is never reported (verified on 3.5.16, independent of the FE-level global
        # default, which tables_config does not reflect). So this dict value is a *decoder* for
        # an absent entry, not a mirror of the cluster default. It must be 'true' because that is
        # the only loop-safe fixpoint: `ALTER ... SET (...='true')` clears the false override
        # (-> absent), and an absent entry decodes back to 'true', so the comparison converges.
        # Populating it from the live global default would break this (absent != global) and
        # could cause a never-converging phantom migration. See _compare_table_properties_impl
        # (compares effective value-or-default) and skip_implicit_reset_properties (which keeps
        # an unmanaged-in-metadata property from being reset).
        'enable_statistic_collect_on_first_load': 'true',
        # 'bloom_filter_columns': None,
        # 'colocate_with': None,
    }
    _DEFAULT_PROPERTIES_SHARED_NOTHING = {**{
        'replication_num': '3',
    }, **_DEFAULT_PROPERTIES}

    _DEFAULT_PROPERTIES_SHARED_DATA = {**{
        'replication_num': '1',  # Different for shared-data
    }, **_DEFAULT_PROPERTIES}

    # Properties that must NOT be implicitly reset to their default when they are absent from
    # the metadata. For most properties, an absent-from-metadata property with a non-default
    # database value triggers an implicit "reset to default" ALTER. That behavior is wrong for
    # properties like `enable_statistic_collect_on_first_load`: StarRocks only reports it in
    # information_schema.tables_config when it is falsy, so it has a meaningful database value
    # only when explicitly set false. When the user does not manage it in their model, we must
    # leave the database value untouched rather than generate a spurious reset to 'true'.
    _SKIP_IMPLICIT_RESET_PROPERTIES = {
        'enable_statistic_collect_on_first_load',
    }

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
    def skip_implicit_reset_properties(cls) -> set:
        """Property names (lowercase) that must never be implicitly reset to their default
        when absent from the metadata. See ``_SKIP_IMPLICIT_RESET_PROPERTIES``.
        """
        return cls._SKIP_IMPLICIT_RESET_PROPERTIES


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
