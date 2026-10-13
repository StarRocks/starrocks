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

"""Unit tests for materialized view PROPERTIES extraction during reflection.

information_schema.tables_config does not report every property for an MV (notably
colocate_with), so those properties are read from the CREATE MATERIALIZED VIEW ddl
instead. See ReflectionMVDefaults._DDL_ONLY_PROPERTY_KEYS.
"""

import json
from types import SimpleNamespace

import pytest

from starrocks.common.consts import TableConfigKey
from starrocks.common.params import TableInfoKeyWithPrefix
from starrocks.reflection import StarRocksTableDefinitionParser

MV_DDL = (
    "CREATE MATERIALIZED VIEW `m` (`id`, `sv`)\n"
    "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
    "REFRESH ASYNC\n"
    "PROPERTIES (\n"
    '"replicated_storage" = "true",\n'
    '"replication_num" = "1",\n'
    '"storage_medium" = "HDD",\n'
    '"colocate_with" = "cg_test"\n'
    ")\n"
    "AS SELECT id, sum(v) AS sv FROM t GROUP BY id"
)


def _parser() -> StarRocksTableDefinitionParser:
    return StarRocksTableDefinitionParser.__new__(StarRocksTableDefinitionParser)


def _ddl_properties(ddl: str = MV_DDL) -> dict:
    state = _parser()._parse_mv_ddl("m", ddl, schema="db")
    return dict(state.table_options.get(TableInfoKeyWithPrefix.PROPERTIES) or {})


def test_colocate_with_taken_from_ddl():
    """tables_config omits colocate_with for MVs, so it must come from the ddl."""
    assert _ddl_properties()["colocate_with"] == "cg_test"


@pytest.mark.parametrize("prop", ["replicated_storage", "replication_num", "storage_medium"])
def test_non_allowlisted_ddl_properties_ignored(prop):
    """Only allowlisted properties are taken from the ddl. SHOW CREATE also prints
    effective values that tables_config leaves out on purpose; merging those in would
    make autogenerate warn about unmanaged database values on every run."""
    assert prop not in _ddl_properties()


def test_mv_without_colocation_has_no_ddl_properties():
    ddl = MV_DDL.replace(',\n"colocate_with" = "cg_test"', "")
    assert _ddl_properties(ddl) == {}


def _parse_mv(config_properties: dict) -> dict:
    """Run full MV parsing with a synthetic tables_config row and return the properties."""
    mv_row = SimpleNamespace(
        TABLE_NAME="m",
        TABLE_SCHEMA="db",
        MATERIALIZED_VIEW_DEFINITION=MV_DDL,
    )
    config_row = {TableConfigKey.PROPERTIES: json.dumps(config_properties)}
    state = _parser().parse_mv(mv_row, table_row=None, config_row=config_row)
    return dict(state.table_options.get(TableInfoKeyWithPrefix.PROPERTIES) or {})


def test_ddl_properties_merged_with_tables_config():
    """The ddl-only property survives, and tables_config supplies the rest."""
    properties = _parse_mv({"mv_rewrite_staleness_second": "0", "replication_num": "1"})
    assert properties == {
        "colocate_with": "cg_test",
        "mv_rewrite_staleness_second": "0",
        "replication_num": "1",
    }


def test_tables_config_wins_on_conflict():
    """tables_config stays authoritative wherever both sources report a property."""
    properties = _parse_mv({"colocate_with": "cg_from_config"})
    assert properties["colocate_with"] == "cg_from_config"
