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

from sqlalchemy import testing
from sqlalchemy.testing import fixtures

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


class ReflectionMVPropertiesTest(fixtures.TestBase):
    def test_colocate_with_taken_from_ddl(self):
        """tables_config omits colocate_with for MVs, so it must come from the ddl."""
        assert _ddl_properties()["colocate_with"] == "cg_test"

    @testing.combinations("replicated_storage", "replication_num", "storage_medium", argnames="prop")
    def test_non_allowlisted_ddl_properties_ignored(self, prop):
        """Only allowlisted properties are taken from the ddl. SHOW CREATE also prints
        effective values that tables_config leaves out on purpose; merging those in would
        make autogenerate warn about unmanaged database values on every run."""
        assert prop not in _ddl_properties()

    @testing.combinations(
        ("session.query_timeout", "900"),
        ("session.enable_spill", "true"),
        ("query_rewrite_consistency", "loose"),
        ("enable_query_rewrite", "false"),
        ("force_external_table_query_rewrite", "true"),
        ("partition_retention_condition", "dt >= current_date() - interval 3 month"),
        ("excluded_refresh_tables", "db.u"),
        ("resource_group", "wg1"),
        ("unique_constraints", "default_catalog.db.t.id,dt;default_catalog.db.u.id"),
        ("foreign_key_constraints", "default_catalog.db.t(id) REFERENCES default_catalog.db.u(id)"),
        argnames="prop,value",
    )
    def test_properties_tables_config_leaves_out_are_taken_from_ddl(self, prop, value):
        ddl = MV_DDL.replace('"colocate_with" = "cg_test"', f'"{prop}" = "{value}"')
        assert _ddl_properties(ddl) == {prop: value}

    @testing.combinations(
        ("single_quotes", "dt >= '2020-01-01'", "dt >= '2020-01-01'"),
        ("double_quotes_as_printed", 'dt >= "2020-01-01"', 'dt >= "2020-01-01"'),
        ("double_quotes_escaped", 'dt >= \\"2020-01-01\\"', 'dt >= "2020-01-01"'),
        ("comma_and_quote", 'dt IN ("a", "b")', 'dt IN ("a", "b")'),
        argnames="printed, value",
        id_="iaa",
    )
    def test_a_value_holding_quotes(self, printed, value):
        """StarRocks 4.1.6 prints a property value's ``"`` as it is, and the value ends before the next key."""
        ddl = MV_DDL.replace(
            '"colocate_with" = "cg_test"', f'"partition_retention_condition" = "{printed}",\n"session.x" = "1"'
        )
        assert _ddl_properties(ddl) == {"partition_retention_condition": value, "session.x": "1"}

    @testing.combinations(
        ("one_literal", "city <> 'zz'"),
        ("two_literals", "city <> 'zz' AND k <> 'y'"),
        ("double_quoted_literal", 'city <> "zz"'),
        argnames="where",
        id_="ia",
    )
    def test_a_property_value_with_an_unpaired_quote_leaves_the_select_whole(self, where):
        """4.1.6 prints ``city != "O'Brien"`` raw, and its ``'`` must not pair with a quote of the SELECT."""
        ddl = MV_DDL.replace(
            '"colocate_with" = "cg_test"',
            '"partition_retention_condition" = "city != "O\'Brien"",\n"session.x" = "1"',
        ).replace(
            "AS SELECT id, sum(v) AS sv FROM t GROUP BY id", f"AS SELECT id, sum(v) AS sv FROM t WHERE {where} GROUP BY id;"
        )
        state = _parser()._parse_mv_ddl("m", ddl, schema="db")
        assert state.definition == f"SELECT id, sum(v) AS sv FROM t WHERE {where} GROUP BY id"
        assert dict(state.table_options[TableInfoKeyWithPrefix.PROPERTIES]) == {
            "partition_retention_condition": "city != \"O'Brien\"",
            "session.x": "1",
        }

    def test_mv_without_colocation_has_no_ddl_properties(self):
        ddl = MV_DDL.replace(',\n"colocate_with" = "cg_test"', "")
        assert _ddl_properties(ddl) == {}

    def test_ddl_properties_merged_with_tables_config(self):
        """The ddl-only property survives, and tables_config supplies the rest."""
        properties = _parse_mv({"mv_rewrite_staleness_second": "0", "replication_num": "1"})
        assert properties == {
            "colocate_with": "cg_test",
            "mv_rewrite_staleness_second": "0",
            "replication_num": "1",
        }

    def test_tables_config_wins_on_conflict(self):
        """tables_config stays authoritative wherever both sources report a property."""
        properties = _parse_mv({"colocate_with": "cg_from_config"})
        assert properties["colocate_with"] == "cg_from_config"
