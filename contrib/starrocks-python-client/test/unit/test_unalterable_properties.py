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

"""Autogenerate leaves out, with a warning, the table and MV property changes StarRocks refuses to SET."""

from unittest.mock import Mock, patch

from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps
from sqlalchemy import Column, Integer, MetaData, Table, testing
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_

from starrocks.alembic.compare import _compare_mv_properties, compare_starrocks_table
from starrocks.common.params import DialectName, TableInfoKey
from starrocks.common.types import SystemRunMode


def _property_ops(key, database_value, model_value, server_version, run_mode=SystemRunMode.SHARED_NOTHING):
    autogen_context = Mock(spec=AutogenContext)
    autogen_context.dialect = Mock()
    autogen_context.dialect.name = DialectName
    autogen_context.dialect.server_version_info = server_version
    autogen_context.dialect.run_mode = run_mode
    tables = [
        Table(
            "t",
            MetaData(),
            Column("id", Integer),
            starrocks_distributed_by="HASH(id) BUCKETS 1",
            starrocks_properties={"write_quorum": "MAJORITY", key: value},
        )
        for value in (database_value, model_value)
    ]
    upgrade_ops = UpgradeOps([])
    compare_starrocks_table(autogen_context, upgrade_ops, None, "t", *tables)
    return [op.properties for op in upgrade_ops.ops]


class UnalterablePropertiesTest(fixtures.TestBase):
    @testing.combinations(
        ("compression", "LZ4", "ZSTD"),
        ("fast_schema_evolution", "true", "false"),
        ("storage_type", "column", "column_with_row"),
        ("storage_format", "DEFAULT", "V2"),
        ("storage_cooldown_time", "2099-12-30 00:00:00", "2099-12-31 00:00:00"),
        ("enable_async_write_back", "false", "true"),
        argnames="key, database_value, model_value",
    )
    def test_a_property_set_only_at_create_is_skipped(self, key, database_value, model_value, caplog):
        eq_(_property_ops(key, database_value, model_value, (4, 1, 6)), [])
        assert f"would set property {key!r} to {model_value!r}, where the database has" in caplog.text
        assert "StarRocks sets it only when the table is created" in caplog.text

    @testing.combinations(
        (SystemRunMode.SHARED_NOTHING, []),
        (SystemRunMode.SHARED_DATA, [{"compaction_strategy": "REAL_TIME"}]),
        argnames="run_mode, expected",
    )
    def test_compaction_strategy_is_set_only_on_shared_data(self, run_mode, expected):
        eq_(_property_ops("compaction_strategy", "DEFAULT", "REAL_TIME", (4, 1, 6), run_mode), expected)

    @testing.combinations(
        ((3, 3, 22), False),
        ((3, 5, 9), False),
        ((3, 5, 10), True),
        ((4, 0, 1), False),
        ((4, 0, 2), True),
        ((4, 1, 6), True),
        (None, True),
        argnames="server_version, alterable",
    )
    def test_statistic_collection_on_first_load_is_set_from_3_5_10_and_4_0_2(self, server_version, alterable):
        ops = _property_ops("enable_statistic_collect_on_first_load", "true", "false", server_version)
        eq_(ops, [{"enable_statistic_collect_on_first_load": "false"}] if alterable else [])

    @testing.combinations(((3, 5, 21), False), ((4, 0, 0), True), argnames="server_version, alterable")
    def test_flat_json_is_set_from_4_0(self, server_version, alterable):
        eq_(_property_ops("flat_json.enable", "false", "true", server_version), [{"flat_json.enable": "true"}] if alterable else [])

    def test_the_other_properties_of_the_change_are_still_set(self):
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect = Mock(server_version_info=(3, 3, 22), run_mode=SystemRunMode.SHARED_NOTHING)
        autogen_context.dialect.name = DialectName
        database, model = (
            Table("t", MetaData(), Column("id", Integer), starrocks_properties=properties)
            for properties in (
                {"compression": "LZ4", "write_quorum": "MAJORITY"},
                {"compression": "ZSTD", "write_quorum": "ALL"},
            )
        )
        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(autogen_context, upgrade_ops, None, "t", database, model)
        eq_([(op.properties, op.existing_properties) for op in upgrade_ops.ops], [({"write_quorum": "ALL"}, {"write_quorum": "MAJORITY"})])


def _mv_property_ops(database_properties, model_properties):
    ops_list = []
    _compare_mv_properties(
        ops_list,
        None,
        "mv",
        {TableInfoKey.PROPERTIES: database_properties},
        {TableInfoKey.PROPERTIES: model_properties},
        SystemRunMode.SHARED_NOTHING,
    )
    return [(op.properties, op.existing_properties) for op in ops_list]


class UnalterableMaterializedViewPropertiesTest(fixtures.TestBase):
    @testing.combinations(
        ("replication_num", "1", "3"),
        ("storage_medium", "HDD", "SSD"),
        ("colocate_with", "g1", "g2"),
        ("compression", "LZ4", "ZSTD"),
        argnames="key, database_value, model_value",
    )
    def test_a_property_alter_materialized_view_cannot_change_is_skipped(
        self, key, database_value, model_value, caplog
    ):
        eq_(_mv_property_ops({key: database_value}, {key: model_value}), [])
        assert f"Materialized view 'mv': the migration would set property {key!r} to {model_value!r}" in caplog.text
        assert "ALTER MATERIALIZED VIEW cannot change it" in caplog.text

    def test_a_property_it_can_change_is_still_set(self):
        eq_(
            _mv_property_ops(
                {"replication_num": "1", "mv_rewrite_staleness_second": "0"},
                {"replication_num": "3", "mv_rewrite_staleness_second": "60"},
            ),
            [({"mv_rewrite_staleness_second": "60"}, {"mv_rewrite_staleness_second": "0"})],
        )

    def test_a_reset_of_an_omitted_property_is_skipped_too(self, caplog):
        """A comparison that resets an omitted property to its default, as the base one did, is caught the same way."""
        reset = (
            {"replication_num": "3", "mv_rewrite_staleness_second": "0"},
            {"replication_num": "1", "mv_rewrite_staleness_second": "60"},
        )
        with patch("starrocks.alembic.compare._compare_table_properties_impl", return_value=reset):
            eq_(
                _mv_property_ops({"replication_num": "1", "mv_rewrite_staleness_second": "60"}, {}),
                [({"mv_rewrite_staleness_second": "0"}, {"mv_rewrite_staleness_second": "60"})],
            )
        assert "would set property 'replication_num' to '3', where the database has '1'" in caplog.text
