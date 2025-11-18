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

"""
Unit tests for Materialized View comparison logic.
"""

from unittest.mock import Mock

from alembic.operations import ops
from alembic.testing import eq_
import pytest
from sqlalchemy import MetaData, Table

from starrocks.alembic.compare import _compare_mv
from starrocks.alembic.ops import AlterMaterializedViewOp, CreateMaterializedViewOp, DropMaterializedViewOp
from starrocks.common.params import DialectName, TableInfoKey, TableKind, TableObjectInfoKey
from starrocks.sql.schema import MaterializedView


class TestCompareMaterializedView:
    """Test _compare_mv function directly."""

    def setup_method(self, method):
        self.mock_autogen_context = Mock()
        self.mock_autogen_context.dialect = Mock()
        self.mock_autogen_context.dialect.name = DialectName
        self.mock_autogen_context.dialect.default_schema_name = None

    def _create_reflected_mv(
        self,
        name: str,
        definition: str,
        comment: str = None,
        partition_by: str = None,
        distributed_by: str = None,
        order_by: str = None,
        refresh: str = None,
        properties: dict = None,
        schema: str = None,
    ) -> Table:
        """Helper to create a Table object simulating a reflected MV."""
        metadata = MetaData()
        # Reflected MVs are represented as Table objects. MV is also OK.
        # table = MaterializedView(name, metadata, schema=schema, definition=definition)
        table = Table(name, metadata, schema=schema)
        table.info[TableObjectInfoKey.DEFINITION] = definition
        table.info[TableObjectInfoKey.TABLE_KIND] = TableKind.MATERIALIZED_VIEW

        if comment:
            table.comment = comment

        dialect_opts = table.dialect_options.setdefault(DialectName, {})
        if partition_by:
            dialect_opts[TableInfoKey.PARTITION_BY] = partition_by
        if distributed_by:
            dialect_opts[TableInfoKey.DISTRIBUTED_BY] = distributed_by
        if order_by:
            dialect_opts[TableInfoKey.ORDER_BY] = order_by
        if refresh:
            dialect_opts[TableInfoKey.REFRESH] = refresh
        if properties:
            dialect_opts[TableInfoKey.PROPERTIES] = properties

        return table

    # ========================================================================
    # ALTER MV - Mutable Attributes (Refresh & Properties)
    # ========================================================================

    def test_alter_mv_refresh_changed(self):
        """ALTER: Refresh strategy changed (generates AlterMaterializedViewOp)."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", refresh="ASYNC")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_refresh="MANUAL")

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(len(upgrade_ops.ops), 1)
        op = upgrade_ops.ops[0]
        assert isinstance(op, AlterMaterializedViewOp)
        eq_(op.view_name, "my_mv")
        eq_(op.refresh, "MANUAL")
        eq_(op.existing_refresh, "ASYNC")
        # Other attributes should be None as they are not changed
        assert op.properties is None
        assert op.existing_properties is None

    def test_alter_mv_properties_changed(self):
        """ALTER: Properties changed (generates AlterMaterializedViewOp)."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv(
            "my_mv", definition="SELECT 1", properties={"replication_num": "1"}
        )
        meta_mv = MaterializedView(
            "my_mv", MetaData(), definition="SELECT 1", starrocks_properties={"replication_num": "3"}
        )

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(len(upgrade_ops.ops), 1)
        op = upgrade_ops.ops[0]
        assert isinstance(op, AlterMaterializedViewOp)
        eq_(op.properties, {"replication_num": "3"})
        eq_(op.existing_properties, {"replication_num": "1"})
        assert op.refresh is None

    # ========================================================================
    # DROP/CREATE MV - Immutable Attributes
    # ========================================================================

    def test_modify_mv_definition_changed(self):
        """DROP/CREATE: Definition changed."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 2")

        with pytest.raises(NotImplementedError, match="does not support altering MV definition"):
            _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

    def test_modify_mv_partition_by_changed(self):
        """DROP/CREATE: Partition by changed."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", partition_by="date_trunc('day', dt)")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_partition_by="dt")

        with pytest.raises(NotImplementedError, match="does not support 'ALTER MATERIALIZED VIEW PARTITION BY'"):
            _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

    def test_modify_mv_distributed_by_changed(self):
        """DROP/CREATE: Distributed by changed."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", distributed_by="HASH(id)")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_distributed_by="RANDOM")

        with pytest.raises(NotImplementedError, match="does not support 'ALTER MATERIALIZED VIEW DISTRIBUTED BY'"):
            _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

    def test_modify_mv_order_by_changed(self):
        """DROP/CREATE: Order by changed."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv("my_mv", definition="SELECT 1", order_by="id")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="SELECT 1", starrocks_order_by="name")

        with pytest.raises(NotImplementedError, match="does not support 'ALTER MATERIALIZED VIEW ORDER BY'"):
            _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

    # ========================================================================
    # No-Change Tests
    # ========================================================================

    def test_no_change_mv(self):
        """No change: Identical MVs."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv(
            name="my_mv",
            definition="SELECT id, name FROM users",
            comment="Test comment",
            partition_by="id",
            distributed_by="HASH(id)",
            order_by="name",
            refresh="ASYNC",
            properties={"replication_num": "1"},
        )
        meta_mv = MaterializedView(
            "my_mv",
            MetaData(),
            definition="SELECT id, name FROM users",
            comment="Test comment",
            starrocks_partition_by="id",
            starrocks_distributed_by="HASH(id)",
            starrocks_order_by="name",
            starrocks_refresh="ASYNC",
            starrocks_properties={"replication_num": "1"},
        )

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(len(upgrade_ops.ops), 0)

    def test_no_change_mv_sql_normalization(self):
        """No change: SQL definition differs only by formatting."""
        upgrade_ops = ops.UpgradeOps([])
        conn_mv = self._create_reflected_mv(name="my_mv", definition="SELECT 1 AS `val`")
        meta_mv = MaterializedView("my_mv", MetaData(), definition="select 1 as val")

        _compare_mv(self.mock_autogen_context, upgrade_ops, None, "my_mv", conn_mv, meta_mv)

        eq_(len(upgrade_ops.ops), 0)
