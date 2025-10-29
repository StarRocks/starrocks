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

from unittest.mock import Mock

from alembic.operations import ops
from alembic.testing import eq_
import pytest
from sqlalchemy import MetaData

from starrocks.alembic.compare import autogen_for_materialized_views
from starrocks.alembic.ops import CreateMaterializedViewOp, DropMaterializedViewOp
from starrocks.sql.schema import MaterializedView


@pytest.skip(reason="Skipping MV autogenerate test for now", allow_module_level=True)
class TestAutogenerateMV:
    def setup_method(self, method):
        self.mock_inspector = Mock()
        self.mock_autogen_context = Mock()
        self.mock_autogen_context.inspector = self.mock_inspector
        self.mock_autogen_context.opts = {
            'include_object': None,
            'include_name': None,
        }
        self.mock_autogen_context.dialect = Mock()
        self.mock_autogen_context.dialect.name = 'starrocks'
        self.mock_autogen_context.dialect.default_schema_name = None

    def test_create_mv_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = []
        m2 = MetaData()
        mv = MaterializedView('my_test_mv', 'SELECT 1', m2)
        m2.info['materialized_views'] = {(mv.schema, mv.name): mv}
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op: CreateMaterializedViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'CreateMaterializedViewOp')
        eq_(op.view_name, 'my_test_mv')
        eq_(op.definition, 'SELECT 1')

    def test_drop_mv_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = ['my_test_mv']
        m2 = MetaData()
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op: DropMaterializedViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'DropMaterializedViewOp')
        eq_(op.view_name, 'my_test_mv')

    def test_modify_mv_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = ['my_test_mv']
        self.mock_inspector.get_materialized_view_definition.return_value = 'SELECT 1'
        m2 = MetaData()
        mv2 = MaterializedView('my_test_mv', 'SELECT 2', m2)
        m2.info['materialized_views'] = {(mv2.schema, mv2.name): mv2}
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op_tuple = upgrade_ops.ops[0]
        eq_(len(op_tuple), 2)
        drop_op, create_op = op_tuple
        eq_(drop_op.__class__.__name__, 'DropMaterializedViewOp')
        eq_(create_op.__class__.__name__, 'CreateMaterializedViewOp')
        eq_(create_op.view_name, 'my_test_mv')
        eq_(create_op.definition, 'SELECT 2')

    def test_create_mv_with_properties_autogenerate(self):
        """Test creating MV with properties."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = []
        m2 = MetaData()
        mv = MaterializedView(
            'my_test_mv',
            'SELECT id, count(*) FROM users GROUP BY id',
            properties={'replication_num': '1', 'storage_medium': 'SSD'}
        )
        m2.info['materialized_views'] = {(mv.schema, mv.name): mv}
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op: CreateMaterializedViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'CreateMaterializedViewOp')
        eq_(op.view_name, 'my_test_mv')
        eq_(op.definition, 'SELECT id, count(*) FROM users GROUP BY id')
        eq_(op.properties, {'replication_num': '1', 'storage_medium': 'SSD'})

    def test_create_mv_with_schema_autogenerate(self):
        """Test creating MV with schema."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = []
        m2 = MetaData()
        mv = MaterializedView('my_test_mv', 'SELECT 1', schema='test_schema')
        m2.info['materialized_views'] = {(mv.schema, mv.name): mv}
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op: CreateMaterializedViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'CreateMaterializedViewOp')
        eq_(op.view_name, 'my_test_mv')
        eq_(op.schema, 'test_schema')

    def test_drop_mv_with_schema_autogenerate(self):
        """Test dropping MV with schema."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = ['my_test_mv']
        m2 = MetaData()
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, ['test_schema'])
        eq_(len(upgrade_ops.ops), 1)
        op: DropMaterializedViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'DropMaterializedViewOp')
        eq_(op.view_name, 'my_test_mv')
        eq_(op.schema, 'test_schema')

    def test_no_change_mv_autogenerate(self):
        """Test that no changes are detected when MV definition matches."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = ['my_test_mv']
        self.mock_inspector.get_materialized_view_definition.return_value = 'SELECT id, count(*) FROM users GROUP BY id'
        m2 = MetaData()
        mv = MaterializedView('my_test_mv', 'SELECT id, count(*) FROM users GROUP BY id')
        m2.info['materialized_views'] = {(mv.schema, mv.name): mv}
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 0)

    def test_modify_mv_properties_autogenerate(self):
        """Test modifying MV properties."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = ['my_test_mv']
        self.mock_inspector.get_materialized_view_definition.return_value = 'SELECT 1'
        m2 = MetaData()
        mv2 = MaterializedView(
            'my_test_mv',
            'SELECT 1',
            properties={'replication_num': '3', 'storage_medium': 'SSD'}
        )
        m2.info['materialized_views'] = {(mv2.schema, mv2.name): mv2}
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op_tuple = upgrade_ops.ops[0]
        eq_(len(op_tuple), 2)
        drop_op, create_op = op_tuple
        eq_(drop_op.__class__.__name__, 'DropMaterializedViewOp')
        eq_(create_op.__class__.__name__, 'CreateMaterializedViewOp')
        eq_(create_op.properties, {'replication_num': '3', 'storage_medium': 'SSD'})

    def test_create_multiple_mvs_autogenerate(self):
        """Test creating multiple MVs in one operation."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = []
        m2 = MetaData()
        mv1 = MaterializedView('mv1', 'SELECT 1')
        mv2 = MaterializedView('mv2', 'SELECT 2')
        m2.info['materialized_views'] = {
            (mv1.schema, mv1.name): mv1,
            (mv2.schema, mv2.name): mv2
        }
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 2)
        op1: CreateMaterializedViewOp = upgrade_ops.ops[0]
        op2: CreateMaterializedViewOp = upgrade_ops.ops[1]
        eq_(op1.__class__.__name__, 'CreateMaterializedViewOp')
        eq_(op2.__class__.__name__, 'CreateMaterializedViewOp')
        # Check that both MVs are created
        view_names = {op1.view_name, op2.view_name}
        eq_(view_names, {'mv1', 'mv2'})

    def test_drop_multiple_mvs_autogenerate(self):
        """Test dropping multiple MVs in one operation."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = ['mv1', 'mv2']
        m2 = MetaData()
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 2)
        op1: DropMaterializedViewOp = upgrade_ops.ops[0]
        op2: DropMaterializedViewOp = upgrade_ops.ops[1]
        eq_(op1.__class__.__name__, 'DropMaterializedViewOp')
        eq_(op2.__class__.__name__, 'DropMaterializedViewOp')
        # Check that both MVs are dropped
        view_names = {op1.view_name, op2.view_name}
        eq_(view_names, {'mv1', 'mv2'})

    def test_mixed_operations_autogenerate(self):
        """Test mixed create, modify, and drop operations."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = ['mv_to_modify', 'mv_to_drop']
        self.mock_inspector.get_materialized_view_definition.return_value = 'SELECT old'
        m2 = MetaData()
        mv_new = MaterializedView('mv_new', 'SELECT new')
        mv_modified = MaterializedView('mv_to_modify', 'SELECT modified')
        m2.info['materialized_views'] = {
            (mv_new.schema, mv_new.name): mv_new,
            (mv_modified.schema, mv_modified.name): mv_modified
        }
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 3)

        # Should have: 1 create, 1 modify (drop+create), 1 drop
        op_types = [op.__class__.__name__ for op in upgrade_ops.ops]
        eq_(op_types.count('CreateMaterializedViewOp'), 2)  # new + modified
        eq_(op_types.count('DropMaterializedViewOp'), 2)    # modified + to_drop
