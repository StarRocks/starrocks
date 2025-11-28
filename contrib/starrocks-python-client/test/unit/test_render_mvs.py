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

from alembic.autogenerate.api import AutogenContext

from starrocks.alembic.ops import (
    AlterMaterializedViewOp,
    CreateMaterializedViewOp,
    DropMaterializedViewOp,
)
from starrocks.alembic.render import (
    _alter_materialized_view,
    _create_materialized_view,
    _drop_materialized_view,
)
from test.unit.test_render import _normalize_py_call


class TestMaterializedViewRendering:
    def setup_method(self, method):
        self.ctx = Mock(spec=AutogenContext)

    # ========================================================================
    # CREATE MATERIALIZED VIEW Rendering
    # ========================================================================

    def test_render_create_mv_basic(self):
        op = CreateMaterializedViewOp("mv1", "SELECT 1")
        rendered = _create_materialized_view(self.ctx, op)
        expected = "op.create_materialized_view('mv1', 'SELECT 1')"
        assert rendered == expected

    def test_render_create_mv_with_attributes(self):
        # Test each attribute independently
        op_schema = CreateMaterializedViewOp("mv1", "SELECT 1", schema="myschema")
        assert "schema='myschema'" in _create_materialized_view(self.ctx, op_schema)

        op_comment = CreateMaterializedViewOp("mv1", "SELECT 1", comment="Test MV")
        assert "comment='Test MV'" in _create_materialized_view(self.ctx, op_comment)

        op_partition = CreateMaterializedViewOp("mv1", "SELECT 1", starrocks_partition_by="dt")
        assert "partition_by='dt'" in _create_materialized_view(self.ctx, op_partition)

        op_dist = CreateMaterializedViewOp("mv1", "SELECT 1", starrocks_distributed_by="HASH(id)")
        assert "distributed_by='HASH(id)'" in _create_materialized_view(
            self.ctx, op_dist
        )

        op_order = CreateMaterializedViewOp("mv1", "SELECT 1", starrocks_order_by="id")
        assert "order_by='id'" in _create_materialized_view(self.ctx, op_order)

        op_refresh = CreateMaterializedViewOp("mv1", "SELECT 1", starrocks_refresh="ASYNC")
        assert "refresh='ASYNC'" in _create_materialized_view(
            self.ctx, op_refresh
        )

        op_props = CreateMaterializedViewOp("mv1", "SELECT 1", starrocks_properties={"k": "v"})
        assert "properties={'k': 'v'}" in _create_materialized_view(
            self.ctx, op_props
        )

    def test_render_create_mv_complex(self):
        op = CreateMaterializedViewOp(
            "mv1",
            "SELECT id, name FROM users",
            schema="myschema",
            comment="User MV",
            starrocks_partition_by="id",
            starrocks_distributed_by="HASH(id)",
            starrocks_order_by="name",
            starrocks_refresh="ASYNC",
            starrocks_properties={"replication_num": "1"},
        )
        rendered = _create_materialized_view(self.ctx, op)
        expected = """
        op.create_materialized_view('mv1', 'SELECT id, name FROM users',
            schema='myschema',
            comment='User MV',
            starrocks_partition_by='id',
            starrocks_distributed_by='HASH(id)',
            starrocks_order_by='name',
            starrocks_refresh='ASYNC',
            starrocks_properties={'replication_num': '1'}
        )
        """
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    # ========================================================================
    # DROP MATERIALIZED VIEW Rendering
    # ========================================================================

    def test_render_drop_mv_basic(self):
        op = DropMaterializedViewOp("mv1")
        rendered = _drop_materialized_view(self.ctx, op)
        assert rendered == "op.drop_materialized_view('mv1')"

    def test_render_drop_mv_with_schema(self):
        op = DropMaterializedViewOp("mv1", schema="myschema")
        rendered = _drop_materialized_view(self.ctx, op)
        expected = "op.drop_materialized_view('mv1', schema='myschema')"
        assert rendered == expected

    # ========================================================================
    # ALTER MATERIALIZED VIEW Rendering
    # ========================================================================

    def test_render_alter_mv_refresh(self):
        op = AlterMaterializedViewOp("mv1", refresh="MANUAL")
        rendered = _alter_materialized_view(self.ctx, op)
        expected = "op.alter_materialized_view('mv1', refresh='MANUAL'"
        assert expected in rendered

    def test_render_alter_mv_properties(self):
        op = AlterMaterializedViewOp("mv1", properties={"k": "v"})
        rendered = _alter_materialized_view(self.ctx, op)
        expected = "op.alter_materialized_view('mv1', properties={'k': 'v'}"
        assert expected in rendered

    def test_render_alter_mv_complex(self):
        op = AlterMaterializedViewOp(
            "mv1",
            schema="myschema",
            refresh="MANUAL",
            properties={"k": "v"},
        )
        rendered = _alter_materialized_view(self.ctx, op)
        expected = """
        op.alter_materialized_view('mv1',
            schema='myschema',
            refresh='MANUAL',
            properties={'k': 'v'}
        """
        assert _normalize_py_call(expected) in _normalize_py_call(rendered)

    # ========================================================================
    # Reverse Operations
    # ========================================================================

    def test_create_mv_reverse(self):
        create_op = CreateMaterializedViewOp("mv1", "SELECT 1", schema="s1")
        reverse_op = create_op.reverse()
        assert isinstance(reverse_op, DropMaterializedViewOp)
        assert reverse_op.view_name == "mv1"
        assert reverse_op.schema == "s1"

    def test_alter_mv_reverse(self):
        alter_op = AlterMaterializedViewOp(
            "mv1",
            schema="s1",
            refresh="MANUAL",
            properties={"k2": "v2"},
            existing_refresh="ASYNC",
            existing_properties={"k1": "v1"},
        )
        reverse_op = alter_op.reverse()
        assert isinstance(reverse_op, AlterMaterializedViewOp)
        assert reverse_op.view_name == "mv1"
        assert reverse_op.schema == "s1"
        assert reverse_op.refresh == "ASYNC"
        assert reverse_op.properties == {"k1": "v1"}
        assert reverse_op.existing_refresh == "MANUAL"
        assert reverse_op.existing_properties == {"k2": "v2"}

    def test_drop_mv_reverse(self):
        drop_op = DropMaterializedViewOp(
            "mv1",
            schema="s1",
            existing_definition="SELECT 1",
            existing_comment="Test MV",
            starrocks_partition_by="id",
            starrocks_distributed_by="HASH(id)",
            starrocks_order_by="name",
            starrocks_refresh="ASYNC",
            starrocks_properties={"k": "v"},
        )
        reverse_op = drop_op.reverse()
        assert isinstance(reverse_op, CreateMaterializedViewOp)
        assert reverse_op.view_name == "mv1"
        assert reverse_op.schema == "s1"
        assert reverse_op.definition == "SELECT 1"
        assert reverse_op.comment == "Test MV"
        assert reverse_op.kwargs["starrocks_partition_by"] == "id"
        assert reverse_op.kwargs["starrocks_distributed_by"] == "HASH(id)"
        assert reverse_op.kwargs["starrocks_order_by"] == "name"
        assert reverse_op.kwargs["starrocks_refresh"] == "ASYNC"
        assert reverse_op.kwargs["starrocks_properties"] == {"k": "v"}
