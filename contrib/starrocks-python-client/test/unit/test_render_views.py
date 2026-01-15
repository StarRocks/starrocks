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

import logging
from unittest.mock import Mock

from alembic.autogenerate.api import AutogenContext

from starrocks.alembic.ops import (
    AlterViewOp,
    CreateViewOp,
    DropViewOp,
)
from starrocks.alembic.render import (
    _alter_view,
    _create_view,
    _drop_view,
)
from test.unit.test_render import _normalize_py_call


logger = logging.getLogger(__name__)


class TestViewRendering:
    def setup_method(self, method):
        self.ctx = Mock(spec=AutogenContext)

    def test_render_create_view_basic(self):
        """Simple: Basic CREATE VIEW rendering."""
        op = CreateViewOp("v1", "SELECT 1", schema=None, comment=None, starrocks_security=None)
        rendered = _create_view(self.ctx, op)
        expected = "op.create_view('v1', 'SELECT 1')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_create_view_with_schema(self):
        """Coverage: CREATE VIEW with schema attribute."""
        op = CreateViewOp("v1", "SELECT 1", schema="myschema")
        rendered = _create_view(self.ctx, op)
        expected = "op.create_view('v1', 'SELECT 1', schema='myschema')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_create_view_with_comment(self):
        """Coverage: CREATE VIEW with comment attribute."""
        op = CreateViewOp("v1", "SELECT 1", comment="Test view")
        rendered = _create_view(self.ctx, op)
        expected = "op.create_view('v1', 'SELECT 1', comment='Test view')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_create_view_with_security(self):
        """Coverage: CREATE VIEW with security attribute (DEFINER/INVOKER)."""
        # Test INVOKER
        op = CreateViewOp("v1", "SELECT 1", starrocks_security="INVOKER")
        rendered = _create_view(self.ctx, op)
        expected = "op.create_view('v1', 'SELECT 1', starrocks_security='INVOKER')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

        # Test DEFINER
        op2 = CreateViewOp("v2", "SELECT 2", starrocks_security="DEFINER")
        rendered2 = _create_view(self.ctx, op2)
        expected2 = "op.create_view('v2', 'SELECT 2', starrocks_security='DEFINER')"
        assert _normalize_py_call(rendered2) == _normalize_py_call(expected2)

    def test_render_create_view_with_columns(self):
        """Coverage: CREATE VIEW with columns attribute."""
        op = CreateViewOp(
            "v1",
            "SELECT id, name FROM users",
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'},
                {'name': 'email', 'comment': 'Email address'}
            ]
        )
        rendered = _create_view(self.ctx, op)
        expected = """
        op.create_view('v1', 'SELECT id, name FROM users', columns=[
            {'name': 'id'},
            {'name': 'name', 'comment': 'User name'},
            {'name': 'email', 'comment': 'Email address'}
        ])
        """
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_create_view_complex(self):
        """Complex: CREATE VIEW with all attributes combined."""
        op = CreateViewOp(
            "v1",
            "SELECT id, name FROM users",
            schema="myschema",
            comment="User view",
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'}
            ],
            starrocks_security="INVOKER",
        )
        rendered = _create_view(self.ctx, op)
        expected = """
        op.create_view('v1', 'SELECT id, name FROM users',
            schema='myschema',
            comment='User view',
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'}
            ],
            starrocks_security='INVOKER'
            )
        """
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_drop_view_basic(self):
        """Simple: Basic DROP VIEW rendering."""
        op = DropViewOp("v1", schema=None, if_exists=False)
        rendered = _drop_view(self.ctx, op)
        assert rendered == "op.drop_view('v1')"

    def test_render_drop_view_with_schema(self):
        """Coverage: DROP VIEW with schema attribute."""
        op = DropViewOp("v1", schema="myschema", if_exists=False)
        rendered = _drop_view(self.ctx, op)
        expected = "op.drop_view('v1', schema='myschema')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_drop_view_with_if_exists(self):
        """Coverage: DROP VIEW with if_exists attribute."""
        op = DropViewOp("v1", schema=None, if_exists=True)
        rendered = _drop_view(self.ctx, op)
        expected = "op.drop_view('v1', if_exists=True)"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_drop_view_complex(self):
        """Complex: DROP VIEW with all attributes combined."""
        op = DropViewOp("v1", schema="myschema", if_exists=True)
        rendered = _drop_view(self.ctx, op)
        expected = "op.drop_view('v1', schema='myschema', if_exists=True)"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_alter_view_basic(self):
        """Simple: Basic ALTER VIEW rendering (definition only)."""
        op = AlterViewOp("v1", "SELECT 2", schema=None, comment=None, security=None)
        rendered = _alter_view(self.ctx, op)
        expected = "op.alter_view('v1', 'SELECT 2')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_alter_view_with_comment(self):
        """Coverage: ALTER VIEW with comment attribute."""
        op = AlterViewOp("v1", definition=None, comment="Modified comment")
        rendered = _alter_view(self.ctx, op)
        expected = "op.alter_view('v1', comment='Modified comment'"
        assert _normalize_py_call(expected) in _normalize_py_call(rendered)

    def test_render_alter_view_with_security(self):
        """Coverage: ALTER VIEW with security attribute."""
        op = AlterViewOp("v1", definition=None, security="DEFINER")
        rendered = _alter_view(self.ctx, op)
        expected = "op.alter_view('v1', security='DEFINER'"
        assert _normalize_py_call(expected) in _normalize_py_call(rendered)

    def test_render_alter_view_partial_attrs(self):
        """Coverage: ALTER VIEW with multiple but not all attributes (key scenario)."""
        op = AlterViewOp(
            "v1",
            definition=None,  # Definition is NOT changed
            schema="myschema",
            comment="Modified comment",
            security=None  # Security is NOT changed
        )
        rendered = _alter_view(self.ctx, op)
        expected = "op.alter_view('v1', schema='myschema', comment='Modified comment'"
        assert _normalize_py_call(expected) in _normalize_py_call(rendered)

    def test_render_alter_view_complex(self):
        """Complex: ALTER VIEW with all attributes combined."""
        op = AlterViewOp(
            "v1",
            "SELECT id, name FROM users",
            schema="myschema",
            comment="Modified view",
            security="DEFINER"
        )
        rendered = _alter_view(self.ctx, op)
        expected = """
        op.alter_view('v1', 'SELECT id, name FROM users',
            schema='myschema',
            comment='Modified view',
            security='DEFINER'
        """
        assert _normalize_py_call(expected) in _normalize_py_call(rendered)

    def test_render_view_with_special_chars(self):
        """Complex: Rendering view with special characters in definition and schema."""
        op = CreateViewOp(
            "v_complex",
            "SELECT `user-id`, 'some_string', \"another_string\\n\" FROM `my_table`",
            schema="s'1"
        )
        rendered = _create_view(self.ctx, op)
        logger.debug(f"rendered={rendered}")
        expected_list = [
            f"op.create_view('v_complex', ",
            f"{repr(op.definition)}, ",
            f"schema={repr(op.schema)})"
        ]
        expected = "".join(expected_list)
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_view_with_single_quotes(self):
        """Coverage: View definition with single quotes."""
        op = CreateViewOp(
            "view_quotes",
            "SELECT id, 'O''Brien' AS name FROM users"
        )
        rendered = _create_view(self.ctx, op)
        expected = f"op.create_view('view_quotes', {repr(op.definition)})"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_view_with_backslashes(self):
        """Coverage: View definition with backslashes."""
        op = CreateViewOp(
            "view_backslash",
            r"SELECT id, 'C:\\Users\\path' AS file_path FROM users"
        )
        rendered = _create_view(self.ctx, op)
        expected = f"op.create_view('view_backslash', {repr(op.definition)})"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_view_multiline_definition(self):
        """Coverage: View definition with newlines (multi-line SQL)."""
        op = CreateViewOp(
            "view_multiline",
            """SELECT
    id,
    name
FROM users
WHERE id > 0"""
        )
        rendered = _create_view(self.ctx, op)
        expected = f"op.create_view('view_multiline', {repr(op.definition)})"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_view_with_mixed_special_chars(self):
        """Complex: View with mixed special characters (quotes, backslashes, newlines)."""
        op = CreateViewOp(
            "view_mixed",
            """SELECT
    id,
    name AS "User's Name",
    'Status: "Active"' AS status,
    'Path: C:\\\\temp' AS temp_path
FROM users"""
        )
        rendered = _create_view(self.ctx, op)
        expected = f"op.create_view('view_mixed', {repr(op.definition)})"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_create_view_reverse(self):
        """Reverse: CreateViewOp reverses to DropViewOp."""
        create_op = CreateViewOp("v1", "SELECT 1", schema="s1")
        reverse_op = create_op.reverse()
        assert isinstance(reverse_op, DropViewOp)
        assert reverse_op.view_name == "v1"
        assert reverse_op.schema == "s1"

    def test_alter_view_reverse(self):
        """Reverse: AlterViewOp reverses to another AlterViewOp with swapped attributes."""
        alter_op = AlterViewOp(
            "v1",
            "SELECT 2",
            schema="s1",
            comment="New Comment",
            security="DEFINER",
            existing_definition="SELECT 1",
            existing_comment="Old Comment",
            existing_security="INVOKER",
        )
        reverse_op = alter_op.reverse()
        assert isinstance(reverse_op, AlterViewOp)
        assert reverse_op.view_name == "v1"
        assert reverse_op.schema == "s1"
        # The new attributes of the reverse op should be the old attributes of the original op
        assert reverse_op.definition == "SELECT 1"
        assert reverse_op.comment == "Old Comment"
        assert reverse_op.security == "INVOKER"
        # And the reverse attributes of the reverse op should be the new attributes of the original op
        assert reverse_op.existing_definition == "SELECT 2"
        assert reverse_op.existing_comment == "New Comment"
        assert reverse_op.existing_security == "DEFINER"

    def test_drop_view_reverse(self):
        """Reverse: DropViewOp reverses to CreateViewOp (with and without columns)."""
        # Test without columns
        drop_op = DropViewOp(
            "v1",
            schema=None,
            existing_definition="SELECT 1",
            existing_comment="Test view",
            starrocks_security="INVOKER"
        )
        reverse_op = drop_op.reverse()
        assert isinstance(reverse_op, CreateViewOp)
        assert reverse_op.view_name == "v1"
        assert reverse_op.definition == "SELECT 1"
        assert reverse_op.comment == "Test view"
        assert reverse_op.kwargs['starrocks_security'] == "INVOKER"

        # Test with columns
        drop_op_with_cols = DropViewOp(
            "v2",
            schema=None,
            existing_definition="SELECT id, name FROM users",
            existing_comment="User view",
            starrocks_security="INVOKER",
            existing_columns=[
                {'name': 'id', 'comment': None},
                {'name': 'name', 'comment': 'User name'}
            ]
        )
        reverse_op_with_cols = drop_op_with_cols.reverse()
        assert isinstance(reverse_op_with_cols, CreateViewOp)
        assert reverse_op_with_cols.view_name == "v2"
        assert reverse_op_with_cols.definition == "SELECT id, name FROM users"
        assert reverse_op_with_cols.columns is not None
        assert len(reverse_op_with_cols.columns) == 2
        assert reverse_op_with_cols.columns[0]['name'] == 'id'
        assert reverse_op_with_cols.columns[1]['name'] == 'name'
        assert reverse_op_with_cols.columns[1]['comment'] == 'User name'
        assert reverse_op_with_cols.kwargs['starrocks_security'] == "INVOKER"
