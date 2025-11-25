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

"""Unit tests for View schema definition and column handling."""

import pytest
from sqlalchemy import Column, MetaData

from starrocks.alembic.ops import AlterViewOp, CreateViewOp, DropViewOp
from starrocks.datatype import STRING
from starrocks.sql.schema import View


class TestViewColumnDefinitions:
    """Test different ways to define columns in View."""

    def test_view_with_column_objects(self):
        """Test View with Column objects via *args."""
        metadata = MetaData()
        view = View(
            'v1',
            metadata,
            Column('id', STRING()),
            Column('name', STRING(), comment='User name'),
            definition='SELECT id, name FROM users',
            comment='User view'
        )

        assert view.name == 'v1'
        assert view.comment == 'User view'
        assert view.definition == 'SELECT id, name FROM users'
        assert len(view.columns) == 2
        assert view.columns['id'].name == 'id'
        assert view.columns['name'].comment == 'User name'

    def test_view_with_string_columns(self):
        """Test View with string column definitions."""
        metadata = MetaData()
        view = View(
            'v2',
            metadata,
            definition='SELECT id, name FROM users',
            columns=['id', 'name']
        )

        assert view.name == 'v2'
        assert len(view.columns) == 2
        assert view.columns['id'].name == 'id'
        assert view.columns['name'].name == 'name'

    def test_view_with_dict_columns(self):
        """Test View with dict column definitions."""
        metadata = MetaData()
        view = View(
            'v3',
            metadata,
            definition='SELECT id, name, email FROM users',
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'},
                {'name': 'email', 'comment': 'User email'}
            ]
        )

        assert view.name == 'v3'
        assert len(view.columns) == 3
        assert view.columns['id'].name == 'id'
        assert view.columns['id'].comment is None
        assert view.columns['name'].comment == 'User name'
        assert view.columns['email'].comment == 'User email'

    def test_view_with_mixed_columns(self):
        """Test View with mixed column definitions."""
        metadata = MetaData()
        view = View(
            'v4',
            metadata,
            Column('id', STRING()),  # Column object via *args
            definition='SELECT id, name, email FROM users',
            columns=[  # Additional columns via columns parameter
                'name',
                {'name': 'email', 'comment': 'User email'}
            ]
        )

        assert view.name == 'v4'
        assert len(view.columns) == 3
        assert view.columns['id'].name == 'id'
        assert view.columns['name'].name == 'name'
        assert view.columns['email'].comment == 'User email'

    def test_view_without_columns(self):
        """Test View without explicit columns."""
        metadata = MetaData()
        view = View(
            'v5',
            metadata,
            definition='SELECT * FROM users'
        )

        assert view.name == 'v5'
        assert view.definition == 'SELECT * FROM users'
        assert len(view.columns) == 0

    def test_view_columns_with_invalid_dict(self):
        """Test that invalid dict column definition raises error."""
        metadata = MetaData()
        with pytest.raises(ValueError, match="Invalid column definition"):
            View(
                'v_invalid',
                metadata,
                definition='SELECT 1',
                columns=[{'comment': 'Missing name'}]
            )

    def test_view_columns_with_invalid_type(self):
        """Test that invalid column type raises error."""
        metadata = MetaData()
        with pytest.raises(ValueError, match="Invalid column definitions"):
            View(
                'v_invalid',
                metadata,
                definition='SELECT 1',
                columns=[123]  # Invalid type
            )


class TestViewOpsWithColumns:
    """Test View operations with columns."""

    def test_create_view_op_from_view_with_columns(self):
        """Test creating CreateViewOp from View object with columns."""
        metadata = MetaData()
        view = View(
            'v1',
            metadata,
            Column('id', STRING()),
            Column('name', STRING(), comment='User name'),
            Column('email', STRING()),
            definition='SELECT id, name, email FROM users',
            comment='User view'
        )

        op = CreateViewOp.from_view(view)

        assert op.view_name == 'v1'
        assert op.definition == 'SELECT id, name, email FROM users'
        assert op.comment == 'User view'
        assert op.columns is not None
        assert len(op.columns) == 3
        assert op.columns[0] == {'name': 'id', 'comment': None}
        assert op.columns[1] == {'name': 'name', 'comment': 'User name'}
        assert op.columns[2] == {'name': 'email', 'comment': None}

    def test_create_view_op_reverse(self):
        """Test CreateViewOp reverse operation."""
        metadata = MetaData()
        view = View(
            'v2',
            metadata,
            Column('id', STRING()),
            Column('name', STRING(), comment='User name'),
            definition='SELECT id, name FROM users',
            comment='User view'
        )

        create_op = CreateViewOp.from_view(view)
        assert create_op.definition is not None
        drop_op = create_op.reverse()

        assert drop_op.view_name == 'v2'

    def test_drop_view_op_from_view_with_columns(self):
        """Test creating DropViewOp from View object with columns."""
        metadata = MetaData()
        view = View(
            'v4',
            metadata,
            Column('id', STRING()),
            Column('name', STRING(), comment='User name'),
            definition='SELECT id, name FROM users'
        )

        drop_op = DropViewOp.from_view(view)

        assert drop_op.view_name == 'v4'
        assert drop_op.existing_columns is not None
        assert len(drop_op.existing_columns) == 2

        # Test reverse
        create_op = drop_op.reverse()
        assert create_op.columns is not None
        assert len(create_op.columns) == 2

    def test_create_view_op_manual_with_columns(self):
        """Test manually creating CreateViewOp with columns."""
        op = CreateViewOp(
            'v3',
            'SELECT id, name FROM users',
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'}
            ],
            comment='Manual view'
        )

        assert op.view_name == 'v3'
        assert op.columns is not None
        assert len(op.columns) == 2

    def test_view_op_without_columns(self):
        """Test View op without columns."""
        metadata = MetaData()
        view = View(
            'v5',
            metadata,
            definition='SELECT * FROM users'
        )

        op = CreateViewOp.from_view(view)
        assert op.columns is None


class TestAlterViewOpsWithColumns:
    """Test AlterView operations with columns."""

    def test_alter_view_op_with_columns(self):
        """Test creating AlterViewOp with columns."""
        op = AlterViewOp(
            'v1',
            'SELECT id, name, email FROM users',
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'},
                {'name': 'email', 'comment': 'Email address'}
            ],
            comment='Updated view'
        )

        assert op.view_name == 'v1'
        assert op.definition == 'SELECT id, name, email FROM users'
        assert op.comment == 'Updated view'
        assert op.columns is not None
        assert len(op.columns) == 3
        assert op.columns[0] == {'name': 'id'}
        assert op.columns[1] == {'name': 'name', 'comment': 'User name'}
        assert op.columns[2] == {'name': 'email', 'comment': 'Email address'}

    def test_alter_view_op_reverse_with_columns(self):
        """Test AlterViewOp reverse operation preserves columns."""
        op = AlterViewOp(
            'v1',
            'SELECT id, name FROM users',
            columns=[
                {'name': 'id', 'comment': None},
                {'name': 'name', 'comment': 'New comment'}
            ],
            comment='New view',
            existing_definition='SELECT id FROM users',
            existing_columns=[
                {'name': 'id', 'comment': None}
            ],
            existing_comment='Old view'
        )

        # Test forward operation
        assert op.view_name == 'v1'
        assert op.definition == 'SELECT id, name FROM users'
        assert len(op.columns) == 2
        assert op.columns[1]['comment'] == 'New comment'

        # Test reverse
        reverse_op = op.reverse()
        assert reverse_op.view_name == 'v1'
        assert reverse_op.definition == 'SELECT id FROM users'
        assert reverse_op.columns is not None
        assert len(reverse_op.columns) == 1
        assert reverse_op.columns[0]['name'] == 'id'
        assert reverse_op.comment == 'Old view'

        # Test reverse of reverse (should restore original)
        re_reverse_op = reverse_op.reverse()
        assert re_reverse_op.definition == 'SELECT id, name FROM users'
        assert len(re_reverse_op.columns) == 2
        assert re_reverse_op.columns[1]['comment'] == 'New comment'

    def test_alter_view_op_with_security_and_columns(self):
        """Test AlterViewOp with both security and columns."""
        op = AlterViewOp(
            'secure_view',
            'SELECT id, sensitive_data FROM users',
            columns=[
                {'name': 'id'},
                {'name': 'sensitive_data', 'comment': 'Protected data'}
            ],
            security='INVOKER',
            comment='Secure view with columns'
        )

        assert op.view_name == 'secure_view'
        assert op.security == 'INVOKER'
        assert op.columns is not None
        assert len(op.columns) == 2
        assert op.columns[1]['comment'] == 'Protected data'

    def test_alter_view_op_without_columns(self):
        """Test AlterViewOp without explicit columns."""
        op = AlterViewOp(
            'v2',
            'SELECT * FROM users',
            comment='Simple view'
        )

        assert op.view_name == 'v2'
        assert op.definition == 'SELECT * FROM users'
        assert op.columns is None

    def test_alter_view_op_change_column_order(self):
        """Test AlterViewOp with different column order."""
        op = AlterViewOp(
            'v1',
            'SELECT name, id FROM users',  # Changed order
            columns=[
                {'name': 'name', 'comment': 'User name'},
                {'name': 'id'}
            ],
            existing_definition='SELECT id, name FROM users',
            existing_columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'}
            ]
        )

        # Columns should reflect the new order
        assert op.columns[0]['name'] == 'name'
        assert op.columns[1]['name'] == 'id'

        # Reverse should have original order
        reverse_op = op.reverse()
        assert reverse_op.columns[0]['name'] == 'id'
        assert reverse_op.columns[1]['name'] == 'name'

    def test_alter_view_op_add_column(self):
        """Test AlterViewOp that adds a column."""
        op = AlterViewOp(
            'v1',
            'SELECT id, name, email FROM users',
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'},
                {'name': 'email', 'comment': 'New column'}
            ],
            existing_definition='SELECT id, name FROM users',
            existing_columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'}
            ]
        )

        # Forward: 3 columns
        assert len(op.columns) == 3
        assert op.columns[2]['name'] == 'email'

        # Reverse: 2 columns
        reverse_op = op.reverse()
        assert len(reverse_op.columns) == 2
        assert 'email' not in [col['name'] for col in reverse_op.columns]

    def test_alter_view_op_remove_column(self):
        """Test AlterViewOp that removes a column."""
        op = AlterViewOp(
            'v1',
            'SELECT id FROM users',
            columns=[
                {'name': 'id'}
            ],
            existing_definition='SELECT id, name FROM users',
            existing_columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'User name'}
            ]
        )

        # Forward: 1 column
        assert len(op.columns) == 1
        assert op.columns[0]['name'] == 'id'

        # Reverse: 2 columns
        reverse_op = op.reverse()
        assert len(reverse_op.columns) == 2
        assert reverse_op.columns[1]['name'] == 'name'
        assert reverse_op.columns[1]['comment'] == 'User name'

    def test_alter_view_op_change_column_comment(self):
        """Test AlterViewOp that only changes column comment."""
        op = AlterViewOp(
            'v1',
            'SELECT id, name FROM users',
            columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'Updated comment'}
            ],
            existing_definition='SELECT id, name FROM users',
            existing_columns=[
                {'name': 'id'},
                {'name': 'name', 'comment': 'Original comment'}
            ]
        )

        # Forward
        assert op.columns[1]['comment'] == 'Updated comment'

        # Reverse
        reverse_op = op.reverse()
        assert reverse_op.columns[1]['comment'] == 'Original comment'

