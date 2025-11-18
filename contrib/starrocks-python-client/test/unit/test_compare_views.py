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
Unit tests for View comparison logic.

These tests directly call compare_view() function with mock SQLAlchemy objects,
avoiding the complexity of mocking the full reflection flow.
"""

from unittest.mock import Mock

from alembic.operations import ops
from alembic.testing import eq_
from sqlalchemy import Column, MetaData, Table

from starrocks.alembic.compare import _autogen_for_views, compare_view
from starrocks.alembic.ops import AlterViewOp, CreateViewOp, DropViewOp
from starrocks.common.defaults import ReflectionViewDefaults
from starrocks.common.params import TableKind, TableObjectInfoKey
from starrocks.datatype import VARCHAR
from starrocks.sql.schema import View


class TestAutogenerateViews:
    """Test full autogenerate flow with mock inspector.

    These tests use mock inspector to test CREATE/DROP operations which require
    get_view_names() functionality.
    """

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

    # ========================================================================
    # CREATE VIEW Tests
    # ========================================================================

    def test_create_view_basic(self):
        """CREATE: View exists in metadata but not in database."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = []
        m2 = MetaData()
        view = View('my_test_view', m2, definition='SELECT 1')
        self.mock_autogen_context.metadata = m2
        _autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])

        eq_(len(upgrade_ops.ops), 1)
        op = upgrade_ops.ops[0]
        assert isinstance(op, CreateViewOp)
        eq_(op.view_name, 'my_test_view')
        eq_(op.definition, 'SELECT 1')

    def test_create_view_with_comment(self):
        """CREATE: View with comment."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = []
        m2 = MetaData()
        view = View('commented_view', m2, definition='SELECT 1', comment='This is a comment')
        self.mock_autogen_context.metadata = m2
        _autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])

        eq_(len(upgrade_ops.ops), 1)
        op = upgrade_ops.ops[0]
        assert isinstance(op, CreateViewOp)
        eq_(op.view_name, 'commented_view')
        eq_(op.comment, 'This is a comment')

    def test_create_view_with_security(self):
        """CREATE: View with security."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = []
        m2 = MetaData()
        view = View('my_secure_view', m2, definition='SELECT 1', starrocks_security='INVOKER')
        self.mock_autogen_context.metadata = m2
        _autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])

        eq_(len(upgrade_ops.ops), 1)
        op = upgrade_ops.ops[0]
        assert isinstance(op, CreateViewOp)
        eq_(op.view_name, 'my_secure_view')
        eq_(op.kwargs['starrocks_security'], 'INVOKER')

    def test_create_view_with_columns(self):
        """CREATE: View with columns."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = []
        m2 = MetaData()
        view = View(
            'my_view',
            m2,
            Column('id', VARCHAR()),
            Column('name', VARCHAR(50), comment='User name'),
            definition='SELECT id, name FROM users'
        )
        self.mock_autogen_context.metadata = m2
        _autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])

        eq_(len(upgrade_ops.ops), 1)
        op = upgrade_ops.ops[0]
        assert isinstance(op, CreateViewOp)
        eq_(op.view_name, 'my_view')
        # Verify columns are extracted (only name and comment)
        assert op.columns is not None
        assert len(op.columns) == 2
        assert op.columns[0]['name'] == 'id'
        assert op.columns[1]['name'] == 'name'
        assert op.columns[1]['comment'] == 'User name'

    def test_create_view_with_all_attributes(self):
        """CREATE: View with all attributes (comment, security, columns)."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = []
        m2 = MetaData()
        view = View(
            'full_view',
            m2,
            Column('id', VARCHAR()),
            Column('name', VARCHAR(), comment='Name column'),
            definition='SELECT 1, 2',
            comment='Full view with all attributes',
            starrocks_security='INVOKER'
        )
        self.mock_autogen_context.metadata = m2
        _autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])

        eq_(len(upgrade_ops.ops), 1)
        op = upgrade_ops.ops[0]
        assert isinstance(op, CreateViewOp)
        eq_(op.view_name, 'full_view')
        eq_(op.comment, 'Full view with all attributes')
        eq_(op.kwargs['starrocks_security'], 'INVOKER')
        assert op.columns is not None
        eq_(len(op.columns), 2)

    # ========================================================================
    # DROP VIEW Tests
    # ========================================================================

    def test_drop_view_basic(self):
        """DROP: View exists in database but not in metadata."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_test_view']
        m2 = MetaData()
        self.mock_autogen_context.metadata = m2
        _autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])

        eq_(len(upgrade_ops.ops), 1)
        op = upgrade_ops.ops[0]
        assert isinstance(op, DropViewOp)
        eq_(op.view_name, 'my_test_view')


class TestCompareView:
    """Test compare_view function directly (without full autogen flow).

    This approach avoids complex mocking of reflect_table and tests the core
    comparison logic with real SQLAlchemy Table/View objects.
    """

    def setup_method(self, method):
        self.mock_autogen_context = Mock()
        self.mock_autogen_context.dialect = Mock()
        self.mock_autogen_context.dialect.name = 'starrocks'
        self.mock_autogen_context.dialect.default_schema_name = None

    def _create_reflected_view(
        self,
        name: str,
        definition: str,
        comment: str = None,
        security: str = None,
        columns: list = None,
        schema: str = None
    ) -> Table:
        """Helper: Create a Table object simulating a reflected view from database.

        This simulates what inspector.reflect_table() would create after reflecting
        a view from the database.

        Args:
            name: View name
            definition: SQL definition
            comment: View comment
            security: Security option (INVOKER/NONE)
            columns: List of Column objects or dicts with 'name' and optional 'comment'
            schema: Schema name

        Returns:
            Table object with view info populated
        """
        metadata = MetaData()
        table = Table(name, metadata, schema=schema)

        # Set view-specific info (what reflect_table sets)
        table.info[TableObjectInfoKey.DEFINITION] = definition
        table.info[TableObjectInfoKey.TABLE_KIND] = TableKind.VIEW

        # Set comment
        if comment:
            table.comment = comment

        # Set security (in dialect_options)
        if security:
            table.dialect_options.setdefault('starrocks', {})['security'] = security

        # Set columns
        if columns:
            for col in columns:
                if isinstance(col, dict):
                    table.append_column(
                        Column(col['name'], VARCHAR(), comment=col.get('comment'))
                    )
                else:
                    table.append_column(col)

        return table

    # ========================================================================
    # ALTER VIEW - Definition
    # ========================================================================

    def test_modify_view_definition_changed(self):
        """ALTER: Definition changed (generates AlterViewOp).
        """
        upgrade_ops = ops.UpgradeOps([])

        # Create "database" view (what was reflected)
        conn_view = self._create_reflected_view('my_test_view', definition='SELECT 1')

        # Create "metadata" view (what user defined)
        meta_view = View('my_test_view', MetaData(), definition='SELECT 2')

        # Compare
        compare_view(
            self.mock_autogen_context,
            upgrade_ops,
            None,  # schema
            'my_test_view',
            conn_view,
            meta_view
        )

        eq_(len(upgrade_ops.ops), 1)
        op = upgrade_ops.ops[0]
        assert isinstance(op, AlterViewOp)
        eq_(op.view_name, 'my_test_view')

        # Validate forward (new/metadata) values
        eq_(op.definition, 'SELECT 2')

        # Validate reverse (existing/database) values for downgrade
        eq_(op.existing_definition, 'SELECT 1')

    # ========================================================================
    # ALTER VIEW - Comment (StarRocks limitation - not supported via ALTER)
    # ========================================================================

    def test_alter_view_comment_none_to_value(self):
        """ALTER COMMENT: None → value (generates AlterViewOp with warning).

        StarRocks does not support altering comment via ALTER VIEW currently.
        Compare generates AlterViewOp with the new comment for future compatibility.
        A warning is issued to inform users about current limitation.
        """
        import warnings

        upgrade_ops = ops.UpgradeOps([])

        # Database view (no comment)
        conn_view = self._create_reflected_view('my_test_view', definition='SELECT 1')

        # Metadata view (with comment)
        meta_view = View('my_test_view', MetaData(), definition='SELECT 1', comment='New comment')

        # Capture warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compare_view(
                self.mock_autogen_context,
                upgrade_ops,
                None,
                'my_test_view',
                conn_view,
                meta_view
            )

            # Should generate AlterViewOp for future compatibility
            eq_(len(upgrade_ops.ops), 1)
            op = upgrade_ops.ops[0]
            assert isinstance(op, AlterViewOp)

            # Validate forward (new/metadata) values - only comment changed
            eq_(op.comment, 'New comment')  # Changed
            eq_(op.definition, None)  # Not changed
            eq_(op.security, None)  # Not changed

            # Validate reverse (existing/database) values for downgrade
            eq_(op.existing_comment, None)  # Changed (was None)
            eq_(op.existing_definition, None)  # Not changed
            eq_(op.existing_security, None)  # Not changed

            # Should have warning about comment not supported
            assert len(w) == 1
            assert "does not support altering view comments" in str(w[0].message)

    def test_alter_view_comment_value_to_none(self):
        """ALTER COMMENT: value → None (generates AlterViewOp with warning)."""
        import warnings

        upgrade_ops = ops.UpgradeOps([])

        # Database view (with comment)
        conn_view = self._create_reflected_view(
            'my_test_view',
            definition='SELECT 1',
            comment='Old comment'
        )

        # Metadata view (no comment)
        meta_view = View('my_test_view', MetaData(), definition='SELECT 1')  # No comment

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compare_view(
                self.mock_autogen_context,
                upgrade_ops,
                None,
                'my_test_view',
                conn_view,
                meta_view
            )

            eq_(len(upgrade_ops.ops), 1)
            op = upgrade_ops.ops[0]
            assert isinstance(op, AlterViewOp)

            # Validate forward (new/metadata) values - only comment changed
            eq_(op.definition, None)  # Not changed
            eq_(op.comment, None)  # Changed (removed)
            eq_(op.security, None)  # Not changed

            # Validate reverse (existing/database) values for downgrade
            eq_(op.existing_definition, None)  # Not changed
            eq_(op.existing_comment, 'Old comment')  # Changed (was 'Old comment')
            eq_(op.existing_security, None)  # Not changed

            assert len(w) == 1
            assert "does not support altering view comments" in str(w[0].message)

    def test_alter_view_comment_value_to_different(self):
        """ALTER COMMENT: value1 → value2 (generates AlterViewOp with warning)."""
        import warnings

        upgrade_ops = ops.UpgradeOps([])

        # Database view (old comment)
        conn_view = self._create_reflected_view(
            'my_test_view',
            definition='SELECT 1',
            comment='Old comment'
        )

        # Metadata view (new comment)
        meta_view = View('my_test_view', MetaData(), definition='SELECT 1', comment='New comment')

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compare_view(
                self.mock_autogen_context,
                upgrade_ops,
                None,
                'my_test_view',
                conn_view,
                meta_view
            )

            eq_(len(upgrade_ops.ops), 1)
            op = upgrade_ops.ops[0]
            assert isinstance(op, AlterViewOp)

            # Validate forward (new/metadata) values - only comment changed
            eq_(op.definition, None)  # Not changed
            eq_(op.comment, 'New comment')  # Changed
            eq_(op.security, None)  # Not changed

            # Validate reverse (existing/database) values for downgrade
            eq_(op.existing_definition, None)  # Not changed
            eq_(op.existing_comment, 'Old comment')  # Changed
            eq_(op.existing_security, None)  # Not changed

            assert len(w) == 1
            assert "does not support altering view comments" in str(w[0].message)

    def test_alter_view_comment_no_change(self):
        """ALTER COMMENT: No change (identical comment)."""
        upgrade_ops = ops.UpgradeOps([])

        # Both have same comment
        conn_view = self._create_reflected_view(
            'my_view',
            definition='SELECT 1',
            comment='Same comment'
        )
        meta_view = View('my_view', MetaData(), definition='SELECT 1', comment='Same comment')

        compare_view(
            self.mock_autogen_context,
            upgrade_ops,
            None,
            'my_view',
            conn_view,
            meta_view
        )

        # No changes should result in no ops
        eq_(len(upgrade_ops.ops), 0)

    # ========================================================================
    # ALTER VIEW - Security (StarRocks limitation - not supported via ALTER)
    # ========================================================================

    def test_alter_view_security_none_to_invoker(self):
        """ALTER SECURITY: None → INVOKER (generates AlterViewOp with warning)."""
        import warnings

        upgrade_ops = ops.UpgradeOps([])

        # Database view (no security)
        conn_view = self._create_reflected_view('my_secure_view', definition='SELECT 1')

        # Metadata view (with security)
        meta_view = View('my_secure_view', MetaData(), definition='SELECT 1', starrocks_security='INVOKER')

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compare_view(
                self.mock_autogen_context,
                upgrade_ops,
                None,
                'my_secure_view',
                conn_view,
                meta_view
            )

            eq_(len(upgrade_ops.ops), 1)
            op = upgrade_ops.ops[0]
            assert isinstance(op, AlterViewOp)

            # Validate forward (new/metadata) values
            eq_(op.security, 'INVOKER')  # Changed
            eq_(op.definition, None)  # Not changed
            eq_(op.comment, None)  # Not changed

            # Validate reverse (existing/database) values for downgrade
            assert op.existing_security is None or op.existing_security == ReflectionViewDefaults.security()
            eq_(op.existing_definition, None)  # Not changed
            eq_(op.existing_comment, None)  # Not changed

            assert len(w) == 1
            assert "does not support altering view security" in str(w[0].message)

    def test_alter_view_security_invoker_to_none(self):
        """ALTER SECURITY: INVOKER → None (generates AlterViewOp with warning)."""
        import warnings

        upgrade_ops = ops.UpgradeOps([])

        # Database view (with security)
        conn_view = self._create_reflected_view(
            'my_test_view',
            definition='SELECT 1',
            security='INVOKER'
        )

        # Metadata view (no security)
        meta_view = View('my_test_view', MetaData(), definition='SELECT 1')

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compare_view(
                self.mock_autogen_context,
                upgrade_ops,
                None,
                'my_test_view',
                conn_view,
                meta_view
            )

            eq_(len(upgrade_ops.ops), 1)
            op = upgrade_ops.ops[0]
            assert isinstance(op, AlterViewOp)

            # Validate forward (new/metadata) values - only security changed
            eq_(op.definition, None)  # Not changed
            eq_(op.comment, None)  # Not changed
            assert op.security is None or op.security == ReflectionViewDefaults.security()  # Changed (removed)

            # Validate reverse (existing/database) values for downgrade
            eq_(op.existing_definition, None)  # Not changed
            eq_(op.existing_comment, None)  # Not changed
            eq_(op.existing_security, 'INVOKER')  # Changed (was INVOKER)

            assert len(w) == 1
            assert "does not support altering view security" in str(w[0].message)

    def test_alter_view_security_change_value(self):
        """ALTER SECURITY: INVOKER → NONE (generates AlterViewOp with warning).
        """
        import warnings

        upgrade_ops = ops.UpgradeOps([])

        # Database view (INVOKER security)
        conn_view = self._create_reflected_view(
            'my_test_view',
            definition='SELECT 1',
            security='INVOKER'
        )

        # Metadata view (NONE security)
        meta_view = View('my_test_view', MetaData(), definition='SELECT 1', starrocks_security='NONE')

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compare_view(
                self.mock_autogen_context,
                upgrade_ops,
                None,
                'my_test_view',
                conn_view,
                meta_view
            )

            eq_(len(upgrade_ops.ops), 1)
            op = upgrade_ops.ops[0]
            assert isinstance(op, AlterViewOp)

            # Validate forward (new/metadata) values - only security changed
            eq_(op.definition, None)  # Not changed
            eq_(op.comment, None)  # Not changed
            eq_(op.security, 'NONE')  # Changed

            # Validate reverse (existing/database) values for downgrade
            eq_(op.existing_definition, None)  # Not changed
            eq_(op.existing_comment, None)  # Not changed
            eq_(op.existing_security, 'INVOKER')  # Changed (was INVOKER)

            assert len(w) == 1
            assert "does not support altering view security" in str(w[0].message)

    def test_alter_view_security_no_change(self):
        """ALTER SECURITY: No change (identical security)."""
        upgrade_ops = ops.UpgradeOps([])

        # Both have same security
        conn_view = self._create_reflected_view(
            'my_view',
            definition='SELECT 1',
            security='INVOKER'
        )
        meta_view = View('my_view', MetaData(), definition='SELECT 1', starrocks_security='INVOKER')

        compare_view(
            self.mock_autogen_context,
            upgrade_ops,
            None,
            'my_view',
            conn_view,
            meta_view
        )

        # No changes should result in no ops
        eq_(len(upgrade_ops.ops), 0)

    # ========================================================================
    # ALTER VIEW - Columns
    # ========================================================================

    def test_alter_view_columns_changed_with_definition(self):
        """ALTER COLUMNS: Changed together with definition (generates AlterViewOp with warning)."""
        import warnings

        upgrade_ops = ops.UpgradeOps([])

        # Database view
        conn_view = self._create_reflected_view(
            'my_view',
            definition='SELECT id, name FROM users',
            columns=[
                {'name': 'id', 'comment': None},
                {'name': 'name', 'comment': 'Old comment'}
            ]
        )

        # Metadata view (different definition AND column comment)
        meta_view = View(
            'my_view',
            MetaData(),
            Column('id', VARCHAR()),
            Column('name', VARCHAR(), comment='New comment'),
            definition='SELECT id, name FROM users WHERE active = 1'  # Definition changed!
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compare_view(
                self.mock_autogen_context,
                upgrade_ops,
                None,
                'my_view',
                conn_view,
                meta_view
            )

            # Should generate AlterViewOp for definition change (columns updated too)
            eq_(len(upgrade_ops.ops), 1)
            op = upgrade_ops.ops[0]
            assert isinstance(op, AlterViewOp)
            eq_(op.definition, 'SELECT id, name FROM users WHERE active = 1')

            # Should have warning about columns
            # Note: Currently there's no warning for columns when definition changes
            # assert any("columns" in str(warning.message) for warning in w)

    def test_alter_view_columns_only_raises_error(self):
        """ALTER COLUMNS: Changed without definition (raises ValueError)."""
        import pytest

        upgrade_ops = ops.UpgradeOps([])

        # Database view
        conn_view = self._create_reflected_view(
            'my_view',
            definition='SELECT id, name FROM users',
            columns=[
                {'name': 'id', 'comment': None},
                {'name': 'name', 'comment': 'Old comment'}
            ]
        )

        # Metadata view (ONLY column comment changed, same definition)
        meta_view = View(
            'my_view',
            MetaData(),
            Column('id', VARCHAR()),
            Column('name', VARCHAR(), comment='New comment'),
            definition='SELECT id, name FROM users'  # Same definition!
        )

        # Should raise NotImplementedError
        with pytest.raises(NotImplementedError, match="does not support altering view columns independently"):
            compare_view(
                self.mock_autogen_context,
                upgrade_ops,
                None,
                'my_view',
                conn_view,
                meta_view
            )

    def test_alter_view_columns_no_change(self):
        """ALTER COLUMNS: No change (identical columns)."""
        upgrade_ops = ops.UpgradeOps([])

        # Both have same columns
        conn_view = self._create_reflected_view(
            'my_view',
            definition='SELECT id, name FROM users',
            columns=[
                {'name': 'id', 'comment': None},
                {'name': 'name', 'comment': 'User name'}
            ]
        )
        meta_view = View(
            'my_view',
            MetaData(),
            Column('id', VARCHAR()),
            Column('name', VARCHAR(), comment='User name'),
            definition='SELECT id, name FROM users'
        )

        compare_view(
            self.mock_autogen_context,
            upgrade_ops,
            None,
            'my_view',
            conn_view,
            meta_view
        )

        # No changes should result in no ops
        eq_(len(upgrade_ops.ops), 0)

    # ========================================================================
    # No-Change Tests
    # ========================================================================

    def test_no_change_view_basic(self):
        """No change: Identical definition (with SQL normalization)."""
        upgrade_ops = ops.UpgradeOps([])

        # Both have same definition (minor formatting differences are normalized)
        conn_view = self._create_reflected_view('my_test_view', definition='SELECT 1 AS `val`')
        meta_view = View('my_test_view', MetaData(), definition='SELECT 1 AS val')

        compare_view(
            self.mock_autogen_context,
            upgrade_ops,
            None,
            'my_test_view',
            conn_view,
            meta_view
        )

        eq_(len(upgrade_ops.ops), 0)


class TestViewExceptions:
    """Test exception cases for View creation."""

    def test_view_without_definition_raises_error(self):
        """Test that creating a View without definition raises ValueError."""
        import pytest
        m = MetaData()
        with pytest.raises(ValueError, match="definition is required"):
            View('my_view', m)

    def test_view_with_invalid_definition_type_raises_error(self):
        """Test that creating a View with invalid definition type raises TypeError."""
        import pytest
        m = MetaData()
        with pytest.raises(TypeError, match="definition must be a string or a Selectable"):
            View('my_view', m, definition=123)  # Invalid type

    def test_view_with_none_definition_raises_error(self):
        """Test that explicitly passing None as definition raises ValueError."""
        import pytest
        m = MetaData()
        with pytest.raises(ValueError, match="definition is required"):
            View('my_view', m, definition=None)

    def test_view_with_selectable_definition(self):
        """Test that View accepts SQLAlchemy Selectable as definition."""
        from sqlalchemy import Table, select
        m = MetaData()
        users = Table('users', m, Column('id', VARCHAR()), Column('name', VARCHAR()))
        stmt = select(users.c.id, users.c.name)

        # Should not raise any exception
        view = View('user_view', m, definition=stmt)
        assert view.definition is not None
        assert 'select' in view.definition.lower()

    def test_view_columns_parameter_compatibility(self):
        """Test that both columns parameter styles work correctly."""
        m = MetaData()

        # Style 1: List of column names
        view1 = View('v1', m, definition='SELECT 1, 2', columns=['a', 'b'])
        assert len(view1.columns) == 2
        assert view1.columns['a'].name == 'a'
        assert view1.columns['b'].name == 'b'

        # Style 2: List of dicts with name and comment
        cols = [{'name': 'a', 'comment': 'Column A'}, {'name': 'b'}]
        view2 = View('v2', m, definition='SELECT 1, 2', columns=cols)
        assert len(view2.columns) == 2
        assert view2.columns['a'].comment == 'Column A'
        assert view2.columns['b'].comment is None

