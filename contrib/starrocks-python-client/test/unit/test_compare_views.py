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
import warnings

from alembic.operations import ops
from sqlalchemy import Column, MetaData, Table, testing
from sqlalchemy.testing import eq_, expect_raises_message, fixtures

from starrocks.alembic.compare import _autogen_for_views, compare_view
from starrocks.alembic.ops import AlterViewOp, CreateViewOp, DropViewOp
from starrocks.common.defaults import ReflectionViewDefaults
from starrocks.common.params import TableKind, TableObjectInfoKey
from starrocks.datatype import VARCHAR
from starrocks.sql.schema import MaterializedView, View


class AutogenerateViewsTest(fixtures.TestBase):
    """Test full autogenerate flow with mock inspector.

    These tests use mock inspector to test CREATE/DROP operations which require
    get_view_names() functionality.
    """

    __requires__ = ("sqlalchemy_2",)

    def setup_test(self):
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
        View('my_test_view', m2, definition='SELECT 1')
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
        View('commented_view', m2, definition='SELECT 1', comment='This is a comment')
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
        View('my_secure_view', m2, definition='SELECT 1', starrocks_security='INVOKER')
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
        View(
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
        View(
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


class CompareViewTest(fixtures.TestBase):
    """Test compare_view function directly (without full autogen flow).

    This approach avoids complex mocking of reflect_table and tests the core
    comparison logic with real SQLAlchemy Table/View objects.
    """

    __requires__ = ("sqlalchemy_2",)

    def setup_test(self):
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
        """ALTER COMMENT: None → value (generates AlterViewOp, with no warning)."""
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
            eq_(op.existing_definition, 'SELECT 1')  # Carried, to recreate the view
            eq_(op.existing_security, None)  # Not changed

            # Should have warning about comment not supported
            eq_(len(w), 0)

    def test_alter_view_comment_value_to_none(self):
        """ALTER COMMENT: value → None (generates AlterViewOp)."""
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
            eq_(op.existing_definition, 'SELECT 1')  # Carried, to recreate the view
            eq_(op.existing_comment, 'Old comment')  # Changed (was 'Old comment')
            eq_(op.existing_security, None)  # Not changed

            eq_(len(w), 0)

    def test_alter_view_comment_value_to_different(self):
        """ALTER COMMENT: value1 → value2 (generates AlterViewOp)."""
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
            eq_(op.existing_definition, 'SELECT 1')  # Carried, to recreate the view
            eq_(op.existing_comment, 'Old comment')  # Changed
            eq_(op.existing_security, None)  # Not changed

            eq_(len(w), 0)

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
    # ALTER VIEW - Security (applied with ALTER VIEW ... SET SECURITY)
    # ========================================================================

    def test_alter_view_security_none_to_invoker(self):
        """ALTER SECURITY: None → INVOKER (generates AlterViewOp)."""
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

            eq_(len(w), 0)

    def test_omitted_security_is_left_alone(self):
        """A metadata view without starrocks_security keeps the INVOKER security it has in the database."""
        upgrade_ops = ops.UpgradeOps([])
        conn_view = self._create_reflected_view('my_test_view', definition='SELECT 1', security='INVOKER')
        meta_view = View('my_test_view', MetaData(), definition='SELECT 1')

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compare_view(self.mock_autogen_context, upgrade_ops, None, 'my_test_view', conn_view, meta_view)

            eq_(upgrade_ops.ops, [])
            eq_(len(w), 0)

    def test_alter_view_security_change_value(self):
        """ALTER SECURITY: INVOKER → NONE (generates AlterViewOp).
        """
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

            eq_(len(w), 0)

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

        with warnings.catch_warnings(record=True):
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

    def test_alter_view_columns_only(self):
        """ALTER COLUMNS: Changed without definition (generates AlterViewOp with the unchanged definition)."""
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

        compare_view(
            self.mock_autogen_context,
            upgrade_ops,
            None,
            'my_view',
            conn_view,
            meta_view
        )

        eq_(len(upgrade_ops.ops), 1)
        op = upgrade_ops.ops[0]
        assert isinstance(op, AlterViewOp)
        eq_(op.definition, 'SELECT id, name FROM users')
        eq_([(col['name'], col.get('comment')) for col in op.columns], [('id', None), ('name', 'New comment')])
        eq_(op.existing_definition, 'SELECT id, name FROM users')
        eq_([(col['name'], col.get('comment')) for col in op.existing_columns], [('id', None), ('name', 'Old comment')])
        eq_(op.security, None)

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


class ViewExceptionsTest(fixtures.TestBase):
    """Test exception cases for View creation."""

    __requires__ = ("sqlalchemy_2",)

    def test_view_without_definition_raises_error(self):
        """Test that creating a View without definition raises ValueError."""
        m = MetaData()
        with expect_raises_message(ValueError, "definition is required"):
            View('my_view', m)

    def test_view_with_invalid_definition_type_raises_error(self):
        """Test that creating a View with invalid definition type raises TypeError."""
        m = MetaData()
        with expect_raises_message(TypeError, "definition must be a string or a Selectable"):
            View('my_view', m, definition=123)  # Invalid type

    def test_view_with_none_definition_raises_error(self):
        """Test that explicitly passing None as definition raises ValueError."""
        m = MetaData()
        with expect_raises_message(ValueError, "definition is required"):
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



def _connection_without_temp_views(failing_only_for=None, error=None):
    """A connection on which temporary views fail to be created, as for a user without CREATE VIEW.

    With ``failing_only_for``, only a temporary view whose SQL holds that text fails, as for a definition that reads
    a column the database does not have yet. ``error`` is what StarRocks says instead.
    """
    def execute(statement, *args, **kwargs):
        if statement.startswith("CREATE") and (failing_only_for is None or failing_only_for in statement):
            raise RuntimeError(error or ("Access denied" if failing_only_for is None else "Column cannot be resolved"))
        return Mock(fetchone=Mock(return_value=("SELECT canonical",)))

    return Mock(exec_driver_sql=Mock(side_effect=execute))


class ViewDefinitionWithoutTempViewTest(fixtures.TestBase):
    """Without the temporary view, a difference only the in-process normalizer sees is a change only sometimes.

    Before 4.0.7 StarRocks keeps a view's SQL rewritten, with table-qualified columns, so the normalizer sees a
    difference in every view; the database's definition below is how 3.3 shows ``SELECT id FROM t``.
    """

    __requires__ = ("sqlalchemy_2",)

    _REWRITTEN = "SELECT `db`.`t`.`id` FROM `db`.`t`"

    @classmethod
    def _operations_for(
        cls, server_version, model_reads="t", database_reads="t", revision_operations=(), model_mvs=(), database_mvs=(),
        model_columns="id", failing_only_for=None, error=None, database_definition=None, name_filter=None,
    ):
        context = Mock()
        context.opts = {}
        context.run_name_filters = Mock(side_effect=name_filter or (lambda name, type_, parent_names: True))
        context.dialect.name = "starrocks"
        context.dialect.default_schema_name = "db"
        context.dialect.server_version_info = server_version
        context.connection = _connection_without_temp_views(failing_only_for, error)
        context.migration_context.opts = {}
        context.metadata = MetaData()
        context.inspector.get_materialized_view_names = Mock(return_value=list(database_mvs))
        model_view = View("v", context.metadata, definition=f"SELECT {model_columns} FROM {model_reads}")
        for name in model_mvs:
            MaterializedView(name, context.metadata, definition="SELECT id FROM t", starrocks_refresh="MANUAL")
        database_view = View(
            "v", MetaData(), definition=database_definition or cls._REWRITTEN.replace("`t`", f"`{database_reads}`")
        )
        upgrade_ops = ops.UpgradeOps(list(revision_operations))
        compare_view(context, upgrade_ops, None, "v", database_view, model_view)
        return [type(op) for op in upgrade_ops.ops[len(revision_operations):]]

    @testing.combinations(((3, 3, 22),), ((3, 5, 21),), ((4, 0, 6),), (None,), argnames="server_version")
    def test_before_4_0_7_an_unchanged_view_is_left_alone(self, server_version, caplog):
        eq_(self._operations_for(server_version), [])
        assert "could not canonicalize them through a temporary view" in caplog.text

    @testing.combinations(((4, 0, 7),), ((4, 1, 0),), ((4, 1, 6),), argnames="server_version")
    def test_from_4_0_7_a_text_difference_is_a_change(self, server_version):
        """StarRocks keeps a view's SQL as written from 4.0.7 (StarRocks #68040, backported as #69192)."""
        eq_(self._operations_for(server_version), [AlterViewOp])

    def test_a_definition_reading_a_table_the_revision_creates_is_a_change(self):
        created = ops.CreateTableOp.from_table(Table("x", MetaData(), Column("id", VARCHAR(10))))
        eq_(self._operations_for((3, 3, 22), model_reads="x", revision_operations=[created]), [AlterViewOp])

    def test_a_definition_reading_a_view_the_revision_creates_is_a_change(self):
        """Autogenerate adds the creates of new views before it compares the existing ones."""
        created = CreateViewOp("x", "SELECT id FROM t")
        eq_(self._operations_for((3, 3, 22), model_reads="x", revision_operations=[created]), [AlterViewOp])

    def test_a_definition_reading_a_materialized_view_the_model_adds_is_a_change(self):
        """Views are compared before materialized views, so the model and the database tell which ones are new."""
        eq_(self._operations_for((3, 3, 22), model_reads="x", model_mvs=["x"]), [AlterViewOp])

    def test_a_definition_reading_an_existing_materialized_view_is_not_certain(self, caplog):
        eq_(self._operations_for((3, 3, 22), model_reads="x", model_mvs=["x"], database_mvs=["x"]), [])
        assert "could not canonicalize them through a temporary view" in caplog.text

    def test_a_database_definition_reading_a_table_the_revision_drops_is_a_change(self):
        dropped = ops.DropTableOp.from_table(Table("x", MetaData(), Column("id", VARCHAR(10))))
        eq_(self._operations_for((3, 3, 22), database_reads="x", revision_operations=[dropped]), [AlterViewOp])

    @staticmethod
    def _dropping(table, column):
        return ops.ModifyTableOps(table, [ops.DropColumnOp.from_column_and_tablename(None, table, Column(column))])

    @testing.combinations(
        ("a_column_it_names", "SELECT `db`.`t`.`id` FROM `db`.`t`", "id"),
        ("a_column_its_star_may_read", "SELECT * FROM `db`.`t`", "other"),
        argnames="database_definition, dropped",
        id_="iaa",
    )
    def test_a_database_definition_reading_a_column_the_revision_drops_is_a_change(
        self, database_definition, dropped
    ):
        """The model cannot read a column it removes, so the definition did change; the view would break."""
        operations = self._operations_for(
            (3, 3, 22), revision_operations=[self._dropping("t", dropped)], database_definition=database_definition
        )
        eq_(operations, [AlterViewOp])

    def test_a_column_the_database_definition_does_not_read_makes_no_change_certain(self, caplog):
        eq_(self._operations_for((3, 3, 22), revision_operations=[self._dropping("t", "other")]), [])
        assert "could not canonicalize them through a temporary view" in caplog.text

    def test_a_database_definition_reading_a_materialized_view_the_model_removes_is_a_change(self):
        """Views are compared before materialized views, so the model and the database tell which ones go."""
        eq_(self._operations_for((3, 3, 22), database_reads="x", database_mvs=["x"]), [AlterViewOp])

    def test_a_materialized_view_the_name_filters_leave_alone_is_not_dropped(self, caplog):
        """Autogenerate drops no materialized view the include_name filter leaves out, so nothing is certain."""
        operations = self._operations_for(
            (3, 3, 22), database_reads="x", database_mvs=["x"],
            name_filter=lambda name, type_, parent_names: type_ != "materialized_view",
        )
        eq_(operations, [])
        assert "could not canonicalize them through a temporary view" in caplog.text

    def test_a_definition_only_the_model_cannot_create_a_temporary_view_for_is_a_change(self):
        """The database's definition goes through the temporary view, so the model's reads something new."""
        operations = self._operations_for((3, 3, 22), model_columns="id, added", failing_only_for="added")
        eq_(operations, [AlterViewOp])

    @testing.combinations(
        ("access_denied", "Access denied; you need the CREATE VIEW privilege on DATABASE tmp", "Grant the migration user CREATE VIEW"),
        ("unknown_database", "Unknown database 'db'", "Create the schema"),
        ("unknown_other_database", "Unknown database 'tmp'", None),
        ("table_select_denied", "Access denied; you need (at least one of) the SELECT privilege(s) on TABLE t2 for this operation.", None),
        ("a_refused_definition", "Duplicate column name 'id'", None),
        argnames="error, advice",
        id_="iaa",
    )
    def test_the_warning_says_what_starrocks_said_and_advises_only_what_would_cure_it(
        self, caplog, error, advice
    ):
        eq_(self._operations_for((3, 3, 22), error=error), [])
        assert f"StarRocks said: {error}." in caplog.text, caplog.text
        for step in ("Grant the migration user CREATE VIEW", "Create the schema"):
            eq_(step in caplog.text, step == advice)
