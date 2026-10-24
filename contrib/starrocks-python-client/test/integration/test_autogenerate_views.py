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
import warnings

from alembic.autogenerate import api
from alembic.operations import Operations
from alembic.operations.ops import AlterTableOp, ModifyTableOps
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column, MetaData, Table, testing, text
from sqlalchemy.engine import Connection
from sqlalchemy.testing import config, engines, fixtures

from starrocks.alembic.compare import (
    _get_canonical_sql_via_temp_view,
    combine_include_object,
    include_object_for_view_mv,
)
from starrocks.alembic.ops import AlterViewOp, CreateViewOp, DropViewOp
from starrocks.common.params import DialectName, TableInfoKey, TableInfoKeyWithPrefix
from starrocks.datatype import INTEGER, STRING
from starrocks.sql.schema import View
from test.util import (
    VIEW_SECURITY_NEEDS_3_4_1,
    VIEW_TEXT_COMPARED_FROM_4_0_7,
    VIEW_TEXT_NOT_COMPARED_BEFORE_4_0_7,
    normalize_sql,
)


logger = logging.getLogger(__name__)


def create_migration_context(conn, target_metadata: MetaData) -> MigrationContext:
    """Create a MigrationContext with standard view/MV configuration."""
    return MigrationContext.configure(
        connection=conn,
        opts={
            'target_metadata': target_metadata,
            'include_object': include_object_for_view_mv
        }
    )


def assert_view_dropped_completely(conn, view_name: str) -> None:
    """Assert that a view is completely dropped and no other views exist."""
    reflected_metadata = MetaData()
    reflected_metadata.reflect(bind=conn, views=True)
    assert view_name not in reflected_metadata.tables, \
        f"View '{view_name}' should be dropped but still exists"
    assert len(reflected_metadata.tables) == 0, \
        f"Expected no views after drop, but found: {list(reflected_metadata.tables.keys())}"


def assert_view_definition(reflected_view, expected_definition: str) -> None:
    """Assert that a view's definition matches the expected SQL."""
    assert 'definition' in reflected_view.info, \
        "View should have 'definition' in info"
    actual_def = reflected_view.info['definition']
    assert normalize_sql(expected_definition) == normalize_sql(actual_def), \
        f"View definition mismatch:\nExpected: {expected_definition}\nActual: {actual_def}"


def apply_downgrade_with_fresh_metadata(conn, migration_script, view_definitions: dict = None) -> None:
    """
    Apply downgrade operations with a fresh MetaData.

    Args:
        conn: Database connection
        migration_script: The migration script containing downgrade_ops
        view_definitions: Optional dict of {view_name: View(...)} for downgrade target state
    """
    downgrade_metadata = MetaData()

    # Add view definitions if provided (for ALTER downgrade scenarios)
    if view_definitions:
        for view in view_definitions.values():
            # Views are already added to downgrade_metadata in the dict
            pass

    mc_downgrade = create_migration_context(conn, downgrade_metadata)
    op_downgrade = Operations(mc_downgrade)
    for op_item in migration_script.downgrade_ops.ops:
        op_downgrade.invoke(op_item)


"""
Integration tests for View autogenerate functionality.

These tests use real database connections to verify:
1. Reflection: Reading view metadata from database
2. Comparison: Detecting differences between metadata and database
3. Operations: Generating correct CREATE/ALTER/DROP operations
4. Execution: Applying and reverting migrations
5. Filters: Testing include_object and name_filters

Test Design Principles:
- Use real database for integration testing
- Test both upgrade and downgrade paths
- Verify database state after operations
- Test filter configurations
"""


class _EmptyDatabaseAutogenerateBase(fixtures.TestBase):
    """Runs each class in a fresh database of its own, which the tests assume holds nothing else.

    Autogenerate compares the whole schema, and the tests assert on every operation it emits and every
    object reflected back, so tables left in the worker's database by other tests would show up. The
    ``connection`` fixture connects through ``bind``, so the tests run in the class's database.
    """

    __backend__ = True
    __only_on__ = "starrocks"
    __requires__ = ("sqlalchemy_2",)

    @classmethod
    def setup_test_class(cls):
        cls.database = f"{config.db.url.database}_views_autogen"
        with config.db.begin() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS `{cls.database}` FORCE")
            conn.exec_driver_sql(f"CREATE DATABASE `{cls.database}`")
        cls.bind = engines.testing_engine(url=config.db.url.set(database=cls.database), options={"scope": "class"})

    @classmethod
    def teardown_test_class(cls):
        with config.db.begin() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS `{cls.database}` FORCE")


class CreateViewTest(_EmptyDatabaseAutogenerateBase):
    """CREATE VIEW related tests."""

    def test_basic(self, connection) -> None:
        """CREATE: Basic view creation (metadata has, database not)."""
        view_name = "test_create_basic"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        try:
            # 1. Define view in metadata only
            logger.debug("start to define view in metadata.")
            target_metadata = MetaData()
            view = View(view_name, target_metadata, definition="SELECT 1 AS val")
            logger.debug(f"view.info.id: {id(view.info)}, view.info: {view.info}")
            logger.debug("start to create migration context.")
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # 2. Verify CREATE operation generated
            logger.debug("start to verify the CREATE operation generated.")
            assert len(migration_script.upgrade_ops.ops) == 1
            create_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(create_op, CreateViewOp)
            assert create_op.view_name == view_name

            # 3. Apply upgrade
            logger.debug(f"start to apply the upgrade: {create_op}")
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # 4. Verify view exists by reflecting it back
            logger.debug("start to reflect the created view.")
            reflected_metadata = MetaData()
            reflected_metadata.reflect(bind=connection, views=True)
            assert len(reflected_metadata.tables) == 1
            assert view_name in reflected_metadata.tables
            reflected_view = reflected_metadata.tables[view_name]
            logger.debug(f"reflected info: {reflected_view.info}")
            assert_view_definition(reflected_view, "SELECT 1 AS val")

            # 5. Apply downgrade with fresh metadata (simulate real downgrade scenario)
            assert len(migration_script.downgrade_ops.ops) == 1
            drop_op: DropViewOp = migration_script.downgrade_ops.ops[0]
            logger.debug(f"start to apply the downgrade: {drop_op}")

            apply_downgrade_with_fresh_metadata(connection, migration_script)

            logger.debug("start to reflect the tables from downgrade.")
            assert_view_dropped_completely(connection, view_name)
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_with_comment(self, connection) -> None:
        """CREATE: View with comment attribute."""
        view_name = "test_create_comment"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        try:
            target_metadata = MetaData()
            View(view_name, target_metadata,
                      definition="SELECT 1 AS val",
                      comment="Test comment")
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify CREATE with comment
            assert len(migration_script.upgrade_ops.ops) == 1
            create_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(create_op, CreateViewOp)
            assert create_op.comment == "Test comment"

            # Apply and verify
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # Verify by reflecting View object
            reflected_metadata = MetaData()
            reflected_metadata.reflect(bind=connection, only=[view_name], views=True)
            reflected_view = reflected_metadata.tables[view_name]
            assert_view_definition(reflected_view, "SELECT 1 AS val")
            assert reflected_view.comment == "Test comment"

            # Test downgrade (DROP) - verify comment is preserved for recreate
            assert len(migration_script.downgrade_ops.ops) == 1
            drop_op: DropViewOp = migration_script.downgrade_ops.ops[0]
            assert isinstance(drop_op, DropViewOp)
            assert drop_op.view_name == view_name

            # Apply downgrade with fresh metadata
            downgrade_metadata = MetaData()
            mc_downgrade = create_migration_context(connection, downgrade_metadata)
            op_downgrade = Operations(mc_downgrade)
            for op_item in migration_script.downgrade_ops.ops:
                op_downgrade.invoke(op_item)

            # Verify view is dropped
            assert_view_dropped_completely(connection, view_name)
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    @VIEW_SECURITY_NEEDS_3_4_1
    def test_with_security(self, connection):
        """CREATE: View with security attribute."""
        view_name = "test_create_security"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        try:
            target_metadata = MetaData()
            View(view_name, target_metadata,
                      definition="SELECT 1 AS val",
                      starrocks_security="INVOKER")
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify CREATE with security
            assert len(migration_script.upgrade_ops.ops) == 1
            create_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(create_op, CreateViewOp)
            assert create_op.kwargs['starrocks_security'] == "INVOKER"

            # Apply and verify
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # Verify by reflecting View object
            reflected_metadata = MetaData()
            reflected_metadata.reflect(bind=connection, only=[view_name], views=True)
            reflected_view = reflected_metadata.tables[view_name]
            assert_view_definition(reflected_view, "SELECT 1 AS val")
            assert reflected_view.dialect_options[DialectName][TableInfoKey.SECURITY] == "INVOKER"

            # Test downgrade (DROP) - verify security is preserved for recreate
            assert len(migration_script.downgrade_ops.ops) == 1
            drop_op: DropViewOp = migration_script.downgrade_ops.ops[0]
            assert isinstance(drop_op, DropViewOp)
            # Verify reverse operation preserves security. (Don't check the reverse of a downgrade op)
            # reverse_create = drop_op.reverse()
            # assert isinstance(reverse_create, CreateViewOp)
            # assert reverse_create.kwargs['starrocks_security'] == "INVOKER"

            # Apply downgrade with fresh metadata
            downgrade_metadata = MetaData()
            mc_downgrade = create_migration_context(connection, downgrade_metadata)
            op_downgrade = Operations(mc_downgrade)
            for op_item in migration_script.downgrade_ops.ops:
                op_downgrade.invoke(op_item)

            # Verify view is dropped
            assert_view_dropped_completely(connection, view_name)
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_with_columns(self, connection) -> None:
        """CREATE: View with column definitions."""
        view_name = "test_create_columns"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        try:
            target_metadata = MetaData()
            View(view_name, target_metadata,
                      definition="SELECT 1 AS id, 'test' AS name",
                      columns=[
                          {'name': 'id', 'comment': 'ID column'},
                          {'name': 'name', 'comment': 'Name column'}
                      ])
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify CREATE with columns
            assert len(migration_script.upgrade_ops.ops) == 1
            create_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(create_op, CreateViewOp)
            assert len(create_op.columns) == 2

            # Apply and verify
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # Verify by reflecting View object
            reflected_metadata = MetaData()
            reflected_metadata.reflect(bind=connection, only=[view_name], views=True)
            reflected_view = reflected_metadata.tables[view_name]
            assert_view_definition(reflected_view, "SELECT 1 AS id, 'test' AS name")
            assert len(reflected_view.columns) == 2
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    @VIEW_SECURITY_NEEDS_3_4_1
    def test_comprehensive(self, connection):
        """CREATE: View with all attributes (definition + comment + security + columns)."""
        view_name = "test_create_comprehensive"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        try:
            target_metadata = MetaData()
            View(view_name, target_metadata,
                      definition="SELECT 1 AS id, 'test' AS name",
                      comment="Comprehensive test view",
                      starrocks_security="INVOKER",
                      columns=[
                          {'name': 'id', 'comment': 'ID column'},
                          {'name': 'name', 'comment': 'Name column'}
                      ])
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify CREATE with all attributes
            assert len(migration_script.upgrade_ops.ops) == 1
            create_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(create_op, CreateViewOp)
            assert create_op.comment == "Comprehensive test view"
            assert create_op.kwargs['starrocks_security'] == "INVOKER"
            assert len(create_op.columns) == 2

            # Apply and verify
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # Verify by reflecting View object
            reflected_metadata = MetaData()
            reflected_metadata.reflect(bind=connection, only=[view_name], views=True)
            reflected_view = reflected_metadata.tables[view_name]
            assert_view_definition(reflected_view, "SELECT 1 AS id, 'test' AS name")
            assert reflected_view.comment == "Comprehensive test view"
            assert reflected_view.dialect_options[DialectName][TableInfoKey.SECURITY] == "INVOKER"
            assert len(reflected_view.columns) == 2

            # Test downgrade (DROP) - verify all attributes are preserved
            assert len(migration_script.downgrade_ops.ops) == 1
            drop_op: DropViewOp = migration_script.downgrade_ops.ops[0]
            assert isinstance(drop_op, DropViewOp)

            # Verify reverse operation preserves all attributes (don't check the reverse of a downgrade op)
            # reverse_create = drop_op.reverse()
            # assert isinstance(reverse_create, CreateViewOp)
            # assert reverse_create.comment == "Comprehensive test view"
            # assert reverse_create.kwargs['starrocks_security'] == "INVOKER"
            # assert reverse_create.columns is not None
            # assert len(reverse_create.columns) == 2

            # Apply downgrade with fresh metadata
            downgrade_metadata = MetaData()
            mc_downgrade = create_migration_context(connection, downgrade_metadata)
            op_downgrade = Operations(mc_downgrade)
            for op_item in migration_script.downgrade_ops.ops:
                op_downgrade.invoke(op_item)

            # Verify view is dropped
            assert_view_dropped_completely(connection, view_name)
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))


class DropViewTest(_EmptyDatabaseAutogenerateBase):
    """DROP VIEW related tests."""

    def test_basic(self, connection) -> None:
        """DROP: Basic view deletion (database has, metadata not)."""
        view_name = "test_drop_basic"
        # Create view in database first
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"CREATE VIEW {view_name} AS SELECT 1 AS val"))
        try:
            # 1. Empty metadata (no views defined)
            target_metadata = MetaData()
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # 2. Verify DROP operation generated
            assert len(migration_script.upgrade_ops.ops) == 1
            drop_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(drop_op, DropViewOp)
            assert drop_op.view_name == view_name

            # 3. Apply upgrade (drop view)
            logger.debug(f"start to apply upgrade ops: {drop_op}")
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # 4. Verify view dropped
            logger.debug("start to reflect metadata after drop_op.")
            reflected_metadata = MetaData()
            reflected_metadata.reflect(bind=connection, views=True)
            assert view_name not in reflected_metadata.tables

            # 5. Apply downgrade (recreate view) with fresh metadata
            assert len(migration_script.downgrade_ops.ops) == 1
            create_op = migration_script.downgrade_ops.ops[0]
            assert isinstance(create_op, CreateViewOp)
            logger.debug(f"start to apply downgrade ops: {create_op}")

            downgrade_metadata = MetaData()
            mc_downgrade = create_migration_context(connection, downgrade_metadata)
            op_downgrade = Operations(mc_downgrade)
            for op_item in migration_script.downgrade_ops.ops:
                op_downgrade.invoke(op_item)

            reflected_metadata_after = MetaData()
            reflected_metadata_after.reflect(bind=connection, only=[view_name], views=True)
            assert view_name in reflected_metadata_after.tables
            reflected_view = reflected_metadata_after.tables[view_name]
            assert_view_definition(reflected_view, "SELECT 1 AS val")
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    @VIEW_SECURITY_NEEDS_3_4_1
    def test_with_attributes(self, connection):
        """DROP: View with attributes (ensure proper reflection for downgrade)."""
        view_name = "test_drop_attrs"
        # Create view with attributes
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"""
            CREATE VIEW {view_name}
            (id COMMENT 'ID column')
            COMMENT 'View to drop'
            SECURITY INVOKER
            AS SELECT 1 AS id
        """))
        try:
            # Empty metadata
            target_metadata = MetaData()
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify DROP operation
            assert len(migration_script.upgrade_ops.ops) == 1
            drop_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(drop_op, DropViewOp)
            logger.debug(f"drop_op: {drop_op}")

            # Verify downgrade CREATE preserves attributes
            assert len(migration_script.downgrade_ops.ops) == 1
            create_op = migration_script.downgrade_ops.ops[0]
            assert isinstance(create_op, CreateViewOp)
            assert create_op.comment == "View to drop"
            assert create_op.kwargs[TableInfoKeyWithPrefix.SECURITY] == "INVOKER"

            # Apply upgrade (drop view)
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # Verify view is dropped
            reflected_after_drop = MetaData()
            reflected_after_drop.reflect(bind=connection, views=True)
            assert view_name not in reflected_after_drop.tables

            # Apply downgrade (recreate view) with fresh metadata
            downgrade_metadata = MetaData()
            mc_downgrade = create_migration_context(connection, downgrade_metadata)
            op_downgrade = Operations(mc_downgrade)
            for op_item in migration_script.downgrade_ops.ops:
                op_downgrade.invoke(op_item)

            # Verify view is recreated with all attributes
            reflected_after_recreate = MetaData()
            reflected_after_recreate.reflect(bind=connection, only=[view_name], views=True)
            assert view_name in reflected_after_recreate.tables
            recreated_view = reflected_after_recreate.tables[view_name]
            assert 'definition' in recreated_view.info
            assert recreated_view.comment == "View to drop"
            assert recreated_view.dialect_options[DialectName][TableInfoKey.SECURITY] == "INVOKER"
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))


class AlterViewTest(_EmptyDatabaseAutogenerateBase):
    """ALTER VIEW related tests."""

    def test_definition_only(self, connection) -> None:
        """ALTER: Only definition changed."""
        view_name = "test_alter_definition"
        # Create initial view
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"CREATE VIEW {view_name} AS SELECT 1 AS val"))
        try:
            # Define altered view (only definition changed)
            target_metadata = MetaData()
            View(view_name, target_metadata, definition="SELECT 2 AS val")
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify ALTER operation
            assert len(migration_script.upgrade_ops.ops) == 1
            alter_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(alter_op, AlterViewOp)
            assert alter_op.definition is not None
            assert normalize_sql(alter_op.definition) == normalize_sql("SELECT 2 AS val")
            assert alter_op.comment is None  # Not changed
            assert alter_op.security is None  # Not changed

            # Verify reverse operation
            reverse_op = alter_op.reverse()
            assert isinstance(reverse_op, AlterViewOp)
            assert reverse_op.definition is not None
            assert normalize_sql(reverse_op.definition) == normalize_sql("SELECT 1 AS val")

            # Apply and verify
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # Verify by reflecting View object
            reflected_metadata = MetaData()
            reflected_metadata.reflect(bind=connection, only=[view_name], views=True)
            reflected_view = reflected_metadata.tables[view_name]
            assert_view_definition(reflected_view, "SELECT 2 AS val")

            # Test downgrade
            downgrade_metadata = MetaData()
            View(view_name, downgrade_metadata, definition="SELECT 1 AS val")
            mc_downgrade = create_migration_context(connection, downgrade_metadata)
            op_downgrade = Operations(mc_downgrade)
            for op_item in migration_script.downgrade_ops.ops:
                op_downgrade.invoke(op_item)

            # Verify downgrade restored original definition
            reflected_after_downgrade = MetaData()
            reflected_after_downgrade.reflect(bind=connection, only=[view_name], views=True)
            reflected_view_after = reflected_after_downgrade.tables[view_name]
            assert_view_definition(reflected_view_after, "SELECT 1 AS val")
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_a_change_of_case_inside_a_string_literal(self, connection) -> None:
        """StarRocks compares strings by case, so a view filtering on 'Active' and one on 'ACTIVE' differ."""
        view_name = "test_alter_literal_case"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"CREATE VIEW {view_name} AS SELECT 'Active' AS status"))
        try:
            target_metadata = MetaData()
            View(view_name, target_metadata, definition="SELECT 'ACTIVE' AS status")
            migration_script = api.produce_migrations(
                create_migration_context(connection, target_metadata), target_metadata
            )
            assert [(type(op), op.definition) for op in migration_script.upgrade_ops.ops] == [
                (AlterViewOp, "SELECT 'ACTIVE' AS status")
            ]
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_definition_with_columns(self, connection) -> None:
        """ALTER: Definition and columns changed together."""
        view_name = "test_alter_def_cols"
        # Create initial view
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"""
            CREATE VIEW {view_name}
            (id COMMENT 'Old ID')
            AS SELECT 1 AS id
        """))
        try:
            # Define altered view
            target_metadata = MetaData()
            View(view_name, target_metadata,
                      definition="SELECT 2 AS id, 'test' AS name",
                      columns=[
                          {'name': 'id', 'comment': 'New ID'},
                          {'name': 'name', 'comment': 'Name column'}
                      ])
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify ALTER operation
            assert len(migration_script.upgrade_ops.ops) == 1
            alter_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(alter_op, AlterViewOp)
            assert alter_op.definition is not None
            # Columns should be set when definition changes (may be None if not explicitly specified)
            assert alter_op.columns is None or len(alter_op.columns) == 2

            # Apply and verify
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # Verify by reflecting View object
            reflected_metadata = MetaData()
            reflected_metadata.reflect(bind=connection, only=[view_name], views=True)
            reflected_view = reflected_metadata.tables[view_name]
            assert_view_definition(reflected_view, "SELECT 2 AS id, 'test' AS name")

            # Apply downgrade with fresh metadata (simulate real downgrade scenario)
            # In real usage, downgrade runs in a new process with fresh metadata
            # The target_metadata for downgrade should contain the target state (original view definition)
            downgrade_metadata = MetaData()
            View(view_name, downgrade_metadata,
                 definition="SELECT 1 AS id",
                 columns=[{'name': 'id', 'comment': 'Old ID'}])
            mc_downgrade = create_migration_context(connection, downgrade_metadata)
            op_downgrade = Operations(mc_downgrade)
            for op_item in migration_script.downgrade_ops.ops:
                op_downgrade.invoke(op_item)

            reflected_metadata_after = MetaData()
            reflected_metadata_after.reflect(bind=connection, only=[view_name], views=True)
            reflected_view_after = reflected_metadata_after.tables[view_name]
            assert 'definition' in reflected_view_after.info
            assert normalize_sql("SELECT 1 AS id") == normalize_sql(reflected_view_after.info['definition'])
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    @staticmethod
    def _apply_and_reproduce(connection, mc, target_metadata, migration_script):
        """Apply the upgrade operations, then produce the migration again; it has nothing left to change."""
        op = Operations(mc)
        for op_item in migration_script.upgrade_ops.ops:
            op.invoke(op_item)
        return api.produce_migrations(create_migration_context(connection, target_metadata), target_metadata)

    @testing.combinations(
        ("column_comment", "", [{"name": "a", "comment": "new"}, {"name": "b"}]),
        ("column_names", "", [{"name": "x"}, {"name": "y"}]),
        ("column_comment_with_view_comment", "COMMENT 'kept'", [{"name": "a", "comment": "new"}, {"name": "b"}]),
        ("column_names_with_view_comment", "COMMENT 'kept'", [{"name": "x"}, {"name": "y"}]),
        argnames="view_comment, columns",
        id_="iaa",
    )
    def test_columns_only_change(self, connection, view_comment, columns) -> None:
        """ALTER: Columns changed with the same definition, generated and applied, with the comment kept."""
        view_name = "test_alter_columns_only"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"CREATE VIEW {view_name} (a COMMENT 'old', b) {view_comment} AS SELECT 1 AS a, 2 AS b"))
        try:
            target_metadata = MetaData()
            View(
                view_name,
                target_metadata,
                definition="SELECT 1 AS a, 2 AS b",
                columns=columns,
                comment="kept" if view_comment else None,
            )
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)
            assert len(migration_script.upgrade_ops.ops) == 1
            alter_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(alter_op, AlterViewOp)
            assert normalize_sql(alter_op.definition) == normalize_sql("SELECT 1 AS a, 2 AS b")

            again = self._apply_and_reproduce(connection, mc, target_metadata, migration_script)
            assert again.upgrade_ops.ops == []
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_comment_change(self, connection) -> None:
        """ALTER: Only comment changed, applied by recreating the view, and nothing is left to change."""
        view_name = "test_alter_comment"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"CREATE VIEW {view_name} COMMENT 'Old comment' AS SELECT 1 AS val"))
        try:
            target_metadata = MetaData()
            View(view_name, target_metadata, definition="SELECT 1 AS val", comment="New comment")
            mc = create_migration_context(connection, target_metadata)
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                migration_script = api.produce_migrations(mc, target_metadata)
                assert len(w) == 0

            assert len(migration_script.upgrade_ops.ops) == 1
            alter_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(alter_op, AlterViewOp)
            assert alter_op.definition is None  # Not changed
            assert alter_op.comment == "New comment"  # Changed
            assert alter_op.security is None  # Not changed
            assert normalize_sql(alter_op.existing_definition) == normalize_sql("SELECT 1 AS val")

            again = self._apply_and_reproduce(connection, mc, target_metadata, migration_script)
            assert again.upgrade_ops.ops == []

            downgrade_metadata = MetaData()
            View(view_name, downgrade_metadata, definition="SELECT 1 AS val", comment="Old comment")
            mc_downgrade = create_migration_context(connection, downgrade_metadata)
            for op_item in migration_script.downgrade_ops.ops:
                Operations(mc_downgrade).invoke(op_item)
            assert api.produce_migrations(mc_downgrade, downgrade_metadata).upgrade_ops.ops == []
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_definition_change_keeps_the_comment(self, connection) -> None:
        """ALTER: A new definition keeps the comment, which ALTER VIEW ... AS alone would drop."""
        view_name = "test_alter_definition_comment"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"CREATE VIEW {view_name} COMMENT 'Kept comment' AS SELECT 1 AS val"))
        try:
            target_metadata = MetaData()
            View(view_name, target_metadata, definition="SELECT 2 AS val", comment="Kept comment")
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)
            assert len(migration_script.upgrade_ops.ops) == 1

            again = self._apply_and_reproduce(connection, mc, target_metadata, migration_script)
            assert again.upgrade_ops.ops == []
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_comment_removed(self, connection) -> None:
        """ALTER: A comment the model no longer has is removed."""
        view_name = "test_alter_comment_removed"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"CREATE VIEW {view_name} COMMENT 'Gone' AS SELECT 1 AS val"))
        try:
            target_metadata = MetaData()
            View(view_name, target_metadata, definition="SELECT 1 AS val")
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)
            assert len(migration_script.upgrade_ops.ops) == 1

            again = self._apply_and_reproduce(connection, mc, target_metadata, migration_script)
            assert again.upgrade_ops.ops == []
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    @VIEW_SECURITY_NEEDS_3_4_1
    def test_security_change(self, connection):
        """ALTER: Only security changed, applied with ALTER VIEW ... SET SECURITY, and nothing is left to change."""
        view_name = "test_alter_security"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"CREATE VIEW {view_name} SECURITY NONE AS SELECT 1 AS val"))
        try:
            target_metadata = MetaData()
            View(view_name, target_metadata, definition="SELECT 1 AS val", starrocks_security="INVOKER")
            mc = create_migration_context(connection, target_metadata)
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                migration_script = api.produce_migrations(mc, target_metadata)
                assert len(w) == 0

            assert len(migration_script.upgrade_ops.ops) == 1
            alter_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(alter_op, AlterViewOp)
            assert alter_op.definition is None  # Not changed
            assert alter_op.comment is None  # Not changed
            assert alter_op.security == "INVOKER"  # Changed

            again = self._apply_and_reproduce(connection, mc, target_metadata, migration_script)
            assert again.upgrade_ops.ops == []

            downgrade_metadata = MetaData()
            View(view_name, downgrade_metadata, definition="SELECT 1 AS val", starrocks_security="NONE")
            mc_downgrade = create_migration_context(connection, downgrade_metadata)
            for op_item in migration_script.downgrade_ops.ops:
                Operations(mc_downgrade).invoke(op_item)
            assert api.produce_migrations(mc_downgrade, downgrade_metadata).upgrade_ops.ops == []
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    @VIEW_SECURITY_NEEDS_3_4_1
    def test_definition_and_security_change_together(self, connection):
        """ALTER: A new definition and a new security both apply, and the comment survives."""
        view_name = "test_alter_definition_security"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"CREATE VIEW {view_name} COMMENT 'c' SECURITY NONE AS SELECT 1 AS val"))
        try:
            target_metadata = MetaData()
            View(view_name, target_metadata, definition="SELECT 2 AS val", comment="c", starrocks_security="INVOKER")
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            again = self._apply_and_reproduce(connection, mc, target_metadata, migration_script)
            assert again.upgrade_ops.ops == []
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    @VIEW_SECURITY_NEEDS_3_4_1
    def test_multiple_attributes(self, connection):
        """ALTER: Multiple attributes changed simultaneously."""
        view_name = "test_alter_multiple"
        # Create initial view
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"""
            CREATE VIEW {view_name}
            (c1 COMMENT 'col 1')
            COMMENT 'Initial version'
            SECURITY INVOKER
            AS SELECT 1 AS c1
            """))
        try:
            # Define altered view
            target_metadata = MetaData()
            View(view_name, target_metadata,
                definition="SELECT 2 AS new_c1, 3 AS new_c2",
                comment="Altered version",
                      starrocks_security="INVOKER",
                columns=[
                    {'name': 'new_c1', 'comment': 'new col 1'},
                    {'name': 'new_c2', 'comment': 'new col 2'}
                ])
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify ALTER operation
            assert len(migration_script.upgrade_ops.ops) == 1
            alter_op = migration_script.upgrade_ops.ops[0]
            assert isinstance(alter_op, AlterViewOp)
            # Verify definition changed
            assert alter_op.definition is not None
            assert normalize_sql(alter_op.definition) == normalize_sql("SELECT 2 AS new_c1, 3 AS new_c2")
            # Verify comment changed
            assert alter_op.comment == "Altered version"
            # Verify security unchanged (should be None in alter_op)
            assert alter_op.security is None

            # Verify reverse operation
            reverse_op = alter_op.reverse()
            assert isinstance(reverse_op, AlterViewOp)
            assert normalize_sql(reverse_op.definition) == normalize_sql("SELECT 1 AS c1")
            assert reverse_op.comment == "Initial version"

            # Apply and verify
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # Verify by reflecting View object
            reflected_metadata = MetaData()
            reflected_metadata.reflect(bind=connection, only=[view_name], views=True)
            reflected_view = reflected_metadata.tables[view_name]
            assert_view_definition(reflected_view, "SELECT 2 AS new_c1, 3 AS new_c2")

            # Apply downgrade with fresh metadata (simulate real downgrade scenario)
            # The target_metadata should contain the target state (original view)
            downgrade_metadata = MetaData()
            View(view_name, downgrade_metadata,
                 definition="SELECT 1 AS c1",
                 comment="Initial version",
                 starrocks_security="INVOKER",
                 columns=[{'name': 'c1', 'comment': 'col 1'}])
            mc_downgrade = create_migration_context(connection, downgrade_metadata)
            op_downgrade = Operations(mc_downgrade)
            for op_item in migration_script.downgrade_ops.ops:
                op_downgrade.invoke(op_item)

            reflected_metadata_after = MetaData()
            reflected_metadata_after.reflect(bind=connection, only=[view_name], views=True)
            reflected_view_after = reflected_metadata_after.tables[view_name]
            assert 'definition' in reflected_view_after.info
            assert normalize_sql("SELECT 1 AS c1") == normalize_sql(reflected_view_after.info['definition'])
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))


class IdempotencyTest(_EmptyDatabaseAutogenerateBase):
    """Idempotency tests."""

    def test_no_change(self, connection) -> None:
        """IDEMPOTENCY: No operations when metadata matches database."""
        view_name = "test_no_change"
        # Create view in database
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"CREATE VIEW {view_name} AS SELECT 1 AS val"))
        try:
            # Define identical view in metadata
            target_metadata = MetaData()
            View(view_name, target_metadata, definition="SELECT 1 AS val")
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify no operations generated
            assert len(migration_script.upgrade_ops.ops) == 0
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_whitespace_ignored(self, connection) -> None:
        """IDEMPOTENCY: Whitespace/formatting differences should be ignored."""
        view_name = "test_whitespace"
        # Create view with specific formatting
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"CREATE VIEW {view_name} AS SELECT   1   AS   val"))
        try:
            # Define view with different whitespace
            target_metadata = MetaData()
            View(view_name, target_metadata, definition="SELECT 1 AS val")
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify no operations generated (whitespace normalized)
            assert len(migration_script.upgrade_ops.ops) == 0
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))


class MultipleViewsTest(_EmptyDatabaseAutogenerateBase):
    """Multiple views operations tests."""

    def test_create(self, connection) -> None:
        """MULTIPLE: Create new views while keeping existing ones."""
        existing_view = "test_multi_existing"
        new_views = ["test_multi_1", "test_multi_2"]
        all_views = [existing_view] + new_views

        # Clean up
        for vname in all_views:
            connection.execute(text(f"DROP VIEW IF EXISTS {vname}"))

        # Create one existing view in database
        connection.execute(text(f"CREATE VIEW {existing_view} AS SELECT 0 AS existing_val"))

        try:
            # Define all views in metadata (existing + new)
            target_metadata = MetaData()
            View(existing_view, target_metadata, definition="SELECT 0 AS existing_val")
            for vname in new_views:
                View(vname, target_metadata, definition="SELECT 1 AS val")

            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Should only CREATE the 2 new views, not the existing one
            assert len(migration_script.upgrade_ops.ops) == 2
            created_names = {op.view_name for op in migration_script.upgrade_ops.ops}
            assert created_names == set(new_views)
            for op_item in migration_script.upgrade_ops.ops:
                assert isinstance(op_item, CreateViewOp)

            # Apply and verify
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # Verify all views exist with correct definitions
            reflected_metadata = MetaData()
            reflected_metadata.reflect(bind=connection, views=True)
            assert len(reflected_metadata.tables) == 3
            for vname in all_views:
                assert vname in reflected_metadata.tables
                reflected_view = reflected_metadata.tables[vname]
                if vname == existing_view:
                    assert_view_definition(reflected_view, "SELECT 0 AS existing_val")
                else:
                    assert_view_definition(reflected_view, "SELECT 1 AS val")
        finally:
            for vname in all_views:
                connection.execute(text(f"DROP VIEW IF EXISTS {vname}"))

    def test_drop(self, connection) -> None:
        """MULTIPLE: Drop some views while keeping others."""
        views_to_drop = ["test_drop_multi_1", "test_drop_multi_2"]
        view_to_keep = "test_keep_multi"
        all_views = views_to_drop + [view_to_keep]

        # Create all views in database
        for vname in all_views:
            connection.execute(text(f"DROP VIEW IF EXISTS {vname}"))
            connection.execute(text(f"CREATE VIEW {vname} AS SELECT 1 AS val"))
        try:
            # Metadata only contains the view to keep
            target_metadata = MetaData()
            View(view_to_keep, target_metadata, definition="SELECT 1 AS val")

            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Should only DROP the 2 views not in metadata
            assert len(migration_script.upgrade_ops.ops) == 2
            dropped_names = {op.view_name for op in migration_script.upgrade_ops.ops}
            assert dropped_names == set(views_to_drop)
            for op_item in migration_script.upgrade_ops.ops:
                assert isinstance(op_item, DropViewOp)

            # Apply and verify
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # Verify dropped views are gone, kept view remains
            reflected_metadata = MetaData()
            reflected_metadata.reflect(bind=connection, views=True)
            assert len(reflected_metadata.tables) == 1
            assert view_to_keep in reflected_metadata.tables
            for vname in views_to_drop:
                assert vname not in reflected_metadata.tables
        finally:
            for vname in all_views:
                connection.execute(text(f"DROP VIEW IF EXISTS {vname}"))

    def test_mixed_operations(self, connection) -> None:
        """MULTIPLE: Mixed CREATE/ALTER/DROP operations."""
        view_to_create = "test_mixed_create"
        view_to_alter = "test_mixed_alter"
        view_to_drop = "test_mixed_drop"

        # Setup: create views for alter and drop
        connection.execute(text(f"DROP VIEW IF EXISTS {view_to_create}"))
        connection.execute(text(f"DROP VIEW IF EXISTS {view_to_alter}"))
        connection.execute(text(f"DROP VIEW IF EXISTS {view_to_drop}"))
        connection.execute(text(f"CREATE VIEW {view_to_alter} AS SELECT 1 AS val"))
        connection.execute(text(f"CREATE VIEW {view_to_drop} AS SELECT 1 AS val"))

        try:
            # Define metadata: create new, alter existing, omit one (to drop)
            target_metadata = MetaData()
            View(view_to_create, target_metadata, definition="SELECT 1 AS new_val")
            View(view_to_alter, target_metadata, definition="SELECT 2 AS altered_val")
            # view_to_drop not in metadata

            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify mixed operations
            assert len(migration_script.upgrade_ops.ops) == 3
            op_types = {type(op).__name__ for op in migration_script.upgrade_ops.ops}
            assert 'CreateViewOp' in op_types
            assert 'AlterViewOp' in op_types
            assert 'DropViewOp' in op_types

            # Apply and verify
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # Verify by reflecting View objects
            reflected_metadata = MetaData()
            reflected_metadata.reflect(bind=connection, views=True)
            assert view_to_create in reflected_metadata.tables
            assert 'definition' in reflected_metadata.tables[view_to_create].info
            assert view_to_alter in reflected_metadata.tables
            assert 'definition' in reflected_metadata.tables[view_to_alter].info
            assert view_to_drop not in reflected_metadata.tables
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_to_create}"))
            connection.execute(text(f"DROP VIEW IF EXISTS {view_to_alter}"))
            connection.execute(text(f"DROP VIEW IF EXISTS {view_to_drop}"))


    def test_views_with_existing_tables(self, connection) -> None:
        """
        MULTIPLE: View operations should not affect existing tables.

        This test verifies that:
        1. When a table exists in both DB and metadata, no ALTER is generated
        2. View operations (CREATE) work correctly alongside tables
        3. include_object_for_view_mv correctly handles both tables and views
        4. Tables remain unaffected by view operations
        """
        table_name = "test_existing_table"
        view_name = "test_view_with_table"

        # Clean up
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

        # Create a table in database (with StarRocks-specific properties)
        connection.execute(text(f"""
            CREATE TABLE {table_name} (
                id INT,
                name STRING
            ) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """))

        try:
            # Define both table and view in metadata
            # Table definition matches DB, so no ALTER should be generated
            # View doesn't exist in DB, so CREATE should be generated
            target_metadata = MetaData()
            Table(table_name, target_metadata,
                  Column('id', INTEGER),
                  Column('name', STRING),
                  starrocks_distributed_by='HASH(id)  BUCKETS 1',
                  starrocks_properties={'replication_num': '1'})
            View(view_name, target_metadata, definition="SELECT 1 AS val")

            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Should only generate CREATE for view
            # No operations for table (already exists and matches metadata)
            assert len(migration_script.upgrade_ops.ops) == 1
            view_ops = [op for op in migration_script.upgrade_ops.ops
                       if isinstance(op, CreateViewOp)]
            assert len(view_ops) == 1, "Should generate CREATE for view"
            assert view_ops[0].view_name == view_name

            # Verify no ALTER operations for table
            from alembic.operations.ops import CreateTableOp, DropTableOp
            table_ops = [op for op in migration_script.upgrade_ops.ops
                        if isinstance(op, (CreateTableOp, DropTableOp, AlterTableOp, ModifyTableOps))]
            assert len(table_ops) == 0, \
                "Table already exists and matches metadata, no operations should be generated"

            # Apply view creation
            op = Operations(mc)
            for op_item in migration_script.upgrade_ops.ops:
                op.invoke(op_item)

            # Verify view was created
            reflected_views = MetaData()
            reflected_views.reflect(bind=connection, views=True)
            assert view_name in reflected_views.tables

            # Verify table still exists and wasn't modified
            reflected_tables = MetaData()
            reflected_tables.reflect(bind=connection, views=False)
            assert table_name in reflected_tables.tables
            table_obj = reflected_tables.tables[table_name]
            assert 'id' in table_obj.c
            assert 'name' in table_obj.c
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))


class FiltersTest(_EmptyDatabaseAutogenerateBase):
    """Filter configuration tests."""

    def test_include_object_basic(self, connection) -> None:
        """
        FILTER: Basic test that include_object_for_view_mv allows views.

        This test verifies that when using include_object_for_view_mv,
        view operations are correctly generated. This is the baseline test
        showing the filter accepts views.
        """
        view_name = "test_filter_basic"
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        try:
            # Define view in metadata
            target_metadata = MetaData()
            View(view_name, target_metadata, definition="SELECT 1 AS val")

            # Use include_object_for_view_mv
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify view operation generated
            assert len(migration_script.upgrade_ops.ops) == 1
            assert isinstance(migration_script.upgrade_ops.ops[0], CreateViewOp)
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_include_object_excludes_tables(self, connection) -> None:
        """
        FILTER: Verify include_object_for_view_mv filters out views/MVs from table comparisons.

        DIFFERENCE from test_include_object_basic:
        - test_include_object_basic: Tests that views are processed by view hooks
        - THIS test: Tests that views are NOT processed by table comparison hooks

        IMPORTANT: include_object_for_view_mv's purpose is to:
        1. EXCLUDE views/MVs from TABLE comparison (type_=="table")
        2. ALLOW everything else (tables, indexes, etc.)

        This prevents views from being treated as tables during reflection,
        which would cause errors since views don't have the same properties as tables.
        """
        table_name = "test_filter_table"
        view_name = "test_filter_view"

        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        # Create view in database
        connection.execute(text(f"CREATE VIEW {view_name} AS SELECT 1 AS val"))

        try:
            # Define only table in metadata (no view)
            # This simulates: view exists in DB, not in metadata
            target_metadata = MetaData()
            Table(table_name, target_metadata, Column('id', INTEGER))

            # Use include_object_for_view_mv
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)

            # The view in DB should be detected as a DROP operation
            # (because it's not in metadata)
            drop_ops = [op for op in migration_script.upgrade_ops.ops if isinstance(op, DropViewOp)]
            assert len(drop_ops) == 1, "View in DB but not in metadata should generate DropViewOp"
            assert drop_ops[0].view_name == view_name

            # Table should also generate CREATE operation
            # (include_object_for_view_mv does NOT exclude tables from being processed)
            from alembic.operations.ops import CreateTableOp
            table_ops = [op for op in migration_script.upgrade_ops.ops if isinstance(op, CreateTableOp)]
            assert len(table_ops) == 1, "Tables should still be processed normally"
        finally:
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_custom_include_object_handles_both(self, connection) -> None:
        """FILTER: Custom include_object handling both tables and views."""
        table_name = "test_custom_table"
        view_name = "test_custom_view"

        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

        try:
            # Define both in metadata
            target_metadata = MetaData()
            Table(table_name, target_metadata, Column('id', INTEGER))
            View(view_name, target_metadata, definition="SELECT 1 AS val")

            # Custom filter that handles both
            def custom_include_object(object, name, type_, reflected, compare_to):
                # First apply dialect filter
                if not include_object_for_view_mv(object, name, type_, reflected, compare_to):
                    return False
                # Then custom logic (accept all in this test)
                return True

            mc = MigrationContext.configure(
                connection=connection,
                opts={
                    'target_metadata': target_metadata,
                    'include_object': custom_include_object
                }
            )
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify view operation generated
            view_ops = [op for op in migration_script.upgrade_ops.ops if isinstance(op, CreateViewOp)]
            assert len(view_ops) == 1
        finally:
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_custom_include_object_excludes_pattern(self, connection) -> None:
        """FILTER: Custom filter excluding specific view patterns."""
        view_included = "test_prod_view"
        view_excluded = "tmp_test_view"

        connection.execute(text(f"DROP VIEW IF EXISTS {view_included}"))
        connection.execute(text(f"DROP VIEW IF EXISTS {view_excluded}"))

        try:
            # Define both views in metadata
            target_metadata = MetaData()
            View(view_included, target_metadata, definition="SELECT 1 AS val")
            View(view_excluded, target_metadata, definition="SELECT 2 AS val")

            # Custom filter excluding tmp_ views
            def custom_include_object(object, name, type_, reflected, compare_to):
                if not include_object_for_view_mv(object, name, type_, reflected, compare_to):
                    return False
                # Exclude views starting with tmp_
                if type_ == "view" and name.startswith("tmp_"):
                    return False
                return True

            mc = MigrationContext.configure(
                connection=connection,
                opts={
                    'target_metadata': target_metadata,
                    'include_object': custom_include_object
                }
            )
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify only non-tmp view included
            view_ops = [op for op in migration_script.upgrade_ops.ops if isinstance(op, CreateViewOp)]
            assert len(view_ops) == 1
            assert view_ops[0].view_name == view_included
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_included}"))
            connection.execute(text(f"DROP VIEW IF EXISTS {view_excluded}"))

    def test_name_filter_includes_pattern(self, connection) -> None:
        """FILTER: include_name filters views by name pattern."""
        view_included = "public_view"
        view_excluded = "private_view"

        connection.execute(text(f"DROP VIEW IF EXISTS {view_included}"))
        connection.execute(text(f"DROP VIEW IF EXISTS {view_excluded}"))

        try:
            # Define both views
            target_metadata = MetaData()
            View(view_included, target_metadata, definition="SELECT 1 AS val")
            View(view_excluded, target_metadata, definition="SELECT 2 AS val")

            # Filter by name pattern
            def include_name_filter(name, type_, parent_names):
                if type_ == "view":
                    return name.startswith("public_")
                return True

            mc = MigrationContext.configure(
                connection=connection,
                opts={
                    'target_metadata': target_metadata,
                    'include_object': include_object_for_view_mv,
                    'include_name': include_name_filter
                }
            )
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify only public_ view included
            view_ops = [op for op in migration_script.upgrade_ops.ops if isinstance(op, CreateViewOp)]
            assert len(view_ops) == 1
            assert view_ops[0].view_name == view_included
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_included}"))
            connection.execute(text(f"DROP VIEW IF EXISTS {view_excluded}"))

    def test_name_filter_excludes_pattern(self, connection) -> None:
        """FILTER: include_name excludes views by pattern."""
        view_names = ["keep_view_1", "tmp_view_1", "keep_view_2"]

        for vname in view_names:
            connection.execute(text(f"DROP VIEW IF EXISTS {vname}"))

        try:
            # Define all views
            target_metadata = MetaData()
            for vname in view_names:
                View(vname, target_metadata, definition="SELECT 1 AS val")

            # Exclude tmp_ views
            def include_name_filter(name, type_, parent_names):
                if type_ == "view":
                    return not name.startswith("tmp_")
                return True

            mc = MigrationContext.configure(
                connection=connection,
                opts={
                    'target_metadata': target_metadata,
                    'include_object': include_object_for_view_mv,
                    'include_name': include_name_filter
                }
            )
            migration_script = api.produce_migrations(mc, target_metadata)

            # Verify tmp_ view excluded
            view_ops = [op for op in migration_script.upgrade_ops.ops if isinstance(op, CreateViewOp)]
            assert len(view_ops) == 2
            created_names = {op.view_name for op in view_ops}
            assert "tmp_view_1" not in created_names
            assert "keep_view_1" in created_names
            assert "keep_view_2" in created_names
        finally:
            for vname in view_names:
                connection.execute(text(f"DROP VIEW IF EXISTS {vname}"))


class CanonicalizationNoPhantomDiffTest(_EmptyDatabaseAutogenerateBase):
    """Regression tests for StarRocks view-definition canonicalization (before 4.0.7).

    On clusters before 4.0.7, StarRocks rewrites a view definition into its own
    canonical form when it is stored, so the SQL the user wrote in their model differs
    syntactically from what is reflected back. Each test creates the view in the database
    using the *model* SQL, declares the identical view in the metadata, and asserts that
    autogenerate detects NO change (no phantom migration).

    See starrocks.common.sql_canonical.canonicalize_sql (the AST-based canonicalizer that
    reconciles these rewrites when the temp-view round-trip is unavailable).
    """

    def _assert_no_phantom_diff(self, connection: Connection, view_name: str, model_template: str) -> None:
        """Create a view from ``model_template`` and assert autogenerate finds no change.

        ``model_template`` may reference the per-test base table via ``{src}``. A unique base
        table name is used so tests running in parallel cannot clobber each other.
        """
        base_table = f"{view_name}_src"
        model_definition = model_template.format(src=base_table)
        connection.execute(text(f"DROP TABLE IF EXISTS {base_table}"))
        connection.execute(text(
            f"CREATE TABLE {base_table} "
            f"(id INT, val INT, cat VARCHAR(20), tags ARRAY<INT>) "
            f"PROPERTIES ('replication_num'='1')"
        ))
        connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
        # Create the view in the DB exactly as the user wrote it in their model.
        connection.execute(text(f"CREATE VIEW {view_name} AS {model_definition}"))
        try:
            target_metadata = MetaData()
            View(view_name, target_metadata, definition=model_definition)
            mc = create_migration_context(connection, target_metadata)
            migration_script = api.produce_migrations(mc, target_metadata)
            # Scope to ops for THIS view only, so unrelated objects left in the test
            # schema cannot affect the result.
            view_ops = [
                op for op in migration_script.upgrade_ops.ops
                if isinstance(op, (AlterViewOp, CreateViewOp, DropViewOp))
                and getattr(op, "view_name", None) == view_name
            ]
            assert len(view_ops) == 0, (
                f"Phantom diff detected for {view_name!r}.\n"
                f"Model definition: {model_definition}\n"
                f"Generated ops: {view_ops}"
            )
        finally:
            connection.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
            connection.execute(text(f"DROP TABLE IF EXISTS {base_table}"))

    def test_plain_join_becomes_inner_join(self, connection) -> None:
        """A plain JOIN is stored as INNER JOIN with 'AS' table aliases."""
        self._assert_no_phantom_diff(
            connection,
            "canon_join",
            "SELECT a.id, b.val FROM {src} a JOIN {src} b ON a.id = b.id",
        )

    def test_left_join_becomes_left_outer_join(self, connection) -> None:
        """A LEFT JOIN is stored as LEFT OUTER JOIN."""
        self._assert_no_phantom_diff(
            connection,
            "canon_ljoin",
            "SELECT a.id, b.val FROM {src} a LEFT JOIN {src} b ON a.id = b.id",
        )

    def test_table_alias_without_as(self, connection) -> None:
        """A table alias written without AS is stored as 'src AS a'."""
        self._assert_no_phantom_diff(
            connection,
            "canon_alias",
            "SELECT s.id, s.val FROM {src} s WHERE s.val > 10",
        )

    def test_case_when_predicate_parenthesized(self, connection) -> None:
        """StarRocks wraps the CASE WHEN predicate in redundant parentheses."""
        self._assert_no_phantom_diff(
            connection,
            "canon_case",
            "SELECT id, CASE WHEN val > 100 THEN 1 ELSE 0 END AS flag FROM {src}",
        )

    def test_where_predicate(self, connection) -> None:
        """A WHERE comparison may be stored parenthesized."""
        self._assert_no_phantom_diff(
            connection,
            "canon_where",
            "SELECT id FROM {src} WHERE val > 100",
        )

    def test_cte_column_list_injected(self, connection) -> None:
        """StarRocks injects a CTE column list after the CTE name."""
        self._assert_no_phantom_diff(
            connection,
            "canon_cte",
            "SELECT cat, n FROM (SELECT cat, count(*) AS n FROM {src} GROUP BY cat) t",
        )

    def test_unnest_with_lateral_and_table_function_alias(self, connection) -> None:
        """The exact bug-report shape: unnest + table-function alias (stored with LATERAL)."""
        self._assert_no_phantom_diff(
            connection,
            "canon_unnest",
            "SELECT id, t FROM {src}, unnest(tags) AS k(t)",
        )

    def test_percent_sign_in_a_literal(self, connection) -> None:
        """A ``%`` reaches the temporary view as written, which is the only comparison that holds before 4.0.7."""
        self._assert_no_phantom_diff(
            connection,
            "canon_pct",
            "SELECT id, cat FROM {src} WHERE cat LIKE 'x%' AND cat <> 'it''s'",
        )


class TemporaryViewCanonicalizationTest(_EmptyDatabaseAutogenerateBase):
    """The temporary view through which StarRocks canonicalizes a model's definition for the comparison."""

    def test_a_percent_sign_reaches_starrocks_as_written(self, connection) -> None:
        connection.execute(text("CREATE TABLE pct_src (id INT, cat VARCHAR(20)) PROPERTIES ('replication_num'='1')"))
        try:
            canonical = _get_canonical_sql_via_temp_view(
                connection, self.database, "SELECT id, date_format(now(), '%Y-%m') AS ym FROM pct_src WHERE cat LIKE 'x%'"
            )
            assert canonical is not None and "'%Y-%m'" in canonical and "'x%'" in canonical, canonical
        finally:
            connection.execute(text("DROP TABLE IF EXISTS pct_src"))

    _JOINED = "SELECT dup_a.id, dup_b.id FROM dup_a JOIN dup_b ON dup_a.id = dup_b.id"

    def _view_ops_for_a_column_list_naming_repeated_names(self, connection, model_definition):
        """Autogenerate a view whose SELECT list names ``id`` twice, which only its column list names apart."""
        for name in ("dup_a", "dup_b"):
            connection.execute(text(f"CREATE TABLE IF NOT EXISTS {name} (id INT) PROPERTIES ('replication_num'='1')"))
        connection.execute(text("DROP VIEW IF EXISTS v_dup"))
        connection.execute(text(f"CREATE VIEW v_dup (id1, id2) AS {self._JOINED}"))
        try:
            target_metadata = MetaData()
            View("v_dup", target_metadata, definition=model_definition, columns=[{"name": "id1"}, {"name": "id2"}])
            context = MigrationContext.configure(
                connection=connection,
                opts={
                    "target_metadata": target_metadata,
                    "include_object": combine_include_object(
                        lambda object, name, type_, reflected, compare_to: type_ != "table"
                    ),
                },
            )
            script = api.produce_migrations(context, target_metadata)
            return [type(op) for op in script.upgrade_ops.ops]
        finally:
            connection.execute(text("DROP VIEW IF EXISTS v_dup"))
            for name in ("dup_a", "dup_b"):
                connection.execute(text(f"DROP TABLE IF EXISTS {name}"))

    def test_an_unchanged_view_whose_column_list_names_repeated_names_apart_is_compared(self, connection, caplog):
        assert self._view_ops_for_a_column_list_naming_repeated_names(connection, self._JOINED) == []
        assert "could not canonicalize" not in caplog.text

    def test_a_changed_view_whose_column_list_names_repeated_names_apart_is_altered(self, connection):
        """Before 4.0.7 only the temporary view tells the change, and only under the column list."""
        changed = f"{self._JOINED} WHERE dup_a.id > 5"
        assert self._view_ops_for_a_column_list_naming_repeated_names(connection, changed) == [AlterViewOp]


class ViewDefinitionWithoutTempViewTest(_EmptyDatabaseAutogenerateBase):
    """With no temporary view, as for a user without CREATE VIEW, only the in-process normalizer compares definitions."""

    _VIEW, _TABLE = "v_not_compared", "t_not_compared"

    def _ops_without_the_temp_view(self, connection, model_definition):
        """Autogenerate against a view with a temp-view schema that does not exist; tables are left out."""
        connection.execute(text(f"DROP VIEW IF EXISTS {self._VIEW}"))
        connection.execute(text(f"DROP TABLE IF EXISTS {self._TABLE}"))
        connection.execute(text(f"CREATE TABLE {self._TABLE} (id INT, a INT) PROPERTIES ('replication_num' = '1')"))
        connection.execute(text(f"CREATE VIEW {self._VIEW} AS SELECT id, a FROM {self._TABLE}"))
        target_metadata = MetaData()
        View(self._VIEW, target_metadata, definition=model_definition)
        context = MigrationContext.configure(
            connection=connection,
            opts={
                "target_metadata": target_metadata,
                "include_object": combine_include_object(
                    lambda object, name, type_, reflected, compare_to: type_ != "table"
                ),
                "starrocks_temp_view_schema": "no_such_schema_for_temp_views",
            },
        )
        return [type(op) for op in api.produce_migrations(context, target_metadata).upgrade_ops.ops]

    def _drop(self, connection):
        connection.execute(text(f"DROP VIEW IF EXISTS {self._VIEW}"))
        connection.execute(text(f"DROP TABLE IF EXISTS {self._TABLE}"))

    def test_an_unchanged_view_is_left_alone(self, connection) -> None:
        """Before 4.0.7 the stored definition has table-qualified columns, which the normalizer reads as a change."""
        try:
            assert self._ops_without_the_temp_view(connection, f"SELECT id, a FROM {self._TABLE}") == []
        finally:
            self._drop(connection)

    @VIEW_TEXT_COMPARED_FROM_4_0_7
    def test_a_changed_definition_starrocks_cannot_compare_is_warned_about(self, connection, caplog):
        try:
            assert self._ops_without_the_temp_view(connection, f"SELECT id, a + 1 AS a FROM {self._TABLE}") == []
            assert "could not canonicalize them through a temporary view" in caplog.text
        finally:
            self._drop(connection)

    @VIEW_TEXT_NOT_COMPARED_BEFORE_4_0_7
    def test_a_changed_definition_is_altered_from_the_text_from_4_0_7(self, connection):
        try:
            ops_ = self._ops_without_the_temp_view(connection, f"SELECT id, a + 1 AS a FROM {self._TABLE}")
            assert ops_ == [AlterViewOp]
        finally:
            self._drop(connection)

    def _ops_for(self, connection, target_metadata, tables):
        """Autogenerate without the temporary view, comparing only the tables named in ``tables``."""
        context = MigrationContext.configure(
            connection=connection,
            opts={
                "target_metadata": target_metadata,
                "include_object": combine_include_object(
                    lambda object, name, type_, reflected, compare_to: type_ != "table" or name in tables
                ),
                "starrocks_temp_view_schema": "no_such_schema_for_temp_views",
            },
        )
        return [type(op).__name__ for op in api.produce_migrations(context, target_metadata).upgrade_ops.ops]

    def test_a_view_reading_a_column_the_revision_drops_is_redefined(self, connection) -> None:
        """The model cannot read a column it removes, so the definition did change; the view would break."""
        connection.execute(text(f"DROP VIEW IF EXISTS {self._VIEW}"))
        connection.execute(text(f"DROP TABLE IF EXISTS {self._TABLE}"))
        connection.execute(text(
            f"CREATE TABLE {self._TABLE} (id INT, a INT, b INT) PROPERTIES ('replication_num' = '1')"
        ))
        connection.execute(text(f"CREATE VIEW {self._VIEW} AS SELECT id, a FROM {self._TABLE}"))
        try:
            target_metadata = MetaData()
            Table(
                self._TABLE, target_metadata, Column("id", INTEGER), Column("b", INTEGER),
                starrocks_properties={"replication_num": "1"},
            )
            View(self._VIEW, target_metadata, definition=f"SELECT id, b FROM {self._TABLE}")
            assert self._ops_for(connection, target_metadata, {self._TABLE}) == [
                "DropViewOp", "ModifyTableOps", "CreateViewOp"
            ]
        finally:
            self._drop(connection)

    def test_a_view_reading_a_materialized_view_the_revision_drops_is_redefined(self, connection) -> None:
        """Views are compared before materialized views, so the drop of the MV is found from the model and the
        database; the view would break."""
        connection.execute(text(f"DROP VIEW IF EXISTS {self._VIEW}"))
        connection.execute(text("DROP MATERIALIZED VIEW IF EXISTS mv_not_compared"))
        connection.execute(text(f"DROP TABLE IF EXISTS {self._TABLE}"))
        connection.execute(text(f"CREATE TABLE {self._TABLE} (id INT, a INT) PROPERTIES ('replication_num' = '1')"))
        connection.execute(text(
            f"CREATE MATERIALIZED VIEW mv_not_compared REFRESH MANUAL AS SELECT id, a FROM {self._TABLE}"
        ))
        connection.execute(text(f"CREATE VIEW {self._VIEW} AS SELECT id, a FROM mv_not_compared"))
        try:
            target_metadata = MetaData()
            View(self._VIEW, target_metadata, definition=f"SELECT id, a FROM {self._TABLE}")
            assert self._ops_for(connection, target_metadata, set()) == [
                "DropViewOp", "DropMaterializedViewOp", "CreateViewOp"
            ]
        finally:
            connection.execute(text("DROP MATERIALIZED VIEW IF EXISTS mv_not_compared"))
            self._drop(connection)
