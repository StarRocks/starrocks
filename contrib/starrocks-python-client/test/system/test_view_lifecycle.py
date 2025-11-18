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
import re

import pytest
from pytest import LogCaptureFixture
from sqlalchemy import Column, MetaData, Table, inspect, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.testing.assertions import is_true

from starrocks.alembic.compare import TableAttributeNormalizer
from starrocks.common.params import TableKind, TableObjectInfoKey
from starrocks.datatype import INTEGER, STRING, VARCHAR
from starrocks.sql.schema import View
from test import test_utils
from test.system.conftest import AlembicTestEnv
from test.system.test_table_lifecycle import EMPTY_DOWNGRADE_STR, EMPTY_UPGRADE_STR, ScriptContentParser
from test.unit.test_render import _normalize_py_call


logger = logging.getLogger(__name__)


def test_create_view_basic(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests creating a view."""
    # 1. Define metadata with a view
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('user_view', Base.metadata, definition='SELECT id FROM user')
    logger.info(f"metadata.tables: {Base.metadata.tables}")

    # 2. Run autogenerate to create the view
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Create view"
    )

    # 3. Verify the script and upgrade
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "create_view")
    logger.debug("extract upgrade content")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.create_view('user_view'" in upgrade_content
    assert "SELECT id FROM user" in upgrade_content
    logger.debug("extract downgrade content")
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    assert "op.drop_view('user_view')" in downgrade_content

    alembic_env.harness.upgrade("head")

    # 4. Verify view exists in the database
    inspector = inspect(sr_engine)
    is_true('user_view' in inspector.get_view_names())
    definition = inspector.get_view_definition('user_view')
    normalized_def = TableAttributeNormalizer.normalize_sql(definition, remove_qualifiers=True)
    assert "select id from user" in normalized_def

    # 5. Downgrade and verify view is dropped
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    is_true('user_view' not in inspector.get_view_names())


def test_create_view_with_columns(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests creating a view with explicit column definitions."""
    # 1. Define metadata with a view that has columns
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        Column('email', VARCHAR(100)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        Base.metadata,
        Column('id', VARCHAR()),
        Column('name', VARCHAR(50), comment='User name'),
        Column('email', VARCHAR(100), comment='Email address'),
        definition='SELECT id, name, email FROM user',
        comment='User information view'
    )

    # 2. Run autogenerate to create the view
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Create view with columns"
    )

    # 3. Verify the script and upgrade
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "create_view_with_columns")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.create_view('user_view'" in _normalize_py_call(upgrade_content)
    assert "'comment': 'User name'" in upgrade_content
    assert "'name': 'name'" in upgrade_content
    assert "'name': 'email'" in upgrade_content

    alembic_env.harness.upgrade("head")

    # 4. Verify view exists in the database
    inspector = inspect(sr_engine)
    is_true('user_view' in inspector.get_view_names())
    definition = inspector.get_view_definition('user_view')
    normalized_def = TableAttributeNormalizer.normalize_sql(definition, remove_qualifiers=True)
    assert "select id, name, email from user" in normalized_def

    # 5. Downgrade and verify view is dropped
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    is_true('user_view' not in inspector.get_view_names())


def test_view_idempotency(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests that no changes are detected if the view is in sync.
    Qualifiers, like '`schema`.`table`.', for column names are removed.
    """
    # 1. Initial state
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        Base.metadata,
        definition='SELECT id FROM user',
        comment='A comment',
        starrocks_security='INVOKER'
    )
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "initial")
    alembic_env.harness.upgrade("head")

    # 2. Second run
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Second run")

    # 3. Verify no new script
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "second_run")
    # upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    # assert upgrade_content is None  # pass
    is_true(re.search(EMPTY_UPGRADE_STR, script_content), "Upgrade script should be empty")
    is_true(re.search(EMPTY_DOWNGRADE_STR, script_content), "Downgrade script should be empty")


def test_alter_view_comment_and_security(
    database: str, alembic_env: AlembicTestEnv, sr_engine
):
    """
    Tests that altering view comment and security generates an AlterViewOp
    and issues warnings, even though StarRocks doesn't support these via ALTER VIEW.
    """
    # 1. Initial state with a view having a comment and security NONE
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        Base.metadata,
        definition='SELECT id FROM user',
        comment='Initial comment',
        starrocks_security='NONE'  # DEFINDER is not supported in StarRocks v3.5.
    )
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Initial view with attrs"
    )
    alembic_env.harness.upgrade("head")

    # 2. Alter metadata with changed comment and security
    AlteredBase = declarative_base()
    Table(
        'user',
        AlteredBase.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_properties={'replication_num': '1'}
    )
    View(
        'user_view',
        AlteredBase.metadata,
        definition='SELECT id FROM user',
        comment='Modified comment',
        starrocks_security='INVOKER'
    )

    with pytest.warns(UserWarning) as record:
        alembic_env.harness.generate_autogen_revision(
            metadata=AlteredBase.metadata, message="Alter comment and security"
        )

    # Check that both warnings were issued
    messages = [str(w.message) for w in record]
    assert any("does not support altering view comments" in m for m in messages)
    assert any("does not support altering view security" in m for m in messages)

    # 3. Verify ALTER is generated
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_comment_and_security")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    upgrade_content = test_utils.normalize_sql(upgrade_content)
    assert 'op.alter_view("user_view"' in upgrade_content
    assert 'comment="Modified comment"' in upgrade_content
    assert 'security="INVOKER"' in upgrade_content
    assert "SELECT id FROM user" not in upgrade_content

    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    downgrade_content = test_utils.normalize_sql(downgrade_content)
    assert 'op.alter_view("user_view"' in downgrade_content
    assert 'comment="Initial comment"' in downgrade_content
    assert 'security="NONE"' in downgrade_content or 'security=None' in downgrade_content
    assert "SELECT id FROM user" not in downgrade_content


def test_alter_view_definition(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests altering a view's definition.
    Qualifiers, like '`schema`.`table`.', for column names are removed.
    """
    # 1. Initial state
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', STRING(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('user_view', Base.metadata, definition='SELECT id FROM user')
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
    alembic_env.harness.upgrade("head")

    # 2. Altered metadata
    AlteredBase = declarative_base()
    Table(
        'user',
        AlteredBase.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', STRING(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('user_view', AlteredBase.metadata, definition="SELECT id, name FROM user")
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter view")

    # 3. Verify and apply ALTER
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_view")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    upgrade_content = TableAttributeNormalizer.normalize_sql(upgrade_content, remove_qualifiers=True)
    upgrade_content = test_utils.normalize_sql(upgrade_content)
    assert 'alter_view("user_view"' in upgrade_content
    assert test_utils.normalize_sql("SELECT id, name FROM user".lower()) in upgrade_content
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    normed_downgrade_content = TableAttributeNormalizer.normalize_sql(downgrade_content, remove_qualifiers=True)
    normed_downgrade_content = test_utils.normalize_sql(normed_downgrade_content)
    assert 'alter_view("user_view"' in normed_downgrade_content
    # Normalize only the view definition inside the downgrade content
    view_def_match = re.search(r"op\.alter_view\(\s*'user_view',\s*'(.*?)'", downgrade_content, re.DOTALL)
    assert view_def_match is not None
    view_def = view_def_match.group(1)
    logger.info(f"view_def: {view_def}")
    normalized_def = TableAttributeNormalizer.normalize_sql(view_def, remove_qualifiers=True)
    assert "select id from user" in normalized_def

    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    definition = inspector.get_view_definition('user_view')
    assert 'name' in definition.lower()
    normalized_def = TableAttributeNormalizer.normalize_sql(definition, remove_qualifiers=True)
    assert "select id, name from user" in normalized_def

    # 4. Downgrade and verify
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    definition = inspector.get_view_definition('user_view')
    assert 'name' not in definition.lower()


def test_drop_view(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests dropping a view."""
    # 1. Initial state
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_properties={'replication_num': '1'}
    )
    View('user_view', Base.metadata, definition='SELECT id FROM user')
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
    alembic_env.harness.upgrade("head")

    # 2. Metadata without the view
    EmptyBase = declarative_base()
    Table(
        'user',
        EmptyBase.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    alembic_env.harness.generate_autogen_revision(metadata=EmptyBase.metadata, message="Drop view")

    # 3. Verify and apply DROP
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "drop_view")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.drop_view('user_view')" in _normalize_py_call(upgrade_content)
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    assert "op.create_view('user_view'" in _normalize_py_call(downgrade_content)

    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    is_true('user_view' not in inspector.get_view_names())

    # 4. Downgrade and verify
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    is_true('user_view' in inspector.get_view_names())
    definition = inspector.get_view_definition('user_view')
    normalized_def = TableAttributeNormalizer.normalize_sql(definition, remove_qualifiers=True)
    assert "select id from user" in normalized_def


def test_columns_change_ignored(database: str, alembic_env: AlembicTestEnv, sr_engine, caplog: LogCaptureFixture):
    """Tests that column changes are detected but not altered (StarRocks limitation)."""
    caplog.set_level("WARNING")

    # 1. Initial state with columns
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        Base.metadata,
        Column('id', VARCHAR()),
        Column('name', VARCHAR(50), comment='User name'),
        definition='SELECT id, name FROM user'
    )
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial with columns")
    alembic_env.harness.upgrade("head")

    # 2. Change column definition (add new column, change comment)
    AlteredBase = declarative_base()
    Table(
        'user',
        AlteredBase.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        AlteredBase.metadata,
        Column('id', VARCHAR()),
        Column('name', VARCHAR(50), comment='Modified name'),  # Changed comment
        definition='SELECT id, name FROM user',  # Definition unchanged
    )
    with pytest.raises(NotImplementedError, match="StarRocks does not support altering view columns independently"):
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Modify column comment")

    # 3. Verify no ALTER is generated (only definition changes trigger ALTER)
    # script_content = ScriptContentParser.check_script_content(alembic_env, 1, "modify_column_comment")
    # is_true(re.search(EMPTY_UPGRADE_STR, script_content), "Upgrade script should be empty for column comment changes")

    # 4. Change definition to test that it triggers ALTER (even with column changes)
    AlteredBase2 = declarative_base()
    Table(
        'user',
        AlteredBase2.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        AlteredBase2.metadata,
        Column('id', VARCHAR()),
        Column('name', VARCHAR(50), comment='Modified name'),
        Column('email', VARCHAR(100), comment='Email'),  # Added column
        definition='SELECT id, name, email FROM user',  # Changed definition
    )
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase2.metadata, message="Change definition and columns")

    # 5. Verify ALTER is generated for definition change
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "change_definition_and_columns")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.alter_view" in upgrade_content
    assert "email" in upgrade_content


def test_create_view_with_schema(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests creating a view in a specific schema (non-default schema)."""
    # 1. Define metadata with a view in a schema
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        schema=database,
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        Base.metadata,
        definition='SELECT id FROM user',
        schema=database
    )

    # 2. Run autogenerate to create the view
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Create view with schema"
    )

    # 3. Verify the script and upgrade
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "create_view_with_schema")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.create_view('user_view'" in _normalize_py_call(upgrade_content)
    assert f"schema='{database}'" in upgrade_content
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    assert "op.drop_view('user_view'" in _normalize_py_call(downgrade_content)
    assert f"schema='{database}'" in downgrade_content

    alembic_env.harness.upgrade("head")

    # 4. Verify view exists in the database
    inspector = inspect(sr_engine)
    is_true('user_view' in inspector.get_view_names(schema=database))

    # 5. Downgrade and verify view is dropped
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    is_true('user_view' not in inspector.get_view_names(schema=database))


def test_multiple_views_in_one_migration(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests creating, altering, and dropping multiple views in one migration.

    This is a system-level test that verifies:
    1. Multiple view operations can be generated in a single migration
    2. All operations are correctly written to the script
    3. Upgrade applies all operations in correct order
    4. Downgrade correctly reverses all operations
    """
    # 1. Initial state: create two views
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('view1', Base.metadata, definition='SELECT id FROM user')
    View('view2', Base.metadata, definition='SELECT name FROM user')
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial two views")
    alembic_env.harness.upgrade("head")

    # 2. Define altered state: create view3, alter view1, drop view2
    AlteredBase = declarative_base()
    Table(
        'user',
        AlteredBase.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('view1', AlteredBase.metadata, definition='SELECT id, name FROM user')  # Altered
    View('view3', AlteredBase.metadata, definition='SELECT id FROM user WHERE id > 10')  # New
    # view2 removed (will be dropped)

    # 3. Generate migration with multiple operations
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Multiple ops")

    # 4. Verify script contains all operations
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "multiple_ops")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    normalized_upgrade_content = _normalize_py_call(upgrade_content)
    assert "op.create_view('view3'" in normalized_upgrade_content
    assert "op.alter_view('view1'" in normalized_upgrade_content
    assert "op.drop_view('view2'" in normalized_upgrade_content
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    normalized_downgrade_content = _normalize_py_call(downgrade_content)
    assert "op.drop_view('view3'" in normalized_downgrade_content
    assert "op.alter_view('view1'" in normalized_downgrade_content
    assert "op.create_view('view2'" in normalized_downgrade_content

    # 5. Apply upgrade and verify all changes
    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    is_true('view1' in inspector.get_view_names())
    is_true('view2' not in inspector.get_view_names())
    is_true('view3' in inspector.get_view_names())

    # 6. Apply downgrade and verify rollback
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    is_true('view1' in inspector.get_view_names())
    is_true('view2' in inspector.get_view_names())
    is_true('view3' not in inspector.get_view_names())


def test_custom_include_object(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests using a custom include_object filter in Alembic configuration.

    This verifies that users can define their own filter logic to:
    - Exclude specific view patterns (e.g., tmp_* views)
    - Combine with the default dialect filter
    - Correctly generate migrations based on custom rules
    """
    # 1. Create views in database (including tmp_ views)
    with sr_engine.connect() as conn:
        conn.execute(text("DROP VIEW IF EXISTS prod_view"))
        conn.execute(text("DROP VIEW IF EXISTS tmp_view"))
        conn.execute(text("CREATE VIEW prod_view AS SELECT 1 AS val"))
        conn.execute(text("CREATE VIEW tmp_view AS SELECT 2 AS val"))
        conn.commit()

    # 2. Define metadata with only prod views (tmp_ views should be ignored)
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('prod_view', Base.metadata, definition='SELECT 1 AS val')
    # tmp_view not in metadata, but we'll configure filter to ignore it

    # 3. Define custom include_object that excludes tmp_* views
    from starrocks.alembic.compare import include_object_for_view_mv

    def custom_include_object(object, name, type_, reflected, compare_to):
        # First apply dialect's default filter
        if not include_object_for_view_mv(object, name, type_, reflected, compare_to):
            return False
        # Then apply custom logic: exclude tmp_* views
        if type_ == "view" and name.startswith("tmp_"):
            logger.info(f"Custom filter: excluding tmp_ view {name}")
            return False
        return True

    # 4. Generate migration with custom filter
    # Note: In real usage, custom include_object would be set in alembic/env.py
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata,
        message="Custom filter test",
        include_object=custom_include_object
    )

    # 5. Verify script: tmp_view should NOT be dropped (filtered out)
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "custom_filter_test")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)

    # Verify: prod_view should match (no change, so no operations)
    # Verify: tmp_view should be filtered out (no DROP operation generated)
    # Note: Without custom filter, tmp_view would generate a DROP operation
    assert "tmp_view" not in upgrade_content, "tmp_view should be filtered out, not dropped"
    assert "tmp_view" not in downgrade_content, "tmp_view should be filtered out"
    assert "prod_view" not in upgrade_content, "prod_view definition matches, no operation needed"

    # 6. Clean up
    with sr_engine.connect() as conn:
        conn.execute(text("DROP VIEW IF EXISTS prod_view"))
        conn.execute(text("DROP VIEW IF EXISTS tmp_view"))
        conn.commit()


def test_view_with_special_characters_in_definition(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests view definitions containing special characters.

    This verifies that special characters in view definitions are correctly:
    1. Escaped in migration scripts (render phase)
    2. Executed to database (execution phase)
    3. Reflected back correctly (reflection phase)

    Special characters tested:
    - Single quotes (')
    - Double quotes (")
    - Backslashes (\)
    - Newlines (\n)
    - Mixed combinations

    This is a critical test because incorrect escaping can cause:
    - Syntax errors in generated migration scripts
    - SQL injection vulnerabilities
    - Data loss during reflection
    """
    # 1. Create base table first
    with sr_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS test_users"))
        conn.execute(text("""
            CREATE TABLE test_users (
                id INT,
                name VARCHAR(50),
                description VARCHAR(200)
            ) PROPERTIES('replication_num'='1')
        """))
        conn.commit()

    # 2. Define metadata with views containing special characters
    Base = declarative_base()

    Table(
        "test_users",
        Base.metadata,
        Column("id", INTEGER),
        Column("name", VARCHAR(50)),
        Column("description", VARCHAR(200)),
        starrocks_PROPERTIES={"replication_num": "1"},
    )

    # Test Case 1: Single quotes in string literals
    View(
        'view_single_quotes',
        Base.metadata,
        definition="SELECT id, name, 'O''Brien' AS nickname FROM test_users"
    )

    # Test Case 2: Double quotes (column aliases)
    # currently, Normalizer can't handle column name with double quotes
    View(
        'view_double_quotes',
        Base.metadata,
        # definition='SELECT id, name AS "User Name" FROM test_users'
        definition='SELECT id, name AS `User Name` FROM test_users'
    )

    # Test Case 3: Backslashes in string literals
    View(
        'view_backslashes',
        Base.metadata,
        definition=r"SELECT id, name, 'C:\\Users\\path' AS file_path FROM test_users"
    )

    # Test Case 4: Newlines in definition (multi-line SQL)
    View(
        'view_multiline',
        Base.metadata,
        definition="""SELECT
    id,
    name,
    description
FROM test_users
WHERE id > 0"""
    )

    # Test Case 5: Mixed special characters
    View(
        'view_mixed_special',
        Base.metadata,
        definition="""SELECT
    id,
    name AS `User's Name`,
    'Status: "Active"' AS status,
    'Path: C:\\\\temp' AS temp_path
FROM test_users"""
    )

    # 3. Generate migration script
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata,
        message="Create views with special characters"
    )

    # 4. Verify script content - check that special characters are properly escaped
    script_content = ScriptContentParser.check_script_content(
        alembic_env, 1, "create_views_with_special_characters"
    )
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)

    # Verify all views are in the script
    assert "op.create_view('view_single_quotes'" in upgrade_content
    assert "op.create_view('view_double_quotes'" in upgrade_content
    assert "op.create_view('view_backslashes'" in upgrade_content
    assert "op.create_view('view_multiline'" in upgrade_content
    assert "op.create_view('view_mixed_special'" in upgrade_content

    # 5. Apply upgrade - this tests execution phase (critical!)
    alembic_env.harness.upgrade("head")

    # 6. Verify views exist in database
    inspector = inspect(sr_engine)
    view_names = inspector.get_view_names()
    is_true('view_single_quotes' in view_names)
    is_true('view_double_quotes' in view_names)
    is_true('view_backslashes' in view_names)
    is_true('view_multiline' in view_names)
    is_true('view_mixed_special' in view_names)

    # 7. Test reflection - verify definitions are preserved correctly
    def _check_view_def(view_name: str):
        logger.info(f"Checking reflection for view: {view_name}")
        original_def = Base.metadata.tables[view_name].info[TableObjectInfoKey.DEFINITION]

        reflected_view = Table(view_name, MetaData(), autoload_with=sr_engine)
        is_true(reflected_view.info.get(TableObjectInfoKey.TABLE_KIND) == TableKind.VIEW)
        reflected_def = reflected_view.info.get(TableObjectInfoKey.DEFINITION)
        is_true(reflected_def is not None)

        # We normalize both for a robust comparison
        # remove_qualifiers=True is important as DB may add schema/table qualifiers
        logger.info(f"Original definition: {original_def}")
        logger.info(f"Reflected definition: {reflected_def}")

        normalized_original = TableAttributeNormalizer.normalize_sql(original_def, remove_qualifiers=True)
        normalized_reflected = TableAttributeNormalizer.normalize_sql(reflected_def, remove_qualifiers=True)

        logger.info(f"Original normalized: {normalized_original}")
        logger.info(f"Reflected normalized: {normalized_reflected}")
        assert normalized_original == normalized_reflected

    _check_view_def('view_single_quotes')
    _check_view_def('view_double_quotes')
    _check_view_def('view_backslashes')
    _check_view_def('view_multiline')
    _check_view_def('view_mixed_special')


    # 8. Test idempotency - re-generate should produce no changes
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata,
        message="No changes expected"
    )
    script_content_2 = ScriptContentParser.check_script_content(alembic_env, 1, "no_changes_expected")
    upgrade_content_2 = ScriptContentParser.extract_upgrade_content(script_content_2)
    # Should have no view operations (or only pass)
    assert "op.create_view" not in upgrade_content_2
    assert "op.alter_view" not in upgrade_content_2
    assert "op.drop_view" not in upgrade_content_2

    # 9. Test downgrade
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    view_names_after_downgrade = inspector.get_view_names()
    is_true('view_single_quotes' not in view_names_after_downgrade)
    is_true('view_double_quotes' not in view_names_after_downgrade)
    is_true('view_backslashes' not in view_names_after_downgrade)
    is_true('view_multiline' not in view_names_after_downgrade)
    is_true('view_mixed_special' not in view_names_after_downgrade)

    # 10. Clean up
    with sr_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS test_users"))
        conn.commit()
