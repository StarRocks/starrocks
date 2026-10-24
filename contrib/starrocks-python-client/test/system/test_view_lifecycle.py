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

from sqlalchemy import Column, MetaData, Table, inspect, testing, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.testing.assertions import eq_, is_true

from starrocks.alembic.compare import TableAttributeNormalizer
from starrocks.common.params import TableKind, TableObjectInfoKey
from starrocks.datatype import DATE, INTEGER, STRING, VARCHAR
from starrocks.sql.schema import MaterializedView, View
from test.system.lifecycle import (
    EMPTY_DOWNGRADE_STR,
    EMPTY_UPGRADE_STR,
    ScriptContentParser,
    _AlembicLifecycleBase,
)
from test.util import (
    VIEW_SECURITY_NEEDS_3_4_1,
    VIEW_TEXT_REWRITTEN_BEFORE_4_0_7,
    normalize_py_call,
    normalize_sql,
)


logger = logging.getLogger(__name__)


class ViewLifecycleTest(_AlembicLifecycleBase):
    def test_create_view_basic(self, database, alembic_env, sr_engine):
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

    def test_create_view_with_columns(self, database, alembic_env, sr_engine):
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
        assert "op.create_view('user_view'" in normalize_py_call(upgrade_content)
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

    @VIEW_SECURITY_NEEDS_3_4_1
    def test_view_idempotency(self, database, alembic_env, sr_engine):
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
        ScriptContentParser.check_script_content(alembic_env, 1, "initial")
        alembic_env.harness.upgrade("head")

        # 2. Second run
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Second run")

        # 3. Verify no new script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "second_run")
        # upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        # assert upgrade_content is None  # pass
        is_true(re.search(EMPTY_UPGRADE_STR, script_content), "Upgrade script should be empty")
        is_true(re.search(EMPTY_DOWNGRADE_STR, script_content), "Downgrade script should be empty")

    @staticmethod
    def _model_with_a_percent_sign(date_format):
        Base = declarative_base()
        Table(
            "t_pct", Base.metadata, Column("id", INTEGER), Column("dt", DATE),
            starrocks_properties={"replication_num": "1"},
        )
        View("v_month", Base.metadata, definition=f"SELECT id, date_format(dt, '{date_format}') AS ym FROM t_pct")
        return Base.metadata

    def test_an_unchanged_view_with_a_percent_sign_is_left_alone(self, alembic_env, sr_engine, caplog):
        """A ``%`` in the definition reaches the temporary view as written, so StarRocks canonicalizes the model's SQL.

        Before 4.0.7 StarRocks stores a view's SQL rewritten, and only the temporary view can compare the two.
        """
        alembic_env.harness.generate_autogen_revision(
            metadata=self._model_with_a_percent_sign("%Y-%m"), message="create"
        )
        alembic_env.harness.upgrade("head")

        alembic_env.harness.generate_autogen_revision(
            metadata=self._model_with_a_percent_sign("%Y-%m"), message="unchanged"
        )

        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "unchanged")
        is_true(re.search(EMPTY_UPGRADE_STR, script_content), script_content)
        is_true("could not canonicalize" not in caplog.text, caplog.text)

    def test_a_changed_view_with_a_percent_sign_is_altered(self, alembic_env, sr_engine):
        """Before 4.0.7, without the temporary view, the change would only be warned about."""
        alembic_env.harness.generate_autogen_revision(
            metadata=self._model_with_a_percent_sign("%Y-%m"), message="create"
        )
        alembic_env.harness.upgrade("head")

        alembic_env.harness.generate_autogen_revision(
            metadata=self._model_with_a_percent_sign("%Y-%m-%d"), message="changed"
        )

        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "changed")
        is_true("op.alter_view(" in ScriptContentParser.extract_upgrade_content(script_content), script_content)

    @VIEW_SECURITY_NEEDS_3_4_1
    def test_alter_view_comment_and_security(self, database, alembic_env, sr_engine):
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
            starrocks_security='NONE'
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

        alembic_env.harness.generate_autogen_revision(
            metadata=AlteredBase.metadata, message="Alter comment and security"
        )

        # 3. Verify ALTER is generated, and carries the existing definition the comment change recreates the view from
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_comment_and_security")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        upgrade_content = normalize_sql(upgrade_content)
        assert 'op.alter_view("user_view"' in upgrade_content
        assert 'comment="Modified comment"' in upgrade_content
        assert 'security="INVOKER"' in upgrade_content
        assert "existing_definition=" in upgrade_content

        downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
        downgrade_content = normalize_sql(downgrade_content)
        assert 'op.alter_view("user_view"' in downgrade_content
        assert 'comment="Initial comment"' in downgrade_content
        assert 'security="NONE"' in downgrade_content or 'security=None' in downgrade_content

        # 4. Apply it and undo it
        def view_ddl() -> str:
            with sr_engine.connect() as connection:
                return connection.exec_driver_sql("SHOW CREATE VIEW user_view").fetchone()[1]

        alembic_env.harness.upgrade("head")
        assert 'COMMENT "Modified comment"' in view_ddl()
        assert "SECURITY INVOKER" in view_ddl()

        alembic_env.harness.downgrade("-1")
        assert 'COMMENT "Initial comment"' in view_ddl()
        assert "SECURITY NONE" in view_ddl()

    def test_alter_view_definition(self, database, alembic_env, sr_engine):
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
        upgrade_content = normalize_sql(upgrade_content)
        assert 'alter_view("user_view"' in upgrade_content
        assert normalize_sql("SELECT id, name FROM user") in upgrade_content
        downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
        normed_downgrade_content = TableAttributeNormalizer.normalize_sql(downgrade_content, remove_qualifiers=True)
        normed_downgrade_content = normalize_sql(normed_downgrade_content)
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

    def test_drop_view(self, database, alembic_env, sr_engine):
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
        assert "op.drop_view('user_view')" in normalize_py_call(upgrade_content)
        downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
        assert "op.create_view('user_view'" in normalize_py_call(downgrade_content)

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

    def test_columns_change(self, database, alembic_env, sr_engine):
        """Tests that a column-only change generates an ALTER VIEW that redefines the view with the new columns."""

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
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Modify column comment")

        # 3. Verify the ALTER carries the unchanged definition and the new column comment, and apply it
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "modify_column_comment")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        assert "op.alter_view" in upgrade_content
        assert "Modified name" in upgrade_content
        alembic_env.harness.upgrade("head")
        with sr_engine.connect() as connection:
            assert '"Modified name"' in connection.exec_driver_sql("SHOW CREATE VIEW user_view").fetchone()[1]

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

    def test_create_view_with_schema(self, database, alembic_env, sr_engine):
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
        assert "op.create_view('user_view'" in normalize_py_call(upgrade_content)
        assert f"schema='{database}'" in upgrade_content
        downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
        assert "op.drop_view('user_view'" in normalize_py_call(downgrade_content)
        assert f"schema='{database}'" in downgrade_content

        alembic_env.harness.upgrade("head")

        # 4. Verify view exists in the database
        inspector = inspect(sr_engine)
        is_true('user_view' in inspector.get_view_names(schema=database))

        # 5. Downgrade and verify view is dropped
        alembic_env.harness.downgrade("-1")
        inspector.clear_cache()
        is_true('user_view' not in inspector.get_view_names(schema=database))

    def test_multiple_views_in_one_migration(self, database, alembic_env, sr_engine):
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
        normalized_upgrade_content = normalize_py_call(upgrade_content)
        assert "op.create_view('view3'" in normalized_upgrade_content
        assert "op.alter_view('view1'" in normalized_upgrade_content
        assert "op.drop_view('view2'" in normalized_upgrade_content
        downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
        normalized_downgrade_content = normalize_py_call(downgrade_content)
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

    def test_custom_include_object(self, database, alembic_env, sr_engine):
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

    @VIEW_TEXT_REWRITTEN_BEFORE_4_0_7
    def test_view_with_special_characters_in_definition(self, database, alembic_env, sr_engine):
        """Tests view definitions containing special characters.

        This verifies that special characters in view definitions are correctly:
        1. Escaped in migration scripts (render phase)
        2. Executed to database (execution phase)
        3. Reflected back correctly (reflection phase)

        Special characters tested:
        - Single quotes (')
        - Double quotes (")
        - Backslashes (\\)
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


class ViewOrderLifecycleTest(_AlembicLifecycleBase):
    """Revisions whose views and materialized views read what the same revision creates, changes or removes."""

    @staticmethod
    def _base_table(metadata, *columns):
        return Table(
            "t1", metadata, Column("id", INTEGER, primary_key=True), *columns, starrocks_properties={"replication_num": "1"}
        )

    @staticmethod
    def _assert_in_order(content, *calls):
        missing = [call for call in calls if call not in content]
        assert not missing, (missing, content)
        positions = [content.index(call) for call in calls]
        assert positions == sorted(positions), content

    def test_removing_a_table_and_what_reads_it_downgrades(self, alembic_env, sr_engine):
        Base = declarative_base()
        self._base_table(Base.metadata, Column("name", VARCHAR(20)))
        View("v1", Base.metadata, definition="SELECT id, name FROM t1")
        MaterializedView(
            "mv1", Base.metadata, definition="SELECT id FROM t1", starrocks_refresh="MANUAL",
            starrocks_properties={"replication_num": "1"},
        )
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="create")
        alembic_env.harness.upgrade("head")
        alembic_env.harness.generate_autogen_revision(metadata=declarative_base().metadata, message="remove")
        upgrade_content = ScriptContentParser.extract_upgrade_content(
            ScriptContentParser.check_script_content(alembic_env, 1, "remove")
        )
        self._assert_in_order(upgrade_content, "op.drop_materialized_view('mv1'", "op.drop_view('v1'", "op.drop_table('t1'")
        alembic_env.harness.upgrade("head")
        alembic_env.harness.downgrade("-1")
        inspector = inspect(sr_engine)
        is_true("v1" in inspector.get_view_names())
        is_true("mv1" in inspector.get_materialized_view_names())

    def test_a_view_reading_a_column_the_revision_adds_is_altered(self, alembic_env, sr_engine):
        """The temporary view fails while the column is missing, but the database's definition goes through it, so
        the change is certain even before 4.0.7, where StarRocks keeps the view's SQL rewritten."""
        Base = declarative_base()
        self._base_table(Base.metadata)
        View("v1", Base.metadata, definition="SELECT id FROM t1")
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="create")
        alembic_env.harness.upgrade("head")
        Widened = declarative_base()
        self._base_table(Widened.metadata, Column("name", VARCHAR(20)))
        View("v1", Widened.metadata, definition="SELECT id, name FROM t1")
        alembic_env.harness.generate_autogen_revision(metadata=Widened.metadata, message="widen")
        upgrade_content = ScriptContentParser.extract_upgrade_content(
            ScriptContentParser.check_script_content(alembic_env, 1, "widen")
        )
        self._assert_in_order(upgrade_content, "op.add_column('t1'", "op.alter_view(")
        alembic_env.harness.upgrade("head")
        alembic_env.harness.generate_autogen_revision(metadata=Widened.metadata, message="again")
        is_true(re.search(EMPTY_UPGRADE_STR, ScriptContentParser.check_script_content(alembic_env, 1, "again")))

    def test_views_over_views_and_mvs_follow_what_they_read(self, alembic_env, sr_engine):
        Base = declarative_base()
        self._base_table(Base.metadata)
        MaterializedView(
            "z_mv", Base.metadata, definition="SELECT id FROM t1", starrocks_refresh="MANUAL",
            starrocks_properties={"replication_num": "1"},
        )
        View("m_view", Base.metadata, definition="SELECT id FROM z_mv")
        View("a_view", Base.metadata, definition="SELECT id FROM m_view")
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="create")
        upgrade_content = ScriptContentParser.extract_upgrade_content(
            ScriptContentParser.check_script_content(alembic_env, 1, "create")
        )
        self._assert_in_order(
            upgrade_content,
            "op.create_table('t1'",
            "'z_mv',\n        'SELECT id FROM t1'",
            "op.create_view('m_view'",
            "op.create_view('a_view'",
        )
        alembic_env.harness.upgrade("head")
        alembic_env.harness.downgrade("base")
        alembic_env.harness.upgrade("head")
        inspector = inspect(sr_engine)
        eq_(sorted(inspector.get_view_names()), ["a_view", "m_view"])

    @staticmethod
    def _mv(metadata, definition):
        return MaterializedView(
            "mv", metadata, definition=definition, starrocks_refresh="MANUAL",
            starrocks_properties={"replication_num": "1"},
        )

    @classmethod
    def _view_over_a_dropped_view(cls, metadata, revision):
        if revision == 1:
            View("vb", metadata, definition="SELECT id, a FROM t1")
            View("va", metadata, definition="SELECT id FROM vb")
        else:
            View("va", metadata, definition="SELECT id FROM t1")

    @classmethod
    def _view_over_a_dropped_mv(cls, metadata, revision):
        if revision == 1:
            cls._mv(metadata, "SELECT id, a FROM t1")
            View("va", metadata, definition="SELECT id FROM mv")
        else:
            View("va", metadata, definition="SELECT id FROM t1")

    @classmethod
    def _view_over_a_recreated_mv(cls, metadata, revision):
        if revision == 1:
            cls._mv(metadata, "SELECT id, a, b FROM t1")
            View("va", metadata, definition="SELECT id, b FROM mv")
        else:
            cls._mv(metadata, "SELECT id, a FROM t1")
            View("va", metadata, definition="SELECT id, a FROM mv")

    @classmethod
    def _view_over_a_narrowed_view(cls, metadata, revision):
        if revision == 1:
            View("w", metadata, definition="SELECT id, a FROM t1")
            View("va", metadata, definition="SELECT id, a FROM w")
        else:
            View("w", metadata, definition="SELECT id FROM t1")
            View("va", metadata, definition="SELECT id FROM w")

    @testing.combinations(
        ("a_dropped_view", "_view_over_a_dropped_view", "vb"),
        ("a_dropped_mv", "_view_over_a_dropped_mv", "mv"),
        ("a_recreated_mv", "_view_over_a_recreated_mv", "b"),
        ("a_narrowed_view", "_view_over_a_narrowed_view", "a"),
        argnames="build, restored",
        id_="iaa",
    )
    def test_a_view_that_stops_reading_a_view_or_mv_downgrades(self, alembic_env, sr_engine, build, restored):
        """The view ``va`` stops reading what the revision drops, recreates or narrows; the downgrade reads it again."""
        for revision in (1, 2):
            metadata = declarative_base().metadata
            self._base_table(metadata, Column("a", INTEGER), Column("b", INTEGER))
            getattr(self, build)(metadata, revision)
            alembic_env.harness.generate_autogen_revision(metadata=metadata, message=f"revision {revision}")
            alembic_env.harness.upgrade("head")
        alembic_env.harness.downgrade("-1")
        inspector = inspect(sr_engine)
        is_true(re.search(rf"\b{restored}\b", inspector.get_view_definition("va")))
