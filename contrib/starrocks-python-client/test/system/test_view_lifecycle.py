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

from pytest import LogCaptureFixture
from sqlalchemy import Column, Table, inspect
from sqlalchemy.orm import declarative_base
from sqlalchemy.testing.assertions import is_true

from starrocks.datatype import INTEGER, STRING
from starrocks.sql.schema import View
from test.system.conftest import AlembicTestEnv
from test.system.test_table_lifecycle import EMPTY_DOWNGRADE_STR, EMPTY_UPGRADE_STR, ScriptContentParser


logger = logging.getLogger(__name__)


def test_create_view(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests creating a view."""
    # 1. Define metadata with a view
    Base = declarative_base()
    user_table = Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    my_view = View('user_view', 'SELECT id FROM user', metadata=Base.metadata)
    logger.info(f"metadata.tables: {Base.metadata.tables}")
    logger.info(f"metadata#views: {Base.metadata.info.get('views', {})}")

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
        'SELECT id FROM user',
        metadata=Base.metadata,
        comment='A comment',
        security='INVOKER'
    )
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
    alembic_env.harness.upgrade("head")

    # 2. Second run
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Second run")

    # 3. Verify no new script
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "second_run")
    # upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    # assert upgrade_content is None  # pass
    is_true(re.search(EMPTY_UPGRADE_STR, script_content), "Upgrade script should be empty")
    is_true(re.search(EMPTY_DOWNGRADE_STR, script_content), "Downgrade script should be empty")


def test_alter_view_unsupported_attributes(
    database: str, alembic_env: AlembicTestEnv, sr_engine, caplog: LogCaptureFixture
):
    """Tests that altering unsupported view attributes (comment, security) is ignored."""
    caplog.set_level("WARNING")
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
        'SELECT id FROM user',
        metadata=Base.metadata,
        comment='Initial comment',
        security='NONE'  # DEFINDER is not supported in StarRocks v3.5.
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
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        'SELECT id FROM user',
        metadata=AlteredBase.metadata,
        comment='Modified comment',
        security='INVOKER'
    )
    caplog.clear()
    alembic_env.harness.generate_autogen_revision(
        metadata=AlteredBase.metadata, message="Alter unsupported attrs"
    )
    # print(f"caplog.text: {caplog.text}")
    # print(f"caplog.text: {caplog.records}")

    # 3. Verify no ALTER is generated and warnings are logged
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_unsupported_attrs")
    is_true(re.search(EMPTY_UPGRADE_STR, script_content), "Upgrade script should be empty for unsupported attr changes")
    is_true(re.search(EMPTY_DOWNGRADE_STR, script_content), "Downgrade script should be empty for unsupported attr changes")
    # TODO: caplog is not working as expected
    # assert "does not support altering view comments" in caplog.text
    # assert "does not support altering view security" in caplog.text


def test_alter_view(database: str, alembic_env: AlembicTestEnv, sr_engine):
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
    View('user_view', 'SELECT id FROM user', metadata=Base.metadata)
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
    View('user_view', "SELECT id, name FROM user", metadata=AlteredBase.metadata)
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter view")

    # 3. Verify and apply ALTER
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_view")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.alter_view('user_view'" in upgrade_content
    assert "SELECT id, name FROM user" in upgrade_content
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    assert "op.alter_view('user_view'" in downgrade_content
    assert "SELECT id FROM user".lower() in downgrade_content

    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    definition = inspector.get_view_definition('user_view')
    assert 'name' in definition.lower()

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
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('user_view', 'SELECT id FROM user', metadata=Base.metadata)
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
    assert "op.drop_view('user_view')" in upgrade_content
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    assert "op.create_view('user_view'" in downgrade_content

    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    is_true('user_view' not in inspector.get_view_names())

    # 4. Downgrade and verify
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    is_true('user_view' in inspector.get_view_names())
