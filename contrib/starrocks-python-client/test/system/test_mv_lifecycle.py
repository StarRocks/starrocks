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
from sqlalchemy import Column, Integer, Table, inspect
from sqlalchemy.orm import declarative_base
from sqlalchemy.testing.assertions import is_true

from starrocks.sql.schema import MaterializedView
from test.system.conftest import AlembicTestEnv
from test.system.test_table_lifecycle import EMPTY_DOWNGRADE_STR, EMPTY_UPGRADE_STR, ScriptContentParser


logger = logging.getLogger(__name__)


@pytest.skip(reason="Skipping MV lifecycle test for now", allow_module_level=True)
class TestMVLifecycle:
    def test_create_mv(database: str, alembic_env: AlembicTestEnv, sr_engine):
        """Tests creating a materialized view."""
        # 1. Define metadata with a table and a materialized view
        Base = declarative_base()
        Table(
            'user',
            Base.metadata,
            Column('id', Integer, primary_key=True),
            starrocks_PROPERTIES={'replication_num': '1'}
        )
        MaterializedView(
            'user_mv',
            'SELECT id, count(*) FROM user GROUP BY id',
            metadata=Base.metadata,
            properties={'replication_num': '1'}
        )

        # 2. Run autogenerate to create the MV
        alembic_env.harness.generate_autogen_revision(
            metadata=Base.metadata, message="Create MV"
        )

        # 3. Verify the script and upgrade
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "create_mv")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        assert "op.create_materialized_view('user_mv'" in upgrade_content
        assert "SELECT id, count(*) FROM user GROUP BY id" in upgrade_content
        downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
        assert "op.drop_materialized_view('user_mv')" in downgrade_content

        alembic_env.harness.upgrade("head")

        # 4. Verify MV exists in the database
        inspector = inspect(sr_engine)
        is_true('user_mv' in inspector.get_materialized_view_names())

        # 5. Downgrade and verify MV is dropped
        alembic_env.harness.downgrade("-1")
        inspector.clear_cache()
        is_true('user_mv' not in inspector.get_materialized_view_names())


    def test_mv_idempotency(database: str, alembic_env: AlembicTestEnv, sr_engine):
        """Tests that no changes are detected if the MV is in sync."""
        # 1. Initial state
        Base = declarative_base()
        Table(
            'user',
            Base.metadata,
            Column('id', Integer, primary_key=True),
            starrocks_PROPERTIES={'replication_num': '1'}
        )
        MaterializedView(
            'user_mv',
            'SELECT id, count(*) FROM user GROUP BY id',
            metadata=Base.metadata,
            properties={'replication_num': '1'}
        )
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
        alembic_env.harness.upgrade("head")

        # 2. Second run
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Second run")

        # 3. Verify no new script
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "second_run")
        is_true(re.search(EMPTY_UPGRADE_STR, script_content), "Upgrade script should be empty")
        is_true(re.search(EMPTY_DOWNGRADE_STR, script_content), "Downgrade script should be empty")


    def test_drop_mv(database: str, alembic_env: AlembicTestEnv, sr_engine):
        """Tests dropping a materialized view."""
        # 1. Initial state
        Base = declarative_base()
        Table(
            'user',
            Base.metadata,
            Column('id', Integer, primary_key=True),
            starrocks_PROPERTIES={'replication_num': '1'}
        )
        MaterializedView(
            'user_mv',
            'SELECT id, count(*) FROM user GROUP BY id',
            metadata=Base.metadata,
            properties={'replication_num': '1'}
        )
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
        alembic_env.harness.upgrade("head")

        # 2. Metadata without the MV
        EmptyBase = declarative_base()
        Table(
            'user',
            EmptyBase.metadata,
            Column('id', Integer, primary_key=True),
            starrocks_PROPERTIES={'replication_num': '1'}
        )
        alembic_env.harness.generate_autogen_revision(metadata=EmptyBase.metadata, message="Drop MV")

        # 3. Verify and apply DROP
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "drop_mv")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        assert "op.drop_materialized_view('user_mv')" in upgrade_content
        downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
        assert "op.create_materialized_view('user_mv'" in downgrade_content

        alembic_env.harness.upgrade("head")
        inspector = inspect(sr_engine)
        is_true('user_mv' not in inspector.get_materialized_view_names())

        # 4. Downgrade and verify
        alembic_env.harness.downgrade("-1")
        inspector.clear_cache()
        is_true('user_mv' in inspector.get_materialized_view_names())


    def test_alter_mv_properties(database: str, alembic_env: AlembicTestEnv, sr_engine):
        """Tests altering a materialized view's properties."""
        # 1. Initial state
        Base = declarative_base()
        Table(
            'user',
            Base.metadata,
            Column('id', Integer, primary_key=True),
            starrocks_PROPERTIES={'replication_num': '1'}
        )
        MaterializedView(
            'user_mv',
            'SELECT id, count(*) FROM user GROUP BY id',
            metadata=Base.metadata,
            properties={'replication_num': '1'}
        )
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
        alembic_env.harness.upgrade("head")

        # 2. Altered metadata with different properties
        AlteredBase = declarative_base()
        Table(
            'user',
            AlteredBase.metadata,
            Column('id', Integer, primary_key=True),
            starrocks_PROPERTIES={'replication_num': '1'}
        )
        MaterializedView(
            'user_mv',
            'SELECT id, count(*) FROM user GROUP BY id',
            metadata=AlteredBase.metadata,
            properties={'replication_num': '3', 'storage_medium': 'SSD'}
        )
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter MV properties")

        # 3. Verify and apply ALTER
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_mv_properties")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        # MV alteration is implemented as DROP + CREATE
        assert "op.drop_materialized_view('user_mv')" in upgrade_content
        assert "op.create_materialized_view('user_mv'" in upgrade_content
        assert "'replication_num': '3'" in upgrade_content
        assert "'storage_medium': 'SSD'" in upgrade_content

        alembic_env.harness.upgrade("head")
        inspector = inspect(sr_engine)
        is_true('user_mv' in inspector.get_materialized_view_names())

        # 4. Downgrade and verify
        alembic_env.harness.downgrade("-1")
        inspector.clear_cache()
        is_true('user_mv' in inspector.get_materialized_view_names())


    def test_alter_mv_definition(database: str, alembic_env: AlembicTestEnv, sr_engine):
        """Tests altering a materialized view's definition."""
        # 1. Initial state
        Base = declarative_base()
        Table(
            'user',
            Base.metadata,
            Column('id', Integer, primary_key=True),
            starrocks_PROPERTIES={'replication_num': '1'}
        )
        MaterializedView(
            'user_mv',
            'SELECT id, count(*) FROM user GROUP BY id',
            metadata=Base.metadata,
            properties={'replication_num': '1'}
        )
        alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
        alembic_env.harness.upgrade("head")

        # 2. Altered metadata with different definition
        AlteredBase = declarative_base()
        Table(
            'user',
            AlteredBase.metadata,
            Column('id', Integer, primary_key=True),
            starrocks_PROPERTIES={'replication_num': '1'}
        )
        MaterializedView(
            'user_mv',
            'SELECT id, sum(id) as total FROM user GROUP BY id',
            metadata=AlteredBase.metadata,
            properties={'replication_num': '1'}
        )
        alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter MV definition")

        # 3. Verify and apply ALTER
        script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_mv_definition")
        upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
        # MV alteration is implemented as DROP + CREATE
        assert "op.drop_materialized_view('user_mv')" in upgrade_content
        assert "op.create_materialized_view('user_mv'" in upgrade_content
        assert "sum(id) as total" in upgrade_content

        alembic_env.harness.upgrade("head")
        inspector = inspect(sr_engine)
        is_true('user_mv' in inspector.get_materialized_view_names())

        # 4. Downgrade and verify
        alembic_env.harness.downgrade("-1")
        inspector.clear_cache()
        is_true('user_mv' in inspector.get_materialized_view_names())
