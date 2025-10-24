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
import os
from typing import Any, Generator

from alembic.autogenerate import api
from alembic.config import Config
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
import pytest
from sqlalchemy import Engine, MetaData, inspect, text

from starrocks.alembic.ops import AlterViewOp, CreateViewOp
from starrocks.sql.schema import View
from test import conftest_sr
from test.unit.test_utils import normalize_sql


logger = logging.getLogger(__name__)


"""
It will use reflection to get the view definition from the database, and compare it
with the view definition in the metadata.

So, it needs a integration test environment.
"""


@pytest.skip(reason="Skipping views test for now", allow_module_level=True)
class TestIntegrationViews:
    STARROCKS_URI = conftest_sr.get_starrocks_url()
    engine: Engine

    @classmethod
    def setup_class(cls):
        cls.engine = conftest_sr.create_test_engine()

    @classmethod
    def teardown_class(cls):
        cls.engine.dispose()

    @pytest.fixture(scope="function")
    def alembic_env(self) -> Generator[Config, Any, None]:
        script_dir_path = "test_alembic_env"
        import shutil
        if os.path.exists(script_dir_path):
            shutil.rmtree(script_dir_path)
        os.makedirs(script_dir_path)
        shutil.copy("test/integration/templates/env.py", os.path.join(script_dir_path, "env.py"))
        config = Config()
        config.set_main_option("script_location", script_dir_path)
        config.set_main_option("sqlalchemy.url", TestIntegrationViews.STARROCKS_URI)
        yield config
        shutil.rmtree(script_dir_path)

    def test_full_autogenerate_and_upgrade(self, alembic_env: Config) -> None:
        config: Config = alembic_env
        engine = self.engine
        view_name = "integration_test_view"
        with engine.connect() as conn:
            conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
            try:
                # 1. Initial state to add a view
                target_metadata = MetaData()
                view = View(view_name, "SELECT 1 AS val", target_metadata, comment="Integration test view")
                mc: MigrationContext = MigrationContext.configure(
                    connection=conn,
                    opts={'target_metadata': target_metadata}
                )
                migration_script: api.MigrationScript = api.produce_migrations(mc, target_metadata)

                # 2. Verify the script and upgrade
                assert len(migration_script.upgrade_ops.ops) == 1
                create_op: CreateViewOp = migration_script.upgrade_ops.ops[0]
                assert isinstance(create_op, CreateViewOp)
                assert create_op.view_name == view_name

                # 3. Apply the migration
                op = Operations(mc)
                for op_item in migration_script.upgrade_ops.ops:
                    op.invoke(op_item)

                # 4. Verify the view exists in the database
                inspector = inspect(conn)
                views: list[str] = inspector.get_view_names()
                logger.info(f"inspected created views : {views}")
                assert view_name in views

                # 5. Downgrade and verify the view is dropped
                for op_item in migration_script.downgrade_ops.ops:
                    op.invoke(op_item)
                inspector = inspect(conn)
                views = inspector.get_view_names()
                assert view_name not in views
            finally:
                conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_full_autogenerate_and_alter(self, alembic_env: Config) -> None:
        config: Config = alembic_env
        engine = self.engine
        view_name = "integration_test_alter_view"
        with engine.connect() as conn:
            initial_ddl = f"""
            CREATE OR REPLACE VIEW {view_name}
            (c1 COMMENT 'col 1')
            COMMENT 'Initial version'
            SECURITY INVOKER
            AS SELECT 1 AS c1
            """
            conn.execute(text(initial_ddl))
            try:
                # 1. Initial state to alter a view
                target_metadata = MetaData()
                altered_view = View(
                    view_name,
                    "SELECT 2 AS new_c1, 3 AS new_c2",
                    target_metadata,
                    comment="Altered version",
                    security="DEFINER",
                    columns=[
                        {'name': 'new_c1', 'comment': 'new col 1'},
                        {'name': 'new_c2', 'comment': 'new col 2'},
                    ]
                )
                mc: MigrationContext = MigrationContext.configure(
                    connection=conn,
                    opts={'target_metadata': target_metadata}
                )
                migration_script = api.produce_migrations(mc, target_metadata)

                # 2. Verify the script and upgrade
                assert len(migration_script.upgrade_ops.ops) == 1
                op_item = migration_script.upgrade_ops.ops[0]
                assert isinstance(op_item, AlterViewOp)
                assert op_item.view_name == view_name

                # 3. Apply the migration
                op = Operations(mc)
                for op_to_run in migration_script.upgrade_ops.ops:
                    op.invoke(op_to_run)

                # 4. Verify the view exists in the database
                inspector = inspect(conn)
                view_info = inspector.get_view(view_name)
                assert view_info is not None
                logger.info(f"view_info.definition: {view_info.definition}")
                assert normalize_sql("SELECT 2 AS new_c1, 3 AS new_c2") == normalize_sql(view_info.definition)

                # 5. Downgrade and verify the view is altered
                for op_item in migration_script.downgrade_ops.ops:
                    op.invoke(op_item)
                inspector = inspect(conn)
                view_info = inspector.get_view(view_name)
                assert view_info is not None
                logger.info(f"view_info.definition: {view_info.definition}")
                assert normalize_sql("SELECT 1 AS c1") == normalize_sql(view_info.definition)
            finally:
                conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
