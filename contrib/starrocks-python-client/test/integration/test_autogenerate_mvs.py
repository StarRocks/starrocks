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
from alembic.runtime.migration import MigrationContext
import pytest
from sqlalchemy import Engine, MetaData, text

from starrocks.alembic.ops import CreateMaterializedViewOp
from starrocks.sql.schema import MaterializedView
from test import conftest_sr


logger = logging.getLogger(__name__)


"""
It will use reflection to get the materialized view definition from the database, and compare it
with the materialized view definition in the metadata.

So, it needs a integration test environment.
"""


@pytest.mark.skip(reason="Skipping mvs test for now")
class TestIntegrationMVs:
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
        config.set_main_option("sqlalchemy.url", TestIntegrationMVs.STARROCKS_URI)
        yield config
        shutil.rmtree(script_dir_path)

    def test_full_autogenerate_and_upgrade(self, alembic_env: Config) -> None:
        config: Config = alembic_env
        engine = self.engine
        mv_name = "integration_test_mv"
        with engine.connect() as conn:
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
            try:
                # 1. Initial state to add a materialized view
                target_metadata = MetaData()
                mv = MaterializedView(mv_name, "SELECT 1 AS val", target_metadata, comment="Integration test mv")
                mc: MigrationContext = MigrationContext.configure(
                    connection=conn,
                    opts={'target_metadata': target_metadata}
                )
                migration_script: api.MigrationScript = api.produce_migrations(mc, target_metadata)

                # 2. Verify the script
                assert len(migration_script.upgrade_ops.ops) == 1
                create_op: CreateMaterializedViewOp = migration_script.upgrade_ops.ops[0]
                assert isinstance(create_op, CreateMaterializedViewOp)
                assert create_op.view_name == mv_name

            finally:
                conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
