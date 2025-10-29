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

from __future__ import annotations

import logging
from pathlib import Path
import shutil
import tempfile
from typing import Generator, NamedTuple
import uuid

from alembic import command
from alembic.config import Config
import pytest
from sqlalchemy import Engine, MetaData, create_engine, text


logger = logging.getLogger(__name__)


class AlembicTestHarness:
    """A wrapper around Alembic commands for testing."""

    def __init__(self, env: AlembicTestEnv):
        self.env = env

    def ensure_version_table(self) -> None:
        """
        Ensures the alembic_version table exists and is stamped with the latest version.
        This is crucial to prevent "revision not found" errors during autogenerate
        on a clean database.
        """
        command.stamp(self.env.alembic_cfg, "head")

    def generate_autogen_revision(self, metadata: MetaData, message: str) -> None:
        """
        Dynamically sets the target metadata and runs the autogenerate revision.
        """
        # Ensure the version table is created and stamped before autogenerate
        self.ensure_version_table()
        # Inject the metadata into the Alembic script context
        self.env.alembic_cfg.attributes['target_metadata'] = metadata
        self.revision(message=message, autogenerate=True)

    def revision(self, message: str, autogenerate: bool = False) -> None:
        """Runs the `alembic revision` command."""
        command.revision(
            self.env.alembic_cfg,
            message=message,
            autogenerate=autogenerate,
        )

    def upgrade(self, revision: str) -> None:
        """Runs the `alembic upgrade` command."""
        command.upgrade(self.env.alembic_cfg, revision)

    def downgrade(self, revision: str) -> None:
        """Runs the `alembic downgrade` command."""
        command.downgrade(self.env.alembic_cfg, revision)


class AlembicTestEnv(NamedTuple):
    """A helper object to manage the Alembic test environment."""
    root_path: Path
    alembic_cfg: Config
    harness: AlembicTestHarness


@pytest.fixture(scope="function")
def sr_engine(sr_root_engine: Engine, database: str) -> Engine:
    """A function-scoped engine that connects to the temporary test database."""
    url = sr_root_engine.url.set(database=database)
    engine = create_engine(url, pool_pre_ping=True)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def alembic_env(sr_engine: Engine):
    """
    Creates a temporary, isolated Alembic environment for a test.
    """
    temp_dir = tempfile.mkdtemp()
    test_root = Path(temp_dir)
    logger.info(f"test_root: {test_root}")

    # Copy the template files directly to the test root
    template_path = Path(__file__).parent / "templates"
    shutil.copytree(template_path, test_root / "alembic")

    # Create the versions directory
    versions_path = test_root / "alembic/versions"
    versions_path.mkdir()

    # Create alembic.ini
    alembic_ini_path = test_root / "alembic.ini"
    with open(alembic_ini_path, "w") as f:
        f.write(f"""
[alembic]
script_location = {test_root / "alembic"}
sqlalchemy.url = {sr_engine.url.render_as_string(hide_password=False)}
prepend_sys_path = .

[loggers]
keys = root,sqlalchemy,alembic,starrocks,test

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = INFO
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[logger_starrocks]
level = DEBUG
handlers =
qualname = starrocks

[logger_test]
level = DEBUG
handlers =
qualname = test

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
# format = %(levelname)-5.5s [%(name)s] %(message)s
format = %(asctime)s %(levelname)-5.5s [%(name)s.%(funcName)s:%(lineno)d] %(message)s
datefmt = %H:%M:%S
""")

    # Create Alembic Config object
    alembic_cfg = Config(str(alembic_ini_path))
    alembic_cfg.set_main_option("version_locations", str(versions_path))

    # The actual test environment object
    env = AlembicTestEnv(
        root_path=test_root,
        alembic_cfg=alembic_cfg,
        harness=None,  # type: ignore
    )

    # Create and attach the harness
    harness = AlembicTestHarness(env)
    env = env._replace(harness=harness)

    yield env

    # Teardown: remove the temporary directory
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="function")
def database(sr_root_engine: Engine) -> Generator[str, None, None]:
    """
    Creates a new database for each test function and drops it afterwards.
    """
    db_name = f"test_db_{uuid.uuid4().hex}"
    logger.info(f"Creating new test database: {db_name}")
    with sr_root_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE {db_name}"))
        conn.commit()

    yield db_name

    with sr_root_engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE {db_name} FORCE"))
        conn.commit()
