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

"""Integration tests for run_mode detection, including for low-privilege users.

``@@run_mode`` (StarRocks PR #69247, backported to 4.0/4.1) needs no privilege;
older servers only expose run_mode through ``ADMIN SHOW FRONTEND CONFIG``, which
needs OPERATE. Tests branch on which one the server under test supports.
"""

import logging
from typing import Any

from sqlalchemy import exc, testing, text
from sqlalchemy.engine import Engine
from sqlalchemy.testing import config, engines, fixtures

from starrocks.common.types import SystemRunMode


READ_ONLY_PASSWORD = "sqla_run_mode_ro_pw"


def _frontend_config_run_mode(root_engine: Engine) -> str:
    """The authoritative run_mode, read with root's OPERATE privilege."""
    with root_engine.connect() as conn:
        rows = conn.execute(text("ADMIN SHOW FRONTEND CONFIG LIKE 'run_mode'")).fetchall()
    return rows[0][2].lower()


def _supports_run_mode_variable(root_engine: Engine) -> bool:
    try:
        with root_engine.connect() as conn:
            conn.execute(text("SELECT @@run_mode"))
        return True
    except exc.DBAPIError:
        return False


def _connect_and_capture_warnings(engine: Engine, caplog: Any) -> list:
    """Connect for the first time (which runs dialect.initialize) and return dialect warnings."""
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="starrocks.dialect"):
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    return [r for r in caplog.records if r.name == "starrocks.dialect" and r.levelno >= logging.WARNING]


class RunModeTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    @testing.fixture
    def read_only_engine(self):
        """An engine for a user holding only SELECT on the test database.

        The user is named after the test database, so parallel workers do not share it.
        """
        database = config.db.url.database
        user = f"sqla_ro_{database}"
        with config.db.begin() as conn:
            conn.execute(text(f"DROP USER IF EXISTS '{user}'@'%'"))
            conn.execute(text(f"CREATE USER '{user}'@'%' IDENTIFIED BY '{READ_ONLY_PASSWORD}'"))
            conn.execute(text(f"GRANT SELECT ON ALL TABLES IN DATABASE {database} TO '{user}'@'%'"))
        engine = engines.testing_engine(url=config.db.url.set(username=user, password=READ_ONLY_PASSWORD))
        try:
            yield engine
        finally:
            engine.dispose()
            with config.db.begin() as conn:
                conn.execute(text(f"DROP USER IF EXISTS '{user}'@'%'"))

    def test_root_detects_run_mode(self):
        assert config.db.dialect.run_mode == _frontend_config_run_mode(config.db)

    def test_read_only_user_detects_run_mode_without_warning(self, read_only_engine, caplog):
        if not _supports_run_mode_variable(config.db):
            config.skip_test("server has no @@run_mode variable (added in StarRocks 4.0)")

        warnings = _connect_and_capture_warnings(read_only_engine, caplog)

        assert read_only_engine.dialect.run_mode == _frontend_config_run_mode(config.db)
        assert not warnings, [r.getMessage() for r in warnings]

    def test_read_only_user_falls_back_to_default_on_old_server(self, read_only_engine, caplog):
        """Neither source is readable without OPERATE, so the dialect warns and falls back to the
        shared_nothing default rather than failing to connect."""
        if _supports_run_mode_variable(config.db):
            config.skip_test("server has @@run_mode; the fallback path is not reached")

        warnings = _connect_and_capture_warnings(read_only_engine, caplog)

        assert read_only_engine.dialect.run_mode == SystemRunMode.SHARED_NOTHING
        assert any("Failed to get run_mode" in r.getMessage() for r in warnings)
