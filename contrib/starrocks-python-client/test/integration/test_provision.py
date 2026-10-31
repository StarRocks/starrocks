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

"""The testing plugin's hooks for each worker's databases, run against databases of the test's own."""

from typing import Iterator
import uuid

from sqlalchemy import exc, inspect, testing
from sqlalchemy.testing import config, fixtures
from sqlalchemy.testing.assertions import eq_, expect_raises
from sqlalchemy.testing.provision import create_db, drop_db


class WorkerDatabasesTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    @testing.fixture
    def ident(self) -> Iterator[str]:
        """A worker ident of the test's own, whose three databases are dropped afterwards."""
        ident = f"{config.db.url.database}_prov_{uuid.uuid4().hex[:8]}"
        yield ident
        with config.db.begin() as conn:
            for database in (ident, f"{ident}_test_schema", f"{ident}_test_schema_2"):
                conn.exec_driver_sql(f"DROP DATABASE IF EXISTS `{database}` FORCE")

    def test_create_db_replaces_the_databases_an_earlier_run_left(self, ident):
        create_db(config, config.db, ident)
        with config.db.begin() as conn:
            conn.exec_driver_sql(
                f"CREATE TABLE `{ident}`.leftover (id INT) DISTRIBUTED BY HASH(id) PROPERTIES ('replication_num' = '1')"
            )

        create_db(config, config.db, ident)

        eq_(inspect(config.db).get_table_names(schema=ident), [])

    def test_drop_db_leaves_no_tables_in_the_recycle_bin(self, ident):
        """A database dropped without FORCE can be recovered with its tables, which keep their tablets until then."""
        create_db(config, config.db, ident)
        with config.db.begin() as conn:
            conn.exec_driver_sql(
                f"CREATE TABLE `{ident}`.dropped (id INT) DISTRIBUTED BY HASH(id) PROPERTIES ('replication_num' = '1')"
            )

        drop_db(config, config.db, ident)

        with config.db.connect() as conn:
            with expect_raises(exc.DBAPIError):
                conn.exec_driver_sql(f"RECOVER DATABASE `{ident}`")
