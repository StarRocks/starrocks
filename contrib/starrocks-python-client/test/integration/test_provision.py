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

from types import SimpleNamespace
from typing import Iterator
import uuid

from sqlalchemy import Column, Integer, Table, exc, inspect, testing
from sqlalchemy.engine import make_url
from sqlalchemy.testing import config, engines, fixtures
from sqlalchemy.testing.assertions import eq_, expect_raises
from sqlalchemy.testing.provision import create_db, drop_all_schema_objects_pre_tables, drop_db, generate_driver_url


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

    def test_dropfirst_drops_the_views_and_materialized_views_of_all_three_databases(self, ident):
        """``--dropfirst`` runs this hook before SQLAlchemy drops the views it knows of, then the tables."""
        create_db(config, config.db, ident)
        schemas = [ident, f"{ident}_test_schema", f"{ident}_test_schema_2"]
        with config.db.begin() as conn:
            for schema in schemas:
                for statement in (
                    f"CREATE TABLE `{schema}`.t (id INT) DISTRIBUTED BY HASH(id) PROPERTIES ('replication_num' = '1')",
                    f"CREATE VIEW `{schema}`.v AS SELECT id FROM `{schema}`.t",
                    f"CREATE VIEW `{schema}`.vv AS SELECT id FROM `{schema}`.v",
                    f"CREATE MATERIALIZED VIEW `{schema}`.mv REFRESH MANUAL PROPERTIES ('replication_num' = '1') "
                    f"AS SELECT id FROM `{schema}`.t",
                ):
                    conn.exec_driver_sql(statement)
        engine = engines.testing_engine(url=config.db.url.set(database=ident))
        worker_config = SimpleNamespace(db=engine, test_schema=schemas[1], test_schema_2=schemas[2])

        drop_all_schema_objects_pre_tables(worker_config, engine)

        inspector = inspect(engine)
        eq_(
            {schema: inspector.get_view_names(schema=schema) + inspector.get_materialized_view_names(schema=schema)
             for schema in schemas},
            {schema: [] for schema in schemas},
        )


class PrimaryKeyInferenceProvisionTest(fixtures.TestBase):
    """The test URL turns ``starrocks_infer_primary_key`` on, as SQLAlchemy's and Alembic's suites need."""

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.combinations(
        ("configured_engine", lambda: config.db),
        ("engine_given_options", lambda: engines.testing_engine(options={"pool_size": 1})),
        argnames="engine",
        id_="ia",
    )
    def test_the_test_engines_make_primary_key_tables(self, engine, metadata):
        """SQLAlchemy 1.4's ``testing_engine`` leaves out the configured engine options when it is given options."""
        engine = engine()
        Table("pk_provision", metadata, Column("id", Integer, primary_key=True), Column("v", Integer))
        metadata.create_all(engine)
        with engine.connect() as conn:
            ddl = conn.exec_driver_sql("SHOW CREATE TABLE pk_provision").fetchone()[1]
        assert "PRIMARY KEY(`id`)" in ddl, ddl

    def test_a_dburi_that_gives_the_option_keeps_its_value(self):
        url = generate_driver_url(make_url("starrocks://root@host:9030/db?starrocks_infer_primary_key=false"), "pymysql", "")
        eq_((url.drivername, url.query["starrocks_infer_primary_key"]), ("starrocks+pymysql", "false"))
