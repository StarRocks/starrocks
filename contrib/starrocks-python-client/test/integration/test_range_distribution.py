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

"""Reflection, autogenerate comparison and CREATE TABLE for RANGE distribution.

With range distribution enabled (opt-in on StarRocks 4.1; the shared-data default in
newer releases), a table that declares a key type or ORDER BY but no DISTRIBUTED BY
clause gets RANGE distribution. There is no DISTRIBUTED BY RANGE
syntax, so a model that leaves DISTRIBUTED BY unset (or sets it to 'RANGE') matches such
a table, and CREATE TABLE omits the clause.

Each connection of the class's own engine opts in with the ``enable_range_distribution``
session variable, which applies in any run mode, so these tests run on a StarRocks 4.1+
cluster and skip on older ones. The plugin's engine never sets it, so other tests keep the
server's default distribution. The tables live in the plugin's alternate test schema, which
each parallel worker has its own of.
"""

import logging
from unittest.mock import Mock

from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps
from sqlalchemy import Column, Integer, MetaData, Table, event, inspect, testing
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DatabaseError
from sqlalchemy.schema import CreateTable
from sqlalchemy.testing import config, engines, expect_raises_message, fixtures

from starrocks.alembic.compare import compare_starrocks_table
from starrocks.common.params import TableInfoKeyWithPrefix


logger = logging.getLogger(__name__)


def _opt_in_to_range_distribution(dbapi_conn, _record) -> None:
    """Turns range distribution on for a new connection, leaving a server without the variable to the probe."""
    cursor = dbapi_conn.cursor()
    try:
        cursor.execute("SET enable_range_distribution = true")
    except Exception as exc:
        logger.info("enable_range_distribution not settable: %s", exc)
    finally:
        cursor.close()


def _distribute_type(connection: Connection, table: str) -> str:
    return str(connection.exec_driver_sql(
        "SELECT DISTRIBUTE_TYPE FROM information_schema.tables_config "
        f"WHERE TABLE_SCHEMA = '{config.test_schema}' AND TABLE_NAME = '{table}'"
    ).scalar()).upper()


def _range_distribution_available(connection: Connection) -> bool:
    """Whether a table created without DISTRIBUTED BY gets RANGE distribution here."""
    probe = f"`{config.test_schema}`.`probe_range`"
    connection.exec_driver_sql(f"DROP TABLE IF EXISTS {probe}")
    try:
        connection.exec_driver_sql(f"CREATE TABLE {probe} (k1 INT, v INT) DUPLICATE KEY(k1)")
    except DatabaseError as exc:
        logger.info("range probe table not creatable: %s", exc)
        return False
    try:
        return _distribute_type(connection, "probe_range") == "RANGE"
    finally:
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {probe}")


class RangeDistributionTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "starrocks"

    @classmethod
    def setup_test_class(cls):
        cls.engine = engines.testing_engine(options={"scope": "class"})
        event.listen(cls.engine, "connect", _opt_in_to_range_distribution)
        with cls.engine.begin() as connection:
            if not _range_distribution_available(connection):
                cls.engine.dispose()
                config.skip_test("cluster does not support RANGE distribution (needs StarRocks 4.1+)")

    @classmethod
    def teardown_test_class(cls):
        cls.engine.dispose()

    @testing.fixture
    def tables_to_drop(self):
        """The names of the tables a test creates in the test schema, which are dropped after it."""
        names: list[str] = []
        yield names
        with self.engine.begin() as connection:
            for name in names:
                connection.exec_driver_sql(f"DROP TABLE IF EXISTS `{config.test_schema}`.`{name}` FORCE")

    def _create_range_table(self, name: str, tables_to_drop: list[str]) -> Table:
        with self.engine.begin() as connection:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS `{config.test_schema}`.`{name}`")
            tables_to_drop.append(name)
            connection.exec_driver_sql(
                f"CREATE TABLE `{config.test_schema}`.`{name}` (k1 INT, v INT) DUPLICATE KEY(k1)"
            )
        return Table(name, MetaData(), autoload_with=self.engine, schema=config.test_schema)

    def _target(self, reflected: Table, distributed_by=None) -> Table:
        """The reflected table's definition, with DISTRIBUTED BY replaced."""
        kwargs = {
            k: v for k, v in reflected.dialect_kwargs.items()
            if k.lower() != TableInfoKeyWithPrefix.DISTRIBUTED_BY
        }
        if distributed_by is not None:
            kwargs[TableInfoKeyWithPrefix.DISTRIBUTED_BY] = distributed_by
        return Table(
            reflected.name, MetaData(), Column("k1", Integer), Column("v", Integer),
            schema=config.test_schema, **kwargs,
        )

    def _compare(self, reflected: Table, target: Table) -> list:
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect = self.engine.dialect
        autogen_context.inspector = inspect(self.engine)
        upgrade_ops = UpgradeOps()
        compare_starrocks_table(
            autogen_context, upgrade_ops, config.test_schema, reflected.name, reflected, target
        )
        return upgrade_ops.ops

    def test_reflects_range(self, tables_to_drop):
        reflected = self._create_range_table("t_reflect", tables_to_drop)
        assert reflected.dialect_options["starrocks"]["distributed_by"] == "RANGE"

    @testing.combinations((None,), ("RANGE",), argnames="distributed_by")
    def test_autogenerate_no_change(self, distributed_by, tables_to_drop):
        reflected = self._create_range_table("t_no_change", tables_to_drop)
        assert self._compare(reflected, self._target(reflected, distributed_by)) == []

    def test_autogenerate_range_to_hash_raises(self, tables_to_drop):
        reflected = self._create_range_table("t_to_hash", tables_to_drop)
        with expect_raises_message(NotImplementedError, "RANGE-distributed table"):
            self._compare(reflected, self._target(reflected, "HASH(k1) BUCKETS 4"))

    def test_create_table_with_range_marker(self, tables_to_drop):
        """A model marked RANGE compiles without DISTRIBUTED BY and yields a RANGE table."""
        name = "t_create"
        table = Table(
            name, MetaData(), Column("k1", Integer), Column("v", Integer),
            schema=config.test_schema,
            starrocks_duplicate_key="k1",
            starrocks_distributed_by="RANGE",
        )
        with self.engine.begin() as connection:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS `{config.test_schema}`.`{name}`")
            ddl = str(CreateTable(table).compile(dialect=self.engine.dialect))
            assert "DISTRIBUTED BY" not in ddl
            tables_to_drop.append(name)
            connection.execute(CreateTable(table))
            assert _distribute_type(connection, name) == "RANGE"
