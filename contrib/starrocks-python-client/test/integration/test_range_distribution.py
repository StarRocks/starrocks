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

In shared-data mode StarRocks 4.1+ gives RANGE distribution to a table that declares
a key type or ORDER BY but no DISTRIBUTED BY clause. There is no DISTRIBUTED BY RANGE
syntax, so a model that leaves DISTRIBUTED BY unset (or sets it to 'RANGE') matches such
a table, and CREATE TABLE omits the clause.

Each connection opts in with the ``enable_range_distribution`` session variable, which
applies in any run mode, so these tests run on a StarRocks 4.1+ cluster and skip on
older ones.
"""

from __future__ import annotations

import logging
from unittest.mock import Mock

from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps
import pytest
from sqlalchemy import Column, Integer, MetaData, Table, event, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.schema import CreateTable

from starrocks.alembic.compare import compare_starrocks_table
from starrocks.common.params import TableInfoKeyWithPrefix
from test.conftest_sr import create_test_engine


logger = logging.getLogger(__name__)

SCHEMA = "sr_range_dist_test"


def _opt_in_to_range_distribution(dbapi_conn, _record) -> None:
    cursor = dbapi_conn.cursor()
    try:
        cursor.execute("SET enable_range_distribution = true")
    except Exception as exc:  # not a StarRocks 4.1+ cluster; the probe below skips
        logger.info("enable_range_distribution not settable: %s", exc)
    finally:
        cursor.close()


def _range_distribution_available(engine: Engine) -> bool:
    """Whether a table created without DISTRIBUTED BY gets RANGE distribution here."""
    with engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS `{SCHEMA}`")
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS `{SCHEMA}`.`probe_range`")
        try:
            conn.exec_driver_sql(
                f"CREATE TABLE `{SCHEMA}`.`probe_range` (k1 INT, v INT) DUPLICATE KEY(k1)"
            )
        except DatabaseError as exc:
            logger.info("range probe table not creatable: %s", exc)
            return False
    return _distribute_type(engine, "probe_range") == "RANGE"


def _distribute_type(engine: Engine, table: str) -> str:
    with engine.connect() as conn:
        return str(conn.exec_driver_sql(
            "SELECT DISTRIBUTE_TYPE FROM information_schema.tables_config "
            f"WHERE TABLE_SCHEMA = '{SCHEMA}' AND TABLE_NAME = '{table}'"
        ).scalar()).upper()


@pytest.mark.integration
class TestRangeDistribution:
    engine: Engine

    @classmethod
    def setup_class(cls) -> None:
        cls.engine = create_test_engine()
        event.listen(cls.engine, "connect", _opt_in_to_range_distribution)
        cls.engine.dispose()  # drop pooled connections opened before the listener
        if not _range_distribution_available(cls.engine):
            with cls.engine.begin() as conn:
                conn.exec_driver_sql(f"DROP DATABASE IF EXISTS `{SCHEMA}`")
            cls.engine.dispose()
            pytest.skip("cluster does not support RANGE distribution (needs StarRocks 4.1+)")

    @classmethod
    def teardown_class(cls) -> None:
        with cls.engine.begin() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS `{SCHEMA}`")
        cls.engine.dispose()

    def _create_range_table(self, name: str) -> Table:
        with self.engine.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS `{SCHEMA}`.`{name}`")
            conn.exec_driver_sql(
                f"CREATE TABLE `{SCHEMA}`.`{name}` (k1 INT, v INT) DUPLICATE KEY(k1)"
            )
        return Table(name, MetaData(), autoload_with=self.engine, schema=SCHEMA)

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
            schema=SCHEMA, **kwargs,
        )

    def _compare(self, reflected: Table, target: Table) -> list:
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect = self.engine.dialect
        autogen_context.inspector = inspect(self.engine)
        upgrade_ops = UpgradeOps()
        compare_starrocks_table(autogen_context, upgrade_ops, SCHEMA, reflected.name, reflected, target)
        return upgrade_ops.ops

    def test_reflects_range(self):
        reflected = self._create_range_table("t_reflect")
        assert reflected.dialect_options["starrocks"]["distributed_by"] == "RANGE"

    @pytest.mark.parametrize("distributed_by", [None, "RANGE"])
    def test_autogenerate_no_change(self, distributed_by):
        reflected = self._create_range_table("t_no_change")
        assert self._compare(reflected, self._target(reflected, distributed_by)) == []

    def test_autogenerate_range_to_hash_raises(self):
        reflected = self._create_range_table("t_to_hash")
        with pytest.raises(NotImplementedError, match="RANGE-distributed table"):
            self._compare(reflected, self._target(reflected, "HASH(k1) BUCKETS 4"))

    def test_create_table_with_range_marker(self):
        """A model marked RANGE compiles without DISTRIBUTED BY and yields a RANGE table."""
        name = "t_create"
        table = Table(
            name, MetaData(), Column("k1", Integer), Column("v", Integer),
            schema=SCHEMA,
            starrocks_duplicate_key="k1",
            starrocks_distributed_by="RANGE",
        )
        with self.engine.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS `{SCHEMA}`.`{name}`")
            ddl = str(CreateTable(table).compile(dialect=self.engine.dialect))
            assert "DISTRIBUTED BY" not in ddl
            conn.execute(CreateTable(table))
        assert _distribute_type(self.engine, name) == "RANGE"
