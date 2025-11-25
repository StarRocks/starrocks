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

from alembic.autogenerate import api
from alembic.runtime.migration import MigrationContext
import pytest
from sqlalchemy import Engine, MetaData, text

from starrocks.alembic.compare import combine_include_object
from starrocks.alembic.ops import (
    AlterMaterializedViewOp,
    CreateMaterializedViewOp,
    DropMaterializedViewOp,
)
from starrocks.sql.schema import MaterializedView
from test import conftest_sr


logger = logging.getLogger(__name__)


def create_migration_context(conn, target_metadata: MetaData) -> MigrationContext:
    """Create a MigrationContext with standard view/MV configuration."""
    def _exclude_tables(object, name, type_, reflected, compare_to):
        # Exclude tables from autogenerate in MV tests to avoid unrelated DropTableOps
        if type_ == "table":
            return False
        return True
    return MigrationContext.configure(
        connection=conn,
        opts={
            "target_metadata": target_metadata,
            "include_object": combine_include_object(_exclude_tables),
        },
    )


class TestAutogenerateBase:
    """Base class for autogenerate integration tests."""
    engine: Engine

    @classmethod
    def setup_class(cls):
        cls.engine = conftest_sr.create_test_engine()
        logger.debug("start to setup class")
        with cls.engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS t_autogen"))
            conn.execute(text(
                "CREATE TABLE t_autogen (val INT) "
                "DISTRIBUTED BY HASH(val) "
                "PROPERTIES ('replication_num' = '1')"
            ))

    @classmethod
    def teardown_class(cls):
        logger.debug("start to teardown class")
        with cls.engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS t_autogen"))
        cls.engine.dispose()


class TestCreateMaterializedView(TestAutogenerateBase):
    """CREATE MATERIALIZED VIEW related tests."""

    def test_basic_mv_creation(self) -> None:
        """CREATE: Basic MV creation."""
        engine = self.engine
        mv_name = "test_create_mv_basic"
        with engine.connect() as conn:
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
            try:
                target_metadata = MetaData()
                MaterializedView(
                    mv_name,
                    target_metadata,
                    definition="SELECT val FROM t_autogen",
                    starrocks_refresh="MANUAL",
                )
                mc = create_migration_context(conn, target_metadata)
                logger.debug("start to produce migrations")
                migration_script = api.produce_migrations(mc, target_metadata)
                logger.debug("migration_script: %s", migration_script)

                assert len(migration_script.upgrade_ops.ops) == 1
                create_op = migration_script.upgrade_ops.ops[0]
                assert isinstance(create_op, CreateMaterializedViewOp)
                assert create_op.view_name == mv_name
            finally:
                conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_comprehensive_mv_creation(self) -> None:
        """CREATE: Comprehensive MV with all attributes."""
        engine = self.engine
        mv_name = "test_create_mv_comprehensive"
        with engine.connect() as conn:
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
            try:
                target_metadata = MetaData()
                MaterializedView(
                    mv_name,
                    target_metadata,
                    definition="SELECT val FROM t_autogen",
                    comment="Test Comment",
                    starrocks_partition_by="val",
                    starrocks_distributed_by="HASH(val)",
                    starrocks_order_by="val",
                    starrocks_refresh="ASYNC",
                    starrocks_properties={"replication_num": "1"},
                )
                mc = create_migration_context(conn, target_metadata)
                migration_script = api.produce_migrations(mc, target_metadata)

                assert len(migration_script.upgrade_ops.ops) == 1
                op = migration_script.upgrade_ops.ops[0]
                assert isinstance(op, CreateMaterializedViewOp)
                assert op.comment == "Test Comment"
                assert op.kwargs["starrocks_partition_by"] == "val"
                assert op.kwargs["starrocks_distributed_by"] == "HASH(val)"
                assert op.kwargs["starrocks_order_by"] == "val"
                assert op.kwargs["starrocks_refresh"] == "ASYNC"
                assert op.kwargs["starrocks_properties"] == {"replication_num": "1"}
            finally:
                conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))


class TestDropMaterializedView(TestAutogenerateBase):
    """DROP MATERIALIZED VIEW related tests."""

    def test_basic_mv_drop(self) -> None:
        """DROP: Basic MV deletion."""
        engine = self.engine
        mv_name = "test_drop_mv_basic"
        with engine.connect() as conn:
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
            conn.execute(text(f"CREATE MATERIALIZED VIEW {mv_name} REFRESH MANUAL AS SELECT val FROM t_autogen"))
            try:
                target_metadata = MetaData()  # Empty metadata
                mc = create_migration_context(conn, target_metadata)
                migration_script = api.produce_migrations(mc, target_metadata)

                assert len(migration_script.upgrade_ops.ops) == 1
                drop_op = migration_script.upgrade_ops.ops[0]
                assert isinstance(drop_op, DropMaterializedViewOp)
                assert drop_op.view_name == mv_name
            finally:
                conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))


class TestAlterMaterializedView(TestAutogenerateBase):
    """ALTER MATERIALIZED VIEW related tests."""

    def test_alter_mutable_attributes(self) -> None:
        """ALTER: Change mutable attributes (refresh, properties)."""
        engine = self.engine
        mv_name = "test_alter_mv_mutable"
        with engine.connect() as conn:
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
            conn.execute(text(f"CREATE MATERIALIZED VIEW {mv_name} REFRESH ASYNC AS SELECT val FROM t_autogen"))
            try:
                target_metadata = MetaData()
                MaterializedView(
                    mv_name,
                    target_metadata,
                    definition="SELECT val FROM t_autogen",
                    starrocks_refresh="MANUAL",
                    starrocks_properties={"replication_num": "2"},
                )
                mc = create_migration_context(conn, target_metadata)
                migration_script = api.produce_migrations(mc, target_metadata)

                assert len(migration_script.upgrade_ops.ops) == 2
                alter_op_refresh = migration_script.upgrade_ops.ops[0]
                alter_op_properties = migration_script.upgrade_ops.ops[1]
                assert isinstance(alter_op_refresh, AlterMaterializedViewOp)
                assert isinstance(alter_op_properties, AlterMaterializedViewOp)
                assert alter_op_refresh.refresh == "MANUAL"
                assert alter_op_properties.properties == {"replication_num": "2"}
            finally:
                conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))

    def test_alter_immutable_attributes(self) -> None:
        """ALTER: Change immutable attributes (definition) triggers DROP/CREATE."""
        engine = self.engine
        mv_name = "test_alter_mv_immutable"
        with engine.connect() as conn:
            conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
            conn.execute(text(f"CREATE MATERIALIZED VIEW {mv_name} REFRESH MANUAL AS SELECT val FROM t_autogen"))
            try:
                target_metadata = MetaData()
                MaterializedView(mv_name, target_metadata, definition="SELECT val + 1 AS val FROM t_autogen")
                mc = create_migration_context(conn, target_metadata)
                with pytest.raises(NotImplementedError):
                    api.produce_migrations(mc, target_metadata)
            finally:
                conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"))
