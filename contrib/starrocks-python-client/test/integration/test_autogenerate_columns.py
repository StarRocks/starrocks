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
from typing import Any, Union
from unittest.mock import Mock

from alembic.autogenerate.api import AutogenContext
import pytest
from sqlalchemy import (
    BIGINT,
    Column,
    MetaData,
    Table,
)
from sqlalchemy.engine import Engine

from starrocks.alembic.compare import compare_starrocks_column_autoincrement
from test.conftest_sr import create_test_engine, test_default_schema


logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestCompareColumnAggTypeIntegration:
    """Compare agg_type column."""

    engine: Engine
    schema: Union[str, None]

    @classmethod
    def setup_class(cls) -> None:
        """Create table before all tests."""
        cls.engine = create_test_engine()
        cls.schema = test_default_schema
        with cls.engine.begin() as conn:
            if cls.schema:
                conn.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS {cls.schema}")

    @classmethod
    def teardown_class(cls) -> None:
        """Drop table after all tests."""
        with cls.engine.begin() as conn:
            # Drop materialized views first
            mvs = conn.exec_driver_sql(f"SELECT table_name FROM information_schema.materialized_views WHERE table_schema = '{cls.schema}'")
            for row in mvs:
                conn.exec_driver_sql(f"DROP MATERIALIZED VIEW IF EXISTS {cls.schema}.{row[0]}")
            # Drop views
            views = conn.exec_driver_sql(f"SELECT table_name FROM information_schema.views WHERE table_schema = '{cls.schema}'")
            for row in views:
                conn.exec_driver_sql(f"DROP VIEW IF EXISTS {cls.schema}.{row[0]}")
            # Drop tables
            tables = conn.exec_driver_sql(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{cls.schema}' AND table_type = 'BASE TABLE'")
            for row in tables:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {cls.schema}.{row[0]}")
        cls.engine.dispose()

    def _full_table_name(self, name: str) -> str:
        """Return full table name with schema."""
        return f"{self.schema}.{name}" if self.schema else name

    def _setup_autogen_context(self) -> Mock:
        """Helper to create AutogenContext for testing."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect = self.engine.dialect
        return autogen_context

    def test_compare_agg_type_no_changes(self) -> None:
        """Test agg_type no changes."""
        tname = "compare_agg_type_no_changes"
        full_tname = self._full_table_name(tname)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {full_tname}")
            conn.exec_driver_sql(
                f"""
                CREATE TABLE {full_tname} (
                    id BIGINT,
                    v BIGINT SUM
                )
                AGGREGATE KEY(id)
                DISTRIBUTED BY HASH(id)
                PROPERTIES("replication_num"="1")
                """
            )
        try:
            conn_db = MetaData()
            conn_table = Table(tname, conn_db, autoload_with=self.engine, schema=self.schema)

            meta_db = MetaData()
            from starrocks.common.types import ColumnAggType
            meta_table = Table(
                tname, meta_db,
                Column("id", BIGINT),
                Column("v", BIGINT, starrocks_agg_type=ColumnAggType.SUM),
                schema=self.schema,
                starrocks_distributed_by='HASH(id)',
            )

            autogen_context = self._setup_autogen_context()
            from starrocks.alembic.compare import compare_starrocks_column_agg_type
            compare_starrocks_column_agg_type(autogen_context, None, self.schema, tname, "v",
                conn_table.columns["v"], meta_table.columns["v"])

        finally:
            with self.engine.begin() as conn:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {full_tname}")

    def test_compare_agg_type_changes_raises(self) -> None:
        """Test agg_type changes raises NotSupportedError."""
        tname = "compare_agg_type_changes_raises"
        full_tname = self._full_table_name(tname)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {full_tname}")
            conn.exec_driver_sql(
                f"""
                CREATE TABLE {full_tname} (
                    id BIGINT,
                    v BIGINT SUM
                )
                AGGREGATE KEY(id)
                DISTRIBUTED BY HASH(id)
                PROPERTIES("replication_num"="1")
                """
            )
        try:
            conn_db = MetaData()
            conn_table = Table(tname, conn_db, autoload_with=self.engine, schema=self.schema)

            meta_db = MetaData()
            from starrocks.common.types import ColumnAggType
            meta_table = Table(
                tname, meta_db,
                Column("id", BIGINT),
                Column("v", BIGINT, starrocks_agg_type=ColumnAggType.REPLACE),
                schema=self.schema,
                starrocks_distributed_by='HASH(id)',
            )

            autogen_context = self._setup_autogen_context()
            from sqlalchemy.exc import NotSupportedError

            from starrocks.alembic.compare import compare_starrocks_column_agg_type
            with pytest.raises(NotSupportedError, match="does not support changing the aggregation type"):
                compare_starrocks_column_agg_type(autogen_context, None, self.schema, tname, "v",
                    conn_table.columns["v"], meta_table.columns["v"])

        finally:
            with self.engine.begin() as conn:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {full_tname}")


@pytest.mark.integration
class TestCompareColumnAutoIncrementIntegration:
    """Compare autoincrement column."""

    engine: Engine
    schema: Union[str, None]

    @classmethod
    def setup_class(cls) -> None:
        """Create table before all tests."""
        cls.engine = create_test_engine()
        cls.schema = test_default_schema
        with cls.engine.begin() as conn:
            if cls.schema:
                conn.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS {cls.schema}")

    @classmethod
    def teardown_class(cls) -> None:
        """Drop table after all tests."""
        with cls.engine.begin() as conn:
            # Drop materialized views first
            mvs = conn.exec_driver_sql(f"SELECT table_name FROM information_schema.materialized_views WHERE table_schema = '{cls.schema}'")
            for row in mvs:
                conn.exec_driver_sql(f"DROP MATERIALIZED VIEW IF EXISTS {cls.schema}.{row[0]}")
            # Drop views
            views = conn.exec_driver_sql(f"SELECT table_name FROM information_schema.views WHERE table_schema = '{cls.schema}'")
            for row in views:
                conn.exec_driver_sql(f"DROP VIEW IF EXISTS {cls.schema}.{row[0]}")
            # Drop tables
            tables = conn.exec_driver_sql(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{cls.schema}' AND table_type = 'BASE TABLE'")
            for row in tables:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {cls.schema}.{row[0]}")
        cls.engine.dispose()

    def _full_table_name(self, name: str) -> str:
        """Return full table name with schema."""
        return f"{self.schema}.{name}" if self.schema else name

    def _setup_autogen_context(self) -> Mock:
        """Helper to create AutogenContext for testing."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect = self.engine.dialect
        return autogen_context

    def test_compare_autoincrement_no_changes(self, caplog: Any) -> None:
        """Test autoincrement no changes."""
        tname = "compare_autoincrement_no_changes"
        full_tname = self._full_table_name(tname)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {full_tname}")
            conn.exec_driver_sql(
                f"""
                CREATE TABLE {full_tname} (
                    id BIGINT NOT NULL AUTO_INCREMENT
                )
                DISTRIBUTED BY RANDOM
                PROPERTIES("replication_num"="1")
                """
            )
        try:
            # Create reflected table from database
            conn_db = MetaData()
            conn_table = Table(tname, conn_db, autoload_with=self.engine, schema=self.schema)

            # Create target table with matching autoincrement
            meta_db = MetaData()
            meta_table = Table(
                tname, meta_db,
                Column("id", BIGINT, autoincrement=True),
                schema=self.schema,
                starrocks_properties={
                    "replication_num": "1"
                }
            )

            # Compare using compare_starrocks_table
            autogen_context = self._setup_autogen_context()
            # result = compare_starrocks_table(autogen_context, conn_table, meta_table)
            result = compare_starrocks_column_autoincrement(autogen_context, None, self.schema, tname, "id",
                conn_table.columns["id"], meta_table.columns["id"])

            # Should detect no changes
            assert not result
        finally:
            with self.engine.begin() as conn:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {full_tname}")

    def test_compare_autoincrement_changes_warns(self, caplog: Any) -> None:
        """Test autoincrement changes warns."""
        tname = "compare_autoincrement_changes_warns"
        full_tname = self._full_table_name(tname)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {full_tname}")
            conn.exec_driver_sql(
                f"""
                CREATE TABLE {full_tname} (
                    -- id BIGINT NOT NULL AUTO_INCREMENT
                    id BIGINT NOT NULL  -- can't reflected from the database now
                )
                DISTRIBUTED BY RANDOM
                PROPERTIES("replication_num"="1")
                """
            )
        try:
            # Create reflected table from database
            conn_db = MetaData()
            conn_table = Table(tname, conn_db, autoload_with=self.engine, schema=self.schema)

            # Create target table with different autoincrement (True)
            meta_db = MetaData()
            meta_table = Table(
                tname, meta_db,
                Column("id", BIGINT, autoincrement=True),
                schema=self.schema,
                starrocks_properties={
                    "replication_num": "1"
                }
            )

            # Compare using compare_starrocks_table
            autogen_context = self._setup_autogen_context()
            caplog.set_level("WARNING")
            # result = compare_starrocks_table(autogen_context, conn_table, meta_table)
            result = compare_starrocks_column_autoincrement(autogen_context, None, self.schema, tname, "id",
                conn_table.columns["id"], meta_table.columns["id"])

            # Should detect no changes but log warning
            assert not result
            assert any(
                "Detected AUTO_INCREMENT is changed" in str(r.getMessage())
                and r.levelname == "WARNING"
                for r in caplog.records
            )
        finally:
            with self.engine.begin() as conn:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {full_tname}")
