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

"""
Integration tests for StarRocks ALTER TABLE comparison logic.

GOAL: Test the end-to-end flow of:
1. Reflecting real StarRocks table structure
2. Comparing with target metadata
3. Generating correct ALTER TABLE operations

This verifies that our comparison logic works with real StarRocks reflection data,
not just mocked objects.

TEST SCOPE:
- Focus on the 5 core comparison scenarios
- Verify reflection + comparison accuracy
- NOT testing actual ALTER execution (some operations aren't supported)

These tests require a real StarRocks database connection.
Set STARROCKS_URL environment variable to run these tests.

Example: STARROCKS_URL=starrocks://user:pass@localhost:9030/test_db
"""

import logging
import textwrap
from typing import Optional
from unittest.mock import Mock

from alembic.autogenerate import comparators
from alembic.operations.ops import ModifyTableOps, UpgradeOps
import pytest
from sqlalchemy import Column, Engine, Integer, MetaData, String, Table

from starrocks.alembic.compare import compare_starrocks_table
from starrocks.common.params import AlterTableEnablement, TableInfoKeyWithPrefix
from starrocks.common.types import PartitionType
from starrocks.engine.interfaces import ReflectedPartitionInfo
from test.conftest_sr import create_test_engine, test_default_schema
from test.unit import test_utils


logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestAlterTableIntegration:
    """
    Integration tests for ALTER TABLE comparison logic.

    Tests table options in StarRocks CREATE TABLE grammar order:
    - Engine changes (not supported, logged as error)
    - Key changes (not supported, logged as error)
    - Comment changes (handled by Alembic built-in), but we should check it
    - Partition changes (not supported, logged as error)
    - Distribution changes detection
    - Order by changes detection
    - Properties changes detection (most common)
    - No changes detection (normalization testing)
    - Multiple changes detection (comprehensive scenario)

    Uses real reflected table data instead of mocks.
    """

    engine: Engine
    test_schema: Optional[str]

    @classmethod
    def setup_class(cls) -> None:
        """Set up test class with StarRocks connection."""
        cls.engine = create_test_engine()
        cls.test_schema = test_default_schema

        # Create test schema if it doesn't exist
        with cls.engine.connect() as conn:
            try:
                conn.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS {cls.test_schema}")
                conn.commit()
            except Exception as e:
                pytest.skip(f"Could not create test schema: {e}")

    @classmethod
    def teardown_class(cls) -> None:
        """Clean up test resources."""
        with cls.engine.connect() as conn:
            try:
                # Clean up test tables from default database
                result = conn.exec_driver_sql("SHOW TABLES")
                for row in result:
                    table_name = row[0]

                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table_name}")
                conn.commit()
            except Exception:
                pass  # Ignore cleanup errors
        cls.engine.dispose()

    def _get_full_table_name(self, table_name: str) -> str:
        return f"{self.test_schema}.{table_name}" if self.test_schema else table_name

    def _setup_autogen_context(self) -> Mock:
        """Helper to create AutogenContext for testing."""
        from alembic.autogenerate.api import AutogenContext
        from sqlalchemy import inspect
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect = self.engine.dialect
        autogen_context.inspector = inspect(self.engine)
        return autogen_context

    def test_table_comment_change_detection(self) -> None:
        """
        Test: Table comment change detection from one value to another.
        """
        table_name = "test_comment_change"
        with self.engine.connect() as conn:
            conn.exec_driver_sql(
                f"""CREATE TABLE {self._get_full_table_name(table_name)} (id INT)
                COMMENT 'old comment'
                PROPERTIES("replication_num" = "1")"""
            )
            conn.commit()
            try:
                metadata_db = MetaData()
                reflected_table = Table(table_name, metadata_db, autoload_with=self.engine, schema=self.test_schema)
                assert reflected_table.comment == 'old comment'

                metadata_target = MetaData()
                target_table = Table(
                    table_name, metadata_target,
                    Column('id', Integer),
                    comment='new comment',
                    schema=self.test_schema,
                    starrocks_PROPERTIES={"replication_num": "1"}
                )

                autogen_context = self._setup_autogen_context()
                modify_table_ops = ModifyTableOps(table_name, [], schema=self.test_schema)

                # Call the table comparator dispatcher which includes comment comparison
                comparators.dispatch("table")(
                    autogen_context,
                    modify_table_ops,
                    self.test_schema,
                    table_name,
                    reflected_table,
                    target_table
                )

                result = modify_table_ops.ops

                assert len(result) == 1
                op = result[0]
                # Alembic uses CreateTableCommentOp for comment changes
                from alembic.operations.ops import CreateTableCommentOp
                assert isinstance(op, CreateTableCommentOp)
                logger.debug(f"op of comment change: {op!r}")
                assert op.table_name == table_name
                assert op.comment == 'new comment'
                assert op.existing_comment == 'old comment'
            finally:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self._get_full_table_name(table_name)}")
                conn.commit()

    def test_table_comment_addition_detection(self) -> None:
        """
        Test: Table comment addition (from no comment to having one).
        """
        table_name = "test_comment_addition"
        with self.engine.connect() as conn:
            conn.exec_driver_sql(f"CREATE TABLE {self._get_full_table_name(table_name)} (id INT) PROPERTIES(\"replication_num\" = \"1\")")
            conn.commit()
            try:
                metadata_db = MetaData()
                reflected_table = Table(table_name, metadata_db, autoload_with=self.engine, schema=self.test_schema)
                assert reflected_table.comment is None

                metadata_target = MetaData()
                target_table = Table(
                    table_name, metadata_target,
                    Column('id', Integer),
                    comment='new comment',
                    schema=self.test_schema,
                    starrocks_PROPERTIES={"replication_num": "1"}
                )

                autogen_context = self._setup_autogen_context()
                modify_table_ops = ModifyTableOps(table_name, [], schema=self.test_schema)

                # Call the table comparator dispatcher which includes comment comparison
                comparators.dispatch("table")(
                    autogen_context,
                    modify_table_ops,
                    self.test_schema,
                    table_name,
                    reflected_table,
                    target_table
                )

                result = modify_table_ops.ops

                assert len(result) == 1
                op = result[0]
                from alembic.operations.ops import CreateTableCommentOp
                assert isinstance(op, CreateTableCommentOp)
                assert op.table_name == table_name
                assert op.comment == 'new comment'
                assert op.existing_comment is None
            finally:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self._get_full_table_name(table_name)}")
                conn.commit()

    def test_table_comment_removal_detection(self) -> None:
        """
        Test: Table comment removal.
        """
        table_name = "test_comment_removal"
        with self.engine.connect() as conn:
            conn.exec_driver_sql(
                f"""CREATE TABLE {self._get_full_table_name(table_name)} (id INT)
                COMMENT 'old comment'
                PROPERTIES("replication_num" = "1")"""
            )
            conn.commit()
            try:
                metadata_db = MetaData()
                reflected_table = Table(table_name, metadata_db, autoload_with=self.engine, schema=self.test_schema)
                assert reflected_table.comment == 'old comment'

                metadata_target = MetaData()
                target_table = Table(
                    table_name, metadata_target,
                    Column('id', Integer),
                    comment=None,
                    schema=self.test_schema,
                    starrocks_PROPERTIES={"replication_num": "1"}
                )

                autogen_context = self._setup_autogen_context()
                modify_table_ops = ModifyTableOps(table_name, [], schema=self.test_schema)

                # Call the table comparator dispatcher which includes comment comparison
                comparators.dispatch("table")(
                    autogen_context,
                    modify_table_ops,
                    self.test_schema,
                    table_name,
                    reflected_table,
                    target_table
                )

                result = modify_table_ops.ops

                assert len(result) == 1
                op = result[0]
                from alembic.operations.ops import DropTableCommentOp
                assert isinstance(op, DropTableCommentOp)
                assert op.table_name == table_name
                assert op.existing_comment == 'old comment'
            finally:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self._get_full_table_name(table_name)}")
                conn.commit()

    def test_table_comment_no_change_detection(self) -> None:
        """
        Test: No change in table comment.
        """
        table_name = "test_comment_no_change"
        with self.engine.connect() as conn:
            conn.exec_driver_sql(
                f"""CREATE TABLE {self._get_full_table_name(table_name)} (id INT)
                COMMENT 'same comment'
                PROPERTIES("replication_num" = "1")"""
            )
            conn.commit()
            try:
                metadata_db = MetaData()
                reflected_table = Table(table_name, metadata_db, autoload_with=self.engine, schema=self.test_schema)
                assert reflected_table.comment == 'same comment'

                metadata_target = MetaData()
                target_table = Table(
                    table_name, metadata_target,
                    Column('id', Integer),
                    comment='same comment',
                    schema=self.test_schema,
                    starrocks_PROPERTIES={"replication_num": "1"}
                )

                autogen_context = self._setup_autogen_context()
                modify_table_ops = ModifyTableOps(table_name, [], schema=self.test_schema)

                # Call the table comparator dispatcher which includes comment comparison
                comparators.dispatch("table")(
                    autogen_context,
                    modify_table_ops,
                    self.test_schema,
                    table_name,
                    reflected_table,
                    target_table
                )

                result = modify_table_ops.ops

                assert len(result) == 0
            finally:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self._get_full_table_name(table_name)}")
                conn.commit()

    def test_distribution_change_detection(self) -> None:
        """
        Test: Distribution change detection with real reflection.

        Scenario (distribution changes):
        - Changed: distribution from HASH(id) BUCKETS 4 to HASH(user_id) BUCKETS 8
        - No change: properties remain the same
        - Expected: 1 AlterTableDistributionOp generated
        """
        table_name = "test_distribution_change"

        with self.engine.connect() as conn:
            # Create table with initial distribution
            full_table_name = self._get_full_table_name(table_name)
            conn.exec_driver_sql(f"""
                CREATE TABLE {full_table_name} (
                    id INT,
                    user_id INT
                )
                DISTRIBUTED BY HASH(id) BUCKETS 4
                PROPERTIES ("replication_num" = "1")
            """)
            conn.commit()

            try:
                # Reflect actual table
                metadata_db = MetaData()
                reflected_table = Table(
                    table_name, metadata_db,
                    autoload_with=self.engine,
                    schema=self.test_schema
                )

                # Target: different distribution
                metadata_target = MetaData()
                target_table = Table(
                    table_name, metadata_target,
                    Column('id', Integer),
                    Column('user_id', Integer),
                    **{
                        "starrocks_DISTRIBUTED_BY": "HASH(user_id) BUCKETS 8",
                        "starrocks_PROPERTIES": {"replication_num": "1"}
                    },
                    schema=self.test_schema
                )

                # Compare
                autogen_context = self._setup_autogen_context()
                upgrade_ops = UpgradeOps()
                compare_starrocks_table(
                    autogen_context, upgrade_ops, self.test_schema, table_name, reflected_table, target_table
                )
                result = upgrade_ops.ops

                # Should detect distribution change
                assert len(result) == 1
                from starrocks.alembic.ops import AlterTableDistributionOp
                op: AlterTableDistributionOp = result[0]
                assert isinstance(op, AlterTableDistributionOp)
                assert op.table_name == table_name
                assert op.distribution_method == "HASH(user_id)"
                assert op.buckets == 8

            finally:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self.test_schema}.{table_name}")
                conn.commit()

    def test_partition_change_detection(self) -> None:
        """
        Test: Partition change detection with real reflection.

        Scenario (partition changes):
        - Changed: from RANGE partition by from_unixtime(dt) to RANGE partition by dt with one partition.
        - Expected: 1 AlterTablePartitionOp generated.
        """
        table_name = "test_partition_change"

        with self.engine.connect() as conn:
            # Create table with no partitioning initially
            conn.exec_driver_sql(textwrap.dedent(f"""
                CREATE TABLE {table_name} (
                    id INT,
                    dt INT
                )
                PARTITION BY RANGE( from_unixtime(dt ) ) (
                    PARTITION p1 VALUES [('2023-01-01'), ('2023-02-01'))
                )
                DISTRIBUTED BY HASH(id) BUCKETS 4
                PROPERTIES ("replication_num" = "1")
            """))
            conn.commit()

            try:
                # Reflect actual table
                metadata_db = MetaData()
                reflected_table = Table(
                    table_name, metadata_db,
                    autoload_with=self.engine,
                    schema=self.test_schema
                )
                partition_info = reflected_table.kwargs.get(TableInfoKeyWithPrefix.PARTITION_BY)
                assert partition_info is not None and isinstance(partition_info, ReflectedPartitionInfo)
                assert partition_info.type == PartitionType.RANGE
                assert test_utils.normalize_sql(partition_info.partition_method) == "RANGE(from_unixtime(dt))"
                assert test_utils.normalize_sql(partition_info.pre_created_partitions) \
                        == test_utils.normalize_sql("(PARTITION p1 VALUES [('2023-01-01 00:00:00'), ('2023-02-01 00:00:00')))")

                # Target: add RANGE partition
                metadata_target = MetaData()
                target_table = Table(
                    table_name, metadata_target,
                    Column('id', Integer),
                    Column('dt', Integer), # Use int for date column in metadata for simplicity
                    **{
                        "starrocks_PARTITION_BY": "RANGE(dt) (PARTITION p1 VALUES [('2025-01-01'), ('2025-02-01')))",
                        "starrocks_DISTRIBUTED_BY": "HASH(id) BUCKETS 4",
                        "starrocks_PROPERTIES": {"replication_num": "1"},
                    },
                    schema=self.test_schema
                )

                # Compare
                AlterTableEnablement.PARTITION_BY = True  # Enable partition comparison
                autogen_context = self._setup_autogen_context()
                upgrade_ops = UpgradeOps()
                compare_starrocks_table(
                    autogen_context, upgrade_ops, self.test_schema, table_name, reflected_table, target_table
                )
                result = upgrade_ops.ops
                AlterTableEnablement.PARTITION_BY = False  # Disable back to default

                # Should detect partition change
                assert len(result) == 1
                from starrocks.alembic.ops import AlterTablePartitionOp
                op: AlterTablePartitionOp = result[0]
                assert isinstance(op, AlterTablePartitionOp)
                assert op.table_name == table_name
                assert op.partition_method == "RANGE(dt)"
            finally:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self.test_schema}.{table_name}")
                conn.commit()

    def test_order_by_addition_detection(self) -> None:
        """
        Test: ORDER BY addition detection with real reflection.

        Scenario (order by changes):
        - Added: ORDER BY created_at (was not present before)
        - No change: distribution and properties remain the same
        - Expected: 1 AlterTableOrderOp generated
        """
        table_name = "test_order_addition"

        with self.engine.connect() as conn:
            # Create table without ORDER BY
            conn.exec_driver_sql(f"""
                CREATE TABLE {self.test_schema}.{table_name} (
                    id INT,
                    created_at DATETIME
                )
                DISTRIBUTED BY HASH(id)
                PROPERTIES ("replication_num" = "1")
            """)
            conn.commit()

            try:
                # Reflect actual table
                metadata_db = MetaData()
                reflected_table = Table(
                    table_name, metadata_db,
                    autoload_with=self.engine,
                    schema=self.test_schema
                )

                # Target: add ORDER BY
                metadata_target = MetaData()
                target_table = Table(
                    table_name, metadata_target,
                    Column('id', Integer),
                    Column('created_at', String),  # Simplified type for test
                    **{
                        "starrocks_DISTRIBUTED_BY": "HASH(id)",
                        "starrocks_ORDER_BY": "created_at",
                        "starrocks_PROPERTIES": {"replication_num": "1"}
                    },
                    schema=self.test_schema
                )

                # Compare
                autogen_context = self._setup_autogen_context()
                upgrade_ops = UpgradeOps()
                compare_starrocks_table(
                    autogen_context, upgrade_ops, self.test_schema, table_name, reflected_table, target_table
                )
                result = upgrade_ops.ops

                # Should detect ORDER BY addition
                assert len(result) == 1
                from starrocks.alembic.ops import AlterTableOrderOp
                op = result[0]
                assert isinstance(op, AlterTableOrderOp)
                assert op.order_by == "created_at"

            finally:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self.test_schema}.{table_name}")
                conn.commit()

    def test_properties_change_detection(self) -> None:
        """
        Test: Properties change detection with real reflection.

        Scenario (properties changes):
        - Changed: replication_num from "1" to "2"
        - Added: storage_medium = "SSD"
        - Expected: 1 AlterTablePropertiesOp generated
        """
        table_name = "test_properties_change"

        with self.engine.connect() as conn:
            # Create table with initial properties
            full_table_name: str = self._get_full_table_name(table_name)
            conn.exec_driver_sql(f"""
                CREATE TABLE {full_table_name} (
                    id INT,
                    name VARCHAR(50)
                )
                DISTRIBUTED BY HASH(id)
                PROPERTIES ("replication_num" = "1")
            """)
            conn.commit()

            try:
                # Reflect actual table from database
                metadata_db = MetaData()
                reflected_table = Table(
                    table_name, metadata_db,
                    autoload_with=self.engine,  # the table will directly be reflected from the database
                    schema=self.test_schema
                )

                # Define target state with different properties
                metadata_target = MetaData()
                target_table = Table(
                    table_name, metadata_target,
                    Column('id', Integer),
                    Column('name', String(50)),
                    **{
                        "starrocks_DISTRIBUTED_BY": "HASH(id)",
                        "starrocks_ORDER_BY": ["id", "name"],
                        "starrocks_PROPERTIES": {
                            "replication_num": "2",  # Changed
                            "storage_medium": "SSD"  # Added
                        }
                    },
                    schema=self.test_schema
                )

                # Debug: log what was reflected
                logger.info("=== REFLECTED TABLE INFO ===")
                logger.info("reflected_table.kwargs (dict): %s", dict(reflected_table.kwargs))
                logger.info("target_table.kwargs (dict): %s", dict(target_table.kwargs))

                # Check if properties are in the expected location
                reflected_props: dict = reflected_table.kwargs.get(TableInfoKeyWithPrefix.PROPERTIES, {})
                target_props: dict = target_table.kwargs.get(TableInfoKeyWithPrefix.PROPERTIES, {})
                logger.info("reflected_props: %s", reflected_props)
                logger.info("target_props: %s", target_props)

                # Compare and verify
                autogen_context: Mock = self._setup_autogen_context()
                upgrade_ops = UpgradeOps()
                compare_starrocks_table(
                    autogen_context, upgrade_ops, self.test_schema, table_name, reflected_table, target_table
                )
                result = upgrade_ops.ops

                # Debug: log operations generated
                logger.info("=== OPERATIONS GENERATED ===")
                for i, op in enumerate(result):
                    logger.info("%d: %s - %s", i, type(op).__name__, op.__dict__)

                # Should detect properties change
                assert len(result) == 1
                from starrocks.alembic.ops import AlterTablePropertiesOp
                op: AlterTablePropertiesOp = result[0]
                assert isinstance(op, AlterTablePropertiesOp)
                assert op.table_name == table_name
                assert op.schema == self.test_schema
                assert op.properties == {"default.replication_num": "2", "default.storage_medium": "SSD"}

            finally:
                # Clean up
                full_table_name: str = self._get_full_table_name(table_name)
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {full_table_name}")
                conn.commit()

    def test_no_changes_detection(self) -> None:
        """
        Test: No changes detection with real reflection.

        Scenario (normalization testing):
        - No change: distribution, properties match exactly (after normalization)
        - Backticks and format differences should be ignored
        - Expected: 0 operations generated
        """
        table_name = "test_no_changes"

        with self.engine.connect() as conn:
            # Create table
            conn.exec_driver_sql(f"""
                CREATE TABLE {self.test_schema}.{table_name} (
                    id INT,
                    name VARCHAR(50)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 8
                PROPERTIES ("replication_num" = "1")
            """)
            conn.commit()

            try:
                # Reflect actual table
                metadata_db = MetaData()
                reflected_table = Table(
                    table_name, metadata_db,
                    autoload_with=self.engine,
                    schema=self.test_schema
                )

                # For true no-change test, we should match what user would write
                # that results in the same logical structure
                metadata_target = MetaData()
                target_table = Table(
                    table_name, metadata_target,
                    Column('id', Integer),
                    Column('name', String(50)),
                    **{
                        # Use normalized format (without backticks) - should be equivalent
                        "starrocks_DISTRIBUTED_BY": "HASH(id)   BUCKETS    8",
                        # Only specify non-default properties that user cares about
                        "starrocks_PROPERTIES": {"replication_num": "1"}
                    },
                    schema=self.test_schema
                )

                # Debug: log what was reflected
                logger.info("=== REFLECTED TABLE INFO ===")
                logger.info("reflected_table.kwargs (dict): %s", dict(reflected_table.kwargs))
                logger.info("target_table.kwargs (dict): %s", dict(target_table.kwargs))

                # Compare
                autogen_context = self._setup_autogen_context()
                upgrade_ops = UpgradeOps()
                compare_starrocks_table(
                    autogen_context, upgrade_ops, self.test_schema, table_name, reflected_table, target_table
                )
                result = upgrade_ops.ops

                # Debug: log operations generated
                logger.info("=== OPERATIONS GENERATED ===")
                for i, op in enumerate(result):
                    logger.info("%d: %s - %s", i, type(op).__name__, op.__dict__)

                # Should detect NO changes
                assert len(result) == 0, f"Expected no changes, but got: {result}"

            finally:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self.test_schema}.{table_name}")
                conn.commit()

    def test_multiple_changes_detection(self) -> None:
        """
        Test: Multiple changes detection with real reflection.

        Scenario (comprehensive changes in grammar order):
        - Changed: distribution from HASH(id) BUCKETS 4 to RANDOM BUCKETS 8
        - Added: ORDER BY id
        - Changed: properties replication_num from "1" to "2"
        - Added: properties storage_medium = "SSD"
        - Expected: 3 operations (AlterTableDistributionOp, AlterTableOrderOp, AlterTablePropertiesOp)
        """
        table_name = "test_multiple_changes"

        with self.engine.connect() as conn:
            # Create table with baseline state
            conn.exec_driver_sql(f"""
                CREATE TABLE {self.test_schema}.{table_name} (
                    id INT,
                    user_id INT
                )
                DISTRIBUTED BY HASH(id) BUCKETS 4
                PROPERTIES ("replication_num" = "1")
            """)
            conn.commit()

            try:
                # Reflect actual table
                metadata_db = MetaData()
                reflected_table = Table(
                    table_name, metadata_db,
                    autoload_with=self.engine,
                    schema=self.test_schema
                )

                # Target: multiple changes
                metadata_target = MetaData()
                target_table = Table(
                    table_name, metadata_target,
                    Column('id', Integer),
                    Column('user_id', Integer),
                    **{
                        "starrocks_DISTRIBUTED_BY": "RANDOM BUCKETS 8",  # Changed
                        "starrocks_ORDER_BY": "id",  # Added
                        "starrocks_PROPERTIES": {  # Changed
                            "replication_num": "2",
                            "storage_medium": "SSD"
                        }
                    },
                    schema=self.test_schema
                )

                # Compare
                autogen_context: Mock = self._setup_autogen_context()
                upgrade_ops = UpgradeOps()
                compare_starrocks_table(
                    autogen_context, upgrade_ops, self.test_schema, table_name, reflected_table, target_table
                )
                result = upgrade_ops.ops

                # Should detect 3 changes
                assert len(result) == 3

                from starrocks.alembic.ops import AlterTableDistributionOp, AlterTableOrderOp, AlterTablePropertiesOp
                # op_types: list = [type(op) for op in result]
                distribution_op: AlterTableDistributionOp = result[0]
                assert distribution_op.distribution_method == "RANDOM"
                assert distribution_op.buckets == 8
                order_op: AlterTableOrderOp = result[1]
                assert order_op.order_by == "id"
                properties_op: AlterTablePropertiesOp = result[2]
                assert properties_op.properties == {"default.replication_num": "2", "default.storage_medium": "SSD"}

            finally:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self.test_schema}.{table_name}")
                conn.commit()
