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
from unittest.mock import Mock, PropertyMock

from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps
import pytest
from sqlalchemy.exc import NotSupportedError

from starrocks.alembic.compare import compare_starrocks_table, extract_starrocks_dialect_attributes
from starrocks.common.defaults import ReflectionTableDefaults
from starrocks.common.params import (
    AlterTableEnablement,
    DialectName,
    SRKwargsPrefix,
    TableInfoKeyWithPrefix,
    TablePropertyForFuturePartitions,
)
from starrocks.common.types import TableType


LOG_ATTRIBUTE_NEED_SPECIFIED = "Please specify this attribute explicitly"
LOG_NO_DEFAULT_VALUE = "no default is defined in ReflectionTableDefaults"
LOG_ALTER_AUTO_GENERATED = "An ALTER TABLE SET operation will be generated"
LOG_NO_ALERT_AUTO_GENERATED = "No ALTER TABLE SET operation will be generated"


logger = logging.getLogger(__name__)


class TestRealTableObjects:
    """Tests using real SQLAlchemy Table objects (without database connection).

    Other test cases in this file, like TestEngineChanges, are using Mock objects
    """

    def test_real_table_schema_diff(self):
        """Test real table schema diff generation with actual Table objects."""
        from unittest.mock import Mock

        from alembic.autogenerate.api import AutogenContext
        from sqlalchemy import Column, Integer, MetaData, String, Table

        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        # Create real metadata and table objects
        metadata_old = MetaData()
        metadata_new = MetaData()

        # Table as it exists in database (reflected)
        conn_table = Table(
            'users', metadata_old,
            Column('id', Integer),
            Column('name', String(50)),
            starrocks_DISTRIBUTED_BY='HASH(id) BUCKETS 8',
            starrocks_PROPERTIES={'replication_num': '2'},
            schema='test_db'
        )

        # Table as defined in new metadata (target state)
        meta_table = Table(
            'users', metadata_new,
            Column('id', Integer),
            Column('name', String(50)),
            starrocks_DISTRIBUTED_BY='HASH(id, name) BUCKETS 16',
            starrocks_ORDER_BY='id, name',
            starrocks_PROPERTIES={'replication_num': '3', 'storage_medium': 'SSD'},
            schema='test_db'
        )

        # Test the comparison
        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops

        # Should detect multiple changes
        assert len(result) == 3  # distribution, properties, order_by

        from starrocks.alembic.ops import AlterTableDistributionOp, AlterTableOrderOp, AlterTablePropertiesOp
        op_types = [type(op) for op in result]
        assert AlterTablePropertiesOp in op_types
        assert AlterTableDistributionOp in op_types
        assert AlterTableOrderOp in op_types

        # Verify specific operation details
        for op in result:
            assert op.table_name == 'users'
            assert op.schema == 'test_db'

            if isinstance(op, AlterTableDistributionOp):
                assert op.distribution_method == 'HASH(id, name)'
                assert op.buckets == 16
                assert op.distributed_by == 'HASH(id, name) BUCKETS 16'
            elif isinstance(op, AlterTableOrderOp):
                assert op.order_by == 'id, name'
            elif isinstance(op, AlterTablePropertiesOp):
                assert op.properties == {'default.replication_num': '3', 'default.storage_medium': 'SSD'}

    def test_real_table_no_changes(self):
        """Test that identical real tables produce no operations."""
        from unittest.mock import Mock

        from alembic.autogenerate.api import AutogenContext
        from sqlalchemy import Column, Integer, MetaData, String, Table

        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        # Use separate MetaData for each table to avoid conflicts
        metadata_conn = MetaData()
        metadata_new = MetaData()

        # Create identical table objects
        table_kwargs = {
            'starrocks_ENGINE': 'OLAP',
            'starrocks_DISTRIBUTED_BY': 'HASH(id) BUCKETS 8',
            'starrocks_PROPERTIES': {'replication_num': '3'},
            'schema': 'test_db'
        }

        conn_table = Table(
            'users', metadata_conn,
            Column('id', Integer),
            Column('name', String(50)),
            **table_kwargs
        )

        meta_table_kwargs = {
            'starrocks_KEY': ReflectionTableDefaults.key(),
            'starrocks_DISTRIBUTED_BY': 'HASH(`id`)  BUCKETS   8',
            'schema': 'test_db'
        }

        meta_table = Table(
            'users', metadata_new,
            Column('id', Integer),
            Column('name', String(50)),
            **meta_table_kwargs
        )

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops

        # No changes should be detected
        assert result == []

    def test_real_table_with_table_args(self):
        """Test real table properties defined via __table_args__ (simulated by kwargs)."""
        from unittest.mock import Mock

        from alembic.autogenerate.api import AutogenContext
        from sqlalchemy import Column, Integer, MetaData, String, Table

        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        metadata_old = MetaData()
        metadata_new = MetaData()

        # Table as it exists in database (reflected)
        # Simulate table reflected from DB, where properties are typically present
        conn_table = Table(
            'products', metadata_old,
            Column('id', Integer),
            Column('name', String(50)),
            starrocks_ENGINE='OLAP',
            starrocks_DISTRIBUTED_BY='HASH(id) BUCKETS 8',
            starrocks_PROPERTIES={'replication_num': '3', 'compression': 'ZSTD'},
            schema='test_db'
        )

        # Table as defined in new metadata (target state), simulating __table_args__
        # Here we directly pass the starrocks_* kwargs, which is how they would be
        # ultimately processed even if originating from __table_args__ in an ORM model.
        meta_table = Table(
            'products', metadata_new,
            Column('id', Integer),
            Column('name', String(50)),
            starrocks_ENGINE='OLAP',
            starrocks_DISTRIBUTED_BY='HASH(id) BUCKETS 8',
            starrocks_PROPERTIES={'replication_num': '3', 'compression': 'LZ4'}, # Changed property
            schema='test_db'
        )

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops

        assert len(result) == 1
        from starrocks.alembic.ops import AlterTablePropertiesOp
        assert isinstance(result[0], AlterTablePropertiesOp)
        # only changed properties will be generated
        # assert result[0].properties == {'replication_num': '3', 'compression': 'LZ4'}
        assert result[0].properties == {'compression': 'LZ4'}


    def test_real_table_unsupported_engine_change(self):
        """Test unsupported ENGINE change using real Table objects."""
        from unittest.mock import Mock

        from alembic.autogenerate.api import AutogenContext
        from sqlalchemy import Column, Integer, MetaData, Table

        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        metadata_old = MetaData()
        metadata_new = MetaData()

        conn_table = Table(
            'users', metadata_old,
            Column('id', Integer),
            starrocks_ENGINE='OLAP',
            schema='test_db'
        )

        meta_table = Table(
            'users', metadata_new,
            Column('id', Integer),
            starrocks_ENGINE='MYSQL',
            schema='test_db'
        )

        with pytest.raises(NotImplementedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )
        assert "StarRocks does not support 'ALTER TABLE ENGINE'" in str(exc_info.value)

    def test_real_table_unsupported_key_change(self):
        """Test unsupported KEY change using real Table objects."""
        from unittest.mock import Mock

        from alembic.autogenerate.api import AutogenContext
        from sqlalchemy import Column, Integer, MetaData, Table

        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        metadata_old = MetaData()
        metadata_new = MetaData()

        conn_table = Table(
            'users', metadata_old,
            Column('id', Integer),
            starrocks_DUPLICATE_KEY='id',
            schema='test_db'
        )

        meta_table = Table(
            'users', metadata_new,
            Column('id', Integer),
            starrocks_PRIMARY_KEY='id',
            schema='test_db'
        )
        with pytest.raises(NotSupportedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )
        assert "not supported to change the key type" in str(exc_info.value)

    def test_real_table_unsupported_partition_change(self):
        """Test unsupported PARTITION_BY change using real Table objects."""
        from unittest.mock import Mock

        from alembic.autogenerate.api import AutogenContext
        from sqlalchemy import Column, Integer, MetaData, String, Table

        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        metadata_old = MetaData()
        metadata_new = MetaData()

        conn_table = Table(
            'users', metadata_old,
            Column('id', Integer),
            Column('name', String(50)),
            starrocks_PARTITION_BY='RANGE(id)',
            schema='test_db'
        )

        meta_table = Table(
            'users', metadata_new,
            Column('id', Integer),
            Column('name', String(50)),
            starrocks_PARTITION_BY='LIST(name)',
            schema='test_db'
        )

        with pytest.raises(NotImplementedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )
        assert "StarRocks does not support 'ALTER TABLE PARTITION BY'" in str(exc_info.value)


# Test cases organized by StarRocks grammar order:
# engine → key → comment → partition → distribution → order by → properties
class TestEngineChanges:
    """Test ENGINE attribute changes (ALTER ENGINE not supported, but changes are detected)."""

    def test_engine_none_to_default_value(self):
        """Test ENGINE from None to default value ('OLAP')."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # Table reflected from SR database, no ENGINE explicitly set, implies default OLAP
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata explicitly sets default OLAP
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.ENGINE: ReflectionTableDefaults.engine(),
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_engine_none_to_non_default_value(self):
        """Test ENGINE from None (implicit default OLAP) to a non-default value (MYSQL)."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # Table reflected from SR database, no ENGINE explicitly set, implies default OLAP
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata sets a non-default ENGINE
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.ENGINE: "MYSQL",
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        with pytest.raises(NotImplementedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )
        assert "StarRocks does not support 'ALTER TABLE ENGINE'" in str(exc_info.value)

    def test_engine_default_to_none(self):
        """Test ENGINE from default value ('OLAP') to None."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # Table reflected from SR database, explicitly OLAP
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.ENGINE: "OLAP",
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata does not specify ENGINE, implies default OLAP
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                # No ENGINE specified
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_engine_non_default_to_none(self):
        """Test ENGINE from a non-default value ('MYSQL') to None."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # Table reflected from SR database, explicitly MYSQL
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.ENGINE: "MYSQL",
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata does not specify ENGINE
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        with pytest.raises(NotImplementedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )
        assert LOG_ATTRIBUTE_NEED_SPECIFIED in str(exc_info.value)

    def test_engine_change(self):
        """Test ENGINE value changes ('OLAP' to 'MYSQL')."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.ENGINE: "OLAP",
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.ENGINE: "MYSQL",
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        # ENGINE changes are not supported in StarRocks, should raise NotImplementedError
        with pytest.raises(NotImplementedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )
        assert "StarRocks does not support 'ALTER TABLE ENGINE'" in str(exc_info.value)

    @pytest.mark.parametrize("conn_engine, meta_engine", [
        ("OLAP", "OLAP"),
        ("OLAP", "olap"),
        ("olap", "OLAP"),
    ])
    def test_engine_no_change(self, conn_engine, meta_engine):
        """Test ENGINE with no change."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.ENGINE: conn_engine,
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.ENGINE: meta_engine,
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_engine_none_to_none(self):
        """Test ENGINE from None to None (no change)."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # No ENGINE explicitly set in database
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # No ENGINE specified in metadata
            kwargs={TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0


class TestKeyChanges:
    """Test KEY attribute changes (ALTER KEY not supported, but changes are detected)."""

    def test_key_none_to_default_value(self):
        """Test KEY from None (implicit default DUPLICATE KEY) to default value ('DUPLICATE KEY')."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        # set default key to 'DUPLICATE KEY (id)' instead of only 'DUPLICATE KEY'
        def default_key(cls):
            return f"{TableType.DUPLICATE_KEY}(id)"
        ReflectionTableDefaults.key = classmethod(default_key)

        conn_table = Mock()  # Table reflected from SR database, no KEY explicitly set, implies default DUPLICATE KEY
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata explicitly sets default DUPLICATE KEY
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.DUPLICATE_KEY: 'id',
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_key_none_to_non_default_value(self):
        """Test KEY from None (implicit default DUPLICATE KEY) to a non-default value (PRIMARY KEY)."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # Table reflected from SR database, no KEY explicitly set, implies default DUPLICATE KEY
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata sets a non-default KEY
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.PRIMARY_KEY: "id",
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        with pytest.raises(NotSupportedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )
        assert "not supported to change the key type" in str(exc_info.value)

    def test_key_default_to_none(self):
        """Test KEY from default value ('DUPLICATE KEY') to None."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # Table reflected from SR database, explicitly DUPLICATE KEY
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.DUPLICATE_KEY: 'id',
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata does not specify KEY, implies default DUPLICATE KEY
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops

        # No change detected since meta doesn't specify KEY
        assert len(result) == 0

    def test_key_non_default_to_none(self):
        """Test KEY from a non-default value ('PRIMARY KEY') to None."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # Table reflected from SR database, explicitly PRIMARY KEY
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.PRIMARY_KEY: "id",
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata does not specify KEY
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        with pytest.raises(NotImplementedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )

        assert LOG_ATTRIBUTE_NEED_SPECIFIED in str(exc_info.value)

    def test_key_change(self):
        """Test KEY value changes ('DUPLICATE KEY' to 'UNIQUE KEY')."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.DUPLICATE_KEY: "id",
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.UNIQUE_KEY: "id",
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        with pytest.raises(NotSupportedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )
        assert "not supported to change the key type" in str(exc_info.value)

    @pytest.mark.parametrize("conn_key, conn_key_columns, meta_key, meta_key_columns", [
        ("UNIQUE_KEY", "id", "UNIQUE_key", "id"),
        ("UNIQUE_KEY", " ( id ) ", "unique_key", "id"),
        ("unique_key", "id", "UNIQUE_KEY", "id"),
        ("DUPLICATE_KEY", "(id, name)", "duplicate_key", "(id, name)"),
    ])
    def test_key_no_change(self, conn_key, conn_key_columns, meta_key, meta_key_columns):
        """Test KEY with no change."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")

        conn_key_str = f"{SRKwargsPrefix}{conn_key}"
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            conn_key_str: conn_key_columns,
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_key_str = f"{SRKwargsPrefix}{meta_key}"
        meta_table = Mock(
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                meta_key_str: meta_key_columns,
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_key_none_to_none(self):
        """Test KEY from None to None (no change)."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # No KEY explicitly set in database
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # No KEY specified in metadata
            kwargs={TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0


class TestCommentChanges:
    """Test COMMENT attribute changes (handled by Alembic's built-in logic)."""
    pass  # Comment comparison is handled by Alembic's _compare_table_comment


class TestPartitionChanges:
    """Test PARTITION_BY attribute changes (ALTER PARTITION not supported, but changes are detected)."""

    def test_partition_none_to_value(self):
        """Test PARTITION_BY from None to value."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # No PARTITION_BY explicitly set in database
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata sets PARTITION_BY
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.PARTITION_BY: "RANGE(date_col)",
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        with pytest.raises(NotImplementedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )
        assert "StarRocks does not support 'ALTER TABLE PARTITION BY'" in str(exc_info.value)

    def test_partition_value_to_none(self):
        """Test PARTITION_BY from value to None."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # Database has PARTITION_BY set
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.PARTITION_BY: "RANGE(date_col)",
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata does not specify PARTITION_BY
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        with pytest.raises(NotImplementedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )
        assert LOG_ATTRIBUTE_NEED_SPECIFIED in str(exc_info.value)

    def test_partition_change(self):
        """Test PARTITION_BY value changes."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.PARTITION_BY: "RANGE(date_col)",
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.PARTITION_BY: "LIST(category)",
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        with pytest.raises(NotImplementedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )
        assert "StarRocks does not support 'ALTER TABLE PARTITION BY'" in str(exc_info.value)

        # Force to make diff of partition by
        old_enablement = AlterTableEnablement.PARTITION_BY
        AlterTableEnablement.PARTITION_BY = True
        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 1
        from starrocks.alembic.ops import AlterTablePartitionOp
        assert isinstance(result[0], AlterTablePartitionOp)
        assert result[0].partition_method == "LIST(category)"
        AlterTableEnablement.PARTITION_BY = old_enablement

    @pytest.mark.parametrize("conn_partition, meta_partition", [
        ("RANGE(`date_col`)", "RANGE(date_col)"),
        ("RANGE( `date_col` )", "RANGE(date_col)"),
        ("RANGE(date_col)", "RANGE(date_col)"),
        ("RANGE( `date_col` , `id`)", "RANGE(date_col,id)"),
        ("range(`DATE_COL`)", "RANGE(date_col)"),
    ])
    def test_partition_no_change(self, conn_partition, meta_partition):
        """Test PARTITION_BY no change."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.PARTITION_BY: conn_partition,
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.PARTITION_BY: meta_partition,
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_partition_none_to_none(self):
        """Test PARTITION_BY from None to None (no change)."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # No PARTITION_BY explicitly set in database
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # No PARTITION_BY specified in metadata
            kwargs={TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0


class TestDistributionChanges:
    """Test DISTRIBUTED_BY attribute changes."""

    def test_distribution_none_to_default_value(self):
        """Test DISTRIBUTED_BY from None to default value ('RANDOM')."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # No DISTRIBUTED_BY explicitly set in database, implies default RANDOM
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata explicitly sets default RANDOM
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: ReflectionTableDefaults.distribution(),
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_distribution_none_to_non_default_value(self):
        """Test DISTRIBUTED_BY from None (implicit default RANDOM) to a non-default value."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # No DISTRIBUTED_BY explicitly set in database
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata sets a non-default DISTRIBUTED_BY
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id) BUCKETS 8",
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 1
        from starrocks.alembic.ops import AlterTableDistributionOp
        assert isinstance(result[0], AlterTableDistributionOp)
        assert result[0].distribution_method == "HASH(id)"
        assert result[0].buckets == 8

    def test_distribution_default_to_none(self):
        """Test DISTRIBUTED_BY from default value ('RANDOM') to None."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # Database explicitly has default RANDOM
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: ReflectionTableDefaults.distribution()}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata does not specify DISTRIBUTED_BY
            kwargs={}
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_distribution_non_default_to_none(self):
        """Test DISTRIBUTED_BY from a non-default value to None."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # Database has non-default DISTRIBUTED_BY
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id) BUCKETS 8"}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata does not specify DISTRIBUTED_BY
            kwargs={}
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        with pytest.raises(NotImplementedError) as exc_info:
            upgrade_ops = UpgradeOps([])
            compare_starrocks_table(
                autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
            )
        assert LOG_ATTRIBUTE_NEED_SPECIFIED in str(exc_info.value)

    def test_distribution_change(self):
        """Test DISTRIBUTED_BY value changes."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: ReflectionTableDefaults.distribution()}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(user_id) BUCKETS 16",
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 1
        from starrocks.alembic.ops import AlterTableDistributionOp
        assert isinstance(result[0], AlterTableDistributionOp)
        assert result[0].distribution_method == "HASH(user_id)"
        assert result[0].buckets == 16

    @pytest.mark.parametrize("conn_distribution, meta_distribution", [
        ("HASH(id) BUCKETS 8", "HASH(`id`)  BUCKETS   8"),
        ("HASH(`id`)", "HASH(id)"),
        ("HASH( `id` , `name`)", "HASH(id,name)"),
        ("RANDOM", "RANDOM"),
        ("hash(`ID`)", "HASH(id)"),
    ])
    def test_distribution_no_change(self, conn_distribution, meta_distribution):
        """Test DISTRIBUTED_BY with no change."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: conn_distribution}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: meta_distribution,
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_distribution_none_to_none(self):
        """Test DISTRIBUTED_BY from None to None (no change)."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # No DISTRIBUTED_BY explicitly set in database
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # No DISTRIBUTED_BY specified in metadata
            kwargs={}
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0


class TestOrderByChanges:
    """Test ORDER_BY attribute changes."""

    def test_order_by_none_to_value(self):
        """Test ORDER_BY from None to value.
        Impossible in reality, because ORDER BY is always set when the table is created.
        """
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # No ORDER_BY explicitly set in database
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata does not specify ORDER_BY (implying default)
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.ORDER_BY: "id, c2, c3"
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        # assert len(result) == 0  # impossible in reality

    def test_order_by_default_to_none(self):
        """Test ORDER_BY from implicit default to None (no op).
        Won't change without explicitly setting ORDER BY.
        """
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.ORDER_BY: "id, c2"
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # Metadata does not specify ORDER_BY, implies empty string
            kwargs={TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_order_by_change(self):
        """Test ORDER_BY value changes."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.ORDER_BY: "id, col3",
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.ORDER_BY: "col2, col3"
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 1
        from starrocks.alembic.ops import AlterTableOrderOp
        assert isinstance(result[0], AlterTableOrderOp)
        assert result[0].order_by == "col2, col3"

    @pytest.mark.parametrize("conn_order_by, meta_order_by", [
        ("id", "`id`"),
        ("`id`, `name`", "id, name"),
        (" id , name ", "id,name"),
        (["id", "name"], "id, name"),
        ("ID", "id"),
    ])
    def test_order_by_no_change(self, conn_order_by, meta_order_by):
        """Test ORDER_BY no change."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.ORDER_BY: conn_order_by,
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(
            kwargs={
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
                TableInfoKeyWithPrefix.ORDER_BY: meta_order_by,
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_order_by_none_to_none(self):
        """Test ORDER_BY from None to None (no change)."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock()  # No ORDER_BY explicitly set in database
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_db")
        conn_table.kwargs = {TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(  # No ORDER_BY specified in metadata
            kwargs={TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)"}
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0


class TestPropertiesChanges:
    """Test PROPERTIES attribute changes."""

    @pytest.mark.parametrize("prop_key, default_value, non_default_value", [
        ("replication_num", "3", "2"),
        ("storage_medium", "HDD", "SSD"),
    ])
    def test_properties_none_to_default_value(self, prop_key, default_value, non_default_value):
        """Test PROPERTIES from None (implicit default) to explicit default value."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock(kwargs={})  # No properties in DB, implies default
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}
        meta_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: {prop_key: default_value}})
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        # Our `_compare_properties` detects this as a change because metadata *explicitly* provides a value
        # even if it's the default. This is consistent with explicit metadata definitions.
        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, "test_table", "test_db", conn_table, meta_table
        )
        result = upgrade_ops.ops
        logger.debug(f"PROPERTIES. result: {result}")
        assert len(result) == 0  # on changes

    @pytest.mark.parametrize("prop_key, default_value, non_default_value", [
        ("replication_num", "3", "2"),
        ("storage_medium", "HDD", "SSD"),
    ])
    def test_properties_none_to_non_default_value(self, prop_key, default_value, non_default_value):
        """Test PROPERTIES from None (implicit default) to explicit non-default value."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock(kwargs={})  # No properties in DB, implies default
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}
        meta_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: {prop_key: non_default_value}})
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, "test_table", "test_db", conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 1
        from starrocks.alembic.ops import AlterTablePropertiesOp
        assert isinstance(result[0], AlterTablePropertiesOp)
        assert result[0].properties == {TablePropertyForFuturePartitions.wrap(prop_key): non_default_value}

    @pytest.mark.parametrize("prop_key, default_value, non_default_value", [
        ("replication_num", "3", "2"),
    ])
    def test_properties_default_to_none(self, prop_key, default_value, non_default_value):
        """Test PROPERTIES from explicit default value to None (no op)."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: {prop_key: default_value}})  # DB has explicit default
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}
        meta_table = Mock(kwargs={})  # Metadata has no properties
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        # If DB has default value and metadata has no value, it's considered no change
        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, "test_table", "test_db", conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    @pytest.mark.parametrize("prop_key, default_value, non_default_value", [
        ("colocate_with", None, "not_exists"),
    ])
    def test_properties_non_default_none_to_none(self, prop_key, default_value, non_default_value, caplog):
        """Test PROPERTIES from implicit default (default value is set to None) to None (no op)."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName
        caplog.set_level("WARNING")

        conn_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: {prop_key: non_default_value}})  # DB has explicit default
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}
        meta_table = Mock(kwargs={})  # Metadata has no properties
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        # If DB has default value and metadata has no value, it's considered no change
        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, "test_table", "test_db", conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0
        assert LOG_NO_DEFAULT_VALUE in caplog.text
        assert LOG_NO_ALERT_AUTO_GENERATED in caplog.text

    @pytest.mark.parametrize("prop_key, default_value, non_default_value", [
        ("replication_num", "3", "2"),
        ("storage_medium", "HDD", "SSD"),
    ])
    def test_properties_non_default_to_none(self, prop_key, default_value, non_default_value, caplog):
        """Test PROPERTIES from explicit non-default value to None (generate ALTER to reset to default)."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName
        caplog.set_level("WARNING")

        conn_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: {prop_key: non_default_value}})  # DB has non-default value
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}
        meta_table = Mock(kwargs={})  # Metadata has no properties
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        # Should generate an ALTER to effectively reset to default (meta_properties will include the default for this prop)
        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, "test_table", "test_db", conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 1
        from starrocks.alembic.ops import AlterTablePropertiesOp
        assert isinstance(result[0], AlterTablePropertiesOp)
        assert result[0].properties == {TablePropertyForFuturePartitions.wrap(prop_key): default_value}
        assert len(caplog.records) >= 1
        assert LOG_ALTER_AUTO_GENERATED in caplog.text

    @pytest.mark.parametrize("prop_key, value1, value2", [
        ("replication_num", "2", "1"),
        ("storage_medium", "SSD", "HDD"),
    ])
    def test_properties_change(self, prop_key, value1, value2):
        """Test PROPERTIES value change."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: {prop_key: value1}})  # DB has value1
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}
        meta_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: {prop_key: value2}})  # Metadata has value2
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, "test_table", "test_db", conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 1
        from starrocks.alembic.ops import AlterTablePropertiesOp
        assert isinstance(result[0], AlterTablePropertiesOp)
        assert result[0].properties == {TablePropertyForFuturePartitions.wrap(prop_key): value2}

    @pytest.mark.parametrize("prop_key, value", [
        ("replication_num", "3"),
        ("storage_medium", "HDD"),
    ])
    def test_properties_no_change(self, prop_key, value):
        """Test PROPERTIES no change."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: {prop_key: value}})  # DB has value
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}
        meta_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: {prop_key: value}})  # Metadata has same value
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, "test_table", "test_db", conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    @pytest.mark.parametrize("conn_props, meta_props", [
        ({"replication_num": "3"}, {"replication_num": "3"}),
        ({"replication_num": "3"}, {"REPLICATION_NUM": "3"}),
        ({"REPLICATION_NUM": "3"}, {"replication_num": "3"}),
    ])
    def test_properties_no_change2(self, conn_props, meta_props):
        """Test PROPERTIES no change."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: conn_props})  # DB has value
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}
        meta_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: meta_props})  # Metadata has same value
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, "test_table", "test_db", conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_properties_none_to_none(self):
        """Test PROPERTIES from None to None (no change)."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_table = Mock(kwargs={})  # No properties in DB
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}
        meta_table = Mock(kwargs={})  # No properties in metadata
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, "test_table", "test_db", conn_table, meta_table
        )
        result = upgrade_ops.ops
        assert len(result) == 0

    def test_properties_multiple_changes(self):
        """Test multiple property changes simultaneously."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        conn_props = {"replication_num": "3", "storage_medium": "HDD", "dynamic_partition.enable": "true"}
        meta_props = {"replication_num": "2", "storage_medium": "SSD", "dynamic_partition.enable": "false"}

        conn_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: conn_props})
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}
        meta_table = Mock(kwargs={TableInfoKeyWithPrefix.PROPERTIES: meta_props})
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, "test_table", "test_db", conn_table, meta_table
        )
        result = upgrade_ops.ops

        assert len(result) == 1
        from starrocks.alembic.ops import AlterTablePropertiesOp
        assert isinstance(result[0], AlterTablePropertiesOp)
        wapped_meta_props = {TablePropertyForFuturePartitions.wrap(k): v for k, v in meta_props.items()}
        assert result[0].properties == wapped_meta_props


class TestORMTableObjects:
    """Tests using real SQLAlchemy ORM Table objects with __table_args__."""

    def test_orm_table_with_table_args(self):
        """Test ORM table properties defined via __table_args__ are correctly handled."""
        from unittest.mock import Mock

        from alembic.autogenerate.api import AutogenContext
        from sqlalchemy import Column, Integer, MetaData, String, Table
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()

        class ORMTestTable(Base):
            __tablename__ = 'orm_test_table'
            __table_args__ = {
                    'schema': 'test_db',
                    'starrocks_ENGINE': 'OLAP',
                    'starrocks_DISTRIBUTED_BY': 'HASH(id) BUCKETS 8',
                    'starrocks_ORDER_BY': 'id, name',
                    'starrocks_PROPERTIES': {'replication_num': '3', 'storage_medium': 'SSD'}
            }
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect.name = DialectName

        # The Table object from the ORM model (target state)
        meta_table = ORMTestTable.__table__

        # Simulate table reflected from DB with some differences
        conn_table = Table(
            'orm_test_table', MetaData(),
            Column('id', Integer),
            Column('name', String(50)),
            starrocks_ENGINE='OLAP',
            starrocks_DISTRIBUTED_BY='HASH(id, name) BUCKETS 16', # Changed
            starrocks_ORDER_BY='id', # Changed
            starrocks_PROPERTIES={'replication_num': '2', 'compression': 'ZSTD'}, # Changed
            schema='test_db'
        )

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops

        assert len(result) == 3
        from starrocks.alembic.ops import AlterTableDistributionOp, AlterTableOrderOp, AlterTablePropertiesOp
        op_types = [type(op) for op in result]
        assert AlterTableDistributionOp in op_types
        assert AlterTableOrderOp in op_types
        assert AlterTablePropertiesOp in op_types

        for op in result:
            if isinstance(op, AlterTableDistributionOp):
                assert op.distribution_method == "HASH(id)"
                assert op.buckets == 8
            elif isinstance(op, AlterTableOrderOp):
                assert op.order_by == "id, name"
            elif isinstance(op, AlterTablePropertiesOp):
                logger.info(f"ALTER TABLE SET PROPERTIES: {op.properties}")
                # the compression property is added although it's not set in the metadata table
                assert op.properties == {'default.replication_num': '3', 'default.storage_medium': 'SSD', 'compression': 'LZ4'}


class TestComplexScenarios:
    """Test complex scenarios with multiple attribute changes."""

    def test_multiple_attributes_change_simultaneously(self):
        """Test multiple StarRocks table options changes at once."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect = Mock()
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_schema")
        conn_table.kwargs = {
            # No ENGINE - avoid the exception for this test
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id) BUCKETS 8",
            TableInfoKeyWithPrefix.ORDER_BY: "id",
            TableInfoKeyWithPrefix.PROPERTIES: {"replication_num": "2"},
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(
            kwargs={
                # No ENGINE - focus on testing other attributes
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(user_id) BUCKETS 16",  # Changed
                TableInfoKeyWithPrefix.ORDER_BY: "created_at, id",  # Changed
                TableInfoKeyWithPrefix.PROPERTIES: {"replication_num": "3", "storage_medium": "SSD"},  # Changed
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops

        # Should generate 3 operations (all detectable changes, excluding ENGINE and PARTITION_BY)
        assert len(result) == 3

        from starrocks.alembic.ops import AlterTableDistributionOp, AlterTableOrderOp, AlterTablePropertiesOp
        op_types = [type(op) for op in result]
        assert AlterTableDistributionOp in op_types
        assert AlterTableOrderOp in op_types
        assert AlterTablePropertiesOp in op_types

        for op in result:
            if isinstance(op, AlterTableDistributionOp):
                assert op.distribution_method == "HASH(user_id)"
                assert op.buckets == 16
            elif isinstance(op, AlterTableOrderOp):
                assert op.order_by == "created_at, id"
            elif isinstance(op, AlterTablePropertiesOp):
                assert op.properties == {"default.replication_num": "3", "default.storage_medium": "SSD"}

    def test_only_supported_operations_detected(self):
        """Test that only supported operations are detected and generated."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect = Mock()
        autogen_context.dialect.name = DialectName

        conn_table = Mock()
        type(conn_table).name = PropertyMock(return_value="test_table")
        type(conn_table).schema = PropertyMock(return_value="test_schema")
        conn_table.kwargs = {
            # No ENGINE - test only supported operations
            TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(id)",
            TableInfoKeyWithPrefix.PROPERTIES: {"replication_num": "1"},
        }
        conn_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(conn_table.kwargs)}

        meta_table = Mock(
            kwargs={
                # No ENGINE - focus on supported operations
                TableInfoKeyWithPrefix.DISTRIBUTED_BY: "HASH(user_id) BUCKETS 8",  # Supported
                TableInfoKeyWithPrefix.PROPERTIES: {"replication_num": "3", "storage_medium": "SSD"},  # Supported
            }
        )
        meta_table.dialect_options = {DialectName: extract_starrocks_dialect_attributes(meta_table.kwargs)}

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, conn_table.schema, conn_table.name, conn_table, meta_table
        )
        result = upgrade_ops.ops

        # Should generate 2 operations (both supported)
        assert len(result) == 2

        from starrocks.alembic.ops import AlterTableDistributionOp, AlterTablePropertiesOp
        op_types = [type(op) for op in result]
        assert AlterTableDistributionOp in op_types  # Supported
        assert AlterTablePropertiesOp in op_types  # Supported

    def test_non_starrocks_dialect_ignored(self):
        """Test that non-StarRocks dialects are ignored."""
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect = Mock()
        autogen_context.dialect.name = "mysql"  # Not StarRocks

        conn_table = Mock()
        meta_table = Mock()

        upgrade_ops = UpgradeOps([])
        compare_starrocks_table(
            autogen_context, upgrade_ops, "test_schema", "test_table", conn_table, meta_table
        )
        result = upgrade_ops.ops

        # Should return empty list for non-StarRocks dialects
        assert len(result) == 0

