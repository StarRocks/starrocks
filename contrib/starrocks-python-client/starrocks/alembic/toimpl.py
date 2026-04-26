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

# Implementation functions for alter_table_xxx, ordered:
# view → mv, alter → create → drop
import logging

from alembic.operations import Operations
from sqlalchemy import MetaData

from starrocks.alembic.ops import (
    AlterMaterializedViewOp,
    AlterTableDistributionOp,
    AlterTableEngineOp,
    AlterTableKeyOp,
    AlterTableOrderOp,
    AlterTablePartitionOp,
    AlterTablePropertiesOp,
    AlterViewOp,
    CreateMaterializedViewOp,
    CreateViewOp,
    DropMaterializedViewOp,
    DropViewOp,
)
from starrocks.sql.ddl import (
    AlterMaterializedView,
    AlterView,
    CreateMaterializedView,
    CreateView,
    DropMaterializedView,
    DropView,
)
from starrocks.sql.schema import MaterializedView, View


logger = logging.getLogger(__name__)


@Operations.implementation_for(AlterViewOp)
def alter_view(operations: Operations, op: AlterViewOp):
    """Execute an ALTER VIEW statement."""
    logger.debug("implementation alter_view: %s", op.view_name)
    view = op.to_view()
    operations.execute(AlterView(view))


@Operations.implementation_for(CreateViewOp)
def create_view(operations: Operations, op: CreateViewOp):
    """Execute a CREATE VIEW statement."""
    logger.debug("implementation create_view: %s", op.view_name)
    view = op.to_view()
    operations.execute(CreateView(view, or_replace=op.or_replace, if_not_exists=op.if_not_exists))


@Operations.implementation_for(DropViewOp)
def drop_view(operations: Operations, op: DropViewOp) -> None:
    """Implementation for the 'drop_view' operation."""
    logger.debug("implementation drop_view: %s", op.view_name)
    # For DROP VIEW, we only need name and schema
    view = op.to_view()
    operations.execute(DropView(view, if_exists=op.if_exists))


@Operations.implementation_for(AlterMaterializedViewOp)
def alter_materialized_view(operations: Operations, op: AlterMaterializedViewOp) -> None:
    """
    Execute ALTER MATERIALIZED VIEW statement.

    Only supports altering mutable attributes: refresh, properties.
    """
    logger.debug("implementation alter_materialized_view: %s", op.view_name)
    operations.execute(AlterMaterializedView(
        mv_name=op.view_name,
        schema=op.schema,
        refresh=op.refresh,
        properties=op.properties,
    ))


@Operations.implementation_for(CreateMaterializedViewOp)
def create_materialized_view(operations: Operations, op: CreateMaterializedViewOp) -> None:
    """Execute a CREATE MATERIALIZED VIEW statement."""
    logger.debug("implementation create_materialized_view: %s", op.view_name)
    mv = op.to_mv()
    operations.execute(CreateMaterializedView(mv, or_replace=op.or_replace, if_not_exists=op.if_not_exists))


@Operations.implementation_for(DropMaterializedViewOp)
def drop_materialized_view(operations: Operations, op: DropMaterializedViewOp) -> None:
    """Implementation for the 'drop_materialized_view' operation."""
    logger.debug("implementation drop_materialized_view: %s", op.view_name)
    # For DROP MATERIALIZED VIEW, we only need name and schema, definition is not required
    # Create a minimal MaterializedView object for DROP operation
    mv = MaterializedView(
        op.view_name,
        MetaData(),
        definition='<placeholder_definition>',  # Empty definition for DROP
        schema=op.schema,
    )
    operations.execute(DropMaterializedView(mv, if_exists=op.if_exists))


# Implementation functions for alter_table_xxx, ordered according to StarRocks grammar:
# engine → key → (comment) → partition → distribution → order by → properties
@Operations.implementation_for(AlterTableEngineOp)
def alter_table_engine(operations, op: AlterTableEngineOp):
    logger.error(
        "ALTER TABLE ENGINE is not currently supported for StarRocks. "
        "Table: %s, Engine: %s", op.table_name, op.engine
    )
    raise NotImplementedError("ALTER TABLE ENGINE is not yet supported")


@Operations.implementation_for(AlterTableKeyOp)
def alter_table_key(operations, op: AlterTableKeyOp):
    logger.error(
        "ALTER TABLE KEY is not currently supported for StarRocks. "
        "Table: %s, Key Type: %s, Columns: %s",
        op.table_name, op.key_type, op.key_columns
    )
    raise NotImplementedError("ALTER TABLE KEY is not yet supported")


@Operations.implementation_for(AlterTablePartitionOp)
def alter_table_partition(operations, op: AlterTablePartitionOp):
    logger.error(
        "ALTER TABLE PARTITION is not currently supported for StarRocks. "
        "Table: %s, Partition: %s", op.table_name, op.partition_method
    )
    raise NotImplementedError("ALTER TABLE PARTITION is not yet supported")


@Operations.implementation_for(AlterTableDistributionOp)
def alter_table_distribution(operations, op: AlterTableDistributionOp):
    from starrocks.sql.ddl import AlterTableDistribution
    operations.execute(
        AlterTableDistribution(
            op.table_name,
            op.distribution_method,
            buckets=op.buckets,
            schema=op.schema
        )
    )


@Operations.implementation_for(AlterTableOrderOp)
def alter_table_order(operations, op: AlterTableOrderOp):
    from starrocks.sql.ddl import AlterTableOrder
    operations.execute(
        AlterTableOrder(op.table_name, op.order_by, schema=op.schema)
    )


@Operations.implementation_for(AlterTablePropertiesOp)
def alter_table_properties(operations, op: AlterTablePropertiesOp):
    from starrocks.sql.ddl import AlterTableProperties
    operations.execute(
        AlterTableProperties(op.table_name, op.properties, schema=op.schema)
    )
