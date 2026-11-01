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
from typing import List, Sequence

from alembic.operations import Operations
from sqlalchemy import MetaData
from sqlalchemy.exc import CompileError
from sqlalchemy.sql.elements import ClauseElement

from starrocks.alembic.ops import (
    AlterMaterializedViewOp,
    AlterTableColumnsOp,
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
    AlterTableProperties,
    AlterView,
    AlterViewSecurity,
    CreateMaterializedView,
    CreateView,
    DropMaterializedView,
    DropView,
    group_properties_for_set,
)
from starrocks.sql.schema import MaterializedView, View


logger = logging.getLogger(__name__)


def _view_to_recreate(op: AlterViewOp) -> View:
    """Build the view CREATE OR REPLACE VIEW recreates for an AlterViewOp, without its security.

    The definition and columns are the op's own when it has a definition, and the existing ones otherwise, as in
    a comment-only change.

    Raises:
        CompileError: if the op has no definition to recreate the view with.
    """
    has_definition = op.definition is not None
    definition = op.definition if has_definition else op.existing_definition
    if not definition:
        raise CompileError(
            f"alter_view({op.view_name!r}) changes the comment, which StarRocks can only do by recreating the "
            "view, so it needs the view's definition or existing_definition."
        )
    return View(
        op.view_name,
        MetaData(),
        definition=definition,
        schema=op.schema,
        columns=op.columns if has_definition else op.existing_columns,
        comment=op.comment,
    )


def _view_security_changes(op: AlterViewOp) -> bool:
    """Return whether the op carries a security that differs from the existing one, or has no existing one."""
    if op.security is None:
        return False
    return op.existing_security is None or str(op.security).upper() != str(op.existing_security).upper()


def _execute_once_every_statement_compiles(operations: Operations, statements: Sequence[ClauseElement]) -> None:
    """Execute an op's statements one at a time, once every one of them has compiled.

    StarRocks runs each DDL statement on its own, and nothing undoes one that has run. Compiling them all first
    means that a statement the dialect refuses, such as SET SECURITY before StarRocks 3.4.1 or a property with
    no value, stops the op before it changes anything.
    """
    for statement in statements:
        statement.compile(dialect=operations.impl.dialect)
    for statement in statements:
        operations.execute(statement)


@Operations.implementation_for(AlterViewOp)
def alter_view(operations: Operations, op: AlterViewOp) -> None:
    """Execute the statements that apply an AlterViewOp, one at a time, once they all compile.

    ALTER VIEW ... AS drops the view's comment, and nothing alters the comment alone, so an op that states a
    comment, or an existing one, is applied as CREATE OR REPLACE VIEW with the full content. CREATE OR REPLACE
    VIEW keeps the view's security whatever it says, so security is always changed with ALTER VIEW ... SET SECURITY.
    """
    logger.debug("implementation alter_view: %s", op.view_name)
    statements: List[ClauseElement] = []
    if op.comment is not None or op.existing_comment is not None:
        statements.append(CreateView(_view_to_recreate(op), or_replace=True))
    elif op.definition is not None:
        statements.append(AlterView(op.to_view()))
    if _view_security_changes(op):
        statements.append(AlterViewSecurity(op.to_view(), op.security))
    _execute_once_every_statement_compiles(operations, statements)


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

    Only supports altering mutable attributes: refresh, properties. The downgrade of a property change whose
    previous values could not be determined has neither, and does nothing. The refresh and the properties are
    altered in statements of their own, so a failure of either stops the migration; both compile before either
    runs.
    """
    logger.debug("implementation alter_materialized_view: %s", op.view_name)
    if op.refresh is None and not op.properties:
        logger.warning("Nothing to alter on materialized view %r: no refresh and no properties.", op.view_name)
        return
    statements: List[ClauseElement] = []
    if op.refresh is not None:
        statements.append(AlterMaterializedView(mv_name=op.view_name, schema=op.schema, refresh=op.refresh))
    if op.properties:
        statements.append(AlterMaterializedView(mv_name=op.view_name, schema=op.schema, properties=op.properties))
    _execute_once_every_statement_compiles(operations, statements)


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
def alter_table_properties(operations: Operations, op: AlterTablePropertiesOp) -> None:
    """Execute one ALTER TABLE ... SET per property, or per group of ``dynamic_partition.`` or ``binlog_`` ones.

    One packet of several statements fails midway when the first starts a schema-change job, as
    ``bloom_filter_columns`` does. Each statement is executed on its own, so the impl can wait for the
    job to finish before the next one when ``starrocks_wait_for_schema_change`` is on. They all compile before the
    first runs.
    """
    _execute_once_every_statement_compiles(
        operations,
        [AlterTableProperties(op.table_name, group, schema=op.schema) for group in group_properties_for_set(op.properties)],
    )


@Operations.implementation_for(AlterTableColumnsOp)
def alter_table_columns(operations, op: AlterTableColumnsOp):
    """Execute a combined ALTER TABLE ADD/DROP COLUMN statement.

    Emits a single ``ALTER TABLE ... ADD COLUMN ..., DROP COLUMN ...`` so
    StarRocks submits all column changes as one schema-change job instead of
    one job per column (which would fail while a prior job is still running).
    """
    from sqlalchemy.sql.schema import Column as _Column

    from starrocks.sql.ddl import AlterTableColumns
    logger.debug(
        "implementation alter_table_columns: %s (adds=%s, drops=%s)",
        op.table_name, [c.name for c in op.adds], [c.name for c in op.drops],
    )

    # Bind the added columns to a lightweight table so the compiler can render
    # their full specification (type, nullability, default, comment), mirroring
    # how Operations.add_column attaches its column via schema_obj.table(...).
    bound_adds = []
    for column in op.adds:
        if column.table is not None:
            column = column._copy() if hasattr(column, "_copy") else _Column(column.name, column.type)
        bound_adds.append(column)
    if bound_adds:
        operations.schema_obj.table(op.table_name, *bound_adds, schema=op.schema)

    operations.execute(
        AlterTableColumns(
            op.table_name,
            adds=bound_adds,
            drops=[c.name for c in op.drops],
            schema=op.schema,
        )
    )
