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
from typing import Any

from alembic.autogenerate import renderers
from alembic.autogenerate.api import AutogenContext
from sqlalchemy.types import TypeEngine

from .ops import (
    AlterMaterializedViewOp,
    AlterTableDistributionOp,
    AlterTableOrderOp,
    AlterTablePropertiesOp,
    AlterViewOp,
    CreateMaterializedViewOp,
    CreateViewOp,
    DropMaterializedViewOp,
    DropViewOp,
)


logger = logging.getLogger("starrocks.alembic.render")


INDENT = " " * 4


def _render_op_call(autogen_context: AutogenContext, op_name: str, args: list[str]) -> str:
    """Render an operation call, with Alembic prefix and standard 4-space indent per line.
    """
    # prefix = alembic_render._alembic_autogenerate_prefix(autogen_context)
    prefix = "op."
    size = sum(len(a) for a in args)
    if len(args) <= 2 or size <= 80:
        return f"{prefix}{op_name}({', '.join(args)})"
    else:
        args_block = ",\n".join(f"{INDENT}{a}" for a in args)
        return f"{prefix}{op_name}(\n{args_block}\n)"


def render_column_type(type_: str, obj: Any, autogen_context: AutogenContext):
    """
    Custom rendering function. To render ARRAY, MAP, STRUCT, etc.
    Currently, it's only used to import the starrocks.datatype module.
    We will use __repr__ to render the type, without the prefix "sr."

    NOTE: StarRocksImpl.compare_type() will do real comparison logic, so we don't need to do it here.
    Such as compasion VARCHAR(65533) and STRING.

    Args:
        type_: The type of the column.
        obj: The object to render.
        autogen_context: The autogen context.

    Returns:
        The string representation of the type.
    """
    # only care about the column type
    if type_ != "type":
        return False

    # logger.debug(f"rendering type: {obj!r}, type: {type_}, module: {obj.__class__.__module__}")
    # Check if the object is a user-defined type in our custom module
    if not isinstance(obj, TypeEngine) or not obj.__class__.__module__.startswith('starrocks.datatype'):
        # For other objects, return False, let Alembic use the default rendering logic
        return False

    # Add the import we need
    # autogen_context.imports.add("import starrocks.datatype as sr")
    autogen_context.imports.add("from starrocks import *")
    # Return the string representation we want
    # obj.__class__.__name__ will get 'INTEGER', 'VARCHAR' etc.
    # repr(obj) will contain parameters, such as 'VARCHAR(255)'

    # if isinstance(obj, ARRAY):
    #     return _render_array_type(obj, autogen_context)
    # elif isinstance(obj, MAP):
    #     return _render_map_type(obj, autogen_context)
    # elif isinstance(obj, STRUCT):
    #     return _render_struct_type(obj, autogen_context)
    # else:
    #     return f"sr.{repr(obj)}"

    return f"{repr(obj)}"


def _render_array_type(type_, autogen_context: AutogenContext) -> str:
    """Render an ARRAY type for autogenerate."""
    item_type_repr = render_column_type('type', type_.item_type, autogen_context)
    return f"sr.ARRAY({item_type_repr})"


def _render_map_type(type_, autogen_context: AutogenContext) -> str:
    """Render a MAP type for autogenerate."""
    key_type_repr = render_column_type('type', type_.key_type, autogen_context)
    value_type_repr = render_column_type('type', type_.value_type, autogen_context)
    return f"sr.MAP({key_type_repr}, {value_type_repr})"


def _render_struct_type(type_, autogen_context: AutogenContext) -> str:
    """Render a STRUCT type for autogenerate."""
    fields_repr = render_column_type('type', type_.fields, autogen_context)
    return f"sr.STRUCT({fields_repr})"


def _quote_schema(schema: str) -> str:
    """Quote schema name using single quotes with proper escaping.
    That is (' -> \')
    """
    return f"'{schema.replace(chr(39), chr(92) + chr(39))}'" if schema else None


@renderers.dispatch_for(AlterViewOp)
def _alter_view(autogen_context: AutogenContext, op: AlterViewOp) -> str:
    """Render an AlterViewOp for autogenerate."""
    args = [f"{op.view_name!r}"]

    if op.definition:
        args.append(f"{op.definition!r}")

    if op.schema:
        args.append(f"schema={op.schema!r}")
    if op.columns:
        # Render columns as a list of dicts
        args.append(f"columns={op.columns!r}")
    if op.comment or op.existing_comment:
        args.append(f"comment={op.comment!r}")
    if op.security or op.existing_security:
        args.append(f"security={op.security!r}")

    # render reverse values for downgrade if present
    if op.existing_definition:
        args.append(f"existing_definition={op.existing_definition!r}")
    if op.existing_columns:
        args.append(f"existing_columns={op.existing_columns!r}")
    if op.existing_comment or op.comment:
        args.append(f"existing_comment={op.existing_comment!r}")
    if op.existing_security or op.security:
        args.append(f"existing_security={op.existing_security!r}")

    call = _render_op_call(autogen_context, "alter_view", args)
    logger.debug("render alter_view: %s", call)
    return call

@renderers.dispatch_for(CreateViewOp)
def _create_view(autogen_context: AutogenContext, op: CreateViewOp) -> str:
    """Render a CreateViewOp as Python code for migration script."""
    args = [
        f"{op.view_name!r}",
        f"{op.definition!r}"
    ]
    if op.schema:
        args.append(f"schema={op.schema!r}")
    if op.comment:
        args.append(f"comment={op.comment!r}")
    if op.columns:
        # Render columns as a list of dicts
        args.append(f"columns={op.columns!r}")

    # Render dialect-specific kwargs (e.g., starrocks_security)
    for key, value in op.kwargs.items():
        if value is not None:
            args.append(f"{key}={value!r}")

    call = _render_op_call(autogen_context, "create_view", args)
    logger.debug("render create_view: %s", call)
    return call


@renderers.dispatch_for(DropViewOp)
def _drop_view(autogen_context: AutogenContext, op: DropViewOp) -> str:
    args = [f"{op.view_name!r}"]
    if op.schema:
        args.append(f"schema={op.schema!r}")
    if op.if_exists:
        args.append(f"if_exists={op.if_exists!r}")

    call = _render_op_call(autogen_context, "drop_view", args)
    logger.debug("render drop_view: %s", call)
    return call


@renderers.dispatch_for(CreateMaterializedViewOp)
def _create_materialized_view(autogen_context: AutogenContext, op: CreateMaterializedViewOp) -> str:
    """
    Render CREATE MATERIALIZED VIEW operation.

    Similar to _create_view but with MV-specific parameters.
    """
    args = [
        f"{op.view_name!r}",
        f"{op.definition!r}"
    ]

    if op.schema:
        args.append(f"schema={op.schema!r}")
    if op.comment:
        args.append(f"comment={op.comment!r}")
    if op.columns:
        # Render columns as a list of dicts
        args.append(f"columns={op.columns!r}")

    # MV-specific attributes from kwargs (should all have "starrocks_" prefix)
    for key, value in op.kwargs.items():
        if value is not None:
            args.append(f"{key}={value!r}")

    call = _render_op_call(autogen_context, "create_materialized_view", args)
    logger.debug("render create_materialized_view: %s", call)
    return call


@renderers.dispatch_for(AlterMaterializedViewOp)
def _alter_materialized_view(autogen_context: AutogenContext, op: AlterMaterializedViewOp) -> str:
    """
    Render ALTER MATERIALIZED VIEW operation.

    Only renders mutable attributes (refresh, properties).
    """
    args = [f"{op.view_name!r}"]

    if op.schema:
        args.append(f"schema={op.schema!r}")

    if op.refresh or op.existing_refresh:
        args.append(f"refresh={op.refresh!r}")
    if op.properties or op.existing_properties:
        args.append(f"properties={op.properties!r}")

    # Render reverse values for downgrade if present
    if op.existing_refresh or op.refresh:
        args.append(f"existing_refresh={op.existing_refresh!r}")
    if op.existing_properties or op.properties:
        args.append(f"existing_properties={op.existing_properties!r}")

    call = _render_op_call(autogen_context, "alter_materialized_view", args)
    logger.debug("render alter_materialized_view: %s", call)
    return call


@renderers.dispatch_for(DropMaterializedViewOp)
def _drop_materialized_view(autogen_context: AutogenContext, op: DropMaterializedViewOp) -> str:
    args = [f"{op.view_name!r}"]
    if op.schema:
        args.append(f"schema={op.schema!r}")
    if op.if_exists:
        args.append(f"if_exists={op.if_exists!r}")

    call = _render_op_call(autogen_context, "drop_materialized_view", args)
    logger.debug("render drop_materialized_view: %s", call)
    return call


@renderers.dispatch_for(AlterTableDistributionOp)
def _render_alter_table_distribution(autogen_context: AutogenContext, op: AlterTableDistributionOp) -> str:
    """Render an AlterTableDistributionOp for autogenerate."""
    args = [
        f"{op.table_name!r}",
        f"{op.distribution_method!r}",
    ]
    if op.buckets is not None:
        args.append(f"buckets={op.buckets}")
    if op.schema:
        args.append(f"schema={op.schema!r}")

    return _render_op_call(autogen_context, "alter_table_distribution", args)


@renderers.dispatch_for(AlterTableOrderOp)
def _render_alter_table_order(autogen_context: AutogenContext, op: AlterTableOrderOp) -> str:
    """Render an AlterTableOrderOp for autogenerate."""
    args = [
        f"{op.table_name!r}",
        f"{op.order_by!r}"
    ]
    if op.schema:
        args.append(f"schema={op.schema!r}")

    return _render_op_call(autogen_context, "alter_table_order", args)


@renderers.dispatch_for(AlterTablePropertiesOp)
def _render_alter_table_properties(autogen_context: AutogenContext, op: AlterTablePropertiesOp) -> str:
    """Render an AlterTablePropertiesOp for autogenerate."""
    args = [
        f"{op.table_name!r}",
        f"{op.properties!r}"
    ]
    if op.schema:
        args.append(f"schema={op.schema!r}")

    return _render_op_call(autogen_context, "alter_table_properties", args)
