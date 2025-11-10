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
from typing import Any, Final

from alembic.autogenerate import renderers
from alembic.autogenerate.api import AutogenContext
from sqlalchemy.types import TypeEngine

from .ops import (
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


op_param_indent: Final[str] = " " * 4


def render_column_type(type_: str, obj: Any, autogen_context: AutogenContext):
    """
    Custom rendering function. To reander ARRAY, MAP, STRUCT, etc.
    Currently, it's only used to import the starrocks.datatype module.
    We will use __repr__ to render the type, without the prefix "sr."
    """
    # only care about the column type
    if type_ != "type":
        return False

    logger.debug(f"rendering type: {obj!r}, type: {type_}, module: {obj.__class__.__module__}")
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
    args = [f"{op.view_name!r}", f"\n{op_param_indent}{op.definition!r}\n"]
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")
    if op.comment:
        args.append(f"comment={op.comment!r}")
    if op.security:
        args.append(f"security={op.security!r}")

    call = f"op.alter_view({', '.join(args)})"
    logger.debug("render alter_view: %s", call)
    return call

@renderers.dispatch_for(CreateViewOp)
def _create_view(autogen_context: AutogenContext, op: CreateViewOp) -> str:
    args = [
        f"{op.view_name!r}",
        f"{op.definition!r}"
    ]
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")
    if op.security:
        args.append(f"security={op.security!r}")
    if op.comment:
        args.append(f"comment={op.comment!r}")

    call = f"op.create_view({', '.join(args)})"
    logger.debug("render create_view: %s", call)
    return call


@renderers.dispatch_for(DropViewOp)
def _drop_view(autogen_context: AutogenContext, op: DropViewOp) -> str:
    args = [f"{op.view_name!r}"]
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")

    call = f"op.drop_view({', '.join(args)})"
    logger.debug("render drop_view: %s", call)
    return call


@renderers.dispatch_for(CreateMaterializedViewOp)
def _create_materialized_view(autogen_context: AutogenContext, op: CreateMaterializedViewOp) -> str:
    args = [
        f"{op.view_name!r}",
        f"{op.definition!r}",
    ]
    if op.properties:
        args.append(f"properties={op.properties!r}")
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")

    call = f"op.create_materialized_view({', '.join(args)})"
    logger.debug("render create_materialized_view: %s", call)
    return call


@renderers.dispatch_for(DropMaterializedViewOp)
def _drop_materialized_view(autogen_context: AutogenContext, op: DropMaterializedViewOp) -> str:
    args = [f"{op.view_name!r}"]
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")

    call = f"op.drop_materialized_view({', '.join(args)})"
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
        args.append(f"schema={_quote_schema(op.schema)}")

    return f"op.alter_table_distribution({', '.join(args)})"


@renderers.dispatch_for(AlterTableOrderOp)
def _render_alter_table_order(autogen_context: AutogenContext, op: AlterTableOrderOp) -> str:
    """Render an AlterTableOrderOp for autogenerate."""
    args = [
        f"{op.table_name!r}",
        f"{op.order_by!r}"
    ]
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")

    return f"op.alter_table_order({', '.join(args)})"


@renderers.dispatch_for(AlterTablePropertiesOp)
def _render_alter_table_properties(autogen_context: AutogenContext, op: AlterTablePropertiesOp) -> str:
    """Render an AlterTablePropertiesOp for autogenerate."""
    args = [
        f"{op.table_name!r}",
        f"{op.properties!r}"
    ]
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")

    return f"op.alter_table_properties({', '.join(args)})"
