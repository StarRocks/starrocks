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

from functools import wraps
import logging
import re
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Sequence, Set, Tuple, Type, Union
import uuid
import warnings

from alembic.autogenerate import comparators
from alembic.autogenerate.api import AutogenContext
from alembic.ddl import DefaultImpl
from alembic.operations.ops import (
    AlterColumnOp,
    AlterTableOp,
    CreateTableOp,
    DropColumnOp,
    DropTableOp,
    MigrateOperation,
    ModifyTableOps,
    RenameTableOp,
    UpgradeOps,
)
from sqlalchemy import Column, __version__ as _sqlalchemy_version
from sqlalchemy.engine import Connection, Dialect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql import quoted_name, schema as sa_schema, sqltypes
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.schema import Table
from sqlalchemy.util import OrderedSet

from starrocks import datatype
from starrocks.alembic.ops import (
    AlterMaterializedViewOp,
    AlterTablePropertiesOp,
    AlterViewOp,
    CreateMaterializedViewOp,
    CreateViewOp,
    DropMaterializedViewOp,
    DropViewOp,
)
from starrocks.common import sql_canonical, utils
from starrocks.common.defaults import ReflectionMVDefaults, ReflectionTableDefaults, ReflectionViewDefaults
from starrocks.common.params import (
    AlterMVEnablement,
    AlterTableEnablement,
    ColumnAggInfoKey,
    ColumnAggInfoKeyWithPrefix,
    DialectName,
    InvalidTableProperty,
    SRKwargsPrefix,
    TableInfoKey,
    TableInfoKeyWithPrefix,
    TableKind,
    TableObjectInfoKey,
    TablePropertyForFuturePartitions,
    UnalterableMaterializedViewProperty,
    UnalterableTableProperty,
)
from starrocks.common.types import PartitionType, TableDistribution, TableType
from starrocks.common.utils import (
    CaseInsensitiveDict,
    TableAttributeNormalizer,
    extract_dialect_options_as_case_insensitive,
)
from starrocks.datatype import ARRAY, BOOLEAN, MAP, STRING, STRUCT, TINYINT, VARCHAR
from starrocks.dialect import stored_sql_text
from starrocks.engine.interfaces import ReflectedPartitionInfo, ReflectedTableKeyInfo
from starrocks.reflection import StarRocksTableDefinitionParser
from starrocks.sql.schema import EVERY_COLUMN, MaterializedView, View, names_in_query, tables_read_by_query


logger = logging.getLogger(__name__)


class UnalterableChange(NotImplementedError):
    """A difference between metadata and database that StarRocks has no ALTER statement for."""


INTEGER_VISIT_NAMES_WITH_DISPLAY_WIDTH = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "LARGEINT"}

# ``context.configure()`` option: schema to host the transient view/MV canonicalization
# view. When unset, it is created in the compared object's own schema (the prior default).
TEMP_VIEW_SCHEMA_OPT = "starrocks_temp_view_schema"


def _is_integer_with_display_width(type_obj: sqltypes.TypeEngine) -> bool:
    visit_name = getattr(type_obj, "__visit_name__", None)
    if visit_name is None:
        return False
    return visit_name.upper() in INTEGER_VISIT_NAMES_WITH_DISPLAY_WIDTH


_UNBOUNDED_TEXT_VISIT_NAMES = frozenset({"text", "unicode_text", "TEXT", "STRING"})


def _is_unbounded_text(type_obj: sqltypes.TypeEngine) -> bool:
    """STRING, Text and UnicodeText all compile to STRING, which the server reports as varchar(65533)."""
    return getattr(type_obj, "__visit_name__", None) in _UNBOUNDED_TEXT_VISIT_NAMES


def _binary_family_matches(metadata_type: Any, inspector_type: Any) -> bool:
    """BINARY, VARBINARY and LargeBinary are one family: StarRocks stores them all as varbinary(n).

    A declared type without a length matches any reflected length.
    """
    if not (isinstance(metadata_type, sqltypes._Binary) and isinstance(inspector_type, sqltypes._Binary)):
        return False
    declared_length = getattr(metadata_type, "length", None)
    return declared_length is None or declared_length == getattr(inspector_type, "length", None)


def compare_simple_type(impl: DefaultImpl, inspector_column: Column[Any], metadata_column: Column[Any]) -> bool:
    """
    Set StarRocks' specific simple type comparison logic for some special cases.

    For some special cases:
        - meta.BOOLEAN equals to conn.TINYINT(1)
        - meta.STRING, Text and UnicodeText equal conn.VARCHAR(65533)
        - meta.BINARY, VARBINARY and LargeBinary equal conn.VARBINARY(n), ignoring a length the model omits

    NOTE: StarRocks will ignore the length of Integer types, so we need to care about the
    comparison of Integer types: ignore the comparison of the display width of Integer types.

    Args:
        impl: The implementation of the dialect.
        inspector_column: The column from the inspector.
        metadata_column: The column from the metadata.

    Returns:
        True if the types are different, False if the types are the same.
    """
    inspector_type = inspector_column.type
    metadata_type = metadata_column.type

    # logger.debug("compare_simple_type: inspector_type: %s, metadata_type: %s", inspector_type, metadata_type)
    # Scenario 1.a: model defined BOOLEAN, database stored TINYINT(1)
    if (isinstance(metadata_type, BOOLEAN)
        and isinstance(inspector_type, TINYINT)
        and getattr(inspector_type, 'display_width', None) == 1):
        # logger.debug("compare_simple_type with BOOLEAN vs TINYINT(1), treat them as the same.")
        return False

    # Scenario 1.b: model defined TINYINT(1), database may display as Boolean (theoretically not possible, but for safety)
    if (isinstance(metadata_type, TINYINT)
        and getattr(metadata_type, 'display_width', None) == 1
        and isinstance(inspector_type, BOOLEAN)):
        # logger.debug("compare_simple_type with TINYINT(1) vs BOOLEAN, treat them as the same.")
        return False

    # Scenario 2.a: model defined STRING or Text, database stored VARCHAR(65533)
    if (_is_unbounded_text(metadata_type) and
        isinstance(inspector_type, VARCHAR) and
        getattr(inspector_type, 'length', None) == 65533):
        logger.debug("compare_simple_type with STRING vs VARCHAR(65533), treat them as the same.")
        return False

    # Scenario 2.b: model defined VARCHAR(65533), database stored STRING (theoretically not possible, but for safety)
    if (isinstance(metadata_type, VARCHAR) and
        getattr(metadata_type, 'length', None) == 65533 and
        isinstance(inspector_type, STRING)):
        logger.debug("compare_simple_type with VARCHAR(65533) vs STRING, treat them as the same.")
        return False

    # Scenario 2.c: BINARY, VARBINARY and LargeBinary are stored as varbinary(n)
    if _binary_family_matches(metadata_type, inspector_type):
        return False

    # Scenario 3: ignore display_width when comparing integer types.
    if (_is_integer_with_display_width(metadata_type) and _is_integer_with_display_width(inspector_type)):
        if type(metadata_type) is type(inspector_type):
            return False

    # Other cases use default comparison logic from the parent class
    from starrocks.alembic.starrocks import StarRocksImpl
    return super(StarRocksImpl, impl).compare_type(inspector_column, metadata_column)


def compare_complex_type(impl: DefaultImpl, inspector_type: sqltypes.TypeEngine, metadata_type: sqltypes.TypeEngine) -> bool:
    """
    Recursively compares two StarRocks SQLAlchemy complex types.
    Returns True if they are different, False if they are the same.

    Args:
        impl: The implementation of the dialect. It should be a StarRocksImpl instance.
        inspector_type: The type from the inspector.
        metadata_type: The type from the metadata.

    Returns:
        True if the types are different, False if the types are the same.
    """
    # First check if they are the exact same type class
    # logger.debug("compare_complex_type with inspector_type: %s, metadata_type: %s.", inspector_type, metadata_type)
    if not isinstance(metadata_type, datatype.StructuredType):
        # For simple types and other types, use compare_simple_type by composing fake columns
        conn_col = Column("fake_conn_col", inspector_type)
        meta_col = Column("fake_meta_col", metadata_type)
        return compare_simple_type(impl, conn_col, meta_col)

    # Now, the type should be StructuredType (complex data type)
    if type(inspector_type) is not type(metadata_type):
        # logger.debug("compare_complex_type with different classes: inspector_type: %s, metadata_type: %s.", inspector_type, metadata_type)
        return True  # Different classes

    if isinstance(inspector_type, ARRAY):
        # We know metadata_type is also ARRAY due to the initial type check
        return compare_complex_type(impl, inspector_type.item_type, metadata_type.item_type)

    if isinstance(inspector_type, MAP):
        # We know metadata_type is also MAP
        if compare_complex_type(impl, inspector_type.key_type, metadata_type.key_type):
            # logger.debug("compare_complex_type with different key types of MAP: inspector_type: %s, metadata_type: %s.", inspector_type, metadata_type)
            return True
        return compare_complex_type(impl, inspector_type.value_type, metadata_type.value_type)

    if isinstance(inspector_type, STRUCT):
        # We know metadata_type is also STRUCT
        if len(inspector_type.field_tuples) != len(metadata_type.field_tuples):
            # logger.debug("compare_complex_type with different number of fields of STRUCT: inspector_type: %s, metadata_type: %s.", inspector_type, metadata_type)
            return True  # Different number of fields

        # Compare field names and types in order. StarRocks STRUCTs are order-sensitive.
        for (name1, type1_sub), (name2, type2_sub) in zip(
            inspector_type.field_tuples, metadata_type.field_tuples
        ):
            if name1 != name2:
                # logger.debug("compare_complex_type with different field names of STRUCT: inspector_type: %s, metadata_type: %s.", inspector_type, metadata_type)
                return True
            if compare_complex_type(impl, type1_sub, type2_sub):
                # logger.debug("compare_complex_type with different field types of STRUCT: inspector_type: %s, metadata_type: %s.", inspector_type, metadata_type)
                return True
        return False

    # should not reach here
    return True


_SQLALCHEMY_2 = int(_sqlalchemy_version.split(".")[0]) >= 2

try:
    from alembic.util import DispatchPriority

    _LAST_PRIORITY: Dict[str, Any] = {"priority": DispatchPriority.LAST}
except ImportError:
    _LAST_PRIORITY = {}


def comparators_dispatch_for_starrocks(
    dispatch_type: str, run_last: bool = False
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    StarRocks-specific dispatch decorator.

    Automatically handles dialect checking, only executes the decorated function under StarRocks dialect.

    Args:
        dispatch_type: Alembic dispatch type ("table", "column", "view", etc.)
        run_last: Run after Alembic's own comparators of the type, which Alembic 1.18 and later runs
            in batches by priority, with its server default comparison in the last one. Earlier
            versions run them in the order registered, so this changes nothing there.

    Usage:
        @starrocks_dispatch_for("table")
        def compare_starrocks_table(autogen_context, conn_table, metadata_table):
            # No need to manually check dialect, decorator handles it automatically
            pass
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            autogen_context = args[0]  # First arg is always autogen_context

            # Only execute for StarRocks dialect
            if autogen_context.dialect.name != DialectName:
                # Return default value based on return type annotation
                return_type = func.__annotations__.get('return')
                if return_type is not None:
                    if hasattr(return_type, '__origin__') and return_type.__origin__ is list:
                        return []
                    elif 'List' in str(return_type):
                        return []
                return None

            # StarRocks dialect, execute actual logic
            return func(*args, **kwargs)

        # Register to Alembic dispatch system
        return comparators.dispatch_for(dispatch_type, **(_LAST_PRIORITY if run_last else {}))(wrapper)

    return decorator


# ==============================================================================
# include_object handling
# ==============================================================================
def include_object_for_view_mv(object, name, type_, reflected, compare_to):
    """
    Filter objects for Alembic'autogenerate - exclude View/MV from table comparisons.

    This function is used to filter out views and materialized views from the
    default table comparison logic, as they are handled by custom hooks.
    """
    if type_ == "table":
        # object is a sqlalchemy.Table object, from metadata or reflected
        table_kind = object.info.get(TableObjectInfoKey.TABLE_KIND)
        if table_kind and table_kind.upper() in (TableKind.VIEW, TableKind.MATERIALIZED_VIEW):
            return False
    return True


def combine_include_object(user_include_object):
    """
    Combine the dialect's include_object function with a user-defined one.

    The dialect's filter is executed first. If it returns False, the object
    is excluded. If it returns True, the user's filter is then executed.
    This allows users to further customize object inclusion without overriding
    the dialect's necessary filters.
    """
    dialect_include_object = include_object_for_view_mv

    if user_include_object is None:
        return dialect_include_object

    @wraps(user_include_object)
    def combined(object, name, type_, reflected, compare_to):
        if not dialect_include_object(object, name, type_, reflected, compare_to):
            return False
        return user_include_object(object, name, type_, reflected, compare_to)

    return combined


# ==============================================================================
# View Comparison
# ==============================================================================
def check_similar_string_and_warn(
        table_name: str,
        attribute_name: str,
        conn_str: Optional[str],
        meta_str: Optional[str],
        schema: Optional[str] = None,
        object_label: str = "Table"
    ) -> None:
    if TableAttributeNormalizer.simply_normalize_quotes(str(conn_str)) == TableAttributeNormalizer.simply_normalize_quotes(str(meta_str)):
        table_fqn = utils.gen_simple_qualified_name(table_name, schema)
        logger.warning(
            f"Although detected changed {attribute_name} on {object_label.lower()} {table_fqn!r}, "
            f"they are only different in quote style, so it may be not a real modification. "
            f"You need to check it and change the quote style in the metadata according to the database "
            f"if you are sure they are the same (you don't want to make a ALTER operation)."
        )


def _left_alone_before_sqlalchemy_2(object_names: Set[Tuple[Optional[str], str]], kind: str) -> bool:
    """Return whether autogenerate leaves the database's views or materialized views alone, as on SQLAlchemy 1.4.

    ``View`` and ``MaterializedView`` need SQLAlchemy 2.0, so a 1.4 model has none, and comparing would reflect
    each one into an object 1.4 cannot build. The objects are left alone, and a warning names them.
    """
    if _SQLALCHEMY_2:
        return False
    if object_names:
        logger.warning(
            "SQLAlchemy 1.4 cannot build a %s, so autogenerate leaves the database's alone: %s",
            kind,
            ", ".join(utils.gen_simple_qualified_name(name, schema) for schema, name in sorted(
                object_names, key=lambda key: (key[0] or "", key[1])
            )),
        )
    return True


@comparators_dispatch_for_starrocks("schema", run_last=True)
def _autogen_for_views(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schemas: Union[Set[None], Set[Optional[str]]]
) -> None:
    """
    Main autogenerate entrypoint for views.

    Scan views in database and compare with metadata.
    """
    inspector: Inspector = autogen_context.inspector
    metadata = autogen_context.metadata
    default_schema = inspector.bind.dialect.default_schema_name

    conn_view_names: Set[Tuple[Optional[str], str]] = set()
    logger.debug("Start to autogenerate for views: schemas: %s", schemas)

    for schema in schemas:
        views = set(inspector.get_view_names(schema=schema))
        conn_view_names.update(
            (schema, vname)
            for vname in views
            if autogen_context.run_name_filters(
                vname, "view", {"schema_name": schema}
            )
        )

    # Get all views from metadata and apply name filters, normalize schema (default -> None)
    all_normed_metadata_view_names = set(
        (table.schema if table.schema != default_schema else None, table.name)
        for table in metadata.tables.values()
        if (table_kind:= table.info.get(TableObjectInfoKey.TABLE_KIND)) and table_kind.upper() == TableKind.VIEW
        and autogen_context.run_name_filters(
            table.name, "view", {"schema_name": table.schema}
        )
    )
    # Filter by target schemas
    metadata_view_names = OrderedSet(
        (s, t) for s, t in all_normed_metadata_view_names if s in schemas
    )

    if len(schemas) == 1 and None in schemas:
        other_schemas = set(
            s for s, _ in all_normed_metadata_view_names if s not in schemas
        )
        if other_schemas:
            warnings.warn(
                "Views in other schemas will be ignored. It's probably you have not set the `include_schemas` "
                "and `include_name` parameters correctly. Set them in you `env.py` properly if you want to manage views "
                f"in other schemas: {other_schemas!r}",
                UserWarning,
            )

    if _left_alone_before_sqlalchemy_2(conn_view_names, "view"):
        return
    _compare_views(
        conn_view_names,
        metadata_view_names,
        inspector,
        upgrade_ops,
        autogen_context,
    )


def _compare_views(
    conn_view_names: Set[Tuple[Optional[str], str]],
    metadata_view_names: Set[Tuple[Optional[str], str]],
    inspector: Inspector,
    upgrade_ops: UpgradeOps,
    autogen_context: AutogenContext,
) -> None:
    """Compare views between database and metadata, generating add/drop/alter operations."""
    metadata = autogen_context.metadata
    logger.debug("start to compare views, conn_view_names (from DB): %s, metadata_view_names (from metadata): %s",
        conn_view_names, metadata_view_names)

    # Normalize metadata schema: replace default schema with None to align with inspector
    default_schema = inspector.bind.dialect.default_schema_name
    metadata_view_names_no_dflt = OrderedSet(
        [
            (schema if schema != default_schema else None, tname)
            for schema, tname in metadata_view_names
        ]
    )
    # Build a lookup from (schema, name) to Table object for metadata views (using normalized schema)
    view_name_to_table = {
        (table.schema if table.schema != default_schema else None, table.name): table
        for table in metadata.tables.values()
        if (table_kind:= table.info.get(TableObjectInfoKey.TABLE_KIND)) and table_kind.upper() == TableKind.VIEW
    }
    metadata_view_names = metadata_view_names_no_dflt

    # Added views (in metadata but not in database)
    added_views = metadata_view_names.difference(conn_view_names)
    logger.debug("Added views (in metadata but not in DB): %s", added_views)
    for s, vname in added_views:
        view_fqn = "%s.%s" % (s, vname) if s else vname
        metadata_view = view_name_to_table[(s, vname)]
        if autogen_context.run_object_filters(metadata_view, vname, "view", False, None):
            upgrade_ops.ops.append(CreateViewOp.from_view(metadata_view))
            logger.info(f"Detected added view {view_fqn!r}")

    # Dropped views (in database but not in metadata)
    # Use a separate MetaData to avoid polluting the user's metadata
    removal_metadata = sa_schema.MetaData()

    dropped_views = conn_view_names.difference(metadata_view_names)
    logger.debug("Dropped views (in DB but not in metadata): %s", dropped_views)
    for s, vname in dropped_views:
        logger.debug("Processing dropped view: schema=%s, name=%s", s, vname)
        name = sa_schema._get_table_key(vname, s)
        exists = name in removal_metadata.tables
        # Create a View object (not Table) since we know it's a view
        # Use empty definition (a placeholder only) - it will be populated by reflect_table
        t = View(vname, removal_metadata, definition='<not_used_definition>', schema=s)

        if not exists:
            # Reflect the view using StarRocks' custom reflection logic
            # This will automatically populate table_kind, definition, and dialect_options
            inspector.reflect_table(t, include_columns=None)

        if autogen_context.run_object_filters(t, vname, "view", True, None):
            upgrade_ops.ops.append(DropViewOp.from_view(t))
            logger.info(f"Detected removed view {name!r}")

    # Modified views (in both database and metadata)
    # Use a separate MetaData for reflected views
    existing_metadata = sa_schema.MetaData()
    existing_views = conn_view_names.intersection(metadata_view_names)
    logger.debug("Existing views (in both DB and metadata): %s", existing_views)

    for s, vname in existing_views:
        # Use reflect_table to get the full view object from database
        name = sa_schema._get_table_key(vname, s)
        exists = name in existing_metadata.tables
        # Create a View object (not Table) since we know it's a view
        # Use empty definition - it will be populated by reflect_table
        t = View(vname, existing_metadata, definition='<placeholder_definition>', schema=s)

        if not exists:
            # Reflect the view using StarRocks' custom reflection logic
            # This will automatically populate table_kind, definition, and dialect_options
            inspector.reflect_table(t, include_columns=None)

    # Compare existing views in sorted order for consistent output
    for s, vname in sorted(existing_views, key=lambda x: (x[0] or "", x[1])):
        s = s or None
        name = sa_schema._get_table_key(vname, s)
        metadata_view = view_name_to_table[(s, vname)]
        conn_view = existing_metadata.tables[name]

        logger.debug("Comparing existing view: %s", name)
        if autogen_context.run_object_filters(metadata_view, vname, "view", False, conn_view):
            # Dispatch to compare_view for detailed comparison
            comparators.dispatch("view")(
                autogen_context,
                upgrade_ops,
                s,
                vname,
                conn_view,
                metadata_view,
            )

@comparators_dispatch_for_starrocks("view")
def compare_view(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schema: Optional[str],
    view_name: str,
    conn_view: View,
    metadata_view: View,
) -> None:
    """
    Compare a single view and generate operations if needed.

    Check for changes in view definition, comment, security attributes, and columns.
    """
    # Handle view creation and deletion scenarios (should not happen here)
    if conn_view is None or metadata_view is None:
        logger.warning(
            "compare_view: both conn_view and metadata_view should not be None for %s.%s, skipping",
            schema or autogen_context.dialect.default_schema_name,
            view_name
        )
        return
    qualifed_view_name = utils.gen_simple_qualified_name(view_name, schema)
    logger.debug("compare_view: view_name=%r", qualifed_view_name)

    # Extract dialect_options for comparison
    conn_view_attributes = extract_dialect_options_as_case_insensitive(conn_view)
    meta_view_attributes = extract_dialect_options_as_case_insensitive(metadata_view)

    logger.debug("View-specific attributes comparison for view %r: Detected in database: %s. Found in metadata: %s.",
        qualifed_view_name, str(conn_view_attributes), str(meta_view_attributes))

    # Create AlterViewOp object first, then each comparison function will set attributes if needed
    alter_view_op = AlterViewOp(
        view_name=metadata_view.name,
        schema=schema,
    )

    view_fqn = utils.gen_simple_qualified_name(view_name, schema)

    # Compare each view attribute using dedicated functions
    # Order: definition+columns -> comment -> security
    resolved_schema = schema or autogen_context.dialect.default_schema_name
    temp_view_schema = _configured_temp_view_schema(autogen_context) or resolved_schema
    definition_changed = _compare_view_definition_and_columns(
        alter_view_op, view_fqn, conn_view, metadata_view,
        conn=autogen_context.connection,
        schema=resolved_schema,
        temp_view_schema=temp_view_schema,
        autogen_context=autogen_context,
        upgrade_ops=upgrade_ops,
    )
    comment_changed = _compare_view_comment(view_fqn, conn_view, metadata_view)
    security_changed = _compare_view_security(
        alter_view_op, view_fqn, conn_view, metadata_view,
        conn_view_attributes, meta_view_attributes
    )

    if definition_changed or comment_changed:
        _carry_view_content_for_replace(
            alter_view_op, conn_view, metadata_view, conn_view_attributes.get(TableInfoKey.SECURITY)
        )

    # If any attribute has changed, append the operation
    if definition_changed or comment_changed or security_changed:
        upgrade_ops.ops.append(alter_view_op)

        # Log which attributes changed
        changed_attrs = []
        if definition_changed:
            changed_attrs.append("definition or columns")
        if comment_changed:
            changed_attrs.append("comment")
        if security_changed:
            changed_attrs.append("security")

        logger.debug(f"Detected changed view attributes ({', '.join(changed_attrs)}) on {view_fqn!r}")

def _carry_view_content_for_replace(
    alter_view_op: AlterViewOp, conn_view: View, metadata_view: View, conn_security: Optional[str]
) -> None:
    """Put on the op what the migration needs to recreate the view and to undo that.

    ALTER VIEW ... AS drops the view's comment and nothing alters a comment alone, so a change to the
    definition, the columns or the comment is applied as CREATE OR REPLACE VIEW with the full content. The
    comment goes on the op whenever either side has one, and the database's definition and columns go on it
    so that a comment-only change, and the downgrade, still have a definition to recreate the view with. A
    security other than the default goes on it too, so a view the migration drops and creates again, as
    :func:`order_view_and_mv_ops` may, keeps it.
    """
    from starrocks.sql.schema import extract_view_columns

    if conn_view.comment or metadata_view.comment:
        alter_view_op.comment = metadata_view.comment
        alter_view_op.existing_comment = conn_view.comment
    if alter_view_op.existing_definition is None:
        alter_view_op.existing_definition = conn_view.info.get(TableObjectInfoKey.DEFINITION)
        alter_view_op.existing_columns = extract_view_columns(conn_view)
    conn_security = TableAttributeNormalizer._simple_normalize(conn_security)
    if alter_view_op.existing_security is None and conn_security not in (None, ReflectionViewDefaults.security()):
        alter_view_op.existing_security = conn_security


def _configured_temp_view_schema(autogen_context: AutogenContext) -> Optional[str]:
    """Return the schema configured by user, or ``None`` when it is not set"""
    migration_context = getattr(autogen_context, "migration_context", None)
    opts = getattr(migration_context, "opts", None) or {}
    return opts.get(TEMP_VIEW_SCHEMA_OPT) or None


_SENT_AS_WRITTEN: Dict[str, Any] = {"no_parameters": True}
"""Execution options that make the driver send a statement as written, without %-formatting it."""


def _backquoted(name: str) -> str:
    """Quote a name as StarRocks reads a backquoted one, doubling each backquote inside it."""
    return "`" + name.replace("`", "``") + "`"


def _column_names_stated_by(view: Table) -> List[str]:
    """Return the column list a view or materialized view states, or an empty list when it states none."""
    return [column.name for column in view.columns]


class _TemporaryViewResult(NamedTuple):
    """What a temporary view gave for a definition: StarRocks' canonical text, or why it gave none."""

    definition: Optional[str]
    error: Optional[str]


_TEMPORARY_VIEW_NOT_SHOWN = "information_schema.views does not show the temporary view to the migration user"
"""Why a temporary view gave no text though StarRocks created it: the user lacks SELECT on the views there."""

_GRANT_ERRORS: Tuple[str, ...] = ("access denied", _TEMPORARY_VIEW_NOT_SHOWN)
"""Text in a temporary view's error that grants on the temporary-view schema would cure."""

_TABLE_PRIVILEGE_DENIED = re.compile(r"privilege\(s\) on TABLE\b", re.IGNORECASE)
"""An access denied error for a table the definition reads, which grants on the temporary-view schema do not cure."""


def _canonicalize_via_temp_view(
    conn: Connection, schema: str, sql: str, column_names: Sequence[str] = ()
) -> _TemporaryViewResult:
    """Canonicalize SQL by round-tripping it through a temporary VIEW in ``schema``.

    Returns the canonical SQL StarRocks reflects for the view, or no SQL and the error StarRocks gave, as when a
    referenced table does not exist yet in this migration, or the migration user lacks ``CREATE VIEW`` on
    ``schema``; the caller then falls back to the in-process normalizer.

    ``column_names`` is the column list the view states, which the temporary view states too: a definition
    whose SELECT list repeats a name, as ``SELECT t.id, t2.id`` does, is a view only under a column list that
    names its columns apart. StarRocks stores the same text with the column list or without it.

    The CREATE and DROP statements run without parameters. Given an empty parameter set,
    PyMySQL and asyncmy %-format the statement, so a ``%`` in the definition, as in
    ``date_format(dt, '%Y-%m')`` or ``LIKE 'x%'``, would make the CREATE fail. The DROP runs only once the
    CREATE has: a view that was never created needs none, and dropping it from a schema that does not exist
    would only fail too.
    """
    temp_name = f"_alembic_cmp_{uuid.uuid4().hex[:12]}"
    quoted = f"{_backquoted(schema)}.{_backquoted(temp_name)}"
    column_list = f" ({', '.join(_backquoted(name) for name in column_names)})" if column_names else ""
    try:
        conn.exec_driver_sql(
            f"CREATE OR REPLACE VIEW {quoted}{column_list} AS {sql}", execution_options=_SENT_AS_WRITTEN
        )
    except Exception as error:
        logger.debug("Temp-view canonicalization could not create %s, so the normalizer compares: %s", quoted, error)
        return _TemporaryViewResult(None, _starrocks_error_text(error))
    try:
        row = conn.exec_driver_sql(
            "SELECT view_definition FROM information_schema.views "
            "WHERE table_schema = %s AND table_name = %s",
            (schema, temp_name),
        ).fetchone()
        definition = row[0] if row else None
        return _TemporaryViewResult(definition, None if definition is not None else _TEMPORARY_VIEW_NOT_SHOWN)
    except Exception as error:
        logger.debug("Temp-view canonicalization could not read %s, so the normalizer compares: %s", quoted, error)
        return _TemporaryViewResult(None, _starrocks_error_text(error))
    finally:
        try:
            conn.exec_driver_sql(f"DROP VIEW IF EXISTS {quoted}", execution_options=_SENT_AS_WRITTEN)
        except Exception as e:
            logger.warning(
                "Failed to drop temp canonicalization view %s; it may need to be dropped manually: %s",
                quoted, e,
            )


def _get_canonical_sql_via_temp_view(
    conn: Connection, schema: str, sql: str, column_names: Sequence[str] = ()
) -> Optional[str]:
    """Return the canonical SQL of :func:`_canonicalize_via_temp_view`, or ``None`` when it gave none."""
    return _canonicalize_via_temp_view(conn, schema, sql, column_names).definition


def _starrocks_error_text(error: Exception) -> str:
    """Return what StarRocks said, without the statement SQLAlchemy adds to its wrapper of the driver's error."""
    return str(getattr(error, "orig", None) or error)


def _normalize_definitions_for_compare(conn_sql: str, meta_sql: str) -> Tuple[Optional[str], Optional[str]]:
    """Canonicalize a (connection, metadata) definition pair when the temp-view path is
    unavailable (no CREATE VIEW privilege, referenced object missing, or no connection).
    """
    conn_canonical = sql_canonical.canonicalize_sql(conn_sql, remove_qualifiers=True)
    meta_canonical = sql_canonical.canonicalize_sql(meta_sql, remove_qualifiers=True)
    if conn_canonical is not None and meta_canonical is not None:
        logger.debug("Definition comparison via sqlglot AST canonicalization.")
        return conn_canonical, meta_canonical
    logger.debug("sqlglot canonicalization unavailable for the pair; using plain string normalization.")
    return (
        TableAttributeNormalizer.normalize_sql(conn_sql, remove_qualifiers=True),
        TableAttributeNormalizer.normalize_sql(meta_sql, remove_qualifiers=True),
    )


def _compare_view_definition_and_columns(
    alter_view_op: AlterViewOp,
    view_fqn: str,
    conn_view: View,
    metadata_view: View,
    conn: Optional[Connection] = None,
    schema: Optional[str] = None,
    temp_view_schema: Optional[str] = None,
    autogen_context: Optional[AutogenContext] = None,
    upgrade_ops: Optional[UpgradeOps] = None,
) -> bool:
    """
    Compare view definition and columns, and update AlterViewOp if changed.

    Definition and columns are compared together because StarRocks changes the columns of a view by redefining
    it, with ALTER VIEW (columns) AS or CREATE OR REPLACE VIEW. A column-only change keeps the definition.

    A model that states the view's columns has its definition canonicalized under that column list, and the
    database's under the database's. When the temporary view cannot canonicalize the model's definition, only the
    in-process normalizer compares them. Given the autogenerate context and the revision's operations so far, a difference it sees is then a
    change only as :func:`_view_definition_difference_is_a_change` decides; otherwise it is warned about, and the
    view keeps the database's definition.

    Args:
        alter_view_op: AlterViewOp object to update
        view_fqn: Fully qualified view name for logging
        conn_view: View reflected from database
        metadata_view: View defined in metadata

    Returns:
        True if the definition or the columns changed, False otherwise
    """
    from starrocks.sql.schema import extract_view_columns

    # Compare definition (the main content of a view)
    # Definition is stored in table.info (not in dialect_options)
    conn_definition = conn_view.info.get(TableObjectInfoKey.DEFINITION, "")
    meta_definition = metadata_view.info.get(TableObjectInfoKey.DEFINITION, "")
    logger.debug("Compare view definition: conn_definition=%s, meta_definition=%s", conn_definition, meta_definition)

    starrocks_could_not_compare = False
    temporary_view_error: Optional[str] = None
    host_schema = temp_view_schema or schema
    model_column_names = _column_names_stated_by(metadata_view)
    database_column_names = _column_names_stated_by(conn_view) if model_column_names else []
    if conn is not None and schema is not None:
        canonical_meta, temporary_view_error = _canonicalize_via_temp_view(
            conn, host_schema, meta_definition, model_column_names
        )
        if canonical_meta is not None:
            conn_def_norm = TableAttributeNormalizer.normalize_sql(conn_definition)
            meta_def_norm = TableAttributeNormalizer.normalize_sql(canonical_meta)
            logger.debug("View definition comparison via temp-view canonicalization (temp schema %r).", host_schema)
        else:
            starrocks_could_not_compare = True
            logger.debug("Temp-view canonicalization failed for %r; falling back to AST/regex normalizer.", view_fqn)
            conn_def_norm, meta_def_norm = _normalize_definitions_for_compare(conn_definition, meta_definition)
    else:
        conn_def_norm, meta_def_norm = _normalize_definitions_for_compare(conn_definition, meta_definition)
    definition_changed = conn_def_norm != meta_def_norm
    if (
        definition_changed
        and starrocks_could_not_compare
        and autogen_context is not None
        and not _view_definition_difference_is_a_change(
            autogen_context, upgrade_ops, schema, conn_definition, meta_definition, conn, host_schema,
            database_column_names,
        )
    ):
        _warn_definition_not_compared(
            "View", view_fqn, host_schema, "the view keeps the database's definition", "alter the view by hand",
            [temporary_view_error],
        )
        definition_changed = False

    # Compare columns (if metadata specifies columns explicitly)
    # Note: Only column names and comments are compared, as StarRocks VIEW
    # does not support explicit column type/nullable specifications
    columns_changed = _compare_view_columns(conn_view, metadata_view)

    # Log comparison results
    logger.debug("compare_view_definition_and_columns: %s, definition_changed=%s, columns_changed=%s",
        view_fqn, definition_changed, columns_changed)

    # Log detailed changes for debugging
    if definition_changed:
        logger.debug(
            "  Definition change for %s:\n"
            "    Database: %s\n"
            "    Metadata: %s",
            view_fqn,
            conn_def_norm[:100] + "..." if len(conn_def_norm) > 100 else conn_def_norm,
            meta_def_norm[:100] + "..." if len(meta_def_norm) > 100 else meta_def_norm,
        )
        check_similar_string_and_warn(
            table_name=conn_view.name,
            attribute_name="DEFINITION",
            conn_str=str(conn_def_norm),
            meta_str=str(meta_def_norm),
            schema=conn_view.schema,
            object_label="View",
        )

    if columns_changed:
        conn_cols = {col.name: (col.comment or '') for col in conn_view.columns} if conn_view.columns else {}
        meta_cols = {col.name: (col.comment or '') for col in metadata_view.columns} if metadata_view.columns else {}
        logger.debug(
            "  Columns change for %s:\n"
            "    Database columns: %s\n"
            "    Metadata columns: %s",
            view_fqn,
            conn_cols,
            meta_cols,
        )

    if definition_changed or columns_changed:
        alter_view_op.definition = meta_definition
        alter_view_op.columns = extract_view_columns(metadata_view)
        alter_view_op.existing_definition = conn_definition
        alter_view_op.existing_columns = extract_view_columns(conn_view)

    if definition_changed:
        logger.info(f"Detected DEFINITION change on view {view_fqn!r}, "
                    f"from '{conn_def_norm}' (in database, normalized) to '{meta_def_norm}' (in metadata, normalized).")
        check_similar_string_and_warn(
            table_name=conn_view.name,
            attribute_name="DEFINITION",
            conn_str=str(conn_def_norm),
            meta_str=str(meta_def_norm),
            schema=conn_view.schema,
            object_label="View",
        )

    if columns_changed and not definition_changed:
        logger.info(f"Detected COLUMN change on view {view_fqn!r}, while its definition is unchanged.")

    return definition_changed or columns_changed


def _view_definition_difference_is_a_change(
    autogen_context: AutogenContext,
    upgrade_ops: Optional[UpgradeOps],
    schema: Optional[str],
    database_definition: str,
    model_definition: str,
    conn: Connection,
    host_schema: Optional[str],
    database_column_names: Sequence[str] = (),
) -> bool:
    """Whether a view definition difference only the in-process normalizer sees is a change to apply.

    It is when the database's definition goes through the temporary view that the model's could not, under the
    database's ``database_column_names``: see :func:`_only_the_model_definition_fails`, or when
    :func:`_definition_change_is_certain` says so. Autogenerate compares views before materialized views, so the
    materialized views the revision creates or drops are not among its operations yet; they are read from the model
    and the database, only when needed: see :func:`_materialized_views_the_model_adds` and
    :func:`_materialized_views_the_model_removes`.
    """
    server_version = autogen_context.dialect.server_version_info
    if _definitions_stored_as_written(server_version, _VIEW_DEFINITIONS_STORED_AS_WRITTEN_FROM):
        return True
    if _only_the_model_definition_fails(conn, host_schema, database_definition, database_column_names):
        return True
    default_schema = autogen_context.dialect.default_schema_name
    operations = upgrade_ops.ops if upgrade_ops is not None else []
    if _definition_change_is_certain(
        database_definition, model_definition, schema, _objects_the_revision_creates(operations, default_schema),
        _objects_the_revision_drops(operations, default_schema), default_schema,
        _columns_the_revision_drops(operations, default_schema),
    ):
        return True
    created_mvs = _materialized_views_the_model_adds(autogen_context)
    if _definition_reads_any(model_definition, schema, created_mvs, default_schema):
        return True
    read_by_the_database = _keys_read_by(database_definition, schema, default_schema)
    return bool(_materialized_views_the_model_removes(autogen_context, schema, read_by_the_database))


def _only_the_model_definition_fails(
    conn: Connection, host_schema: Optional[str], database_definition: str, database_column_names: Sequence[str] = ()
) -> bool:
    """Whether the database's definition goes through the temporary view, after the model's could not.

    Then the migration user has what the temporary view needs, so the model's definition reads something the
    database lacks, such as a column the revision adds, and it changed for certain.
    """
    return _get_canonical_sql_via_temp_view(conn, host_schema, database_definition, database_column_names) is not None


def _materialized_views_the_model_removes(
    autogen_context: AutogenContext, view_schema: Optional[str], keys: Set[Tuple[Optional[str], str]]
) -> Set[Tuple[Optional[str], str]]:
    """Return those of ``keys`` that name a materialized view autogenerate drops: the database has it, the model not.

    ``keys`` are those a view in ``view_schema`` reads. Autogenerate drops only what it compares: the materialized
    views of the view's own schema, and of another schema only with ``include_schemas`` and when the schema passes
    the name filters, and only those whose names pass them, as :func:`_autogen_for_mvs` filters them.
    """
    default_schema = autogen_context.dialect.default_schema_name
    own_schema = _object_key(view_schema, "", default_schema)[0]
    model_mvs = {
        _object_key(table.schema, table.name, default_schema)
        for table in autogen_context.metadata.tables.values()
        if (kind := table.info.get(TableObjectInfoKey.TABLE_KIND)) and kind.upper() == TableKind.MATERIALIZED_VIEW
    }
    removed: Set[Tuple[Optional[str], str]] = set()
    for schema in {schema for schema, _ in keys}:
        if schema != own_schema and not _other_schema_is_compared(autogen_context, schema):
            continue
        reflected_schema = view_schema if schema is not None and schema == own_schema else schema
        try:
            database_mvs = autogen_context.inspector.get_materialized_view_names(schema=reflected_schema)
        except Exception as error:
            logger.debug("Could not list the materialized views of schema %r: %s", reflected_schema, error)
            continue
        for name in database_mvs:
            key = (schema, name.lower())
            if key in keys and key not in model_mvs and autogen_context.run_name_filters(
                name, "materialized_view", {"schema_name": reflected_schema}
            ):
                removed.add(key)
    return removed


def _other_schema_is_compared(autogen_context: AutogenContext, schema: Optional[str]) -> bool:
    """Whether autogenerate compares ``schema``, which is not the compared view's: as Alembic chooses the schemas."""
    if schema is None:
        return True
    return bool(autogen_context.opts.get("include_schemas")) and autogen_context.run_name_filters(schema, "schema", {})


def _materialized_views_the_model_adds(autogen_context: AutogenContext) -> Set[Tuple[Optional[str], str]]:
    """Return the keys of the model's materialized views that the database does not have yet.

    Autogenerate compares views before materialized views, so when a view is compared the revision's creates of new
    materialized views are not among its operations yet.
    """
    default_schema = autogen_context.dialect.default_schema_name
    names_by_schema: Dict[Optional[str], List[str]] = {}
    for table in autogen_context.metadata.tables.values():
        kind = table.info.get(TableObjectInfoKey.TABLE_KIND)
        if kind and kind.upper() == TableKind.MATERIALIZED_VIEW:
            names_by_schema.setdefault(table.schema, []).append(table.name)
    keys: Set[Tuple[Optional[str], str]] = set()
    for schema, names in names_by_schema.items():
        existing = {name.lower() for name in autogen_context.inspector.get_materialized_view_names(schema=schema)}
        keys.update(_object_key(schema, name, default_schema) for name in names if name.lower() not in existing)
    return keys


def _compare_view_comment(
    view_fqn: str,
    conn_view: View,
    metadata_view: View,
) -> bool:
    """
    Compare view comment.

    StarRocks has no ALTER for a view's comment, so a change is applied by recreating the view, see
    `_carry_view_content_for_replace`.

    Args:
        view_fqn: Fully qualified view name for logging
        conn_view: View reflected from database
        metadata_view: View defined in metadata

    Returns:
        True if comment changed, False otherwise
    """
    # Compare comment (views don't use Alembic's built-in _compare_table_comment)
    conn_comment = (conn_view.comment or "").strip()
    meta_comment = (metadata_view.comment or "").strip()
    logger.debug("Compare view comment: conn_comment=%s, meta_comment=%s", conn_comment, meta_comment)

    comment_changed = conn_comment != meta_comment

    if comment_changed:
        logger.info(f"Detected COMMENT change on view {view_fqn!r}, "
                    f"from {conn_comment!r} (in database) to {meta_comment!r} (in metadata).")

    return comment_changed


def _compare_view_security(
    alter_view_op: AlterViewOp,
    view_fqn: str,
    conn_view: View,
    metadata_view: View,
    conn_view_attributes: CaseInsensitiveDict,
    meta_view_attributes: CaseInsensitiveDict,
) -> bool:
    """
    Compare view security attribute and update AlterViewOp if changed.

    A view whose metadata states no ``starrocks_security`` keeps the security it has in the database, as other
    attributes the metadata omits do.

    Args:
        alter_view_op: AlterViewOp object to update
        view_fqn: Fully qualified view name for logging
        conn_view: View reflected from database
        metadata_view: View defined in metadata
        conn_view_attributes: View attributes reflected from database
        meta_view_attributes: View attributes defined in metadata

    Returns:
        True if security changed, False otherwise
    """
    # Compare security attribute
    conn_security = TableAttributeNormalizer._simple_normalize(
        conn_view_attributes.get(TableInfoKey.SECURITY)
    )
    meta_security = TableAttributeNormalizer._simple_normalize(
        meta_view_attributes.get(TableInfoKey.SECURITY)
    )
    logger.debug("Compare view security: conn_security=%s, meta_security=%s", conn_security, meta_security)
    if meta_security is None:
        return False

    conn_security = (conn_security or ReflectionViewDefaults.security())
    security_changed = conn_security != meta_security

    if security_changed:
        logger.info(f"Detected SECURITY change on view {view_fqn!r}, "
                    f"from {conn_security!r} (in database) to {meta_security!r} (in metadata).")
        alter_view_op.security = meta_security
        alter_view_op.existing_security = conn_security

    return security_changed


def _compare_view_columns(conn_view: View, metadata_view: View) -> bool:
    """
    Compare columns between connection view and metadata view.

    Returns True if columns have changed, False otherwise.
    Only compares if metadata_view explicitly defines columns.

    Note: StarRocks VIEW columns only support name and comment (not type or nullable).
    These are derived from the query statement and cannot be explicitly specified.

    Args:
        conn_view: View reflected from database
        metadata_view: View defined in metadata

    Returns:
        True if column names or comments differ, False otherwise
    """
    # logger.debug("Compare view columns.")
    # If metadata doesn't define columns, skip comparison
    if not metadata_view.columns:
        return False

    # If conn_view has no columns (reflection failed), we can't compare
    # This might happen if reflection encountered an error
    if not conn_view.columns:
        logger.warning(
            f"View '{metadata_view.name}' has columns defined in metadata, "
            f"but no columns were reflected from database. Cannot compare columns."
        )
        return False

    # Build column name -> comment mapping
    conn_cols = {col.name: (col.comment or '') for col in conn_view.columns}
    meta_cols = {col.name: (col.comment or '') for col in metadata_view.columns}

    # Check for added or removed columns
    conn_col_names = set(conn_cols.keys())
    meta_col_names = set(meta_cols.keys())

    if conn_col_names != meta_col_names:
        logger.debug(
            f"View '{metadata_view.name}': Column names differ. "
            f"Database: {sorted(conn_col_names)}, Metadata: {sorted(meta_col_names)}"
        )
        return True

    # Check for column comment changes
    for col_name in meta_col_names:
        conn_comment = conn_cols[col_name]
        meta_comment = meta_cols[col_name]

        if conn_comment != meta_comment:
            logger.debug(
                f"View '{metadata_view.name}': Column '{col_name}' comment differs. "
                f"Database: '{conn_comment}', Metadata: '{meta_comment}'"
            )
            return True

    return False


# ==============================================================================
# Materialized View Comparison
# ==============================================================================
@comparators_dispatch_for_starrocks("schema", run_last=True)
def _autogen_for_mvs(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schemas: Union[Set[None], Set[Optional[str]]]
) -> None:
    """
    Main autogenerate entrypoint for materialized views.

    Scan materialized views in database and compare with metadata.
    """
    inspector: Inspector = autogen_context.inspector
    metadata = autogen_context.metadata
    default_schema = inspector.bind.dialect.default_schema_name

    conn_mv_names: Set[Tuple[Optional[str], str]] = set()
    logger.debug("Start to autogenerate for mvs: schemas: %r", schemas)

    for schema in schemas:
        logger.debug("Start to get materialized view names from database, schema: %s", schema)
        mvs = set(inspector.get_materialized_view_names(schema))
        conn_mv_names.update(
            (schema, mvname)
            for mvname in mvs
            if autogen_context.run_name_filters(
                mvname, "materialized_view", {"schema_name": schema}
            )
        )

    # Get all MVs from metadata and apply name filters, normalize schema (default -> None)
    all_normed_metadata_mv_names = set(
        (table.schema if table.schema != default_schema else None, table.name)
        for table in metadata.tables.values()
        if (table_kind:= table.info.get(TableObjectInfoKey.TABLE_KIND)) and table_kind.upper() == TableKind.MATERIALIZED_VIEW
        and autogen_context.run_name_filters(
            table.name, "materialized_view", {"schema_name": table.schema}
        )
    )
    # Filter by target schemas
    metadata_mv_names = OrderedSet(
        (s, t) for s, t in all_normed_metadata_mv_names if s in schemas
    )

    if len(schemas) == 1 and None in schemas:
        other_schemas = set(
            s for s, _ in all_normed_metadata_mv_names if s not in schemas
        )
        if other_schemas:
            warnings.warn(
                "Materialized views in other schemas will be ignored. It's probably you have not set the `include_schemas` "
                "and `include_name` parameters correctly. Set them in your `env.py` properly if you want to manage materialized views "
                f"in other schemas: {other_schemas!r}",
                UserWarning,
            )

    if _left_alone_before_sqlalchemy_2(conn_mv_names, "materialized view"):
        return
    _compare_mvs(
        conn_mv_names,
        metadata_mv_names,
        inspector,
        upgrade_ops,
        autogen_context,
    )


def _compare_mvs(
    conn_mv_names: Set[Tuple[Optional[str], str]],
    metadata_mv_names: Set[Tuple[Optional[str], str]],
    inspector: Inspector,
    upgrade_ops: UpgradeOps,
    autogen_context: AutogenContext,
) -> None:
    """Compare materialized views between database and metadata, generating add/drop/alter operations."""
    metadata = autogen_context.metadata
    logger.debug("Start to compare mvs, conn_mv_names (from DB): %s, metadata_mv_names (from metadata): %s",
        conn_mv_names, metadata_mv_names)

    # Normalize metadata schema: replace default schema with None to align with inspector
    default_schema = inspector.bind.dialect.default_schema_name
    metadata_mv_names_no_dflt = OrderedSet(
        [
            (schema if schema != default_schema else None, tname)
            for schema, tname in metadata_mv_names
        ]
    )
    # Build a lookup from (schema, name) to Table object for metadata MVs (using normalized schema)
    mv_name_to_table = {
        (table.schema if table.schema != default_schema else None, table.name): table
        for table in metadata.tables.values()
        if (table_kind:= table.info.get(TableObjectInfoKey.TABLE_KIND)) and table_kind.upper() == TableKind.MATERIALIZED_VIEW
    }
    metadata_mv_names = metadata_mv_names_no_dflt

    # Added MVs (in metadata but not in database)
    added_mvs = metadata_mv_names.difference(conn_mv_names)
    logger.debug("Added MVs (in metadata but not in DB): %s", added_mvs)
    for s, mvname in added_mvs:
        logger.debug("Processing added MV: schema=%s, name=%s", s, mvname)
        mv_fqn = utils.gen_simple_qualified_name(mvname, s)
        metadata_mv = mv_name_to_table[(s, mvname)]
        if autogen_context.run_object_filters(
            metadata_mv, mvname, "materialized_view", False, None
        ):
            upgrade_ops.ops.append(CreateMaterializedViewOp.from_materialized_view(metadata_mv))
            logger.info(f"Detected added materialized view {mv_fqn!r}")

    # Dropped MVs (in database but not in metadata)
    # Use a separate MetaData to avoid polluting the user's metadata
    removal_metadata = sa_schema.MetaData()

    dropped_mvs = conn_mv_names.difference(metadata_mv_names)
    logger.debug("Dropped MVs (in DB but not in metadata): %s", dropped_mvs)
    for s, mvname in dropped_mvs:
        logger.debug("Processing dropped MV: schema=%s, name=%s", s, mvname)
        name = sa_schema._get_table_key(mvname, s)
        exists = name in removal_metadata.tables
        # Create a MaterializedView object (not Table) since we know it's a materialized view
        # Use empty definition - it will be populated by reflect_table
        t = MaterializedView(mvname, removal_metadata, definition='<placeholder_definition>', schema=s)

        if not exists:
            # Reflect the MV using StarRocks' custom reflection logic
            # This will automatically populate table_kind, definition, and dialect_options
            inspector.reflect_table(t, include_columns=None)

        if autogen_context.run_object_filters(t, mvname, "materialized_view", True, None):
            upgrade_ops.ops.append(DropMaterializedViewOp.from_materialized_view(t))
            logger.info(f"Detected removed materialized view {name!r}")

    # Modified MVs (in both database and metadata)
    # Use a separate MetaData for reflected MVs
    existing_metadata = sa_schema.MetaData()
    existing_mvs = conn_mv_names.intersection(metadata_mv_names)
    logger.debug("Existing MVs (in both DB and metadata): %s", existing_mvs)
    for s, mvname in existing_mvs:
        logger.debug("Processing existing MV: schema=%s, name=%s", s, mvname)
        # Use reflect_table to get the full MV object from database
        name = sa_schema._get_table_key(mvname, s)
        exists = name in existing_metadata.tables
        # Create a MaterializedView object (not Table) since we know it's a materialized view
        # Use empty definition - it will be populated by reflect_table
        t = MaterializedView(mvname, existing_metadata, definition='<placeholder_definition>', schema=s)

        if not exists:
            # Reflect the MV using StarRocks' custom reflection logic
            # This will automatically populate table_kind, definition, and dialect_options
            inspector.reflect_table(t, include_columns=None)

    # Compare existing MVs in sorted order for consistent output
    for s, mvname in sorted(existing_mvs, key=lambda x: (x[0] or "", x[1])):
        s = s or None
        name = sa_schema._get_table_key(mvname, s)
        metadata_mv = mv_name_to_table[(s, mvname)]
        conn_mv = existing_metadata.tables[name]

        # Apply object filters
        if autogen_context.run_object_filters(
            metadata_mv, mvname, "materialized_view", False, conn_mv
        ):
            # Dispatch to _compare_mv for detailed comparison
            comparators.dispatch("materialized_view")(
                autogen_context,
                upgrade_ops,
                s,
                mvname,
                conn_mv,
                metadata_mv,
            )


def _compare_mv_definition(
    ops_list: List,
    schema: Optional[str],
    mv_name: str,
    conn_mv: Union[MaterializedView, Table],
    metadata_mv: Union[MaterializedView, Table],
    conn: Optional[Connection] = None,
    temp_view_schema: Optional[str] = None,
    server_version: Optional[Tuple[int, ...]] = None,
    revision_creates: Optional[Set[Tuple[Optional[str], str]]] = None,
    default_schema: Optional[str] = None,
    revision_drops: Optional[Set[Tuple[Optional[str], str]]] = None,
    revision_drops_columns: Optional[Dict[Tuple[Optional[str], str], Set[str]]] = None,
) -> None:
    """
    Compare MV definition, raising UnalterableChange if it changed.

    StarRocks does not support altering an MV definition, so the caller recreates the MV on a change. With a
    connection, the definitions are compared as StarRocks canonicalizes them through temporary views, which state
    the column lists when the model's MV states one. When it cannot,
    only the in-process normalizer compares them, and a difference it sees is a change only from
    ``_MV_DEFINITIONS_STORED_AS_WRITTEN_FROM``, when only the model's definition fails to go through the temporary
    view (see :func:`_only_the_model_definition_fails`), or as :func:`_definition_change_is_certain` decides;
    otherwise it is warned about, and no operation is generated.
    ``revision_creates`` and ``revision_drops`` hold the ``(schema, name)`` keys of the objects the revision creates,
    and of those it drops and does not create again, and ``revision_drops_columns`` the columns it drops, by table.
    """
    # Always extract definition from table.info to support metadata defined as plain Table
    conn_def_raw = getattr(conn_mv, "definition", None) or conn_mv.info.get(TableObjectInfoKey.DEFINITION, "")
    meta_def_raw = getattr(metadata_mv, "definition", None) or metadata_mv.info.get(TableObjectInfoKey.DEFINITION, "")
    logger.debug("Compare mv definition: conn_def_raw=%s, meta_def_raw=%s", conn_def_raw, meta_def_raw)

    starrocks_could_not_compare = False
    only_the_model_definition_failed = False
    temporary_view_errors: List[Optional[str]] = []
    host_schema = temp_view_schema or schema
    model_column_names = _column_names_stated_by(metadata_mv)
    database_column_names = _column_names_stated_by(conn_mv) if model_column_names else []
    # Preferred path: canonicalize both sides through temporary VIEWs.
    if conn is not None and schema is not None:  # schema is already resolved by _compare_mv
        canonical_conn, database_error = _canonicalize_via_temp_view(
            conn, host_schema, conn_def_raw, database_column_names
        )
        canonical_meta, model_error = _canonicalize_via_temp_view(conn, host_schema, meta_def_raw, model_column_names)
        temporary_view_errors = [model_error, database_error]
        if canonical_conn is not None and canonical_meta is not None:
            conn_def_norm = TableAttributeNormalizer.normalize_sql(canonical_conn)
            meta_def_norm = TableAttributeNormalizer.normalize_sql(canonical_meta)
            logger.debug("MV definition comparison via temp-view canonicalization.")
        else:
            starrocks_could_not_compare = True
            only_the_model_definition_failed = canonical_conn is not None
            logger.debug("Temp-view canonicalization failed for MV %r; falling back to AST/regex normalizer.", mv_name)
            conn_def_norm, meta_def_norm = _normalize_definitions_for_compare(conn_def_raw, meta_def_raw)
    else:
        # Canonicalize and remove qualifiers (schema/table) to avoid false diffs on equivalent SQL
        conn_def_norm, meta_def_norm = _normalize_definitions_for_compare(conn_def_raw, meta_def_raw)

    if conn_def_norm != meta_def_norm:
        check_similar_string_and_warn(
            table_name=mv_name,
            attribute_name="DEFINITION",
            conn_str=str(conn_def_norm),
            meta_str=str(meta_def_norm),
            schema=schema,
            object_label="Materialized view",
        )
        mv_fqn = utils.gen_simple_qualified_name(mv_name, schema)
        if (
            starrocks_could_not_compare
            and not _definitions_stored_as_written(server_version, _MV_DEFINITIONS_STORED_AS_WRITTEN_FROM)
            and not only_the_model_definition_failed
            and not _definition_change_is_certain(
                conn_def_raw, meta_def_raw, schema, revision_creates or set(), revision_drops or set(), default_schema,
                revision_drops_columns,
            )
        ):
            _warn_definition_not_compared(
                "Materialized view", mv_fqn, host_schema, "no operation is generated for it",
                "drop and create the materialized view by hand", temporary_view_errors,
            )
            return
        raise UnalterableChange(
            f"Detected DEFINITION change on materialized view {mv_fqn!r}, "
            f"from {conn_def_norm!r} (in database, normalized) to {meta_def_norm!r} (in metadata, normalized). "
            f"StarRocks does not support altering MV definition."
        )


_MV_DEFINITIONS_STORED_AS_WRITTEN_FROM: Tuple[int, ...] = (4, 0, 2)
"""The first StarRocks version that shows an MV's definition as written, rather than rewritten (StarRocks #64318)."""

_VIEW_DEFINITIONS_STORED_AS_WRITTEN_FROM: Tuple[int, ...] = (4, 0, 7)
"""The first StarRocks version that keeps a view's definition as written, rather than rewritten (StarRocks #68040).

A view created before the server reached it keeps its rewritten definition until it is altered.
"""


def _definitions_stored_as_written(server_version: Optional[Tuple[int, ...]], first_version: Tuple[int, ...]) -> bool:
    """Whether StarRocks shows the definitions as written, so that a difference the normalizer sees is a change.

    Before ``first_version`` it shows a rewritten definition, with table-qualified columns that the in-process
    normalizer keeps, so the normalizer would see a change in every definition on every run.
    """
    return bool(server_version) and tuple(server_version) >= first_version


def _definition_change_is_certain(
    database_definition: str,
    model_definition: str,
    schema: Optional[str],
    revision_creates: Set[Tuple[Optional[str], str]],
    revision_drops: Set[Tuple[Optional[str], str]],
    default_schema: Optional[str],
    revision_drops_columns: Optional[Dict[Tuple[Optional[str], str], Set[str]]] = None,
) -> bool:
    """Whether a definition changed for certain, though no temporary view could compare the two.

    It did when the model's definition reads an object the same revision creates, which is why the temporary view
    failed, or when the database's reads an object or a column the revision drops, which the model's then cannot
    read; ``revision_drops_columns`` holds the lower-cased column names the revision drops or renames, by table.
    """
    return (
        _definition_reads_any(model_definition, schema, revision_creates, default_schema)
        or _definition_reads_any(database_definition, schema, revision_drops, default_schema)
        or _definition_reads_a_column_of(database_definition, schema, revision_drops_columns or {}, default_schema)
    )


def _temporary_view_advice(said: Sequence[str], host_schema: Optional[str]) -> str:
    """Return what to do about the errors a temporary view gave, or nothing when no step of the user's would cure them.

    Grants on the temporary-view schema cure an access denied error there, or a view it cannot see. A schema that
    does not exist has to be created, or another one named instead. Neither helps when the denied privilege is
    SELECT on a table the definition reads, or the unknown database is not the temporary-view schema.
    """
    lowered = [error.lower() for error in said]
    advice = []
    if host_schema and any("unknown database" in error and f"'{host_schema.lower()}'" in error for error in lowered):
        advice.append(
            f"Create the schema {host_schema!r}, or set {TEMP_VIEW_SCHEMA_OPT} to a schema that exists and where "
            f"the migration user has CREATE VIEW."
        )
    if any(
        any(marker in error for marker in _GRANT_ERRORS) and not _TABLE_PRIVILEGE_DENIED.search(error)
        for error in lowered
    ):
        advice.append(
            f"Grant the migration user CREATE VIEW on that schema, and SELECT and DROP on its views, or set "
            f"{TEMP_VIEW_SCHEMA_OPT} to a schema where it has them."
        )
    return "".join(f" {step}" for step in advice)


def _warn_definition_not_compared(
    kind: str, fqn: str, host_schema: Optional[str], what_happens: str, by_hand: str, errors: Sequence[Optional[str]]
) -> None:
    """Warn that a view's or MV's definition could not be compared through StarRocks, so it is left as it is.

    ``errors`` holds what StarRocks said when the temporary views failed, each one said once. Only an error that
    privileges or another schema cure, such as access denied or an unknown database, gets the advice to grant
    the temporary view what it needs: ``CREATE VIEW`` on its schema, and ``SELECT`` and ``DROP`` on the views
    there; see :func:`_canonicalize_via_temp_view`. Any other error, as for a definition StarRocks refuses, one
    that reads what the database lacks, or a table the user may not read, would fail the same way with every
    privilege on the temporary-view schema; see :func:`_temporary_view_advice`.
    """
    said = list(dict.fromkeys(error for error in errors if error))
    advice = _temporary_view_advice(said, host_schema)
    logger.warning(
        "%s %r: its definition in the model differs from the database's as the dialect's own normalizer reads them, "
        "but StarRocks could not canonicalize them through a temporary view in schema %r, so they cannot be "
        "compared, and %s. StarRocks said: %s.%s If the definition did change, %s.",
        kind, fqn, host_schema, what_happens, "; ".join(said) or "nothing", advice, by_hand,
    )


def _compare_mv_comment(
    ops_list: List,
    schema: Optional[str],
    mv_name: str,
    conn_mv: Union[MaterializedView, Table],
    metadata_mv: Union[MaterializedView, Table]
) -> None:
    """
    Compare MV comment, raising UnalterableChange if it changed.

    StarRocks cannot alter a materialized view's comment, so the caller recreates the materialized view.
    """
    conn_comment = (conn_mv.comment or "").strip()
    meta_comment = (metadata_mv.comment or "").strip()
    logger.debug("Compare mv comment: conn_comment=%r, meta_comment=%r", conn_comment, meta_comment)

    if conn_comment != meta_comment:
        mv_fqn = utils.gen_simple_qualified_name(mv_name, schema)
        raise UnalterableChange(
            f"Detected COMMENT change on materialized view {mv_fqn!r}, "
            f"from {conn_comment!r} to {meta_comment!r}. "
            f"StarRocks does not support altering MV comment."
        )


def _split_refresh_moment(refresh: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Split a refresh string into its stated moment (IMMEDIATE, DEFERRED or None) and the rest.

    The result is upper-cased with whitespace collapsed; the scheme keeps whatever follows the moment.
    """
    if refresh is None:
        return None, None
    normalized = " ".join(str(refresh).split()).upper()
    for moment in ("IMMEDIATE", "DEFERRED"):
        if normalized.startswith(moment + " "):
            return moment, normalized[len(moment) + 1:]
    return None, normalized


def _normalize_refresh_scheme(refresh: Optional[str]) -> Optional[str]:
    """Normalize the scheme of a refresh string for comparison, leaving out its IMMEDIATE or DEFERRED moment.

    SHOW CREATE spells a periodic refresh SCHEDULE since 4.1.1 where the model says ASYNC, quotes START("...")
    with double quotes where the model uses single quotes, writes ``EVERY(`` where the model may write
    ``EVERY (``, so whitespace around parentheses does not count, and gives a START time in full, as
    ``2026-10-25 00:00:00`` for a model's ``2026-10-25``.
    """
    _, scheme = _split_refresh_moment(refresh)
    if scheme is None:
        return None
    if scheme.startswith("SCHEDULE"):
        scheme = "ASYNC" + scheme[len("SCHEDULE"):]
    scheme = re.sub(r"\s*([()])\s*", r"\1", " ".join(scheme.split())).replace('"', "'")
    return _REFRESH_START.sub(_full_refresh_start, scheme)


_REFRESH_START: re.Pattern[str] = re.compile(r"START\('(\d{4}-\d{2}-\d{2})(?: (\d{2}:\d{2})(:\d{2})?)?'\)")


def _full_refresh_start(match: re.Match[str]) -> str:
    """Write a refresh START time as StarRocks keeps it: a date gets midnight, and a time without seconds ``:00``."""
    date, hours_and_minutes, seconds = match.groups()
    return f"START('{date} {hours_and_minutes or '00:00'}{seconds or ':00'}')"


def _compare_mv_refresh_moment(
    schema: Optional[str],
    mv_name: str,
    conn_mv_attributes: Dict[str, Any],
    meta_mv_attributes: Dict[str, Any],
) -> None:
    """Raise UnalterableChange when the model states a refresh moment the database does not have.

    Only the model's stated moment counts. IMMEDIATE is the default and SHOW CREATE never renders it, so an absent
    moment in the database means IMMEDIATE, and ALTER MATERIALIZED VIEW ... REFRESH keeps whatever moment the view
    was created with, so a different moment needs the view recreated.
    """
    meta_moment, _ = _split_refresh_moment(meta_mv_attributes.get(TableInfoKey.REFRESH))
    if meta_moment is None:
        return
    conn_moment, _ = _split_refresh_moment(conn_mv_attributes.get(TableInfoKey.REFRESH))
    if (conn_moment or "IMMEDIATE") != meta_moment:
        mv_fqn = utils.gen_simple_qualified_name(mv_name, schema)
        raise UnalterableChange(
            f"Detected REFRESH moment change on materialized view {mv_fqn!r}, "
            f"from {conn_moment or 'IMMEDIATE'} to {meta_moment}. "
            f"StarRocks does not support altering the refresh moment of a materialized view."
        )


def _compare_mv_refresh(
    ops_list: List,
    schema: Optional[str],
    mv_name: str,
    conn_mv_attributes: Dict[str, Any],
    meta_mv_attributes: Dict[str, Any]
) -> None:
    """
    Compare MV refresh schemes and add AlterMaterializedViewOp if changed.

    This is a mutable attribute that can be altered using ALTER MATERIALIZED VIEW. The refresh moment
    (IMMEDIATE or DEFERRED) is not part of the comparison, see `_compare_mv_refresh_moment`. A model that states no
    refresh, which an MV with a distribution may omit, leaves the database's alone.
    """
    conn_refresh_raw = conn_mv_attributes.get(TableInfoKey.REFRESH)
    meta_refresh_raw = meta_mv_attributes.get(TableInfoKey.REFRESH)
    logger.debug("Compare mv refresh: conn_refresh_raw=%s, meta_refresh_raw=%s", conn_refresh_raw, meta_refresh_raw)
    if meta_refresh_raw is None:
        return

    conn_refresh = _normalize_refresh_scheme(conn_refresh_raw)
    meta_refresh = _normalize_refresh_scheme(meta_refresh_raw)

    if conn_refresh != meta_refresh:

        mv_fqn = utils.gen_simple_qualified_name(mv_name, schema)
        ops_list.append(
            AlterMaterializedViewOp(
                view_name=mv_name,
                schema=schema,
                refresh=str(meta_refresh_raw),
                properties=None,
                existing_refresh=str(conn_refresh_raw) if conn_refresh_raw is not None else None,
                existing_properties=None,
            )
        )
        logger.info(f"Detected REFRESH change on materialized view {mv_fqn!r}, "
                    f"from {str(conn_refresh_raw)!r} to {str(meta_refresh_raw)!r}.")
        check_similar_string_and_warn(
            table_name=mv_name, attribute_name="REFRESH",
            conn_str=str(conn_refresh_raw),
            meta_str=str(meta_refresh_raw),
            schema=schema, object_label="Materialized view")

def _compare_mv_properties(
    ops_list: List,
    schema: Optional[str],
    mv_name: str,
    conn_mv_attributes: Dict[str, Any],
    meta_mv_attributes: Dict[str, Any],
    run_mode: str,
) -> None:
    """
    Compare MV properties and add AlterMaterializedViewOp if changed.

    This is a mutable attribute that can be altered using ALTER MATERIALIZED VIEW, except for the properties of
    :class:`UnalterableMaterializedViewProperty`, which are left out with a warning.
    """
    properties_to_set, properties_for_reverse = _compare_table_properties_impl(
        schema, mv_name, conn_mv_attributes, meta_mv_attributes, run_mode,
        default_cls=ReflectionMVDefaults, object_label="Materialized view", add_default_prefix=False)
    mv_fqn = utils.gen_simple_qualified_name(mv_name, schema)
    _leave_out_properties_starrocks_cannot_set(
        properties_to_set, properties_for_reverse, "Materialized view", mv_fqn, UnalterableMaterializedViewProperty.reason
    )
    if properties_to_set:
        ops_list.append(
            AlterMaterializedViewOp(
                view_name=mv_name,
                schema=schema,
                refresh=None,
                properties=properties_to_set,
                existing_refresh=None,
                existing_properties=properties_for_reverse if properties_for_reverse else None,
            )
        )
        logger.debug("Detected PROPERTIES change on materialized view %r.", mv_fqn)


@comparators_dispatch_for_starrocks("materialized_view")
def _compare_mv(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schema: Optional[str],
    mv_name: str,
    conn_mv: Union[MaterializedView, Table],
    metadata_mv: Union[MaterializedView, Table],
) -> None:
    """
    Compare a single materialized view and generate operations if needed.

    Categorizes changes into:
    - Mutable attributes (can use ALTER): refresh, properties
    - Immutable attributes (require DROP + CREATE): definition, partition_by,
      distributed_by, order_by, comment

    The immutable attributes are compared first. A change to any of them is applied by dropping the materialized
    view and creating it again, since its data is derived from its base tables; the mutable attributes then need no
    ALTER. Otherwise it generates one AlterOp per changed mutable attribute.
    """
    # Handle MV creation and deletion scenarios (should not happen here)
    if conn_mv is None or metadata_mv is None:
        logger.warning(
            "_compare_mv: both conn_mv and metadata_mv should not be None for %s.%s, skipping",
            schema or autogen_context.dialect.default_schema_name,
            mv_name
        )
        return

    logger.debug("Compare materialized view: mv_name=%r, schema=%r", mv_name, schema)

    # Extract dialect_options for comparison using case-insensitive helper
    conn_mv_attributes = extract_dialect_options_as_case_insensitive(conn_mv)
    meta_mv_attributes = extract_dialect_options_as_case_insensitive(metadata_mv)

    logger.debug(
        "MV-specific attributes comparison for materialized view %r: "
        "Detected in database: %s. Found in metadata: %s.",
        mv_name,
        conn_mv_attributes,
        meta_mv_attributes,
    )

    # Track the number of operations before comparison
    ops_before = len(upgrade_ops.ops)

    # Compare each MV attribute using dedicated functions
    resolved_schema = schema or autogen_context.dialect.default_schema_name
    temp_view_schema = _configured_temp_view_schema(autogen_context) or resolved_schema
    unalterable_changes = _collect_unalterable_changes([
        lambda: _compare_mv_definition(
            upgrade_ops.ops,
            resolved_schema,
            mv_name,
            conn_mv,
            metadata_mv,
            conn=autogen_context.connection,
            temp_view_schema=temp_view_schema,
            server_version=autogen_context.dialect.server_version_info,
            revision_creates=_objects_the_revision_creates(
                upgrade_ops.ops, autogen_context.dialect.default_schema_name
            ),
            default_schema=autogen_context.dialect.default_schema_name,
            revision_drops=_objects_the_revision_drops(upgrade_ops.ops, autogen_context.dialect.default_schema_name),
            revision_drops_columns=_columns_the_revision_drops(
                upgrade_ops.ops, autogen_context.dialect.default_schema_name
            ),
        ),
        lambda: _compare_table_partition(
            upgrade_ops.ops,
            schema,
            mv_name,
            conn_mv_attributes,
            meta_mv_attributes,
            default_value_override=ReflectionMVDefaults.partition_by(),
            support_change_override=AlterMVEnablement.PARTITION_BY,
            ddl_object="MATERIALIZED VIEW",
            object_label="Materialized view",
        ),
        lambda: _compare_table_distribution(
            upgrade_ops.ops,
            schema,
            mv_name,
            conn_mv_attributes,
            meta_mv_attributes,
            default_value_override=ReflectionMVDefaults.distribution_type(),
            support_change_override=AlterMVEnablement.DISTRIBUTED_BY,
            ddl_object="MATERIALIZED VIEW",
            object_label="Materialized view",
            skip_when_meta_unset=True,
        ),
        lambda: _compare_table_order_by(
            upgrade_ops.ops,
            schema,
            mv_name,
            conn_mv_attributes,
            meta_mv_attributes,
            default_value_override=ReflectionMVDefaults.order_by(),
            support_change_override=AlterMVEnablement.ORDER_BY,
            ddl_object="MATERIALIZED VIEW",
            object_label="Materialized view",
        ),
        lambda: _compare_mv_comment(upgrade_ops.ops, schema, mv_name, conn_mv, metadata_mv),
        lambda: _compare_mv_refresh_moment(schema, mv_name, conn_mv_attributes, meta_mv_attributes),
    ])
    run_mode = autogen_context.dialect.run_mode
    if unalterable_changes:
        _recreate_materialized_view(
            upgrade_ops, schema, mv_name, conn_mv, metadata_mv, unalterable_changes, autogen_context
        )
        return

    # Mutable attributes (will generate AlterMaterializedViewOp if changed):
    _compare_mv_refresh(upgrade_ops.ops, schema, mv_name, conn_mv_attributes, meta_mv_attributes)
    _compare_mv_properties(upgrade_ops.ops, schema, mv_name, conn_mv_attributes, meta_mv_attributes, run_mode)

    # Log summary if any operations were generated
    ops_after = len(upgrade_ops.ops)
    if ops_after > ops_before:
        mv_fqn = utils.gen_simple_qualified_name(mv_name, schema)
        num_changes = ops_after - ops_before
        logger.debug("Materialized view %r comparison complete: %d ALTER operation(s) generated", mv_fqn, num_changes)

def _collect_unalterable_changes(comparisons: List[Callable[[], None]]) -> List[str]:
    """Run each attribute comparison and return the messages of the changes StarRocks cannot ALTER."""
    changes = []
    for compare_attribute in comparisons:
        try:
            compare_attribute()
        except UnalterableChange as change:
            changes.append(str(change))
    return changes


def _recreate_materialized_view(
    upgrade_ops: UpgradeOps,
    schema: Optional[str],
    mv_name: str,
    conn_mv: Union[MaterializedView, Table],
    metadata_mv: Union[MaterializedView, Table],
    changes: List[str],
    autogen_context: AutogenContext,
) -> None:
    """Apply changes ALTER MATERIALIZED VIEW cannot express by dropping the materialized view and creating it again.

    Its data is derived from its base tables, so recreating it applies the model as written; StarRocks then fills it
    again according to its refresh scheme. What the model leaves out and an ALTER would have left alone is kept from
    the database: see :func:`_keep_what_the_model_leaves_out`. The drop keeps the database's definition, so the
    downgrade restores it.
    """
    mv_fqn = utils.gen_simple_qualified_name(mv_name, schema)
    create_op = CreateMaterializedViewOp.from_materialized_view(metadata_mv)
    database_op = CreateMaterializedViewOp.from_materialized_view(conn_mv)
    database_properties = {key.lower() for key in database_op.kwargs.get(TableInfoKeyWithPrefix.PROPERTIES) or {}}
    kept, lost, reasons = _keep_what_the_model_leaves_out(
        create_op,
        database_op,
        autogen_context.dialect.run_mode,
        _MaterializedViewFacts(
            partitions_are_range=(
                _partitions_of_the_new_mv_are_range(create_op, autogen_context)
                if database_properties & _RANGE_PARTITIONED_MV_ONLY_PROPERTIES else None
            ),
            server_version=autogen_context.dialect.server_version_info,
            default_schema=autogen_context.dialect.default_schema_name,
        ),
    )
    logger.warning(
        "Materialized view %r changed in ways ALTER MATERIALIZED VIEW cannot express, so the migration drops it and "
        "creates it again, and StarRocks refills it as it refreshes. %s%s%s",
        mv_fqn,
        " ".join(changes),
        f" The new one keeps the database's {', '.join(kept)}, which the model leaves out." if kept else "",
        f" It loses the database's properties {lost!r}, which StarRocks refuses for a materialized view with "
        f"{' or '.join(reasons)}." if lost else "",
    )
    upgrade_ops.ops.append(DropMaterializedViewOp.from_materialized_view(conn_mv))
    upgrade_ops.ops.append(create_op)


_PARTITIONED_MV_ONLY_PROPERTIES: frozenset = frozenset({
    'auto_refresh_partitions_limit', 'partition_refresh_number', 'partition_retention_condition', 'partition_ttl',
    'partition_ttl_number',
})
"""MV properties CREATE MATERIALIZED VIEW refuses for an MV with no PARTITION BY, on StarRocks 3.3.22 and 4.1.6."""

_RANGE_PARTITIONED_MV_ONLY_PROPERTIES: frozenset = frozenset({'partition_ttl_number'})
"""MV properties CREATE MATERIALIZED VIEW refuses for an MV whose partitions are not RANGE partitions.

StarRocks refuses them from ``_RANGE_PARTITIONED_MV_ONLY_FROM``, as 4.1.6 does; 3.3.22 takes them for a
LIST-partitioned MV. Both take ``partition_ttl`` for either kind.
"""

_RANGE_PARTITIONED_MV_ONLY_FROM: Tuple[int, ...] = (3, 4, 1)
"""The first StarRocks version that refuses ``partition_ttl_number`` for an MV whose partitions are not RANGE ones."""

_ASYNC_MV_ONLY_PROPERTIES: frozenset = frozenset({'excluded_refresh_tables', 'excluded_trigger_tables'})
"""MV properties CREATE MATERIALIZED VIEW refuses for an MV whose refresh is not ASYNC, on StarRocks 3.3.22 and 4.1.6."""

_BASE_TABLE_LIST_PROPERTIES: frozenset = frozenset({'excluded_refresh_tables', 'excluded_trigger_tables'})
"""MV properties that list tables, each of which CREATE MATERIALIZED VIEW refuses unless the MV reads it."""

_UNREAD_TABLES = "a definition that does not read the tables they name"
"""Why CREATE MATERIALIZED VIEW refuses a table of ``_BASE_TABLE_LIST_PROPERTIES``."""

_COLUMN_CONDITION_PROPERTIES: frozenset = frozenset({'partition_retention_condition'})
"""MV properties holding a condition on the MV's columns, which CREATE MATERIALIZED VIEW refuses for an MV without
them ("Column 'dt' cannot be resolved", on StarRocks 4.1.6; 3.3 has no such property for an MV)."""

_COLUMNS_THE_CONDITION_NAMES_GONE = "columns that leave out one its condition names"
"""Why a condition of ``_COLUMN_CONDITION_PROPERTIES`` is lost: the new MV lacks a column it names."""

_COLUMNS_THE_CONDITION_NAMES_UNTOLD = "columns the dialect cannot match against its condition"
"""Why a condition of ``_COLUMN_CONDITION_PROPERTIES`` is lost: the condition or the new MV's columns cannot be read."""

_MANUAL_MV_REFUSED_PROPERTIES: frozenset = _ASYNC_MV_ONLY_PROPERTIES | {'auto_refresh_partitions_limit'}
"""MV properties CREATE MATERIALIZED VIEW refuses for an MV refreshed MANUAL, on StarRocks 3.3.22 and 4.1.6."""

_RANGE_PARTITION_FUNCTIONS: frozenset = frozenset({'date_trunc', 'time_slice'})
"""The functions of an expression partitioning that StarRocks makes RANGE partitions for; others make LIST ones."""


class _MaterializedViewFacts(NamedTuple):
    """What CREATE MATERIALIZED VIEW refuses properties by, beyond what the create operation itself says.

    ``partitions_are_range`` is None when the dialect cannot tell what partitions the new MV gets.
    """

    partitions_are_range: Optional[bool]
    server_version: Optional[Tuple[int, ...]]
    default_schema: Optional[str]


def _is_partitioned(create_op: CreateMaterializedViewOp) -> bool:
    return bool(create_op.kwargs.get(TableInfoKeyWithPrefix.PARTITION_BY))


def _refresh_kind(create_op: CreateMaterializedViewOp) -> str:
    """Return ASYNC, MANUAL or INCREMENTAL: how the MV refreshes. StarRocks makes one that states no refresh MANUAL."""
    scheme = _normalize_refresh_scheme(create_op.kwargs.get(TableInfoKeyWithPrefix.REFRESH)) or "MANUAL"
    return scheme.split("(")[0].split()[0]


def _partitions_of_the_new_mv_are_range(
    create_op: CreateMaterializedViewOp, autogen_context: AutogenContext
) -> Optional[bool]:
    """Whether the partitions StarRocks gives ``create_op``'s MV are RANGE ones, or None when that cannot be told.

    StarRocks gives a partitioned MV the kind of partitions of the base table its PARTITION BY refers to, and LIST
    partitions for a PARTITION BY of more than one column. The partitioned tables the definition reads are looked
    up in the model, then in the database; when they do not all have the same kind, which of them the PARTITION BY
    refers to is not told.
    """
    partition_by = create_op.kwargs.get(TableInfoKeyWithPrefix.PARTITION_BY)
    if not partition_by:
        return None
    if len(_top_level_items(TableAttributeNormalizer.remove_outer_parentheses(str(partition_by)))) > 1:
        return False
    default_schema = autogen_context.dialect.default_schema_name
    kinds = {
        _partition_kind_of_table(autogen_context, reference_schema or create_op.schema, name, default_schema)
        for reference_schema, name in tables_read_by_query(str(create_op.definition or ""))
    }
    kinds.discard("")
    if len(kinds) != 1 or None in kinds:
        return None
    return kinds == {PartitionType.RANGE}


def _top_level_items(text: str) -> List[str]:
    """Split ``text`` at the commas outside parentheses and string literals."""
    items, depth, quote, start = [], 0, None, 0
    for index, character in enumerate(text):
        if quote:
            quote = None if character == quote else quote
        elif character in "'\"`":
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif character == "," and depth == 0:
            items.append(text[start:index])
            start = index + 1
    items.append(text[start:])
    return [item.strip() for item in items if item.strip()]


def _partition_kind_of_table(
    autogen_context: AutogenContext, schema: Optional[str], name: str, default_schema: Optional[str]
) -> Optional[str]:
    """Return RANGE or LIST for a partitioned table, "" for an unpartitioned one, or None when it is not known.

    The model's table counts first, as the revision makes the database's like it; a view, a materialized view or a
    table neither has is not known.
    """
    key = _object_key(schema, name, default_schema)
    for table in autogen_context.metadata.tables.values():
        if _object_key(table.schema, table.name, default_schema) != key:
            continue
        kind = table.info.get(TableObjectInfoKey.TABLE_KIND)
        if kind and kind.upper() != TableKind.TABLE:
            return None
        return _partition_kind(table.kwargs.get(TableInfoKeyWithPrefix.PARTITION_BY))
    inspector = autogen_context.inspector
    try:
        views = {*inspector.get_view_names(schema=schema), *inspector.get_materialized_view_names(schema=schema)}
        if name in {view.lower() for view in views}:
            return None
        options = inspector.get_table_options(name, schema=schema)
    except Exception as error:
        logger.debug("Could not read the partitioning of %r: %s", utils.gen_simple_qualified_name(name, schema), error)
        return None
    return _partition_kind(options.get(TableInfoKeyWithPrefix.PARTITION_BY))


def _partition_kind(partition_by: Any) -> Optional[str]:
    """Return RANGE or LIST for a table's PARTITION BY, "" when it has none, or None when it cannot be read."""
    if not partition_by:
        return ""
    try:
        partition = (
            partition_by if isinstance(partition_by, ReflectedPartitionInfo)
            else StarRocksTableDefinitionParser.parse_partition_clause(str(partition_by))
        )
    except Exception:
        return None
    if partition is None:
        return ""
    if partition.type in (PartitionType.RANGE, PartitionType.LIST):
        return partition.type
    function = re.match(r"\s*`?(\w+)`?\s*\(", partition.partition_method or "")
    return PartitionType.RANGE if function and function.group(1).lower() in _RANGE_PARTITION_FUNCTIONS else (
        PartitionType.LIST
    )


def _properties_refused_for(create_op: CreateMaterializedViewOp, facts: _MaterializedViewFacts) -> Dict[str, frozenset]:
    """Return the lowercase MV properties CREATE MATERIALIZED VIEW would refuse for ``create_op``'s MV, by reason."""
    refused: Dict[str, frozenset] = {}
    if not _is_partitioned(create_op):
        refused["no PARTITION BY"] = _PARTITIONED_MV_ONLY_PROPERTIES
    elif not facts.partitions_are_range and _server_reached(facts.server_version, _RANGE_PARTITIONED_MV_ONLY_FROM):
        reason = "LIST partitions" if facts.partitions_are_range is False else "partitions not known to be RANGE ones"
        refused[reason] = _RANGE_PARTITIONED_MV_ONLY_PROPERTIES
    refresh_kind = _refresh_kind(create_op)
    if refresh_kind == "MANUAL":
        refused["a MANUAL refresh"] = _MANUAL_MV_REFUSED_PROPERTIES
    elif refresh_kind != "ASYNC":
        refused[f"an {refresh_kind} refresh"] = _ASYNC_MV_ONLY_PROPERTIES
    return refused


def _server_reached(server_version: Optional[Tuple[int, ...]], first_version: Tuple[int, ...]) -> bool:
    """Whether the server is ``first_version`` or later; a server whose version is not known counts as later."""
    return not server_version or tuple(server_version) >= first_version


def _tables_the_new_mv_reads(
    value: str, create_op: CreateMaterializedViewOp, default_schema: Optional[str]
) -> Tuple[List[str], List[str]]:
    """Split a property's comma-separated tables into those ``create_op``'s definition reads and the others.

    StarRocks matches a name without a database against the MV's base tables by name alone, and ``db.table``
    by both, as ``PropertyAnalyzer.analyzeExcludedTables`` does.
    """
    read = _keys_read_by(create_op.definition, create_op.schema, default_schema)
    read_names = {name for _, name in read}
    kept, dropped = [], []
    for entry in (part.strip() for part in str(value).split(",")):
        if not entry:
            continue
        parts = entry.split(".")
        if len(parts) == 1:
            is_read = parts[0].lower() in read_names
        else:
            is_read = _object_key(parts[-2], parts[-1], default_schema) in read
        (kept if is_read else dropped).append(entry)
    return kept, dropped


def _keep_what_the_model_leaves_out(
    create_op: CreateMaterializedViewOp,
    database_op: CreateMaterializedViewOp,
    run_mode: str,
    facts: _MaterializedViewFacts,
) -> Tuple[List[str], Dict[str, str], List[str]]:
    """Give a recreated materialized view the database's refresh and the properties the model leaves out.

    A model that states no refresh leaves the refresh alone, so the new MV keeps the database's rather than
    becoming MANUAL, or, with no distribution either, a synchronous MV the dialect refuses after the DROP has run.
    A property the model leaves out keeps the database's value, as an ALTER would leave it, unless that value is
    the default of :meth:`ReflectionMVDefaults.properties`, which the new MV gets anyway, or CREATE MATERIALIZED VIEW
    would refuse it for the new MV after the DROP has run:

    - one of ``_PARTITIONED_MV_ONLY_PROPERTIES`` for a new MV with no PARTITION BY;
    - one of ``_RANGE_PARTITIONED_MV_ONLY_PROPERTIES`` for one whose partitions are not known to be RANGE ones;
    - one of ``_MANUAL_MV_REFUSED_PROPERTIES`` for one refreshed MANUAL, and of ``_ASYNC_MV_ONLY_PROPERTIES`` for
      another that is not refreshed ASYNC;
    - of ``_BASE_TABLE_LIST_PROPERTIES``, each table the new definition does not read;
    - one of ``_COLUMN_CONDITION_PROPERTIES`` whose condition names a column the new MV may not have.

    Returns what was kept, the properties or tables that could not be, and why.
    """
    kept: List[str] = []
    refresh = database_op.kwargs.get(TableInfoKeyWithPrefix.REFRESH)
    if not create_op.kwargs.get(TableInfoKeyWithPrefix.REFRESH) and refresh:
        create_op.kwargs[TableInfoKeyWithPrefix.REFRESH] = refresh
        kept.append(f"refresh {str(refresh)!r}")
    model_properties = dict(create_op.kwargs.get(TableInfoKeyWithPrefix.PROPERTIES) or {})
    stated = {key.lower() for key in model_properties}
    defaults = ReflectionMVDefaults.properties(run_mode)
    refused_by_reason = _properties_refused_for(create_op, facts)
    refused = set().union(*refused_by_reason.values())
    carried: Dict[str, str] = {}
    lost: Dict[str, str] = {}
    lost_by: Set[str] = set()
    for key, value in (database_op.kwargs.get(TableInfoKeyWithPrefix.PROPERTIES) or {}).items():
        lowered = key.lower()
        if lowered in stated or _is_mv_default_property_value(lowered, value, defaults):
            continue
        if lowered in refused:
            lost[key] = value
            lost_by.update(reason for reason, keys in refused_by_reason.items() if lowered in keys)
        elif lowered in _BASE_TABLE_LIST_PROPERTIES:
            read_tables, unread_tables = _tables_the_new_mv_reads(value, create_op, facts.default_schema)
            if read_tables:
                carried[key] = ",".join(read_tables)
            if unread_tables:
                lost[key] = ",".join(unread_tables)
                lost_by.add(_UNREAD_TABLES)
        elif lowered in _COLUMN_CONDITION_PROPERTIES and (
            missing := _why_the_new_mv_lacks_columns_of(value, create_op)
        ):
            lost[key] = value
            lost_by.add(missing)
        else:
            carried[key] = value
    if carried:
        create_op.kwargs[TableInfoKeyWithPrefix.PROPERTIES] = {**model_properties, **carried}
        kept.append(f"properties {carried!r}")
    reasons = [
        *refused_by_reason, _UNREAD_TABLES, _COLUMNS_THE_CONDITION_NAMES_GONE, _COLUMNS_THE_CONDITION_NAMES_UNTOLD
    ]
    return kept, lost, [reason for reason in reasons if reason in lost_by]


def _why_the_new_mv_lacks_columns_of(condition: str, create_op: CreateMaterializedViewOp) -> Optional[str]:
    """Return why ``create_op``'s MV cannot take ``condition``, or None when it has every column the condition names.

    The new MV's columns are the model's column list, or else the outputs the definition reads as, and the columns
    its PARTITION BY names, which StarRocks requires the MV to have.
    """
    named = sql_canonical.column_names_in_expressions(condition)
    if named is None:
        return _COLUMNS_THE_CONDITION_NAMES_UNTOLD
    partition_by = create_op.kwargs.get(TableInfoKeyWithPrefix.PARTITION_BY)
    partition_columns = sql_canonical.column_names_in_expressions(
        TableAttributeNormalizer.remove_outer_parentheses(str(partition_by)) if partition_by else None
    ) or set()
    if create_op.columns:
        outputs: Optional[List[str]] = [column["name"] for column in create_op.columns]
    else:
        outputs = sql_canonical.output_column_names(str(create_op.definition or ""))
    columns = partition_columns | {name.lower() for name in outputs or []}
    if named <= columns:
        return None
    return _COLUMNS_THE_CONDITION_NAMES_UNTOLD if outputs is None else _COLUMNS_THE_CONDITION_NAMES_GONE


def _is_mv_default_property_value(key: str, value: Any, defaults: Dict[str, str]) -> bool:
    """Whether ``value`` is the default StarRocks gives MV property ``key`` (lowercase), in any case."""
    default = defaults.get(key)
    return default is not None and str(value).strip().lower() == str(default).strip().lower()


@comparators_dispatch_for_starrocks("table")
def check_table_kind_for_view_mv(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schema: Optional[str],
    table_name: str,
    conn_table: Optional[Table],
    metadata_table: Optional[Table],
) -> None:
    """It's not a real comparison of table, it's a checker to check whether users have
    properly set the `include_object_for_view_mv` when there are views or MVs.
    """
    # Check if we're comparing View/MaterializedView objects
    # This comparator is only for TABLE objects
    conn_table_kind = (
        conn_table.info.get(TableObjectInfoKey.TABLE_KIND, TableKind.TABLE)
        if conn_table is not None
        else TableKind.TABLE
    )
    meta_table_kind = (
        metadata_table.info.get(TableObjectInfoKey.TABLE_KIND, TableKind.TABLE)
        if metadata_table is not None
        else TableKind.TABLE
    )
    # logger.debug("Check table kind for table comparison: conn_table_kind=%s, meta_table_kind=%s", conn_table_kind, meta_table_kind)

    if conn_table_kind != TableKind.TABLE or meta_table_kind != TableKind.TABLE:
        error_msg = (
            f"You need to properly set the `include_object_for_view_mv` callback when there are views or MVs.\n"
            f"Please configure your env.py:\n\n"
            f">>>>\n"
            f"    from starrocks.alembic import include_object_for_view_mv\n"
            f"    context.configure(\n"
            f"        ...,\n"
            f"        include_object=include_object_for_view_mv,\n"
            f"    )\n\n"
            f"Or if you have a custom include_object:\n\n"
            f"    from starrocks.alembic import combine_include_object\n"
            f"    context.configure(\n"
            f"        ...,\n"
            f"        include_object=combine_include_object(my_custom_filter),\n"
            f"    )\n"
            f"<<<<\n"
        )
        raise ValueError(error_msg)


# ==============================================================================
# Order of view and materialized view operations
# ==============================================================================
_ObjectKey = Tuple[Optional[str], str]
_VIEW_AND_MV_OPS: Tuple[type, ...] = (CreateViewOp, DropViewOp, AlterViewOp, AlterMaterializedViewOp)


@comparators_dispatch_for_starrocks("schema", run_last=True)
def order_view_and_mv_ops(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schemas: Union[Set[None], Set[Optional[str]]],
) -> None:
    """Place view and materialized view operations around the table operations, so both directions of a revision run.

    Alembic writes the downgrade as the upgrade reversed. So the upgrade drops views and materialized views first,
    each before what it reads, then runs the table operations, then creates and alters views and materialized
    views, each after what it reads; the downgrade then creates the dropped ones after the tables they read are
    back, and drops the created ones before those tables go. An ALTER VIEW whose old definition reads what the
    revision removes could not be undone before that is restored, so it becomes a drop before the table
    operations and a create after them, with a warning that StarRocks drops the view's grants with it: see
    :func:`_view_alters_to_split`. Names break ties, so the order is the same on every run. This comparator is
    registered after the view and materialized view ones, so it runs last.
    """
    view_ops = [op for op in upgrade_ops.ops if isinstance(op, _VIEW_AND_MV_OPS)]
    if not view_ops:
        return
    default_schema = autogen_context.dialect.default_schema_name
    table_ops = [op for op in upgrade_ops.ops if not isinstance(op, _VIEW_AND_MV_OPS)]
    alters_to_split = _view_alters_to_split(
        view_ops,
        _tables_the_revision_loses(table_ops, default_schema),
        _columns_the_revision_drops(table_ops, default_schema),
        default_schema,
    )
    _warn_of_the_grants_split_views_lose(alters_to_split)
    drops: List[MigrateOperation] = []
    creates: List[MigrateOperation] = []
    for op in view_ops:
        if isinstance(op, DropViewOp):
            drops.append(op)
        elif op in alters_to_split:
            drops.append(_drop_half_of_view_alter(op))
            creates.append(_create_half_of_view_alter(op))
        else:
            creates.append(op)
    upgrade_ops.ops[:] = (
        _in_dependency_order(drops, default_schema, readers_first=True)
        + table_ops
        + _in_dependency_order(creates, default_schema, readers_first=False)
    )


def _object_key(schema: Optional[str], name: str, default_schema: Optional[str]) -> _ObjectKey:
    """Return the lower-cased ``(schema, name)`` of an object, with None for the default schema."""
    normalized_schema = schema.lower() if schema else None
    if default_schema and normalized_schema == default_schema.lower():
        normalized_schema = None
    return normalized_schema, name.lower()


def _tables_the_revision_loses(table_ops: List[MigrateOperation], default_schema: Optional[str]) -> Set[_ObjectKey]:
    """Return the tables the operations drop or rename."""
    return {
        _object_key(op.schema, op.table_name, default_schema)
        for op in table_ops
        if isinstance(op, (DropTableOp, RenameTableOp))
    }


def _warn_of_the_grants_split_views_lose(alters_to_split: List[AlterViewOp]) -> None:
    """Warn that each view whose alter is split loses its grants, which ALTER VIEW would have kept."""
    if alters_to_split:
        names = ", ".join(repr(utils.gen_simple_qualified_name(op.view_name, op.schema)) for op in alters_to_split)
        logger.warning(
            "%s what the revision removes, so the migration drops %s before the table operations and creates %s "
            "again after them, in both directions. StarRocks drops a view's grants with the view, where ALTER VIEW "
            "keeps them, so grant them again after the upgrade and after the downgrade.",
            f"The view {names} reads" if len(alters_to_split) == 1 else f"The views {names} read",
            "it" if len(alters_to_split) == 1 else "each one",
            "it",
        )


def _objects_the_revision_creates(operations: List[MigrateOperation], default_schema: Optional[str]) -> Set[_ObjectKey]:
    """Return the tables, views and materialized views the operations create.

    Autogenerate compares tables before views and materialized views, and adds new materialized views before it
    compares the existing ones, so when an existing materialized view is compared the creates are already there.
    """
    keys: Set[_ObjectKey] = set()
    for op in operations:
        if isinstance(op, CreateTableOp):
            keys.add(_object_key(op.schema, op.table_name, default_schema))
        elif isinstance(op, CreateViewOp):
            keys.add(_object_key(op.schema, op.view_name, default_schema))
    return keys


def _objects_the_revision_drops(operations: List[MigrateOperation], default_schema: Optional[str]) -> Set[_ObjectKey]:
    """Return the tables, views and materialized views the operations drop and do not create again.

    Autogenerate adds the drops of removed views before it compares the existing views, and likewise for
    materialized views, so when an existing materialized view is compared they are already there. A materialized
    view recreated for a change is not dropped for good.
    """
    keys: Set[_ObjectKey] = set()
    for op in operations:
        if isinstance(op, DropTableOp):
            keys.add(_object_key(op.schema, op.table_name, default_schema))
        elif isinstance(op, DropViewOp):
            keys.add(_object_key(op.schema, op.view_name, default_schema))
    return keys - _objects_the_revision_creates(operations, default_schema)


def _columns_the_revision_drops(
    operations: List[MigrateOperation], default_schema: Optional[str]
) -> Dict[_ObjectKey, Set[str]]:
    """Return the lower-cased names of the columns the operations drop or rename, by the key of their table."""
    columns: Dict[_ObjectKey, Set[str]] = {}
    for op in operations:
        if not isinstance(op, ModifyTableOps):
            continue
        for column_op in op.ops:
            if isinstance(column_op, DropColumnOp) or (
                isinstance(column_op, AlterColumnOp) and column_op.modify_name is not None
            ):
                columns.setdefault(_object_key(op.schema, op.table_name, default_schema), set()).add(
                    column_op.column_name.lower()
                )
    return columns


def _definition_reads_a_column_of(
    definition: Optional[str],
    schema: Optional[str],
    columns_by_table: Dict[_ObjectKey, Set[str]],
    default_schema: Optional[str],
) -> bool:
    """Whether a definition reads one of ``columns_by_table``'s columns of a table it reads.

    It does when it reads the table and names the column anywhere, or holds a ``*``, which may read every column.
    The name may be another table's column or an alias; that only counts the definition as reading too much.
    """
    if not definition or not columns_by_table:
        return False
    read = _keys_read_by(definition, schema, default_schema)
    names = names_in_query(str(definition))
    return any(
        key in read and (EVERY_COLUMN in names or not names.isdisjoint(columns))
        for key, columns in columns_by_table.items()
    )


def _view_alters_to_split(
    view_ops: List[MigrateOperation],
    tables_lost: Set[_ObjectKey],
    columns_lost: Dict[_ObjectKey, Set[str]],
    default_schema: Optional[str],
) -> List[AlterViewOp]:
    """Return the ALTER VIEWs whose old definition reads what the revision removes.

    That is a table of ``tables_lost``, a column of ``columns_lost``, a view or materialized view the revision
    drops, including the drop half of a materialized view it recreates, or a view whose own alter is split. For an
    alter that redefines its view, it is also a column another view the revision redefines loses: see
    :func:`_columns_a_redefined_view_loses`. The downgrade could not alter such a view back before what it reads is
    restored, so each one is split into a drop and a create. Reading a column is as
    :func:`_definition_reads_a_column_of` tells it.
    """
    lost = set(tables_lost) | {
        _object_key(op.schema, op.view_name, default_schema) for op in view_ops if isinstance(op, DropViewOp)
    }
    redefined_losses = {
        _object_key(op.schema, op.view_name, default_schema): _columns_a_redefined_view_loses(op)
        for op in view_ops
        if isinstance(op, AlterViewOp) and op.definition is not None
    }
    pending = [op for op in view_ops if isinstance(op, AlterViewOp)]
    split: List[AlterViewOp] = []
    while newly_split := [
        op for op in pending
        if _old_definition_reads_what_changes(op, lost, columns_lost, redefined_losses, default_schema)
    ]:
        split.extend(newly_split)
        pending = [op for op in pending if op not in newly_split]
        lost.update(_object_key(op.schema, op.view_name, default_schema) for op in newly_split)
    return split


def _columns_a_redefined_view_loses(op: AlterViewOp) -> Optional[Set[str]]:
    """Return the lower-cased columns an alter takes from its view, or None when they cannot be told.

    The view's columns in the database are reflected. Its new ones are the model's column list, or else the
    outputs :func:`sql_canonical.output_column_names` reads from the new definition.
    """
    old_columns = {column["name"].lower() for column in op.existing_columns or []}
    if op.columns:
        new_columns: Optional[List[str]] = [column["name"] for column in op.columns]
    else:
        new_columns = sql_canonical.output_column_names(str(op.definition))
    if not old_columns or new_columns is None:
        return None
    return old_columns - {name.lower() for name in new_columns}


def _old_definition_reads_what_changes(
    op: AlterViewOp,
    lost: Set[_ObjectKey],
    columns_lost: Dict[_ObjectKey, Set[str]],
    redefined_losses: Dict[_ObjectKey, Optional[Set[str]]],
    default_schema: Optional[str],
) -> bool:
    """Whether the downgrade could not alter the view back to its old definition in place.

    It could not while something that definition reads is lost, an object or a column of one. When the alter
    redefines the view, it could not while a view that definition reads is redefined to lose a column it reads, or
    columns that cannot be told: the upgrade may alter that view first, as it must when the new definition reads
    it, and the reversed downgrade would then restore this view's old definition before that view's. An alter that
    keeps the definition, as for a comment, restores the same definition, which reads either version of that view.
    """
    definition = op.existing_definition
    if _definition_reads_any(definition, op.schema, lost, default_schema):
        return True
    if _definition_reads_a_column_of(definition, op.schema, columns_lost, default_schema):
        return True
    if op.definition is None:
        return False
    losing_untold = {key for key, columns in redefined_losses.items() if columns is None}
    losing_columns = {key: columns for key, columns in redefined_losses.items() if columns}
    return _definition_reads_any(definition, op.schema, losing_untold, default_schema) or (
        _definition_reads_a_column_of(definition, op.schema, losing_columns, default_schema)
    )


def _keys_read_by(definition: Optional[str], schema: Optional[str], default_schema: Optional[str]) -> Set[_ObjectKey]:
    """Return every object key a definition's table references could name.

    An unqualified name may be in the view's own schema or in the default one.
    """
    if not definition:
        return set()
    keys: Set[_ObjectKey] = set()
    for reference_schema, name in tables_read_by_query(str(definition)):
        if reference_schema is not None:
            keys.add(_object_key(reference_schema, name, default_schema))
        else:
            keys.add(_object_key(schema, name, default_schema))
            keys.add((None, name))
    return keys


def _definition_reads_any(
    definition: Optional[str], schema: Optional[str], keys: Set[_ObjectKey], default_schema: Optional[str]
) -> bool:
    return not _keys_read_by(definition, schema, default_schema).isdisjoint(keys)


def _definition_of(op: MigrateOperation) -> Optional[str]:
    """Return the definition whose reads order the operation: the new one, or the database's for a drop."""
    if isinstance(op, CreateViewOp):
        return op.definition
    if isinstance(op, AlterViewOp):
        return op.definition or op.existing_definition
    if isinstance(op, DropViewOp):
        return op.existing_definition
    return None


def _in_dependency_order(
    ops: List[MigrateOperation], default_schema: Optional[str], readers_first: bool
) -> List[MigrateOperation]:
    """Return the view and materialized view operations so each comes after what it reads, or before it.

    Among operations whose order is free, the one named first by ``(schema, name)`` comes first. A cycle, which
    StarRocks would refuse anyway, keeps the remaining operations in name order.
    """
    pending = sorted(ops, key=lambda op: (op.schema or "", op.view_name))
    keys = [_object_key(op.schema, op.view_name, default_schema) for op in pending]
    must_follow = {
        index: {
            other for other, key in enumerate(keys)
            if other != index and key in _keys_read_by(_definition_of(op), op.schema, default_schema)
        }
        for index, op in enumerate(pending)
    }
    if readers_first:
        readers = {index: {other for other in must_follow if index in must_follow[other]} for index in must_follow}
        must_follow = readers
    ordered: List[int] = []
    while len(ordered) < len(pending):
        ready = [index for index in range(len(pending)) if index not in ordered and must_follow[index] <= set(ordered)]
        if not ready:
            remaining = [index for index in range(len(pending)) if index not in ordered]
            logger.warning(
                "The views and materialized views %s read each other in a cycle; they are kept in name order.",
                [pending[index].view_name for index in remaining],
            )
            ordered.extend(remaining)
            break
        ordered.append(ready[0])
    return [pending[index] for index in ordered]


def _drop_half_of_view_alter(op: AlterViewOp) -> DropViewOp:
    """Return the drop of the view as the database has it, so the downgrade creates it again after its tables."""
    return DropViewOp(
        op.view_name,
        schema=op.schema,
        existing_definition=op.existing_definition,
        existing_columns=op.existing_columns,
        existing_comment=op.existing_comment,
        starrocks_security=op.existing_security,
    )


def _create_half_of_view_alter(op: AlterViewOp) -> CreateViewOp:
    """Return the create of the view as the model has it, with what the alter leaves unchanged from the database."""
    return CreateViewOp(
        op.view_name,
        op.definition or op.existing_definition,
        schema=op.schema,
        columns=op.columns if op.definition else op.existing_columns,
        comment=op.comment,
        starrocks_security=op.security if op.security is not None else op.existing_security,
    )


# ==============================================================================
# Table Comparison
# Only starrocks-specific table attributes are compared.
# Other table attributes are compared using generic comparison logic in Alembic.
# ==============================================================================
def _skip_unalterable_change(compare_attribute: Callable[[], None]) -> None:
    """Run one table attribute comparison, turning a change StarRocks cannot ALTER into a warning.

    Autogenerate then still writes the operations for the table's other attributes and for every other table,
    instead of aborting the whole run; the warning says what has to be changed by hand.
    """
    try:
        compare_attribute()
    except UnalterableChange as change:
        logger.warning("%s No operation is generated for it; apply this change by hand.", change)


@comparators_dispatch_for_starrocks("table")
def compare_starrocks_table(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schema: Optional[str],
    table_name: str,
    conn_table: Optional[Table],
    metadata_table: Optional[Table],
) -> None:
    """
    Compare StarRocks-specific table attributes and generate operations.

    Other table attributes are compared using generic comparison logic in Alembic.
    For some starrocks-specific attributes of columns, see compare_starrocks_column.

    Args:
        autogen_context: AutogenContext
        conn_table: Table object in the database, already reflected from the database
        metadata_table: Table object in the metadata

    A change StarRocks cannot ALTER, such as a new key type or partitioning, is logged as a warning and skipped.

    Raises:
        ValueError: If comparing View/MaterializedView objects (should use include_object_for_view_mv).
    """
    # Handle table creation and deletion scenarios
    if conn_table is None:
        # Table exists in metadata but not in DB; this is a CREATE TABLE.
        # Alembic handles CreateTableOp separately. Our comparator should do nothing.
        logger.debug("compare_starrocks_table: conn_table is None for %r, skipping.", metadata_table.name)
        return
    if metadata_table is None:
        # Table exists in DB but not in metadata; this is a DROP TABLE.
        # Alembic handles DropTableOp separately. Our comparator should do nothing.
        logger.debug("compare_starrocks_table: metadata_table is None for %r, skipping.", conn_table.name)
        return

    logger.debug("Compare StarRocks table: conn_table: %r, metadata_table: %r", conn_table, metadata_table)
    # Get the system run_mode for proper default value comparison
    run_mode = autogen_context.dialect.run_mode
    # logger.debug("compare starrocks table. table: %s, schema:%s, run_mode: %s", table_name, schema, run_mode)

    # Extract dialect_options for comparison using case-insensitive helper
    conn_table_attributes = extract_dialect_options_as_case_insensitive(conn_table)
    meta_table_attributes = extract_dialect_options_as_case_insensitive(metadata_table)
    _add_inferred_primary_key_against_primary_key_table(
        metadata_table, conn_table_attributes, meta_table_attributes, autogen_context.dialect
    )

    logger.debug(
        "StarRocks-specific attributes comparison for table %r: "
        "Detected in database: %s. Found in metadata: %s.",
        conn_table.name,
        conn_table_attributes,
        meta_table_attributes,
    )

    if metadata_table is not None and metadata_table.comment is None:
        # Handle backward compatibility for 'starrocks_comment'.
        if starrocks_comment := meta_table_attributes.get(TableInfoKey.COMMENT):
            import warnings
            warnings.warn(
                f"The 'starrocks_comment' dialect argument is deprecated for table '{table_name}'. "
                "Please use the standard 'comment' argument on the Table object instead.",
                DeprecationWarning,
            )
            metadata_table.comment = starrocks_comment

    # Note: Table comment comparison is handled by Alembic's built-in _compare_table_comment

    # Compare each type of table attribute using dedicated functions
    # Order follows StarRocks CREATE TABLE grammar:
    #   engine -> key -> comment -> partition -> distribution -> order by -> properties
    table, schema = conn_table.name, conn_table.schema

    # Track the number of operations before comparison
    ops_before = len(upgrade_ops.ops)

    attributes = (upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _skip_unalterable_change(lambda: _compare_table_engine(*attributes))
    _skip_unalterable_change(lambda: _compare_table_key(*attributes))
    # Note: COMMENT comparison is handled by Alembic's built-in _compare_table_comment
    _skip_unalterable_change(lambda: _compare_table_partition(*attributes))
    _skip_unalterable_change(lambda: _compare_table_distribution(*attributes))
    _skip_unalterable_change(lambda: _compare_table_order_by(*attributes))
    server_version = autogen_context.dialect.server_version_info
    _skip_unalterable_change(lambda: _compare_table_properties(*attributes, run_mode, server_version))

    # Log summary if any operations were generated
    ops_after = len(upgrade_ops.ops)
    if ops_after > ops_before:
        table_fqn = utils.gen_simple_qualified_name(table_name, schema)
        num_changes = ops_after - ops_before
        logger.debug("Table %r comparison complete: %d ALTER operation(s) generated", table_fqn, num_changes)

    return False


def _add_inferred_primary_key_against_primary_key_table(
    metadata_table: Table,
    conn_table_attributes: Dict[str, Any],
    meta_table_attributes: Dict[str, Any],
    dialect: Dialect,
) -> None:
    """Compare the PRIMARY KEY a model only implies, with ``primary_key=True`` columns, against a PRIMARY KEY table.

    With ``starrocks_infer_primary_key`` on, CREATE TABLE makes such a model a PRIMARY KEY table where StarRocks can
    hold one (see :func:`utils.inferred_primary_key_columns`), so against a PRIMARY KEY table its key is compared
    as if the model named it. Against any other key model, such as the DUPLICATE KEY table earlier versions of the
    dialect created from the same model, the model names no key, and the key is left as the database has it, as
    for other attributes the model omits. With the option off, CREATE TABLE sends no key clause for the model, so
    the model names no key against any table, a PRIMARY KEY one included, and the key is left alone.
    """
    conn_key = _get_table_key_type(conn_table_attributes)
    if not isinstance(metadata_table, Table) or conn_key is None or conn_key.type != TableType.PRIMARY_KEY:
        return
    if inferred_key := utils.inferred_primary_key_columns(metadata_table, dialect):
        meta_table_attributes[TableInfoKey.PRIMARY_KEY] = ", ".join(inferred_key)


def _compare_table_engine(
    ops_list: List[AlterTableOp],
    schema: Optional[str],
    table_name: str,
    conn_table_attributes: Dict[str, Any],
    meta_table_attributes: Dict[str, Any]
) -> None:
    """Compare engine changes and add AlterTableEngineOp if needed.

    Note: StarRocks does not support ALTER TABLE ENGINE, so this will raise an error
    if a change is detected.
    """
    meta_engine = meta_table_attributes.get(TableInfoKey.ENGINE)
    conn_engine = conn_table_attributes.get(TableInfoKey.ENGINE)
    logger.debug("Compares table ENGINE. meta_engine: %s, conn_engine: %s", meta_engine, conn_engine)

    # if not meta_engine:
    #     logger.error(f"Engine info should be specified in metadata to change for table {table_name} in schema {schema}.")
    #     return

    normalized_meta: Optional[str] = TableAttributeNormalizer.normalize_engine(meta_engine)
    # Reflected table must have a default ENGINE, so we need to normalize it
    normalized_conn: Optional[str] = ReflectionTableDefaults.normalize_engine(conn_engine)

    _compare_single_table_attribute(
        table_name,
        schema,
        TableInfoKey.ENGINE,
        normalized_conn,
        normalized_meta,
        default_value=ReflectionTableDefaults.engine(),
        support_change=AlterTableEnablement.ENGINE
    )


def _compare_table_key(
    ops_list: List[AlterTableOp], schema: Optional[str], table_name: str,
    conn_table_attributes: Dict[str, Any], meta_table_attributes: Dict[str, Any]
) -> None:
    """Compare key changes and add AlterTableKeyOp if needed.

    Note: StarRocks does not support ALTER TABLE KEY, so this will raise an error
    if a change is detected.
    But, if only the key columns are changed, we can generate an WARNING, but not .
    """
    conn_key: Optional[ReflectedTableKeyInfo] = _get_table_key_type(conn_table_attributes)
    meta_key: Optional[ReflectedTableKeyInfo] = _get_table_key_type(meta_table_attributes)
    # logger.debug("Compares table KEY. conn_key: %s, meta_key: %s", conn_key, meta_key)

    if isinstance(conn_key, str):
        conn_key = StarRocksTableDefinitionParser.parse_key_clause(conn_key)
    if isinstance(meta_key, str):
        meta_key = StarRocksTableDefinitionParser.parse_key_clause(meta_key)

    # Reflected table must have a default KEY, so we need to normalize it
    # Actually, the conn key must not be None, because it is inspected from database.
    normalized_conn: Optional[str] = TableAttributeNormalizer.normalize_key(conn_key)
    normalized_meta: Optional[str] = TableAttributeNormalizer.normalize_key(meta_key)
    logger.debug("Compares table KEY. normalized_conn: %r, normalized_meta: %r", normalized_conn, normalized_meta)

    if _compare_single_table_attribute(
        table_name,
        schema,
        TableInfoKey.KEY,
        normalized_conn,
        normalized_meta,
        default_value=ReflectionTableDefaults.key(),
        equal_to_default_cmp_func=_is_equal_key_with_defaults,
        support_change=AlterTableEnablement.KEY
    ):
        if conn_key is None:
            conn_key = ReflectionTableDefaults.reflected_key_info()
        if meta_key is None:
            meta_key = ReflectionTableDefaults.reflected_key_info()
        if conn_key.type != meta_key.type:
            raise UnalterableChange(
                f"Table '{table_name}' has different key types: {conn_key.type} to {meta_key.type}, "
                "but it's not supported to change the key type."
            )
        else:
            logger.warning(f"Table '{table_name}' has different key columns: ({conn_key.columns}) to ({meta_key.columns}), "
                           f"with the same table type: {conn_key.type}. "
                           f"But it's not explicitly supported to change the key columns.")

def _get_table_key_type(table_attributes: Dict[str, Any]) -> Optional[ReflectedTableKeyInfo]:
    """Get table key type. like 'PRIMARY KEY (id, name)'
    The key in table_attributes is like 'PRIMARY_KEY' without prefix 'starrocks_',
    and the value is like 'id, name'.

    Args:
        table_attributes: All table attributes without prefix 'starrocks_'.

    Returns:
        The table key type. like ReflectedTableKeyInfo('PRIMARY KEY', 'id, name').
        None if the table key type is not found.
    """
    for key_type in TableInfoKey.KEY_KWARG_MAP:
        key_columns: str = table_attributes.get(key_type)
        if key_columns:
            key_columns = TableAttributeNormalizer.remove_outer_parentheses(key_columns)
            # return f"{TableInfoKey.KEY_KWARG_MAP[key_type]} ({key_columns})"
            return ReflectedTableKeyInfo(type=TableInfoKey.KEY_KWARG_MAP[key_type], columns=key_columns)
    return None

def _is_equal_key_with_defaults(
    conn_value: Optional[str], default_value: Optional[str]
) -> bool:
    """
    Compare key / table type, considering that the reflected default might be more specific.

    For example, a default of 'DUPLICATE KEY' should match a connection value of
    'DUPLICATE KEY(id, dt)'.

    Args:
        conn_value: The key attribute value reflected from the database.
        default_value: The known default value for the key attribute.

    Returns:
        True if the connection value is considered equal to the default (e.g., it starts
        with the default), False otherwise.
    """
    if conn_value is None:
        return default_value is None
    if default_value is None:
        return conn_value is None

    # Normalize by converting to lowercase and removing extra spaces
    conn_norm = conn_value.lower()
    default_norm = default_value.lower()

    # Check if conn_value starts with the default_value, ignoring case and spaces
    return conn_norm.startswith(default_norm)


def _is_equal_partition_method(
    conn_partition: Optional[Union[ReflectedPartitionInfo, str]],
    default_partition: Optional[str]
) -> bool:
    """
    Compare two ReflectedPartitionInfo objects for equality.

    This comparison deliberately ignores pre-created partition info (e.g., `VALUES
    LESS THAN (...)`) and only compares the partitioning scheme itself (type and
    the column list or expression list).

    Args:
        conn_partition: The partition info reflected from the database.
        meta_partition: The partition info from the target metadata.

    Returns:
        True if the partitioning schemes are considered equal, False otherwise.
    """
    if conn_partition is None:
        return default_partition is None
    if default_partition is None:
        return conn_partition is None

    # If the partition info is a string, it's the partition_by expression, not a ReflectedPartitionInfo object
    if isinstance(conn_partition, ReflectedPartitionInfo):
        conn_partition = conn_partition.partition_method

    # Only compare the partition_method.
    return conn_partition == default_partition

def _compare_table_comment_sr(
    ops_list: List[AlterTableOp],
    schema: Optional[str],
    table_name: str,
    conn_table: Table,
    metadata_table: Table,
    meta_table_attributes: CaseInsensitiveDict,
) -> None:
    """Compare table comments, with backward compatibility for 'starrocks_comment'.
    Note: useless now.
    """
    conn_comment = conn_table.comment
    meta_comment = metadata_table.comment

    if meta_comment is None:
        # For backward compatibility
        if starrocks_comment := meta_table_attributes.get(TableInfoKey.COMMENT):
            import warnings
            warnings.warn(
                f"The 'starrocks_comment' dialect argument is deprecated for table '{table_name}'. "
                "Please use the standard 'comment' argument on the Table object instead.",
                DeprecationWarning,
            )
            meta_comment = starrocks_comment

    if conn_comment != meta_comment:
        from alembic.operations import ops

        if meta_comment is None:
            ops_list.append(
                ops.DropTableCommentOp(
                    table_name, schema=schema, existing_comment=conn_comment
                )
            )
        else:
            ops_list.append(
                ops.CreateTableCommentOp(
                    table_name,
                    meta_comment,
                    schema=schema,
                    existing_comment=conn_comment,
                )
            )

def _compare_table_partition(
    ops_list: List[AlterTableOp],
    schema: Optional[str],
    table_name: str,
    conn_table_attributes: Dict[str, Any],
    meta_table_attributes: Dict[str, Any],
    default_value_override: Optional[str] = None,
    support_change_override: Optional[bool] = None,
    ddl_object: str = "TABLE",
    object_label: str = "Table",
) -> None:
    """Compare partition changes and add AlterTablePartitionOp if needed."""
    conn_partition = conn_table_attributes.get(TableInfoKey.PARTITION_BY)
    meta_partition = meta_table_attributes.get(TableInfoKey.PARTITION_BY)
    logger.debug("Compares table PARTITION_BY. conn_partition: %s, meta_partition: %s", conn_partition, meta_partition)

    # if not meta_partition:
    #     logger.error(f"Partition info should be specified in metadata for table {table_name} in schema {schema}.")
    #     return

    # Parse the partition info if it's a string
    if isinstance(conn_partition, str):
        conn_partition = StarRocksTableDefinitionParser.parse_partition_clause(conn_partition)
    if isinstance(meta_partition, str):
        meta_partition = StarRocksTableDefinitionParser.parse_partition_clause(meta_partition)

    # Normalize the partition method, such as 'RANGE(dt)', 'LIST(dt, col2)', which is used to be compared.
    normalized_conn: Optional[str] = TableAttributeNormalizer.normalize_partition_method(conn_partition)
    normalized_meta: str = TableAttributeNormalizer.normalize_partition_method(meta_partition)

    changed = _compare_single_table_attribute(
        table_name,
        schema,
        TableInfoKey.PARTITION_BY,
        normalized_conn,
        normalized_meta,
        default_value=(default_value_override if default_value_override is not None else ReflectionTableDefaults.partition_by()),
        support_change=(support_change_override if support_change_override is not None else AlterTableEnablement.PARTITION_BY),
        equal_to_default_cmp_func=_is_equal_partition_method,
        ddl_object=ddl_object,
        object_label=object_label,
    )
    if changed:
        from starrocks.alembic.ops import AlterTablePartitionOp
        ops_list.append(
            AlterTablePartitionOp(
                table_name,
                meta_partition.partition_method,
                schema=schema,
            )
        )
        table_fqn = utils.gen_simple_qualified_name(table_name, schema)
        logger.info(f"Detected PARTITON change on {object_label.lower()} {table_fqn!r}, "
                    f"from {conn_partition!r} to {meta_partition!r}")


def _is_random_distribution(distribution: Optional[Any]) -> bool:
    """Whether a parsed distribution is RANDOM, which StarRocks refuses every ALTER of."""
    return distribution is not None and str(distribution.distribution_method).upper() == TableDistribution.RANDOM


def _compare_table_distribution(
    ops_list: List[AlterTableOp],
    schema: Optional[str],
    table_name: str,
    conn_table_attributes: Dict[str, Any],
    meta_table_attributes: Dict[str, Any],
    default_value_override: Optional[str] = None,
    support_change_override: Optional[bool] = None,
    ddl_object: str = "TABLE",
    object_label: str = "Table",
    skip_when_meta_unset: bool = False,
) -> None:
    """Compare distribution changes and add AlterTableDistributionOp if needed."""
    conn_distribution = conn_table_attributes.get(TableInfoKey.DISTRIBUTED_BY)
    meta_distribution = meta_table_attributes.get(TableInfoKey.DISTRIBUTED_BY)
    if meta_distribution is None:
        # For backward compatibility
        if starrocks_distribution := meta_table_attributes.get("DISTRIBUTION"):
            import warnings
            warnings.warn(
                f"The 'starrocks_distribution' dialect argument is deprecated for table '{table_name}'. "
                "Please use 'starrocks_distributed_by' instead.",
                DeprecationWarning,
            )
            meta_distribution = starrocks_distribution

    # For materialized views, if the distribution is undefined, it is auto-assigned, but appears in SHOW CREATE
    # Should be skipped because the auto-assign is not managed by user
    if meta_distribution is None and skip_when_meta_unset:
        return

    if isinstance(conn_distribution, str):
        conn_distribution = StarRocksTableDefinitionParser.parse_distribution(conn_distribution)
    if isinstance(meta_distribution, str):
        meta_distribution = StarRocksTableDefinitionParser.parse_distribution(meta_distribution)

    # If distribution method is the same and meta doesn't specify buckets,
    # consider it unchanged, as conn buckets might be system-assigned.
    if (
        conn_distribution and meta_distribution and
        conn_distribution.distribution_method == meta_distribution.distribution_method and
        meta_distribution.buckets is None
    ):
        return

    # Normalize both strings for comparison (handles backticks)
    normalized_conn: Optional[str] = TableAttributeNormalizer.normalize_distribution_string(conn_distribution)
    normalized_meta: Optional[str] = TableAttributeNormalizer.normalize_distribution_string(meta_distribution)
    logger.debug("Compares table DISTRIBUTED_BY. normalized_conn: %s, normalized_meta: %s", normalized_conn, normalized_meta)

    # Use generic comparison logic with default distribution
    changed = _compare_single_table_attribute(
        table_name,
        schema,
        TableInfoKey.DISTRIBUTED_BY,
        normalized_conn,
        normalized_meta,
        default_value=(default_value_override if default_value_override is not None else ReflectionTableDefaults.distribution_type()),
        support_change=(support_change_override if support_change_override is not None else AlterTableEnablement.DISTRIBUTED_BY),
        ddl_object=ddl_object,
        object_label=object_label,
    )
    if changed and _is_random_distribution(conn_distribution):
        table_fqn = utils.gen_simple_qualified_name(table_name, schema)
        raise UnalterableChange(
            f"Detected DISTRIBUTION change on {object_label.lower()} {table_fqn!r}, from {conn_distribution!r} to "
            f"{meta_distribution!r}. StarRocks refuses to alter the distribution of a RANDOM-distributed table, to "
            f"HASH or to a different bucket count, since it scales its buckets automatically."
        )
    if changed:
        from starrocks.alembic.ops import AlterTableDistributionOp

        ops_list.append(
            AlterTableDistributionOp(
                table_name,
                meta_distribution.distribution_method,
                meta_distribution.buckets,
                schema=schema,
                existing_distribution_method=conn_distribution.distribution_method if conn_distribution else None,
                existing_buckets=conn_distribution.buckets if conn_distribution else None,
            )
        )
        table_fqn = utils.gen_simple_qualified_name(table_name, schema)
        logger.info(f"Detected DISTRIBUTION change on {object_label.lower()} {table_fqn!r}, "
                    f"from {conn_distribution!r} to {meta_distribution!r}")


def _compare_table_order_by(
    ops_list: List[AlterTableOp],
    schema: Optional[str],
    table_name: str,
    conn_table_attributes: Dict[str, Any],
    meta_table_attributes: Dict[str, Any],
    default_value_override: Optional[str] = None,
    support_change_override: Optional[bool] = None,
    ddl_object: str = "TABLE",
    object_label: str = "Table",
) -> None:
    """Compare ORDER BY changes and add AlterTableOrderOp if needed."""
    conn_order = conn_table_attributes.get(TableInfoKey.ORDER_BY)
    meta_order = meta_table_attributes.get(TableInfoKey.ORDER_BY)

    # Normalize both for comparison (handles backticks and list vs string)
    normalized_conn: Optional[str] = TableAttributeNormalizer.normalize_order_by_string(conn_order) if conn_order else None
    normalized_meta: Optional[str] = TableAttributeNormalizer.normalize_order_by_string(meta_order) if meta_order else None
    logger.debug("Compares table ORDERY BY. normalized_conn: %s, normalized_meta: %s", normalized_conn, normalized_meta)

    # if ORDER BY is not set, we directly recoginize it as no change
    if not normalized_meta:
        return

    # Use generic comparison logic with default ORDER BY
    changed = _compare_single_table_attribute(
        table_name,
        schema,
        TableInfoKey.ORDER_BY,
        normalized_conn,
        normalized_meta,
        default_value=(default_value_override if default_value_override is not None else ReflectionTableDefaults.order_by()),
        support_change=(support_change_override if support_change_override is not None else AlterTableEnablement.ORDER_BY),
        ddl_object=ddl_object,
        object_label=object_label,
    )
    if changed:
        from starrocks.alembic.ops import AlterTableOrderOp
        ops_list.append(
            AlterTableOrderOp(
                table_name,
                meta_order,  # Use original format
                schema=schema,
                existing_order_by=conn_order if conn_order else None,
            )
        )
        table_fqn = utils.gen_simple_qualified_name(table_name, schema)
        logger.info(f"Detected ORDER BY change on {object_label.lower()} {table_fqn!r}, "
                    f"from {conn_order!r} to {meta_order!r}")

def _compare_table_properties_impl(
    schema: Optional[str],
    table_name: str,
    conn_table_attributes: Dict[str, Any],
    meta_table_attributes: Dict[str, Any],
    run_mode: str,
    default_cls: Union[Type[ReflectionTableDefaults], Type[ReflectionMVDefaults]] = ReflectionTableDefaults,
    object_label: str = "Table",
    add_default_prefix: bool = True,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Compare properties changes and add AlterTablePropertiesOp if needed.

    - If a property is specified in metadata, it is compared with the database. When the
      database does not report it, the default from ``default_cls`` stands in for its value.
    - If a property is NOT specified in metadata, it is unmanaged: the database value is left
      alone and no operation is generated. Many defaults depend on the StarRocks version or
      FE config (e.g. ``bucket_size``, ``replication_num``), so a hardcoded default cannot
      tell a user-set value from one StarRocks filled in. Unknown properties (no entry in
      ``default_cls``) are additionally warned about.
    - The generated operation will set only the properties that have changed.
      Because some of the properties are not supported to be changed.

    Args:
        add_default_prefix: Whether to add the default prefix to the property name.
            If True, the property name will be prefixed with 'default.'.
            If False, the property name will not be prefixed.
            This is used to control the behavior of the generated operation.
            If the property is a future partition property, it should be prefixed.
            If the property is a past partition property, it should not be prefixed.
            The default value is True.
    """
    conn_properties: Dict[str, str] = conn_table_attributes.get(TableInfoKey.PROPERTIES, {})
    meta_properties: Dict[str, str] = meta_table_attributes.get(TableInfoKey.PROPERTIES, {})
    logger.debug("Compares %s PROPERTIES. conn_properties: %s, meta_properties: %s", object_label.lower(), conn_properties, meta_properties)

    normalized_conn = InvalidTableProperty.purify_dict(CaseInsensitiveDict(conn_properties), run_mode, table_name=table_name)
    normalized_meta = InvalidTableProperty.purify_dict(CaseInsensitiveDict(meta_properties), run_mode, table_name=table_name)
    # logger.debug("PROPERTIES. normalized_conn: %s, normalized_meta: %s", normalized_conn, normalized_meta)

    properties_to_set = {}
    properties_for_reverse = {}

    all_keys = set([k.lower() for k in normalized_conn.keys() | normalized_meta.keys()])
    full_name = utils.gen_simple_qualified_name(table_name, schema)

    for key in all_keys:
        conn_value = normalized_conn.get(key)
        meta_value = normalized_meta.get(key)
        default_value = default_cls.properties(run_mode).get(key)

        # Convert all to strings for comparison to avoid type issues (e.g., int vs str)
        conn_str = str(conn_value) if conn_value is not None else None
        meta_str = str(meta_value) if meta_value is not None else None
        default_str = str(default_value) if default_value is not None else None

        # The effective value in the database is conn_str if set, otherwise default_str
        effective_conn_str = conn_str if conn_str is not None else default_str
        # The effective value in the metadata is meta_str if set, otherwise default_str
        effective_meta_str = meta_str if meta_str is not None else default_str

        if (default_cls.normalize_property_value(key, effective_conn_str)
                == default_cls.normalize_property_value(key, effective_meta_str)):
            # logger.debug("Property no changes. key: %s, effective_conn_str: %s, effective_meta_str: %s", key, effective_conn_str, effective_meta_str)
            continue

        if meta_value is None:
            # Unmanaged property: leave the database value alone. See the docstring.
            if default_value is None:
                logger.warning(
                    f"{object_label} '{full_name}': Property '{key}' exists in the database with value '{conn_value}' "
                    f"but is not specified in metadata and no default is defined in {default_cls.__name__}. "
                    f"To manage this property, please specify it explicitly in your metadata. "
                    f"No ALTER TABLE SET operation will be generated for this property."
                )
            else:
                logger.debug(
                    "Property %r is not specified in metadata for %r; leaving the database "
                    "value %r unchanged (default %r).",
                    key, full_name, conn_value, default_value,
                )
            continue

        unreported_reason = default_cls.unreported_property_reason(key)
        if conn_value is None and unreported_reason:
            # The database does not report it, so the table cannot hold it: an ALTER would
            # fail or repeat on every run.
            logger.warning(
                f"{object_label} '{full_name}': Property '{key}' is set to '{meta_value}' in metadata "
                f"but the database does not report it ({unreported_reason}). It cannot be compared. "
                f"No ALTER TABLE SET operation will be generated for this property."
            )
            continue

        logger.debug("Property changes. key: %s, effective_conn_str: %s, effective_meta_str: %s", key, effective_conn_str, effective_meta_str)
        # Determine the value for the upgrade operation.
        target_val_upgrade = meta_str
        prop_key = (
            TablePropertyForFuturePartitions.wrap(key)
            if add_default_prefix and TablePropertyForFuturePartitions.contains(key)
            else key
        )
        # logger.debug("Newly changed property. prop_key: %r, target_val_upgrade: %r", prop_key, target_val_upgrade)
        properties_to_set[prop_key] = target_val_upgrade
        # A meaningful change has been detected for this property.
        logger.info(
            f"Detected PROPERTY {key!r} change on {object_label.lower()} {full_name!r}, "
            f"from {effective_conn_str!r} to {effective_meta_str!r}"
        )
        if prop_key != key:
            logger.warning(f"The property '{key}' will be changed to '{target_val_upgrade}' "
                f"for the future partitions only by using '{prop_key}'. "
                f"If you want to change the property for all partitions, "
                f"please modify it by removing the 'default.' prefix."
            )

        target_val_downgrade = default_cls.downgrade_value(key, conn_str, default_str, normalized_conn.keys())
        if target_val_downgrade is None:
            logger.warning(
                f"{object_label} '{full_name}': The database reports no value for property '{key}', and the dialect "
                f"has no value to set it back to, so the downgrade cannot restore it and leaves it as the "
                f"upgrade sets it."
            )
        else:
            properties_for_reverse[prop_key] = target_val_downgrade

    return properties_to_set, properties_for_reverse


def _compare_table_properties(
    ops_list: List[AlterTableOp],
    schema: Optional[str],
    table_name: str,
    conn_table_attributes: Dict[str, Any],
    meta_table_attributes: Dict[str, Any],
    run_mode: str,
    server_version: Optional[Tuple[int, ...]] = None,
) -> None:
    """Add an AlterTablePropertiesOp for the changed properties StarRocks can SET on this cluster."""
    properties_to_set, properties_for_reverse = _compare_table_properties_impl(
        schema, table_name, conn_table_attributes, meta_table_attributes, run_mode,
        default_cls=ReflectionTableDefaults, object_label="Table")
    _leave_out_properties_starrocks_cannot_set(
        properties_to_set,
        properties_for_reverse,
        "Table",
        utils.gen_simple_qualified_name(table_name, schema),
        lambda key: UnalterableTableProperty.reason(key, run_mode, server_version),
    )
    if properties_to_set:
        table_fqn = utils.gen_simple_qualified_name(table_name, schema)
        ops_list.append(
            AlterTablePropertiesOp(
                table_name,
                properties_to_set,
                schema=schema,
                existing_properties=properties_for_reverse,
            )
        )
        logger.debug(f"Detected PROPERTIES change on table {table_fqn!r}.")


def _leave_out_properties_starrocks_cannot_set(
    properties_to_set: Dict[str, str],
    properties_for_reverse: Dict[str, str],
    object_label: str,
    object_fqn: str,
    refusal_reason: Callable[[str], Optional[str]],
) -> None:
    """Remove from both directions, with a warning, each property StarRocks refuses to SET on this object.

    ``refusal_reason`` gives why StarRocks refuses a property (lowercase), or None; see
    :class:`UnalterableTableProperty` and :class:`UnalterableMaterializedViewProperty`. The property may be one the
    model states, or one the comparison resets to its default. StarRocks takes one property per SET, so a refused
    property would stop the migration after the properties before it were already set.
    """
    for key in list(properties_to_set):
        reason = refusal_reason(key.lower())
        if reason is None:
            continue
        logger.warning(
            f"{object_label} {object_fqn!r}: the migration would set property {key!r} to "
            f"{properties_to_set.pop(key)!r}, where the database has {properties_for_reverse.pop(key, None)!r}, but "
            f"{reason}. No operation is generated for it; recreate the {object_label.lower()} to change it."
        )


def _compare_single_table_attribute(
        table_name: Optional[str],
        schema: Optional[str],
        attribute_name: str,
        conn_value: Optional[str],
        meta_value: Optional[str],
        default_value: Optional[str] = None,
        equal_to_default_cmp_func: Optional[Callable[[Any, Any], bool]] = None,
        support_change: bool = True,
        ddl_object: str = "TABLE",
        object_label: str = "Table",
) -> bool:
    """
    Generic comparison logic for a single table attribute.

    Args:
        table_name: Table name for logging context.
        schema: Schema name for logging context.
        attribute_name: Name of the attribute for logging.
        conn_value: Value reflected from database (None if not present).
        meta_value: Value specified in metadata (None if not specified).
        default_value: Known default value for this attribute (None if no default).
        support_change: Whether this attribute supports ALTER operations.
        equal_to_default_cmp_func: An optional function to perform a custom comparison
            between the connection value and the default value. If provided, this is
            used when `meta_value` is None.

    Returns:
        True if there's a meaningful change that requires an ALTER statement.

    Raises:
        UnalterableChange: If support_change=False and a change is detected

    Logic:
        1. If meta specifies value != (conn value or default value) -> change needed
        2. If meta specifies value == (conn value or default value) -> no change
        3. If meta not specified and (conn is None or conn == default) -> no change
        4. If meta not specified and conn != default -> leave the database value alone, return False
    """
    # Convert values to strings for comparison (handle None gracefully)
    conn_str = str(conn_value) if conn_value is not None else None
    meta_str = str(meta_value) if meta_value is not None else None
    default_str = str(default_value) if default_value is not None else None

    table_fqn = utils.gen_simple_qualified_name(table_name, schema)
    attribute_name: str = attribute_name.upper().replace('_', ' ')

    if meta_str is not None:
        # Case 1 & 2: meta_table specifies this attribute
        if meta_str != (conn_str or default_str):
            # Case 1: meta specified, different from conn -> has change
            logger.debug(
                f"{object_label} {table_fqn!r}, Attribute '{attribute_name}' "
                f"has changed from '{conn_str or '(not set)'}' to '{meta_str}' "
                f"with default value '{default_str}'")
            if meta_value.lower() == (conn_str or default_str or '').lower():
                logger.warning(
                    f"{object_label} {table_fqn!r}: Attribute '{attribute_name}' has a case-only difference: "
                    f"'{conn_value}' (database) vs '{meta_value}' (metadata). "
                    f"Consider making them consistent for clarity."
                    f"No ALTER statement will be generated automatically."
                )
                return False
            if not support_change:
                # This attribute doesn't support ALTER operations
                error_msg = (
                    f"StarRocks does not support 'ALTER {ddl_object} {attribute_name}'. "
                    f"{object_label} {table_fqn!r} has {attribute_name.upper()} '{conn_str or '(not set)'}' in database "
                    f"but '{meta_str}' in metadata. "
                    f"Please update your metadata to match the database."
                )
                raise UnalterableChange(error_msg)
            # Log the detected change at INFO level
            logger.info(
                f"Detected attribute {attribute_name.upper()} change on {object_label.lower()} {table_fqn!r}, "
                f"from {conn_str or '(not set)'!r} (in database) to {meta_str!r} (in metadata)")
            return True
        # Case 2: meta specified, same as conn -> no change
        return False
    else:
        # Case 3.1: both conn and meta are None
        if conn_str is None:
            return False
        # Case 3 & 4: meta_table does NOT specify this attribute
        if conn_str != default_str:
            # If custom comparison function is provided, use it for default comparison
            if equal_to_default_cmp_func and equal_to_default_cmp_func(conn_value, default_value):
                logger.debug(
                    f"{object_label} {table_fqn!r}: Attribute '{attribute_name}' in database is considered "
                    f"equal to default '{default_str}' via custom comparison function."
                )
                return False

            # Case 4: meta not specified, conn is non-default -> log error, NO automatic change
            if conn_str.lower() == (default_str or '').lower():
                logger.warning(
                    f"{object_label} {table_fqn!r}: Attribute '{attribute_name}' has a case-only difference: "
                    f"'{conn_str}' (database) vs '{default_str}' (default). "
                    f"Consider making them consistent for clarity. "
                    f"No ALTER statement will be generated automatically."
                )
                pass
            else:
                logger.debug(
                    f"{object_label} {table_fqn!r}: Attribute '{attribute_name}' is '{conn_str}' in the database "
                    f"(default: '{default_str}') and not set in metadata, so it is left as it is."
                )
        # Case 3: meta not specified, conn is default (or no default defined) -> no change
        return False

def extract_starrocks_dialect_attributes(kwargs: Dict[str, Any]) -> CaseInsensitiveDict:
    """Extract StarRocks-specific dialect attributes from a dict, with each attribute prefixed with 'starrocks_'.

    Returns a CaseInsensitiveDict for case-insensitive key access, with prefix 'starrocks_' removed.

    Currently, it's useless, because we use Table.dialect_options[dialect] to get it.
    """
    result = CaseInsensitiveDict()
    if not kwargs:
        return result
    for k, v in kwargs.items():
        if k.lower().startswith(SRKwargsPrefix):
            result[k[len(SRKwargsPrefix):]] = v
    return result


@comparators_dispatch_for_starrocks("column")
def compare_starrocks_column_agg_type(
    autogen_context: AutogenContext,
    alter_column_op: AlterColumnOp,
    schema: Optional[str],
    tname: Union[quoted_name, str],
    cname: Union[quoted_name, str],
    conn_col: Column[Any],
    metadata_col: Column[Any],
) -> None:
    """
    Compare StarRocks-specific column options.

    Check for changes in StarRocks-specific attributes like aggregate type.
    """
    if conn_col is None or metadata_col is None:
        raise ArgumentError("Both conn column and meta column should not be None.")

    # Extract dialect_options for comparison using case-insensitive helper
    conn_opts = extract_dialect_options_as_case_insensitive(conn_col)
    meta_opts = extract_dialect_options_as_case_insensitive(metadata_col)
    conn_agg_type: Union[str, None] = conn_opts.get(ColumnAggInfoKey.AGG_TYPE)
    meta_agg_type: Union[str, None] = meta_opts.get(ColumnAggInfoKey.AGG_TYPE)
    # logger.debug("Compares column AGG_TYPE. conn_agg_type: %s, meta_agg_type: %s", conn_agg_type, meta_agg_type)

    if meta_agg_type != conn_agg_type:
        # Update the alter_column_op with the new aggregate type. useless now
        # "KEY", "SUM" for set, None for unsert
        if alter_column_op is not None:
            alter_column_op.kw[ColumnAggInfoKeyWithPrefix.AGG_TYPE] = meta_agg_type
        logger.warning(
            f"StarRocks does not support changing the aggregation type of a column: '{cname}', "
            f"from {conn_agg_type} to {meta_agg_type}. No operation is generated for it; apply this change by hand."
        )
        return

    # we need to set it in the AlterColumnOp, because the KEY/AGG_TYPE is always needed.
    # TODO: But currently, it's not passed to MySQLModifyColumn
    if alter_column_op:
        alter_column_op.kw[ColumnAggInfoKeyWithPrefix.AGG_TYPE] = meta_agg_type


@comparators_dispatch_for_starrocks("column", run_last=True)
def compare_starrocks_column_autoincrement(
    autogen_context: AutogenContext,
    alter_column_op: AlterColumnOp,
    schema: Optional[str],
    tname: Union[quoted_name, str],
    cname: quoted_name,
    conn_col: Column[Any],
    metadata_col: Column[Any],
) -> None:
    """Keep ``autoincrement`` on an altered column only when the model declares one the database does not have.

    Alembic gives the alter of any single Integer primary key column, and of one declared ``autoincrement=True``,
    ``autoincrement=True``, and the script carries no ``existing_autoincrement``. The dialect renders
    AUTO_INCREMENT only for a column declared with it, and from 4.1.3 StarRocks keeps a column's AUTO_INCREMENT
    through a MODIFY COLUMN that leaves it out and changes nothing but the comment (earlier versions refuse any
    MODIFY COLUMN of it), so ``autoincrement`` is dropped unless it differs from the database. StarRocks cannot add
    or remove AUTO_INCREMENT, so a difference is warned about here and refused by the impl. This comparator runs
    after Alembic's column comparators.
    """
    if conn_col is None or metadata_col is None:
        raise ArgumentError("Both conn column and meta column should not be None.")

    conn_autoincrement = conn_col.autoincrement is True
    declared = metadata_col.autoincrement if isinstance(metadata_col.autoincrement, bool) else None
    if alter_column_op is not None and (declared is None or declared == conn_autoincrement):
        alter_column_op.kw.pop("autoincrement", None)
    if declared is not None and declared != conn_autoincrement:
        table_fqn = utils.gen_simple_qualified_name(tname, schema)
        logger.warning(
            f"Column '{table_fqn}.{cname}' is declared with autoincrement={declared!r}, but the database "
            f"{'does not give it' if declared else 'gives it'} AUTO_INCREMENT. StarRocks cannot add or remove "
            "AUTO_INCREMENT, so an alter of this column will be refused; recreate the table to change it."
        )
    return None


def server_defaults_match(
    metadata_column: Column[Any],
    rendered_metadata_default: Optional[str],
    rendered_inspector_default: Optional[str],
    inspector_column_type: Any = None,
    boolean_kept_as_digits: bool = True,
) -> bool:
    """
    Whether a column's default in the metadata is the default reflected from StarRocks.

    Alembic renders a string column's default as a quoted literal even when it is an expression such as
    ``text("uuid()")``, and StarRocks reports a function default wrapped in parentheses, ``(uuid())``. So the
    metadata default is read from the column itself, and both sides are compared as canonical defaults: a
    literal by its decoded value, a function or keyword case insensitively and without wrapping parentheses.
    An empty string is a default like any other. The defaults are then reduced as StarRocks keeps them, the way
    ``alter_column`` compares a restated default (see :func:`utils.canonical_default_as_kept`), so that ``now()``
    matches the ``CURRENT_TIMESTAMP`` StarRocks reflects, and ``'true'`` the ``'1'`` it keeps for a BOOLEAN column.
    """
    if rendered_metadata_default is None or rendered_inspector_default is None:
        return False
    metadata_sql = _metadata_default_sql(metadata_column, rendered_metadata_default)
    column_type = inspector_column_type if isinstance(inspector_column_type, sqltypes.Boolean) else metadata_column.type
    return utils.canonical_default_as_kept(
        utils.canonical_server_default(metadata_sql), column_type, boolean_kept_as_digits
    ) == utils.canonical_default_as_kept(
        utils.canonical_server_default(rendered_inspector_default), column_type, boolean_kept_as_digits
    )


def _metadata_default_sql(metadata_column: Column[Any], rendered_metadata_default: str) -> str:
    """The SQL a metadata column's server default stands for: a str is a literal, an expression is its text.

    An expression is compiled to the text StarRocks reflects, so a ``%`` in it is not doubled.
    """
    default = metadata_column.server_default
    if not isinstance(default, sa_schema.DefaultClause):
        return rendered_metadata_default
    if isinstance(default.arg, str):
        return utils.quote_sql_string_literal(default.arg)
    if isinstance(default.arg, TextClause):
        return default.arg.text
    return stored_sql_text(default.arg)


def server_default_text(server_default: Any) -> Optional[str]:
    """Return the text of a server default, as a string, a ``text()`` or a ``DefaultClause``, or ``None`` for none."""
    if server_default is None or server_default is False:
        return None
    argument = getattr(server_default, "arg", server_default)
    return str(getattr(argument, "text", argument))


def _warn_and_skip_default_change(
    alter_column_op: AlterColumnOp,
    schema: Optional[str],
    tname: Union[quoted_name, str],
    existing_default: Any,
) -> None:
    """Warn about a change of a column's default value and leave it out of the ALTER COLUMN op.

    StarRocks cannot change a default ("Can not change default value"), so an op that carries the new default
    could not be applied.
    """
    table_fqn = utils.gen_simple_qualified_name(tname, schema)
    logger.warning(
        f"Detected DEFAULT change on column '{table_fqn}.{alter_column_op.column_name}', "
        f"from {server_default_text(existing_default)!r} (in database) to "
        f"{server_default_text(alter_column_op.modify_server_default)!r} (in metadata). StarRocks does not support "
        f"changing a column's default value. No operation is generated for it; recreate the column or the table "
        f"with the new default."
    )
    alter_column_op.modify_server_default = False


@comparators_dispatch_for_starrocks("column")
def skip_starrocks_column_default_change(
    autogen_context: AutogenContext,
    alter_column_op: AlterColumnOp,
    schema: Optional[str],
    tname: Union[quoted_name, str],
    cname: Union[quoted_name, str],
    conn_col: Column[Any],
    metadata_col: Column[Any],
) -> None:
    """Skip a default change where Alembic runs its default comparison before this comparator.

    That is every Alembic before 1.18, which runs its own comparators first; later ones run theirs after, and
    `skip_starrocks_default_changes` clears the change there.
    """
    if alter_column_op is not None and alter_column_op.modify_server_default is not False:
        _warn_and_skip_default_change(alter_column_op, schema, tname, conn_col.server_default)


def skip_starrocks_default_changes(autogen_context: AutogenContext, upgrade_ops: UpgradeOps) -> None:
    """Skip every default change in the net changes, after Alembic 1.18's own comparators have produced them.

    Alembic 1.18 and later compare server defaults after the dialect comparators of the same priority have run,
    so the column comparator above sees no change yet. An ALTER COLUMN op left with no change, and a table
    left with no op, are removed.
    """
    for table_ops in list(upgrade_ops.ops):
        column_ops = getattr(table_ops, "ops", None)
        if column_ops is None:
            continue
        skipped_any = False
        for column_op in list(column_ops):
            if isinstance(column_op, AlterColumnOp) and column_op.modify_server_default is not False:
                _warn_and_skip_default_change(
                    column_op, table_ops.schema, table_ops.table_name, column_op.existing_server_default
                )
                skipped_any = True
                if not column_op.has_changes():
                    column_ops.remove(column_op)
        if skipped_any and not column_ops:
            upgrade_ops.ops.remove(table_ops)


if _LAST_PRIORITY:
    comparators_dispatch_for_starrocks("autogenerate", run_last=True)(skip_starrocks_default_changes)
