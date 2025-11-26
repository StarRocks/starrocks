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
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union
import warnings

from alembic.autogenerate import comparators
from alembic.autogenerate.api import AutogenContext
from alembic.ddl import DefaultImpl
from alembic.operations.ops import AlterColumnOp, AlterTableOp, UpgradeOps
from sqlalchemy import Column
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql import schema as sa_schema, sqltypes, quoted_name
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
from starrocks.common import utils
from starrocks.common.defaults import ReflectionMVDefaults, ReflectionTableDefaults, ReflectionViewDefaults
from starrocks.common.params import (
    AlterMVEnablement,
    AlterTableEnablement,
    ColumnAggInfoKey,
    ColumnAggInfoKeyWithPrefix,
    DialectName,
    SRKwargsPrefix,
    TableInfoKey,
    TableKind,
    TableObjectInfoKey,
    TablePropertyForFuturePartitions,
)
from starrocks.common.utils import (
    CaseInsensitiveDict,
    TableAttributeNormalizer,
    extract_dialect_options_as_case_insensitive,
)
from starrocks.datatype import ARRAY, BOOLEAN, MAP, STRING, STRUCT, TINYINT, VARCHAR
from starrocks.engine.interfaces import ReflectedPartitionInfo, ReflectedTableKeyInfo
from starrocks.reflection import StarRocksTableDefinitionParser
from starrocks.sql.schema import MaterializedView, View


logger = logging.getLogger(__name__)


def compare_simple_type(impl: DefaultImpl, inspector_column: Column[Any], metadata_column: Column[Any]) -> bool:
    """
    Set StarRocks' specific simple type comparison logic for some special cases.

    For some special cases:
        - meta.BOOLEAN equals to conn.TINYINT(1)
        - meta.STRING equals to conn.VARCHAR(65533)

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
    if (isinstance(metadata_type, BOOLEAN) and
        isinstance(inspector_type, TINYINT) and
        getattr(inspector_type, 'display_width', None) == 1):
        # logger.debug("compare_simple_type with BOOLEAN vs TINYINT(1), treat them as the same.")
        return False

    # Scenario 1.b: model defined TINYINT(1), database may display as Boolean (theoretically not possible, but for safety)
    if (isinstance(metadata_type, TINYINT) and
        getattr(metadata_type, 'display_width', None) == 1 and
        isinstance(inspector_type, BOOLEAN)):
        # logger.debug("compare_simple_type with TINYINT(1) vs BOOLEAN, treat them as the same.")
        return False

    # Scenario 2.a: model defined STRING, database stored VARCHAR(65533)
    if (isinstance(metadata_type, STRING) and
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


def comparators_dispatch_for_starrocks(dispatch_type: str):
    """
    StarRocks-specific dispatch decorator.

    Automatically handles dialect checking, only executes the decorated function under StarRocks dialect.

    Args:
        dispatch_type: Alembic dispatch type ("table", "column", "view", etc.)

    Usage:
        @starrocks_dispatch_for("table")
        def compare_starrocks_table(autogen_context, conn_table, metadata_table):
            # No need to manually check dialect, decorator handles it automatically
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
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
        return comparators.dispatch_for(dispatch_type)(wrapper)

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
        if table_kind in (TableKind.VIEW, TableKind.MATERIALIZED_VIEW):
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


@comparators_dispatch_for_starrocks("schema")
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
        if table.info.get(TableObjectInfoKey.TABLE_KIND) == TableKind.VIEW
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
                "and `include_name` correctly. Set them in you `env.py` properly if you want to manage views "
                f"in other schemas: {other_schemas!r}",
                UserWarning,
            )

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
        if table.info.get(TableObjectInfoKey.TABLE_KIND) == TableKind.VIEW
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
    definition_changed = _compare_view_definition_and_columns(
        alter_view_op, view_fqn, conn_view, metadata_view
    )
    comment_changed = _compare_view_comment(
        alter_view_op, view_fqn, conn_view, metadata_view
    )
    security_changed = _compare_view_security(
        alter_view_op, view_fqn, conn_view, metadata_view,
        conn_view_attributes, meta_view_attributes
    )

    # If any attribute has changed, append the operation
    if definition_changed or comment_changed or security_changed:
        upgrade_ops.ops.append(alter_view_op)

        # Log which attributes changed
        changed_attrs = []
        if definition_changed:
            changed_attrs.append("definition")
        if comment_changed:
            changed_attrs.append("comment")
        if security_changed:
            changed_attrs.append("security")

        logger.debug(f"Detected changed view attributes ({', '.join(changed_attrs)}) on {view_fqn!r}")

def _compare_view_definition_and_columns(
    alter_view_op: AlterViewOp,
    view_fqn: str,
    conn_view: View,
    metadata_view: View,
) -> bool:
    """
    Compare view definition and columns, and update AlterViewOp if changed.

    Definition and columns are compared together because in StarRocks,
    columns can only be changed by changing the definition (SELECT statement).

    Args:
        alter_view_op: AlterViewOp object to update
        view_fqn: Fully qualified view name for logging
        conn_view: View reflected from database
        metadata_view: View defined in metadata

    Returns:
        True if definition changed, False otherwise
    """
    from starrocks.sql.schema import extract_view_columns

    # Compare definition (the main content of a view)
    # Definition is stored in table.info (not in dialect_options)
    conn_definition = conn_view.info.get(TableObjectInfoKey.DEFINITION, "")
    meta_definition = metadata_view.info.get(TableObjectInfoKey.DEFINITION, "")
    logger.debug("Compare view definition: conn_definition=%s, meta_definition=%s", conn_definition, meta_definition)

    conn_def_norm = TableAttributeNormalizer.normalize_sql(conn_definition, remove_qualifiers=True)
    meta_def_norm = TableAttributeNormalizer.normalize_sql(meta_definition, remove_qualifiers=True)
    definition_changed = conn_def_norm != meta_def_norm

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

    # StarRocks limitation: Columns can only be changed together with definition
    if columns_changed and not definition_changed:
        raise NotImplementedError(
            f"Detected COLUMN change on view {view_fqn!r}, while definition is unchanged. "
            f"But StarRocks does not support altering view columns independently. "
            f"You must change the definition (SELECT statement) together with columns, "
            f"or use DROP + CREATE to apply this change.",
        )

    # Set definition and columns if definition changed
    if definition_changed:
        alter_view_op.definition = meta_definition
        alter_view_op.columns = extract_view_columns(metadata_view)
        alter_view_op.existing_definition = conn_definition
        alter_view_op.existing_columns = extract_view_columns(conn_view)
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

    return definition_changed


def _compare_view_comment(
    alter_view_op: AlterViewOp,
    view_fqn: str,
    conn_view: View,
    metadata_view: View,
) -> bool:
    """
    Compare view comment and update AlterViewOp if changed.

    Args:
        alter_view_op: AlterViewOp object to update
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

    # logger.debug("compare_view_comment: %s, comment_changed=%s", view_fqn, comment_changed)
    if comment_changed:
        # logger.debug("Comment change for %s Database: %r, Metadata: %r", view_fqn, conn_comment, meta_comment)

        # Warn about comment changes (not supported via ALTER VIEW)
        warnings.warn(
            f"Detected COMMENT change on view {view_fqn!r}, from '{conn_comment}' (in database) to '{meta_comment}' (in metadata). "
            f"But StarRocks does not support altering view comments via ALTER VIEW. "
            f"Consider using DROP + CREATE to apply this change if you want to change the comment.",
            UserWarning,
        )

        # Set comment in AlterViewOp for future compatibility
        alter_view_op.comment = metadata_view.comment
        alter_view_op.existing_comment = conn_view.comment

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

    conn_security = (conn_security or ReflectionViewDefaults.security())
    meta_security = (meta_security or ReflectionViewDefaults.security())
    security_changed = conn_security != meta_security

    # logger.debug("compare_view_security: %s, security_changed=%s", view_fqn, security_changed,)
    if security_changed:
        # logger.debug("Security change for %s Database: %r, Metadata: %r", view_fqn, conn_security, meta_security)

        # Warn about security changes (not supported via ALTER VIEW)
        warnings.warn(
            f"Detected SECURITY change on view {view_fqn!r}, from '{conn_security}' (in database) to '{meta_security}' (in metadata).     "
            f"But StarRocks does not support altering view security via ALTER VIEW. "
            f"Consider using DROP + CREATE to apply this change if you want to change the security.",
            UserWarning,
        )

        # Set security in AlterViewOp for future compatibility
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
@comparators_dispatch_for_starrocks("schema")
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
        if table.info.get(TableObjectInfoKey.TABLE_KIND) == TableKind.MATERIALIZED_VIEW
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
                "and `include_name` correctly. Set them in your `env.py` properly if you want to manage materialized views "
                f"in other schemas: {other_schemas!r}",
                UserWarning,
            )

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
        if table.info.get(TableObjectInfoKey.TABLE_KIND) == TableKind.MATERIALIZED_VIEW
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
    metadata_mv: Union[MaterializedView, Table]
) -> None:
    """
    Compare MV definition and raise error if changed.

    Note: StarRocks does not support altering MV definition, so this will raise an error
    if a change is detected.
    """
    # Always extract definition from table.info to support metadata defined as plain Table
    conn_def_raw = getattr(conn_mv, "definition", None) or conn_mv.info.get(TableObjectInfoKey.DEFINITION, "")
    meta_def_raw = getattr(metadata_mv, "definition", None) or metadata_mv.info.get(TableObjectInfoKey.DEFINITION, "")
    logger.debug("Compare mv definition: conn_def_raw=%s, meta_def_raw=%s", conn_def_raw, meta_def_raw)

    # Normalize and remove qualifiers (schema/table) to avoid false diffs on equivalent SQL
    conn_def_norm = TableAttributeNormalizer.normalize_sql(conn_def_raw, remove_qualifiers=True)
    meta_def_norm = TableAttributeNormalizer.normalize_sql(meta_def_raw, remove_qualifiers=True)

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
        raise NotImplementedError(
            f"Detected DEFINITION change on materialized view {mv_fqn!r}, "
            f"from {conn_def_norm!r} (in database, normalized) to {meta_def_norm!r} (in metadata, normalized). "
            f"StarRocks does not support altering MV definition. "
            f"You need to manually DROP and CREATE the materialized view."
        )


def _compare_mv_comment(
    ops_list: List,
    schema: Optional[str],
    mv_name: str,
    conn_mv: Union[MaterializedView, Table],
    metadata_mv: Union[MaterializedView, Table]
) -> None:
    """
    Compare MV comment and raise error if changed.

    Note: StarRocks does not support altering MV comment, so this will raise an error
    if a change is detected.
    """
    conn_comment = (conn_mv.comment or "").strip()
    meta_comment = (metadata_mv.comment or "").strip()
    logger.debug("Compare mv comment: conn_comment=%r, meta_comment=%r", conn_comment, meta_comment)

    if conn_comment != meta_comment:
        mv_fqn = utils.gen_simple_qualified_name(mv_name, schema)
        raise NotImplementedError(
            f"Detected COMMENT change on materialized view {mv_fqn!r}, "
            f"from {conn_comment!r} to {meta_comment!r}. "
            f"StarRocks does not support altering MV comment. "
            f"You need to manually DROP and CREATE the materialized view."
        )


def _compare_mv_refresh(
    ops_list: List,
    schema: Optional[str],
    mv_name: str,
    conn_mv_attributes: Dict[str, Any],
    meta_mv_attributes: Dict[str, Any]
) -> None:
    """
    Compare MV refresh attributes and add AlterMaterializedViewOp if changed.

    This is a mutable attribute that can be altered using ALTER MATERIALIZED VIEW.
    """
    def _normalize_refresh_string(val: Optional[str]) -> Optional[str]:
        if val is None:
            return None
        # Collapse whitespace and compare case-insensitively
        return " ".join(str(val).split()).upper()

    conn_refresh_raw = conn_mv_attributes.get(TableInfoKey.REFRESH)
    meta_refresh_raw = meta_mv_attributes.get(TableInfoKey.REFRESH)
    logger.debug("Compare mv refresh: conn_refresh_raw=%s, meta_refresh_raw=%s", conn_refresh_raw, meta_refresh_raw)

    conn_refresh = _normalize_refresh_string(conn_refresh_raw)
    meta_refresh = _normalize_refresh_string(meta_refresh_raw)

    if conn_refresh != meta_refresh:

        mv_fqn = utils.gen_simple_qualified_name(mv_name, schema)
        ops_list.append(
            AlterMaterializedViewOp(
                view_name=mv_name,
                schema=schema,
                refresh=str(meta_refresh_raw),
                properties=None,
                existing_refresh=str(conn_refresh_raw),
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

    This is a mutable attribute that can be altered using ALTER MATERIALIZED VIEW.
    """
    properties_to_set, properties_for_reverse = _compare_table_properties_impl(
        schema, mv_name, conn_mv_attributes, meta_mv_attributes, run_mode,
        default_cls=ReflectionMVDefaults, object_label="Materialized view", add_default_prefix=False)
    if properties_to_set:
        mv_fqn = utils.gen_simple_qualified_name(mv_name, schema)
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

    Note: For immutable attributes, this will raise NotImplementedError if changes are detected,
    as StarRocks does not support altering these attributes.
    It will generate multiple AlterOps if there are several different attribute changes.
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
    # Order: immutable attributes first (will raise error if changed), then mutable attributes
    # Immutable attributes (will raise NotImplementedError if changed):
    _compare_mv_definition(upgrade_ops.ops, schema, mv_name, conn_mv, metadata_mv)
    _compare_table_partition(
        upgrade_ops.ops,
        schema,
        mv_name,
        conn_mv_attributes,
        meta_mv_attributes,
        default_value_override=ReflectionMVDefaults.partition_by(),
        support_change_override=AlterMVEnablement.PARTITION_BY,
        ddl_object="MATERIALIZED VIEW",
        object_label="Materialized view",
    )
    _compare_table_distribution(
        upgrade_ops.ops,
        schema,
        mv_name,
        conn_mv_attributes,
        meta_mv_attributes,
        default_value_override=ReflectionMVDefaults.distribution_type(),
        support_change_override=AlterMVEnablement.DISTRIBUTED_BY,
        ddl_object="MATERIALIZED VIEW",
        object_label="Materialized view",
    )
    _compare_table_order_by(
        upgrade_ops.ops,
        schema,
        mv_name,
        conn_mv_attributes,
        meta_mv_attributes,
        default_value_override=ReflectionMVDefaults.order_by(),
        support_change_override=AlterMVEnablement.ORDER_BY,
        ddl_object="MATERIALIZED VIEW",
        object_label="Materialized view",
    )
    _compare_mv_comment(upgrade_ops.ops, schema, mv_name, conn_mv, metadata_mv)

    # Mutable attributes (will generate AlterMaterializedViewOp if changed):
    _compare_mv_refresh(upgrade_ops.ops, schema, mv_name, conn_mv_attributes, meta_mv_attributes)
    run_mode = autogen_context.dialect.run_mode
    _compare_mv_properties(upgrade_ops.ops, schema, mv_name, conn_mv_attributes, meta_mv_attributes, run_mode)

    # Log summary if any operations were generated
    ops_after = len(upgrade_ops.ops)
    if ops_after > ops_before:
        mv_fqn = utils.gen_simple_qualified_name(mv_name, schema)
        num_changes = ops_after - ops_before
        logger.debug("Materialized view %r comparison complete: %d ALTER operation(s) generated", mv_fqn, num_changes)

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
# Table Comparison
# Only starrocks-specific table attributes are compared.
# Other table attributes are compared using generic comparison logic in Alembic.
# ==============================================================================
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

    Raises:
        NotImplementedError: If a change is detected that is not supported in StarRocks.
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

    _compare_table_engine(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _compare_table_key(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    # Note: COMMENT comparison is handled by Alembic's built-in _compare_table_comment
    _compare_table_partition(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _compare_table_distribution(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _compare_table_order_by(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _compare_table_properties(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes, run_mode)

    # Log summary if any operations were generated
    ops_after = len(upgrade_ops.ops)
    if ops_after > ops_before:
        table_fqn = utils.gen_simple_qualified_name(table_name, schema)
        num_changes = ops_after - ops_before
        logger.debug("Table %r comparison complete: %d ALTER operation(s) generated", table_fqn, num_changes)

    return False

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
            raise NotImplementedError(
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

    - If a property is specified in metadata, it is compared with the database.
    - If a property is NOT specified in metadata but exists in the database with a NON-DEFAULT value,
      a change is detected.
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

    normalized_conn = CaseInsensitiveDict(conn_properties)
    normalized_meta = CaseInsensitiveDict(meta_properties)
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

        if effective_conn_str == effective_meta_str:
            # logger.debug("Property no changes. key: %s, effective_conn_str: %s, effective_meta_str: %s", key, effective_conn_str, effective_meta_str)
            continue

        logger.debug("Property changes. key: %s, effective_conn_str: %s, effective_meta_str: %s", key, effective_conn_str, effective_meta_str)
        if meta_value is None:
            if default_value is None:
                # Scenario 1: Implicit deletion of a property with no default.
                if conn_value is not None:
                    logger.warning(
                        f"{object_label} '{full_name}': Property '{key}' exists in the database with value '{conn_value}' "
                        f"but is not specified in metadata and no default is defined in {default_cls.__name__}."
                        f"Implicit deletion is not recommended. "
                        f"To manage this property, please specify it explicitly in your metadata. "
                        f"No ALTER TABLE SET operation will be generated for this property."
                    )
                    continue  # Skip generating an op for this property
            else:
                # Scenario 2: Implicit reset to default.
                logger.warning(
                    f"{object_label} '{full_name}': Property '{key}' has non-default value '{conn_value}' in database "
                    f"but is not specified in metadata. An ALTER TABLE SET operation will be generated to "
                    f"reset it to its default value '{default_value}'. "
                    f"Consider explicitly setting default properties in your metadata to avoid ambiguity."
                )
        # Determine the value for the upgrade operation.
        target_val_upgrade = meta_str if meta_str is not None else default_str
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

        # Determine the value for the downgrade (reverse) operation.
        target_val_downgrade = conn_str if conn_str is not None else default_str
        properties_for_reverse[prop_key] = target_val_downgrade

    return properties_to_set, properties_for_reverse


def _compare_table_properties(
    ops_list: List[AlterTableOp],
    schema: Optional[str],
    table_name: str,
    conn_table_attributes: Dict[str, Any],
    meta_table_attributes: Dict[str, Any],
    run_mode: str,
) -> None:
    properties_to_set, properties_for_reverse = _compare_table_properties_impl(
        schema, table_name, conn_table_attributes, meta_table_attributes, run_mode,
        default_cls=ReflectionTableDefaults, object_label="Table")
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
        NotImplementedError: If support_change=False and a change is detected

    Logic:
        1. If meta specifies value != (conn value or default value) -> change needed
        2. If meta specifies value == (conn value or default value) -> no change
        3. If meta not specified and (conn is None or conn == default) -> no change
        4. If meta not specified and conn != default -> log error, return False (user must decide)
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
                logger.error(error_msg)
                raise NotImplementedError(error_msg)
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
                error_msg = (
                    f"{object_label} {table_fqn!r}: Attribute '{attribute_name}' "
                    f"in database has non-default value '{conn_str}' (default: '{default_str}'), "
                    f"but not specified in metadata. Please specify this attribute explicitly "
                    "in your table definition to avoid unexpected behavior. "
                    "No ALTER statement will be generated automatically."
                )
                logger.error(error_msg)
                raise NotImplementedError(error_msg)  # Don't generate ALTER - user must decide explicitly
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
        raise NotImplementedError(
            f"StarRocks does not support changing the aggregation type of a column: '{cname}', "
            f"from {conn_agg_type} to {meta_agg_type}."
        )

    # we need to set it in the AlterColumnOp, because the KEY/AGG_TYPE is always needed.
    # TODO: But currently, it's not passed to MySQLModifyColumn
    if alter_column_op:
        alter_column_op.kw[ColumnAggInfoKeyWithPrefix.AGG_TYPE] = meta_agg_type


@comparators_dispatch_for_starrocks("column")
def compare_starrocks_column_autoincrement(
    autogen_context: AutogenContext,
    alter_column_op: AlterColumnOp,
    schema: Optional[str],
    tname: Union[quoted_name, str],
    cname: quoted_name,
    conn_col: Column[Any],
    metadata_col: Column[Any],
) -> None:
    """
    Compare StarRocks-specific column options.
    It will run after the  built-in comparator for "column" auto_increment.

    StarRocks does not support changing the autoincrement of a column.
    """
    if conn_col is None or metadata_col is None:
        raise ArgumentError("Both conn column and meta column should not be None.")

    # Because we can't inspect the autoincrement, we can't do the check of difference.
    if conn_col.autoincrement != metadata_col.autoincrement and \
            "auto" != metadata_col.autoincrement:
        table_fqn = utils.gen_simple_qualified_name(tname, schema)
        logger.warning(
            f"Detected AUTO_INCREMENT change on column '{table_fqn}.{cname}'. "
            f"from {conn_col.autoincrement!r} (in database) to {metadata_col.autoincrement!r} (in metadata). "
            f"No ALTER statement will be generated automatically, "
            f"Because we can't inspect the column's autoincrement currently."
        )
    return None

