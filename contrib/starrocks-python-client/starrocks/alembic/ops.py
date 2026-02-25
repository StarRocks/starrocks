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
from typing import Dict, List, Optional, Union

from alembic.operations import Operations, ops
from sqlalchemy import MetaData, Table

from starrocks.common.params import (
    SRKwargsPrefix,
    TableObjectInfoKey,
)
from starrocks.sql.schema import MaterializedView, View, extract_view_columns


logger = logging.getLogger(__name__)


def _extract_starrocks_dialect_kwargs(kwargs: dict) -> dict:
    """
    Return only StarRocks dialect kwargs that start with the SRKwargsPrefix (e.g., 'starrocks_'),
    and normalize the suffix (after the prefix) to lowercase.

    Notes:
    - The 'starrocks_' prefix is required to be lowercase and matched case-sensitively.
    - The attribute part after the prefix is normalized to lowercase, so both
      'starrocks_PARTITION_BY' and 'starrocks_partition_by' are accepted and stored as
      'starrocks_partition_by'.
    """
    return {k.lower(): v for k, v in kwargs.items() if k.startswith(SRKwargsPrefix)}


def _columns_dicts_to_column_objects(columns: Union[List[Dict], None]) -> List:
    """
    Helper function to convert column dicts to Column objects.

    This is used by both CreateViewOp and CreateMaterializedViewOp when
    converting Op to View/MaterializedView objects for toimpl execution.

    Args:
        columns: List of column dicts with 'name' and optional 'comment' keys

    Returns:
        List of Column objects

    Example:
        >>> columns = [{'name': 'id', 'comment': 'ID'}, {'name': 'name'}]
        >>> _columns_dicts_to_column_objects(columns)
        [Column('id', STRING(), comment='ID'), Column('name', STRING())]
    """
    from sqlalchemy import Column

    from starrocks.datatype import STRING

    if not columns:
        return []

    result = []
    for col_dict in columns:
        col = Column(col_dict['name'], STRING())
        if col_dict.get('comment'):
            col.comment = col_dict['comment']
        result.append(col)
    return result


@Operations.register_operation("alter_view")
class AlterViewOp(ops.MigrateOperation):
    """Represent an ALTER VIEW operation."""
    def __init__(
        self,
        view_name: str,
        definition: Optional[str] = None,
        schema: Optional[str] = None,
        columns: Union[List[Dict], None] = None,
        comment: Optional[str] = None,
        security: Optional[str] = None,
        existing_definition: Optional[str] = None,
        existing_columns: Union[List[Dict], None] = None,
        existing_comment: Optional[str] = None,
        existing_security: Optional[str] = None,
    ):
        """
        Definition usually should not be None. But for future, we may want to support Alter View by only comment or security,
        so we keep the definition as Noneable.
        """
        self.view_name = view_name
        self.definition = definition
        self.schema = schema
        self.columns = columns
        self.comment = comment
        self.security = security
        self.existing_definition = existing_definition
        self.existing_columns = existing_columns
        self.existing_comment = existing_comment
        self.existing_security = existing_security

    def to_view(self, metadata: Optional[MetaData] = None) -> "View":
        return View(
            self.view_name,
            MetaData(),
            definition=self.definition or '<not_used_definition>',
            schema=self.schema,
            columns=self.columns,
            comment=self.comment,
            starrocks_security=self.security,
        )

    @classmethod
    def alter_view(
        cls,
        operations: Operations,
        view_name: str,
        definition: Optional[str] = None,
        schema: Optional[str] = None,
        columns: Union[List[Dict], None] = None,
        comment: Optional[str] = None,
        security: Optional[str] = None,
        existing_definition: Optional[str] = None,
        existing_columns: Union[List[Dict], None] = None,
        existing_comment: Optional[str] = None,
        existing_security: Optional[str] = None,
    ):
        """Invoke an ALTER VIEW operation."""
        op = cls(
            view_name,
            definition,
            schema=schema,
            columns=columns,
            comment=comment,
            security=security,
            existing_definition=existing_definition,
            existing_columns=existing_columns,
            existing_comment=existing_comment,
            existing_security=existing_security,
        )
        return operations.invoke(op)

    def reverse(self) -> "AlterViewOp":
        # Reversing an ALTER is another ALTER, using the stored "reverse" attributes.
        logger.debug("reverse AlterViewOp for %s", self.view_name)

        return AlterViewOp(
            self.view_name,
            definition=self.existing_definition,
            schema=self.schema,
            columns=self.existing_columns,
            comment=self.existing_comment,
            security=self.existing_security,
            existing_definition=self.definition,
            existing_columns=self.columns,
            existing_comment=self.comment,
            existing_security=self.security,
        )

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"AlterViewOp(view_name={self.view_name!r}, schema={self.schema!r}, "
            f"definition=({self.definition!r}), columns={self.columns!r}, comment={self.comment!r}, "
            f"security={self.security}, "
            f"existing_definition=({self.existing_definition}), "
            f"existing_columns={self.existing_columns!r}, "
            f"existing_comment={self.existing_comment!r}, "
            f"existing_security={self.existing_security})"
        )


@Operations.register_operation("create_view")
class CreateViewOp(ops.MigrateOperation):
    def __init__(
        self,
        view_name: str,
        definition: str,
        schema: Optional[str] = None,
        columns: Union[List[Dict], None] = None,
        comment: Optional[str] = None,
        or_replace: bool = False,
        if_not_exists: bool = False,
        **kwargs,
    ) -> None:
        self.view_name = view_name
        self.definition = definition
        self.schema = schema
        self.columns = columns
        self.comment = comment
        self.or_replace = or_replace
        self.if_not_exists = if_not_exists
        self.kwargs = _extract_starrocks_dialect_kwargs(kwargs)

    def to_view(self, metadata: Optional[MetaData] = None) -> "View":
        return View(
            self.view_name,
            MetaData(),
            definition=self.definition,
            schema=self.schema,
            columns=self.columns,
            comment=self.comment,
            **self.kwargs,
        )

    @classmethod
    def from_view(cls, view: Table) -> "CreateViewOp":
        """Create Op from a View object (which is a Table)."""
        return cls(
            view.name,
            definition=view.info.get(TableObjectInfoKey.DEFINITION),
            schema=view.schema,
            columns=extract_view_columns(view),
            comment=view.comment,
            **view.dialect_kwargs,
        )

    @classmethod
    def create_view(
        cls,
        operations: Operations,
        view_name: str,
        definition: str,
        schema: Optional[str] = None,
        columns: Union[List[Dict], None] = None,
        comment: Optional[str] = None,
        or_replace: bool = False,
        if_not_exists: bool = False,
        **kwargs,
    ) -> None:
        op = cls(
            view_name,
            definition,
            schema=schema,
            columns=columns,
            comment=comment,
            or_replace=or_replace,
            if_not_exists=if_not_exists,
            **kwargs,
        )
        return operations.invoke(op)

    def reverse(self) -> "DropViewOp":
        # logger.debug("reverse CreateViewOp for %s", self.view_name)
        return DropViewOp(
            self.view_name,
            schema=self.schema,
            **self.kwargs,
        )

    def __str__(self) -> str:
        return (
            f"CreateViewOp(view_name={self.view_name!r}, schema={self.schema!r}, "
            f"definition=({self.definition!r}), columns={self.columns!r}, comment={self.comment!r}, "
            f"or_replace={self.or_replace}, "
            f"if_not_exists={self.if_not_exists}, kwargs=({self.kwargs!r}), "
            f"kwargs={self.kwargs!r}"
        )


@Operations.register_operation("drop_view")
class DropViewOp(ops.MigrateOperation):
    def __init__(
        self,
        view_name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        existing_definition: Optional[str] = None,
        existing_columns: Optional[List[Dict]] = None,
        existing_comment: Optional[str] = None,
        **kwargs,
    ):
        self.view_name = view_name
        self.schema = schema
        self.if_exists = if_exists
        self.existing_definition = existing_definition
        self.existing_columns = existing_columns
        self.existing_comment = existing_comment
        self.kwargs = _extract_starrocks_dialect_kwargs(kwargs)

    def to_view(self, metadata: Optional[MetaData] = None) -> "View":
        """Create a View object for DROP VIEW operation.

        This is used by toimpl to construct the DROP VIEW DDL statement.
        For DROP VIEW, we only need the view name and schema, not the full definition.
        """
        return View(
            self.view_name,
            MetaData(),
            definition='<not_used_definition>',  # Empty definition for DROP VIEW
            schema=self.schema,
            **self.kwargs,
        )

    @classmethod
    def from_view(cls, view: Table) -> "DropViewOp":
        """Create DropViewOp from a View object (which is a Table)."""
        return cls(
            view.name,
            schema=view.schema,
            existing_definition=view.info.get(TableObjectInfoKey.DEFINITION),
            existing_columns=extract_view_columns(view),
            existing_comment=view.comment,
            **view.dialect_kwargs,
        )

    @classmethod
    def drop_view(
        cls,
        operations: Operations,
        view_name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        **kwargs,
    ):
        op = cls(view_name, schema=schema, if_exists=if_exists, **kwargs)
        return operations.invoke(op)

    def reverse(self) -> "CreateViewOp":
        if self.existing_definition is None:
            raise NotImplementedError("Cannot reverse a DropViewOp without the view's definition.")
        op = CreateViewOp(
            self.view_name,
            definition=self.existing_definition,
            schema=self.schema,
            columns=self.existing_columns,
            comment=self.existing_comment,
            **self.kwargs,
        )
        # logger.debug("reverse DropViewOp for %s, with op: (%s)", self.view_name, op)
        return op

    def __str__(self) -> str:
        return (
            f"DropViewOp(view_name={self.view_name!r}, schema={self.schema!r}, "
            f"if_exists={self.if_exists}, existing_definition=({self.existing_definition!r}), "
            f"existing_comment=({self.existing_comment!r}), "
            f"existing_columns={self.existing_columns!r}), "
            f"kwargs=({self.kwargs!r})"
        )


@Operations.register_operation("alter_materialized_view")
class AlterMaterializedViewOp(ops.MigrateOperation):
    """
    Alter a materialized view (mutable attributes only).

    Based on StarRocks ALTER MATERIALIZED VIEW documentation:
    https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW/

    Only supports altering mutable attributes:
    - refresh: ALTER MATERIALIZED VIEW ... REFRESH <new_scheme>
    - properties: ALTER MATERIALIZED VIEW ... SET ("<key>" = "<value>")

    Immutable attributes (definition, partition_by, distributed_by, order_by, comment, columns)
    cannot be altered via ALTER MATERIALIZED VIEW and require DROP + CREATE.

    Currently, we make an Alter including all the mutable attributes, which is different with AlterTableXxxOp.
    """

    def __init__(
        self,
        view_name: str,
        schema: Optional[str] = None,
        # Mutable attributes only
        refresh: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        # Reverse values for downgrade
        existing_refresh: Optional[str] = None,
        existing_properties: Optional[Dict[str, str]] = None,
    ) -> None:
        self.view_name = view_name
        self.schema = schema
        self.refresh = refresh
        self.properties = properties
        self.existing_refresh = existing_refresh
        self.existing_properties = existing_properties

    @classmethod
    def alter_materialized_view(
        cls,
        operations: Operations,
        view_name: str,
        schema: Optional[str] = None,
        refresh: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        existing_refresh: Optional[str] = None,
        existing_properties: Optional[Dict[str, str]] = None,
    ):
        """Invoke an ALTER MATERIALIZED VIEW operation."""
        op = cls(
            view_name,
            schema=schema,
            refresh=refresh,
            properties=properties,
            existing_refresh=existing_refresh,
            existing_properties=existing_properties,
        )
        return operations.invoke(op)

    def reverse(self) -> "AlterMaterializedViewOp":
        return AlterMaterializedViewOp(
            self.view_name,
            schema=self.schema,
            refresh=self.existing_refresh,
            properties=self.existing_properties,
            existing_refresh=self.refresh,
            existing_properties=self.properties,
        )

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"AlterMaterializedViewOp(view_name={self.view_name!r}, schema={self.schema!r}, "
            f"refresh={self.refresh!r}, "
            f"properties={self.properties!r}, "
            f"existing_refresh={self.existing_refresh!r}, "
            f"existing_properties={self.existing_properties!r})"
        )


@Operations.register_operation("create_materialized_view")
class CreateMaterializedViewOp(CreateViewOp):
    """
    Create a materialized view.

    Inherits from CreateViewOp to reuse view_name, definition, schema, comment, columns.
    Adds dialect-specific parameters via kwargs, e.g., starrocks_partition_by.
    """

    def __init__(
        self,
        view_name: str,
        definition: str,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        columns: Union[List[Dict], None] = None,
        or_replace: bool = False,
        if_not_exists: bool = False,
        **kwargs,
    ):
        super().__init__(
            view_name,
            definition,
            schema=schema,
            comment=comment,
            columns=columns,
            or_replace=or_replace,
            if_not_exists=if_not_exists,
            **kwargs,  # Processed by CreateViewOp.__init__ to only keep starrocks_*
        )

    def to_mv(self, metadata: Optional[MetaData] = None) -> "MaterializedView":
        """Convert Op to MaterializedView object (for toimpl)."""
        # Convert columns dict to Column objects
        columns = _columns_dicts_to_column_objects(self.columns)

        return MaterializedView(
            self.view_name,
            metadata or MetaData(),
            *columns,
            definition=self.definition,
            schema=self.schema,
            comment=self.comment,
            **self.kwargs,
        )

    # Keep backward compatibility alias
    def to_materialized_view(self, metadata: Optional[MetaData] = None) -> "MaterializedView":
        """Backward compatibility alias for to_mv()."""
        return self.to_mv(metadata)

    @classmethod
    def from_mv(cls, mv: Table) -> "CreateMaterializedViewOp":
        """Create Op from MaterializedView Table object."""
        columns = extract_view_columns(mv)

        return cls(
            view_name=mv.name,
            definition=mv.info.get(TableObjectInfoKey.DEFINITION),
            schema=mv.schema,
            comment=mv.comment,
            columns=columns,
            **mv.dialect_kwargs,
        )

    # Keep backward compatibility alias
    @classmethod
    def from_materialized_view(cls, mv: Table) -> "CreateMaterializedViewOp":
        """Backward compatibility alias for from_mv()."""
        return cls.from_mv(mv)

    @classmethod
    def create_materialized_view(
        cls,
        operations: Operations,
        view_name: str,
        definition: str,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        columns: Union[List[Dict], None] = None,
        or_replace: bool = False,
        if_not_exists: bool = False,
        **kwargs,
    ):
        """Invoke a CREATE MATERIALIZED VIEW operation."""
        op = cls(
            view_name,
            definition,
            schema=schema,
            comment=comment,
            columns=columns,
            or_replace=or_replace,
            if_not_exists=if_not_exists,
            **kwargs,
        )
        return operations.invoke(op)

    def reverse(self) -> "DropMaterializedViewOp":
        return DropMaterializedViewOp(
            self.view_name,
            schema=self.schema,
        )

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"CreateMaterializedViewOp(view_name={self.view_name!r}, schema={self.schema!r}, "
            f"definition={self.definition!r}, comment={self.comment!r}, columns={self.columns!r}, "
            f"or_replace={self.or_replace}, if_not_exists={self.if_not_exists}, "
            f"kwargs={self.kwargs!r})"
        )


@Operations.register_operation("drop_materialized_view")
class DropMaterializedViewOp(DropViewOp):
    """Drop a materialized view operation."""

    def __init__(
        self,
        view_name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        existing_definition: Optional[str] = None,
        existing_columns: Optional[List[Dict]] = None,
        existing_comment: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        kwargs are already processed by DropViewOp.__init__ to only keep starrocks_*.
        """
        super().__init__(
            view_name,
            schema=schema,
            if_exists=if_exists,
            existing_definition=existing_definition,
            existing_columns=existing_columns,
            existing_comment=existing_comment,
            **kwargs,
        )

    @classmethod
    def from_mv(cls, mv: Table) -> "DropMaterializedViewOp":
        """Create DropMaterializedViewOp from a MaterializedView object."""
        return cls(
            mv.name,
            schema=mv.schema,
            existing_definition=mv.info.get(TableObjectInfoKey.DEFINITION),
            existing_columns=extract_view_columns(mv),
            existing_comment=mv.comment,
            **mv.dialect_kwargs,
        )

    @classmethod
    def from_materialized_view(cls, mv: Table) -> "DropMaterializedViewOp":
        return cls.from_mv(mv)

    @classmethod
    def drop_materialized_view(
        cls,
        operations: Operations,
        view_name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        **kwargs,
    ):
        """Invoke a DROP MATERIALIZED VIEW operation."""
        op = cls(view_name, schema=schema, if_exists=if_exists, **kwargs)
        return operations.invoke(op)

    def reverse(self) -> "CreateMaterializedViewOp":
        if self.existing_definition is None:
            raise NotImplementedError("Cannot reverse a DropMaterializedViewOp without the view's definition.")
        return CreateMaterializedViewOp(
            self.view_name,
            definition=self.existing_definition,
            schema=self.schema,
            comment=self.existing_comment,
            columns=self.existing_columns,
            **self.kwargs,
        )

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"DropMaterializedViewOp(view_name={self.view_name!r}, schema={self.schema!r}, "
            f"if_exists={self.if_exists}, existing_definition={self.existing_definition!r}, "
            f"kwargs={self.kwargs!r})"
        )


# Operation classes ordered according to StarRocks grammar:
# engine → key → (comment) → partition → distribution → order by → properties
@Operations.register_operation("alter_table_engine")
class AlterTableEngineOp(ops.AlterTableOp):
    """Represent an ALTER TABLE ENGINE operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        engine: str,
        schema: Optional[str] = None,
        existing_engine: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.engine = engine
        self.existing_engine = existing_engine

    @classmethod
    def alter_table_engine(
        cls,
        operations: Operations,
        table_name: str,
        engine: str,
        schema: Optional[str] = None,
        existing_engine: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE ENGINE operation for StarRocks."""
        op = cls(table_name, engine, schema=schema, existing_engine=existing_engine)
        return operations.invoke(op)

    def reverse(self) -> AlterTableEngineOp:
        if self.existing_engine is None:
            raise NotImplementedError("Cannot reverse AlterTableEngineOp without existing_engine")
        return AlterTableEngineOp(
            table_name=self.table_name,
            engine=self.existing_engine,
            schema=self.schema,
            existing_engine=self.engine,
        )

    def __str__(self) -> str:
        return (f"AlterTableEngineOp(table_name={self.table_name!r}, "
               f"engine={self.engine!r}, schema={self.schema!r}, "
               f"existing_engine={self.existing_engine!r})")

@Operations.register_operation("alter_table_key")
class AlterTableKeyOp(ops.AlterTableOp):
    """Represent an ALTER TABLE KEY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        key_type: str,
        key_columns: str,
        schema: Optional[str] = None,
        existing_key_type: Optional[str] = None,
        existing_key_columns: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.key_type = key_type
        self.key_columns = key_columns
        self.existing_key_type = existing_key_type
        self.existing_key_columns = existing_key_columns

    @classmethod
    def alter_table_key(
        cls,
        operations: Operations,
        table_name: str,
        key_type: str,
        key_columns: str,
        schema: Optional[str] = None,
        existing_key_type: Optional[str] = None,
        existing_key_columns: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE KEY operation for StarRocks."""
        op = cls(
            table_name,
            key_type,
            key_columns,
            schema=schema,
            existing_key_type=existing_key_type,
            existing_key_columns=existing_key_columns,
        )
        return operations.invoke(op)

    def reverse(self) -> "AlterTableKeyOp":
        if self.existing_key_type is None or self.existing_key_columns is None:
            raise NotImplementedError("Cannot reverse AlterTableKeyOp without existing_key_type and existing_key_columns")
        return AlterTableKeyOp(
            table_name=self.table_name,
            key_type=self.existing_key_type,
            key_columns=self.existing_key_columns,
            schema=self.schema,
            existing_key_type=self.key_type,
            existing_key_columns=self.key_columns,
        )

    def __str__(self) -> str:
        return (f"AlterTableKeyOp(table_name={self.table_name!r}, "
               f"key_type={self.key_type!r}, key_columns={self.key_columns!r}, "
               f"schema={self.schema!r}, existing_key_type={self.existing_key_type!r}, "
               f"existing_key_columns={self.existing_key_columns!r})")

@Operations.register_operation("alter_table_partition")
class AlterTablePartitionOp(ops.AlterTableOp):
    """Represent an ALTER TABLE PARTITION BY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        partition_method: str,
        schema: Optional[str] = None,
        existing_partition_method: Optional[str] = None,
    ):
        """
        Invoke an ALTER TABLE PARTITION BY operation for StarRocks.
        Args:
            table_name: The name of the table.
            partition_method: The method of the partition, such as 'RANGE(dt)', without pre-created partitions.
            schema: The schema of the table.
        """
        super().__init__(table_name, schema=schema)
        self.partition_method = partition_method
        self.existing_partition_method = existing_partition_method

    @property
    def partition_by(self) -> str:
        """
        Get the partition by string for the ALTER TABLE PARTITION BY operation.
        It DOES NOT include the pre-created partitions.
        Because pre-created partitions should be created with ALTER TABLE ADD PARTITION operation.
        """
        return self.partition_method

    @classmethod
    def alter_table_partition(
        cls,
        operations: Operations,
        table_name: str,
        partition_method: str,
        schema: Optional[str] = None,
        existing_partition_method: Optional[str] = None,
    ):
        """
        Invoke an ALTER TABLE PARTITION BY operation for StarRocks.
        The same as __init__ method.
        """
        op = cls(table_name, partition_method, schema=schema, existing_partition_method=existing_partition_method)
        return operations.invoke(op)

    def reverse(self) -> "AlterTablePartitionOp":
        if self.existing_partition_method is None:
            raise NotImplementedError("Cannot reverse AlterTablePartitionOp without existing_partition_method")
        return AlterTablePartitionOp(
            table_name=self.table_name,
            partition_method=self.existing_partition_method,
            schema=self.schema,
            existing_partition_method=self.partition_method,
        )

    def __str__(self) -> str:
        return (f"AlterTablePartitionOp(table_name={self.table_name!r}, "
               f"partition_method={self.partition_method!r}, schema={self.schema!r}, "
               f"existing_partition_method={self.existing_partition_method!r})")

@Operations.register_operation("alter_table_distribution")
class AlterTableDistributionOp(ops.AlterTableOp):
    """Represent an ALTER TABLE DISTRIBUTED BY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        distribution_method: str,
        buckets: Optional[int] = None,
        schema: Optional[str] = None,
        existing_distribution_method: Optional[str] = None,
        existing_buckets: Optional[int] = None,
    ):
        """Invoke an ALTER TABLE DISTRIBUTED BY operation for StarRocks.
        Args:
            table_name: The name of the table.
            distribution_method: The method of the distribution, without BUCKETS.
            buckets: The buckets of the distribution.
            schema: The schema of the table.
        """
        super().__init__(table_name, schema=schema)
        self.distribution_method = distribution_method
        self.buckets = buckets
        self.existing_distribution_method = existing_distribution_method
        self.existing_buckets = existing_buckets

    @property
    def distributed_by(self) -> str:
        """Get the integrated distributed by string for the ALTER TABLE DISTRIBUTED BY operation.
        It includes the BUCKETS if it's not None.
        """
        return f"{self.distribution_method}{f' BUCKETS {self.buckets}' if self.buckets else ''}"

    @classmethod
    def alter_table_distribution(
        cls,
        operations: Operations,
        table_name: str,
        distribution_method: str,
        buckets: Optional[int] = None,
        schema: Optional[str] = None,
        existing_distribution_method: Optional[str] = None,
        existing_buckets: Optional[int] = None,
    ):
        """Invoke an ALTER TABLE DISTRIBUTED BY operation for StarRocks.
        The same as __init__ method.
        """
        op = cls(
            table_name,
            distribution_method,
            buckets,
            schema=schema,
            existing_distribution_method=existing_distribution_method,
            existing_buckets=existing_buckets,
        )
        return operations.invoke(op)

    def reverse(self) -> "AlterTableDistributionOp":
        if self.existing_distribution_method is None:
            raise NotImplementedError("Cannot reverse AlterTableDistributionOp without existing_distribution_method")
        return AlterTableDistributionOp(
            table_name=self.table_name,
            distribution_method=self.existing_distribution_method,
            buckets=self.existing_buckets,
            schema=self.schema,
            existing_distribution_method=self.distribution_method,
            existing_buckets=self.buckets,
        )

    def __str__(self) -> str:
        return (f"AlterTableDistributionOp(table_name={self.table_name!r}, "
               f"distribution_method={self.distribution_method!r}, buckets={self.buckets!r}, "
               f"schema={self.schema!r}, existing_distribution_method={self.existing_distribution_method!r}, "
               f"existing_buckets={self.existing_buckets!r})")


@Operations.register_operation("alter_table_order")
class AlterTableOrderOp(ops.AlterTableOp):
    """Represent an ALTER TABLE ORDER BY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        order_by: Union[str, List[str]],
        schema: Optional[str] = None,
        existing_order_by: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.order_by = order_by
        self.existing_order_by = existing_order_by

    @classmethod
    def alter_table_order(
        cls,
        operations: Operations,
        table_name: str,
        order_by: str,
        schema: Optional[str] = None,
        existing_order_by: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE ORDER BY operation for StarRocks."""
        op = cls(table_name, order_by, schema=schema, existing_order_by=existing_order_by)
        return operations.invoke(op)

    def reverse(self) -> "AlterTableOrderOp":
        if self.existing_order_by is None:
            raise NotImplementedError("Cannot reverse AlterTableOrderOp without existing_order_by")
        return AlterTableOrderOp(
            table_name=self.table_name,
            order_by=self.existing_order_by,
            schema=self.schema,
            existing_order_by=self.order_by,
        )

    def __str__(self) -> str:
        return (f"AlterTableOrderOp(table_name={self.table_name!r}, "
               f"order_by={self.order_by!r}, schema={self.schema!r}, "
               f"existing_order_by={self.existing_order_by!r})")


@Operations.register_operation("alter_table_properties")
class AlterTablePropertiesOp(ops.AlterTableOp):
    """Represent an ALTER TABLE SET (...) operation for StarRocks properties."""

    def __init__(
        self,
        table_name: str,
        properties: Dict[str, str],
        schema: Optional[str] = None,
        existing_properties: Optional[Dict[str, str]] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.properties = properties
        self.existing_properties = existing_properties

    @classmethod
    def alter_table_properties(
        cls,
        operations: Operations,
        table_name: str,
        properties: dict,
        schema: Optional[str] = None,
        existing_properties: Optional[Dict[str, str]] = None,
    ):
        """Invoke an ALTER TABLE SET (...) operation for StarRocks properties."""
        op = cls(table_name, properties, schema=schema, existing_properties=existing_properties)
        return operations.invoke(op)

    def reverse(self) -> "AlterTablePropertiesOp":
        if self.existing_properties is None:
            raise NotImplementedError("Cannot reverse AlterTablePropertiesOp without existing_properties")
        return AlterTablePropertiesOp(
            table_name=self.table_name,
            properties=self.existing_properties,
            schema=self.schema,
            existing_properties=self.properties,
        )

    def __str__(self) -> str:
        return (f"AlterTablePropertiesOp(table_name={self.table_name!r}, "
               f"properties={self.properties!r}, schema={self.schema!r}, "
               f"existing_properties={self.existing_properties!r})")
