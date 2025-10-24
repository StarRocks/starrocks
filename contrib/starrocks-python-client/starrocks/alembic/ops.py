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


logger = logging.getLogger(__name__)


@Operations.register_operation("alter_view")
class AlterViewOp(ops.MigrateOperation):
    """Represent an ALTER VIEW operation."""
    def __init__(
        self,
        view_name: str,
        definition: str,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        security: Optional[str] = None,
        reverse_view_definition: Optional[str] = None,
        reverse_view_comment: Optional[str] = None,
        reverse_view_security: Optional[str] = None,
    ):
        self.view_name = view_name
        self.definition = definition
        self.schema = schema
        self.comment = comment
        self.security = security
        self.reverse_view_definition = reverse_view_definition
        self.reverse_view_comment = reverse_view_comment
        self.reverse_view_security = reverse_view_security

    @classmethod
    def alter_view(
        cls,
        operations: Operations,
        view_name: str,
        definition: str,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        security: Optional[str] = None,
        reverse_view_definition: Optional[str] = None,
        reverse_view_comment: Optional[str] = None,
        reverse_view_security: Optional[str] = None,
    ):
        """Invoke an ALTER VIEW operation."""
        op = cls(
            view_name,
            definition,
            schema=schema,
            comment=comment,
            security=security,
            reverse_view_definition=reverse_view_definition,
            reverse_view_comment=reverse_view_comment,
            reverse_view_security=reverse_view_security,
        )
        return operations.invoke(op)

    def reverse(self):
        # Reversing an ALTER is another ALTER, using the stored "reverse" attributes.
        logger.debug("reverse AlterViewOp for %s", self.view_name)
        return AlterViewOp(
            self.view_name,
            self.reverse_view_definition,
            schema=self.schema,
            comment=self.reverse_view_comment,
            security=self.reverse_view_security,
            reverse_view_definition=self.definition,
            reverse_view_comment=self.comment,
            reverse_view_security=self.security,
        )


@Operations.register_operation("create_view")
class CreateViewOp(ops.MigrateOperation):
    def __init__(
        self,
        view_name: str,
        definition: str,
        schema: Union[str, None] = None,
        security: Union[str, None] = None,
        comment: Union[str, None] = None,
        or_replace: bool = False,
        if_not_exists: bool = False,
    ) -> None:
        self.view_name = view_name
        self.definition = definition
        self.schema = schema
        self.security = security
        self.comment = comment
        self.or_replace = or_replace
        self.if_not_exists = if_not_exists

    @classmethod
    def create_view(
        cls,
        operations: Operations,
        view_name: str,
        definition: str,
        schema: Union[str, None] = None,
        security: Union[str, None] = None,
        comment: Union[str, None] = None,
        or_replace: bool = False,
        if_not_exists: bool = False,
    ):
        op = cls(
            view_name,
            definition,
            schema=schema,
            security=security,
            comment=comment,
            or_replace=or_replace,
            if_not_exists=if_not_exists,
        )
        return operations.invoke(op)

    def reverse(self) -> ops.MigrateOperation:
        logger.debug("reverse CreateViewOp for %s", self.view_name)
        return DropViewOp(
            self.view_name,
            schema=self.schema,
            _reverse_view_definition=self.definition,
            _reverse_view_comment=self.comment,
            _reverse_view_security=self.security,
        )


@Operations.register_operation("drop_view")
class DropViewOp(ops.MigrateOperation):
    def __init__(
        self,
        view_name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        _reverse_view_definition: Optional[str] = None,
        _reverse_view_comment: Optional[str] = None,
        _reverse_view_security: Optional[str] = None,
    ):
        self.view_name = view_name
        self.schema = schema
        self.if_exists = if_exists
        self._reverse_view_definition = _reverse_view_definition
        self._reverse_view_comment = _reverse_view_comment
        self._reverse_view_security = _reverse_view_security

    @classmethod
    def drop_view(cls, operations: Operations, view_name: str, schema: Optional[str] = None, if_exists: bool = False):
        op = cls(view_name, schema=schema, if_exists=if_exists)
        return operations.invoke(op)

    def reverse(self) -> ops.MigrateOperation:
        if self._reverse_view_definition is None:
            raise NotImplementedError("Cannot reverse a DropViewOp without the view's definition.")
        logger.debug("reverse DropViewOp for %s", self.view_name)
        return CreateViewOp(
            self.view_name,
            self._reverse_view_definition,
            schema=self.schema,
            comment=self._reverse_view_comment,
            security=self._reverse_view_security,
        )


@Operations.register_operation("alter_materialized_view")
class AlterMaterializedViewOp(ops.MigrateOperation):
    def __init__(self, view_name: str, definition: str, properties: Union[dict, None] = None, schema: Union[str, None] = None,
                 reverse_definition: Union[str, None] = None,
                 reverse_properties: Union[dict, None] = None
                 ) -> None:
        self.view_name = view_name
        self.definition = definition
        self.properties = properties
        self.schema = schema
        self.reverse_definition = reverse_definition
        self.reverse_properties = reverse_properties

    @classmethod
    def alter_materialized_view(cls, operations, view_name: str, definition: str, properties: Union[dict, None] = None, schema: Union[str, None] = None,
                                reverse_definition: Union[str, None] = None,
                                reverse_properties: Union[dict, None] = None):
        op = cls(view_name, definition, properties=properties, schema=schema,
                 reverse_definition=reverse_definition,
                 reverse_properties=reverse_properties)
        return operations.invoke(op)

    def reverse(self) -> ops.MigrateOperation:
        return AlterMaterializedViewOp(
            self.view_name,
            definition=self.reverse_definition,
            properties=self.reverse_properties,
            schema=self.schema,
            reverse_definition=self.definition,
            reverse_properties=self.properties
        )


@Operations.register_operation("create_materialized_view")
class CreateMaterializedViewOp(ops.MigrateOperation):
    def __init__(self, view_name: str, definition: str, properties: Union[dict, None] = None,
                 schema: Union[str, None] = None,
                 if_not_exists: bool = False) -> None:
        self.view_name = view_name
        self.definition = definition
        self.properties = properties
        self.schema = schema
        self.if_not_exists = if_not_exists

    @classmethod
    def create_materialized_view(cls, operations, view_name: str, definition: str, properties: Union[dict, None] = None, schema: Union[str, None] = None, if_not_exists: bool = False):
        op = cls(view_name, definition, properties=properties,
                 schema=schema, if_not_exists=if_not_exists)
        return operations.invoke(op)

    def reverse(self) -> ops.MigrateOperation:
        return DropMaterializedViewOp(
            self.view_name,
            schema=self.schema,
            _reverse_definition=self.definition,
            _reverse_properties=self.properties,
        )


@Operations.register_operation("drop_materialized_view")
class DropMaterializedViewOp(ops.MigrateOperation):
    def __init__(
        self,
        view_name: str,
        schema: Union[str, None] = None,
        if_exists: bool = False,
        _reverse_definition: Optional[str] = None,
        _reverse_properties: Optional[Dict[str, str]] = None,
    ) -> None:
        self.view_name = view_name
        self.schema = schema
        self.if_exists = if_exists
        self._reverse_definition = _reverse_definition
        self._reverse_properties = _reverse_properties

    @classmethod
    def drop_materialized_view(cls, operations, view_name: str, schema: Union[str, None] = None, if_exists: bool = False):
        op = cls(view_name, schema=schema, if_exists=if_exists)
        return operations.invoke(op)

    def reverse(self) -> ops.MigrateOperation:
        if self._reverse_definition is None:
            raise NotImplementedError("Cannot reverse a DropMaterializedViewOp without the view's definition and properties.")
        return CreateMaterializedViewOp(
            self.view_name,
            definition=self._reverse_definition,
            properties=self._reverse_properties,
            schema=self.schema,
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
        reverse_engine: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.engine = engine
        self.reverse_engine = reverse_engine

    @classmethod
    def alter_table_engine(
        cls,
        operations: Operations,
        table_name: str,
        engine: str,
        schema: Optional[str] = None,
        reverse_engine: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE ENGINE operation for StarRocks."""
        op = cls(table_name, engine, schema=schema, reverse_engine=reverse_engine)
        return operations.invoke(op)

    def reverse(self) -> AlterTableEngineOp:
        if self.reverse_engine is None:
            raise NotImplementedError("Cannot reverse AlterTableEngineOp without reverse_engine")
        return AlterTableEngineOp(
            table_name=self.table_name,
            engine=self.reverse_engine,
            schema=self.schema,
            reverse_engine=self.engine,
        )


@Operations.register_operation("alter_table_key")
class AlterTableKeyOp(ops.AlterTableOp):
    """Represent an ALTER TABLE KEY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        key_type: str,
        key_columns: str,
        schema: Optional[str] = None,
        reverse_key_type: Optional[str] = None,
        reverse_key_columns: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.key_type = key_type
        self.key_columns = key_columns
        self.reverse_key_type = reverse_key_type
        self.reverse_key_columns = reverse_key_columns

    @classmethod
    def alter_table_key(
        cls,
        operations: Operations,
        table_name: str,
        key_type: str,
        key_columns: str,
        schema: Optional[str] = None,
        reverse_key_type: Optional[str] = None,
        reverse_key_columns: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE KEY operation for StarRocks."""
        op = cls(
            table_name,
            key_type,
            key_columns,
            schema=schema,
            reverse_key_type=reverse_key_type,
            reverse_key_columns=reverse_key_columns,
        )
        return operations.invoke(op)

    def reverse(self) -> "AlterTableKeyOp":
        if self.reverse_key_type is None or self.reverse_key_columns is None:
            raise NotImplementedError("Cannot reverse AlterTableKeyOp without reverse_key_type and reverse_key_columns")
        return AlterTableKeyOp(
            table_name=self.table_name,
            key_type=self.reverse_key_type,
            key_columns=self.reverse_key_columns,
            schema=self.schema,
            reverse_key_type=self.key_type,
            reverse_key_columns=self.key_columns,
        )


@Operations.register_operation("alter_table_partition")
class AlterTablePartitionOp(ops.AlterTableOp):
    """Represent an ALTER TABLE PARTITION BY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        partition_method: str,
        schema: Optional[str] = None,
        reverse_partition_method: Optional[str] = None,
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
        self.reverse_partition_method = reverse_partition_method

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
        reverse_partition_method: Optional[str] = None,
    ):
        """
        Invoke an ALTER TABLE PARTITION BY operation for StarRocks.
        The same as __init__ method.
        """
        op = cls(table_name, partition_method, schema=schema, reverse_partition_method=reverse_partition_method)
        return operations.invoke(op)

    def reverse(self) -> "AlterTablePartitionOp":
        if self.reverse_partition_method is None:
            raise NotImplementedError("Cannot reverse AlterTablePartitionOp without reverse_partition_method")
        return AlterTablePartitionOp(
            table_name=self.table_name,
            partition_method=self.reverse_partition_method,
            schema=self.schema,
            reverse_partition_method=self.partition_method,
        )


@Operations.register_operation("alter_table_distribution")
class AlterTableDistributionOp(ops.AlterTableOp):
    """Represent an ALTER TABLE DISTRIBUTED BY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        distribution_method: str,
        buckets: Optional[int] = None,
        schema: Optional[str] = None,
        reverse_distribution_method: Optional[str] = None,
        reverse_buckets: Optional[int] = None,
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
        self.reverse_distribution_method = reverse_distribution_method
        self.reverse_buckets = reverse_buckets

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
        reverse_distribution_method: Optional[str] = None,
        reverse_buckets: Optional[int] = None,
    ):
        """Invoke an ALTER TABLE DISTRIBUTED BY operation for StarRocks.
        The same as __init__ method.
        """
        op = cls(
            table_name,
            distribution_method,
            buckets,
            schema=schema,
            reverse_distribution_method=reverse_distribution_method,
            reverse_buckets=reverse_buckets,
        )
        return operations.invoke(op)

    def reverse(self) -> "AlterTableDistributionOp":
        if self.reverse_distribution_method is None:
            raise NotImplementedError("Cannot reverse AlterTableDistributionOp without reverse_distribution_method")
        return AlterTableDistributionOp(
            table_name=self.table_name,
            distribution_method=self.reverse_distribution_method,
            buckets=self.reverse_buckets,
            schema=self.schema,
            reverse_distribution_method=self.distribution_method,
            reverse_buckets=self.buckets,
        )


@Operations.register_operation("alter_table_order")
class AlterTableOrderOp(ops.AlterTableOp):
    """Represent an ALTER TABLE ORDER BY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        order_by: Union[str, List[str]],
        schema: Optional[str] = None,
        reverse_order_by: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.order_by = order_by
        self.reverse_order_by = reverse_order_by

    @classmethod
    def alter_table_order(
        cls,
        operations: Operations,
        table_name: str,
        order_by: str,
        schema: Optional[str] = None,
        reverse_order_by: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE ORDER BY operation for StarRocks."""
        op = cls(table_name, order_by, schema=schema, reverse_order_by=reverse_order_by)
        return operations.invoke(op)

    def reverse(self) -> "AlterTableOrderOp":
        if self.reverse_order_by is None:
            raise NotImplementedError("Cannot reverse AlterTableOrderOp without reverse_order_by")
        return AlterTableOrderOp(
            table_name=self.table_name,
            order_by=self.reverse_order_by,
            schema=self.schema,
            reverse_order_by=self.order_by,
        )



@Operations.register_operation("alter_table_properties")
class AlterTablePropertiesOp(ops.AlterTableOp):
    """Represent an ALTER TABLE SET (...) operation for StarRocks properties."""

    def __init__(
        self,
        table_name: str,
        properties: Dict[str, str],
        schema: Optional[str] = None,
        reverse_properties: Optional[Dict[str, str]] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.properties = properties
        self.reverse_properties = reverse_properties

    @classmethod
    def alter_table_properties(
        cls,
        operations: Operations,
        table_name: str,
        properties: dict,
        schema: Optional[str] = None,
        reverse_properties: Optional[Dict[str, str]] = None,
    ):
        """Invoke an ALTER TABLE SET (...) operation for StarRocks properties."""
        op = cls(table_name, properties, schema=schema, reverse_properties=reverse_properties)
        return operations.invoke(op)

    def reverse(self) -> "AlterTablePropertiesOp":
        if self.reverse_properties is None:
            raise NotImplementedError("Cannot reverse AlterTablePropertiesOp without reverse_properties")
        return AlterTablePropertiesOp(
            table_name=self.table_name,
            properties=self.reverse_properties,
            schema=self.schema,
            reverse_properties=self.properties,
        )
