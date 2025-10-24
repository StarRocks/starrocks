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

from typing import Any, Dict, Optional

from sqlalchemy.sql.ddl import DDLElement

from .schema import MaterializedView, View


# Currently we choose to use __visit_name__ to identify the DDL statement.
# If it's not a good idea, maybe it's not easy to understand, then
# we can use the `compiles` method to identify the DDL statement.


class AlterView(DDLElement):
    """Represents an ALTER VIEW DDL statement."""
    __visit_name__ = "alter_view"
    def __init__(self, element: View) -> None:
        self.element = element


class CreateView(DDLElement):
    """Represents a CREATE VIEW DDL statement."""
    __visit_name__ = "create_view"
    def __init__(self, element: View, or_replace: bool = False, if_not_exists: bool = False) -> None:
        self.element = element
        self.or_replace = or_replace
        self.if_not_exists = if_not_exists
        self.security = element.security


class DropView(DDLElement):
    """Represents a DROP VIEW DDL statement."""
    __visit_name__ = "drop_view"
    def __init__(self, element: View, if_exists: bool = False) -> None:
        self.element = element
        self.if_exists = if_exists


class AlterMaterializedView(DDLElement):
    """Represents an ALTER MATERIALIZED VIEW DDL statement."""
    __visit_name__ = "alter_materialized_view"
    def __init__(self, element: MaterializedView) -> None:
        self.element = element


class CreateMaterializedView(DDLElement):
    """Represents a CREATE MATERIALIZED VIEW DDL statement."""
    __visit_name__ = "create_materialized_view"
    def __init__(self, element: MaterializedView, if_not_exists: bool = False) -> None:
        self.element = element
        self.if_not_exists = if_not_exists


class DropMaterializedView(DDLElement):
    """Represents a DROP MATERIALIZED VIEW DDL statement."""
    __visit_name__ = "drop_materialized_view"
    def __init__(self, element: MaterializedView, if_exists: bool = False) -> None:
        self.element = element
        self.if_exists = if_exists


# DDL classes ordered according to StarRocks grammar:
# engine → key → (comment) → partition → distribution → order by → properties
class AlterTableEngine(DDLElement):
    """Represent an ALTER TABLE ENGINE statement for StarRocks."""

    __visit_name__ = "alter_table_engine"

    def __init__(
        self,
        table_name: str,
        engine: str,
        schema: Optional[str] = None
    ):
        self.table_name = table_name
        self.schema = schema
        self.engine = engine


class AlterTableKey(DDLElement):
    """Represent an ALTER TABLE KEY statement for StarRocks."""

    __visit_name__ = "alter_table_key"

    def __init__(
        self,
        table_name: str,
        key_type: str,
        key_columns: str,
        schema: Optional[str] = None
    ):
        self.table_name = table_name
        self.schema = schema
        self.key_type = key_type  # PRIMARY KEY, UNIQUE KEY, DUPLICATE KEY, etc.
        self.key_columns = key_columns


class AlterTablePartition(DDLElement):
    """Represent an ALTER TABLE PARTITION BY statement for StarRocks."""

    __visit_name__ = "alter_table_partition"

    def __init__(
        self,
        table_name: str,
        partition_by: str,
        schema: Optional[str] = None
    ):
        self.table_name = table_name
        self.schema = schema
        self.partition_by = partition_by


class AlterTableDistribution(DDLElement):
    """Represent an ALTER TABLE DISTRIBUTED BY statement for StarRocks."""

    __visit_name__ = "alter_table_distribution"

    def __init__(
        self,
        table_name: str,
        distribution_method: str,
        buckets: Optional[int] = None,
        schema: Optional[str] = None
    ):
        """
        Invoke an ALTER TABLE DISTRIBUTED BY operation for StarRocks.
        Args:
            table_name: The name of the table.
            distribution_method: The method of the distribution, without BUCKETS.
            buckets: The buckets of the distribution.
            schema: The schema of the table.
        """
        self.table_name = table_name
        self.schema = schema
        self.distribution_method = distribution_method
        self.buckets = buckets


class AlterTableOrder(DDLElement):
    """Represent an ALTER TABLE ORDER BY statement for StarRocks."""

    __visit_name__ = "alter_table_order"

    def __init__(
        self,
        table_name: str,
        order_by: str,
        schema: Optional[str] = None
    ):
        self.table_name = table_name
        self.schema = schema
        self.order_by = order_by


class AlterTableProperties(DDLElement):
    """Represent an ALTER TABLE SET (...) statement for StarRocks properties."""

    __visit_name__ = "alter_table_properties"

    def __init__(
        self,
        table_name: str,
        properties: Dict[str, Any],
        schema: Optional[str] = None
    ):
        self.table_name = table_name
        self.schema = schema
        self.properties = properties
