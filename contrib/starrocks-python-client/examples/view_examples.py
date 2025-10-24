#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Examples demonstrating how to use View and MaterializedView classes
with StarRocks SQLAlchemy dialect.
"""

from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, 
    select, text, event
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from starrocks import (
    View, MaterializedView, ViewMixin, MaterializedViewMixin,
    create_view, create_materialized_view, setup_view_events,
    setup_materialized_view_events
)


# Example 1: Using View and MaterializedView classes directly
def example_direct_usage():
    """Example of using View and MaterializedView classes directly"""
    
    # Create engine and metadata
    engine = create_engine("starrocks+pymysql://user:pass@localhost:9030/db")
    metadata = MetaData()
    
    # Create base tables
    users = Table(
        "users", metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("email", String(100))
    )
    
    orders = Table(
        "orders", metadata,
        Column("id", Integer, primary_key=True),
        Column("user_id", Integer),
        Column("amount", Integer),
        Column("status", String(20))
    )
    
    # Create a view
    user_orders_view = create_view(
        name="user_orders_view",
        metadata=metadata,
        selectable=select(
            users.c.id,
            users.c.name,
            orders.c.amount,
            orders.c.status
        ).select_from(users.join(orders, users.c.id == orders.c.user_id)),
        comment="View showing user orders"
    )
    
    # Create a materialized view
    user_stats_mv = create_materialized_view(
        name="user_stats_mv",
        metadata=metadata,
        selectable=select(
            users.c.id,
            users.c.name,
            orders.c.amount.label("total_amount"),
            orders.c.id.count().label("order_count")
        ).select_from(users.join(orders, users.c.id == orders.c.user_id))
        .group_by(users.c.id, users.c.name),
        comment="Materialized view with user statistics",
        refresh_strategy="MANUAL"
    )
    
    # Setup event listeners for automatic creation
    setup_view_events(metadata, [user_orders_view])
    setup_materialized_view_events(metadata, [user_stats_mv])
    
    # Create all objects
    metadata.create_all(engine)
    
    # Refresh materialized view
    with engine.connect() as conn:
        user_stats_mv.refresh(conn)


# Example 2: Using ViewMixin and MaterializedViewMixin with ORM
def example_orm_usage():
    """Example of using ViewMixin and MaterializedViewMixin with ORM"""
    
    class Base(DeclarativeBase):
        pass
    
    # Define base tables
    class User(Base):
        __tablename__ = "users"
        
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
        email: Mapped[str] = mapped_column(String(100))
    
    class Order(Base):
        __tablename__ = "orders"
        
        id: Mapped[int] = mapped_column(primary_key=True)
        user_id: Mapped[int] = mapped_column(Integer)
        amount: Mapped[int] = mapped_column(Integer)
        status: Mapped[str] = mapped_column(String(20))
    
    # Define a view using ViewMixin
    class UserOrdersView(Base, ViewMixin):
        __view_name__ = "user_orders_view"
        __view_comment__ = "View showing user orders"
        
        # Define columns that will be in the view
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
        amount: Mapped[int] = mapped_column(Integer)
        status: Mapped[str] = mapped_column(String(20))
        
        @classmethod
        def create_view_definition(cls, metadata: MetaData):
            """Create the view definition"""
            users = metadata.tables["users"]
            orders = metadata.tables["orders"]
            
            selectable = select(
                users.c.id,
                users.c.name,
                orders.c.amount,
                orders.c.status
            ).select_from(users.join(orders, users.c.id == orders.c.user_id))
            
            return cls.create_view(metadata, selectable)
    
    # Define a materialized view using MaterializedViewMixin
    class UserStatsMV(Base, MaterializedViewMixin):
        __view_name__ = "user_stats_mv"
        __view_comment__ = "Materialized view with user statistics"
        __refresh_strategy__ = "MANUAL"
        
        # Define columns that will be in the materialized view
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
        total_amount: Mapped[int] = mapped_column(Integer)
        order_count: Mapped[int] = mapped_column(Integer)
        
        @classmethod
        def create_view_definition(cls, metadata: MetaData):
            """Create the materialized view definition"""
            users = metadata.tables["users"]
            orders = metadata.tables["orders"]
            
            selectable = select(
                users.c.id,
                users.c.name,
                orders.c.amount.label("total_amount"),
                orders.c.id.count().label("order_count")
            ).select_from(users.join(orders, users.c.id == orders.c.user_id))
            .group_by(users.c.id, users.c.name)
            
            return cls.create_materialized_view(metadata, selectable)
    
    # Create engine and metadata
    engine = create_engine("starrocks+pymysql://user:pass@localhost:9030/db")
    metadata = MetaData()
    
    # Create base tables
    Base.metadata = metadata
    Base.metadata.create_all(engine)
    
    # Create views
    user_orders_view = UserOrdersView.create_view_definition(metadata)
    user_stats_mv = UserStatsMV.create_view_definition(metadata)
    
    # Setup event listeners
    setup_view_events(metadata, [user_orders_view])
    setup_materialized_view_events(metadata, [user_stats_mv])
    
    # Create views
    metadata.create_all(engine)


# Example 3: Using DDL elements directly
def example_ddl_usage():
    """Example of using DDL elements directly"""
    
    from starrocks import CreateView, DropView, CreateMaterializedView, DropMaterializedView
    
    engine = create_engine("starrocks+pymysql://user:pass@localhost:9030/db")
    
    # Create a simple view
    users = Table("users", MetaData(), Column("id", Integer), Column("name", String(50)))
    
    view_select = select(users.c.id, users.c.name).where(users.c.id > 0)
    
    create_view_ddl = CreateView(
        name="active_users_view",
        selectable=view_select,
        comment="View of active users"
    )
    
    drop_view_ddl = DropView(name="active_users_view")
    
    # Execute DDL
    with engine.connect() as conn:
        conn.execute(create_view_ddl)
        # ... do something with the view ...
        conn.execute(drop_view_ddl)
        conn.commit()


if __name__ == "__main__":
    print("View and MaterializedView Examples")
    print("==================================")
    print()
    print("1. Direct usage example:")
    print("   - Creates views and materialized views directly")
    print("   - Uses event listeners for automatic creation")
    print()
    print("2. ORM usage example:")
    print("   - Uses ViewMixin and MaterializedViewMixin")
    print("   - Integrates with SQLAlchemy ORM")
    print()
    print("3. DDL usage example:")
    print("   - Uses DDL elements directly")
    print("   - Manual control over view creation/dropping")
    print()
    print("Run individual functions to see examples in action.") 