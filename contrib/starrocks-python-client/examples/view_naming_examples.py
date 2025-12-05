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

"""Examples demonstrating how to avoid naming conflicts between views and tables
with StarRocks SQLAlchemy dialect (English only)."""

from sqlalchemy import Integer, MetaData, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from starrocks import MaterializedViewMixin, ViewMixin


# Example 1: Use naming conventions to avoid conflicts
def example_naming_convention():
    """Use naming conventions to distinguish views and tables."""

    class Base(DeclarativeBase):
        pass

    # Define table - plain name
    class User(Base):
        __tablename__ = "users"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
        email: Mapped[str] = mapped_column(String(100))

    # Define view - use suffix _view to distinguish
    class UserView(Base, ViewMixin):
        __view_name__ = "users_view"  # not "users"
        __view_comment__ = "user view"

        # Columns in the view
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))

        @classmethod
        def create_view_definition(cls, metadata: MetaData):
            """Create the view definition"""
            users = metadata.tables["users"]

            selectable = select(
                users.c.id,
                users.c.name
            ).where(users.c.id > 0)

            return cls.create_view_definition(metadata, selectable)

    # Define materialized view - use suffix _mv
    class UserStatsMV(Base, MaterializedViewMixin):
        __view_name__ = "users_mv"  # not "users"
        __view_comment__ = "user stats materialized view"
        __refresh_strategy__ = "MANUAL"

        # Columns in MV
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))
        count: Mapped[int] = mapped_column(Integer)

        @classmethod
        def create_view_definition(cls, metadata: MetaData):
            """Create the materialized view definition"""
            users = metadata.tables["users"]

            selectable = select(
                users.c.id,
                users.c.name,
                users.c.id.count().label("count")
            ).group_by(users.c.id, users.c.name)

            return cls.create_materialized_view_definition(metadata, selectable)

    print("Naming convention example:")
    print(f"- table name: {User.__tablename__}")
    print(f"- view name: {UserView.__view_name__}")
    print(f"- materialized view name: {UserStatsMV.__view_name__}")
    print("This avoids naming conflicts.")


# Example 2: Use a different schema to isolate
def example_schema_isolation():
    """Use a different schema to isolate views and tables."""

    class Base(DeclarativeBase):
        pass

    # Table - default schema
    class User(Base):
        __tablename__ = "users"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))

    # View - in schema "views"
    class UserView(Base, ViewMixin):
        __view_name__ = "users"  # can reuse same name
        __view_schema__ = "views"  # in schema "views"
        __view_comment__ = "user view"

        # 定义视图中的列
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))

        @classmethod
        def create_view_definition(cls, metadata: MetaData):
            """Create the view definition"""
            users = metadata.tables["users"]

            selectable = select(
                users.c.id,
                users.c.name
            ).where(users.c.id > 0)

            return cls.create_view_definition(metadata, selectable)

    print("Schema isolation example:")
    print(f"- table: {User.__tablename__} (default schema)")
    print(f"- view: {UserView.__view_schema__}.{UserView.__view_name__}")
    print("Using different schemas avoids conflicts.")


# Example 3: Use class attributes to identify
def example_class_attributes():
    """Use class attributes to identify views and tables."""

    class Base(DeclarativeBase):
        pass

    # Define table
    class User(Base):
        __tablename__ = "users"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))

    # Define view
    class UserView(Base, ViewMixin):
        __view_name__ = "user_view"
        __view_comment__ = "user view"

        # 定义视图中的列
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))

    print("Class attribute example:")
    print(f"- User.__tablename__: {User.__tablename__}")
    print(f"- User.__is_view__: {getattr(User, '__is_view__', False)}")
    print(f"- UserView.__tablename__: {UserView.__tablename__}")
    print(f"- UserView.__is_view__: {UserView.__is_view__}")
    print(f"- UserView.__is_materialized_view__: {UserView.__is_materialized_view__}")
    print("Class attributes clearly distinguish views and tables.")


# Example 4: Explain differences between View and Table
def example_view_vs_table():
    """Explain differences between View and Table."""

    class Base(DeclarativeBase):
        pass

    # Define table
    class User(Base):
        __tablename__ = "users"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))

    # Define view
    class UserView(Base, ViewMixin):
        __view_name__ = "user_view"
        __view_comment__ = "user view"

        # 定义视图中的列
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(50))

    print("View vs Table:")
    print("1. Table class:")
    print(f"   - __tablename__: {User.__tablename__}")
    print(f"   - Registered as table: {User.__tablename__ in Base.metadata.tables}")
    print(f"   - No __is_view__ attribute")
    print()
    print("2. View class:")
    print(f"   - __tablename__: {UserView.__tablename__}")
    print(f"   - Not registered as table: {UserView.__view_name__ in Base.metadata.tables}")
    print(f"   - Has __is_view__: {UserView.__is_view__}")
    print()
    print("3. Objects:")
    print("   - Table class: creates Table objects")
    print("   - View class: creates View objects")


# Example 5: Best practices
def example_best_practices():
    """Show best practices."""

    print("Best practices:")
    print("1. Use naming conventions:")
    print("   - tables: users, orders, products")
    print("   - views: users_view, orders_view, products_view")
    print("   - materialized views: users_mv, orders_mv, products_mv")
    print()
    print("2. Use different schemas:")
    print("   - table: public.users")
    print("   - view: views.users")
    print("   - materialized view: materialized_views.users")
    print()
    print("3. Use descriptive names:")
    print("   - table: user_profiles")
    print("   - view: active_users_view")
    print("   - materialized view: user_statistics_mv")
    print()
    print("4. Understand differences:")
    print("   - View classes don't create Table objects")
    print("   - View classes create View objects")
    print("   - View objects can generate Table objects for querying")


if __name__ == "__main__":
    print("View naming conflict avoidance examples")
    print("=" * 50)
    print()
    example_naming_convention()
    print()
    example_schema_isolation()
    print()
    example_class_attributes()
    print()
    example_view_vs_table()
    print()
    example_best_practices()
