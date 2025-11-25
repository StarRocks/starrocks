# Defining and Managing Materialized Views with SQLAlchemy and Alembic

This document explains how to define and manage materialized views in StarRocks using the `starrocks-sqlalchemy` dialect and Alembic.

## Defining a Materialized View

To define a materialized view, you use the `starrocks.schema.MaterializedView` object.

### Syntax

```python
from sqlalchemy import func, select, Table, Column
from sqlalchemy.orm import declarative_base
from starrocks import INTEGER, BIGINT, DATE, DECIMAL, DATETIME, VARCHAR
from starrocks.schema import MaterializedView

Base = declarative_base()
metadata = Base.metadata

orders = Table('orders', metadata,
    Column('user_id', BIGINT),
    Column('order_id', BIGINT),
    Column('order_date', DATE),
    Column('created_at', DATETIME),
    Column('amount', INTEGER),
    Column('price', DECIMAL(10, 2)),
    starrocks_partition_by="date_trunc('day', order_date)",
    starrocks_properties={"replication_num": "1"},
)

users = Table('users', metadata,
    Column('user_id', BIGINT),
    Column('user_name', VARCHAR(50)),
    starrocks_properties={"replication_num": "1"},
)

# Basic materialized view
daily_sales_mv = MaterializedView(
    "daily_sales_mv",
    metadata,
    definition="SELECT order_date, SUM(price) as total_sales FROM orders GROUP BY order_date",
    schema="analytics",
    starrocks_partition_by="order_date",
    starrocks_distributed_by="HASH(order_date) BUCKETS 8",
    starrocks_refresh='ASYNC START("2025-01-01 00:00:00") EVERY(INTERVAL 1 DAY)',
    starrocks_properties={"replication_num": "1"}
)

# Materialized view with all options
comprehensive_mv = MaterializedView(
    "user_stats_mv",
    metadata,
    definition="""
        SELECT
            o.user_id,
            u.user_name,
            o.order_date,
            COUNT(*) as order_count,
            SUM(o.amount) as total_amount
        FROM orders as o
        inner JOIN users as u ON o.user_id = u.user_id
        GROUP BY o.user_id, u.user_name, o.order_date
    """,
    schema="analytics",
    starrocks_partition_by="date_trunc('day', order_date)",
    starrocks_distributed_by="HASH(user_id) BUCKETS 10",
    starrocks_refresh='ASYNC START("2025-01-01 00:00:00") EVERY(INTERVAL 1 DAY)',
    starrocks_properties={"replication_num": "1"},
    comment="User order statistics"
)

# Materialized view from SQLAlchemy Selectable
stmt_for_mv = select(
    orders.c.user_id,
    func.count(orders.c.order_id).label('order_count')
).group_by(orders.c.user_id)

mv_from_stmt = MaterializedView(
    "user_order_counts",
    metadata,
    definition=stmt,
    starrocks_refresh="MANUAL",
    starrocks_properties={"replication_num": "1"},
)
```

The `MaterializedView` object is used to define a materialized view in your Python code. Once defined, the materialized view is automatically registered with the `metadata.tables` collection.

> Info: `MaterializedView` is simply a wrapper of `Table`.

### `MaterializedView` Parameters

- **`name`** (`str`): The name of the materialized view.
- **`metadata`** (`MetaData`): The SQLAlchemy MetaData object to associate with. A `MaterailizedView` can be recoganized from `table.info['table_kind']` when you get objects from `metadata.tables`.
- **`*args`** (`Column`): Optional Column objects for explicit column definitions (SQLAlchemy style).
- **`definition`** (`str | Selectable`): **Required.** The `SELECT` statement that defines the materialized view. Can be a raw SQL string or a SQLAlchemy `Selectable` object.
- **`schema`** (`Optional[str]`): The database schema where the materialized view will be created.
- **`comment`** (`Optional[str]`): An optional comment to describe the materialized view's purpose.
- **`starrocks_partition_by`** (`Optional[str]`): Partitioning strategy (e.g., `"order_date"` or `"date_trunc('day', created_at)"`).
- **`starrocks_distributed_by`** (`Optional[str]`): Distribution strategy (e.g., `"HASH(order_id) BUCKETS 16"`).
- **`starrocks_refresh`** (`Optional[str]`): Refresh method - `"ASYNC"`, `"MANUAL"`, or a full refresh schedule specification. Specify it when you want to create an Async Materialized View. Sync Materialized View is not supported now.
- **`starrocks_properties`** (`Optional[dict]`): Additional StarRocks-specific properties (e.g., `{"replication_num": "3"}`).

## Alembic Integration

The `starrocks-sqlalchemy` dialect allows you to manage materialized views within your Alembic migrations. There are two primary workflows: using autogenerate or writing migrations manually.

Alembic's `--autogenerate` feature is supported for materialized views. When you define a `MaterializedView` object and associate it with your `MetaData` object, Alembic will detect it and automatically generate the `create_materialized_view`, `drop_materialized_view`, and `alter_materialized_view` operations in your migration scripts.

### Method 1: Using Autogenerate (Recommended)

This approach is ideal for keeping your materialized view definitions in your Python codebase.

1. **Define a `MaterializedView` Object**: As shown in the syntax example above, define your `MaterializedView` and associate it with your `MetaData` object.
2. **Generate the Migration**: Run `alembic revision --autogenerate -m "add daily_sales_mv"`. Alembic will generate a migration script with the appropriate `op.create_materialized_view()` or `op.drop_materialized_view()` calls, or `op.alter_materialized_view()` when a same-named MV exists with mutable differences.

### Method 2: Manually Writing Migration Scripts

If you prefer to define materialized views directly in migration scripts, you can use the `op.create_materialized_view` operation.

```python
# In your Alembic migration script
from alembic import op

def upgrade():
    op.create_materialized_view(
        "my_mv",
        "SELECT product_id, SUM(total_sales) as aggregated_sales FROM sales_records GROUP BY product_id",
        starrocks_refresh="ASYNC",
    )

def downgrade():
    op.drop_materialized_view("my_mv")
```

### Note

No matter whether using `--autogenerate` or manual scripting, you should use `alembic upgrade head --sql` to inspect the raw SQL and verify it carefully.

## ALTER Support and Limitations

- Only the following attributes can be altered for a Materialized View:
  - **REFRESH**
  - **PROPERTIES**
- The following attributes are not supported to be altered in StarRocks:
  - definition (AS SELECT),
  - comment, columns,
  - partition_by, distributed_by, order_by,
- The following operations not supported by starrocks-sqlalchemy now:
  - rename,
  - activate/deactivate,
  - swap,

Autogenerate will raise errors for immutable changes instead of producing ALTER statements. You should decide explicitly (usually DROP + CREATE) in those cases or with direct SQL statement, such as `engine.connect().execute(text("ALTER MATERIALIZED VIEW ..."))`.
