# Defining and Managing Materialized Views with SQLAlchemy and Alembic

This document explains how to define and manage materialized views in StarRocks using the `sqlalchemy-starrocks` dialect and Alembic.

## Defining a Materialized View

To define a materialized view, you use the `starrocks.sql.schema.MaterializedView` object.

### Syntax

```python
from starrocks.sql.schema import MaterializedView
from sqlalchemy import MetaData

metadata = MetaData()

daily_sales_mv = MaterializedView(
    "daily_sales_mv",
    "SELECT order_date, SUM(price) as total_sales FROM orders GROUP BY order_date",
    schema="analytics",
    properties={
        "PARTITION BY": "order_date",
        "DISTRIBUTED BY": "HASH(order_date) BUCKETS 8",
        "REFRESH": "ASYNC START('2025-01-01 00:00:00') EVERY(INTERVAL 1 DAY)",
        "replication_num": "3"
    },
    metadata=metadata  # Associate the materialized view with the metadata object
)

# You can associate the materialized view with the metadata object,if you don't set it in MaterializedView()
# metadata.info.setdefault('materialized_views', {})[('analytics', 'daily_sales_mv')] = daily_sales_mv
```

The `MaterializedView` object is used to define a materialized view in your Python code.

### `MaterializedView` Parameters

- **`name`** (`str`): The name of the materialized view.
- **`definition`** (`str`): The `SELECT` statement that defines the materialized view.
  > **Note:** This must be a raw string, not a SQLAlchemy `Selectable` object.
- **`schema`** (`Optional[str]`): The database schema where the materialized view will be created.
- **`properties`** (`Optional[dict]`): A dictionary to specify various properties and clauses for the materialized view. This is used to configure settings like the refresh strategy, partitioning, and distribution.

### Configuring Properties

> **Note on Implementation**: The `properties` dictionary is not yet implemented for materialized views. Future versions will likely adopt individual arguments (e.g., `PARTITION_BY`, `REFRESH`) for consistency with table definitions, rather than using a single `properties` dictionary.

The `properties` dictionary is the key to configuring the behavior of your materialized view. Here are some of the common keys you can use:

- **`PARTITION BY`**: A string defining the partitioning strategy (e.g., `"date_trunc('day', order_date)"`).
- **`DISTRIBUTED BY`**: A string defining the distribution strategy (e.g., `"HASH(order_id) BUCKETS 16"`).
- **`REFRESH`**: A string specifying the refresh method (e.g., `"ASYNC"` or `"MANUAL"`).
- Other standard StarRocks properties like `"replication_num"` can also be included.

## Alembic Integration

The `sqlalchemy-starrocks` dialect allows you to manage materialized views within your Alembic migrations. There are two primary workflows: using autogenerate or writing migrations manually.

Alembic's `--autogenerate` feature is supported for materialized views. When you define a `MaterializedView` object and associate it with your `MetaData` object, Alembic will detect it and automatically generate the `create_materialized_view` and `drop_materialized_view` operations in your migration scripts.

### Method 1: Using Autogenerate (Recommended)

This approach is ideal for keeping your materialized view definitions in your Python codebase.

1. **Define a `MaterializedView` Object**: As shown in the syntax example above, define your `MaterializedView` and associate it with your `MetaData` object.
2. **Generate the Migration**: Run `alembic revision --autogenerate -m "add daily_sales_mv"`. Alembic will generate a migration script with the appropriate `op.create_materialized_view()` and `op.drop_materialized_view()` calls.

### Method 2: Manually Writing Migration Scripts

If you prefer to define materialized views directly in migration scripts, you can use the `op.create_materialized_view` operation.

```python
# In your Alembic migration script
from alembic import op
from starrocks.alembic import ops

def upgrade():
    mv = ops.CreateMaterializedViewOp(
        "my_mv",
        "SELECT product_id, SUM(total_sales) as aggregated_sales FROM sales_records GROUP BY product_id",
        properties={"REFRESH": "ASYNC"}
    )
    op.create_materialized_view(mv)

def downgrade():
    op.drop_materialized_view(ops.DropMaterializedViewOp("my_mv"))
```

### Note

No matter whether using `--autogenerate` or manual scripting, you should use `alembic upgrade head --sql` to inspect the raw SQL and verify it carefully.

## Important Considerations

### Temporary Note on Object Registration

Please be aware that the current method for registering a `MaterializedView` with SQLAlchemy's `MetaData` is by using `metadata.add_object(my_mv)`. This approach is a temporary solution and may be subject to change in future versions.
