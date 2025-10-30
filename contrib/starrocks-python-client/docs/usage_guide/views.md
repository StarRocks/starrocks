# Defining and Managing Views with SQLAlchemy and Alembic

This document describes how to define, create, and manage database views in StarRocks using the `sqlalchemy-starrocks` dialect and Alembic.

## Defining a View

To define a view, you use the `starrocks.sql.schema.View` object. This object captures the name, schema, and definition of the view.

### Syntax

```python
from starrocks.sql.schema import View
from sqlalchemy import MetaData

metadata = MetaData()

my_view = View(
    "my_view",
    "SELECT id, name, salary FROM employees WHERE department = 'Sales'",
    schema="my_schema",
    # other optional options
    comment="A view to aggregate sales data by product.",
    columns=["product", "amount", "date"],
    security="INVOKER",
    metadata=metadata  # Associate the view with the metadata object
)

# You can associate the view with the metadata object manually, if you don't set it in View()
# metadata.info.setdefault('views', {})[('my_schema', 'my_view')] = my_view
```

The `View` object is the primary way to define a database view in your Python code.

### `View` Parameters

- **`name`** (`str`): The name of the view.
- **`definition`** (`str`): The `SELECT` statement that forms the view. This is the core logic of your view.
  > **Note:** This must be a raw string, not a SQLAlchemy `Selectable` object.
- **`schema`** (`Optional[str]`): The database schema where the view will be created. If not provided, the default schema is used.
- **`comment`** (`Optional[str]`): An optional comment to describe the view's purpose. This comment will be stored in the database.
- **`columns`** (`Optional[List[str]]`): You can explicitly specify the column names for the view. If omitted, the column names are inferred from the `SELECT` statement.
- **`security`** (`Optional[str]`): Defines the SQL security context. Can be set to `'NONE'` or `'INVOKER'`.
  > **Note**: `'DEFINER'` is not implemented in StarRocks as of v3.5.

## Alembic Integration

The `sqlalchemy-starrocks` dialect allows you to manage views within your Alembic migrations. There are two primary workflows: using autogenerate or writing migrations manually.

Alembic's `--autogenerate` feature is supported for views. When you define a `View` object and associate it with your `MetaData` object, Alembic will detect it and automatically generate the `create_view` and `drop_view` operations in your migration scripts.

### Method 1: Using Autogenerate (Recommended)

This approach is ideal for keeping your view definitions in your Python codebase alongside your table models.

1. **Define a `View` Object**: As shown in the syntax example above, define your `View` and associate it with your `MetaData` object.

2. **Generate the Migration**: Run `alembic revision --autogenerate -m "add detailed_sales_view"`. Alembic will detect the new view and automatically generate a migration script containing `op.create_view()` for the `upgrade` and `op.drop_view()` for the `downgrade`.

### Method 2: Manually Writing Migration Scripts

If you prefer to define views directly in migration scripts, you can use the `op.create_view` operation.

```python
# In your Alembic migration script (e.g., versions/xxxxx_create_my_view.py)
from alembic import op
from starrocks.alembic import ops

def upgrade():
    view = ops.CreateViewOp(
        "my_view",
        "SELECT id, name, salary FROM employees WHERE department = 'Sales'",
        schema="my_schema"
    )
    op.create_view(view)

def downgrade():
    op.drop_view(ops.DropViewOp("my_view", schema="my_schema"))
```

When you run `alembic upgrade head`, Alembic will execute this script to create the view.

### Note

No matter whether using `--autogenerate` to create the `upgrade`/`downgrade` script automatically or manually creating the script, you should use `alembic upgrade head --sql` to see the raw SQL and check it carefully.

## Important Considerations

### Temporary Note on Object Registration

Please be aware that the current method for registering a `View` with SQLAlchemy's `MetaData` is by using `metadata.add_object(my_view)`. This approach is a temporary solution and may be subject to change in future versions as the integration with SQLAlchemy's core reflection mechanisms is improved.

### `information_schema.views` and SECURITY TYPE

Please be aware of the following limitation when working with `information_schema.views`:

- Currently, `information_schema.views` does not return information for views that have their `SECURITY` type set to `INVOKER`.
- Information is only returned for views where the `SECURITY` type is set to `NONE`.

### Modifying the SECURITY TYPE

The `ALTER VIEW` statement in StarRocks **does not** support changing the `SECURITY` type of an existing view. If you need to change the security type, you must drop and recreate the view with the desired `SECURITY` type.
