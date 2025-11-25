# Defining and Managing Views with SQLAlchemy and Alembic

This document describes how to define, create, and manage database views in StarRocks using the `starrocks-sqlalchemy` dialect and Alembic.

## Defining a View

To define a view, you use the `starrocks.schema.View` object. This object captures the name, schema, and definition of the view.

### Syntax

```python
from sqlalchemy import Column, select, Table
from sqlalchemy.orm import declarative_base
from starrocks.schema import View
from starrocks.datatype import INTEGER, VARCHAR

Base = declarative_base()
metadata = Base.metadata

# Base tables (all views must be based on these tables and select only their columns)
employees = Table(
    'employees',
    metadata,
    Column('id', INTEGER),
    Column('name', VARCHAR(50)),
    Column('salary', INTEGER),
    Column('department', VARCHAR(50)),
    starrocks_properties={"replication_num": "1"},
)

sales = Table(
    'sales',
    metadata,
    Column('product', VARCHAR(100)),
    Column('amount', INTEGER),
    Column('date', VARCHAR(50)),
    starrocks_properties={"replication_num": "1"},
)

# Basic view
basic_view = View(
    "basic_view",
    metadata,
    definition="SELECT id, name, salary FROM employees WHERE department = 'Sales'",
    schema="analytics",
    comment="A view for sales employees",
    starrocks_properties={"replication_num": "1"},
)

# View with column definitions (SQLAlchemy style)
comprehensive_view = View(
    "comprehensive_view",
    metadata,
    Column('emp_id', INTEGER),
    Column('emp_name', VARCHAR(50)),
    Column('product', VARCHAR(100)),
    Column('amount', INTEGER),
    Column('date', VARCHAR(50)),
    definition="SELECT e.id AS emp_id, e.name AS emp_name, s.product, s.amount, s.date "
               "FROM employees as e INNER JOIN sales as s ON e.name = s.product",
    schema="analytics",
    comment="A comprehensive two-table join view.",
    starrocks_security="INVOKER",
    starrocks_properties={"replication_num": "1"},
)

# View with column aliases (simplified styles)
# Style 1: List of column name strings
simple_view_with_col_names = View(
    "simple_view_with_col_names",
    metadata,
    columns=["employee_id", "employee_name"],  # Just column names
    definition="SELECT id, name FROM employees",
    starrocks_properties={"replication_num": "1"},
)

# Style 2: List of dicts with name and optional comment
detailed_alias_view = View(
    "detailed_alias_view",
    metadata,
    definition="SELECT id, name FROM employees",
    columns=[
        {"name": "id", "comment": "Employee ID"},
        {"name": "name", "comment": "Employee full name"}
    ],
    starrocks_properties={"replication_num": "1"},
)

# View from SQLAlchemy Selectable
stmt_for_view = select(employees.c.id, employees.c.name).where(employees.c.id > 100)
active_employees_view = View(
    "active_employees",
    metadata,
    definition=stmt,
    starrocks_properties={"replication_num": "1"},
)
```

The `View` object is the primary way to define a database view in your Python code. Once defined, the view is automatically registered with the `metadata.tables` collection.

> Info: `View` is simply a wrapper of `Table`.

### `View` Parameters

- **`name`** (`str`): The name of the view.
- **`metadata`** (`MetaData`): The SQLAlchemy MetaData object to associate with.
- **`*args`** (`Column`): Optional Column objects for explicit column definitions (SQLAlchemy style).
- **`definition`** (`str | Selectable`): **Required.** The `SELECT` statement that forms the view. Can be a raw SQL string or a SQLAlchemy `Selectable` object (e.g., `select()`).
- **`schema`** (`Optional[str]`): The database schema where the view will be created. If not provided, the default schema is used.
- **`comment`** (`Optional[str]`): An optional comment to describe the view's purpose. This comment will be stored in the database.
- **`columns`** (`Optional[List[str | dict]]`): Alternative way to specify column aliases. Three formats supported:
  - List of strings: `['col1', 'col2']` - column names only
  - List of dicts: `[{'name': 'col1', 'comment': 'Comment'}, ...]` - column names with optional comments
  - Not specified: column names inferred from `Column`s if specified in `*args`.
- **`starrocks_security`** (`Optional[str]`): Defines the SQL security context. Can be set to `'INVOKER'` or `'NONE'`.
  > **Note**: StarRocks does **not** support `'DEFINER'`. Only `'INVOKER'` and `'NONE'` are supported now.

## Alembic Integration

The `starrocks-sqlalchemy` dialect allows you to manage views within your Alembic migrations. There are two primary workflows: using autogenerate or writing migrations manually.

Alembic's `--autogenerate` feature is supported for views. When you define a `View` object and associate it with your `MetaData` object, Alembic will detect it and automatically generate the `create_view` and `drop_view` operations in your migration scripts.

### Method 1: Using Autogenerate (Recommended)

This approach is ideal for keeping your view definitions in your Python codebase alongside your table models.

1. **Define a `View` Object**: As shown in the syntax example above, define your `View` and associate it with your `MetaData` object.

2. **Generate the Migration**: Run `alembic revision --autogenerate -m "add detailed_sales_view"`. Alembic will detect the new view and automatically generate a migration script containing `op.create_view()` for the `upgrade` and `op.drop_view()` for the `downgrade`, or `op.alter_view()` when a same-named view exists with mutable differences.

### Method 2: Manually Writing Migration Scripts

If you prefer to define views directly in migration scripts, you can use the `op.create_view` operation.

```python
# In your Alembic migration script (e.g., versions/xxxxx_create_my_view.py)
from alembic import op
from starrocks.alembic import ops

def upgrade():
    op.create_view(
        "basic_view",
        "SELECT id, name, salary FROM employees WHERE department = 'Sales'",
        schema="my_schema"
    )

def downgrade():
    op.drop_view("basic_view", schema="my_schema")
```

When you run `alembic upgrade head`, Alembic will execute this script to create the view.

### Note

No matter whether using `--autogenerate` to create the `upgrade`/`downgrade` script automatically or manually creating the script, you should use `alembic upgrade head --sql` to see the raw SQL and check it carefully.

## Important Considerations

### `information_schema.views` and SECURITY TYPE

Please be aware of the following limitation when working with `information_schema.views`:

Currently, `information_schema.views` does not return information for views that have their `SECURITY` type set to `INVOKER`.
So, we extract the SECURITY info from MV definition now.

### Modifying the SECURITY TYPE

The `ALTER VIEW` statement in StarRocks **does not** support changing the `SECURITY` type of an existing view. If you need to change the security type, you must drop and recreate the view with the desired `SECURITY` type.

### ALTER Support and Limitations

- Only the following attribute can be altered for a View:
  - **definition** (the SELECT statement). Column aliases change is allowed only together with definition.
- The following attributes are not supported to be altered by StarRocks:
  - comment
  - security
  - columns-only change (without definition change)

Autogenerate will warn or raise for immutable changes instead of producing ALTER statements. Decide explicitly (usually DROP + CREATE) for those cases.
