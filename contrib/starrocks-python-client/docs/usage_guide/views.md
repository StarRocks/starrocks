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
)

# View with column aliases (simplified styles)
# Style 1: List of column name strings
simple_view_with_col_names = View(
    "simple_view_with_col_names",
    metadata,
    columns=["employee_id", "employee_name"],  # Just column names
    definition="SELECT id, name FROM employees",
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
)

# View from SQLAlchemy Selectable
stmt_for_view = select(employees.c.id, employees.c.name).where(employees.c.id > 100)
active_employees_view = View(
    "active_employees",
    metadata,
    definition=stmt_for_view,
)
```

The `View` object is the primary way to define a database view in your Python code. Once defined, the view is automatically registered with the `metadata.tables` collection.

> Info: `View` is simply a wrapper of `Table`.

### `View` Parameters

- **`name`** (`str`): The name of the view.
- **`metadata`** (`MetaData`): The SQLAlchemy MetaData object to associate with.
- **`*args`** (`Column`): Optional Column objects for explicit column definitions (SQLAlchemy style).
- **`definition`** (`str | Selectable`): **Required.** The `SELECT` statement that forms the view. Can be a raw SQL string or a SQLAlchemy `Selectable` object (e.g., `select()`). A `Selectable` is compiled once, with no server version. A `cast()` to ARRAY, MAP or STRUCT that it leaves out, such as one of text, or of JSON to a MAP, warns, and the view's column has the operand's type; see [Column Data Types](tables.md#column-data-types). Write a single `%`, as in `date_format(d, '%Y')`; see the [note on `%`](tables.md#available-table-level-options) for upgrading from `%%`.
- **`schema`** (`Optional[str]`): The database schema where the view will be created. If not provided, the default schema is used.
- **`comment`** (`Optional[str]`): An optional comment to describe the view's purpose. This comment will be stored in the database.
- **`columns`** (`Optional[List[str | dict]]`): Alternative way to specify column aliases. Three formats supported:
  - List of strings: `['col1', 'col2']` - column names only
  - List of dicts: `[{'name': 'col1', 'comment': 'Comment'}, ...]` - column names with optional comments
  - Not specified: column names inferred from `Column`s if specified in `*args`.
- **`starrocks_properties`**: StarRocks' `CREATE VIEW` takes no properties. A `starrocks_properties` given to a `View` is accepted and not sent, and `properties=` raises `TypeError`.
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
So, we extract the SECURITY info from the view definition now.

### Modifying the SECURITY TYPE

`ALTER VIEW ... SET SECURITY {NONE | INVOKER}` changes the security of an existing view, and `op.alter_view(..., starrocks_security=...)` (or `security=`) emits it. `CREATE OR REPLACE VIEW` keeps the view's existing security whatever it says, so the dialect never relies on it for security.

### ALTER Support and Limitations

- StarRocks has no ALTER for a view's comment, and `ALTER VIEW ... AS` drops it. So a change to the definition, the columns or the comment is applied as `CREATE OR REPLACE VIEW` with the full definition, columns and comment, whenever the migration states a comment or an existing one. A migration that states neither, such as a hand-written `op.alter_view('v', 'SELECT ...')`, is a plain `ALTER VIEW ... AS`, which leaves the view without a comment.
- A security change is a separate `ALTER VIEW ... SET SECURITY` statement. Autogenerate compares security only when the model states `starrocks_security`; a view whose model omits it keeps the security it has.
- A change to the columns alone, a name or a column comment with the same definition, is generated as `op.alter_view` with the unchanged definition and the new columns, which redefines the view as `ALTER VIEW v (columns) AS <definition>` (or `CREATE OR REPLACE VIEW` when the view has a comment).
- A view whose old definition reads what the revision removes, a table, view or materialized view it drops or renames, or a column it drops or renames, is dropped before the table operations and created again after them, so the downgrade can restore it. StarRocks drops a view's grants with the view, where `ALTER VIEW` keeps them, so such a view loses its grants in both directions, and autogenerate warns about it. See [Views over what the revision removes](alembic.md#view-autogenerate-details-and-limitations) in the Alembic guide.

Autogenerate generates an `op.alter_view` for each of these changes. A change it can't make is reported as a warning instead, with no operation for it, and the rest of the revision is still generated; decide such a change explicitly (usually DROP + CREATE). Before StarRocks 4.0.7, a definition change that can't be checked through a temporary view is also only warned about, unless it is certain: see [Choosing the canonicalization schema](alembic.md#advanced-choosing-the-canonicalization-schema-for-viewmv-comparison) in the Alembic guide.
