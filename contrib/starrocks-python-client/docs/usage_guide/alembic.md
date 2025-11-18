# Alembic Integration for Schema Management

Alembic is a database migration tool for SQLAlchemy that allows you to manage changes to your database schema over time. This StarRocks dialect extends Alembic to support automated schema migrations for Tables, Views, and Materialized Views. Views and Materialized Views are supported via autogenerate when you configure `include_object_for_view_mv` (or `combine_include_object`) and `render_item=render_column_type`.

## 1. Create and configure your Alembic project

### Installing Alembic

First, ensure you have Alembic installed. If not, you can install it via pip:

```bash
pip install "alembic>=1.16"
```

For more detailed installation instructions, refer to the [Alembic Installation Guide](https://alembic.sqlalchemy.org/en/latest/front.html#installation).

### Initializing an Alembic Project

If you are starting a new project, initialize an Alembic environment in your project directory:

```bash
mkdir my_sr_alembic_project  # your alembic project directory
cd my_sr_alembic_project/
alembic init alembic
```

This command creates a new `alembic` directory with the necessary configuration files and a `versions` directory for your migration scripts. For more information on setting up an Alembic project, see the [Alembic Basic Usage Guide](https://alembic.sqlalchemy.org/en/latest/tutorial.html).

### Configuration

First, configure your Alembic environment.

**In `alembic.ini`:**

Set your `sqlalchemy.url` to point to your StarRocks database.

```ini
[alembic]
# ... other configs
sqlalchemy.url = starrocks://root@localhost:9030/mydatabase
```

**In `alembic/env.py`:**

Set the metadata of you models as the `target_metadata`.

Ensure the StarRocks dialect and Alembic integration is imported and configure `render_item` and `include_object` in both `run_migrations_offline()` and `run_migrations_online()`, so autogenerate recognizes StarRocks column types and properly handles Views/MVs.

```python
# Add these imports at the top of your env.py
from starrocks.alembic import render_column_type, include_object_for_view_mv  # For type rendering and View/MV support
from starrocks.alembic.starrocks import StarRocksImpl  # Ensure impl registered

from myapps import models  # Adjust 'myapps.models' to your actual models file path as defined later
# from myapps import models_view  # Import mv as well

# Set the target metadata to be the metadata of my models
target_metadata = models.Base.metadata

# Set the replication (by using kwargs) for test env if there is only on BE
# Generally, you don't need to set it for production env.
version_table_kwargs = {
    "starrocks_properties": {"replication_num":"1"},
}

# In both run_migrations_offline() and run_migrations_online()
def run_migrations_offline() -> None:
    ...
    context.configure(
        # ... other parameters ...
        render_item=render_column_type,
        include_object=include_object_for_view_mv,
        version_table_kwargs=version_table_kwargs,
    )
    ...

def run_migrations_online() -> None:
    ...
    with connectable.connect() as connection:
        context.configure(
            # ... other parameters ...
            render_item=render_column_type,
            include_object=include_object_for_view_mv,
            version_table_kwargs=version_table_kwargs,
        )
        ...
```

#### Working with multiple schemas

If your project uses multiple schemas (databases in StarRocks or MySQL), you must tell Alembic which schemas/names to include; otherwise, autogenerate may ignore schema-qualified objects.

- Set `include_schemas=True` in both `run_migrations_offline()` and `run_migrations_online()`.
- Provide an `include_name` callback to filter which schemas should be included in both `run_migrations_offline()` and `run_migrations_online()`.
- You can still use `include_object` (or the combined filter shown below) for object-level filtering; `include_name` works at the name/schema level.

> See [Alembic include_schema](https://alembic.sqlalchemy.org/en/latest/api/runtime.html#alembic.runtime.environment.EnvironmentContext.configure.params.include_name) for more information.
>
> **Note**: Make sure you have added `None` in the allowed schemas, where `None` is the current defult schema. And it's also recommended to add the default schema name in the allowed schemas.

Example:

```python
# alembic/env.py
from starrocks.alembic import render_column_type, include_object_for_view_mv
from starrocks.alembic.starrocks import StarRocksImpl
from myapps import models

target_metadata = models.Base.metadata

# Only include these schemas during autogenerate
ALLOWED_SCHEMAS = {None, "my_schema", "another_schema"}

def include_name(name, type_, parent_names):
    if type_ == "schema":
        return name in ALLOWED_SCHEMAS
    return True

def run_migrations_offline() -> None:
    ...
    context.configure(
        # ... other parameters ...
        render_item=render_column_type,
        include_schemas=True,
        include_name=include_name,
        include_object=include_object_for_view_mv,
    )
    ...

def run_migrations_online() -> None:
    ...
    with connectable.connect() as connection:
        context.configure(
            # ... other parameters ...
            render_item=render_column_type,
            include_schemas=True,
            include_name=include_name,
            include_object=include_object_for_view_mv,
        )
        ...
```

### Advanced: Custom Object Filtering

If you need to add custom filtering logic to control which database objects Alembic should process during autogeneration (e.g., excluding temporary tables, test tables, or certain schemas), you can use the `combine_include_object` helper function.

#### Why is this needed?

The `include_object_for_view_mv` callback is required for proper View/MV support. If you want to add your own filtering rules on top of this, you must combine them properly rather than replacing the callback entirely. And it's easy to using `combine_include_object`.

Normally, you only need to set `include_object=include_object_for_view_mv` without further object filtering.

#### Example: Excluding temporary objects

```python
# alembic/env.py
from starrocks.alembic import combine_include_object, include_object_for_view_mv, render_column_type
from starrocks.alembic.starrocks import StarRocksImpl

from myapps import models

target_metadata = models.Base.metadata

# Define your custom filter function
def my_custom_filter(object, name, type_, reflected, compare_to):
    """
    Custom filter to exclude temporary objects.

    Args:
        object: The schema object (may be None for reflected objects)
        name: The name of the object
        type_: The type of object ("table", "column", "index", etc.)
        reflected: Whether the object is from the database (True) or metadata (False)
        compare_to: The corresponding object from the other side (metadata or database)

    Returns:
        True to include the object, False to exclude it
    """
    # Skip any object starting with 'temp_'
    if name.startswith('temp_'):
        return False

    # Skip objects in 'test' schema (if your database has schemas)
    if type_ == "table" and compare_to is not None:
        if getattr(compare_to, 'schema', None) == 'test_sqla':
            return False

    # Include everything else
    return True

# Combine your filter with View/MV filter
combined_filter = combine_include_object(my_custom_filter)

def run_migrations_offline() -> None:
    ...
    context.configure(
        # ... other parameters ...
        render_item=render_column_type,
        include_object=combined_filter,  # Use combined filter
        version_table_kwargs=version_table_kwargs,
    )
    ...

def run_migrations_online() -> None:
    ...
    with connectable.connect() as connection:
        context.configure(
            # ... other parameters ...
            render_item=render_column_type,
            include_object=combined_filter,  # Use combined filter
            version_table_kwargs=version_table_kwargs,
        )
        ...
```

#### Common use cases

1. **Exclude temporary tables**: Filter out tables with specific prefixes or patterns
2. **Exclude test objects**: Skip objects in test schemas or with test names for production env.
3. **Include only specific schemas**: Filter by schema name
4. **Skip system tables**: Exclude internal or system-managed tables

For more details on the `include_object` callback, see [Alembic's documentation on filtering](https://alembic.sqlalchemy.org/en/latest/autogenerate.html#controlling-what-to-be-autogenerated).

## 2. Defining Schema Objects

You define your schema in your Python models file (e.g., `models.py`) using SQLAlchemy's declarative style.

> You can put `models.py` into a directory `myapps` under your alembic project. such as `my_sr_alembic_project/myapps/models.py`.

### Generating models from an existing database using sqlacodegen

If your StarRocks database already contains tables, views, or materialized views, you can bootstrap your models by generating them with `sqlacodegen`. This is particularly useful when introducing Alembic to an existing system.

- Choose one or more schemas (StarRocks “databases”) to include using `--schemas` (comma-separated).
- Preserve StarRocks-specific types with `--options keep-dialect-types`, so generated code imports and uses the StarRocks types instead of generic SQLAlchemy types.

Examples:

```bash
# Or via the generic --options mechanism
sqlacodegen --schemas mydb1,mydb2 --options keep-dialect-types \
  starrocks://root@localhost:9030 > models_all.py
```

Notes:

- The generated models will include StarRocks-specific dialect options and the `info` mapping when present, so objects like Views/MVs and table properties can be round-tripped more reliably.
- You can split the generated file into multiple modules (e.g., `models.py`, `models_view.py`, `models_mv.py`) as needed.
- Review/clean the generated code to align naming, comments, and any project conventions. Especially the `info` and StarRocks-specific parameters as `starrocks_xxx`.
  - For `info`: the `definition` of views and materialized views will be stored here, check the `definition` here whether it's what you defined.

See the full [`sqlacodegen`](https://github.com/agronholm/sqlacodegen) feature set and flags.

### Defining Tables with StarRocks Attributes and Types

Use uppercase types (such as `INTEGER` instead of `Integer`) from `starrocks` and specify StarRocks attributes via `__table_args__` (in ORM style).

```python
# models.py
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from starrocks import INTEGER, VARCHAR

Base = declarative_base()

class MyTable(Base):
    __tablename__ = 'my_table'
    id = Column(INTEGER, primary_key=True)
    name = Column(VARCHAR(50))

    __table_args__ = {
        'comment': 'table comment',

        'starrocks_PRIMARY_KEY': 'id',
        'starrocks_ENGINE': 'OLAP',
        'starrocks_PARTITION_BY': """RANGE (id) (
                PARTITION p1 VALUES LESS THAN ('100')
            )""",
        'starrocks_DISTRIBUTED_BY': 'HASH(id) BUCKETS 10',
        'starrocks_PROPERTIES': {
            'storage_medium': 'SSD',
            'replication_num': '1'
        }
    }
```

**Note**: All columns that appear in a StarRocks key (`starrocks_PRIMARY_KEY`, `starrocks_UNIQUE_KEY`, `starrocks_DUPLICATE_KEY`, or `starrocks_AGGREGATE_KEY`) must also be marked with `primary_key=True` in their `Column(...)` declarations.

> In the above example, it the `id` column.

**Note**: Usage mirrors SQLAlchemy’s patterns (e.g., MySQL), but always import and use uppercase types from `starrocks`.

### Defining Views and Materialized Views

Define Views and Materialized Views using the provided `View` and `MaterializedView` classes. These objects are automatically registered with your `MetaData` object.

```python
# models_view.py
from sqlalchemy import Column, MetaData
from starrocks.schema import View, MaterializedView
from starrocks.datatype import INTEGER, VARCHAR

from . import models

# --- Comprehensive View and Materialized View Definitions ---

# Get the metadata object from the Base
metadata = models.Base.metadata

# Define a View with all supported clauses
my_view = View(
    'my_view',
    metadata,
    Column('user_id', INTEGER),
    Column('user_name', VARCHAR(50)),
    definition="SELECT id, name FROM my_table WHERE id > 50",
    schema='my_schema',
    comment='A sample view with all options.',
    starrocks_security='INVOKER',
)

# Define a View with simplified column aliases
simple_view = View(
    'simple_view',
    metadata,
    definition="SELECT id, name FROM my_table",
    columns=['user_id', 'user_name'],
    schema='my_schema',
    properties={'replication_num': '1'},
)

# Define a Materialized View
my_mv = MaterializedView(
    'my_mv',
    metadata,
    definition="SELECT name, count(*) as cnt FROM my_table GROUP BY name",
    schema='my_schema',
    starrocks_refresh='ASYNC',
    starrocks_properties={'replication_num': '1'},
)
```

## 3. Generating and Applying Migrations

Follow the standard Alembic workflow:

1. **Generate a new revision:**
   Alembic will compare your Python models with the database and generate a migration script.

   ```bash
   alembic revision --autogenerate -m "Create initial tables and views"
   ```

   The generated script (e.g., `versions/<revision_id>_...py`) contains an `upgrade()` function with operations like `op.create_table()` to apply your schema, and a `downgrade()` function to revert it.

2. **Review the script:**
   Check the generated file in your `versions/` directory. It will contain `op.create_table()`, `op.create_view()`, etc.

3. **Apply the migration:**
   Run the `upgrade` command to apply the changes to your StarRocks database.

   ```bash
   alembic upgrade head
   ```

If there is some problems of the generated script (e.g., `versions/<revision_id>_...py`), or some problems of the `models.py`, you should delete the generated script file, and re-run the `--autogenerate` commond above, to re-generate a migration script.

### View Autogenerate Details and Limitations (Invalid for the moment)

When you define `View` or `MaterializedView` objects in your model files (e.g., `models_view.py`), Alembic's autogenerate process will detect them and create the appropriate migration operations, which are similar with Tables.

The generated migration script will contain `op.create_view`, `op.drop_view`, `op.create_materialized_view`, and `op.drop_materialized_view`.

A typical generated snippet for creating a view might look like this:

```python
# inside versions/<revision_id>_...py
def upgrade():
    op.create_view('my_view', 'SELECT id, name FROM my_table WHERE id > 50')

def downgrade():
    op.drop_view('my_view')
```

- **Autogenerate will detect:**
  - New views/MVs in metadata: emits `op.create_view(...)` or `op.create_materialized_view(...)`.
  - Dropped views/MVs from metadata: emits `op.drop_view(...)` or `op.drop_materialized_view(...)`.
  - Definition changes: emits `op.alter_view(...)` or `op.alter_materialized_view(...)`.
- **StarRocks Limitation:** `ALTER VIEW` only supports redefining the `AS SELECT` clause. It does not support changing `COMMENT` or `SECURITY` directly. If only `COMMENT`/`SECURITY` change, no operation is emitted; if the definition also changes, those attributes are ignored and only `ALTER VIEW` is generated.
- **Definition Comparison:** View/MV definition comparison uses normalization: remove identifier backticks, strip comments, collapse whitespace, and compare case-insensitively. But, it's still recommended to give the definition with a good and unified SQL style.

#### Caveats: SQL rewriting and quoted strings

- StarRocks may rewrite SQL for Views/Materialized Views (e.g., implicit casts, function normalization, expression reformatting). Even with normalization, this can still lead to unexpected diffs during autogenerate. When that happens, adjust the `definition` in your metadata to exactly match the definition stored in the database, then rerun autogenerate to avoid noisy diffs.
- Any attributes that contain complex or quoted strings (e.g., `properties`, `comments`, partition expressions) may experience similar discrepancies. If you see unexpected diffs, manually reconcile your model values with the database values.

## 4. Modifying Existing Tables and Applying a New Migration

After your initial migration, you may need to modify your tables. Let's say you want to add a new column to `MyTable`.

First, you would update your `myapps/models.py` to reflect the new schema:

```python
# myapps/models.py (after modification)
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from starrocks import INTEGER, VARCHAR, DATETIME

Base = declarative_base()

class MyTable(Base):
    __tablename__ = 'my_table'
    id = Column(INTEGER, primary_key=True)
    name = Column(VARCHAR(100))
    new_column = Column(DATETIME, nullable=True) # Newly added column

    __table_args__ = {
        'comment': 'A modified table comment', # Modified comment

        'starrocks_PRIMARY_KEY': 'id',
        'starrocks_ENGINE': 'OLAP',
        'starrocks_PARTITION_BY': """RANGE (id) (
                PARTITION p1 VALUES LESS THAN ('100')
            )""",
        'starrocks_DISTRIBUTED_BY': 'HASH(id) BUCKETS 10',
        'starrocks_PROPERTIES': {
            'storage_medium': 'SSD',
            'replication_num': '1'
        }
    }
```

### 4.1 Generating a New Migration Script

```bash
alembic revision --autogenerate -m "Add column and modify comment"
```

Alembic will detect both changes and generate a script with `op.add_column` and `op.alter_table`:

```python
# inside versions/<new_revision_id>_...py
def upgrade():
    op.add_column('my_table', sa.Column('new_column', DATETIME(), nullable=True))
    op.create_table_comment(
        'my_table',
        'A modified table comment',
        existing_comment='table comment',
        schema=None
    )

def downgrade():
    op.create_table_comment(
        'my_table',
        'table comment',
        existing_comment='A modified table comment',
        schema=None
    )
    op.drop_column('my_table', 'new_column')
```

### 4.2 Viewing the Generated SQL (optional)

For users who are more familiar with SQL, it can be helpful to see the exact SQL statements that Alembic will execute before applying a migration. You can do this using the `--sql` flag. This will output the SQL to your console without actually running it against the database.

To see the SQL for all migrations up to the `head`:

```bash
alembic upgrade head --sql
```

You can also see the SQL for changes between two revisions:

```bash
alembic upgrade <current_rev>:<target_rev> --sql
```

Setting `<target_rev>` to be `head`, you can see the SQL for changes to the newest version.

### 4.3 Applying This Migration to Your Database

```bash
alembic upgrade head
```

> **Important Note on Schema Changes:** > **No Transactional DDL:** StarRocks does not support multiple-DDL statements transactional operations. This means that if a migration script fails midway, the changes that were successfully executed will _not_ be automatically rolled back. This can leave your database schema in a partially migrated state, which may require manual intervention to fix.
>
> **Time-Consuming Operations:** StarRocks schema change operations (like `ALTER TABLE ... MODIFY COLUMN`) can be time-consuming. Because one table can have only one ongoing schema change operation at a time, StarRocks does not allow other schema change jobs to be submitted for the same table.
>
> **Recommendation:** For potentially slow `ALTER TABLE` operations, it is recommended to modify only **one column or one table property at a time**. After `autogenerate` creates a migration script, review it. If you see multiple `ALTER` operations for the same table that you suspect might be slow, you should split them into separate migration scripts. (We will try to optimize it in the future.)
>
> For more detailed information on StarRocks table attributes and modification limitations, please refer to the [Tables Usage Guide](docs/usage_guide/tables.md).

### 4.4 Verifying No Further Changes (optional)

After you have applied all migrations and your database schema is in sync with your models, running the `autogenerate` command again should produce an empty migration script. This is a good way to verify that your schema is up-to-date.

```bash
alembic revision --autogenerate -m "Verify no changes"
```

If there are no differences between your models and the database, Alembic will report that no changes were detected, both `upgrade()` and `downgrade()` have only a `pass` code.

> Delete the generated empty script after you have checked.

### 4.5 Downgrade the Migration (optional)

If you discover an issue after applying a migration, you can revert it using the `alembic downgrade` command. To revert the most recent migration, you can use `-1`:

```bash
alembic downgrade -1
```

This will execute the `downgrade()` function in the latest revision file, effectively undoing the changes. You can also specify a target revision to downgrade to. For more details, refer to the [Alembic documentation](https://alembic.sqlalchemy.org/en/latest/tutorial.html#running-our-first-migration).

## 5. Debugging and Logging

To see the raw SQL that the dialect compiles and executes during an Alembic migration, you can configure logging.

Add a logger for `starrocks` in your `alembic.ini` and set the level to `DEBUG`. This will print all generated SQL statements to your console.

### For Alembic commands

```ini
[loggers]
keys = root,sqlalchemy,alembic,starrocks

# ... other loggers

[logger_starrocks]
level = DEBUG
handlers =
qualname = starrocks
```

### For `pytest`

Create a `pytest.ini` file in your project root with the following content, if you want to do some simple tests:

```ini
[pytest]
log_cli = true
log_cli_level = DEBUG
log_cli_format = %(levelname)-5.5s [%(name)s] %(message)s
```
