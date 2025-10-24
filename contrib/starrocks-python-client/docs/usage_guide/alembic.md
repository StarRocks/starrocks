# Alembic Integration for Schema Management

Alembic is a database migration tool for SQLAlchemy that allows you to manage changes to your database schema over time. This StarRocks dialect extends Alembic to support automated schema migrations for Tables, Views, and Materialized Views.

> Views and Materialized Views will be supported in the near future.

### 1. Create and configure your Alembic project

#### Installing Alembic

First, ensure you have Alembic installed. If not, you can install it via pip:

```bash
pip install "alembic>=1.16"
```

For more detailed installation instructions, refer to the [Alembic Installation Guide](https://alembic.sqlalchemy.org/en/latest/front.html#installation).

#### Initializing an Alembic Project

If you are starting a new project, initialize an Alembic environment in your project directory:

```bash
mkdir my_sr_alembic_project  # your alembic project directory
cd my_sr_alembic_project/
alembic init alembic
```

This command creates a new `alembic` directory with the necessary configuration files and a `versions` directory for your migration scripts. For more information on setting up an Alembic project, see the [Alembic Basic Usage Guide](https://alembic.sqlalchemy.org/en/latest/tutorial.html).

#### Configuration

First, configure your Alembic environment.

**In `alembic.ini`:**

Set your `sqlalchemy.url` to point to your StarRocks database.

```ini
[alembic]
# ... other configs
sqlalchemy.url = starrocks://myname:pswd1234@localhost:9030/mydatabase
```

**In `alembic/env.py`:**

Set the metadata of you models as the `target_metadata`.

Ensure the StarRocks dialect and Alembic integration is imported and configure `render_item` in both `run_migrations_offline()` and `run_migrations_online()`, so autogenerate recognizes StarRocks column types.

```python
# Add these imports at the top of your env.py
from starrocks.alembic import render  # For type rendering
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
        render_item=render.render_column_type,
        version_table_kwargs=version_table_kwargs,
    )
    ...

def run_migrations_online() -> None:
    ...
    with connectable.connect() as connection:
        context.configure(
            # ... other parameters ...
            render_item=render.render_column_type,
            version_table_kwargs=version_table_kwargs,
        )
        ...
```

### 2. Defining Schema Objects

You define your schema in your Python models file (e.g., `models.py`) using SQLAlchemy's declarative style.

> You can put `models.py` into a directory `myapps` under your alembic project. such as `my_sr_alembic_project/myapps/models.py`.

#### Defining Tables with StarRocks Attributes and Types

Use uppercase types (such as `INTEGER` instead of `Integer`) from `starrocks` and specify StarRocks attributes via `__table_args__` (in ORM style).

```python
# models.py
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from starrocks import INTEGER, VARCHAR, DATETIME

Base = declarative_base()

class MyTable(Base):
    __tablename__ = 'my_table'
    id = Column(INTEGER, primary_key=True)
    name = Column(VARCHAR(50))

    __table_args__ = {
        'comment': 'table comment',

        'starrocks_PRIMARY_KEY': 'id',
        'starrocks_ENGINE': 'OLAP',
        'starrocks_DISTRIBUTED_BY': 'HASH(id) BUCKETS 10',
        'starrocks_PARTITION_BY': """RANGE (id) (
                PARTITION p1 VALUES LESS THAN ('100')
            )""",
        'starrocks_PROPERTIES': {
            'storage_medium': 'SSD',
            'storage_cooldown_time': '2025-06-04 00:00:00',
            'replication_num': '1'
        }
    }
```

**Note**: All columns that appear in a StarRocks key (`starrocks_PRIMARY_KEY`, `starrocks_UNIQUE_KEY`, `starrocks_DUPLICATE_KEY`, or `starrocks_AGGREGATE_KEY`) must also be marked with `primary_key=True` in their `Column(...)` declarations.

> In the above example, it the `id` column.

**Note**: Usage mirrors SQLAlchemy’s patterns (e.g., MySQL), but always import and use uppercase types from `starrocks`.

#### Defining Views and Materialized Views (Invalid for the moment)

Define Views and Materialized Views using the provided `View` and `MaterializedView` classes. These objects should be associated with your `MetaData` object.

```python
# models_view.py
from sqlalchemy import Column, Integer, String, MetaData
from starrocks.schema import View, MaterializedView

from . import models

# --- Comprehensive View and Materialized View Definitions ---

# Get the metadata object from the Base
metadata = models.Base.metadata

# Define a View with all supported clauses
my_view = View(
    'my_view',
    "SELECT id, name FROM my_table WHERE id > 50",
    metadata,
    schema='my_schema',
    comment='A sample view with all options.',
    columns=['user_id', 'user_name'],
    security='INVOKER',
)

# Define a Materialized View
my_mv = MaterializedView(
    'my_mv',
    "SELECT name, count(1) FROM my_table GROUP BY name",
    metadata,
    properties={'replication_num': '1'},
)
```

### 3. Generating and Applying Migrations

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

#### View Autogenerate Details and Limitations (Invalid for the moment)

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

### 4. Modifying Existing Tables and Applying a New Migration

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
    name = Column(VARCHAR(50))
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

#### 4.1 Generating a New Migration Script

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

#### 4.2 Viewing the Generated SQL (optional)

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

#### 4.3 Applying This Migration to Your Database

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

#### 4.4 Verifying No Further Changes (optional)

After you have applied all migrations and your database schema is in sync with your models, running the `autogenerate` command again should produce an empty migration script. This is a good way to verify that your schema is up-to-date.

```bash
alembic revision --autogenerate -m "Verify no changes"
```

If there are no differences between your models and the database, Alembic will report that no changes were detected, both `upgrade()` and `downgrade()` have only a `pass` code.

> Delete the generated empty script after you have checked.

#### 4.5 Downgrade the Migration (optional)

If you discover an issue after applying a migration, you can revert it using the `alembic downgrade` command. To revert the most recent migration, you can use `-1`:

```bash
alembic downgrade -1
```

This will execute the `downgrade()` function in the latest revision file, effectively undoing the changes. You can also specify a target revision to downgrade to. For more details, refer to the [Alembic documentation](https://alembic.sqlalchemy.org/en/latest/tutorial.html#running-our-first-migration).

### 5. Debugging and Logging

To see the raw SQL that the dialect compiles and executes during an Alembic migration, you can configure logging.

Add a logger for `starrocks` in your `alembic.ini` and set the level to `DEBUG`. This will print all generated SQL statements to your console.

#### For Alembic commands

```ini
[loggers]
keys = root,sqlalchemy,alembic,starrocks

# ... other loggers

[logger_starrocks]
level = DEBUG
handlers =
qualname = starrocks
```

#### For `pytest`

Create a `pytest.ini` file in your project root with the following content, if you want to do some simple tests:

```ini
[pytest]
log_cli = true
log_cli_level = DEBUG
log_cli_format = %(levelname)-5.5s [%(name)s] %(message)s
```
