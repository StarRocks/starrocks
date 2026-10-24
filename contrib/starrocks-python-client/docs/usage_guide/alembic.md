# Alembic Integration for Schema Management

Alembic is a database migration tool for SQLAlchemy that allows you to manage changes to your database schema over time. This StarRocks dialect extends Alembic to support automated schema migrations for Tables, Views, and Materialized Views. Views and Materialized Views are supported via autogenerate when you configure `include_object_for_view_mv` (or `combine_include_object`) and `render_item=render_column_type`.

## 1. Create and configure your Alembic project

### Installing Alembic

First, ensure you have Alembic installed. If not, you can install it via pip:

```bash
pip install "alembic>=1.14"
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

### Advanced: Choosing the canonicalization schema for view/MV comparison

On StarRocks versions before 4.0.7 a view definition is stored, and before 4.0.2 an MV definition
is shown, in the engine's own canonical form, which could differ textually from the SQL in the
model (dropped `col AS col` aliases, added parentheses, etc.) but the same semantically. To avoid
reporting a phantom change on every autogenerate, the dialect canonicalizes your model's
definition by round-tripping it through a **temporary view** and comparing the engine's
stored form of both sides.

By default that temporary view is created in the schema of the object being compared, so
the migration user needs privileges to create views in every schema that contains a view or
MV. If your user is locked down, set `starrocks_temp_view_schema` to a single dedicated
schema and grant the required privileges only there.

```python
# alembic/env.py
context.configure(
    # ... your existing parameters (render_item, include_object, etc.) ...
    starrocks_temp_view_schema="__alembic_canon__",  # host the transient comparison view here
)
```

Grant the migration user, on that one schema, the privileges needed for the
create → read-back → drop round-trip (`CREATE VIEW` alone is not enough):

```sql
GRANT CREATE VIEW ON DATABASE __alembic_canon__ TO '<user>';
GRANT SELECT, DROP ON ALL VIEWS IN DATABASE __alembic_canon__ TO '<user>';
```

When `starrocks_temp_view_schema` is unset the previous behaviour is unchanged (the temp
view is created in the compared object's own schema). A view or MV that states its columns
is compared through a temporary view that states them too, so a definition whose SELECT list
repeats a name, as `SELECT t.id, t2.id` does, compares as well. If the temporary view cannot
be created (missing privilege, or a referenced object that does not exist yet), the comparison
falls back to in-process AST/regex normalization. StarRocks keeps
a view's definition as written from 4.0.7, and shows an MV's as written from 4.0.2; from
then on a definition that differs is altered (a view) or recreated (an MV) as usual. Before
those versions the stored definition has table-qualified columns that the normalization
keeps, so a difference only it finds is reported as a warning, and the definition is left as
the database has it, unless the change is certain: the database's definition goes through
the temporary view and only the model's fails, as when it reads a column the revision adds;
the model's definition reads a table, view or materialized view that the same revision
creates; or the database's reads a table, view or materialized view that the revision drops without
creating it again, or a column it drops or renames. The warning quotes the error StarRocks gave the temporary view, and advises the
grants above only for an error they cure: access denied, an unknown database, or a temporary
view the user cannot see. A view created before the
server reached 4.0.7 keeps its rewritten definition until it is altered, so without the
temporary view it gets one `alter_view`.

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

- Choose one or more schemas (StarRocks “databases”) to include using `--schemas` (comma-separated). Empty for all schemas (databases). [Optional]
- Render StarRocks-specific dialect options and table kind (to distinguish `Table`, `View`, and `Materialized View`) with `--options include_dialect_options`. **[Required]**
- Preserve StarRocks-specific types with `--options keep_dialect_types`, so generated code imports and uses the StarRocks types instead of generic SQLAlchemy types. **[Required]**

Examples:

```bash
# Or via the generic --options mechanism
sqlacodegen --schemas mydb1,mydb2 --options include_dialect_options,keep_dialect_types \
  starrocks://root@localhost:9030 > models_all.py
```

Notes:

- The generated models will include StarRocks-specific dialect options and the `info` mapping when present, so objects like Views/MVs and table properties can be round-tripped more reliably.
- You can split the generated file into multiple modules (e.g., `models.py`, `models_view.py`, `models_mv.py`) as needed.
- Review/clean the generated code to align naming, comments, and any project conventions. Especially the `info` and StarRocks-specific parameters as `starrocks_xxx`.
  - For `info`: the `definition` of views and materialized views will be stored here, check the `definition` here whether it's what you defined.

Warning:

- It's recommended to add `--generator tables` flag, when running the sqlacodegen command, to generate Core style model script. Because `sqlacodegen` will **reorder columns** in ORM style (putting all NOT NULL columns ahead).

    ```bash
    sqlacodegen --schemas mydb1,mydb2 \
        --options include-dialect-options,keep-dialect-types \
        --generator tables \
        starrocks://root@localhost:9030 > models_all.py
    ```

- Key columns will be recognized as `NOT NULL` columns. If you want to keep `nullable` for key columns, you need to add `nullable=True` parameter for each key column in the generated model python script **manually**.

    ```python
    Column('abm_id', BIGINT(20), primary_key=True)
    # --> change above one to below one
    Column('abm_id', BIGINT(20), primary_key=True, nullable=True)
    ```

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

        'starrocks_primary_key': 'id',
        'starrocks_engine': 'OLAP',
        'starrocks_partition_by': """RANGE (id) (
                PARTITION p1 VALUES LESS THAN ('100')
            )""",
        'starrocks_distributed_by': 'HASH(id) BUCKETS 10',
        'starrocks_properties': {
            'storage_medium': 'SSD',
            'replication_num': '1'
        }
    }
```

**Note**: All columns that appear in a Primary, Unique or Aggregate Key (`starrocks_primary_key`, `starrocks_unique_key` or `starrocks_aggregate_key`) must also be marked with `primary_key=True` in their `Column(...)` declarations, since reflection reports those keys as the primary key. A Duplicate Key table has no primary key: mark all of its key columns `primary_key=True` only if the ORM must map the table, or none of them.

> In the above example, it the `id` column.

**Note**: A table that names no `starrocks_*_key` gets no key clause from its `primary_key=True` columns, as in earlier versions of the dialect, so StarRocks makes a Duplicate Key table. With `starrocks_infer_primary_key` on, it is created as a Primary Key table on those columns when StarRocks can hold it as one: they are its first columns, in key order, not nullable, of types StarRocks takes in a key, their declared widths fit the 128 bytes StarRocks loads in a row's key, and its partition and HASH distribution columns are among them (the [table guide](tables.md) lists every rule). Otherwise the dialect warns, and StarRocks makes a Duplicate Key table. Set the option where `env.py` makes its engine, as `sqlalchemy.starrocks_infer_primary_key = true` in `alembic.ini` or `?starrocks_infer_primary_key=true` in `sqlalchemy.url`, and in `run_migrations_offline()`'s `dialect_opts`, since offline Alembic reads neither. A Primary Key table replaces a row whose key it holds, and can't alter its key columns later, where a Duplicate Key table can; autogenerate leaves the Duplicate Key tables earlier versions made alone. The Table Type section of the [table guide](tables.md) has the details, and how to map a reflected Duplicate Key table with the ORM.

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

### View Autogenerate Details and Limitations

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
  - Definition changes: a view gets `op.alter_view(...)`. A materialized view, whose definition ALTER cannot change, is dropped and created again (`op.drop_materialized_view(...)` and `op.create_materialized_view(...)`), with a warning that StarRocks refills it.
  - Changes to a materialized view's refresh scheme (ASYNC, MANUAL or the schedule) and properties: emits `op.alter_materialized_view(...)`. A change of the refresh moment (IMMEDIATE or DEFERRED) drops the materialized view and creates it again.
- **Views over what the revision removes:** the downgrade could not alter a view back to a definition that reads a table, view or materialized view the revision drops or renames, or a column it drops or renames, before that is restored. So autogenerate drops such a view before the table operations and creates it again after them (`op.drop_view(...)` and `op.create_view(...)`, in both directions), and warns. StarRocks drops a view's grants with the view, where `ALTER VIEW` keeps them, so grant them again after the upgrade and after the downgrade. A view that reads none of it, such as one over a table that only loses a column the view does not read, is altered after the table operations and keeps its grants.
- **View comment and security:** `ALTER VIEW ... AS` drops the view's comment and StarRocks has no ALTER for it, so `op.alter_view` applies a definition, column or comment change as `CREATE OR REPLACE VIEW` with the full definition, columns and comment. A security change is applied with `ALTER VIEW ... SET SECURITY`. Autogenerate emits an `op.alter_view` that carries all of this.
- **Definition Comparison:** View/MV definition comparison uses normalization: remove identifier backticks, strip comments, collapse whitespace, and compare case-insensitively outside string literals, since StarRocks compares strings by case. But, it's still recommended to give the definition with a good and unified SQL style.

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

        'starrocks_primary_key': 'id',
        'starrocks_engine': 'OLAP',
        'starrocks_partition_by': """RANGE (id) (
                PARTITION p1 VALUES LESS THAN ('100')
            )""",
        'starrocks_distributed_by': 'HASH(id) BUCKETS 10',
        'starrocks_properties': {
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

> **Offline SQL needs the `named` paramstyle**: Alembic's `env.py` template passes `dialect_opts={"paramstyle": "named"}` to `context.configure()` in `run_migrations_offline()`; keep it there. Without it, the dialect writes the SQL for a driver that formats parameters with `%`, so each `%` in DDL text comes out doubled: `date_format(d, '%Y')` in a view's definition is written `date_format(d, '%%Y')`, and a column comment `50%` is written `'50%%'`. A mysql client running the script then stores `%%`. Earlier versions of the dialect doubled the `%` of a comment already, but not of a definition or another clause.

> **The PRIMARY KEY inference offline**: Alembic makes the offline dialect from `dialect_opts` alone, without the URL's query string or `env.py`'s engine options, so with `starrocks_infer_primary_key` on give it there too: `dialect_opts={"paramstyle": "named", "starrocks_infer_primary_key": True}`. Without it, the script creates Duplicate Key tables where the online run creates Primary Key tables. A DDL dump from `create_mock_engine()` reads the URL's query string no more than offline Alembic does; pass the option as a keyword there, `create_mock_engine(url, dump, starrocks_infer_primary_key=True)`.

> **Offline limits of `op.alter_column`**: with no database to read, the SQL written for `--sql` can differ from what the dialect sends when connected.
>
> - StarRocks wants a key column's `KEY`, or a value column's aggregate type, repeated in `MODIFY COLUMN` for a key column of a DUPLICATE KEY or UNIQUE KEY table and a value column of an AGGREGATE KEY table. Connected, the dialect reads it from the table; offline it can't, so the script's statement leaves it out and fails, with "Can not change aggregation type". Add it after the type before you run the script, as in `ALTER TABLE t MODIFY COLUMN k2 VARCHAR(20) KEY NULL` or `ALTER TABLE t MODIFY COLUMN v BIGINT SUM NULL`.
> - `autoincrement=True` needs `existing_autoincrement`: the dialect can't tell whether it would add AUTO_INCREMENT, which StarRocks can't do. `autoincrement=False`, which autogenerate writes for a `primary_key=True, autoincrement=False` column, leaves AUTO_INCREMENT out.
> - StarRocks can't change a column's default, and wants a type change to restate it. Connected, a `server_default` given without `existing_server_default` is compared with the default the dialect reads from the table, as StarRocks keeps both: `DEFAULT NULL` is no default, so `sa.null()` or `sa.text("NULL")` restates a column without one, and from 3.5.11, 4.0.4 and 4.1.0 a BOOLEAN column's `'true'` and `'false'`, in any case, are `'1'` and `'0'`. A changed one raises CompileError. Offline the dialect can't read it, so the statement keeps the `server_default` as given, and StarRocks refuses a changed one with "Can not change default value".

### 4.3 Applying This Migration to Your Database

```bash
alembic upgrade head
```

> **Important Note on Schema Changes:** > **No Transactional DDL:** StarRocks does not support multiple-DDL statements transactional operations. This means that if a migration script fails midway, the changes that were successfully executed will _not_ be automatically rolled back. This can leave your database schema in a partially migrated state, which may require manual intervention to fix.
>
> **Time-Consuming Operations:** StarRocks schema change operations (like `ALTER TABLE ... MODIFY COLUMN`) can be time-consuming. Because one table can have only one ongoing schema change operation at a time, StarRocks does not allow other schema change jobs to be submitted for the same table. The dialect therefore holds back each `ALTER TABLE` that an Alembic operation compiles until the table's running job has ended; see [Waiting for schema changes](#waiting-for-schema-changes-to-reach-a-terminal-state-opt-in).
>
> **Adding/dropping several columns at once:** Each `ALTER TABLE ... ADD/DROP COLUMN` submits an *asynchronous* schema-change job, and only one job may run per table at a time. The dialect waits for the job of the statement before, so one statement per column runs, each after the previous job, and each job rewrites the table's data. With the wait turned off (`starrocks_wait_for_schema_change=False` on the engine), or for SQL sent through `op.execute()`, the second statement is refused while the first job is still running. Enable [coalescing of column changes](#44-coalescing-column-changes-into-a-single-alter-table) so all `ADD`/`DROP COLUMN` for a table are combined into a **single** `ALTER TABLE` (a single job), and optionally have Alembic [wait for each schema change to finish](#waiting-for-schema-changes-to-reach-a-terminal-state-opt-in) so the migration stays in lock-step with the cluster.
>
> **Recommendation:** For potentially slow `ALTER TABLE` operations that are *not* automatically coalesced (e.g. `MODIFY COLUMN`, or property/distribution changes), it is still best to modify only **one column or one table property at a time**. After `autogenerate` creates a migration script, review it. Several such `ALTER` operations for the same table run one after the other, each waiting for the job before it, so the migration takes as long as all their jobs together; split them into separate migration scripts if you want to apply them apart.
>
> For more detailed information on StarRocks table attributes and modification limitations, please refer to the [Tables Usage Guide](tables.md).

### 4.4 Coalescing column changes into a single ALTER TABLE

StarRocks runs each `ALTER TABLE ... ADD/DROP COLUMN` as an **asynchronous** schema-change job and allows only **one in-flight job per table**. Statements sent without a wait, as with `starrocks_wait_for_schema_change=False` or through `op.execute()`, therefore fail as soon as the second statement is submitted while the first job is still running. The dialect's wait, on by default, makes the statements that Alembic's operations compile run one after another instead, as one job per column. Without the wait, they would run like this:

```sql
ALTER TABLE t ADD COLUMN a INT;    -- submits job #1 (async, returns immediately)
ALTER TABLE t DROP COLUMN c;       -- ERROR: table already has a running schema change
ALTER TABLE t ADD COLUMN b INT;    -- never reached
```

StarRocks supports combining all column changes into a **single** statement (hence a single job):

```sql
ALTER TABLE t ADD COLUMN a INT, DROP COLUMN c, ADD COLUMN b INT;
```

The dialect provides a `combine_column_alters` autogenerate rewriter that does this for you. Wire it into `context.configure(...)` in **both** `run_migrations_offline()` and `run_migrations_online()`:

```python
# alembic/env.py
from starrocks.alembic import (
    render_column_type,
    include_object_for_view_mv,
    combine_column_alters,   # <-- add this
)

context.configure(
    # ... other parameters ...
    render_item=render_column_type,
    include_object=include_object_for_view_mv,
    process_revision_directives=combine_column_alters,   # <-- add this
)
```

With the rewriter enabled, autogenerate emits a single combined operation for each table instead of one `op.add_column`/`op.drop_column` per column:

```python
# inside versions/<revision_id>_...py
def upgrade():
    op.alter_table_columns(
        'my_table',
        adds=[sa.Column('a', INTEGER(), nullable=True),
              sa.Column('b', VARCHAR(50), nullable=True)],
        drops=[sa.Column('c')],
    )

def downgrade():
    op.alter_table_columns(
        'my_table',
        adds=[sa.Column('c', INTEGER(), nullable=True)],
        drops=[sa.Column('a'), sa.Column('b')],
    )
```

**Notes and limitations:**

- When the table's sort key (`ORDER BY`) or properties change in the same revision, or its distribution changes to or from one that names an added or dropped column, the adds are combined before that change and the drops after it, so the table gets two statements: the new sort key, distribution or `bloom_filter_columns` may name an added column, StarRocks refuses to drop a column the distribution still names, and it refuses a SET that takes out a column it has already dropped (`Bloom filter index has no change`).
- Only `ADD COLUMN` and `DROP COLUMN` are coalesced. `MODIFY COLUMN` (type/nullability changes) and property/distribution changes remain separate statements. If a table has both a combined add/drop *and* a modify in the same revision, they are still two schema-change jobs, which run one after the other (see below).
- The combined operation compiles to a single `ALTER TABLE`, so its column ordering within one statement does not matter to StarRocks.
- **Known gap — RANGE-distributed tables (StarRocks 4.2 / CelerData 26.2, private preview).** In shared-data mode with the FE config `enable_range_distribution = true`, a table declared with an explicit key type or `ORDER BY` and no `DISTRIBUTED BY` clause is distributed by range. On such a table, StarRocks routes a change that shifts the range sort key — adding a key column, or dropping a sort-key column — to a dedicated rewrite job that it refuses to run alongside any other clause:

  ```
  ERROR 1064: DROP COLUMN that changes the range sort key on a range-distribution
  table can not be combined with other alter operations
  ```

  The rewriter coalesces those changes like any other, so a revision that mixes one of them with a second column change produces a statement the FE rejects — even though the same changes succeed as separate statements. Until this is fixed, on a range-distributed table either keep such a revision to a single column change, or skip the rewriter for it and apply the changes as individual `op.add_column`/`op.drop_column` calls, which the dialect's default wait runs one after another (the routed job is a real data rewrite, so the next statement is only accepted once it has finished). Earlier versions are unaffected: range distribution does not exist before 4.1, and on 4.1 these column changes are rejected outright whether or not they are combined.
- Already-generated migration scripts are not rewritten retroactively; the rewriter only affects new `--autogenerate` runs. You can also call `op.alter_table_columns(...)` by hand.

#### Waiting for schema changes to reach a terminal state (opt-in)

Even with coalescing, `ALTER TABLE` returns *before* the async job finishes, and StarRocks rejects any other change to the table until it has. The dialect therefore holds back a later `ALTER TABLE`, `CREATE INDEX` or `DROP INDEX` that an Alembic operation compiles on the same table until the table's running `COLUMN`, `OPTIMIZE` or `ROLLUP` job has ended, so the operations of one `alembic upgrade` run do not collide. SQL that `op.execute()` sends, as text or as SQLAlchemy's `DDL()`, doesn't wait, so StarRocks refuses it with "A schema change operation is in progress" while the job of an operation before it runs. That wait does not look at how the job ended, and it is bounded by the engine's `starrocks_schema_change_timeout`, 300 seconds by default: see [Handling Time-Consuming ALTER TABLE Operations](tables.md#handling-time-consuming-alter-table-operations). To make your migration pipeline stay 100% in sync with the cluster, opt in to blocking until each column schema change reaches a terminal state (`FINISHED`/`CANCELLED`), and stopping when one is cancelled:

```python
context.configure(
    # ... other parameters ...
    process_revision_directives=combine_column_alters,
    starrocks_wait_for_schema_change=True,             # default: False
    starrocks_schema_change_poll_interval=2.0,         # seconds between polls (default: 2.0)
    starrocks_schema_change_timeout=None,              # seconds; None waits indefinitely
)
```

When enabled, after each column-altering, sort key (`ORDER BY`) or property-setting statement the dialect polls `SHOW ALTER TABLE COLUMN` until the job it started finishes, and after a distribution change it polls `SHOW ALTER TABLE OPTIMIZE`. A statement that starts no job, such as most property changes, does not wait. A `CANCELLED` job or an elapsed timeout raises an error so the migration stops instead of silently drifting from the cluster state. This only applies to online mode (`alembic upgrade`); it is a no-op in offline `--sql` mode.

`op.alter_table_properties` sends one `ALTER TABLE ... SET` per property, because StarRocks accepts one property at a time (the `dynamic_partition.*` properties, and the `binlog_*` ones, may share a statement) and a property such as `bloom_filter_columns` starts a schema-change job. The dialect holds back each statement until the job of the one before it has ended; with the option on, each statement also waits for its own job and stops the migration if it is cancelled.

### 4.5 Verifying No Further Changes (optional)

After you have applied all migrations and your database schema is in sync with your models, running the `autogenerate` command again should produce an empty migration script. This is a good way to verify that your schema is up-to-date.

```bash
alembic revision --autogenerate -m "Verify no changes"
```

If there are no differences between your models and the database, Alembic will report that no changes were detected, both `upgrade()` and `downgrade()` have only a `pass` code.

> Delete the generated empty script after you have checked.

### 4.6 Downgrade the Migration (optional)

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
