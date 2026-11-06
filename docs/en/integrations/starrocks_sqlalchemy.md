---
sidebar_position: 120
displayed_sidebar: docs
description: "How to manage StarRocks schemas using SQLAlchemy, Alembic, and sqlacodegen for schema migration."
sidebar_label: Schema Migration
---

# Schema Management and Migration with SQLAlchemy and Alembic

This guide introduces how to manage StarRocks schemas using the Python ecosystem — including SQLAlchemy, Alembic, and sqlacodegen — through the **`starrocks` SQLAlchemy** dialect. It is designed to help you understand **why schema migration is useful** and **how to use it effectively with StarRocks**.

## Overview

Many users manage StarRocks tables, views, and materialized views using SQL DDL directly. However, as projects grow, manually maintaining `ALTER TABLE` statements becomes error-prone and hard to track.

The **StarRocks SQLAlchemy dialect (`starrocks`)** provides:

- A full SQLAlchemy model layer for StarRocks **tables**, **views**, and **materialized views**
- **Declarative** definitions for table schema and table properties (including views and materialized views)
- Integration with **Alembic** to allow schema changes to be **detected** and **generated** automatically
- Compatibility with tools like **sqlacodegen** for reverse-generating models

This allows Python users to maintain StarRocks schemas in a **declarative**, **version-controlled**, and **automated** way.

## Key benefits

Although schema migration is traditionally associated with OLTP databases, it is also valuable in data warehousing systems such as StarRocks. Teams use [Alembic](https://alembic.sqlalchemy.org/) together with the StarRocks dialect because of the benefits listed below.

### Declarative schema definition

Once you define schema in Python [ORM](https://docs.sqlalchemy.org/en/20/orm/quickstart.html#orm-quickstart) models or [SQLAlchemy](https://docs.sqlalchemy.org) core style, writing `ALTER TABLE` statements manually is no longer required.

### Automatic diffing and autogeneration

Alembic compares **current StarRocks schema** with **your SQLAlchemy models**, and generates migration scripts automatically (`CREATE`/`DROP`/`ALTER`).

### Reviewable, version-controlled migrations

Each schema change becomes a migration file (Python), so users can track changes and roll back if needed.

### Consistent workflow across environments

Schema changes can be applied to development, staging, and production with the same process.

## Installation and Connection

### Prerequisites

- StarRocks: 3.3.3 or later. View `starrocks_security` needs 3.4.1, and `VARIANT` columns need 4.1; on an older server the dialect refuses them with a `CompileError`. StarRocks transactions, through `isolation_level="READ COMMITTED"` or a `BEGIN` sent as text, need 3.5, and the session variable `enable_sql_transaction` on where StarRocks has it, from 4.0.7 and 4.1.0; otherwise the dialect refuses them with an `InvalidRequestError`.
- StarRocks Python client: 1.4.0 or later. Earlier clients lack the version checks above and some of the features on this page.
- `SQLAlchemy`: 1.4 or later. Views, materialized views, the `starrocks+asyncmy` driver and `sqlacodegen` need SQLAlchemy 2.0. On SQLAlchemy 1.4, Alembic autogenerate compares tables and leaves the database's views and materialized views alone.
- `Alembic`: 1.14 or later

### Installing StarRocks Python client

Run the following command to install the StarRocks Python client.

```bash
pip install starrocks
```

### Connecting to StarRocks

Connect to your StarRocks cluster using the following URL.

```bash
starrocks://<user>:<password>@<FE_host>:<query_port>/[<catalog>.]<database>
```

- `user`: Username used to connect to the cluster.
- `password`: User password.
- `FE_host`: FE IP address.
- `query_port`: FE `query_port` (Default: 9030).
- `catalog`: The name of the catalog where your database locates.
- `database`: The name of the database you want to connect.

After installation, you can quickly validate connectivity using the following code example:

```python
from sqlalchemy import create_engine, text

# you need to create `mydatabase` first
engine = create_engine("starrocks://root@localhost:9030/mydatabase")

with engine.connect() as conn:
    conn.execute(text("SELECT 1")).fetchall()
    print("Connection successful!")
```

## Defining StarRocks Models (Declarative ORM)

The StarRocks dialect supports:

- Tables
- Views
- Materialized Views

It also supports StarRocks-specific table attributes such as:

- `ENGINE` (OLAP)
- Key models (`DUPLICATE KEY`, `PRIMARY KEY`, `UNIQUE KEY`, `AGGREGATE KEY`)
- `PARTITION BY` variants (RANGE / LIST / Expression partitioning)
- `DISTRIBUTED BY` variants (HASH / RANDOM)
- `ORDER BY`
- Table properties (e.g., `replication_num`, `storage_medium`)

:::important
- StarRocks dialect options are passed as keyword arguments prefixed with `starrocks_`.
- The `starrocks_` **prefix must be lowercase**. The suffix is accepted in either case (for example, `PRIMARY_KEY` and `primary_key`).
- If you specify a `PRIMARY KEY`, `UNIQUE KEY` or `AGGREGATE KEY` (e.g. `starrocks_primary_key="id"`), also mark its columns with `primary_key=True` in `Column(...)`: reflection reports the key of those tables as the primary key, so the model then matches the database. A `DUPLICATE KEY` table has no primary key: mark all of its key columns `primary_key=True` only if the ORM must map the table, or none of them.
:::

A table that names no table key but has `primary_key=True` columns gets no key clause by default, as in earlier versions of the dialect, so StarRocks creates a `DUPLICATE KEY` table, which keeps every row loaded into it. `create_engine(..., starrocks_infer_primary_key=True)`, or `?starrocks_infer_primary_key=true` in the URL, creates such a table as a `PRIMARY KEY` table on those columns when StarRocks can hold it as one; Alembic's offline mode takes the option in `context.configure(dialect_opts=...)`. Among other rules, which the table guide ([Reference [4]](#references)) lists, the key columns come first, in key order, are not nullable, are `BOOLEAN`, integer, `DATE`, `DATETIME` or `VARCHAR` columns, and fit the 128 bytes StarRocks loads in a row's key, and the table is distributed by `HASH` of key columns or by default, not `RANDOM`, and partitioned by key columns only, so a `Uuid`, `DECIMAL`, `String(255)` or `Text` key makes none. When StarRocks can't hold the table as one, the dialect warns, saying what to change, and sends no key clause, so StarRocks creates a `DUPLICATE KEY` table. A `PRIMARY KEY` table replaces a row whose key it already holds, and its key columns can't be altered later: StarRocks takes only a wider `VARCHAR` from 4.1.1 and a new comment from 4.1.3. With the option on, autogenerate compares such a model's key only with a `PRIMARY KEY` table; with it off, it leaves every table's key alone.

:::note Upgrading from an earlier version of the dialect
- By default a table that names no table key still gets no key clause, so a model with `primary_key=True` columns makes a `DUPLICATE KEY` table, as before. With `starrocks_infer_primary_key=True`, a new database built from the same model, or from the same migration scripts, gets `PRIMARY KEY` tables where StarRocks can hold one, while the tables earlier versions created stay `DUPLICATE KEY` tables, which autogenerate leaves as they are. A `PRIMARY KEY` table replaces a row whose key it already holds, refuses `NULL` keys, and can't alter its key columns later, so a migration that alters a key column can then run on an old database and fail on a new one.
- To keep a table a `DUPLICATE KEY` table with the option on, name its key as `SHOW CREATE TABLE` reports it, for example `starrocks_duplicate_key="id, name"`. The columns marked `primary_key=True` must then be exactly the key's columns, or none of them.
- A `DUPLICATE KEY` table no longer reflects a primary key, so `automap_base().prepare(autoload_with=engine)` skips it, and the ORM cannot map a reflected `Table` of it. Give the ORM the key yourself: reflect the table with an overriding `Column("id", INTEGER, primary_key=True)`, or set `__mapper_args__ = {"primary_key": [table.c.id]}` on the mapped class.
- Write `%` once in DDL text, such as a view's definition, `starrocks_partition_by` or `starrocks_properties`, in models and migration scripts alike. Earlier versions raised on a single `%`, so a `%%` written for them now reaches StarRocks as `%%`, which changes what a `date_format` or `str2date` pattern means; the dialect warns when it finds one.
- Offline SQL (`alembic upgrade --sql`) needs `dialect_opts={"paramstyle": "named"}` in `run_migrations_offline()`, as Alembic's `env.py` template has, or each `%` in DDL text is written doubled.
:::

Examples below reflect the real public API and parameter names.

### Table Example

StarRocks table options can be specified in both ORM (via `__table_args__`) and Core (via `Table(..., starrocks_...=...)`) styles.

#### ORM (Declarative) style

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from starrocks import INTEGER, STRING

# with the same engine as the quick test
engine = create_engine("starrocks://root@localhost:9030/mydatabase")

Base = declarative_base()

class MyTable(Base):
    __tablename__ = 'my_orm_table'
    id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    name: Mapped[str] = mapped_column(STRING)

    __table_args__ = {
        'comment': 'table comment',

        'starrocks_primary_key': 'id',
        'starrocks_distributed_by': 'HASH(id) BUCKETS 10',
        'starrocks_properties': {'replication_num': '1'}
    }

# Create the table in the database
Base.metadata.create_all(engine)
```

#### Core style

```python
from sqlalchemy import Column, MetaData, Table, create_engine
from starrocks import INTEGER, VARCHAR

# with the same engine as the quick test
engine = create_engine("starrocks://root@localhost:9030/mydatabase")

metadata = MetaData()

my_core_table = Table(
    'my_core_table',
    metadata,
    Column('id', INTEGER, primary_key=True),
    Column('name', VARCHAR(50)),

    # StarRocks-specific arguments
    starrocks_primary_key='id',
    starrocks_distributed_by='HASH(id) BUCKETS 10',
    starrocks_properties={"replication_num": "1"}
)

# Create the table in the database
metadata.create_all(engine)
```

:::note
For a comprehensive reference of table attributes and data types, see [Reference [4]](#references).
:::

### View Example

Below is the recommended view definition style, using `columns` as a list of dicts (`name`/`comment`). This example is based on an existing table `my_core_table`.

```python
from starrocks.schema import View

# Reuse the metadata from the Core table example above
metadata = my_core_table.metadata

user_view = View(
    "user_view",
    metadata,
    definition="SELECT id, name FROM my_core_table WHERE name IS NOT NULL",
    columns=[
        {"name": "id", "comment": "ID"},
        {"name": "name", "comment": "Name"},
    ],
    comment="Active users",
)
```

:::note
For more View options and limitations, see [Reference [5]](#references).
:::

### Materialized View Example

Materialized views are defined similarly. The `starrocks_refresh` property is a syntax string that indicates the refresh strategy.

```python
from starrocks.schema import MaterializedView

# Reuse the metadata from the Core table example above
metadata = my_core_table.metadata

# Create a simple Materialized View (asynchronous refresh)
user_stats_mv = MaterializedView(
    'user_stats_mv',
    metadata,
    definition='SELECT id, COUNT(*) AS cnt FROM my_core_table GROUP BY id',
    starrocks_refresh='ASYNC'
)
```

:::note
For more options and ALTER limitations, see [Reference [6]](#references).
:::

## Alembic Integration

The StarRocks SQLAlchemy dialect provides full support for:

- Create / Drop table
- Create / Drop view
- Create / Drop materialized view
- Detecting supported changes on StarRocks-specific attributes (for example, table properties and distribution)

This enables Alembic’s **autogenerate** to work properly.

### Initializing Alembic

1. Initialize Alembic:

   ```bash
   alembic init migrations
   ```

2. Configure your database URL in `alembic.ini`:

   ```ini
   # alembic.ini
   sqlalchemy.url = starrocks://<user>:<password>@<FE_host>:<query_port>/[<catalog>.]<database>
   ```

3. Enable StarRocks dialect logging (optional):

   You can enable the `starrocks` logger in `alembic.ini` to observe the detected changes of a table via logs. For details, see [Reference [2]](#references).

   Edit `env.py` (configure both offline and online paths):

    ```python
    from alembic import context
    from starrocks.alembic import render_column_type, include_object_for_view_mv
    from starrocks.alembic.starrocks import StarRocksImpl  # noqa: F401  (ensure impl registered)

    from myapp.models import Base  # adjust to your project

    target_metadata = Base.metadata


    def run_migrations_offline() -> None:
        url = context.config.get_main_option("sqlalchemy.url")
        context.configure(
            url=url,
            target_metadata=target_metadata,
            dialect_opts={"paramstyle": "named"},  # as Alembic's template has; keeps each % in DDL text single
            render_item=render_column_type,
            include_object=include_object_for_view_mv
        )

        with context.begin_transaction():
            context.run_migrations()


    def run_migrations_online() -> None:
        # ... create engine and connect as in alembic default env.py ...
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                render_item=render_column_type,
                include_object=include_object_for_view_mv
            )

            with context.begin_transaction():
                context.run_migrations()
    ```

### Choosing the canonicalization schema for view/MV comparison

On StarRocks versions before 4.0.7, a view definition is stored in the engine's own canonical form, and before 4.0.2 a materialized-view definition is shown in that form. It can differ textually from the SQL in your model (dropped `col AS col` aliases, added parentheses, and similar) while remaining semantically identical. To avoid reporting a phantom change on every autogenerate, the dialect canonicalizes your model's definition by round-tripping it through a **temporary view** and comparing the engine's stored form of both sides.

By default that temporary view is created in the schema of the object being compared, so the migration user needs privileges to create views in every schema that contains a view or MV. If your user is locked down, set `starrocks_temp_view_schema` to a single dedicated schema and grant the required privileges only there. A `__…__` style name (for example `__alembic_canon__`) makes the schema's purpose clear; it can be any schema the user can write to, including an existing one such as your `version_table_schema`:

```python
# env.py
context.configure(
    # ... your existing parameters (render_item, include_object, etc.) ...
    starrocks_temp_view_schema="__alembic_canon__",  # host the transient comparison view here
)
```

Grant the migration user, on that one schema, the privileges needed for the create → read-back → drop round-trip (`CREATE VIEW` alone is not enough):

```sql
GRANT CREATE VIEW ON DATABASE __alembic_canon__ TO '<user>';
GRANT SELECT, DROP ON ALL VIEWS IN DATABASE __alembic_canon__ TO '<user>';
```

When `starrocks_temp_view_schema` is unset, behavior is unchanged (the temporary view is created in the compared object's own schema). If the temporary view cannot be created (missing privilege, or a referenced object that does not exist yet), the comparison logs a DEBUG message and falls back to in-process AST/regex normalization. StarRocks keeps a view's definition as written from 4.0.7, and shows a materialized view's as written from 4.0.2; from then on a definition that differs is altered (a view) or recreated (a materialized view) as usual. Before those versions the stored definition has table-qualified columns that the normalization keeps, so a difference only it finds is reported as a warning, and the definition is left as the database has it, unless the change is certain: the database's definition goes through the temporary view and only the model's fails, as when it reads a column the revision adds; the model's definition reads a table, view or materialized view that the same revision creates; or the database's reads a table, view or materialized view that the revision drops without creating it again, or a column it drops or renames.

### Generating migrations automatically

```bash
alembic revision --autogenerate -m "initial schema"
```

Alembic will compare  SQLAlchemy models with the actual StarRocks schema, and output the correct DDL.

### Applying migrations

```bash
alembic upgrade head
```

Downgrade is also supported (where reversible).

:::important
StarRocks DDL is not transactional across multiple statements. If an upgrade fails midway, you may need to inspect what has already been applied and **perform manual remediation** (for example, write a compensating migration or run manual DDL) before re-running.
:::

## Supported Schema Change Operations

The dialect supports Alembic autogenerate for:

- **Tables**: create / drop, and diffing of StarRocks-specific attributes declared via `starrocks_*` (within StarRocks ALTER support)
- **Views**: create / drop / alter (definition, columns, comment and security)
- **Materialized Views**: create / drop / alter of the refresh strategy and properties; a change ALTER cannot make, such as a new definition, drops the materialized view and creates it again

Some StarRocks DDL changes are not reversible or not alterable. You can only make these changes by dropping and recreating the table/view/materialized view. Autogenerate does that for a materialized view. For a table, it **warns** about such a change and generates no operation for it, and still generates the rest of the revision.

## End-to-End Example (Recommended Reading for Beginners)

This section shows a runnable end-to-end workflow, including where to pause and review generated files.

### Step 1. Create a project directory and initialize Alembic

```bash
mkdir my_sr_alembic_project
cd my_sr_alembic_project

alembic init alembic
```

### Step 2. Configure `alembic.ini`

Edit the URL in `alembic.ini`:

```ini
sqlalchemy.url = starrocks://root@localhost:9030/mydatabase
```

### Step 3. Define your models

Create a package for your models:

```bash
mkdir -p myapp
touch myapp/__init__.py
```

Create `myapp/models.py` and put your table/view/materialized view definitions in the package:

:::note
When using Alembic migrations, do not call `metadata.create_all(engine)` in your models module.
:::

```python
from sqlalchemy import Column, Table
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

from starrocks import INTEGER, STRING, VARCHAR
from starrocks.schema import MaterializedView, View

Base = declarative_base()


# --- ORM table ---
class MyOrmTable(Base):
    __tablename__ = "my_orm_table"

    id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    name: Mapped[str] = mapped_column(STRING)

    __table_args__ = {
        "comment": "table comment",
        "starrocks_primary_key": "id",
        "starrocks_distributed_by": "HASH(id) BUCKETS 10",
        "starrocks_properties": {"replication_num": "1"},
    }


# --- Core table on the same metadata (important for Alembic target_metadata) ---
my_core_table = Table(
    "my_core_table",
    Base.metadata,
    Column("id", INTEGER, primary_key=True),
    Column("name", VARCHAR(50)),
    comment="core table comment",
    starrocks_primary_key="id",
    starrocks_distributed_by="HASH(id) BUCKETS 10",
    starrocks_properties={"replication_num": "1"},
)


# --- View ---
user_view = View(
    "user_view",
    Base.metadata,
    definition="SELECT id, name FROM my_core_table WHERE name IS NOT NULL",
    columns=[
        {"name": "id", "comment": "ID"},
        {"name": "name", "comment": "Name"},
    ],
    comment="Active users",
)


# --- Materialized View ---
user_stats_mv = MaterializedView(
    "user_stats_mv",
    Base.metadata,
    definition="SELECT id, COUNT(*) AS cnt FROM my_core_table GROUP BY id",
    starrocks_refresh="ASYNC",
)
```

### Step 4. Configure `env.py` for autogenerate

Edit `alembic/env.py`:

1. Import `myapp.models` to set the `target_metadata`.
2. Import `render_column_type`, and `include_object_for_view_mv` to set them in both `run_migrations_offline()` and `run_migrations_online()` to properly handle views and MVs, and to properly render StarRocks column types.

:::note
You need to add or modify these lines in `env.py`, rather than replace the generated `env.py` file.
:::

```python
from alembic import context
from starrocks.alembic import render_column_type, include_object_for_view_mv
from starrocks.alembic.starrocks import StarRocksImpl  # noqa: F401

from myapp.models import Base

target_metadata = Base.metadata

# Optional: set version table replication for single-BE dev clusters
version_table_kwargs = {"starrocks_properties": {"replication_num": "1"}}

# In both run_migrations_offline() and run_migrations_online(), ensure:
def run_migrations_offline() -> None:
    url = context.config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},  # as Alembic's template has; keeps each % in DDL text single
        render_item=render_column_type,
        include_object=include_object_for_view_mv,
        version_table_kwargs=version_table_kwargs,
    )


def run_migrations_online() -> None:
    # ... create engine and connect as in alembic default env.py ...
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_item=render_column_type,
            include_object=include_object_for_view_mv,
            version_table_kwargs=version_table_kwargs,
        )
```

### Step 5. Autogenerate the first revision

```bash
alembic revision --autogenerate -m "create initial schema"
```

Pause and review:

1. Check the generated migration file under `alembic/versions/`.
2. Ensure it contains the expected operations (for example, `create_table`, `create_view`, `create_materialized_view`).
3. Make sure it does not contain unexpected drops or alters.

### Step 6. Preview SQL and apply

Preview SQL:

```bash
alembic upgrade head --sql
```

Pause and review:

1. Confirm the DDL is in the order you expect.
2. Identify any potentially heavy operations and consider splitting migrations if needed.

Apply:

```bash
alembic upgrade head
```

:::important
StarRocks DDL is not transactional across multiple statements. If an upgrade fails midway, you may need to inspect what has already been applied and perform manual remediation before re-running.
:::

### Step 7. Make a change and autogenerate again

Update `myapp/models.py` to:

- **Modify an existing table** (`my_core_table`): add a column, or update the table comment, and change one table property.
- **Add a new table** (`my_new_table`).

:::note
Adding a column can be a time-consuming schema change, and StarRocks allows only **one running schema change job per table** at a time. The dialect waits for the table's running job before each `ALTER TABLE`, `CREATE INDEX` and `DROP INDEX` that an Alembic operation compiles, for up to the engine's `starrocks_schema_change_timeout` (300 seconds by default), so the operations of one revision run one after the other. SQL that `op.execute()` sends, as text or as SQLAlchemy's `DDL()`, doesn't wait. Each slow job rewrites the table's data, so to keep them few, combine a table's added and dropped columns into one `ALTER TABLE` with the `combine_column_alters` rewriter ([Reference [2]](#references)).
:::

```python
from sqlalchemy import Column, Table
from starrocks import INTEGER, VARCHAR

# Modify an existing table (add a column)
# (Update the existing my_core_table definition in-place.)
my_core_table = Table(
    "my_core_table",
    Base.metadata,
    Column("id", INTEGER, primary_key=True),
    Column("name", VARCHAR(50)),
    Column("age", INTEGER),  # added column only

    starrocks_primary_key='id',
    starrocks_distributed_by='HASH(id) BUCKETS 10',
    starrocks_properties={"replication_num": "1"},
)

my_new_table = Table(
    "my_new_table",
    Base.metadata,
    Column("id", INTEGER, primary_key=True),
    Column("name", VARCHAR(50)),
    starrocks_primary_key="id",
    starrocks_distributed_by="HASH(id) BUCKETS 10",
    starrocks_properties={"replication_num": "1"},
)
```

```bash
alembic revision --autogenerate -m "add a new table, change a old table"
```

Pause and review:

Make sure the new migration contains:
- a `create_table(...)` for `my_new_table`, and
- expected operations for the `my_core_table` changes (for example, add column / set comment / set properties).

Preview SQL and apply:

```bash
alembic upgrade head --sql
alembic upgrade head
```

## Using sqlacodegen

[`sqlacodegen`](https://github.com/agronholm/sqlacodegen) can reverse-generate SQLAlchemy models directly from StarRocks:

```bash
sqlacodegen --options include_dialect_options,keep_dialect_types \
  --generator tables \
  starrocks://<user>:<password>@<FE_host>:<query_port>/[catalog.]<database> > models.py
```

Supported objects:

- Tables
- Views
- Materialized views
- Partitioning, distribution, and order-by clauses, and properties

This is useful when onboarding an existing StarRocks schema into Alembic.

You can directly use above command to generate the Python script for tables/views/materialized views defined in the **End-to-End Example** section.

:::note
- It is recommended to add `--generator tables` when generating Core-style models (ORM generators may reorder columns according to `NOT NULL` / `NULL` attribute).
- Key columns may be generated as `NOT NULL`. If you want them nullable, adjust the generated model manually.
:::

## Limitations and Best Practices

- Some StarRocks DDL operations require dropping and recreating the table; autogenerate warns about such a change and generates no operation for it, rather than producing SQL StarRocks refuses.
- Keys model changes (for example, changing DUPLICATE KEY to PRIMARY KEY) are not supported via `ALTER TABLE`; use an explicit plan (usually dropping and recreating with backfill).
- StarRocks does not provide transactional DDL across multiple statements; review generated migrations and apply them operationally. If a migration fails midway, you may need to handle rollback **manually**.
- For distribution, if you omit the `BUCKETS` clause, StarRocks may auto-assign bucket count; the dialect is designed to avoid noisy diffs in that case.
- **Only declared table properties are managed:** autogenerate compares only the properties you list in `starrocks_properties`. A property you leave out keeps whatever value the database has, even if it differs from the StarRocks default, because many defaults depend on the StarRocks version or FE configuration. To change a property, declare it explicitly. Removing a property from your model does not reset it.
- **View and materialized view definition comparison (StarRocks < 4.0.7):** On clusters older than 4.0.7, StarRocks rewrites view definitions into a canonical form when they are stored, and before 4.0.2 it shows materialized view definitions in that form, so the SQL you write in your model may differ syntactically from what the cluster reflects back. The dialect reconciles this by round-tripping your model SQL through a temporary view to obtain the DB's canonical form before comparison. This requires all tables and views referenced by the model definition to already exist in the database. When they do not (for example, during a forward migration that also creates those objects), the dialect falls back to a regex-based normalizer, and it changes the definition only when the change is certain, as [Choosing the canonicalization schema](#choosing-the-canonicalization-schema-for-viewmv-comparison) describes; otherwise it warns.
- **View definition drift after a cluster upgrade:** If a StarRocks cluster is upgraded while views already exist, those views were canonicalized by the old version and may not match the verbatim form the upgraded cluster would produce. Autogenerate may emit spurious view migrations in that window. Recreating the affected views (drop and re-apply the migration) resolves the drift.

## Summary

With the StarRocks SQLAlchemy dialect and Alembic integration, you can:

- ✔ Use declarative models to define StarRocks schemas
- ✔ Automatically detect and generate schema migration scripts
- ✔ Use version control for schema evolution
- ✔ Manage views and materialized views declaratively
- ✔ Reverse-engineer existing schemas using sqlacodegen

This brings StarRocks schema management into the modern Python data engineering ecosystem and significantly simplifies cross-environment schema consistency.

## References

[1]: [starrocks-python-client README](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/README.md)

[2]: [Alembic Integration](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/alembic.md)

[3]: [SQLAlchemy details](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/sqlalchemy.md)

[4]: [Table Support](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/tables.md)

[5]: [View Support](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/views.md)

[6]: [Materialized View Support](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/materialized_views.md)
