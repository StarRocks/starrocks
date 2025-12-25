---
displayed_sidebar: docs
description: Schema management and migration
sidebar_label: Schema Migration
---

# Schema Management and Migration with SQLAlchemy and Alembic

This guide introduces how to manage StarRocks schemas using the Python ecosystem — including SQLAlchemy, Alembic, and sqlacodegen — through the **`starrocks` SQLAlchemy** dialect.
It is designed to help both new and experienced users understand **why schema migration is useful** and **how to use it effectively with StarRocks**.

## 1. Overview

Many teams manage StarRocks tables, views, and materialized views using SQL DDL directly. However, as projects grow, manually maintaining `ALTER TABLE` statements becomes error-prone and hard to track.

The **StarRocks SQLAlchemy dialect (`starrocks`)** provides:

- A full SQLAlchemy model layer for StarRocks **tables**, **views**, and **materialized views**
- **Declarative** definitions for table schema and table properties (including views and MVs)
- Integration with **Alembic** so schema changes can be **detected** and **generated** automatically
- Compatibility with tools like **sqlacodegen** for reverse-generating models

This allows Python users to maintain StarRocks schemas in a **declarative**, **version-controlled**, and **automated** way.

## 2. Why Use Schema Migration?

Although schema migration is traditionally associated with OLTP databases, it is also valuable in data warehousing systems such as StarRocks. Teams use [Alembic](https://alembic.sqlalchemy.org/) together with the StarRocks dialect because:

### 2.1 Declarative schema definition

You define your schema once in Python [ORM](https://docs.sqlalchemy.org/en/20/orm/quickstart.html#orm-quickstart) models or [SQLAlchemy](https://docs.sqlalchemy.org) core style.
You no longer write `ALTER TABLE` manually.

### 2.2 Automatic diffing and autogeneration

Alembic compares: **current StarRocks schema** vs. **your SQLAlchemy models**,
and generates migration scripts automatically (CREATE/DROP/ALTER).

### 2.3 Reviewable, version-controlled migrations

Each schema change becomes a migration file (Python), so teams can track changes and roll back if needed.

### 2.4 Consistent workflow across environments

Schema changes can be applied to development, staging, and production with the same process.

## 3. Installing and Connecting

```bash
pip install starrocks
```

Minimum required versions:

- `starrocks >= 1.3.2`
- `SQLAlchemy >= 1.4` (SQLAlchemy 2.0 is recommended and is required to use `sqlacodegen`)
- `Alembic >= 1.16`

### Connection URL

```bash
starrocks://<user>:<password>@<host>:<port>/[<catalog>.]<database>
```

### Quick test

After installation, you can quickly validate connectivity:

```python
from sqlalchemy import create_engine, text

# you need to create `mydatabase` first
engine = create_engine("starrocks://root@localhost:9030/mydatabase")

with engine.connect() as conn:
    conn.execute(text("SELECT 1")).fetchall()
    print("Connection successful!")
```

## 4. Defining StarRocks Models (Declarative ORM)

The StarRocks dialect supports:

- Tables ([GitHub][5])
- Views ([GitHub][6])
- Materialized Views ([GitHub][7])

Including StarRocks-specific table attributes such as:

- `ENGINE` (OLAP)
- Keys models (`DUPLICATE KEY`, `PRIMARY KEY`, `UNIQUE KEY`, `AGGREGATE KEY`)
- `PARTITION BY` (RANGE / LIST / Expression partitioning)
- `DISTRIBUTED BY` (HASH / RANDOM)
- `ORDER BY`
- Table properties (e.g., `replication_num`, `storage_medium`)

Examples below reflect the real public API and parameter names from this repository’s README and usage guide.

> **Important**:
>
> - StarRocks dialect options are passed as keyword arguments prefixed with `starrocks_`.
> - The `starrocks_` **prefix must be lowercase**. The suffix (e.g. `PRIMARY_KEY` vs `primary_key`) is accepted in either case.
> - If you specify a StarRocks key (e.g. `starrocks_primary_key="id"`), the involved columns **must** also be marked with `primary_key=True` in `Column(...)`, so SQLAlchemy metadata and Alembic autogenerate can behave correctly.

### 4.1 Table Example

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

> **Note**: For a comprehensive reference of table attributes and data types, see **`tables.md`** in the reference section.

### 4.2 View Example

Below is the recommended View definition style, using `columns` as a list of dicts (`name`/`comment`). This example assumes `my_core_table` already exists.

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

> **Note**: For more View options and limitations, see **`views.md`** in the reference section.

### 4.3 Materialized View Example

Materialized Views are defined similarly. The `starrocks_refresh` value is a StarRocks syntax string (for full syntax, see the reference).

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

> **Note**: For more MV options and ALTER limitations, see **`materialized_views.md`** in the reference section.

## 5. Alembic Integration

The StarRocks SQLAlchemy dialect provides full support for:

- Create / Drop table
- Create / Drop view
- Create / Drop materialized view
- Detecting supported changes on StarRocks-specific attributes (for example, table properties and distribution)

This enables Alembic’s **autogenerate** to work properly.

### 5.1 Initializing Alembic

```bash
alembic init migrations
```

Configure your database URL in `alembic.ini`:

```ini
# alembic.ini
sqlalchemy.url = starrocks://root@localhost:9030/mydatabase
```

Enable StarRocks dialect logging (optional):

To see some useful logging, such as the detected changes of a table, you can enable the `starrocks` logger in `alembic.ini`. For details, see the “Debugging and Logging” section in **`alembic.md`**.

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

### 5.2 Generating migrations automatically

```bash
alembic revision --autogenerate -m "initial schema"
```

Alembic will compare: SQLAlchemy models vs. Actual StarRocks schema, and emit the correct DDL.

### 5.3 Applying migrations

```bash
alembic upgrade head
```

Downgrade is also supported (where reversible).

> **Operational note (important)**: StarRocks DDL is not transactional across multiple statements. If an upgrade fails midway, you may need to inspect what has already been applied and **perform manual remediation** (for example, write a compensating migration or run manual DDL) before re-running.

## 6. Supported Schema Change Operations

The dialect supports Alembic autogenerate for:

- **Tables**: create / drop, and diffing of StarRocks-specific attributes declared via `starrocks_*` (within StarRocks ALTER support)
- **Views**: create / drop / alter (mainly definition-related changes; some attributes are immutable)
- **Materialized Views**: create / drop / alter (limited to mutable clauses such as refresh/properties)

Some StarRocks DDL changes are not reversible or not alterable (must drop/recreate). In those cases, autogenerate will **warn or raise** so you can make an explicit operational choice.

## 7. End-to-End Example (Recommended Reading for Beginners)

This section shows a runnable end-to-end workflow (similar to `alembic.md`), including where to pause and review generated files.

### Step 1 — Create a project directory and initialize Alembic

```bash
mkdir my_sr_alembic_project
cd my_sr_alembic_project

alembic init alembic
```

### Step 2 — Configure `alembic.ini`

Edit `alembic.ini`:

```ini
sqlalchemy.url = starrocks://root@localhost:9030/mydatabase
```

### Step 3 — Define your models (`myapp/models.py`)

Create a small package for your models (example):

```bash
mkdir -p myapp
touch myapp/__init__.py
```

Create `myapp/models.py` and put your `Table` / `View` / `MaterializedView` definitions there (example below).

> **Note**: When using Alembic migrations, do not call `metadata.create_all(engine)` in your models module. Creation should happen via Alembic migrations.

Example `myapp/models.py`:

> The same table/view/mv definition python script shown in Section 4.

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

### Step 4 — Configure `env.py` for autogenerate

Edit `alembic/env.py`:

1. Import `myapp.models` to set the `target_metadata`.
2. Import `render_column_type`, and `include_object_for_view_mv` to set them in both `run_migrations_offline()` and `run_migrations_online()` to properly handle views and MVs, and to properly render StarRocks column types.

> Note: You need to add/modify these lines into `env.py`, rather than replace the generated `env.py` file.

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

### Step 5 — Autogenerate the first revision

```bash
alembic revision --autogenerate -m "create initial schema"
```

Pause and review:

- Check the generated migration file under `alembic/versions/`.
- Ensure it contains the expected operations (for example, `create_table`, `create_view`, `create_materialized_view`).
- Make sure it does not contain unexpected drops or alters.

### Step 6 — Preview SQL (`--sql`) and apply

Preview SQL:

```bash
alembic upgrade head --sql
```

Pause and review:

- Confirm the DDL is in the order you expect.
- Identify any potentially heavy operations and consider splitting migrations if needed.

Apply:

```bash
alembic upgrade head
```

> **Operational note (important)**: StarRocks DDL is not transactional across multiple statements. If an upgrade fails midway, you may need to inspect what has already been applied and perform manual remediation before re-running.

### Step 7 — Make a change (add a new table) and autogenerate again

Update `myapp/models.py`:

- **Modify an existing table** (`my_core_table`): add a column, or update the table comment, and change one table property.
- **Add a new table** (`my_new_table`).

> **Note (schema change job limits)**: Adding a column can be a time-consuming schema change. StarRocks allows only **one running schema change job per table** at a time. In practice, it is recommended to keep “add/drop/modify columns” changes separate from other heavy changes (for example, additional add/drop columns or mass property changes), and split them into multiple Alembic revisions if needed.

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

- Check the new migration contains:
  - a `create_table(...)` for `my_new_table`, and
  - expected operations for the `my_core_table` changes (for example, add column / set comment / set properties).

Preview SQL and apply:

```bash
alembic upgrade head --sql
alembic upgrade head
```

## 8. Using sqlacodegen

[`sqlacodegen`](https://github.com/agronholm/sqlacodegen) can reverse-generate SQLAlchemy models directly from StarRocks:

```bash
sqlacodegen --options include_dialect_options,keep_dialect_types \
  --generator tables \
  starrocks://user:pwd@host:port/[catalog.]db > models.py
```

Supported objects:

- Tables
- Views
- Materialized views
- Partitioning, distribution, order-by, properties

This is useful when onboarding an existing StarRocks schema into Alembic.

> You can directly use above command to generate the python script for tables/views/mvs defined in Section 4 or 7.

Notes:

- It is recommended to add `--generator tables` when generating Core-style models (ORM generators may reorder columns according to `NOT NULL` / `NULL` column attribute).
- Key columns may be generated as `NOT NULL`. If you want them nullable, adjust the generated model manually.

## 9. Limitations and Best Practices

- Some StarRocks DDL operations require table rebuilds; autogenerate will warn/raise rather than silently producing unsafe SQL.
- Keys model changes (e.g., DUPLICATE → PRIMARY) are not supported via `ALTER TABLE`; use an explicit plan (usually drop/recreate with backfill).
- StarRocks does not provide transactional DDL across multiple statements; review generated migrations and apply them operationally. If a migration fails midway, you may need to handle rollback **manually**.
- For distribution, if you omit the `BUCKETS` clause, StarRocks may auto-assign bucket count; the dialect is designed to avoid noisy diffs in that case.

## 10. Summary

With the StarRocks SQLAlchemy dialect and Alembic integration, teams can now:

✔ Use declarative models to define StarRocks schemas
✔ Automatically detect and generate schema migration scripts
✔ Use version control for schema evolution
✔ Manage views and materialized views declaratively
✔ Reverse-engineer existing schemas using sqlacodegen

This brings StarRocks schema management into the modern Python data engineering ecosystem and significantly simplifies cross-environment schema consistency.

## 11. Reference and Further Reading

- GitHub: **starrocks-python-client README** for overview and installation.([GitHub][1])
- GitHub `usage_guide` docs:
  - _sqlalchemy.md_ (SQLAlchemy details)([GitHub][3])
  - _alembic.md_ (migration workflow)([GitHub][2])
  - _tables.md_ (table features & parameters)([GitHub][5])
  - _views.md_ (view support)([GitHub][6])
  - _materialized_views.md_ (MV support)([GitHub][7])

[1]: https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/README.md "starrocks/contrib/starrocks-python-client/README.md at main · StarRocks/starrocks · GitHub"
[2]: https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/alembic.md "starrocks/contrib/starrocks-python-client/docs/usage_guide/alembic.md at main · StarRocks/starrocks · GitHub"
[3]: https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/sqlalchemy.md "starrocks/contrib/starrocks-python-client/docs/usage_guide/sqlalchemy.md at main · StarRocks/starrocks · GitHub"
[5]: https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/tables.md "starrocks/contrib/starrocks-python-client/docs/usage_guide/tables.md at main · StarRocks/starrocks · GitHub"
[6]: https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/views.md "starrocks/contrib/starrocks-python-client/docs/usage_guide/views.md at main · StarRocks/starrocks · GitHub"
[7]: https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/materialized_views.md "starrocks/contrib/starrocks-python-client/docs/usage_guide/materialized_views.md at main · StarRocks/starrocks · GitHub"
