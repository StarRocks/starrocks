# StarRocks Python Client

A StarRocks client for the Python programming language, including a SQLAlchemy dialect and an Alembic extension.

StarRocks is a next-generation data platform designed for fast,real-time analytics. This package allows developers to interact with StarRocks using Python, leveraging SQLAlchemy's powerful ORM and expression language, and managing database schema migrations with Alembic.

## Quick Start

### Installation

```bash
pip install starrocks
```

#### Supported Python Versions

Python >= 3.10, <= 3.14

#### Using a Virtual Environment (Recommended)

It is highly recommended to install `starrocks` in a virtual environment to avoid conflicts with system-wide packages.

**Mac/Linux:**

```bash
pip install virtualenv
virtualenv <your-env-name>
source <your-env-name>/bin/activate
<your-env-name>/bin/pip install starrocks
```

**Windows:**

```bash
pip install virtualenv
virtualenv <your-env-name>
<your-env-name>\Scripts\activate
<your-env-name>\Scripts\pip.exe install starrocks
```

### Basic SQLAlchemy Usage

To connect to StarRocks, use the SQLAlchemy connection string format:

```ini
starrocks://<User>:<Password>@<Host>:<Port>/[<Catalog>.]<Database>
```

Or, for an asynchronous connection, use asyncmy driver:
```ini
starrocks+asyncmy://<User>:<Password>@<Host>:<Port>/[<Catalog>.]<Database>
```

- **User**: User Name
- **Password**: DBPassword
- **Host**: StarRocks FE Host
- **Catalog**: Catalog Name
- **Database**: Database Name
- **Port**: StarRocks FE port

> Note: The `Catalog` can be omitted and is managed by StarRocks.
> The default is `default_catalog`.

### Example: Basic Operations

Connect to your database and do a query.

```python
from sqlalchemy import create_engine, text

engine = create_engine('starrocks://root@localhost:9030/mydatabase')

# make sure you have created the table `mytable` in `mydatabase`.

with engine.connect() as connection:
    print("Connection successful!")
    rows = connection.execute(text("SELECT * FROM mytable LIMIT 2")).fetchall()
    print(rows)

# async version
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine('starrocks+asyncmy://root@localhost:9030/mydatabase')

async def async_query():
    async with engine.connect() as connection:
        print("Connection successful!")
        rows = await connection.execute(text("SELECT * FROM mytable LIMIT 2")).fetchall()
        print(rows)

    await engine.dispose()

asyncio.run(async_query())

```

### Example: Defining a Table (ORM Style)

You can define a table with StarRocks-specific attributes using SQLAlchemy's ORM declarative style: [orm-quickstart](https://docs.sqlalchemy.org/en/20/orm/quickstart.html#orm-quickstart).

```python
from sqlalchemy import Column
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from starrocks import INTEGER, STRING

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

### Example: Defining a Table (Core Style)

Alternatively, you can use SQLAlchemy Core to define tables programmatically.

```python
from sqlalchemy import Column, MetaData, Table
from starrocks import INTEGER, VARCHAR

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

For a complete guide on defining tables (Core and ORM), executing queries, and using advanced features, please see the **[SQLAlchemy Usage Guide](./docs/usage_guide/sqlalchemy.md)**.

For a detailed reference on all StarRocks-specific table attributes and data types, please see the **[Table Definition Reference](./docs/usage_guide/tables.md)**.

### Example: Views and Materialized Views

Create a View and a Materialized View using the StarRocks helpers. These behave like SQLAlchemy `Table` objects and are created with `metadata.create_all(engine)`.

```python
from sqlalchemy import MetaData, text
from starrocks.sql.schema import View, MaterializedView

metadata = MetaData()

# Create a simple View (columns inferred from SELECT)
user_view = View(
    'user_view',
    metadata,
    definition='SELECT id, name FROM my_core_table WHERE name IS NOT NULL',
    comment='Active users'
)

# Create a simple Materialized View (asynchronous refresh)
user_stats_mv = MaterializedView(
    'user_stats_mv',
    metadata,
    definition='SELECT id, COUNT(*) AS cnt FROM my_core_table GROUP BY id',
    starrocks_refresh='ASYNC'
)

# Create the view and MV in the database
metadata.create_all(engine)

# Query the view or MV like normal tables
with engine.connect() as conn:
    rows = conn.execute(text("SELECT * FROM user_view LIMIT 5")).fetchall()
    print(rows)
```

You can refer to **[Views Definition Reference](./docs/usage_guide/views.md)** and **[Materialized View Definition Reference](./docs/usage_guide/materialized_views.md)** for more detailed information.

### Alembic Integration for Schema Migrations

This dialect integrates with Alembic to support automated schema migrations. Here’s a quick-start guide to get you up and running.

#### Generate models from an existing database (Optional)

If you already have tables/views/materialized views in your StarRocks database, you can generate `models.py` (or a consolidated models file) using `sqlacodegen`.

```bash
sqlacodegen --options include_dialect_options,keep_dialect_types \
  starrocks://root@localhost:9030 > models.py
```

Refer to [generating models](./docs/usage_guide/alembic.md#Generating-models-from-an-existing-database-using-sqlacodegen) and [`sqlacodegen`](https://github.com/agronholm/sqlacodegen) for more options and features.

#### 1. Install and Initialize Alembic

```bash
pip install "alembic>=1.16"
alembic init alembic
```

#### 2. Configure your Database URL, Logging Info

In `alembic.ini`, set the `sqlalchemy.url` to your StarRocks connection string.

```ini
# alembic.ini
sqlalchemy.url = starrocks://root@localhost:9030/mydatabase
```

It's better to print the log from this `starrocks-sqlalchemy` when runing alembic command. You can add following logging configuration in the `alembic.ini` file.

```ini
# alembic.ini
[loggers]
# Append starrocks model at the following line
# keys = root,sqlalchemy,alembic
keys = root,sqlalchemy,alembic,starrocks


# Add following lines after `[logger_alembic]` section
[logger_starrocks]
level = INFO
handlers =
qualname = starrocks
```

#### 3. Configure your Models for Autogeneration

In `alembic/env.py`, import your models' metadata and assign it to `target_metadata`.

```python
# alembic/env.py
# Add these imports
from myapp.models import Base  # Adjust to your models' location
from starrocks.alembic import render_column_type, include_object_for_view_mv

# ...
# And set the target_metadata
target_metadata = Base.metadata

def run_migrations_online() -> None:
    # ... inside this function
    context.configure(
        # ...
        render_item=render_column_type,            # Add this line (required for column comparison)
        include_object=include_object_for_view_mv  # Add this line (required for View/MV support)
    )
    # ...
```

> **Note**: For advanced filtering options (e.g., excluding temporary tables), see the [Alembic Integration Guide](./docs/usage_guide/alembic.md#Advanced-Custom-Object-Filtering).

#### 4. Generate and Apply Your First Migration

With your models defined (as shown in the SQLAlchemy examples above), you can now generate and apply a migration.

```bash
# Generate the migration script
alembic revision --autogenerate -m "Create initial tables"

# Apply the migration to the database
alembic upgrade head
```

For a full tutorial on advanced topics like data migrations, handling complex types, and managing views, please refer to the **[Alembic Integration Guide](./docs/usage_guide/alembic.md)**.

## Contributing

### Tests

Every test runs under [SQLAlchemy's testing plugin](https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst), against a running StarRocks cluster. A single `pytest` run covers the StarRocks tests in `test/sql`, `test/unit`, `test/integration` and `test/system`, SQLAlchemy's dialect compliance suite (`test/test_suite.py`) and Alembic's dialect suite (`test/test_suite_alembic.py`).

#### Setting Up

Install the package in editable mode with its test and Alembic dependencies:

```bash
pip install -e ".[test,alembic]"
```

Create the database the tests use, and the two extra schemas SQLAlchemy's suite needs:

```SQL
CREATE DATABASE IF NOT EXISTS test_sqla;
CREATE DATABASE IF NOT EXISTS test_schema;
CREATE DATABASE IF NOT EXISTS test_schema_2;

-- set it if you're testing on a small shared-nothing cluster
ADMIN SET FRONTEND CONFIG ("default_replication_num" = "1");
```

The connection URL is taken from, in order of precedence:

1. `--dburi starrocks://<User>:<Password>@<Host>:<Port>/<Database>` on the command line;
2. the `STARROCKS_URL` environment variable;
3. a `[db]` section with a `default` URL in a git-ignored `test.cfg` next to `setup.cfg`;
4. `setup.cfg`'s `[db] default`, which is `starrocks://root@localhost:9030/test_sqla`.

#### Running the Tests

Run `pytest` from this directory:

```bash
pytest
```

`pyproject.toml` runs the tests on four `pytest-xdist` workers. Each worker gets its own databases, created from the configured one and dropped at the end, so the workers do not interfere; pass `-n0` to run serially against the configured database. The system tests keep Alembic's scripts in a `scratch` directory here (`scratch_<worker>` under `pytest-xdist`), so do not run two serial runs from this directory at once.

Every test needs a reachable cluster, the ones in `test/sql` and `test/unit` included, because the plugin connects when the session starts.

Tests that StarRocks itself cannot support are skipped. Tests that fail because of a known bug, in the dialect or in StarRocks, are marked `fails_on`, or `fails_if` with a condition, and the reason, so a fix shows up as an unexpected pass. Such a test is reported as passed when it fails as expected; run with `-n0 -s` to see each "failed as expected" line and its reason, since the `pytest-xdist` workers don't pass a test's output on.

The tests need SQLAlchemy 1.4.47 or later on 1.4, or 2.0.21 or later below 2.1, and were last run on 1.4.54 and 2.0.55. Before 1.4.47, `pytest` stops before any test runs: the testing package of Alembic 1.17 or later, which pip installs on 1.4, imports a part of SQLAlchemy's testing package that 1.4.47 added. On 1.4, SQLAlchemy's compliance suite and the compile, reflection, comparison and rendering tests run; Alembic's suite, the system tests, and the tests that build `View` or `MaterializedView` objects or need SQLAlchemy 2.0's inspector need SQLAlchemy 2.0 and are skipped.

SQLAlchemy 2.1 is untested, so the `test` extra installs SQLAlchemy below 2.1. SQLAlchemy 2.1.6 adds suite tests that the dialect does not pass yet.

#### Running Part of the Tests

Give `pytest` a directory, a module, a class or a test:

```bash
pytest test/sql
pytest test/unit/test_compare_tables.py
pytest test/unit/test_compare_tables.py::BucketSizeTest
pytest test/unit/test_compare_tables.py::BucketSizeTest::test_explicit_change_detected
```

The last one runs every combination of that test. A single combination has its parameters in brackets, as in `test_explicit_change_detected[1073741824-2147483648]`.

The plugin renames each class that runs against the database (one with `__backend__ = True`, which covers nearly every class in the two suites) after the driver and the server version, for example `ComponentReflectionTest_starrocks+pymysql_4_1_6`. Select those with `-k` rather than by node ID:

```bash
pytest test/test_suite.py -k "ComponentReflectionTest and test_get_table_names"
pytest test/integration/test_reflection_table.py -k ReflectionTableOptionsTest
```

`pytest --collect-only -qq` lists the node IDs. For a short run, `-n0` saves starting the four workers, and `-x` stops at the first failure.

#### Test Layout

| Path | What it covers |
| --- | --- |
| `test/sql` | Compiling DDL and DML with `assert_compile`, without executing it |
| `test/unit` | Parsing, reflection helpers, the defaults tables, and Alembic comparison and rendering, without executing SQL |
| `test/integration` | Reflection, result processing and Alembic autogenerate comparison, against tables the tests create |
| `test/system` | Alembic revisions end to end (autogenerate, upgrade and downgrade), each test in a fresh database |
| `test/test_suite.py` | SQLAlchemy's dialect compliance suite |
| `test/test_suite_alembic.py` | Alembic's dialect suite |

The shared pieces:

- `test/conftest.py` loads the plugin and fails collection for each kind of test, listed under Writing Tests, that the plugin would otherwise drop.
- `starrocks/requirements.py` holds the `Requirements` class. Both suites consult it, and so do `__requires__` and `testing.requires.<name>` in the StarRocks tests.
- A skip or `fails_on` mark that several tests share is a module-level constant named after its cause, defined before the first test that uses it, or in `test/util.py` when several modules share it. A mark on one test can be written inline. `test/util.py` also holds the shared helpers.
- Join the shared marks a test carries into one with `+`, as in `@(TRANSACTIONS_NEED_3_5 + SQL_TRANSACTION_SWITCH_NEEDS_4_0)`, which makes a new mark. Stacked as separate decorators, SQLAlchemy adds every mark above the innermost one to that mark itself, so a shared innermost mark, from `test/util.py` or a module-level constant, would carry them to every other test that uses it. Marks can be stacked over an inline mark or a `testing.requires.<name>` one, which is the test's own.
- `test/system/lifecycle.py` holds the base class and helpers for the system tests.

#### Writing Tests

Tests are classes derived from `sqlalchemy.testing.fixtures.TestBase`, with methods named `test_*`. The plugin silently ignores anything else, so collection fails for a module-level test function; a test method not named `test_*`, written as a classmethod or staticmethod, or that is not a plain function, such as a callable object or a `functools.partialmethod`; a test whose decorator doesn't use `functools.wraps`, which leaves it another name; a class of any name, nested or not, that has test methods but does not derive from `TestBase`; and a class with tests that inherits `__test__ = False` instead of setting it, which pytest skips. Give a mixin a name that starts with `_`.

A test under a mark, or under another of SQLAlchemy's decorators such as `testing.provide_metadata`, and a helper under one, can't have a return annotation, a parameter annotated with a type that isn't a plain builtin, such as `Connection` (`list[int]` and `int | None` are fine), or a default value of a builtin type whose `repr` isn't source, such as `object()`: SQLAlchemy 1.4 writes the wrapper's signature as source, which then fails, and the module fails to import on 1.4 only. Collection fails for such a function on 2.0 too.

Use `AssertsCompiledSQL` and `__dialect__` for compile tests, the `connection` and `metadata` fixtures for database tests, and `testing.combinations` for parametrized cases.

#### Plugin Options and Logging

SQLAlchemy's plugin adds its own options, which `pytest --help` lists. Besides `--dburi`, these are the useful ones:

- `--dropfirst` drops the views and tables earlier runs left in the configured database and the two extra schemas, for example after an interrupted `-n0` run.
- `--log-info=sqlalchemy.engine` logs each statement the tests execute, and `--log-debug=sqlalchemy.engine` adds the rows. `pyproject.toml` turns on pytest's live log at INFO, so they show as the tests run.

For the dialect's own debug messages too, add `--log-cli-level=DEBUG`.

### Build and Deploy

To build and deploy the package, you'll need the `build` and `twine` tools. You can install them using pip:

```bash
pip install build twine
```

Once installed, follow these steps to build and release a new version:

1. **Build the package:**

   This command packages your project into distribution files (`.tar.gz` for source and `.whl` for a built distribution) and places them in the `dist/` directory.

   ```bash
   python3 -m build
   ```

   The main outputs are `dist/starrocks-$version.tar.gz` and `dist/starrocks-$version-py3-none-any.whl`.

2. **Upload to the test environment (TestPyPI):**

   Before publishing to the official PyPI, it's a good practice to upload to TestPyPI to ensure everything works as expected.

   ```bash
   python3 -m twine upload --repository testpypi dist/*
   ```

3. **Test the package from TestPyPI:**

   Install the package from TestPyPI to verify that it was uploaded correctly and can be installed by others.

   ```bash
   python3 -m pip install --index-url https://test.pypi.org/simple/ starrocks
   ```

4. **Upload to the production environment (PyPI):**

   After verifying the package on TestPyPI, upload it to the official Python Package Index (PyPI) to make it publicly available.

   ```bash
   twine upload dist/*
   ```
