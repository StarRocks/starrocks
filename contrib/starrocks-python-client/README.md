# StarRocks Python Client

A StarRocks client for the Python programming language, including a SQLAlchemy dialect and an Alembic extension.

StarRocks is a next-generation data platform designed for fast,real-time analytics. This package allows developers to interact with StarRocks using Python, leveraging SQLAlchemy's powerful ORM and expression language, and managing database schema migrations with Alembic.

## Quick Start

### Installation

```bash
pip install starrocks
```

#### Supported Python Versions

Python >= 3.8, <= 3.12

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

To connect to StarRocks, use a standard SQLAlchemy connection string.

```ini
starrocks://<User>:<Password>@<Host>:<Port>/[<Catalog>.]<Database>
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

engine = create_engine('starrocks://myname:pswd1234@localhost:9030/mydatabase')

# make sure you have create a table `mytable` in `mydatabase`.

with engine.connect() as connection:
    rows = connection.execute(text("SELECT * FROM mytable LIMIT 2")).fetchall()
    print("Connection successful!")
    print(rows)
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

        'starrocks_PRIMARY_KEY': 'id',
        'starrocks_DISTRIBUTED_BY': 'HASH(id) BUCKETS 10',
        'starrocks_PROPERTIES': {'replication_num': '1'}
    }

# Create the table in the database
Base.metadata.create_all(engine)
```

### Example: Defining a Table (Core Style)

Alternatively, you can use SQLAlchemy Core to define tables programmatically.

```python
from sqlalchemy import Table, MetaData, Column
from starrocks import INTEGER, VARCHAR

metadata = MetaData()

my_core_table = Table(
    'my_core_table',
    metadata,
    Column('id', INTEGER, primary_key=True),
    Column('name', VARCHAR(50)),

    # StarRocks-specific arguments
    starrocks_PRIMARY_KEY='id',
    starrocks_DISTRIBUTED_BY='HASH(id) BUCKETS 10',
    starrocks_PROPERTIES={"replication_num": "1"}
)

# Create the table in the database
metadata.create_all(engine)
```

> For a complete guide on defining tables (Core and ORM), executing queries, and using advanced features, please see the **[SQLAlchemy Usage Guide](./docs/usage_guide/sqlalchemy.md)**.

### Alembic Integration for Schema Migrations

This dialect integrates with Alembic to support automated schema migrations. Here’s a quick-start guide to get you up and running.

#### 1. Install and Initialize Alembic

```bash
pip install "alembic>=1.16"
alembic init alembic
```

#### 2. Configure your Database URL

In `alembic.ini`, set the `sqlalchemy.url` to your StarRocks connection string.

```ini
# alembic.ini
sqlalchemy.url = starrocks://myname:pswd1234@localhost:9030/mydatabase
```

#### 3. Configure your Models for Autogeneration

In `alembic/env.py`, import your models' metadata and assign it to `target_metadata`.

```python
# alembic/env.py
# Add these imports
from myapp.models import Base  # Adjust to your models' location
from starrocks.alembic import render

# ...
# And set the target_metadata
target_metadata = Base.metadata

def run_migrations_online() -> None:
    # ... inside this function
    context.configure(
        # ...
        render_item=render.render_column_type # Add this line
    )
    # ...
```

#### 4. Generate and Apply Your First Migration

With your models defined (as shown in the SQLAlchemy examples above), you can now generate and apply a migration.

```bash
# Generate the migration script
alembic revision --autogenerate -m "Create initial tables"

# Apply the migration to the database
alembic upgrade head
```

For a full tutorial on advanced topics like data migrations, handling complex types, and managing views, please refer to the **[Alembic Integration Guide](./docs/usage_guide/alembic.md)**.

For a detailed reference on all StarRocks-specific table attributes and data types, please see the **[Table Definition Reference](./docs/usage_guide/tables.md)**.

## Contributing

### Tests

#### Running Unit Tests

To run tests for the StarRocks SQLAlchemy dialect, first install the package in editable mode along with its testing dependencies:

```bash
pip install -e .
pip install pytest mock
```

Then, you can run the test suite using `pytest`:

```bash
pytest
```

This will run the standard SQLAlchemy dialect test suite as well as StarRocks-specific tests. For more details, please check [SQLAlchemy's guide for dialect development](https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst).

#### Test Logging

To see the raw SQL that the dialect compiles and executes during tests, you can modify the `[tool.pytest.ini_options]` section in the `pyproject.toml` file in your project with the following content:

```ini
[tool.pytest.ini_options]
log_cli = true
log_cli_level = DEBUG
log_cli_format = %(levelname)-5.5s [%(name)s] %(message)s
```

#### Running Integration and System Tests

To run the integration and system tests, you must have a running StarRocks cluster. The tests require a connection URL to be provided via the `STARROCKS_URL` environment variable.

1. **Set up your StarRocks database:**
   Ensure your StarRocks instance is running and you have a database available for testing (e.g., `test`).

   ```SQL
   CREATE DATABASE IF NOT EXISTS test;
   ```

2. **Configure the connection URL:**
   Set the `STARROCKS_URL` environment variable in your shell. The format should be:

   ```bash
   export STARROCKS_URL="starrocks://<User>:<Password>@<Host>:<Port>/<Database>"
   ```

   For example (the default url will be this if you don't set it):

   ```bash
   export STARROCKS_URL="starrocks://myname:pswd1234@127.0.0.1:9030/test"
   ```

3. **Run the tests:**
   With the development environment set up and the environment variable configured, you can run the tests using `pytest`:

   ```bash
   pytest
   ```

#### Running the Full SQLAlchemy Test Suite

In addition to the StarRocks-specific tests, you can run the comprehensive test suite provided by SQLAlchemy to ensure full compatibility. To do this, you need to enable the SQLAlchemy test plugin in `test/conftest.py`.

1. **Enable the SQLAlchemy test plugin:**
   Open the file `test/conftest.py` and uncomment the line that imports from `sqlalchemy.testing.plugin.pytestplugin`.

   ```python
   # test/conftest.py

   # ... other imports

   # To run the test_suite.py and full SQLAlchemy test suite, uncomment the following line:
   from sqlalchemy.testing.plugin.pytestplugin import *
   ```

   > NOTE: If you enable this line, StarRocks-specific tests will not run. We will modify the StarRocks-specific tests using SQLAlchemy's test framework in the future.

   You can enable SQLAlchemy's tests from the file `test/test_suite.py` by uncomment the following line:

   ```python
   # To run the full SQLAlchemy test suite, uncomment the following line:
   from sqlalchemy.testing.suite import *
   ```

2. **Run the tests:**

   ```bash
   pytest test/test_suite.py
   ```

This will run the standard SQLAlchemy dialect test suite as well as StarRocks-specific tests. For more details, please check [SQLAlchemy's guide for dialect development](https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst).

**Note:** After running the full suite, it's good practice to re-comment the line in `test/conftest.py` to keep standard test runs focused on the dialect-specific tests.

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
