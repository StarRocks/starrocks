# StarRocks Python Client
A StarRocks client for the Python programming language.

StarRocks is the next-generation data platform designed to make data-intensive real-time analytics fast and easy. It delivers query speeds 5 to 10 times faster than other popular solutions. StarRocks can perform real-time analytics well while updating historical records. It can also enhance real-time analytics with historical data from data lakes easily. With StarRocks, you can get rid of the de-normalized tables and get the best performance and flexibility.

## Installation
```
pip install starrocks
```


## SQLAlchemy Usage

To connect to StarRocks using SQLAlchemy, use a connection string (URL) following this pattern:

- **User**: User Name
- **Password**: DBPassword
- **Host**: StarRocks FE Host
- **Catalog**: Catalog Name
- **Database**: Database Name
- **Port**: StarRocks FE port

Here's what the connection string looks like:

```
starrocks://<User>:<Password>@<Host>:<Port>/<Catalog>.<Database>
```

## Example
Python connector supports only Python 3 and SQLAlchemy 1.4 and 2:
```
from sqlalchemy import create_engine, Integer, insert
from sqlalchemy.schema import Table, MetaData, Column
from sqlalchemy.sql.expression import select, text

engine = create_engine('starrocks://root:xxx@localhost:9030/hive_catalog.hive_db')

### Querying data
with engine.connect() as connection:
    rows = connection.execute(text("SELECT * FROM hive_table")).fetchall()
    print(rows)


### DDL Operation
meta = MetaData()
tbl = Table(
    'table1',
    meta,
    Column("id", Integer),
    starrocks_engine='OLAP',
    starrocks_comment='table comment',
    starrocks_properties=(
        ("storage_medium", "SSD"),
        ("storage_cooldown_time", "2025-06-04 00:00:00"),
        ("replication_num", "1")
    ))

meta.create_all(engine)

### Insert data
stmt = insert(tbl).values(id=1)
stmt.compile()
with engine.connect() as connection:
    connection.execute(stmt)
    rows = connection.execute(tbl.select()).fetchall()
    print(rows)
```

You can also use `Mapped` on SQLAlchemy 2 like [orm-quickstart](https://docs.sqlalchemy.org/en/20/orm/quickstart.html#orm-quickstart):

```
from datetime import datetime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

engine = create_engine('starrocks://root:xxx@localhost:9030/hive_catalog.hive_db')

class Base(AsyncAttrs, DeclarativeBase):
    pass

class table1(Base):
    __tablename__ = "table1"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[str] = mapped_column(DateTime, default=datetime.now())
    
    __table_args__ = {
<<<<<<< HEAD
        "starrocks_PRIMARY_KEY": "id",
        "starrocks_engine": "OLAP",
        "starrocks_comment": "table comment",
        "starrocks_properties": (
            ("storage_medium", "SSD"),
            ("storage_cooldown_time", "2025-06-04 00:00:00"),
            ("replication_num", "1")
        ))
=======
        'comment': 'table comment',

        'starrocks_primary_key': 'id',
        'starrocks_distributed_by': 'HASH(id) BUCKETS 10',
        'starrocks_properties': {'replication_num': '1'}
>>>>>>> d28a0e2a25 ([Doc] Add doc for starrocks sqlalchemy: schema management and migration (#67214))
    }

Base.metadata.create_all(bind=engine)
```

<<<<<<< HEAD
=======
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

It's better to print the log from this `starrocks-sqlalchemy` when runing alembic command. You can add following logging configration in the `alembic.ini` file.

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

>>>>>>> d28a0e2a25 ([Doc] Add doc for starrocks sqlalchemy: schema management and migration (#67214))
## Contributing
### Unit tests
To run tests for Starrocks SQLAlchamy dialect (this module), run:
```bash
pytest
```
It will run SQLAlchemy tests as well as Starrocks tests. For more details, please check [SQLAlchemy dialect developing](https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst)
