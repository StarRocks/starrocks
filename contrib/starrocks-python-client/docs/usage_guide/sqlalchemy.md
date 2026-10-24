# SQLAlchemy Usage Guide

This guide provides a comprehensive overview of how to use the StarRocks dialect with SQLAlchemy, from basic connections to defining complex tables and executing queries.

## Connecting to StarRocks

You can connect to StarRocks using a standard SQLAlchemy connection string.

```python
from sqlalchemy import create_engine, text

# Format: starrocks://<User>:<Password>@<Host>:<Port>/<Database>
engine = create_engine('starrocks://root@localhost:9030/mydatabase')

with engine.connect() as connection:
    rows = connection.execute(text("SELECT 1")).fetchall()
    print("Connection successful!")
    print(rows)
```

## Inserting many rows

An `executemany` of an INSERT, such as `connection.execute(table.insert(), rows)` or an ORM flush that adds several objects, is sent as multi-row INSERTs. Each INSERT is one StarRocks load: its rows are stored together or not at all. StarRocks refuses a load into a table that already has about a thousand loads waiting for compaction, so one INSERT per row would be slow, and soon refused. An INSERT holds at most 10,000 rows, the most StarRocks takes in one `INSERT ... VALUES`, and at most the driver's `max_stmt_length`, about a megabyte of SQL, so more rows go in several INSERTs. Each of those is its own statement, so a failure part way leaves the INSERTs before it written.

The driver builds these INSERTs itself when the VALUES clause is bare placeholders and there are at most 10,000 rows; the dialect splits more rows than that into INSERTs of 10,000. When the VALUES clause holds more than placeholders, as the bind expression of a JSON, ARRAY, MAP or STRUCT column does, the driver would send one INSERT per row, so the dialect renders the rows with the driver's escaping and sends the multi-row INSERTs itself. INSERT text with more than placeholders in its VALUES, such as `text("INSERT INTO t VALUES (:id, parse_json(:doc))")`, is still sent by the driver, one INSERT per row.

`INSERT OVERWRITE` and `INSERT ... WITH LABEL` need all the rows in one INSERT: a second overwrite would replace the rows of the first, and a second INSERT would reuse a label already used. An `executemany` of either, as `insert().with_dialect_options(starrocks_overwrite=True)` or `starrocks_label=...`, or as text, is sent as one INSERT, and raises an `InvalidRequestError` when its rows need more than one, or when it is text the driver would send one row at a time.

From StarRocks 4.0, a JSON null stored with JSON objects can read back as `{}`. Flat JSON, on by default from 4.0, stores the keys of a JSON column's objects as columns of their own, and rebuilds a null among those objects as an empty object. That happens to a null inserted in one INSERT with objects, and to a null inserted on its own once compaction merges it with them. A table created with `starrocks_properties={"flat_json.enable": "false"}` keeps its JSON nulls. See [StarRocks issue 81490](https://github.com/StarRocks/starrocks/issues/81490).

## Transactions

StarRocks commits each statement as it runs, unless a transaction has been opened with `BEGIN WORK`. StarRocks has transactions from version 3.5. The dialect offers two isolation levels:

- **`AUTOCOMMIT`**, the default. Every statement is committed when it runs, so a rollback undoes nothing.
- **`READ COMMITTED`**. The first INSERT, UPDATE or DELETE of a SQLAlchemy transaction opens a StarRocks transaction, and `commit()` or `rollback()` ends it. A transaction that only reads opens none.

Set the level on the engine, or on one connection:

```python
engine = create_engine("starrocks://root@localhost:9030/mydatabase", isolation_level="READ COMMITTED")

with engine.begin() as connection:
    connection.execute(orders.insert(), {"id": 1, "total": 10})
    connection.execute(order_lines.insert(), {"order_id": 1, "item": "pen"})
    # both inserts commit together, or neither does

with engine.connect() as connection:
    connection.execution_options(isolation_level="READ COMMITTED")
    with connection.begin():
        ...
```

Only a level given to `create_engine(isolation_level=...)` is set on every new DBAPI connection. SQLAlchemy sets a level given to `connection.execution_options()`, `engine.execution_options()` or `create_engine(execution_options=...)` on the DBAPI connection the connection has at the time. After a disconnect or `invalidate()`, the same connection goes on with a new DBAPI connection at the engine's level, which by default is `AUTOCOMMIT`, so StarRocks would commit each statement at once and a rollback would undo nothing. The dialect gives an `SAWarning` at the first statement that runs at a level other than the connection's; roll back, then set the level again. On SQLAlchemy 1.4 without `future=True`, `execution_options()` returns a new connection, and only that one is warned.

`READ UNCOMMITTED`, `REPEATABLE READ` and `SERIALIZABLE` are deprecated: they run as `AUTOCOMMIT`, as they did in earlier versions of the dialect, with a `FutureWarning` that names the line that gave the level. Given to an engine, as `create_engine(isolation_level=...)`, `create_engine(execution_options=...)` or `engine.execution_options()`, such a level is replaced by `AUTOCOMMIT` when the engine is made, with one warning, so a connection can still change its level and return to the pool. Given to `connection.execution_options()`, it warns at each call. Any other level raises an `ArgumentError`.

StarRocks accepts `BEGIN WORK` and does nothing with it before version 3.5, and from 4.0.7, the first 4.0 release with the variable `enable_sql_transaction`, in a session where it is off. So on such a server or session the dialect raises an `InvalidRequestError` when a `READ COMMITTED` connection is made or a connection is switched to it, before a migration's first statement runs. A session that turns `enable_sql_transaction` off after a `READ COMMITTED` connection is made is not noticed. The dialect also raises at a `BEGIN` or `START TRANSACTION` sent as text on such a server or in such a session, which it checks at each one.

StarRocks restricts what a transaction can do, and refuses the statement when the transaction oversteps:

- The tables a transaction changes must all be in one database.
- A transaction can't read a table it has changed, so an ORM `Session` fails when a query reads a table its flush has written. Use `AUTOCOMMIT` with the ORM, or commit before reading back.
- On shared-nothing clusters, a transaction can write each table once. An `executemany` that runs as several statements writes the table once for each, so the second statement fails. That happens for every `executemany` of an UPDATE or DELETE, and of INSERT text whose VALUES holds more than placeholders, which the driver sends once per row, and for an INSERT `executemany` of more rows than one statement takes (see [Inserting many rows](#inserting-many-rows)). An ORM flush that updates two objects of one class is such an `executemany`.
- An INSERT that leaves out the AUTO_INCREMENT column of a PRIMARY KEY table is a partial update, which a transaction can't apply to a table it has already written (error 5308). An ORM flush that adds two objects of such a class sends one such INSERT per object, so the second fails. Give the key values, or use `AUTOCOMMIT` with the ORM.
- UPDATE and DELETE inside a transaction are refused by StarRocks 3.5. StarRocks documents them for shared-data clusters from 4.0, and 4.1 accepts them on shared-nothing clusters as well.
- Inside a transaction StarRocks runs only queries, `SHOW`, `DESCRIBE`, `EXPLAIN`, `SET`, INSERT, UPDATE, DELETE, `COMMIT` and `ROLLBACK`, and no `INSERT OVERWRITE`.
- StarRocks aborts a transaction that stays open longer than the session's `query_timeout`, 300 seconds by default. The next statement and the rollback then fail with "transaction not found". For a long `READ COMMITTED` migration, raise it, for example with `connect_args={"init_command": "SET query_timeout = 3600"}`.
- Alembic's version table is updated in the migration's transaction, and deleted from on a downgrade to base, so a `READ COMMITTED` migration needs a StarRocks that accepts UPDATE and DELETE inside a transaction. On 3.5, a downgrade to base fails at that DELETE, after the downgrade's DDL has run.

A transaction covers the statements that go through SQLAlchemy. A statement sent through a raw DBAPI cursor, from `connection.connection.cursor()`, opens no transaction, so on a `READ COMMITTED` connection its DML commits at once, and a rollback doesn't undo it.

The dialect adds rules of its own:

- **DDL first commits the open transaction**, as MySQL does, because StarRocks refuses DDL inside one. What ran before the DDL is committed, and the next INSERT, UPDATE or DELETE opens a new transaction.
- **A DELETE on a Duplicate, Aggregate or Unique Key table raises an `InvalidRequestError`.** StarRocks applies a DELETE on any table but a Primary Key table at once, outside the transaction, and ROLLBACK doesn't undo it ([StarRocks issue 81471](https://github.com/StarRocks/starrocks/issues/81471)). Run those deletes on an `AUTOCOMMIT` connection. A model's table is a Duplicate Key table unless it names another key, or the engine is made with `starrocks_infer_primary_key=True` and StarRocks can hold the table as a Primary Key table (see [Table Type](tables.md#2-table-type-starrocks_key)). The dialect reads the table's type from `information_schema.tables_config` before each DELETE in a transaction.
- **A `delete()` without a WHERE clause raises an `InvalidRequestError`.** It compiles to `TRUNCATE TABLE`, which StarRocks runs outside any transaction.
- **Text holding several statements raises an `InvalidRequestError`**, such as `SELECT 1; INSERT ...` passed to `exec_driver_sql()` or `op.execute()`. The dialect could open the transaction only for the first statement, and the driver drops the errors of the others. Execute each statement on its own. Under `AUTOCOMMIT` such text runs as before.
- **`INSERT INTO FILES` and `INSERT OVERWRITE` open no transaction.** StarRocks can't run them inside one, so they run on their own and commit at once, and a rollback doesn't undo them. After the transaction's first INSERT, UPDATE or DELETE they raise an `InvalidRequestError`.
- **Other statements StarRocks can't run inside a transaction raise an `InvalidRequestError` while one is open**, before they are sent, and the transaction stays open. That covers `ANALYZE`, `USE`, `KILL`, `CANCEL`, `RECOVER`, `SUBMIT TASK` and `SAVEPOINT`. Commit first, or run them on an `AUTOCOMMIT` connection. StarRocks has no savepoints, so `begin_nested()` fails in any case.

`BEGIN WORK`, `COMMIT` and `ROLLBACK` sent as text are tracked too, so DDL commits a transaction opened by hand on an `AUTOCOMMIT` connection first. The connection's `commit()` and `rollback()` always reach StarRocks, and the pool rolls back a connection when it is returned, which ends a transaction opened through a raw DBAPI cursor.

With Alembic, migrations behave as before under `AUTOCOMMIT`. Under `READ COMMITTED`, a migration's data changes, such as `op.bulk_insert`, are transactional until the next DDL operation commits them.

**Upgrading from version 1.3.4 or earlier:**

- Connections report `AUTOCOMMIT` where they used to report `REPEATABLE READ`. StarRocks commits each statement on its own either way.
- An engine or connection set to `READ COMMITTED`, perhaps on MySQL's advice, used to run like any other. It now runs DML in StarRocks transactions, with the restrictions above, and from 3.5 only: before 3.5 it can't connect. To keep the old behaviour, set `AUTOCOMMIT` or leave the level unset.
- `READ UNCOMMITTED`, `REPEATABLE READ` and `SERIALIZABLE` work as before, with a `FutureWarning`; set `AUTOCOMMIT` instead.
- The driver's autocommit setting doesn't change the level: the dialect turns it on for each new connection. A URL with `autocommit=false`, `connect_args={"autocommit": False}`, and a connection that a `creator` or `async_creator` function makes, which PyMySQL and asyncmy start with autocommit off, all run as `AUTOCOMMIT`, as before. Use `isolation_level="READ COMMITTED"` for transactions.
- A `BEGIN` or `START TRANSACTION` sent as text raises an `InvalidRequestError` where StarRocks ignores it: before 3.5, and from 4.0.7 in a session whose `enable_sql_transaction` is off.

## Defining Schema: Tables, Views, and Materialized Views

You can define your StarRocks schema using SQLAlchemy's Core or ORM approaches.

### Example: Basic Table Definition (Core Style)

SQLAlchemy Core allows you to define tables using the `Table` construct. This approach is explicit and well-suited for projects that don't use the ORM.

```python
from sqlalchemy import Table, MetaData, Column
from starrocks import INTEGER, VARCHAR, DATETIME

metadata = MetaData()

my_table = Table(
    'my_table',
    metadata,
    Column('id', INTEGER, primary_key=True),
    Column('name', VARCHAR(50)),
    Column('timestamp', DATETIME),

    # StarRocks-specific arguments
    starrocks_primary_key='id',
    starrocks_distributed_by='HASH(id) BUCKETS 10',
    starrocks_properties={"replication_num": "1"}
)

# Create the table in the database
metadata.create_all(engine)
```

### Example: Advanced Table Operations (ORM Declarative Style)

For more complex table definitions, including `PRIMARY KEY`, `AGGREGATE KEY` tables, and various StarRocks-specific attributes and data types, you can use SQLAlchemy's ORM declarative style. This approach allows you to define your schema directly within Python classes.

```python
from sqlalchemy import Column, Integer, String, Date, text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from starrocks import BITMAP, INTEGER, DATE, STRING # Import StarRocks specific types
from datetime import date


# --- StarRocks Table declaration ---

Base = declarative_base()

class PageViewAggregates(Base):
    __tablename__ = 'page_view_aggregates'

    page_id = Column(INTEGER, primary_key=True, starrocks_is_agg_key=True)
    visit_date = Column(DATE, primary_key=True, starrocks_is_agg_key=True)
    total_views = Column(INTEGER, starrocks_agg_type='SUM')
    last_user = Column(STRING, starrocks_agg_type='REPLACE')
    distinct_users = Column(BITMAP, starrocks_agg_type='BITMAP_UNION')

    __table_args__ = {
        'starrocks_aggregate_key': 'page_id, visit_date',
        'starrocks_partition_by': 'date_trunc("day", visit_date)',
        'starrocks_distributed_by': 'HASH(page_id)',
        'starrocks_properties': {"replication_num": "1"}
    }

# --- Data Insertion and Query Examples ---

# Assuming `engine` is already created as shown in "Basic Operations"
engine = create_engine('starrocks://root@localhost:9030/mydatabase')

Base.metadata.create_all(engine) # Create the table in the database

Session = sessionmaker(bind=engine)
session = Session()

# Insert data
new_data = PageViewAggregates(
    page_id=1,
    visit_date=date(2023, 10, 26),
    total_views=100,
    last_user="user_A",
    distinct_users=None # BITMAP types might require specific functions for value insertion
)
session.add(new_data)
session.commit()

# Insert data using raw SQL (two records with same key for aggregation)
with engine.connect() as connection:
    connection.execute(
        text("""
            INSERT INTO page_view_aggregates
            (page_id, visit_date, total_views, last_user, distinct_users)
            VALUES
                (:page_id_1, :visit_date_1, :total_views_1, :last_user_1, NULL),
                (:page_id_2, :visit_date_2, :total_views_2, :last_user_2, NULL)"""),
            {
                "page_id_1": 2, "visit_date_1": "2023-10-27",
                "total_views_1": 200, "last_user_1": "user_B",
                "page_id_2": 2, "visit_date_2": "2023-10-27",
                "total_views_2": 150, "last_user_2": "user_C"
            })
    connection.commit()

# Query all data to verify aggregation
all_results = session.query(PageViewAggregates).order_by(PageViewAggregates.page_id).all()
print(f"Total unique rows after insertions: {len(all_results)}")
for row in all_results:
    print(f"  Page ID: {row.page_id}, Visit Date: {row.visit_date}, Total Views: {row.total_views}, Last User: {row.last_user}")

session.close()
```

For a detailed reference on all StarRocks-specific table attributes and data types, please see the **[Table Definition Reference](./tables.md)**.
