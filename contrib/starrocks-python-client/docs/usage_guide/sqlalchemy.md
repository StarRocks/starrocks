# SQLAlchemy Usage Guide

This guide provides a comprehensive overview of how to use the StarRocks dialect with SQLAlchemy, from basic connections to defining complex tables and executing queries.

## Connecting to StarRocks

You can connect to StarRocks using a standard SQLAlchemy connection string.

```python
from sqlalchemy import create_engine, text

# Format: starrocks://<User>:<Password>@<Host>:<Port>/<Database>
engine = create_engine('starrocks://myname:pswd1234@localhost:9030/mydatabase')

with engine.connect() as connection:
    rows = connection.execute(text("SELECT 1")).fetchall()
    print("Connection successful!")
    print(rows)
```

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
    starrocks_PRIMARY_KEY='id',
    starrocks_DISTRIBUTED_BY='HASH(id) BUCKETS 10',
    starrocks_PROPERTIES={"replication_num": "1"}
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
        'starrocks_AGGREGATE_KEY': 'page_id, visit_date',
        'starrocks_PARTITION_BY': 'date_trunc("day", visit_date)',
        'starrocks_DISTRIBUTED_BY': 'HASH(page_id)',
        'starrocks_PROPERTIES': {"replication_num": "1"}
    }

# --- Data Insertion and Query Examples ---

# Assuming `engine` is already created as shown in "Basic Operations"
engine = create_engine('starrocks://myname:pswd1234@localhost:9030/mydatabase')

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
