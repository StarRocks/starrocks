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
        "starrocks_PRIMARY_KEY": "id",
        "starrocks_engine": "OLAP",
        "starrocks_comment": "table comment",
        "starrocks_distributed_by": "id",
        "starrocks_distributed_by_buckets": 5,
        "starrocks_partition_by": "name",
        "starrocks_order_by": "created_at",
        "starrocks_properties": (
            ("storage_medium", "SSD"),
            ("storage_cooldown_time", "2025-06-04 00:00:00"),
            ("replication_num", "1")
        ))
    }

Base.metadata.create_all(bind=engine)
```

## Contributing
### Unit tests
To run tests for Starrocks SQLAlchamy dialect (this module), run:
```bash
pytest
```
It will run SQLAlchemy tests as well as Starrocks tests. For more details, please check [SQLAlchemy dialect developing](https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst)
