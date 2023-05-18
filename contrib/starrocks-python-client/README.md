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
It is recommended to use python 3.x to connect to the StarRocks database, eg:
```
from sqlalchemy import create_engine
from sqlalchemy.schema import Table, MetaData
from sqlalchemy.sql.expression import select, text

engine = create_engine('starrocks://root:xxx@localhost:9030/hive_catalog.hive_db')
connection = engine.connect()

rows = connection.execute(text("SELECT * FROM hive_table")).fetchall()
```
