# StarRocks Python Client
A StarRocks client for the Python programming language.

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

## Resources
1. [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/13/index.html)
2. [SQLAlchemy dialects](https://docs.sqlalchemy.org/en/13/dialects/index.html)