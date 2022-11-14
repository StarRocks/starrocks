# StarRocks Dialect for SQLAlchemy
A dialect for StarRocks that can be used with [Apache Superset](https://superset.apache.org).

## Installation
### Install from Source Code
```
pip install .
```
### Uninstall
```
pip uninstall sqlalchemy-starrocks
```


## Usage

To connect to StarRocks with SQLAlchemy, the following URL pattern can be used:

```
starrocks://<username>:<password>@<host>:<port>/<database>[?charset=utf8]
```

## Basic Example
### Sqlalchemy Example
It is recommended to use python 3.x to connect to the StarRocks database, eg:
```
from sqlalchemy import create_engine
import pandas as pd

conn = create_engine('starrocks://root:@x.x.x.x:9030/superset_db?charset=utf8')
sql = """select * from xxx"""
df = pd.read_sql(sql, conn)
```

### Superset Example
If you install `superset` with Docker, install `sqlalchemy-starrocks` with `root`. In superset, use `Other` database, and set url is：
```
starrocks://root:@x.x.x.x:9030/superset_db?charset=utf8
```

## Resources
1. [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/13/index.html)
2. [SQLAlchemy dialects](https://docs.sqlalchemy.org/en/13/dialects/index.html)