---
displayed_sidebar: docs
---

# Superset サポート

[Apache Superset](https://superset.apache.org) は、モダンなデータ探索と可視化プラットフォームです。データのクエリには [SQLAlchemy](https://github.com/StarRocks/starrocks/tree/main/contrib/starrocks-python-client/starrocks) を使用します。

[MySQL Dialect](https://superset.apache.org/docs/databases/mysql) を使用することもできますが、`largeint` をサポートしていません。そのため、[StarRocks Dialect](https://github.com/StarRocks/starrocks/tree/main/contrib/starrocks-python-client/starrocks/) を開発しました。

## 環境

- Python 3.x
- mysqlclient (pip install mysqlclient)
- [Apache Superset](https://superset.apache.org)

注意: `mysqlclient` がインストールされていない場合、例外がスローされます:

```plain text
No module named 'MySQLdb'
```

## インストール

`dialect` は `SQLAlchemy` に貢献していないため、ソースコードからインストールする必要があります。

`superset` を Docker でインストールする場合、`sqlalchemy-starrocks` は `root` でインストールしてください。

[ソースコード](https://github.com/StarRocks/starrocks/tree/main/contrib/starrocks-python-client/starrocks) からインストール

```shell
pip install .
```

アンインストール

```shell
pip uninstall sqlalchemy-starrocks
```

## 使用法

SQLAlchemy を使用して StarRocks に接続するには、次の URL パターンを使用できます:

```shell
starrocks://<username>:<password>@<host>:<port>/<database>[?charset=utf8]
```

## 基本例

### Sqlalchemy の例

StarRocks データベースに接続するには、Python 3.x を使用することをお勧めします。例:

```python
from sqlalchemy import create_engine
import pandas as pd
conn = create_engine('starrocks://root:@x.x.x.x:9030/superset_db?charset=utf8')
sql = """select * from xxx"""
df = pd.read_sql(sql, conn)
```

### Superset の例

superset では、`Other` データベースを使用し、URL を次のように設定します:

```shell
starrocks://root:@x.x.x.x:9030/superset_db?charset=utf8
```