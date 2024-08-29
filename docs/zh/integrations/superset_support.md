---
displayed_sidebar: docs
---

# 支持 Superset

[Apache Superset](https://superset.apache.org) 是一个现代数据探索和可视化平台。它使用 [SQLAlchemy](https://github.com/StarRocks/starrocks/tree/main/contrib/starrocks-python-client/starrocks) 来查询数据。
虽然可以使用 [Mysql Dialect](https://superset.apache.org/docs/databases/mysql)，但是它不支持 LARGEINT。所以我们开发了 [StarRocks Dialect](https://github.com/StarRocks/starrocks/tree/main/contrib/starrocks-python-client/starrocks/)。

## 环境准备

- Python 3.x
- mysqlclient (pip install mysqlclient)
- [Apache Superset](https://superset.apache.org)

注意: 如果没有安装 `mysqlclient`，将抛出异常: No module named 'MySQLdb'。

## 安装

由于 `dialect` 还没有贡献给 SQLAlchemy 社区，需要使用源码进行安装。

如果你使用 Docker 安装 Superset，需要使用 `root` 用户安装 `sqlalchemy-starrocks`。

安装通过[源码](https://github.com/StarRocks/starrocks/tree/main/contrib/starrocks-python-client/starrocks)。

```shell
pip install .
```

卸载

```shell
pip uninstall sqlalchemy-starrocks
```

## 使用

通过 SQLAlchemy 连接 StarRocks，可以使用下述链接：

```shell
starrocks://<username>:<password>@<host>:<port>/<database>[?charset=utf8]
```

## 示例

### Sqlalchemy 示例

建议使用 python 3.x 连接 StarRocks 数据库，比如：

```shell
from sqlalchemy import create_engine
import pandas as pd
conn = create_engine('starrocks://root:@x.x.x.x:9030/superset_db?charset=utf8')
sql = """select * from xxx"""
df = pd.read_sql(sql, conn)
```

### Superset 示例

使用 Superset 时，使用 `Other` 数据库连接，并且设置 url 为：

```shell
starrocks://root:@x.x.x.x:9030/superset_db?charset=utf8
```
