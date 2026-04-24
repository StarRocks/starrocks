---
displayed_sidebar: docs
---

# Marimo

将您的 StarRocks 集群与 [Marimo](https://marimo.io/)，一个为可复现性和交互性而构建的响应式 Python 笔记本集成。

## 先决条件

首先，根据 [Marimo 快速入门文档](https://github.com/marimo-team/marimo#quickstart)安装 Marimo 并设置笔记本。

您还需要以下软件包：

```bash
pip install starrocks sqlalchemy pandas
```

## 连接到 StarRocks

使用 [SQLAlchemy](https://www.sqlalchemy.org/) 创建连接引擎。连接字符串格式为：

```
starrocks://username:password@host:port/database
```

```python
import marimo as mo
import sqlalchemy as sa

engine = sa.create_engine("starrocks://username:password@<host>:9030")
```

将 `<host>` 替换为您的 StarRocks FE 主机。

## 使用 Marimo UI 获取凭据

为避免硬编码凭据，请使用 Marimo 的交互式 UI 元素在运行时收集它们。

**单元格 1** — 渲染输入字段：

```python
user = mo.ui.text(label="Username")
pw = mo.ui.text(label="Password", kind="password")
mo.hstack([user, pw])
```

**单元格 2** — 使用输入的值创建引擎：

```python
engine = sa.create_engine(
    f"starrocks://{user.value}:{pw.value}@<host>:9030"
)
```

## 查询 StarRocks

建立引擎后，使用 pandas 运行查询：

```python
import pandas as pd

df = pd.read_sql("SELECT * FROM my_database.my_table LIMIT 100", engine)
mo.ui.table(df)
```

![连接到 StarRocks 的 Marimo 笔记本](../../_assets/marimo_starrocks.png)

:::注意

多目录支持需要 Marimo 0.22.5 或更高版本。

:::
