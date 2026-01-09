---
displayed_sidebar: docs
description: "Schema 管理与迁移"
sidebar_label: "Schema 管理与迁移"
---

# 使用 SQLAlchemy 和 Alembic 进行 Schema 管理和迁移

本指南介绍如何通过 **`starrocks` SQLAlchemy** 方言，使用 Python 生态系统（包括 SQLAlchemy、Alembic 和 sqlacodegen）来管理 StarRocks 的 schema。旨在帮助您理解 **为什么 schema 迁移有用** 以及 **如何在 StarRocks 中有效使用它**。

## 概述

许多用户直接使用 SQL DDL 管理 StarRocks 的表、视图和物化视图。然而，随着项目的增长，手动维护 `ALTER TABLE` 语句变得容易出错且难以跟踪。

**StarRocks SQLAlchemy 方言 (`starrocks`)** 提供：

- 针对 StarRocks **表**、**视图** 和 **物化视图** 的完整 SQLAlchemy 模型层
- 表结构和表属性（包括视图和物化视图）的 **声明式** 定义
- 与 **Alembic** 集成，允许自动 **检测** 和 **生成** schema 变更
- 与 **sqlacodegen** 等工具的兼容性，用于反向生成模型

这使得 Python 用户可以以 **声明式**、**版本控制** 和 **自动化** 的方式维护 StarRocks 的 schema。

## 主要优势

虽然 schema 迁移传统上与 OLTP 数据库相关联，但在像 StarRocks 这样的数据仓库系统中也很有价值。团队使用 [Alembic](https://alembic.sqlalchemy.org/) 和 StarRocks 方言是因为以下列出的好处。

### 声明式 schema 定义

一旦您在 Python [ORM](https://docs.sqlalchemy.org/en/20/orm/quickstart.html#orm-quickstart) 模型或 [SQLAlchemy](https://docs.sqlalchemy.org) 核心风格中定义了 schema，就不再需要手动编写 `ALTER TABLE` 语句。

### 自动差异化和自动生成

Alembic 将 **当前的 StarRocks schema** 与 **您的 SQLAlchemy 模型** 进行比较，并自动生成迁移脚本 (`CREATE`/`DROP`/`ALTER`)。

### 可审查的、版本控制的迁移

每个 schema 变更都会成为一个迁移文件（Python），因此用户可以跟踪变更并在需要时回滚。

### 跨环境的一致工作流程

可以通过相同的流程将 schema 变更应用于开发、预发布和生产环境。

## 安装和连接

### 先决条件

- StarRocks Python 客户端：1.3.2 或更高版本
- `SQLAlchemy`：1.4 或更高版本（推荐使用 SQLAlchemy 2.0，且使用 `sqlacodegen` 时是必需的）
- `Alembic`：1.16 或更高版本

### 安装 StarRocks Python 客户端

运行以下命令安装 StarRocks Python 客户端。

```bash
pip install starrocks
```

### 连接到 StarRocks

使用以下 URL 连接到您的 StarRocks 集群。

```bash
starrocks://<user>:<password>@<FE_host>:<query_port>/[<catalog>.]<database>
```

- `user`：用于连接集群的用户名。
- `password`：用户密码。
- `FE_host`：FE IP 地址。
- `query_port`：FE `query_port`（默认：9030）。
- `catalog`：数据库所在的 catalog 名称。
- `database`：您要连接的数据库名称。

安装后，您可以使用以下代码示例快速验证连接性：

```python
from sqlalchemy import create_engine, text

# 您需要先创建 `mydatabase`
engine = create_engine("starrocks://root@localhost:9030/mydatabase")

with engine.connect() as conn:
    conn.execute(text("SELECT 1")).fetchall()
    print("Connection successful!")
```

## 定义 StarRocks 模型（声明式 ORM）

StarRocks 方言支持：

- 表
- 视图
- 物化视图

它还支持 StarRocks 特定的表属性，例如：

- `ENGINE` (OLAP)
- 键模型 (`DUPLICATE KEY`, `PRIMARY KEY`, `UNIQUE KEY`, `AGGREGATE KEY`)
- `PARTITION BY` 变体（RANGE / LIST / 表达式分区）
- `DISTRIBUTED BY` 变体（HASH / RANDOM）
- `ORDER BY`
- 表属性（例如，`replication_num`, `storage_medium`）

:::important
- StarRocks 方言选项作为以 `starrocks_` 为前缀的关键字参数传递。
- `starrocks_` **前缀必须为小写**。后缀可以接受大小写（例如，`PRIMARY_KEY` 和 `primary_key`）。
- 如果您指定了一个表键（例如 `starrocks_primary_key="id"`），则涉及的列 **必须** 也在 `Column(...)` 中标记为 `primary_key=True`，以便 SQLAlchemy 元数据和 Alembic 自动生成能够正确运行。
:::

下面的示例反映了真实的公共 API 和参数名称。

### 表示例

StarRocks 表选项可以在 ORM（通过 `__table_args__`）和 Core（通过 `Table(..., starrocks_...=...)`）风格中指定。

#### ORM（声明式）风格

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from starrocks import INTEGER, STRING

# 使用与快速测试相同的引擎
engine = create_engine("starrocks://root@localhost:9030/mydatabase")

Base = declarative_base()

class MyTable(Base):
    __tablename__ = 'my_orm_table'
    id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    name: Mapped[str] = mapped_column(STRING)

    __table_args__ = {
        'comment': 'table comment',

        'starrocks_primary_key': 'id',
        'starrocks_distributed_by': 'HASH(id) BUCKETS 10',
        'starrocks_properties': {'replication_num': '1'}
    }

# 在数据库中创建表
Base.metadata.create_all(engine)
```

#### Core 风格

```python
from sqlalchemy import Column, MetaData, Table, create_engine
from starrocks import INTEGER, VARCHAR

# 使用与快速测试相同的引擎
engine = create_engine("starrocks://root@localhost:9030/mydatabase")

metadata = MetaData()

my_core_table = Table(
    'my_core_table',
    metadata,
    Column('id', INTEGER, primary_key=True),
    Column('name', VARCHAR(50)),

    # StarRocks 特定参数
    starrocks_primary_key='id',
    starrocks_distributed_by='HASH(id) BUCKETS 10',
    starrocks_properties={"replication_num": "1"}
)

# 在数据库中创建表
metadata.create_all(engine)
```

:::note
有关表属性和数据类型的全面参考，请参见 [Reference [4]](#references)。
:::

### 视图示例

下面是推荐的视图定义风格，使用 `columns` 作为字典列表（`name`/`comment`）。此示例基于现有表 `my_core_table`。

```python
from starrocks.schema import View

# 重用上面 Core 表示例中的元数据
metadata = my_core_table.metadata

user_view = View(
    "user_view",
    metadata,
    definition="SELECT id, name FROM my_core_table WHERE name IS NOT NULL",
    columns=[
        {"name": "id", "comment": "ID"},
        {"name": "name", "comment": "Name"},
    ],
    comment="Active users",
)
```

:::note
有关更多视图选项和限制，请参见 [Reference [5]](#references)。
:::

### 物化视图示例

物化视图的定义方式类似。`starrocks_refresh` 属性是一个语法字符串，表示刷新策略。

```python
from starrocks.schema import MaterializedView

# 重用上面 Core 表示例中的元数据
metadata = my_core_table.metadata

# 创建一个简单的物化视图（异步刷新）
user_stats_ = MaterializedView(
    'user_stats_',
    metadata,
    definition='SELECT id, COUNT(*) AS cnt FROM my_core_table GROUP BY id',
    starrocks_refresh='ASYNC'
)
```

:::note
有关更多选项和 ALTER 限制，请参见 [Reference [6]](#references)。
:::

## Alembic 集成

StarRocks SQLAlchemy 方言提供对以下功能的全面支持：

- 创建 / 删除表
- 创建 / 删除视图
- 创建 / 删除物化视图
- 检测 StarRocks 特定属性的支持变更（例如，表属性和分布）

这使得 Alembic 的 **自动生成** 能够正常工作。

### 初始化 Alembic

1. 初始化 Alembic：

   ```bash
   alembic init migrations
   ```

2. 在 `alembic.ini` 中配置您的数据库 URL：

   ```ini
   # alembic.ini
   sqlalchemy.url = starrocks://<user>:<password>@<FE_host>:<query_port>/[<catalog>.]<database>
   ```

3. 启用 StarRocks 方言日志记录（可选）：

   您可以在 `alembic.ini` 中启用 `starrocks` 日志记录，以通过日志观察表的检测变更。有关详细信息，请参见 [Reference [2]](#references)。

   编辑 `env.py`（配置离线和在线路径）：

    ```python
    from alembic import context
    from starrocks.alembic import render_column_type, include_object_for_view_
    from starrocks.alembic.starrocks import StarRocksImpl  # noqa: F401  (确保实现已注册)

    from myapp.models import Base  # 根据您的项目进行调整

    target_metadata = Base.metadata


    def run_migrations_offline() -> None:
        url = context.config.get_main_option("sqlalchemy.url")
        context.configure(
            url=url,
            target_metadata=target_metadata,
            render_item=render_column_type,
            include_object=include_object_for_view_
        )

        with context.begin_transaction():
            context.run_migrations()


    def run_migrations_online() -> None:
        # ... 按照 alembic 默认的 env.py 创建引擎并连接 ...
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                render_item=render_column_type,
                include_object=include_object_for_view_
            )

            with context.begin_transaction():
                context.run_migrations()
    ```

### 自动生成迁移

```bash
alembic revision --autogenerate -m "initial schema"
```

Alembic 将比较 SQLAlchemy 模型与实际的 StarRocks schema，并输出正确的 DDL。

### 应用迁移

```bash
alembic upgrade head
```

降级也支持（如果可逆）。

:::important
StarRocks DDL 在多个语句中不是事务性的。如果升级中途失败，您可能需要检查已经应用的内容，并在重新运行之前 **执行手动补救**（例如，编写补偿迁移或运行手动 DDL）。
:::

## 支持的 Schema 变更操作

该方言支持 Alembic 自动生成：

- **表**：创建 / 删除，以及通过 `starrocks_*` 声明的 StarRocks 特定属性的差异化（在 StarRocks ALTER 支持范围内）
- **视图**：创建 / 删除 / 修改（主要是定义相关的变更；某些属性是不可变的）
- **物化视图**：创建 / 删除 / 修改（限于可变的子句，如刷新策略和属性）

某些 StarRocks DDL 变更不可逆或不可修改。您只能通过删除并重新创建表/视图/物化视图来进行这些更改。如果您在方言中指定了这些更改，自动生成将 **警告或引发**。

## 端到端示例（推荐初学者阅读）

本节展示了一个可运行的端到端工作流程，包括在何处暂停并查看生成的文件。

### 步骤 1. 创建项目目录并初始化 Alembic

```bash
mkdir my_sr_alembic_project
cd my_sr_alembic_project

alembic init alembic
```

### 步骤 2. 配置 `alembic.ini`

编辑 `alembic.ini` 中的 URL：

```ini
sqlalchemy.url = starrocks://root@localhost:9030/mydatabase
```

### 步骤 3. 定义您的模型

为您的模型创建一个包：

```bash
mkdir -p myapp
touch myapp/__init__.py
```

创建 `myapp/models.py` 并将您的表/视图/物化视图定义放入包中：

:::note
使用 Alembic 迁移时，不要在您的模型模块中调用 `metadata.create_all(engine)`。
:::

```python
from sqlalchemy import Column, Table
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

from starrocks import INTEGER, STRING, VARCHAR
from starrocks.schema import MaterializedView, View

Base = declarative_base()


# --- ORM 表 ---
class MyOrmTable(Base):
    __tablename__ = "my_orm_table"

    id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    name: Mapped[str] = mapped_column(STRING)

    __table_args__ = {
        "comment": "table comment",
        "starrocks_primary_key": "id",
        "starrocks_distributed_by": "HASH(id) BUCKETS 10",
        "starrocks_properties": {"replication_num": "1"},
    }


# --- 在相同元数据上的 Core 表（对 Alembic target_metadata 很重要）---
my_core_table = Table(
    "my_core_table",
    Base.metadata,
    Column("id", INTEGER, primary_key=True),
    Column("name", VARCHAR(50)),
    comment="core table comment",
    starrocks_primary_key="id",
    starrocks_distributed_by="HASH(id) BUCKETS 10",
    starrocks_properties={"replication_num": "1"},
)


# --- 视图 ---
user_view = View(
    "user_view",
    Base.metadata,
    definition="SELECT id, name FROM my_core_table WHERE name IS NOT NULL",
    columns=[
        {"name": "id", "comment": "ID"},
        {"name": "name", "comment": "Name"},
    ],
    comment="Active users",
)


# --- 物化视图 ---
user_stats_mv = MaterializedView(
    "user_stats_mv",
    Base.metadata,
    definition="SELECT id, COUNT(*) AS cnt FROM my_core_table GROUP BY id",
    starrocks_refresh="ASYNC",
)
```

### 步骤 4. 配置 `env.py` 以进行自动生成

编辑 `alembic/env.py`：

1. 导入 `myapp.models` 以设置 `target_metadata`。
2. 导入 `render_column_type` 和 `include_object_for_view_mv`，在 `run_migrations_offline()` 和 `run_migrations_online()` 中设置它们，以正确处理视图和物化视图，并正确渲染 StarRocks 列类型。

:::note
您需要在 `env.py` 中添加或修改这些行，而不是替换生成的 `env.py` 文件。
:::

```python
from alembic import context
from starrocks.alembic import render_column_type, include_object_for_view_mv
from starrocks.alembic.starrocks import StarRocksImpl  # noqa: F401

from myapp.models import Base

target_metadata = Base.metadata

# 可选：为单 BE 开发集群设置版本表复制
version_table_kwargs = {"starrocks_properties": {"replication_num": "1"}}

# 在 run_migrations_offline() 和 run_migrations_online() 中确保：
def run_migrations_offline() -> None:
    url = context.config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        render_item=render_column_type,
        include_object=include_object_for_view_mv,
        version_table_kwargs=version_table_kwargs,
    )


def run_migrations_online() -> None:
    # ... 按照 alembic 默认的 env.py 创建引擎并连接 ...
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_item=render_column_type,
            include_object=include_object_for_view_mv,
            version_table_kwargs=version_table_kwargs,
        )
```

### 步骤 5. 自动生成第一个修订版

```bash
alembic revision --autogenerate -m "create initial schema"
```

暂停并查看：

1. 检查 `alembic/versions/` 下生成的迁移文件。
2. 确保它包含预期的操作（例如，`create_table`，`create_view`，`create_materialized_view`）。
3. 确保它不包含意外的删除或修改。

### 步骤 6. 预览 SQL 并应用

预览 SQL：

```bash
alembic upgrade head --sql
```

暂停并查看：

1. 确认 DDL 的顺序符合您的预期。
2. 识别任何可能的重操作，并考虑在需要时拆分迁移。

应用：

```bash
alembic upgrade head
```

:::important
StarRocks DDL 在多个语句中不是事务性的。如果升级中途失败，您可能需要检查已经应用的内容，并在重新运行之前执行手动补救。
:::

### 步骤 7. 进行更改并再次自动生成

更新 `myapp/models.py` 以：

- **修改现有表**（`my_core_table`）：添加列，或更新表注释，并更改一个表属性。
- **添加新表**（`my_new_table`）。

:::note
添加列可能是一个耗时的 schema 变更。StarRocks 仅允许每个表同时运行 **一个 schema 变更作业**。在实践中，建议将“添加/删除/修改列”变更与其他重变更（例如，额外的添加/删除列或大量属性变更）分开，并在需要时将它们拆分为多个 Alembic 修订版。
:::

```python
from sqlalchemy import Column, Table
from starrocks import INTEGER, VARCHAR

# 修改现有表（添加列）
# （在原地更新现有的 my_core_table 定义。）
my_core_table = Table(
    "my_core_table",
    Base.metadata,
    Column("id", INTEGER, primary_key=True),
    Column("name", VARCHAR(50)),
    Column("age", INTEGER),  # 仅添加的列

    starrocks_primary_key='id',
    starrocks_distributed_by='HASH(id) BUCKETS 10',
    starrocks_properties={"replication_num": "1"},
)

my_new_table = Table(
    "my_new_table",
    Base.metadata,
    Column("id", INTEGER, primary_key=True),
    Column("name", VARCHAR(50)),
    starrocks_primary_key="id",
    starrocks_distributed_by="HASH(id) BUCKETS 10",
    starrocks_properties={"replication_num": "1"},
)
```

```bash
alembic revision --autogenerate -m "add a new table, change a old table"
```

暂停并查看：

确保新的迁移包含：
- 一个 `create_table(...)` 用于 `my_new_table`，以及
- 对 `my_core_table` 变更的预期操作（例如，添加列/设置注释/设置属性）。

预览 SQL 并应用：

```bash
alembic upgrade head --sql
alembic upgrade head
```

## 使用 sqlacodegen

[`sqlacodegen`](https://github.com/agronholm/sqlacodegen) 可以直接从 StarRocks 反向生成 SQLAlchemy 模型：

```bash
sqlacodegen --options include_dialect_options,keep_dialect_types \
  --generator tables \
  starrocks://<user>:<password>@<FE_host>:<query_port>/[catalog.]<database> > models.py
```

支持的对象：

- 表
- 视图
- 物化视图
- 分区、分布和 order-by 子句，以及属性

这在将现有 StarRocks schema 引入 Alembic 时非常有用。

您可以直接使用上述命令生成 **端到端示例** 部分中定义的表/视图/物化视图的 Python 脚本。

:::note
- 生成 Core 风格模型时，建议添加 `--generator tables`（ORM 生成器可能会根据 `NOT NULL` / `NULL` 属性重新排序列）。
- 键列可能会生成为 `NOT NULL`。如果您希望它们可为空，请手动调整生成的模型。
:::

## 限制和最佳实践

- 某些 StarRocks DDL 操作需要删除并重新创建表；自动生成将警告或引发，而不是默默地产生不可用的 SQL。
- 键模型更改（例如，将 DUPLICATE KEY 更改为 PRIMARY KEY）不支持通过 `ALTER TABLE`；使用明确的计划（通常是删除并重新创建并进行回填）。
- StarRocks 不提供跨多个语句的事务性 DDL；请审查生成的迁移并在操作上应用它们。如果迁移中途失败，您可能需要 **手动** 处理回滚。
- 对于分布，如果您省略 `BUCKETS` 子句，StarRocks 可能会自动分配桶数；方言设计为避免在这种情况下产生噪声差异。

## 总结

通过 StarRocks SQLAlchemy 方言和 Alembic 集成，您可以：

- ✔ 使用声明式模型定义 StarRocks 的 schema
- ✔ 自动检测和生成 schema 迁移脚本
- ✔ 使用版本控制进行 schema 演变
- ✔ 声明式管理视图和物化视图
- ✔ 使用 sqlacodegen 反向工程现有 schema

这将 StarRocks 的 schema 管理引入现代 Python 数据工程生态系统，并显著简化跨环境的 schema 一致性。

## 参考资料

[1]: [starrocks-python-client README](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/README.md)

[2]: [Alembic Integration](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/alembic.md)

[3]: [SQLAlchemy details](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/sqlalchemy.md)

[4]: [Table Support](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/tables.md)

[5]: [View Support](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/views.md)

[6]: [Materialized View Support](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/materialized_views.md)
