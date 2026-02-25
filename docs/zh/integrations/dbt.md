---
displayed_sidebar: docs
---

import Experimental from '../_assets/commonMarkdown/_experimental.mdx'

# dbt

`dbt-starrocks` 允许使用 `dbt` 在 StarRocks 中使用 dbt 的建模模式和最佳实践来转换数据。

`dbt-starrocks` [GitHub repo](https://github.com/StarRocks/dbt-starrocks).

<Experimental />

## 支持的功能

| StarRocks >= 3.1 | StarRocks >= 3.4 |              功能              |
|:----------------:|:----------------:|:---------------------------------:|
|        ✅        |        ✅         |       表物化       |
|        ✅        |        ✅         |       视图物化        |
|        ✅        |        ✅         | 物化视图物化 |
|        ✅        |        ✅         |    增量物化    |
|        ✅        |        ✅         |         主键模型         |
|        ✅        |        ✅         |              源              |
|        ✅        |        ✅         |         自定义数据测试         |
|        ✅        |        ✅         |           文档生成           |
|        ✅        |        ✅         |       表达式分区        |
|        ❌        |        ❌         |               Kafka               |
|        ❌        |        ✅         |         动态覆盖         |
|        `*`       |        ✅         |            提交任务            |
|        ✅        |        ✅         |  微批处理 (插入覆盖)   |
|        ❌        |        ✅         | 微批处理 (动态覆盖)   |

`*` 请验证您的版本是否支持 `提交任务`，参见 [SUBMIT TASK](../sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK.md)

## 安装

使用 pip 安装 StarRocks DBT 适配器：

```sh
pip install dbt-starrocks
```

## 验证安装

通过检查版本来验证安装：

```sh
dbt --version
```

这应该在插件下列出 `starrocks`。

## 配置

### 配置文件

创建或更新 `profiles.yml`，添加 StarRocks 特定设置。

```yaml
starrocks_project:
  target: dev
  outputs:
    dev:
      type: starrocks
      host: your-starrocks-host.com
      port: 9030
      schema: your_database
      username: your_username
      password: your_password
      catalog: test_catalog
```

### 参数

#### `type`

**描述**: 要使用的特定适配器，必须设置为 `starrocks`\
**是否必需**: 必需\
**示例**: `starrocks`

#### `host`

**描述**: 要连接的主机名\
**是否必需**: 必需\
**示例**: `192.168.100.28`

#### `port`

**描述**: 要使用的端口\
**是否必需**: 必需\
**示例**: `9030`

#### `catalog`

**描述**: 指定要构建模型的 catalog\
**是否必需**: 可选\
**示例**: `default_catalog`

#### `schema`

**描述**: 指定要构建模型的 schema（在 StarRocks 中为数据库）\
**是否必需**: 必需\
**示例**: `analytics`

#### `username`

**描述**: 用于连接服务器的用户名\
**是否必需**: 必需\
**示例**: `dbt_admin`

#### `password`

**描述**: 用于验证服务器的密码\
**是否必需**: 必需\
**示例**: `correct-horse-battery-staple` 

#### `version`

**描述**: 让插件尝试使用兼容的 starrocks 版本\
**是否必需**: 可选\
**示例**: `3.1.0`

#### `use_pure`

**描述**: 设置为 "true" 以使用 C 扩展\
**是否必需**: 可选\
**示例**: `true`

#### `is_async`

**描述**: "true" 表示将合适的任务作为 ETL 任务提交。\
**是否必需**: 可选\
**示例**: `true`

#### `async_query_timeout`

**描述**: 设置将任务提交到 StarRocks 时的 `query_timeout` 值\
**是否必需**: 可选\
**示例**: `300`

### 源

创建或更新 `sources.yml`

```yml
sources:
  - name: your_source
    database: your_sr_catalog
    schema: your_sr_database
    tables:
      - name: your_table
```

如果在 schema 中未指定 catalog，则会默认使用配置文件中定义的 catalog。使用之前的配置文件，如果未定义 catalog，模型将假定源位于 `test_catalog.your_sr_database`。

## 物化

### 表

基本表配置

```sql
{{ config(
    materialized='table',
    engine='OLAP',
    keys=['id', 'name', 'created_date'],
    table_type='PRIMARY',
    distributed_by=['id'],
    buckets=3,
    partition_by=['created_date'],
    properties=[
        {"replication_num": "1"}
    ]
) }}

SELECT 
    id,
    name,
    email,
    created_date,
    last_modified_date
FROM {{ source('your_source', 'users') }}
```

## 配置选项

- engine: 存储引擎（默认: `OLAP`）
- keys: 定义排序键的列
- table_type: 表模型类型 
  - `PRIMARY`: 主键模型（支持更新和删除）
  - `DUPLICATE`: 重复键模型（允许重复行）
  - `UNIQUE`: 唯一键模型（强制唯一性）
- `distributed_by`: 用于哈希分布的列
- `buckets`: 数据分布的桶数（留空以自动分桶）
- `partition_by`: 表分区的列
- `partition_by_init`: 初始分区定义
- `properties`: 其他 StarRocks 表属性

## 外部 Catalog 中的表

### 从外部读取到 StarRocks

此示例在 StarRocks 中创建一个物化表，其中包含来自外部 Hive catalog 的聚合数据。

:::tip
如果外部 catalog 尚不存在，请进行配置：

```sql
CREATE EXTERNAL CATALOG `hive_external`
PROPERTIES (
    "hive.metastore.uris"  =  "thrift://127.0.0.1:8087",
    "type"="hive"
);
```
:::

```sql
{{ config(
    materialized='table',
    keys=['product_id', 'order_date'],
    distributed_by=['product_id'],
    partition_by=['order_date']
) }}

-- 将 Hive 外部 catalog 中的数据聚合到 StarRocks 表中
SELECT 
    h.product_id,
    h.order_date,
    COUNT(*) as order_count,
    SUM(h.amount) as total_amount,
    MAX(h.last_updated) as last_updated
FROM {{ source('hive_external', 'orders') }} h
GROUP BY 
    h.product_id,
    h.order_date
```

### 写入到外部

```sql
{{
  config(
    materialized='table',
    on_table_exists = 'replace',
    partition_by=['order_date'],
    properties={},
    catalog='external_catalog',
    database='test_db'
  )
}}

SELECT * FROM {{ source('iceberg_external', 'orders') }}
```

物化到外部 catalogs 的配置支持较少的选项。支持 `on_table_exists`、`partition_by` 和 `properties`。如果未设置 `catalog` 和 `database`，将使用配置文件中的默认值。

### 增量

StarRocks 也支持增量物化：

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    table_type='PRIMARY',
    keys=['id'],
    distributed_by=['id'],
    incremental_strategy='default'
) }}

SELECT
    id,
    user_id,
    event_name,
    event_timestamp,
    properties
FROM {{ source('raw', 'events') }}

{% if is_incremental() %}
    WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
```

#### 增量策略

`dbt-starrocks` 支持多种增量策略：

1. `append` (默认): 仅追加新记录，不进行去重
2. `insert_overwrite`: 用插入覆盖表分区
3. `dynamic_overwrite`: 覆盖、创建并写入表分区

有关使用哪种覆盖策略的更多信息，请参见 [INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md) 文档。

:::note
目前不支持增量合并。
:::

## 故障排除

- 在 dbt 中使用外部 catalogs 之前，必须在 StarRocks 中创建它们。相关文档在 [这里](../data_source/catalog/catalog_overview.md)。
- 外部源应使用 `{{ source('external_source_name', 'table_name' }}` 宏访问。
- `dbt seed` 未针对外部 catalogs 进行测试，目前不支持。
- 为了让 `dbt` 在当前不存在的外部数据库中创建模型，必须通过属性设置模型的位置。
- 外部模型需要定义其存储位置。如果目标数据库存在并设置了位置属性，则会定义此位置。否则，需要设置位置。
  - 我们目前仅支持在已存在的数据库中创建外部模型。