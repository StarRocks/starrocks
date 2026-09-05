---
sidebar_position: 110
displayed_sidebar: docs
description: "使用 dbt-starrocks 在 StarRocks 中通过 dbt 建模模式和最佳实践进行数据转换。"
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
|        ✅        |        ✅         | 数据测试（通用测试和单一测试） |
|        ✅        |        ✅         |            单元测试             |
|        ✅        |        ✅         |        存储测试失败记录        |
|        ✅        |        ✅         |  源数据新鲜度（`loaded_at_field`） |
|        ❌        |        ❌         |    基于元数据的源数据新鲜度    |
|        ✅        |        ✅         |           文档生成           |
|        ❌        |        ❌         |          `persist_docs`           |
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

## 测试

`dbt-starrocks` 支持 dbt 的所有测试类型，无需任何适配器专属配置。测试会被编译为标准 SQL，由 StarRocks 直接执行。

### 数据测试

支持四种内置通用测试（`not_null`、`unique`、`accepted_values` 和 `relationships`），同时也支持在 `macros/` 中定义的自定义通用测试，以及在 `test-paths` 下以 `.sql` 文件定义的单一测试。

```yml
models:
  - name: stg_customers
    columns:
      - name: id
        data_tests: [not_null, unique]
      - name: region
        data_tests:
          - accepted_values:
              values: ['us', 'eu']
          - relationships:
              to: ref('dim_customers')
              field: region
```

测试严重级别相关配置（`severity`、`error_if`、`warn_if`、`fail_calc` 和 `limit`）由 dbt 在 SQL 下发到 StarRocks 之前处理，其行为与在其他适配器上一致。

### 单元测试

支持单元测试。dbt 会将测试数据直接内联到编译后的查询中，因此不会向 StarRocks 写入任何数据：

```yml
unit_tests:
  - name: test_dim_customers_counts
    model: dim_customers
    given:
      - input: ref('stg_customers')
        rows:
          - {id: 1, name: 'a', region: 'us'}
          - {id: 2, name: 'b', region: 'us'}
    expect:
      rows:
        - {region: 'us', n: 2}
```

运行单元测试：

```sh
dbt test --select test_type:unit
```

### 存储测试失败记录

支持 `dbt test --store-failures`、`store_failures` 配置项以及 `store_failures_as`（`table` 或 `view`）。失败的数据行会被写入名为 `<schema>_dbt_test__audit` 的独立数据库中，如果该数据库不存在，适配器会自动创建：

```sql
SELECT * FROM `analytics_dbt_test__audit`.`not_null_stg_customers_name`;
```

失败记录表通过 `CREATE TABLE AS` 创建，因此支持与表物化相同的配置选项。您可以借此控制失败记录表的存储方式：

```yml
      - name: name
        data_tests:
          - not_null:
              config:
                store_failures: true
                alias: nn_name_failures
                distributed_by: ['id']
                buckets: 3
```

如果未指定 `distributed_by`，失败记录表将采用随机分桶方式创建。

:::tip
`store_failures_as: view` 会将失败的数据行存储为视图而非表，从而避免在每次测试运行时创建并重新写入表。
:::

### 源数据新鲜度

`dbt source freshness` 要求每个源表都声明 `loaded_at_field`：

```yml
sources:
  - name: raw
    tables:
      - name: events
        loaded_at_field: updated_at
        freshness:
          warn_after: {count: 1, period: hour}
          error_after: {count: 24, period: hour}
```

:::warning
不支持基于元数据的新鲜度检查。如果省略 `loaded_at_field`，检查将失败并报错 `no 'loaded_at_field' provided and starrocks adapter does not support metadata-based freshness checks`。因此，每个需要进行新鲜度检查的源表都必须包含一个时间戳列。
:::

## 生成文档

`dbt docs generate` 通过查询 `information_schema.tables` 和 `information_schema.columns` 来构建项目目录。模型、种子数据、视图、物化视图和源都会被包含在内，并附带各自的列、列序号和数据类型。`dbt docs generate --static` 和 `dbt docs serve` 同样可用。

您在 `.yml` 文件中编写的描述信息来自 dbt manifest，因此会照常显示在文档站点中。但以下元数据不会从 StarRocks 中采集：

- 存储在数据库中的表注释和列注释。
- 表的所有者。
- 表的统计信息，例如行数和大小。

此外，以下两点会影响关系的描述方式：

- 列类型不带精度信息。`VARCHAR(64)` 列会显示为 `varchar`，`DECIMAL(18,4)` 列会显示为 `decimal`。如需确认精确类型，请使用 [SHOW CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md)。
- 物化视图会被记录为视图。此行为仅影响文档站点，不影响 `dbt run`。

:::warning
不支持 `persist_docs`。在视图、增量和快照模型上，它会失败并报错 `alter_relation_comment macro not implemented for adapter starrocks`；在表模型上，它会被静默忽略，不会写入任何注释。请勿在 `dbt_project.yml` 中设置 `persist_docs`，因为该配置会作用于项目中的所有模型。
:::

## 故障排除

- 在 dbt 中使用外部 catalogs 之前，必须在 StarRocks 中创建它们。相关文档在 [这里](../data_source/catalog/catalog_overview.md)。
- 外部源应使用 `{{ source('external_source_name', 'table_name' }}` 宏访问。
- `dbt seed` 未针对外部 catalogs 进行测试，目前不支持。
- 为了让 `dbt` 在当前不存在的外部数据库中创建模型，必须通过属性设置模型的位置。
- 外部模型需要定义其存储位置。如果目标数据库存在并设置了位置属性，则会定义此位置。否则，需要设置位置。
  - 我们目前仅支持在已存在的数据库中创建外部模型。