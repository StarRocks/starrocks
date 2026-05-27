---
displayed_sidebar: docs
---

# 创建表

在 StarRocks 中创建一张新表。

:::tip
此操作需要目标数据库的 CREATE TABLE 权限。
:::

## 语法

```SQL
CREATE [EXTERNAL] [TEMPORARY] TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...]
[, index_definition1[, index_definition12,]])
[ENGINE = [olap|mysql|elasticsearch|hive|hudi|iceberg|jdbc]]
[key_desc]
[COMMENT "table comment"]
[partition_desc]
[distribution_desc]
[rollup_index]
[ORDER BY (column_name1,...)]
[PROPERTIES ("key"="value", ...)]
```

:::tip

- 您创建的表名、分区名、列名和索引名必须遵循[系统限制](../../System_limit.md)。
- 当您指定数据库名、表名、列名或分区名时，请注意某些字面量在 StarRocks 中被用作保留关键字。请勿在 SQL 语句中直接使用这些关键字。如果您想在 SQL 语句中使用此类关键字，请将其用一对反引号 (`) 括起来。请参阅[关键字](../keywords.md)以了解这些保留关键字。

:::

## 关键字

### `EXTERNAL`

:::caution
`EXTERNAL` 关键字已弃用。

我们建议您使用[外部 Catalog](../../../data_source/catalog/catalog_overview.md)来查询 Hive、Iceberg、Hudi 和 JDBC 数据源中的数据，而不是使用 `EXTERNAL` 关键字创建外部表。

:::

:::tip
**建议**

从 v3.1 起，StarRocks 支持在 Iceberg Catalog 中创建 Parquet 格式的表，并支持使用 INSERT INTO 将数据导入到这些 Parquet 格式的 Iceberg 表中。

从 v3.2 起，StarRocks 支持在 Hive Catalog 中创建 Parquet 格式的表，并支持使用 INSERT INTO 将数据导入到这些 Parquet 格式的 Hive 表中。从 v3.3 起，StarRocks 支持在 Hive Catalog 中创建 ORC 和 Textfile 格式的表，并支持使用 INSERT INTO 将数据导入到这些 ORC 和 Textfile 格式的 Hive 表中。
:::

如果您想使用已弃用的 `EXTERNAL` 关键字，请展开**`EXTERNAL` 关键字详情**

<details>
  <summary>`EXTERNAL` 关键字详情</summary>

  要创建外部表以查询外部数据源，请指定 `CREATE EXTERNAL TABLE` 并将 `ENGINE` 设置为以下任意值。您可以参考[外部表](../../../data_source/External_table.md)了解更多信息。

  - 对于 MySQL 外部表，请指定以下属性：

    ```plaintext
    PROPERTIES (
        "host" = "mysql_server_host",
        "port" = "mysql_server_port",
        "user" = "your_user_name",
        "password" = "your_password",
        "database" = "database_name",
        "table" = "table_name"
    )
    ```

    注意：

    MySQL 中的“table_name”应指真实的表名。相比之下，CREATE TABLE 语句中的“table_name”指此 MySQL 表在 StarRocks 上的名称。它们可以不同，也可以相同。

    在 StarRocks 中创建 MySQL 表的目的是访问 MySQL 数据库。StarRocks 本身不维护或存储任何 MySQL 数据。

  - 对于 Elasticsearch 外部表，请指定以下属性：

    ```plaintext
    PROPERTIES (
    "hosts" = "http://192.168.xx.xx:8200,http://192.168.xx0.xx:8200",
    "user" = "root",
    "password" = "root",
    "index" = "tindex",
    "type" = "doc"
    )
    ```

    - `hosts`：用于连接 Elasticsearch 集群的 URL。您可以指定一个或多个 URL。
    - `user`：用于登录已启用基本身份验证的 Elasticsearch 集群的根用户帐户。
    - `password`：上述根帐户的密码。
    - `index`：StarRocks 表在 Elasticsearch 集群中的索引。索引名称与 StarRocks 表名相同。您可以将此参数设置为 StarRocks 表的别名。
    - `type`：索引类型。默认值为 `doc`。

  - 对于 Hive 外部表，请指定以下属性：

    ```plaintext
    PROPERTIES (
        "database" = "hive_db_name",
        "table" = "hive_table_name",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
    )
    ```

    此处，database 是 Hive 表中对应数据库的名称。Table 是 Hive 表的名称。`hive.metastore.uris` 是服务器地址。

  - 对于 JDBC 外部表，请指定以下属性：

    ```plaintext
    PROPERTIES (
    "resource"="jdbc0",
    "table"="dest_tbl"
    )
    ```

    `resource` 是 JDBC 资源名称，`table` 是目标表。

  - 对于 Iceberg 外部表，请指定以下属性：

    ```plaintext
     PROPERTIES (
     "resource" = "iceberg0", 
     "database" = "iceberg", 
     "table" = "iceberg_table"
     )
    ```

    `resource` 是 Iceberg 资源名称。`database` 是 Iceberg 数据库。`table` 是 Iceberg 表。

  - 对于 Hudi 外部表，请指定以下属性：

    ```plaintext
      PROPERTIES (
      "resource" = "hudi0", 
      "database" = "hudi", 
      "table" = "hudi_table" 
      )
    ```

</details>

### `TEMPORARY`

创建临时表。从 v3.3.1 开始，StarRocks 支持在 Default Catalog 中创建临时表。更多信息，请参见[临时表](../../../table_design/StarRocks_table_design.md#temporary-table)。

:::note
创建临时表时，您必须将 `ENGINE` 设置为 `olap`。
:::

## 列定义

```SQL
col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"] [AUTO_INCREMENT] [AS generation_expr]
```

### `col_name`

请注意，通常您不能创建以 `__op` 或 `__row` 开头的列名，因为这些名称格式在 StarRocks 中保留用于特殊目的，创建此类列可能会导致未定义的行为。如果您确实需要创建此类列，请将 FE 动态参数[`allow_system_reserved_names`](../../../administration/management/FE_configuration.md#allow_system_reserved_names) 设置为 `TRUE`。

### `col_type`

具体列信息，例如类型和范围：

- TINYINT (1 字节)：范围从 -2^7 + 1 到 2^7 - 1。

- SMALLINT (2 字节)：范围从 -2^15 + 1 到 2^15 - 1。

- INT (4 字节)：范围从 -2^31 + 1 到 2^31 - 1。

- BIGINT (8 字节)：范围从 -2^63 + 1 到 2^63 - 1。

- LARGEINT (16 字节)：范围从 -2^127 + 1 到 2^127 - 1。

- FLOAT (4 字节)：支持科学计数法。

- DOUBLE (8 字节)：支持科学计数法。

- DECIMAL[(precision, scale)] (16 字节)

  - 默认值：DECIMAL(10, 0)
  - 精度：1 ~ 38
  - 标度：0 ~ 精度
  - 整数部分：精度 - 标度

    不支持科学计数法。

- DATE (3 字节)：范围从 0000-01-01 到 9999-12-31。

- DATETIME (8 字节)：范围从 0000-01-01 00:00:00 到 9999-12-31 23:59:59。

- CHAR[(length)]：定长字符串。范围：1 ~ 255。默认值：1。

- VARCHAR[(length)]：变长字符串。默认值为 1。单位：字节。在 StarRocks 2.1 之前的版本中，`length` 的取值范围是 1–65533。[预览] 在 StarRocks 2.1 及更高版本中，`length` 的取值范围是 1–1048576。

- HLL (1~16385 字节)：对于 HLL 类型，无需指定长度或默认值。长度将根据数据聚合在系统内部控制。HLL 列只能通过[hll_union_agg](../../sql-functions/aggregate-functions/hll_union_agg.md)、[Hll_cardinality](../../sql-functions/scalar-functions/hll_cardinality.md)、[hll_hash](../../sql-functions/scalar-functions/hll_hash.md) 进行查询或使用。

- BITMAP：Bitmap 类型不需要指定长度或默认值。它表示一组无符号的 bigint 数字。最大元素可达 2^64 - 1。

### `agg_type`

聚合类型。如果未指定，则此列是键列。
如果指定，则此列是值列。支持的聚合类型如下：

- `SUM`、`MAX`、`MIN`、`REPLACE`
- `HLL_UNION`（仅适用于 `HLL` 类型）
- `BITMAP_UNION`（仅适用于 `BITMAP`）
- `REPLACE_IF_NOT_NULL`：这意味着只有当导入数据为非空值时才会被替换。如果导入数据为空值，StarRocks 将保留原始值。

:::note

- 导入聚合类型为 BITMAP_UNION 的列时，其原始数据类型必须是 TINYINT、SMALLINT、INT 和 BIGINT。
- 如果在建表时，REPLACE_IF_NOT_NULL 列指定了 NOT NULL，StarRocks 仍会将数据转换为 NULL，而不会向用户发送错误报告。这样，用户可以导入选定的列。
:::

此聚合类型仅适用于 key_desc 类型为 AGGREGATE KEY 的聚合表。自 v3.1.9 起，`REPLACE_IF_NOT_NULL` 新增支持 BITMAP 类型的列。

### `NULL` | `NOT NULL`

是否允许列为 `NULL`。默认情况下，对于使用 Duplicate Key、Aggregate 或 Unique Key 表的所有列，都指定 `NULL`。在使用 Primary Key 表的表中，默认情况下，值列指定为 `NULL`，而键列指定为 `NOT NULL`。如果原始数据中包含 `NULL` 值，请使用 `\N` 表示它们。StarRocks 在数据加载期间将 `\N` 视为 `NULL`。

### `DEFAULT`

列的默认值。当您将数据加载到 StarRocks 中时，如果映射到该列的源字段为空，StarRocks 会自动在该列中填充默认值。您可以通过以下方式之一指定默认值：

- **DEFAULT current_timestamp**：使用当前时间作为默认值。更多信息，请参见 [current_timestamp()](../../sql-functions/date-time-functions/current_timestamp.md)。
- **DEFAULT (<expr>)**：使用给定表达式或函数返回的结果作为默认值。支持以下表达式：
  - [uuid()](../../sql-functions/utility-functions/uuid.md) 和 [uuid_numeric()](../../sql-functions/utility-functions/uuid_numeric.md)：生成唯一标识符。
  - ARRAY 字面量表达式（例如，`[1, 2, 3]`）：适用于 ARRAY 类型列。
  - MAP 表达式（例如，`map{key: value}`）：适用于 MAP 类型列。
  - row() 函数（例如，`row(val1, val2)`）：适用于 STRUCT 类型列。
- **DEFAULT `<default_value>`**：使用列数据类型的给定值作为默认值。StarRocks 支持为不同类型指定默认值：

  **基本类型**：使用字符串字面量指定默认值。

  ```sql
  -- 数值类型
  age INT DEFAULT '18'
  price DECIMAL(10,2) DEFAULT '99.99'

  -- 字符串类型
  name VARCHAR(50) DEFAULT 'Anonymous'

  -- 日期/时间类型
  created_at DATETIME DEFAULT '2024-01-01 00:00:00'

  -- 布尔类型
  is_active BOOLEAN DEFAULT 'true'  -- Supports 'true'/'false'/'1'/'0'
  ```

  **JSON 类型**：使用 JSON 格式的字符串指定默认值。

  ```sql
  metadata JSON DEFAULT '{"status": "active"}'
  tags JSON DEFAULT '[1, 2, 3]'
  ```

  **VARBINARY 类型**：仅支持空字符串作为默认值。

  ```sql
  binary_data VARBINARY DEFAULT ''
  ```

  **BITMAP 和 HLL 类型**：仅支持空字符串作为默认值，仅适用于 AGGREGATE KEY 表。

  ```sql
  -- 在 AGGREGATE KEY 表中
  bm BITMAP BITMAP_UNION DEFAULT ''
  h HLL HLL_UNION DEFAULT ''
  ```

  **复杂类型 (ARRAY/MAP/STRUCT)**：使用表达式语法指定默认值，仅支持 OLAP 表。

  :::note
复杂类型的默认值是 **仅在 `fast_schema_evolution = true` 时支持**。如果表的 `fast_schema_evolution` 属性被明确设置为 `false`，则为复杂类型添加默认值将导致错误。
:::

  ```sql
  -- ARRAY 类型
  tags ARRAY<VARCHAR(20)> DEFAULT ['tag1', 'tag2']
  scores ARRAY<INT> DEFAULT [90, 85, 92]

  -- MAP 类型
  attrs MAP<VARCHAR(20), INT> DEFAULT map{'age': 25, 'score': 100}

  -- STRUCT 类型
  person STRUCT<name VARCHAR(20), age INT> DEFAULT row('John', 30)

  -- 复杂嵌套：包含嵌套 STRUCT、ARRAY 和 MAP 的 STRUCT
  user_profile STRUCT<
    id INT, 
    name VARCHAR(50), 
    contact STRUCT<email VARCHAR(100), phone VARCHAR(20)>,
    tags ARRAY<VARCHAR(20)>,
    attributes MAP<VARCHAR(20), VARCHAR(50)>
  > DEFAULT row(1, 'Alice', row('alice@example.com', '123-456-7890'), ['admin', 'user'], map{'level': 'premium', 'status': 'active'})
  ```

  **限制**：

  - TIME 和 VARIANT 类型暂不支持默认值。
  - 复杂类型（ARRAY/MAP/STRUCT）的默认值仅支持 OLAP 表，并且需要启用 `fast_schema_evolution` 属性。

### `AUTO_INCREMENT`

指定 `AUTO_INCREMENT` 列。`AUTO_INCREMENT` 列的数据类型必须是 BIGINT。自增 ID 从 1 开始，步长为 1。有关 `AUTO_INCREMENT` 列的更多信息，请参阅 [自动递增](auto_increment.md)。自 v3.0 起，StarRocks 支持 `AUTO_INCREMENT` 列。

### `AS`

指定生成列及其表达式。[生成列](../generated_columns.md) 可用于预计算和存储表达式结果，这显著加速了包含相同复杂表达式的查询。自 v3.1 起，StarRocks 支持生成列。

## 索引定义

```SQL
INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'
```

有关参数说明和使用注意事项的更多信息，请参阅 [Bitmap 索引](../../../table_design/indexes/Bitmap_index.md#create-an-index)。

## `ENGINE`

默认值：`olap`。如果未指定此参数，则默认创建 OLAP 表（StarRocks 原生表）。

可选值：`mysql`、`elasticsearch`、`hive`、`jdbc`、`iceberg` 和 `hudi`。

## 键

语法：

```SQL
key_type(k1[,k2 ...])
```

数据在指定的键列中排序，并针对不同的键类型具有不同的属性：

- AGGREGATE KEY：键列中相同的内容将根据指定的聚合类型聚合到值列中。它通常适用于财务报表和多维分析等业务场景。
- UNIQUE KEY/PRIMARY KEY：键列中相同的内容将根据导入顺序在值列中被替换。它可用于对键列进行增、删、改、查操作。
- DUPLICATE KEY：键列中相同的内容在 StarRocks 中共存。它可用于存储明细数据或没有聚合属性的数据。

  :::note
DUPLICATE KEY 是默认类型。数据将根据键列进行排序。
:::

:::note
除 AGGREGATE KEY 外，当使用其他键类型创建表时，值列无需指定聚合类型。
:::

### 基于范围的分布

从 v4.1 起，StarRocks 支持 **基于范围的分布语义**（默认禁用），由 FE 配置 `enable_range_distribution` 控制。数据将根据键列的数据范围进行排序，每个 Tablet 包含来自特定范围的数据。

基于范围的分布语义与默认语义在以下方面有所不同：

- 如果显式指定了键类型（AGGREGATE KEY/UNIQUE KEY/PRIMARY KEY/DUPLICATE KEY），但未指定 DISTRIBUTED BY 子句，则数据将默认按范围分布。
- 如果未指定键类型、DISTRIBUTED BY 子句或 ORDER BY 子句，则将创建一个采用随机分桶策略的 Duplicate Key 表。
- 如果未指定键类型和 DISTRIBUTED BY 子句，但指定了 ORDER BY 子句，则将创建一个采用基于范围的分布策略的 Duplicate Key 表。在这种情况下，DUPLICATE KEY 等同于 ORDER BY 子句，反之亦然。
- 如果同时指定了 DUPLICATE KEY 和 ORDER BY 子句，则只有 ORDER BY 子句生效，DUPLICATE KEY 将被忽略。

## `COMMENT`

您可以在创建表时添加表注释，可选。请注意，COMMENT 必须放在 `key_desc` 之后。否则，无法创建表。

从 v3.1 起，您可以使用 `ALTER TABLE <table_name> COMMENT = "new table comment"` 修改表注释。

## 分区

分区可以通过以下方式进行管理：

### 动态创建分区

[动态分区](../../../table_design/data_distribution/dynamic_partitioning.md) 提供分区的生命周期 (TTL) 管理。StarRocks 会提前自动创建新分区并删除过期分区，以确保数据新鲜度。要启用此功能，您可以在创建表时配置动态分区相关属性。

### 逐个创建分区

#### 仅指定分区的上限

语法：

```sql
PARTITION BY RANGE ( <partitioning_column1> [, <partitioning_column2>, ... ] )
  PARTITION <partition1_name> VALUES LESS THAN ("<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] )
  [ ,
  PARTITION <partition2_name> VALUES LESS THAN ("<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] )
  , ... ] 
)
```

:::note
请使用指定的键列和指定的值范围进行分区。
:::

- 有关分区的命名约定，请参阅[系统限制](../../System_limit.md)。
- 在 v3.3.0 之前，范围分区列仅支持以下类型：TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DATE 和 DATETIME。从 v3.3.0 开始，三个特定的时间函数可以用作范围分区列。有关详细用法，请参阅[数据分布](../../../table_design/data_distribution/Data_distribution.md#manually-create-partitions)。
- 分区是左闭右开的。第一个分区的左边界是最小值。
- NULL 值仅存储在包含最小值的分区中。当包含最小值的分区被删除时，NULL 值将无法再导入。
- 分区列可以是单列或多列。分区值是默认的最小值。
- 当只指定一列作为分区列时，您可以将 `MAXVALUE` 设置为最新分区中分区列的上限。

  ```SQL
  PARTITION BY RANGE (pay_dt) (
    PARTITION p1 VALUES LESS THAN ("20210102"),
    PARTITION p2 VALUES LESS THAN ("20210103"),
    PARTITION p3 VALUES LESS THAN MAXVALUE
  )
  ```

:::note

- 分区通常用于管理与时间相关的数据。
- 当需要数据回溯时，您可以考虑清空第一个分区，以便在以后需要时添加分区。
:::

#### 为分区指定下限和上限

语法：

```SQL
PARTITION BY RANGE ( <partitioning_column1> [, <partitioning_column2>, ... ] )
(
    PARTITION <partition_name1> VALUES [( "<lower_bound_for_partitioning_column1>" [ , "<lower_bound_for_partitioning_column2>", ... ] ), ( "<upper_bound_for_partitioning_column1?" [ , "<upper_bound_for_partitioning_column2>", ... ] ) ) 
    [,
    PARTITION <partition_name2> VALUES [( "<lower_bound_for_partitioning_column1>" [ , "<lower_bound_for_partitioning_column2>", ... ] ), ( "<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] ) ) 
    , ...]
)
```

:::note

- Fixed Range 比 LESS THAN 更灵活。您可以自定义左右分区。
- Fixed Range 在其他方面与 LESS THAN 相同。
- 当只指定一列作为分区列时，您可以将 `MAXVALUE` 设置为最新分区中分区列的上限。
:::

  ```SQL
  PARTITION BY RANGE (pay_dt) (
    PARTITION p202101 VALUES [("20210101"), ("20210201")),
    PARTITION p202102 VALUES [("20210201"), ("20210301")),
    PARTITION p202103 VALUES [("20210301"), (MAXVALUE))
  )
  ```

### 批量创建多个分区

语法

- 如果分区列是日期类型。

  ```sql
  PARTITION BY RANGE (<partitioning_column>) (
      START ("<start_date>") END ("<end_date>") EVERY (INTERVAL <N> <time_unit>)
  )
  ```

- 如果分区列是整数类型。

  ```sql
  PARTITION BY RANGE (<partitioning_column>) (
      START ("<start_integer>") END ("<end_integer>") EVERY (<partitioning_granularity>)
  )
  ```

说明

您可以在 `START()` 和 `END()` 中指定起始值和结束值，并在 `EVERY()` 中指定时间单位或分区粒度，以批量创建多个分区。

- 在 v3.3.0 之前，范围分区列仅支持以下类型：TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DATE 和 DATETIME。从 v3.3.0 开始，三个特定的时间函数可以用作范围分区列。有关详细用法，请参阅[数据分布](../../../table_design/data_distribution/Data_distribution.md#manually-create-partitions)。
- 如果分区列是日期类型，您需要使用 `INTERVAL` 关键字来指定时间间隔。您可以将时间单位指定为小时（自 v3.0 起）、天、周、月或年。分区的命名约定与动态分区相同。

更多信息，请参阅[数据分布](../../../table_design/data_distribution/Data_distribution.md)。

## 分布

StarRocks 支持 Hash 分桶和随机分桶。如果您不配置分桶，StarRocks 默认使用随机分桶并自动设置分桶数量。

- 随机分桶（自 v3.1 起）

  对于分区中的数据，StarRocks 会将数据随机分布到所有分桶中，这不基于特定的列值。如果您希望 StarRocks 自动设置分桶数量，则无需指定任何分桶配置。如果您选择手动指定分桶数量，语法如下：

  ```SQL
  DISTRIBUTED BY RANDOM BUCKETS <num>
  ```

  但是，请注意，当您查询大量数据并频繁使用某些列作为条件列时，随机分桶提供的查询性能可能不理想。在这种情况下，建议使用 Hash 分桶。因为只需要扫描和计算少量分桶，从而显著提高查询性能。

  **注意事项**

  - 您只能使用随机分桶创建 Duplicate Key 表。
  - 您不能指定[局部性组](../../../using_starrocks/Colocate_join.md)用于随机分桶的表。
  - Spark Load 不能用于向随机分桶的表加载数据。
  - 从 StarRocks v2.5.7 开始，您在创建表时无需设置分桶数量。StarRocks 会自动设置分桶数量。如果您想设置此参数，请参阅[设置分桶数量](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)。

  更多信息，请参阅[随机分桶](../../../table_design/data_distribution/Data_distribution.md#random-bucketing-since-v31)。

- Hash 分桶

  语法：

  ```SQL
  DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
  ```

  分区中的数据可以根据分桶列的哈希值和分桶数量进一步细分为桶。我们建议您选择满足以下两个要求的列作为分桶列。

  - 高基数列，例如 ID
  - 经常在查询中用作过滤条件的列

  如果不存在这样的列，您可以根据查询的复杂性来确定分桶列。

  - 如果查询复杂，我们建议您选择高基数列作为分桶列，以确保数据在桶之间均衡分布，并提高集群资源利用率。
  - 如果查询相对简单，我们建议您选择经常用作查询条件的列作为分桶列，以提高查询效率。

  如果分区数据无法通过一个分桶列均匀分布到每个桶中，您可以选择多个分桶列（最多三个）。更多信息，请参阅[选择分桶列](../../../table_design/data_distribution/Data_distribution.md#hash-bucketing)。

  **注意事项**：

  - **创建表时，必须指定其分桶列**。
  - 分桶列的值不能更新。
  - 分桶列一旦指定，就不能修改。
  - 从 StarRocks v2.5.7 开始，您在创建表时无需设置分桶数量。StarRocks 会自动设置分桶数量。如果您想设置此参数，请参阅[设置分桶数量](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)。

- 基于范围的分布

  从 v4.1 开始，StarRocks 支持**基于范围的分布语义**（默认禁用），由 FE 配置 `enable_range_distribution` 控制。有关详细信息，请参阅[基于范围的分布](#range-based-distribution)。

## Rollup 索引

您可以在创建表时批量创建 Rollup。

语法：

```SQL
ROLLUP (rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...)
```

## `ORDER BY`

从 v3.0 版本开始，主键表支持使用 `ORDER BY` 定义排序键。从 v3.3 版本开始，明细表、聚合表和唯一键表支持使用 `ORDER BY` 定义排序键。

有关排序键的更多说明，请参阅[排序键和前缀索引](../../../table_design/indexes/Prefix_index_sort_key.md)。

## `PROPERTIES`

### 存储和副本

如果引擎类型为 `OLAP`，您可以在创建表时指定初始存储介质 (`storage_medium`)、自动存储冷却时间 (`storage_cooldown_time`) 或时间间隔 (`storage_cooldown_ttl`)，以及副本数 (`replication_num`)。

属性生效范围：如果表只有一个分区，则属性属于表。如果表被划分为多个分区，则属性属于每个分区。当您需要为指定分区配置不同的属性时，可以执行[ALTER TABLE ... ADD PARTITION 或 ALTER TABLE ... MODIFY PARTITION](ALTER_TABLE.md)在表创建后。

#### 设置初始存储介质和自动存储冷却时间

```sql
PROPERTIES (
    "storage_medium" = "[SSD|HDD]",
    { "storage_cooldown_ttl" = "<num> { YEAR | MONTH | DAY | HOUR } "
    | "storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss" }
)
```

**属性**

- `storage_medium`：初始存储介质，可以设置为 `SSD` 或 `HDD`。请确保您明确指定的存储介质类型与 StarRocks 集群的 BE 静态参数 `storage_root_path` 中指定的 BE 磁盘类型一致。<br />

  如果 FE 配置项 `enable_strict_storage_medium_check` 设置为 `true`，系统在创建表时会严格检查 BE 磁盘类型。如果您在 CREATE TABLE 中指定的存储介质与 BE 磁盘类型不一致，系统将返回错误 "Failed to find enough host in all backends with storage medium is SSD|HDD." 并且表创建失败。如果 `enable_strict_storage_medium_check` 设置为 `false`，系统将忽略此错误并强制创建表。但是，数据加载后集群磁盘空间可能会分布不均。<br />

  从 v2.3.6、v2.4.2、v2.5.1 和 v3.0 版本开始，如果未明确指定 `storage_medium`，系统会根据 BE 磁盘类型自动推断存储介质。<br />

  - 在以下场景中，系统会自动将此参数设置为 SSD：

    - BE 报告的磁盘类型 (`storage_root_path`) 只包含 SSD。
    - BE 报告的磁盘类型 (`storage_root_path`) 同时包含 SSD 和 HDD。请注意，从 v2.3.10、v2.4.5、v2.5.4 和 v3.0 版本开始，当 BE 报告的 `storage_root_path` 同时包含 SSD 和 HDD 且指定了属性 `storage_cooldown_time` 时，系统会将 `storage_medium` 设置为 SSD。

  - 在以下场景中，系统会自动将此参数设置为 HDD：

    - BE 报告的磁盘类型 (`storage_root_path`) 只包含 HDD。
    - 从 2.3.10、2.4.5、2.5.4 和 3.0 版本开始，当 BE 报告的 `storage_root_path` 同时包含 SSD 和 HDD 且未指定属性 `storage_cooldown_time` 时，系统会将 `storage_medium` 设置为 HDD。

- `storage_cooldown_ttl` 或 `storage_cooldown_time`：自动存储冷却时间或时间间隔。自动存储冷却是指将数据从 SSD 自动迁移到 HDD。此功能仅在初始存储介质为 SSD 时有效。

  - `storage_cooldown_ttl`：此表中分区的自动存储冷却的**时间间隔**。如果您需要将最新分区保留在 SSD 上，并在一定时间间隔后自动将旧分区冷却到 HDD，则可以使用此参数。每个分区的自动存储冷却时间是使用此参数的值加上分区的上限时间计算的。

  支持的值为 `<num> YEAR`、`<num> MONTH`、`<num> DAY` 和 `<num> HOUR`。`<num>` 是一个非负整数。默认值为 null，表示不自动执行存储冷却。

  例如，您在创建表时将值指定为 `"storage_cooldown_ttl"="1 DAY"`，并且存在范围为 `[2023-08-01 00:00:00,2023-08-02 00:00:00)` 的分区 `p20230801`。此分区的自动存储冷却时间为 `2023-08-03 00:00:00`，即 `2023-08-02 00:00:00 + 1 DAY`。如果您在创建表时将值指定为 `"storage_cooldown_ttl"="0 DAY"`，则此分区的自动存储冷却时间为 `2023-08-02 00:00:00`。

  - `storage_cooldown_time`：自动存储冷却时间（**绝对时间**），即表从 SSD 冷却到 HDD 的时间。指定的时间需要晚于当前时间。格式为：“yyyy-MM-dd HH:mm:ss”。当您需要为指定分区配置不同的属性时，可以执行[ALTER TABLE ... ADD PARTITION 或 ALTER TABLE ... MODIFY PARTITION](ALTER_TABLE.md)。

##### 用法

- 与自动存储冷却相关的参数比较如下：
  - `storage_cooldown_ttl`：一个表属性，用于指定表中分区的自动存储冷却时间间隔。系统会在 `the value of this parameter plus the upper time bound of the partition` 时自动冷却分区。因此，自动存储冷却是在分区粒度上执行的，这更灵活。
  - `storage_cooldown_time`：一个表属性，用于指定自动存储冷却时间（**绝对时间**）用于此表。此外，您可以在表创建后为指定分区配置不同的属性。
  - `storage_cooldown_second`：一个静态 FE 参数，用于指定集群中所有表的自动存储冷却延迟。

- 表属性 `storage_cooldown_ttl` 或 `storage_cooldown_time` 优先于 FE 静态参数 `storage_cooldown_second`。

- 配置这些参数时，您需要指定 `"storage_medium = "SSD"`。

- 如果您不配置这些参数，将不会自动执行存储自动降冷。

- 执行 `SHOW PARTITIONS FROM <table_name>` 查看每个分区的存储自动降冷时间。

##### 限制

- 不支持表达式分区和列表分区。
- 分区列必须是日期类型。
- 不支持多分区列。
- 不支持主键表。

#### 设置分区中每个 tablet 的副本数

`replication_num`：分区中每个表的副本数。默认值：`3`。

```sql
PROPERTIES (
    "replication_num" = "<num>"
)
```

### Bloom Filter 索引

如果 Engine 类型为 `olap`，您可以指定列来采用 Bloom Filter 索引。

使用 Bloom Filter 索引时，存在以下限制：

- 您可以为 Duplicate Key 或 Primary Key 表的所有列创建 Bloom Filter 索引。对于 Aggregate 表或 Unique Key 表，您只能为 Key 列创建 Bloom Filter 索引。
- TINYINT、FLOAT、DOUBLE 和 DECIMAL 列不支持创建 Bloom Filter 索引。
- Bloom Filter 索引只能提高包含 `in` 和 `=` 运算符的查询性能，例如 `Select xxx from table where x in {}` 和 `Select xxx from table where column = xxx`。此列中离散值越多，查询越精确。

更多信息，请参见 [Bloom Filter 索引](../../../table_design/indexes/Bloomfilter_index.md)

```SQL
PROPERTIES (
    "bloom_filter_columns"="k1,k2,k3"
)
```

### Colocate Join

如果您想使用 Colocate Join 属性，请在 `properties` 中指定。

```SQL
PROPERTIES (
    "colocate_with"="table1"
)
```

### 动态分区

如果您想使用动态分区属性，请在 properties 中指定。

```SQL
PROPERTIES (
    "dynamic_partition.enable" = "true|false",
    "dynamic_partition.time_unit" = "DAY|WEEK|MONTH",
    "dynamic_partition.start" = "${integer_value}",
    "dynamic_partition.end" = "${integer_value}",
    "dynamic_partition.prefix" = "${string_value}",
    "dynamic_partition.buckets" = "${integer_value}"
```

**`PROPERTIES`**

| 参数 | 必选 | 描述 |
| --- | --- | --- |
| dynamic_partition.enable | 否 | 是否启用动态分区。有效值：`TRUE` 和 `FALSE`。默认值：`TRUE`。 |
| dynamic_partition.time_unit | 是 | 动态创建分区的时间粒度。这是一个必选参数。有效值：`DAY`、`WEEK` 和 `MONTH`。时间粒度决定了动态创建分区的后缀格式。<br />  - 如果值为 `DAY`，动态创建分区的后缀格式为 `yyyyMMdd`。分区名称后缀示例为 `20200321`。<br />  - 如果值为 `WEEK`，动态创建分区的后缀格式为 `yyyy_ww`，例如 2020 年第 13 周的 `2020_13`。<br />  - 如果值为 `MONTH`，动态创建分区的后缀格式为 `yyyyMM`，例如 `202003`。 |
| dynamic_partition.start | 否 | 动态分区的起始偏移量。此参数的值必须为负整数。在此偏移量之前的分区将根据 `dynamic_partition.time_unit` 确定的当前日、周或月删除。默认值为 `Integer.MIN_VALUE`，即 -2147483648，这意味着历史分区不会被删除。 |
| dynamic_partition.end | 是 | 动态分区的结束偏移量。此参数的值必须为正整数。从当前日、周或月到结束偏移量的分区将提前创建。 |
| dynamic_partition.prefix | 否 | 添加到动态分区名称的前缀。默认值：`p`。 |
| dynamic_partition.buckets | 否 | 每个动态分区的 bucket 数量。默认值与保留字 `BUCKETS` 确定的 bucket 数量相同，或由 StarRocks 自动设置。 |

:::note

当分区列为 INT 类型时，无论分区时间粒度如何，其格式都必须为 `yyyyMMdd`。

:::

### 随机分桶的 bucket 大小

从 v3.2 开始，对于配置了随机分桶的表，您可以在创建表时使用 `bucket_size` 参数在 `PROPERTIES` 中指定 bucket 大小，以实现按需动态增加 bucket 数量。单位：B。

```sql
PROPERTIES (
    "bucket_size" = "1073741824"
)
```

### 数据压缩算法

您可以在创建表时通过添加属性 `compression` 来为表指定数据压缩算法。

`compression` 的有效值包括：

- `LZ4`：LZ4 算法。
- `ZSTD`：Zstandard 算法。
- `ZLIB`：zlib 算法。
- `SNAPPY`：Snappy 算法。

从 v3.3.2 开始，StarRocks 支持在创建表时为 zstd 压缩格式指定压缩级别。

语法：

```sql
PROPERTIES ("compression" = "zstd(<compression_level>)")
```

`compression_level`：ZSTD 压缩格式的压缩级别。类型：Integer。范围：[1,22]。默认值：`3`（推荐）。数字越大，压缩比越高。压缩级别越高，压缩和解压缩的时间消耗越大。

示例：

```sql
PROPERTIES ("compression" = "zstd(3)")
```

有关如何选择合适的数据压缩算法的更多信息，请参见[数据压缩](../../../table_design/data_compression.md)。

### 数据导入的写入法定人数

如果您的 StarRocks 集群有多个数据副本，您可以为表设置不同的写入法定人数，即 StarRocks 确定导入任务成功之前需要多少个副本返回导入成功。您可以在创建表时通过添加属性 `write_quorum` 来指定写入法定人数。此属性从 v2.5 开始支持。

`write_quorum` 的有效值包括：

- `MAJORITY`：默认值。当**大多数**个数据副本返回导入成功时，StarRocks 返回导入任务成功。否则，StarRocks 返回导入任务失败。
- `ONE`：当**一个**个数据副本返回导入成功时，StarRocks 返回导入任务成功。否则，StarRocks 返回导入任务失败。
- `ALL`：当**所有**个数据副本返回导入成功时，StarRocks 返回导入任务成功。否则，StarRocks 返回导入任务失败。

:::caution

- 为导入设置较低的写入法定人数会增加数据不可访问甚至丢失的风险。例如，您在具有两个副本的 StarRocks 集群中，以一个写入法定人数将数据导入到表中，并且数据仅成功导入到一个副本中。尽管 StarRocks 确定导入任务成功，但数据只有一个幸存副本。如果存储导入数据 Tablet 的服务器发生故障，这些 Tablet 中的数据将变得不可访问。如果服务器的磁盘损坏，数据将丢失。
- StarRocks 仅在所有数据副本都返回状态后才返回导入任务状态。当存在导入状态未知的副本时，StarRocks 不会返回导入任务状态。在副本中，导入超时也被视为导入失败。
:::

### 副本数据写入和复制模式

如果您的 StarRocks 集群有多个数据副本，您可以在 `PROPERTIES` 中指定 `replicated_storage` 参数来配置副本之间的数据写入和复制模式。

- `true`（v3.0 及更高版本中的默认值）表示“单主复制”，这意味着数据仅写入主副本。其他副本从主副本同步数据。此模式显著降低了因数据写入多个副本而导致的 CPU 开销。它从 v2.5 开始支持。
- `false`（v2.5 中的默认值）表示“无主复制”，这意味着数据直接写入多个副本，不区分主副本和次副本。CPU 开销是副本数量的倍数。

在大多数情况下，使用默认值可以获得更好的数据写入性能。如果您想更改副本之间的数据写入和复制模式，请运行 ALTER TABLE 命令。示例：

```sql
ALTER TABLE example_db.my_table
SET ("replicated_storage" = "false");
```

### Delta Join 唯一键和外键约束

要在 View Delta Join 场景中启用查询重写，您必须为 Delta Join 中要连接的表定义唯一键约束 `unique_constraints` 和外键约束 `foreign_key_constraints`。请参见[异步物化视图 - 在 View Delta Join 场景中重写查询](../../../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#query-delta-join-rewrite)以获取更多信息。

```SQL
PROPERTIES (
    "unique_constraints" = "<unique_key>[, ...]",
    "foreign_key_constraints" = "
    (<child_column>[, ...]) 
    REFERENCES 
    [catalog_name].[database_name].<parent_table_name>(<parent_column>[, ...])
    [;...]
    "
)
```

- `child_column`：表的外部键。您可以定义多个 `child_column`。
- `catalog_name`：要连接的表所在的目录名称。如果未指定此参数，则使用默认目录。
- `database_name`：要连接的表所在的数据库名称。如果未指定此参数，则使用当前数据库。
- `parent_table_name`：要连接的表的名称。
- `parent_column`：要连接的列。它们必须是相应表的主键或唯一键。

:::caution

- `unique_constraints` 和 `foreign_key_constraints` 仅用于查询重写。当数据加载到表中时，不保证外键约束检查。您必须确保加载到表中的数据符合约束。
- Primary Key 表的主键或 Unique Key 表的唯一键，默认情况下是相应的 `unique_constraints`。您无需手动设置。
- 表 `foreign_key_constraints` 中的 `child_column` 必须引用另一个表 `unique_constraints` 中的 `unique_key`。
- `child_column` 和 `parent_column` 的数量必须一致。
- `child_column` 和相应的 `parent_column` 的数据类型必须匹配。
:::

### 共享数据集群的云原生表

要使用您的 StarRocks 共享数据集群，您必须创建具有以下属性的云原生表：

```SQL
PROPERTIES (
    "storage_volume" = "<storage_volume_name>",
    "datacache.enable" = "{ true | false }",
    "datacache.partition_duration" = "<string_value>",
    "file_bundling" = "{ true | false }"
)
```

- `storage_volume`：用于存储您要创建的云原生表的存储卷的名称。如果未指定此属性，则使用默认存储卷。此属性从 v3.1 开始支持。

- `datacache.enable`：是否启用本地磁盘缓存。默认值：`true`。

  - 当此属性设置为 `true` 时，要加载的数据会同时写入对象存储和本地磁盘（作为查询加速的缓存）。
  - 当此属性设置为 `false` 时，数据仅加载到对象存储中。

  :::note
要启用本地磁盘缓存，您必须在 BE 配置项 `storage_root_path` 中指定磁盘目录。
:::

- `datacache.partition_duration`：热数据的有效期。启用本地磁盘缓存后，所有数据都会加载到缓存中。当缓存满时，StarRocks 会从缓存中删除最近使用较少的数据。当查询需要扫描已删除的数据时，StarRocks 会检查数据是否在有效期内。如果数据在有效期内，StarRocks 会再次将数据加载到缓存中。如果数据不在有效期内，StarRocks 则不会将其加载到缓存中。此属性是一个字符串值，可以使用以下单位指定：`YEAR`、`MONTH`、`DAY` 和 `HOUR`，例如 `7 DAY` 和 `12 HOUR`。如果未指定，所有数据都将作为热数据缓存。

  :::note
此属性仅在 `datacache.enable` 设置为 `true` 时可用。
:::

- `file_bundling`（可选）：是否为云原生表启用文件捆绑优化。从 v4.0 开始支持。启用此功能（设置为 `true`）后，系统会自动捆绑加载、Compaction 或 Publish 操作生成的数据文件，从而降低因高频访问外部存储系统而产生的 API 成本。

  :::note

  - 文件捆绑仅适用于 StarRocks v4.0 或更高版本的共享数据集群。
  - 对于在 v4.0 或更高版本中创建的表，文件捆绑默认启用，由 FE 配置 `enable_file_bundling` 控制（默认值：true）。
  - 启用文件捆绑后，您只能将集群降级到 v3.5.2 或更高版本。如果您想降级到 v3.5.2 之前的版本，您必须首先删除已启用文件捆绑的表。
  - 集群升级到 v4.0 后，现有表的文件捆绑默认保持禁用状态。
  - 您可以使用 [ALTER TABLE](ALTER_TABLE.md) 语句手动为现有表启用文件捆绑，但有以下限制：
    - 您不能为在 v4.0 之前版本中创建的带有 Rollup Index 的表启用文件捆绑。您可以在 v4.0 或更高版本中删除并重新创建索引，然后为表启用文件捆绑。
    - 您不能修改 `file_bundling` 属性 **重复地** 在特定时间内。否则，系统将返回错误。您可以通过执行以下 SQL 语句检查 `file_bundling` 属性是否可修改：

      ```SQL
      SELECT METADATA_SWITCH_VERSION FROM information_schema.partitions_meta WHERE TABLE_NAME = '<table_name>';
      ```

      只有当返回 `0` 时，才允许修改 `file_bundling` 属性。非零值表示 `METADATA_SWITCH_VERSION` 对应的数据版本尚未被 GC 机制回收。您必须等到数据版本被回收。

      您可以通过为 FE 动态配置 `lake_autovacuum_grace_period_minutes` 设置一个较低的值来缩短此间隔。但是，请记住在修改 `file_bundling` 属性后将配置重置为其原始值。
:::

### 快速 Schema 演进

- `fast_schema_evolution`：是否为表启用快速 Schema 演进。有效值为 `TRUE` 或 `FALSE`（默认）。启用快速 Schema 演进可以提高 Schema 变更的速度，并减少添加或删除列时的资源使用。目前，此属性只能在表创建时启用，创建表后不能使用 ALTER TABLE 进行修改。

  :::note

  - 快速 Schema 演进从 v3.2.0 开始支持无共享集群。
  - 快速 Schema 演进从 v3.3 开始支持共享数据集群，并默认启用。在共享数据集群中创建云原生表时，您无需指定此属性。FE 动态参数 `enable_fast_schema_evolution`（默认值：true）控制此行为。
:::

- `cloud_native_fast_schema_evolution_v2`：是否为 **云原生表** 启用快速 Schema 演进 v2。从 v4.1 开始支持。有效值为 `TRUE`（默认）或 `FALSE`。启用快速 Schema 演进 v2 后，Schema 变更成为一个同步过程。当 ALTER TABLE 语句成功返回时，新 Schema 立即生效。系统将只修改 FE 元数据，而不是 S3 上的 Tablet 元数据，因此无论表中有多少分区或 Tablet，它都可以始终实现秒级延迟。而在旧行为中，Schema 变更作为异步作业运行，随时间更新 Tablet 元数据。

  :::note

  - 快速 Schema 演进 v2 从 v4.1 开始支持，并且仅适用于 **云原生表** 在共享数据集群中。
  - 默认行为：
    - 对于在 v4.1 集群中创建的新表，快速 Schema 演进 v2 默认启用。
    - 对于从升级到 v4.1 的集群中的现有表，快速 Schema 演进 v2 默认禁用。您可以通过 [ALTER TABLE](ALTER_TABLE.md) 显式地将此属性设置为 `true` 来启用它。
  - 降级要求：
    - 要将共享数据集群从 v4.1 降级到 v4.0.5 或更高版本，您可以按照标准降级过程直接降级。
    - 在将共享数据集群从 v4.1 降级到 v3.x 或早于 v4.0.5 的补丁版本之前，对于任何通过 ALTER TABLE 启用了 Fast Schema Evolution v2 的表，您必须手动将 `cloud_native_fast_schema_evolution_v2` 设置为 `false`。您必须等到异步作业变为 FINISHED。您可以通过 SHOW ALTER 跟踪作业状态。
:::

您可以通过 [SHOW ALTER TABLE COLUMN](./SHOW_ALTER.md)。

示例：

```SQL
-- 列出表中最近的列/模式变更作业
SHOW ALTER TABLE COLUMN FROM test_db WHERE TableName = "test_tbl";
```

对于启用了 Fast Schema Evolution v2 的云原生表，模式变更作业通常会显示为 FINISHED，因为变更仅通过更新 FE 元数据来应用。

### 禁止 Base Compaction

`base_compaction_forbidden_time_ranges`：禁止对表进行 Base Compaction 的时间范围。设置此属性后，系统仅在指定时间范围之外对符合条件的 Tablet 执行 Base Compaction。此属性从 v3.2.13 开始支持。

:::note
请确保在禁止 Base Compaction 期间，向表中加载的数据量不超过 500。
:::

`base_compaction_forbidden_time_ranges` 的值遵循 [Quartz cron 语法](https://productresources.collibra.com/docs/collibra/latest/Content/Cron/co_quartz-cron-syntax.htm)，并且只支持这些字段：`<minute> <hour> <day-of-the-month> <month> <day-of-the-week>`，其中 `<minute>` 必须是 `*`。

```SQL
crontab_param_value ::= [ "" | crontab ]

crontab ::= * <hour> <day-of-the-month> <month> <day-of-the-week>
```

- 当此属性未设置或设置为 `""`（空字符串）时，Base Compaction 在任何时候都不被禁止。
- 当此属性设置为 `* * * * *` 时，Base Compaction 始终被禁止。
- 其他值遵循 Quartz cron 语法。
  - 独立值表示字段的单位时间。例如，`<hour>` 字段中的 `8` 表示 8:00-8:59。
  - 值范围表示字段的时间范围。例如，`<hour>` 字段中的 `8-9` 表示 8:00-9:59。
  - 多个用逗号分隔的值范围表示字段的多个时间范围。
  - `<day of the week>` 的起始值为 `1` 表示星期日，`7` 表示星期六。

示例：

```SQL
-- 禁止每天上午 8:00 到晚上 9:00 进行 Base Compaction。
'base_compaction_forbidden_time_ranges' = '* 8-20 * * *'

-- 禁止每天凌晨 0:00 到凌晨 5:00 以及晚上 9:00 到晚上 11:00 进行 Base Compaction。
'base_compaction_forbidden_time_ranges' = '* 0-4,21-22 * * *'

-- 禁止周一到周五进行 Base Compaction（即允许在周六和周日进行）。
'base_compaction_forbidden_time_ranges' = '* * * * 2-6'

-- 禁止每个工作日（即周一到周五）上午 8:00 到晚上 9:00 进行 Base Compaction。
'base_compaction_forbidden_time_ranges' = '* 8-20 * * 2-6'
```

### 指定通用分区表达式 TTL

从 v3.5.0 开始，StarRocks 原生表支持通用分区表达式 TTL。

`partition_retention_condition`：声明要动态保留的分区的表达式。不符合表达式中条件的分区将定期被删除。

- 表达式只能包含分区列和常量。不支持非分区列。
- 通用分区表达式对 List 分区和 Range 分区有不同的应用方式：
  - 对于 List 分区表，StarRocks 支持删除通过通用分区表达式过滤的分区。
  - 对于 Range 分区表，StarRocks 只能使用 FE 的分区裁剪能力来过滤和删除分区。与分区裁剪不支持的谓词对应的分区无法被过滤和删除。

示例：

```SQL
-- 保留最近三个月的数据。列 `dt` 是表的分区列。
"partition_retention_condition" = "dt >= CURRENT_DATE() - INTERVAL 3 MONTH"
```

要禁用此功能，您可以使用 ALTER TABLE 语句将此属性设置为空字符串：

```SQL
ALTER TABLE tbl SET('partition_retention_condition' = '');
```

### 在表级别设置 Flat JSON 属性

在 v3.3 中，StarRocks 引入了 [Flat JSON](../../../using_starrocks/Flat_json.md) 功能，以提高 JSON 数据查询效率并降低使用 JSON 的复杂性。此功能由特定的 BE 配置项和系统变量控制。因此，它只能全局启用（或禁用）。

从 v4.0 开始，您可以在表级别设置 Flat JSON 相关属性。

```SQL
PROPERTIES (
    "flat_json.enable" = "{ true | false }",
    "flat_json.null.factor" = "",
    "flat_json.sparsity.factor" = "",
    "flat_json.column.max" = ""
)
```

- `flat_json.enable`（可选）：是否启用 Flat JSON 功能。启用此功能后，新加载的 JSON 数据将自动扁平化，从而提高 JSON 查询性能。
- `flat_json.null.factor`（可选）：列中 NULL 值的比例阈值。如果列中 NULL 值的比例高于此阈值，则 Flat JSON 不会提取该列。此参数仅在 `flat_json.enable` 设置为 `true` 时生效。默认值：`0.3`。
- `flat_json.sparsity.factor`（可选）：同名列的比例阈值。如果同名列的比例低于此值，则 Flat JSON 不会提取该列。此参数仅在 `flat_json.enable` 设置为 `true` 时生效。默认值：`0.3`。
- `flat_json.column.max`（可选）：Flat JSON 可提取的最大子字段数。此参数仅在 `flat_json.enable` 设置为 `true` 时生效。默认值：`100`。

## 示例

### 带有Hash分桶和列式存储的聚合表

```SQL
CREATE TABLE example_db.table_hash
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### 设置了存储介质和冷却时间的聚合表

```SQL
CREATE TABLE example_db.table_hash
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT SUM DEFAULT "10"
)
ENGINE=olap
UNIQUE KEY(k1, k2)
DISTRIBUTED BY HASH (k1, k2)
PROPERTIES(
    "storage_type"="column",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

### 带有Range分区、Hash分桶、列式存储、存储介质和冷却时间的Duplicate Key表

小于

```SQL
CREATE TABLE example_db.table_range
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "storage_medium" = "SSD", 
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

注意：

此语句将创建三个数据分区：

```SQL
( {    MIN     },   {"2014-01-01"} )
[ {"2014-01-01"},   {"2014-06-01"} )
[ {"2014-06-01"},   {"2014-12-01"} )
```

超出这些范围的数据将不会被加载。

固定范围

```SQL
CREATE TABLE table_range
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1, k2, k3)
(
    PARTITION p1 VALUES [("2014-01-01", "10", "200"), ("2014-01-01", "20", "300")),
    PARTITION p2 VALUES [("2014-06-01", "100", "200"), ("2014-07-01", "100", "300"))
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "storage_medium" = "SSD"
);
```

### MySQL外表

```SQL
CREATE EXTERNAL TABLE example_db.table_mysql
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=mysql
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "8239",
    "user" = "mysql_user",
    "password" = "mysql_passwd",
    "database" = "mysql_db_test",
    "table" = "mysql_table_test"
)
```

### 带有HLL列的表

```SQL
CREATE TABLE example_db.example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 HLL HLL_UNION,
    v2 HLL HLL_UNION
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### 使用BITMAP_UNION聚合类型的表

`v1` 和 `v2` 列的原始数据类型必须是 TINYINT、SMALLINT 或 INT。

```SQL
CREATE TABLE example_db.example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 BITMAP BITMAP_UNION,
    v2 BITMAP BITMAP_UNION
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### 支持Colocate Join的表

```SQL
CREATE TABLE `t1` 
(
     `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) 
ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES 
(
    "colocate_with" = "t1"
);

CREATE TABLE `t2` 
(
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) 
ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES 
(
    "colocate_with" = "t1"
);
```

### 带有位图索引的表

```SQL
CREATE TABLE example_db.table_hash
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM,
    INDEX k1_idx (k1) USING BITMAP COMMENT 'xxxxxx'
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### 动态分区表

动态分区功能必须在FE配置中启用（"dynamic_partition.enable" = "true"）。更多信息，请参阅[配置动态分区](#configure-dynamic-partitions)。

此示例创建未来三天的分区，并删除三天前创建的分区。例如，如果今天是2020-01-08，将创建以下名称的分区：p20200108、p20200109、p20200110、p20200111，其范围是：

```plaintext
[types: [DATE]; keys: [2020-01-08]; ‥types: [DATE]; keys: [2020-01-09]; )
[types: [DATE]; keys: [2020-01-09]; ‥types: [DATE]; keys: [2020-01-10]; )
[types: [DATE]; keys: [2020-01-10]; ‥types: [DATE]; keys: [2020-01-11]; )
[types: [DATE]; keys: [2020-01-11]; ‥types: [DATE]; keys: [2020-01-12]; )
```

```SQL
CREATE TABLE example_db.dynamic_partition
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "storage_medium" = "SSD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10"
);
```

### 批量创建多个分区，并以整型列作为分区列的表

在以下示例中，分区列 `datekey` 的类型为 INT。所有分区仅通过一个简单的分区子句 `START ("1") END ("5") EVERY (1)` 创建。所有分区的范围从 `1` 开始，到 `5` 结束，分区粒度为 `1`：

> **注意**
>
> 分区列的值在**START()**和**END()**中需要用引号括起来，而分区粒度在**EVERY()**中不需要用引号括起来。

```SQL
CREATE TABLE site_access (
    datekey INT,
    site_id INT,
    city_code SMALLINT,
    user_name VARCHAR(32),
    pv BIGINT DEFAULT '0'
)
ENGINE=olap
DUPLICATE KEY(datekey, site_id, city_code, user_name)
PARTITION BY RANGE (datekey) (START ("1") END ("5") EVERY (1)
)
DISTRIBUTED BY HASH(site_id)
PROPERTIES ("replication_num" = "3");
```

### Hive外表

在创建Hive外表之前，您必须已经创建了Hive资源和数据库。更多信息，请参阅[外表](../../../data_source/External_table.md#deprecated-hive-external-table)。

```SQL
CREATE EXTERNAL TABLE example_db.table_hive
(
    k1 TINYINT,
    k2 VARCHAR(50),
    v INT
)
ENGINE=hive
PROPERTIES
(
    "resource" = "hive0",
    "database" = "hive_db_name",
    "table" = "hive_table_name"
);
```

### 带有特定排序键的Primary Key表

假设您需要从用户地址和上次活跃时间等维度实时分析用户行为。创建表时，您可以将 `user_id` 列定义为主键，并将 `address` 和 `last_active` 列的组合定义为排序键。

```SQL
create table users (
    user_id bigint NOT NULL,
    name string NOT NULL,
    email string NULL,
    address string NULL,
    age tinyint NULL,
    sex tinyint NULL,
    last_active datetime,
    property0 tinyint NOT NULL,
    property1 tinyint NOT NULL,
    property2 tinyint NOT NULL,
    property3 tinyint NOT NULL
) 
PRIMARY KEY (`user_id`)
DISTRIBUTED BY HASH(`user_id`)
ORDER BY(`address`,`last_active`)
PROPERTIES(
    "replication_num" = "3",
    "enable_persistent_index" = "true"
);
```

### 分区临时表

```SQL
CREATE TEMPORARY TABLE example_db.temp_table
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2);
```

### 带有Flat JSON属性的表

```SQL
CREATE TABLE example_db.example_table
(
    k1 DATE,
    k2 INT,
    v1 VARCHAR(2048),
    v2 JSON
)
ENGINE=olap
DUPLICATE KEY(k1, k2)
DISTRIBUTED BY HASH(k2)
PROPERTIES (
    "flat_json.enable" = "true",
    "flat_json.null.factor" = "0.5",
    "flat_json.sparsity.factor" = "0.5",
    "flat_json.column.max" = "50"
);
```

## 参考

- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
- [显示表](SHOW_TABLES.md)
- [使用](../Database/USE.md)
- [修改表](ALTER_TABLE.md)
- [删除表](DROP_TABLE.md)
