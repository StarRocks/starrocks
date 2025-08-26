---
displayed_sidebar: docs
keywords: ['chuang jian']
---

# CREATE TABLE

在 StarRocks 中创建一个新表。

:::tip
此操作需要对目标数据库具有 CREATE TABLE 权限。
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

- 您创建的表名、分区名、列名和索引名必须遵循 [系统限制](../../System_limit.md) 中的命名约定。
- 当您指定数据库名、表名、列名或分区名时，请注意，某些字面量在 StarRocks 中被用作保留关键字。不要在 SQL 语句中直接使用这些关键字。如果您想在 SQL 语句中使用这样的关键字，请将其用一对反引号（`）括起来。有关这些保留关键字，请参见 [关键字](../keywords.md)。

:::

## 关键字

### `EXTERNAL`

:::caution
`EXTERNAL` 关键字已弃用。

我们建议您使用 [external catalogs](../../../data_source/catalog/catalog_overview.md) 从 Hive、Iceberg、Hudi 和 JDBC 数据源查询数据，而不是使用 `EXTERNAL` 关键字创建外部表。

:::

:::tip 
**推荐**

从 v3.1 开始，StarRocks 支持在 Iceberg catalogs 中创建 Parquet 格式的表，并支持使用 INSERT INTO 将数据写入这些 Parquet 格式的 Iceberg 表。

从 v3.2 开始，StarRocks 支持在 Hive catalogs 中创建 Parquet 格式的表，并支持使用 INSERT INTO 将数据写入这些 Parquet 格式的 Hive 表。从 v3.3 开始，StarRocks 支持在 Hive catalogs 中创建 ORC 和 Textfile 格式的表，并支持使用 INSERT INTO 将数据写入这些 ORC 和 Textfile 格式的 Hive 表。
:::

如果您想使用已弃用的 `EXTERNAL` 关键字，请展开 **`EXTERNAL` 关键字详情**

<details>

<summary>`EXTERNAL` 关键字详情</summary>

要创建一个外部表以查询外部数据源，请指定 `CREATE EXTERNAL TABLE` 并将 `ENGINE` 设置为以下任一值。您可以参考 [External table](../../../data_source/External_table.md) 了解更多信息。

- 对于 MySQL 外部表，指定以下属性：

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

    MySQL 中的 "table_name" 应指示实际的表名。相反，CREATE TABLE 语句中的 "table_name" 指示此 MySQL 表在 StarRocks 中的名称。它们可以不同也可以相同。

    在 StarRocks 中创建 MySQL 表的目的是访问 MySQL 数据库。StarRocks 本身不维护或存储任何 MySQL 数据。

- 对于 Elasticsearch 外部表，指定以下属性：

    ```plaintext
    PROPERTIES (
    "hosts" = "http://192.168.xx.xx:8200,http://192.168.xx0.xx:8200",
    "user" = "root",
    "password" = "root",
    "index" = "tindex",
    "type" = "doc"
    )
    ```

  - `hosts`: 用于连接您的 Elasticsearch 集群的 URL。您可以指定一个或多个 URL。
  - `user`: 用于登录启用了基本身份验证的 Elasticsearch 集群的 root 用户的账户。
  - `password`: 上述 root 账户的密码。
  - `index`: StarRocks 表在您的 Elasticsearch 集群中的索引。索引名称与 StarRocks 表名称相同。您可以将此参数设置为 StarRocks 表的别名。
  - `type`: 索引的类型。默认值为 `doc`。

- 对于 Hive 外部表，指定以下属性：

    ```plaintext
    PROPERTIES (
        "database" = "hive_db_name",
        "table" = "hive_table_name",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
    )
    ```

    这里，database 是 Hive 表中对应数据库的名称。table 是 Hive 表的名称。`hive.metastore.uris` 是服务器地址。

- 对于 JDBC 外部表，指定以下属性：

    ```plaintext
    PROPERTIES (
    "resource"="jdbc0",
    "table"="dest_tbl"
    )
    ```

    `resource` 是 JDBC 资源名称，`table` 是目标表。

- 对于 Iceberg 外部表，指定以下属性：

   ```plaintext
    PROPERTIES (
    "resource" = "iceberg0", 
    "database" = "iceberg", 
    "table" = "iceberg_table"
    )
    ```

    `resource` 是 Iceberg 资源名称。`database` 是 Iceberg 数据库。`table` 是 Iceberg 表。

- 对于 Hudi 外部表，指定以下属性：

  ```plaintext
    PROPERTIES (
    "resource" = "hudi0", 
    "database" = "hudi", 
    "table" = "hudi_table" 
    )
    ```

</details>

### `TEMPORARY`

创建一个临时表。从 v3.3.1 开始，StarRocks 支持在 Default Catalog 中创建临时表。有关更多信息，请参见 [Temporary Table](../../../table_design/StarRocks_table_design.md#temporary-table)。

:::note
创建临时表时，必须将 `ENGINE` 设置为 `olap`。
:::

## column_definition

```SQL
col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"] [AUTO_INCREMENT] [AS generation_expr]
```

### col_name

请注意，通常您不能创建以 `__op` 或 `__row` 开头的列名，因为这些名称格式在 StarRocks 中保留用于特殊用途，创建此类列可能会导致未定义的行为。如果您确实需要创建此类列，请将 FE 动态参数 [`allow_system_reserved_names`](../../../administration/management/FE_configuration.md#allow_system_reserved_names) 设置为 `TRUE`。

### col_type

特定列信息，例如类型和范围：

- TINYINT (1 字节)：范围从 -2^7 + 1 到 2^7 - 1。
- SMALLINT (2 字节)：范围从 -2^15 + 1 到 2^15 - 1。
- INT (4 字节)：范围从 -2^31 + 1 到 2^31 - 1。
- BIGINT (8 字节)：范围从 -2^63 + 1 到 2^63 - 1。
- LARGEINT (16 字节)：范围从 -2^127 + 1 到 2^127 - 1。
- FLOAT (4 字节)：支持科学计数法。
- DOUBLE (8 字节)：支持科学计数法。
- DECIMAL[(precision, scale)] (16 字节)

  - 默认值：DECIMAL(10, 0)
  - precision: 1 ~ 38
  - scale: 0 ~ precision
  - 整数部分：precision - scale

    不支持科学计数法。

- DATE (3 字节)：范围从 0000-01-01 到 9999-12-31。
- DATETIME (8 字节)：范围从 0000-01-01 00:00:00 到 9999-12-31 23:59:59。
- CHAR[(length)]：固定长度字符串。范围：1 ~ 255。默认值：1。
- VARCHAR[(length)]：可变长度字符串。默认值为 1。单位：字节。在 StarRocks 2.1 之前的版本中，`length` 的值范围为 1–65533。[预览] 在 StarRocks 2.1 及更高版本中，`length` 的值范围为 1–1048576。
- HLL (1~16385 字节)：对于 HLL 类型，无需指定长度或默认值。长度将根据数据聚合在系统内控制。HLL 列只能通过 [hll_union_agg](../../sql-functions/aggregate-functions/hll_union_agg.md)、[Hll_cardinality](../../sql-functions/scalar-functions/hll_cardinality.md) 和 [hll_hash](../../sql-functions/scalar-functions/hll_hash.md) 查询或使用。
- BITMAP：Bitmap 类型不需要指定长度或默认值。它表示一组无符号 bigint 数字。最大元素可以达到 2^64 - 1。

### agg_type

聚合类型。如果未指定，则此列为键列。
如果指定，则为值列。支持的聚合类型如下：

- `SUM`、`MAX`、`MIN`、`REPLACE`
- `HLL_UNION`（仅适用于 `HLL` 类型）
- `BITMAP_UNION`（仅适用于 `BITMAP`）
- `REPLACE_IF_NOT_NULL`：这意味着只有在导入的数据为非空值时才会替换。如果为 null 值，StarRocks 将保留原始值。

:::note
- 当导入聚合类型为 BITMAP_UNION 的列时，其原始数据类型必须为 TINYINT、SMALLINT、INT 和 BIGINT。
- 如果在创建表时为 REPLACE_IF_NOT_NULL 列指定了 NOT NULL，StarRocks 仍会将数据转换为 NULL 而不会向用户发送错误报告。这样，用户可以选择性地导入列。
:::

此聚合类型仅适用于键描述类型为 AGGREGATE KEY 的聚合表。自 v3.1.9 起，`REPLACE_IF_NOT_NULL` 新增支持 BITMAP 类型的列。

**NULL | NOT NULL**：列是否允许为 `NULL`。默认情况下，表中使用明细表、聚合表或更新表的所有列都指定为 `NULL`。在使用主键表的表中，默认情况下，值列指定为 `NULL`，而键列指定为 `NOT NULL`。如果原始数据中包含 `NULL` 值，请使用 `\N` 表示。在数据导入期间，StarRocks 将 `\N` 视为 `NULL`。

**DEFAULT "default_value"**：列的默认值。当您将数据导入 StarRocks 时，如果映射到该列的源字段为空，StarRocks 会自动在该列中填充默认值。您可以通过以下方式之一指定默认值：

- **DEFAULT current_timestamp**：使用当前时间作为默认值。有关更多信息，请参见 [current_timestamp()](../../sql-functions/date-time-functions/current_timestamp.md)。
- **DEFAULT `<default_value>`**：使用列数据类型的给定值作为默认值。例如，如果列的数据类型为 VARCHAR，您可以指定一个 VARCHAR 字符串，例如 beijing，作为默认值，如 `DEFAULT "beijing"` 所示。请注意，默认值不能是以下类型之一：ARRAY、BITMAP、JSON、HLL 和 BOOLEAN。
- **DEFAULT (\<expr\>)**：使用给定函数返回的结果作为默认值。仅支持 [uuid()](../../sql-functions/utility-functions/uuid.md) 和 [uuid_numeric()](../../sql-functions/utility-functions/uuid_numeric.md) 表达式。

**AUTO_INCREMENT**：指定一个 `AUTO_INCREMENT` 列。`AUTO_INCREMENT` 列的数据类型必须为 BIGINT。自增 ID 从 1 开始，步长为 1。有关 `AUTO_INCREMENT` 列的更多信息，请参见 [AUTO_INCREMENT](auto_increment.md)。自 v3.0 起，StarRocks 支持 `AUTO_INCREMENT` 列。

**AS generation_expr**：指定生成列及其表达式。[生成列](../generated_columns.md) 可用于预计算和存储表达式的结果，这显著加速了具有相同复杂表达式的查询。自 v3.1 起，StarRocks 支持生成列。

## index_definition

```SQL
INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'
```

有关参数描述和使用注意事项的更多信息，请参见 [Bitmap 索引](../../../table_design/indexes/Bitmap_index.md#create-an-index)。

## `ENGINE`

默认值：`olap`。如果未指定此参数，则默认创建 OLAP 表（StarRocks 内表）。

可选值：`mysql`、`elasticsearch`、`hive`、`jdbc`、`iceberg` 和 `hudi`。

## Key

语法：

```SQL
key_type(k1[,k2 ...])
```

数据按指定的键列排序，并且不同的键类型具有不同的属性：

- AGGREGATE KEY：键列中的相同内容将根据指定的聚合类型聚合到值列中。通常适用于财务报表和多维分析等业务场景。
- UNIQUE KEY/PRIMARY KEY：键列中的相同内容将根据导入顺序在值列中替换。可以应用于对键列进行增、删、改、查。
- DUPLICATE KEY：键列中的相同内容同时存在于 StarRocks 中。可用于存储明细数据或无聚合属性的数据。

  :::note
  DUPLICATE KEY 是默认类型。数据将根据键列排序。
  :::

:::note
当使用其他 key_type 创建表时，值列不需要指定聚合类型，AGGREGATE KEY 除外。
:::

## COMMENT

您可以在创建表时添加表注释，选填。请注意，COMMENT 必须放在 `key_desc` 之后。否则，无法创建表。

从 v3.1 开始，您可以使用 `ALTER TABLE <table_name> COMMENT = "new table comment"` 修改表注释。

## 分区

可以通过以下方式管理分区：

### 动态创建分区

[动态分区](../../../table_design/data_distribution/dynamic_partitioning.md) 提供了分区的生存时间 (TTL) 管理。StarRocks 自动提前创建新分区并删除过期分区，以确保数据的新鲜度。要启用此功能，您可以在创建表时配置动态分区相关属性。

### 逐个创建分区

#### 仅为分区指定上限

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

- 有关分区命名约定，请参见 [系统限制](../../System_limit.md)。
- 在 v3.3.0 之前，范围分区的列仅支持以下类型：TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DATE 和 DATETIME。自 v3.3.0 起，三种特定时间函数可以用作范围分区的列。有关详细用法，请参见 [数据分布](../../../table_design/data_distribution/Data_distribution.md#manually-create-partitions)。
- 分区是左闭右开的。第一个分区的左边界为最小值。
- NULL 值仅存储在包含最小值的分区中。当删除包含最小值的分区时，无法再导入 NULL 值。
- 分区列可以是单列或多列。分区值为默认最小值。
- 当仅指定一个列作为分区列时，可以将 `MAXVALUE` 设置为最近分区的分区列的上限。

  ```SQL
  PARTITION BY RANGE (pay_dt) (
    PARTITION p1 VALUES LESS THAN ("20210102"),
    PARTITION p2 VALUES LESS THAN ("20210103"),
    PARTITION p3 VALUES LESS THAN MAXVALUE
  )
  ```

:::note
- 分区通常用于管理与时间相关的数据。
- 当需要数据回溯时，您可能需要考虑清空第一个分区，以便在必要时添加分区。
:::

#### 为分区指定上下限

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
- 指定上下限的方式比 LESS THAN 更灵活。您可以自定义左右分区。
- 指定上下限的方式在其他方面与 LESS THAN 相同。
- 当仅指定一个列作为分区列时，可以将 `MAXVALUE` 设置为最近分区的分区列的上限。
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

描述

您可以在 `START()` 和 `END()` 中指定起始和结束值，并在 `EVERY()` 中指定时间单位或分区粒度，以批量创建多个分区。

- 在 v3.3.0 之前，范围分区的列仅支持以下类型：TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DATE 和 DATETIME。自 v3.3.0 起，三种特定时间函数可以用作范围分区的列。有关详细用法，请参见 [数据分布](../../../table_design/data_distribution/Data_distribution.md#manually-create-partitions)。
- 如果分区列是日期类型，则需要使用 `INTERVAL` 关键字指定时间间隔。您可以将时间单位指定为小时（自 v3.0 起）、天、周、月或年。分区的命名约定与动态分区相同。

有关更多信息，请参见 [数据分布](../../../table_design/data_distribution/Data_distribution.md)。

## 数据分布

StarRocks 支持哈希分桶和随机分桶。如果您不配置分桶，StarRocks 默认使用随机分桶并自动设置桶的数量。

- 随机分桶（自 v3.1 起）

  对于分区中的数据，StarRocks 将数据随机分布在所有桶中，而不是基于特定的列值。如果您希望 StarRocks 自动设置桶的数量，则无需指定任何分桶配置。如果您选择手动指定桶的数量，语法如下：

  ```SQL
  DISTRIBUTED BY RANDOM BUCKETS <num>
  ```
  
  但是，请注意，当您查询大量数据并频繁使用某些列作为条件列时，随机分桶提供的查询性能可能不理想。在这种情况下，建议使用哈希分桶。因为只需扫描和计算少量桶，从而显著提高查询性能。

  **注意事项**
  - 您只能使用随机分桶创建明细表。
  - 您不能为随机分桶的表指定 [Colocation Group](../../../using_starrocks/Colocate_join.md)。
  - Spark Load 不能用于将数据导入随机分桶的表。
  - 自 StarRocks v2.5.7 起，创建表时无需设置桶的数量。StarRocks 会自动设置桶的数量。如果您想设置此参数，请参见 [设置桶的数量](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)。

  有关更多信息，请参见 [随机分桶](../../../table_design/data_distribution/Data_distribution.md#random-bucketing-since-v31)。

- 哈希分桶

  语法：

  ```SQL
  DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
  ```

  分区中的数据可以根据分桶列的哈希值和桶的数量细分为多个桶。我们建议您选择满足以下两个要求的列作为分桶列。

  - 高基数列，例如 ID
  - 经常在查询中用作过滤器的列

  如果不存在这样的列，您可以根据查询的复杂性确定分桶列。

  - 如果查询复杂，建议选择高基数列作为分桶列，以确保桶之间的数据分布均衡，提高集群资源利用率。
  - 如果查询相对简单，建议选择经常用作查询条件的列作为分桶列，以提高查询效率。

  如果使用一个分桶列无法将分区数据均匀分布到每个桶中，您可以选择多个分桶列（最多三个）。有关更多信息，请参见 [选择分桶列](../../../table_design/data_distribution/Data_distribution.md#hash-bucketing)。

  **注意事项**：
  - **创建表时，必须指定其分桶列**。
  - 分桶列的值不能更新。
  - 指定后，分桶列不能修改。
  - 自 StarRocks v2.5.7 起，创建表时无需设置桶的数量。StarRocks 会自动设置桶的数量。如果您想设置此参数，请参见 [设置桶的数量](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)。

## Rollup 索引

您可以在创建表时批量创建 rollup。

语法：

```SQL
ROLLUP (rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...)
```

## ORDER BY

自 v3.0 起，主键表支持使用 `ORDER BY` 定义排序键。自 v3.3 起，明细表、聚合表和更新表支持使用 `ORDER BY` 定义排序键。

有关排序键的更多描述，请参见 [排序键和前缀索引](../../../table_design/indexes/Prefix_index_sort_key.md)。

## PROPERTIES

### 存储和副本

如果引擎类型为 `OLAP`，您可以在创建表时指定初始存储介质（`storage_medium`）、自动存储降冷时间（`storage_cooldown_time`）或时间间隔（`storage_cooldown_ttl`）以及副本数量（`replication_num`）。

属性生效的范围：如果表只有一个分区，则属性属于表。如果表分为多个分区，则属性属于每个分区。当您需要为指定分区配置不同的属性时，可以在创建表后执行 [ALTER TABLE ... ADD PARTITION 或 ALTER TABLE ... MODIFY PARTITION](ALTER_TABLE.md)。

#### 设置初始存储介质和自动存储降冷时间

```sql
PROPERTIES (
    "storage_medium" = "[SSD|HDD]",
    { "storage_cooldown_ttl" = "<num> { YEAR | MONTH | DAY | HOUR } "
    | "storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss" }
)
```

**属性**

- `storage_medium`：初始存储介质，可以设置为 `SSD` 或 `HDD`。确保您明确指定的存储介质类型与 BE 静态参数 `storage_root_path` 为您的 StarRocks 集群指定的 BE 磁盘类型一致。<br />

    如果 FE 配置项 `enable_strict_storage_medium_check` 设置为 `true`，系统在创建表时严格检查 BE 磁盘类型。如果您在 CREATE TABLE 中指定的存储介质与 BE 磁盘类型不一致，则返回错误 "Failed to find enough host in all backends with storage medium is SSD|HDD."，并且表创建失败。如果 `enable_strict_storage_medium_check` 设置为 `false`，系统会忽略此错误并强制创建表。但是，加载数据后，集群磁盘空间可能会分布不均。<br />

    从 v2.3.6、v2.4.2、v2.5.1 和 v3.0 起，如果未明确指定 `storage_medium`，系统会根据 BE 磁盘类型自动推断存储介质。<br />

  - 在以下场景中，系统会自动将此参数设置为 SSD：

    - BEs 报告的磁盘类型（`storage_root_path`）仅包含 SSD。
    - BEs 报告的磁盘类型（`storage_root_path`）同时包含 SSD 和 HDD。请注意，从 v2.3.10、v2.4.5、v2.5.4 和 v3.0 起，当 BEs 报告的 `storage_root_path` 同时包含 SSD 和 HDD 且属性 `storage_cooldown_time` 被指定时，系统会将 `storage_medium` 设置为 SSD。

  - 在以下场景中，系统会自动将此参数设置为 HDD：

    - BEs 报告的磁盘类型（`storage_root_path`）仅包含 HDD。
    - 从 2.3.10、2.4.5、2.5.4 和 3.0 起，当 BEs 报告的 `storage_root_path` 同时包含 SSD 和 HDD 且属性 `storage_cooldown_time` 未指定时，系统会将 `storage_medium` 设置为 HDD。

- `storage_cooldown_ttl` 或 `storage_cooldown_time`：自动存储降冷时间或时间间隔。自动存储降冷是指将数据从 SSD 自动迁移到 HDD。此功能仅在初始存储介质为 SSD 时有效。

  - `storage_cooldown_ttl`：此表中分区的自动存储降冷的**时间间隔**。如果您需要保留最近的分区在 SSD 上，并在一定时间间隔后自动将较旧的分区降冷到 HDD，可以使用此参数。每个分区的自动存储降冷时间是使用此参数的值加上分区的上限时间计算的。

  支持的值为 `<num> YEAR`、`<num> MONTH`、`<num> DAY` 和 `<num> HOUR`。`<num>` 是一个非负整数。默认值为 null，表示不自动执行存储降冷。

  例如，您在创建表时将值指定为 `"storage_cooldown_ttl"="1 DAY"`，并且存在范围为 `[2023-08-01 00:00:00,2023-08-02 00:00:00)` 的分区 `p20230801`。此分区的自动存储降冷时间为 `2023-08-03 00:00:00`，即 `2023-08-02 00:00:00 + 1 DAY`。如果您在创建表时将值指定为 `"storage_cooldown_ttl"="0 DAY"`，则此分区的自动存储降冷时间为 `2023-08-02 00:00:00`。

  - `storage_cooldown_time`：表从 SSD 降冷到 HDD 的自动存储降冷时间（**绝对时间**）。指定的时间需要晚于当前时间。格式："yyyy-MM-dd HH:mm:ss"。当您需要为指定分区配置不同的属性时，可以执行 [ALTER TABLE ... ADD PARTITION 或 ALTER TABLE ... MODIFY PARTITION](ALTER_TABLE.md)。

##### 用法

- 自动存储降冷相关参数之间的比较如下：
  - `storage_cooldown_ttl`：一个表属性，指定表中分区的自动存储降冷的时间间隔。系统会在 `此参数的值加上分区的上限时间` 时自动降冷分区。因此，自动存储降冷在分区粒度上执行，更加灵活。
  - `storage_cooldown_time`：一个表属性，指定此表的自动存储降冷时间（**绝对时间**）。此外，您可以在创建表后为指定分区配置不同的属性。
  - `storage_cooldown_second`：一个静态 FE 参数，指定集群内所有表的自动存储降冷延迟。

- 表属性 `storage_cooldown_ttl` 或 `storage_cooldown_time` 优先于 FE 静态参数 `storage_cooldown_second`。
- 配置这些参数时，需要指定 `"storage_medium = "SSD"`。
- 如果您不配置这些参数，则不会自动执行自动存储降冷。
- 执行 `SHOW PARTITIONS FROM <table_name>` 查看每个分区的自动存储降冷时间。

##### 限制

- 不支持表达式和 List 分区。
- 分区列需要是日期类型。
- 不支持多个分区列。
- 不支持主键表。

#### 设置分区中每个 tablet 的副本数量

`replication_num`：分区中每个表的副本数量。默认数量：`3`。

```sql
PROPERTIES (
    "replication_num" = "<num>"
)
```

### Bloom filter 索引

如果引擎类型为 `olap`，您可以指定一个列采用 Bloom filter 索引。

使用 Bloom filter 索引时适用以下限制：

- 您可以为明细表或主键表的所有列创建 Bloom filter 索引。对于聚合表或更新表，您只能为键列创建 Bloom filter 索引。
- TINYINT、FLOAT、DOUBLE 和 DECIMAL 列不支持创建 Bloom filter 索引。
- Bloom filter 索引只能提高包含 `in` 和 `=` 运算符的查询性能，例如 `Select xxx from table where x in {}` 和 `Select xxx from table where column = xxx`。此列中更多离散值将导致更精确的查询。

有关更多信息，请参见 [Bloom filter 索引](../../../table_design/indexes/Bloomfilter_index.md)

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

| 参数                   | 必需 | 描述                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| dynamic_partition.enable    | 否       | 是否启用动态分区。有效值：`TRUE` 和 `FALSE`。默认值：`TRUE`。 |
| dynamic_partition.time_unit | 是      | 动态创建分区的时间粒度。它是一个必需参数。有效值：`DAY`、`WEEK` 和 `MONTH`。时间粒度决定了动态创建分区的后缀格式。<br/>  - 如果值为 `DAY`，动态创建分区的后缀格式为 `yyyyMMdd`。示例分区名称后缀为 `20200321`。<br/>  - 如果值为 `WEEK`，动态创建分区的后缀格式为 `yyyy_ww`，例如 `2020_13` 表示 2020 年的第 13 周。<br/>  - 如果值为 `MONTH`，动态创建分区的后缀格式为 `yyyyMM`，例如 `202003`。 |
| dynamic_partition.start     | 否       | 动态分区的起始偏移量。此参数的值必须为负整数。基于当前日、周或月（由 `dynamic_partition.time_unit` 确定），在此偏移量之前的分区将被删除。默认值为 `Integer.MIN_VALUE`，即 -2147483648，表示不会删除历史分区。 |
| dynamic_partition.end       | 是      | 动态分区的结束偏移量。此参数的值必须为正整数。从当前日、周或月到结束偏移量的分区将提前创建。 |
| dynamic_partition.prefix    | 否       | 添加到动态分区名称的前缀。默认值：`p`。 |
| dynamic_partition.buckets   | 否       | 每个动态分区的桶数。默认值与保留字 `BUCKETS` 确定的桶数或 StarRocks 自动设置的桶数相同。 |

:::note

当分区列的类型为 INT 时，其格式必须为 `yyyyMMdd`，无论分区的时间粒度如何。

:::

### 随机分桶的桶大小

自 v3.2 起，对于配置了随机分桶的表，您可以在创建表时使用 `PROPERTIES` 中的 `bucket_size` 参数指定桶大小，以实现按需和动态增加桶的数量。单位：B。

```sql
PROPERTIES (
    "bucket_size" = "1073741824"
)
```

### 数据压缩算法

您可以通过在创建表时添加属性 `compression` 来指定表的数据压缩算法。

`compression` 的有效值为：

- `LZ4`：LZ4 算法。
- `ZSTD`：Zstandard 算法。
- `ZLIB`：zlib 算法。
- `SNAPPY`：Snappy 算法。

从 v3.3.2 起，StarRocks 支持在创建表时为 zstd 压缩格式指定压缩级别。

语法：

```sql
PROPERTIES ("compression" = "zstd(<compression_level>)")
```

`compression_level`：ZSTD 压缩格式的压缩级别。类型：整数。范围：[1,22]。默认：`3`（推荐）。数字越大，压缩比越高。压缩级别越高，压缩和解压缩所需的时间越多。

示例：

```sql
PROPERTIES ("compression" = "zstd(3)")
```

有关如何选择合适的数据压缩算法的更多信息，请参见 [数据压缩](../../../table_design/data_compression.md)。

### 设置数据导入安全等级

如果您的 StarRocks 集群有多个数据副本，您可以为表设置不同的导入安全等级裁，即在 StarRocks 确定导入任务成功之前需要多少个副本返回导入成功。您可以在创建表时通过添加属性 `write_quorum` 指定导入安全等级。此属性自 v2.5 起支持。

`write_quorum` 的有效值为：

- `MAJORITY`：默认值。当**大多数**数据副本返回导入成功时，StarRocks 返回导入任务成功。否则，StarRocks 返回导入任务失败。
- `ONE`：当**一个**数据副本返回导入成功时，StarRocks 返回导入任务成功。否则，StarRocks 返回导入任务失败。
- `ALL`：当**所有**数据副本返回导入成功时，StarRocks 返回导入任务成功。否则，StarRocks 返回导入任务失败。

:::caution
- 为导入设置较低的写入仲裁会增加数据不可访问甚至丢失的风险。例如，您在一个具有两个副本的 StarRocks 集群中将数据导入到一个写入仲裁为 ONE 的表中，并且数据仅成功导入到一个副本中。尽管 StarRocks 确定导入任务成功，但数据只有一个存活的副本。如果存储已加载数据的 tablets 的服务器宕机，这些 tablets 中的数据将无法访问。如果服务器的磁盘损坏，数据将丢失。
- StarRocks 仅在所有数据副本返回状态后才返回导入任务状态。当有副本的导入状态未知时，StarRocks 不会返回导入任务状态。在一个副本中，导入超时也被视为导入失败。
:::

### 副本数据写入和复制模式

如果您的 StarRocks 集群有多个数据副本，您可以在 `PROPERTIES` 中指定 `replicated_storage` 参数来配置副本之间的数据写入和复制模式。

- `true`（v3.0 及更高版本中的默认值）表示“单领导者复制 (single leader replication)”，这意味着数据仅写入主副本 (primary replica)。其他副本从主副本同步数据。此模式显著降低了因数据写入多个副本而导致的 CPU 成本。自 v2.5 起支持。
- `false`（v2.5 中的默认值）表示“无领导者复制 (leaderless replication)”，这意味着数据直接写入多个副本，而不区分主副本和次副本。CPU 成本乘以副本数量。

在大多数情况下，使用默认值可以获得更好的数据写入性能。如果您想更改副本之间的数据写入和复制模式，请运行 ALTER TABLE 命令。示例：

```sql
ALTER TABLE example_db.my_table
SET ("replicated_storage" = "false");
```

### Delta Join 唯一键和外键约束

要在 View Delta Join 场景中启用查询改写，您必须为 Delta Join 中要连接的表定义唯一键约束 `unique_constraints` 和外键约束 `foreign_key_constraints`。有关更多信息，请参见 [异步物化视图 - 在 View Delta Join 场景中改写查询](../../../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#query-delta-join-rewrite)。

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

- `child_column`：表的外键。您可以定义多个 `child_column`。
- `catalog_name`：要连接的表所在的 catalog 名称。如果未指定此参数，则使用默认 catalog。
- `database_name`：要连接的表所在的数据库名称。如果未指定此参数，则使用当前数据库。
- `parent_table_name`：要连接的表的名称。
- `parent_column`：要连接的列。它们必须是相应表的主键或唯一键。

:::caution
- `unique_constraints` 和 `foreign_key_constraints` 仅用于查询改写。在将数据加载到表中时，不保证外键约束检查。您必须确保加载到表中的数据符合约束。
- 主键表的主键或唯一键表的唯一键默认是相应的 `unique_constraints`。您无需手动设置。
- 表中 `foreign_key_constraints` 的 `child_column` 必须引用另一个表中 `unique_constraints` 的 `unique_key`。
- `child_column` 和 `parent_column` 的数量必须一致。
- `child_column` 和相应 `parent_column` 的数据类型必须匹配。
:::

### 存算分离集群的云原生表

要使用您的 StarRocks 存算分离集群，您必须创建具有以下属性的云原生表：

```SQL
PROPERTIES (
    "storage_volume" = "<storage_volume_name>",
    "datacache.enable" = "{ true | false }",
    "datacache.partition_duration" = "<string_value>",
    "file_bundling" = "{ true | false }"
)
```

- `storage_volume`：用于存储要创建的云原生表的存储卷名称。如果未指定此属性，则使用默认存储卷。此属性自 v3.1 起支持。

- `datacache.enable`：是否启用本地磁盘缓存。默认值：`true`。

  - 当此属性设置为 `true` 时，加载的数据同时写入对象存储和本地磁盘（作为查询加速的缓存）。
  - 当此属性设置为 `false` 时，数据仅加载到对象存储中。

  :::note
  要启用本地磁盘缓存，您必须在 BE 配置项 `storage_root_path` 中指定磁盘目录。
  :::

- `datacache.partition_duration`：热数据的有效期。当启用本地磁盘缓存时，所有数据都加载到缓存中。当缓存已满时，StarRocks 从缓存中删除最近未使用的数据。当查询需要扫描已删除的数据时，StarRocks 检查数据是否在有效期内。如果数据在有效期内，StarRocks 将数据重新加载到缓存中。如果数据不在有效期内，StarRocks 不会将其加载到缓存中。此属性是一个字符串值，可以使用以下单位指定：`YEAR`、`MONTH`、`DAY` 和 `HOUR`，例如 `7 DAY` 和 `12 HOUR`。如果未指定，则所有数据都作为热数据缓存。

  :::note
  此属性仅在 `datacache.enable` 设置为 `true` 时可用。
  :::

- `file_bundling`（可选）：是否为云原生表启用 File Bundling 优化功能。该功能自 v4.0 版本起支持。当启用该功能（设置为 `true`）时，系统会自动将导入、Compaction 或 Publish 操作生成的数据文件进行打包，从而减少因频繁访问外部存储系统而产生的 API 成本。

  :::note
  - File Bundling 功能仅适用于使用 StarRocks v4.0 或更高版本的存算分离集群。
  - File Bundling 功能在 v4.0 或更高版本中创建的表格中默认启用，由 FE 配置项 `enable_file_bundling` (默认值：true) 控制。
  - 启用 File Bundling 功能后，您只能将集群降级到 v3.5.2 或更高版本。如果您想降级到 v3.5.2 之前的版本，必须先删除已启用 File Bundling 功能的表。
  - 对集群中已有的表，在集群升级至 v4.0 后，File Bundling 功能仍默认处于禁用状态。
  - 您可以通过 [ALTER TABLE](ALTER_TABLE.md) 语句手动为已有的表启用 File Bundling 功能，但仍存在以下限制：
    - 对于带有在 v4.0 版本之前创建的 Rollup Index 的表，您无法为其启用 File Bundling 功能。您可以在 v4.0 或更高版本中删除并重新创建这些索引，然后为这些表启用 File Bundling 功能。
    - 您无法在特定时间段内**多次**修改 `file_bundling` 属性。否则，系统将返回错误。您可以通过执行以下 SQL 语句来检查 `file_bundling` 属性是否可修改：

      ```SQL
      SELECT METADATA_SWITCH_VERSION FROM information_schema.partitions_meta WHERE TABLE_NAME = '<table_name>';
      ```

      您仅可在返回值为 `0` 时修改 `file_bundling` 属性。非零值表示与 `METADATA_SWITCH_VERSION` 对应的数据版本尚未被 GC 机制回收。您必须等待该数据版本被回收后再进行操作。

      您可以通过将 FE 动态配置 `lake_autovacuum_grace_period_minutes` 的值设置为较小的数值来缩短此间隔。但在修改 `file_bundling` 属性后，请务必将该配置恢复为原始值。
  :::

### 快速模式架构演进

`fast_schema_evolution`：是否为表启用快速模式架构演进。有效值为 `TRUE` 或 `FALSE`（默认）。启用快速模式架构演进可以在添加或删除列时提高架构更改的速度并减少资源使用。目前，此属性只能在创建表时启用，不能在创建表后使用 [ALTER TABLE](ALTER_TABLE.md) 修改。

  :::note
  - 自 v3.2.0 起，存算一体集群支持快速模式架构演进。
  - 自 v3.3 起，存算分离集群支持快速模式架构演进，并默认启用。在存算分离集群中创建云原生表时，您无需指定此属性。FE 动态参数 `enable_fast_schema_evolution`（默认值：true）控制此行为。
  :::

### 禁止 Base Compaction

`base_compaction_forbidden_time_ranges`：表禁止 Base Compaction 的时间范围。设置此属性后，系统仅在指定时间范围之外对符合条件的 tablets 执行 Base Compaction。此属性自 v3.2.13 起支持。

:::note
确保在禁止 Base Compaction 的期间，表的数据加载次数不超过 500。
:::

`base_compaction_forbidden_time_ranges` 的值遵循 [Quartz cron 语法](https://productresources.collibra.com/docs/collibra/latest/Content/Cron/co_quartz-cron-syntax.htm)，仅支持以下字段：`<minute> <hour> <day-of-the-month> <month> <day-of-the-week>`，其中 `<minute>` 必须为 `*`。

```SQL
crontab_param_value ::= [ "" | crontab ]

crontab ::= * <hour> <day-of-the-month> <month> <day-of-the-week>
```

- 当此属性未设置或设置为 `""`（空字符串）时，Base Compaction 在任何时间都不被禁止。
- 当此属性设置为 `* * * * *` 时，Base Compaction 始终被禁止。
- 其他值遵循 Quartz cron 语法。
  - 独立值表示字段的单位时间。例如，`8` 在 `<hour>` 字段中表示 8:00-8:59。
  - 值范围表示字段的时间范围。例如，`8-9` 在 `<hour>` 字段中表示 8:00-9:59。
  - 用逗号分隔的多个值范围表示字段的多个时间范围。
  - `<day of the week>` 的起始值为 `1` 表示星期日，`7` 表示星期六。

示例：

```SQL
-- 每天从上午 8:00 到晚上 9:00 禁止 Base Compaction。
'base_compaction_forbidden_time_ranges' = '* 8-20 * * *'

-- 每天从凌晨 0:00 到凌晨 5:00 和晚上 9:00 到晚上 11:00 禁止 Base Compaction。
'base_compaction_forbidden_time_ranges' = '* 0-4,21-22 * * *'

-- 从星期一到星期五禁止 Base Compaction（即在星期六和星期日允许）。
'base_compaction_forbidden_time_ranges' = '* * * * 2-6'

-- 每个工作日（即星期一到星期五）从上午 8:00 到晚上 9:00 禁止 Base Compaction。
'base_compaction_forbidden_time_ranges' = '* 8-20 * * 2-6'
```

### 指定通用分区表达式 TTL

从 v3.5.0 起，StarRocks 内表支持通用分区表达式 TTL。

`partition_retention_condition`：声明要动态保留的分区的表达式。不符合表达式中条件的分区将定期删除。
- 表达式只能包含分区列和常量。不支持非分区列。
- 通用分区表达式在 List 分区和 Range 分区中的应用不同：
  - 对于具有 List 分区的表，StarRocks 支持删除通过通用分区表达式过滤的分区。
  - 对于具有 Range 分区的表，StarRocks 只能使用 FE 的分区裁剪功能过滤和删除分区。分区对应于分区裁剪不支持的谓词无法被过滤和删除。

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

在 v3.3 版本中，StarRocks 引入了 [Flat JSON](../../../using_starrocks/Flat_json.md) 功能，以提升 JSON 数据查询效率并简化 JSON 的使用复杂度。该功能通过特定的 BE 配置项和系统变量进行控制，因此只能全局启用（或禁用）。

从 v4.0 开始，您可以在表级别设置与 Flat JSON 相关的属性。

```SQL
PROPERTIES (
    "flat_json.enable" = "{ true | false }",
    "flat_json.null.factor" = "",
    "flat_json.sparsity.factor" = "",
    "flat_json.column.max" = ""
)
```

- `flat_json.enable`（可选）：是否启用 Flat JSON 功能。启用此功能后，新导入的 JSON 数据将自动进行扁平化处理，从而提升 JSON 查询性能。
- `flat_json.null.factor`（可选）：列中 NULL 值的比例阈值。如果某列的 NULL 值比例高于此阈值，则该列不会被 Flat JSON 提取。此参数仅在 `flat_json.enable` 设置为 `true` 时生效。默认值：`0.3`。
- `flat_json.sparsity.factor`（可选）：具有相同名称的列的比例阈值。如果具有相同名称的列的比例低于此值，则 Flat JSON 不会提取该列。此参数仅在 `flat_json.enable` 设置为 `true` 时生效。默认值：`0.9`。
- `flat_json.column.max`（可选）：Flat JSON 可提取的子字段最大数量。此参数仅在 `flat_json.enable` 设置为 `true` 时生效。默认值：`100`。

## 示例

### 使用哈希分桶和列式存储的聚合表

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

### 设置存储介质和降冷时间的聚合表

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

### 使用 Range 分区、哈希分桶、列式存储、存储介质和降冷时间的明细表

LESS THAN

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

### MySQL 外部表

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

### 带有 HLL 列的表

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

### 使用 BITMAP_UNION 聚合类型的表

`v1` 和 `v2` 列的原始数据类型必须为 TINYINT、SMALLINT 或 INT。

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

### 支持 Colocate Join 的表

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

### 带有 bitmap 索引的表

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

动态分区功能必须在 FE 配置中启用（"dynamic_partition.enable" = "true"）。有关更多信息，请参见 [配置动态分区](#configure-dynamic-partitions)。

此示例为接下来的三天创建分区，并删除三天前创建的分区。例如，如果今天是 2020-01-08，将创建以下名称的分区：p20200108、p20200109、p20200110 和 p20200111，其范围为：

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

### 使用整数列作为分区列，并批量创建多个分区的表

在以下示例中，分区列 `datekey` 的类型为 INT。所有分区仅通过一个简单的分区子句 `START ("1") END ("5") EVERY (1)` 创建。所有分区的范围从 `1` 开始，到 `5` 结束，分区粒度为 `1`：
> **注意**
>
> **START()** 和 **END()** 中的分区列值需要用引号括起来，而 **EVERY()** 中的分区粒度不需要用引号括起来。

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

### Hive 外部表

在创建 Hive 外部表之前，您必须创建 Hive 资源和数据库。有关更多信息，请参见 [外部表](../../../data_source/External_table.md#deprecated-hive-external-table)。

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

### 带有特定排序键的主键表

假设您需要从用户地址和最后活跃时间等维度实时分析用户行为。在创建表时，您可以将 `user_id` 列定义为主键，并将 `address` 和 `last_active` 列的组合定义为排序键。

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

### 带有 Flat JSON 属性的表

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
- [SHOW TABLES](SHOW_TABLES.md)
- [USE](../Database/USE.md)
- [ALTER TABLE](ALTER_TABLE.md)
- [DROP TABLE](DROP_TABLE.md)
