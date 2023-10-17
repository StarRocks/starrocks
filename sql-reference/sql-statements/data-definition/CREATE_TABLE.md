# CREATE TABLE

## 功能

该语句用于创建表。

> **注意**
>
> 该操作需要有在对应数据库内的建表权限 (CREATE TABLE)。

## 语法

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...]
[, index_definition1[, index_definition2, ...]])
[ENGINE = [olap|mysql|elasticsearch|hive|iceberg|hudi|jdbc]]
[key_desc]
[COMMENT "table comment"]
[partition_desc]
[distribution_desc]
[rollup_index]
[ORDER BY (column_definition1,...)]
[PROPERTIES ("key"="value", ...)]
[BROKER PROPERTIES ("key"="value", ...)]
```

## 参数说明

在指定数据库名、表名和列名等变量时，如果使用了保留关键字，必须使用反引号 (`) 包裹，否则可能会产生报错。有关 StarRocks 的保留关键字列表，请参见[关键字](../keywords.md#保留关键字)。

### **column_definition**

语法：

```sql
col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"] [AUTO_INCREMENT] [AS generation_expr]
```

说明：

**col_name**：列名称

**col_type**：列数据类型

支持的列类型以及取值范围等信息如下：

* TINYINT（1字节）
  范围：-2^7 + 1 ~ 2^7 - 1

* SMALLINT（2字节）
  范围：-2^15 + 1 ~ 2^15 - 1

* INT（4字节）
  范围：-2^31 + 1 ~ 2^31 - 1

* BIGINT（8字节）
  范围：-2^63 + 1 ~ 2^63 - 1

* LARGEINT（16字节）
  范围：-2^127 + 1 ~ 2^127 - 1

* FLOAT（4字节）
  支持科学计数法。

* DOUBLE（8字节）
  支持科学计数法。

* DECIMAL[(precision, scale)] (16字节)
  保证精度的小数类型。默认是 DECIMAL(10, 0)
    precision: 1 ~ 38
    scale: 0 ~ precision
  其中整数部分为：precision - scale
  不支持科学计数法。

* DATE（3字节）
  范围：0000-01-01 ~ 9999-12-31

* DATETIME（8字节）
  范围：0000-01-01 00:00:00 ~ 9999-12-31 23:59:59

* CHAR[(length)]

  定长字符串。长度范围：1 ~ 255。默认为 1。

* VARCHAR[(length)]

  变长字符串。单位：字节，默认取值为 `1`。
  * StarRocks 2.1.0 之前的版本，`length` 的取值范围为 1~65533。
  * 【公测中】自 StarRocks 2.1.0 版本开始，`length` 的取值范围为 1~1048576。

* HLL (1~16385个字节)

  HLL 列类型，不需要指定长度和默认值，长度根据数据的聚合程度系统内控制，并且 HLL 列只能通过配套的 [hll_union_agg](../../sql-functions/aggregate-functions/hll_union_agg.md)、[Hll_cardinality](../../sql-functions/scalar-functions/hll_cardinality.md)、[hll_hash](../../sql-functions/aggregate-functions/hll_hash.md)进行查询或使用。

* BITMAP
  BITMAP 列类型，不需要指定长度和默认值。表示整型的集合，元素个数最大支持到 2^64 - 1。

* ARRAY
  支持在一个数组中嵌套子数组，最多可嵌套 14 层。您必须使用尖括号（ < 和 > ）来声明 ARRAY 的元素类型，如 ARRAY < INT >。目前不支持将数组中的元素声明为 [Fast Decimal](../data-types/DECIMAL.md) 类型。

**agg_type**：聚合类型，如果不指定，则该列为 key 列。否则，该列为 value 列。

```plain
支持的聚合类型如下：

* SUM、MAX、MIN、REPLACE

* HLL_UNION（仅用于 HLL列，为 HLL 独有的聚合方式)。

* BITMAP_UNION（仅用于 BITMAP 列，为 BITMAP 独有的聚合方式)。

* REPLACE_IF_NOT_NULL：这个聚合类型的含义是当且仅当新导入数据是非 NULL 值时会发生替换行为。如果新导入的数据是 NULL，那么 StarRocks 仍然会保留原值。
```

注意：

1. BITMAP_UNION 聚合类型列在导入时的原始数据类型必须是 `TINYINT, SMALLINT, INT, BIGINT`。
2. 如果在建表时 `REPLACE_IF_NOT_NULL` 列指定了 NOT NULL，那么 StarRocks 仍然会将其转化 NULL，不会向用户报错。用户可以借助这个类型完成「部分列导入」的功能。
  该类型只对聚合模型有用 (`key_desc` 的 `type` 为 `AGGREGATE KEY`)。

**NULL | NOT NULL**：列数据是否允许为 `NULL`。其中明细模型、聚合模型和更新模型表中所有列都默认指定 `NULL`。主键模型表的指标列默认指定 `NULL`，维度列默认指定 `NOT NULL`。如源数据文件中存在 `NULL` 值，可以用 `\N` 来表示，导入时 StarRocks 会将其解析为 `NULL`。

**DEFAULT "default_value"**：列数据的默认值。导入数据时，如果该列对应的源数据文件中的字段为空，则自动填充 `DEFAULT` 关键字中指定的默认值。支持以下三种指定方式：

* **DEFAULT current_timestamp**：默认值为当前时间。参见 [current_timestamp()](../../sql-functions/date-time-functions/current_timestamp.md) 。
* **DEFAULT `<默认值>`**：默认值为指定类型的值。例如，列类型为 VARCHAR，即可指定默认值为 `DEFAULT "beijing"`。当前不支持指定 ARRAY、BITMAP、JSON、HLL 和 BOOLEAN 类型为默认值。
* **DEFAULT (`<表达式>`)**：默认值为指定函数返回的结果。目前仅支持 [uuid()](../../sql-functions/utility-functions/uuid.md) 和 [uuid_numeric()](../../sql-functions/utility-functions/uuid_numeric.md) 表达式。

**AUTO_INCREMENT**：指定自增列。自增列的数据类型只支持 BIGINT，自增 ID 从 1 开始增加，自增步长为 1。有关自增列的详细说明，请参见 [AUTO_INCREMENT](../auto_increment.md)。自 v3.0，StarRocks 支持该功能。

**AS generation_expr**：指定生成列和其使用的表达式。[生成列](../generated_columns.md)用于预先计算并存储表达式的结果，可以加速包含复杂表达式的查询。自 v3.1，StarRocks 支持该功能。

### **index_definition**

建表时仅支持创建 bitmap 索引，语法如下。有关参数说明和使用限制，请参见 [Bitmap 索引](/using_starrocks/Bitmap_index.md#创建索引)。

```sql
INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] [COMMENT '']
```

### **ENGINE 类型**

默认为 `olap`，表示创建的是 StarRocks 内部表。

可选值：`mysql`、`elasticsearch`、`hive`、`jdbc` (2.3 及以后)、`iceberg`、`hudi`（2.2 及以后）。如果指定了可选值，则创建的是对应类型的外部表 (external table)，在建表时需要使用 CREATE EXTERNAL TABLE。更多信息，参见[外部表](../../../data_source/External_table.md)。

**从 3.0 版本起，对于查询 Hive、Iceberg、Hudi 和 JDBC 数据源的场景，推荐使用 Catalog 直接查询，不再推荐外部表的方式。具体参见 [Hive catalog](../../../data_source/catalog/hive_catalog.md)、[Iceberg catalog](../../../data_source/catalog/iceberg_catalog.md)、[Hudi catalog](../../../data_source/catalog/hudi_catalog.md) 和 [JDBC catalog](../../../data_source/catalog/jdbc_catalog.md)。**

**从 3.1 版本起，支持直接在 Iceberg catalog 内创建表（当前仅支持 Parquet 格式的表），您可以通过 [INSERT INTO](../data-manipulation/insert.md) 把数据插入到 Iceberg 表中。参见 [创建 Iceberg 表](../../../data_source/catalog/iceberg_catalog.md#创建-iceberg-表)。**

1. 如果是 mysql，则需要在 properties 提供以下信息：

    ```sql
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
    "table" 条目中的 "table_name" 是 MySQL 中的真实表名。
    而 CREATE TABLE 语句中的 table_name 是该 MySQL 表在 StarRocks 中的名字，可以不同。

    在 StarRocks 创建 MySQL 表的目的是可以通过 StarRocks 访问 MySQL 数据库。
    而 StarRocks 本身并不维护、存储任何 MySQL 数据。

2. 如果是 elasticsearch，则需要在 properties 提供以下信息：

    ```sql
    PROPERTIES (
        "hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
        "user" = "root",
        "password" = "root",
        "index" = "tindex",
        "type" = "doc"
    )
    ```

    其中 `hosts` 为 Elasticsearch 集群连接地址，可指定一个或者多个，`user 和 password` 为开启 basic 认证的 Elasticsearch 集群的用户名/密码，`index` 是 StarRocks 中的表对应的 Elasticsearch 的 index 名字，可以是 alias，`type` 指定 index 的类型，默认是 `doc`。

3. 如果是 hive，则需要在 properties 提供以下信息：

    ```sql
    PROPERTIES (
        "database" = "hive_db_name",
        "table" = "hive_table_name",
        "hive.metastore.uris" = "thrift://127.0.0.1:9083"
    )
    ```

    其中 `database` 是 Hive 表对应的库名字，`table` 是 hive 表的名字，`hive.metastore.uris` 是 Hive metastore 服务地址。

4. 如果是 jdbc，则需要在 properties 提供以下信息：

    ```sql
    PROPERTIES (
        "resource"="jdbc0",
        "table"="dest_tbl"
    )
    ```

    其中 `resource` 是所使用 JDBC 资源的名称。`table` 是目标数据库表名。

5. 如果是 iceberg，则需要在 properties 提供以下信息：

    ```sql
    PROPERTIES (
        "resource" = "iceberg0", 
        "database" = "iceberg", 
        "table" = "iceberg_table"
    )
    ```

    其中 `resource` 是引用的 Iceberg 资源的名称。`database` 是 Iceberg 表所属的数据库名称。`table` Iceberg 表名称。
6. 如果是 hudi，则需要在 properties 提供以下信息：

    ```sql
    PROPERTIES (
        "resource" = "hudi0", 
        "database" = "hudi", 
        "table" = "hudi_table" 
    )
    ```

    其中 `resource` 是 Hudi 资源的名称。`database` 是 Hudi 表所属的数据库名称。`table` Hudi 表名称。

### **key_desc**

语法：

```sql
`key_type(k1[,k2 ...])`
```

> 说明
>
数据按照指定的 key 列进行排序，且根据不同的 `key_type` 具有不同特性。
`key_type` 支持以下类型：

* AGGREGATE KEY: key 列相同的记录，value 列按照指定的聚合类型进行聚合，适合报表、多维分析等业务场景。
* UNIQUE KEY/PRIMARY KEY: key 列相同的记录，value 列按导入顺序进行覆盖，适合按 key 列进行增删改查的点查询 (point query) 业务。
* DUPLICATE KEY: key 列相同的记录，同时存在于 StarRocks 中，适合存储明细数据或者数据无聚合特性的业务场景。

默认为 DUPLICATE KEY，数据按 key 列做排序。

除 AGGREGATE KEY 外，其他 `key_type` 在建表时，`value` 列不需要指定聚合类型 (agg_type)。

### COMMENT

表的注释，可选。注意建表时 COMMENT 必须在 `key_desc` 之后，否则建表失败。

如果后续想修改表的注释，可以使用 `ALTER TABLE <table_name> COMMENT = "new table comment"`（3.1 版本开始支持）。

### **partition_desc**

支持三种分区方式，[表达式分区](../../../table_design/expression_partitioning.md)（推荐）、[Range 分区](../../../table_design/Data_distribution.md#range-分区) 和 [List 分区](../../../table_design/list_partitioning.md)。

使用 Range 分区时，提供三种创建方式，其语法、说明和示例如下：

* **动态创建分区**

    [动态分区](../../../table_design/dynamic_partitioning.md)提供了分区生命周期管理（TTL）。StarRocks 会自动提前创建新的分区，并删除过期的分区，以确保数据时效性。要启用这个功能，您可以在创建表时配置与动态分区相关的属性。

* **手动创建分区**

  * 仅指定各个分区的上界

    语法：

    ```sql
    PARTITION BY RANGE ( <partitioning_column1> [, <partitioning_column2>, ... ] )
    PARTITION <partition1_name> VALUES LESS THAN ("<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] )
    [ ,
    PARTITION <partition2_name> VALUES LESS THAN ("<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] )
    , ... ] 
    )
    ```

    说明：

    使用指定的 key 列和指定的数值范围进行分区。

    * 分区名称仅支持字母开头，由字母、数字和下划线组成。
    * 仅支持以下类型的列作为 Range 分区列：`TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, DATETIME`。
    * 分区为左闭右开区间，首个分区的左边界为最小值。
    * NULL 值只会存放在包含 **最小值** 的分区中。当包含最小值的分区被删除后，NULL 值将无法导入。
    * 可以指定一列或多列作为分区列。如果分区值缺省，则会默认填充最小值。
    * 当只指定一个列作为分区列时，您可以设置最后一个分区的分区列的上界为 MAXVALUE。

    注意：

    1. 分区一般用于时间维度的数据管理。
    2. 有数据回溯需求的，可以考虑首个分区为空分区，以便后续增加分区。

    示例：

    1. 分区列 `pay_dt` 为 DATE 类型，并且按天分区。

        ```sql
        PARTITION BY RANGE(pay_dt)
        (
            PARTITION p1 VALUES LESS THAN ("2021-01-02"),
            PARTITION p2 VALUES LESS THAN ("2021-01-03"),
            PARTITION p3 VALUES LESS THAN ("2021-01-04")
        )
        ```

    2. 分区列 `pay_dt` 为 INT 类型，并且按天分区。

        ```sql
        PARTITION BY RANGE(pay_dt)
        (
            PARTITION p1 VALUES LESS THAN ("20210102"),
            PARTITION p2 VALUES LESS THAN ("20210103"),
            PARTITION p3 VALUES LESS THAN ("20210104")
        )
        ```

    3. 分区列 `pay_dt` 为 INT 类型，并且按天分区，最后一个分区没有上界。

        ```sql
        PARTITION BY RANGE(pay_dt)
        (
            PARTITION p1 VALUES LESS THAN ("20210102"),
            PARTITION p2 VALUES LESS THAN ("20210103"),
            PARTITION p3 VALUES LESS THAN MAXVALUE
        )
        ```

  * 指定各个分区的上界和下界

    语法：

    ```sql
    PARTITION BY RANGE ( <partitioning_column1> [, <partitioning_column2>, ... ] )
    (
        PARTITION <partition_name1> VALUES [( "<lower_bound_for_partitioning_column1>" [ , "<lower_bound_for_partitioning_column2>", ... ] ), ( "<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] ) ) 
        [,
        PARTITION <partition_name2> VALUES [( "<lower_bound_for_partitioning_column1>" [ , "<lower_bound_for_partitioning_column2>", ... ] ), ( "<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] ) ) 
        , ...]
    )
    ```

    说明：

    * 与仅指定分区下界相比，指定各个分区的上界和下界相对灵活，并且您可以自定义左右区间。
    * 其他与 LESS THAN 保持同步。
    * 当只指定一个列作为分区列时，您可以设置最后一个分区的分区列的上界为 MAXVALUE。

    示例：

    1. 分区列 `pay_dt` 为 DATE 类型，并且按月分区。

        ```sql
        PARTITION BY RANGE (pay_dt)
        (
            PARTITION p202101 VALUES [("2021-01-01"), ("2021-02-01")),
            PARTITION p202102 VALUES [("2021-02-01"), ("2021-03-01")),
            PARTITION p202103 VALUES [("2021-03-01"), ("2021-04-01"))
        )
        ```

    2. 分区列 `pay_dt` 为 INT 类型，并且按月分区。

        ```sql
        PARTITION BY RANGE (pay_dt)
        (
            PARTITION p202101 VALUES [("20210101"), ("20210201")),
            PARTITION p202102 VALUES [("20210201"), ("20210301")),
            PARTITION p202103 VALUES [("20210301"), ("20210401"))
        )
        ```

    3. 分区列 `pay_dt` 为 INT 类型，并且按月分区，最后一个分区没有上界。

        ```sql
        PARTITION BY RANGE (pay_dt)
        (
            PARTITION p202101 VALUES [("20210101"), ("20210201")),
            PARTITION p202102 VALUES [("20210201"), ("20210301")),
            PARTITION p202103 VALUES [("20210301"), (MAXVALUE))
        )

* **批量创建分区**

  语法：

  * 如果分区列为时间类型

    ```sql
    PARTITION BY RANGE (<partitioning_column>) (
        START ("<start_date>") END ("<end_date>") EVERY (INTERVAL <N> <time_unit>)
    )
    ```

    * 如果分区列为整数类型

    ```sql
    PARTITION BY RANGE (<partitioning_column>) (
        START ("<start_integer>") END ("<end_integer>") EVERY (<partitioning_granularity>)
    )
    ```

    说明：
    用户可以通过给出一个 START 值、一个 END 值以及一个定义分区增量值的 EVERY 子句批量产生分区。

    * 当前分区列仅支持日期类型和整数类型。
    * 当分区列为日期类型时，需要指定 `INTERVAL` 关键字来表示日期间隔。目前日期间隔支持 hour (v3.0）、day、week、month、year，分区的命名规则同动态分区一样。
    * 当分区列为整数类型时，START 值、END 值仍需要用双引号包裹。
    * 仅支持指定一列作为分区列。

    更多信息，请参见[批量创建分区](/table_design/Data_distribution.md#range-分区)。

    示例：

    1. 分区列 `pay_dt` 为 DATE 类型，并且按年分区。

        ```sql
        PARTITION BY RANGE (pay_dt) (
            START ("2018-01-01") END ("2023-01-01") EVERY (INTERVAL 1 YEAR)
        )
        ```

    2. 分区列 `pay_dt` 为 INT 类型，并且按年分区。

        ```sql
        PARTITION BY RANGE (pay_dt) (
            START ("2018") END ("2023") EVERY (1)
        )
        ```

### **distribution_desc**

支持随机分桶（Random bucketing）和哈希分桶（Hash bucketing）。如果不指定分桶信息，则 StarRocks 默认使用随机分桶且自动设置分桶数量。

* 随机分桶（自 v3.1）

  对每个分区的数据，StarRocks 将数据随机地分布在所有分桶中，而不受到特定列值的影响。并且如果选择由系统设置分桶数量，则您无需设置分桶信息。如果选择手动指定分桶数量，则语法如下：

  ```SQL
  DISTRIBUTED BY RANDOM BUCKETS <num>
  ```

  不过值得注意的是，如果查询海量数据且查询时经常使用一些列会作为条件列，随机分桶提供的查询性能可能不够理想。在该场景下建议您使用哈希分桶，当查询时经常使用这些列作为条件列时，只需要扫描和计算查询命中的少量分桶，则可以显著提高查询性能。

  **注意事项**

  * 不支持主键模型表、更新模型表和聚合表。
  * 不支持指定 [Colocation Group](../../../using_starrocks/Colocate_join.md)。
  * 不支持 [Spark Load](../../../loading/SparkLoad.md)。
  * 自 2.5.7 版本起，建表时**无需手动指定分桶数量**，StarRocks 自动设置分桶数量。如果您需要手动设置分桶数量，请参见[确定分桶数量](../../../table_design/Data_distribution.md#确定分桶数量)。

  更多随机分桶的信息，请参见[随机分桶](../../../table_design/Data_distribution.md#随机分桶自-v31)。

* 哈希分桶

  语法：

  ```SQL
  DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
  ```

  对每个分区的数据，StarRocks 会根据分桶键和分桶数量进行哈希分桶。

  对于分桶键的选择，如果列是高基数且经常作为查询条件，则优先选择其为分桶键，进行哈希分桶。
  如果不存在同时满足两个条件的列，则需要根据查询进行判断。
  * 如果查询比较复杂，则建议选择高基数的列为分桶键，保证数据在各个分桶中尽量均衡，提高集群资源利用率。
  * 如果查询比较简单，则建议选择经常作为查询条件的列为分桶键，提高查询效率。
  并且，如果数据倾斜情况严重，您还可以使用多个列作为数据的分桶键，但是建议不超过 3 个列。
  更多选择分桶键的信息，请参见[选择分桶键](../../../table_design/Data_distribution.md#哈希分桶).

  **注意事项**

  * **建表时，必须指定分桶键**。
  * 作为分桶键的列，该列的值不支持更新。
  * 分桶键指定后不支持修改。
  * 自 2.5.7 版本起，建表时**无需手动指定分桶数量**，StarRocks 自动设置分桶数量。如果您需要手动设置分桶数量，请参见[确定分桶数量](../../../table_design/Data_distribution.md#确定分桶数量)。

### **ORDER BY**

自 3.0 版本起，主键模型解耦了主键和排序键，排序键通过 `ORDER BY` 指定，可以为任意列的排列组合。
> **注意**
>
> 如果指定了排序键，就根据排序键构建前缀索引；如果没指定排序键，就根据主键构建前缀索引。

### **PROPERTIES**

#### 设置数据的初始存储介质、自动降冷时间和副本数

如果 ENGINE 类型为 OLAP，可以在属性 `properties` 中设置该表数据的初始存储介质（storage_medium）、自动降冷时间（storage_cooldown_time）或者时间间隔（storage_cooldown_ttl）和副本数（replication_num）。

属性生效范围：当表为单分区表时，以上属性为表的属性。当表划分成多个分区时，以上属性属于每一个分区。并且如果希望不同分区有不同属性，则建表后可以执行 [ALTER TABLE ... ADD PARTITION 或 ALTER TABLE ... MODIFY PARTITION](../data-definition/ALTER_TABLE.md)。

**设置数据的初始存储介质、自动降冷时间**

```sql
PROPERTIES (
    "storage_medium" = "[SSD|HDD]",
    { "storage_cooldown_ttl" = "<num> { YEAR | MONTH | DAY | HOUR } "
    | "storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss" }
)
```

* `storage_medium`：数据初始存储介质，取值为 `SSD` 或 `HDD`。显式指定该参数时，请确保该值与 BE 静态参数 `storage_root_path` 中指定的集群存储介质相匹配。
  
  当 FE 配置项 `enable_strict_storage_medium_check` 为 `true` 时，表示在建表时会严格校验 BE 上的存储介质。如果建表语句中的存储介质和 BE 的存储类型不一致，建表语句会报错 `Failed to find enough hosts with storage medium [SSD|HDD] at all backends...`。当 `enable_strict_storage_medium_check` 为 `false` 时，可以忽略该报错强行建表，但是后续可能会导致集群磁盘空间分布出现不均衡。

  从 2.3.6，2.4.2，2.5.1，3.0 及以上版本开始，支持在未显式指定该参数的情况下，由系统自动推导存储介质。

  * 在以下场景中，系统推导该参数为 SSD：
    * BE 上报的存储路径 (`storage_root_path`) 都是 SSD。
    * BE 上报的存储路径包含 SSD 和 HDD。并且从 2.3.10，2.4.5，2.5.4，3.0 及以上版本开始，如果 BE 上报的存储路径两者都有且 FE 配置文件中设置了 `storage_cooldown_second`。
  * 在以下场景中，系统推导该参数为 HDD：
    * BE 上报的存储路径都是 HDD。
    * 从 2.3.10，2.4.5，2.5.4，3.0 及以上版本开始，如果 BE 上报的存储路径两者都有且 FE 配置文件中未设置 `storage_cooldown_second`。

* `storage_cooldown_ttl` 或 `storage_cooldown_time`：数据自动降冷的时间间隔或者时间点。数据自动降冷，即数据自动从 SSD 介质迁移到 HDD 介质。只在数据初始存储介质为 SSD 时生效。

  **参数说明**

  * `storage_cooldown_ttl`：该表分区自动降冷**时间间隔**。如果您需要保留最近几个分区在 SSD，其它较早的分区经过一定时间间隔自动降冷至 HDD，则您可以使用该参数，各个分区的自动降冷时间点为该参数值 + 该分区的时间上界。
  
      取值为 `<num> YEAR`，`<num> MONTH`，`<num> DAY` 或 `<num> HOUR`。`<num>` 为非负整数。默认值为空，表示该表分区不进行自动降冷。
  
      例如建表时指定 `"storage_cooldown_ttl"="1 DAY"`，建表后存在分区 `p20230801` ，其范围为 `[2023-08-01 00:00:00,2023-08-02 00:00:00)`，则该分区的自动降冷时间点是 `2023-08-03 00:00:00`，即 `2023-08-02 00:00:00 + 1 DAY`。如果建表时指定 `"storage_cooldown_ttl"="0 DAY"`，则该分区自动降冷时间点是 `2023-08-02 00:00:00`。

  * `storage_cooldown_time`：该表自动降冷**时间点**（绝对时间）。数据在该时间点之后数据从 SSD 自动降冷到 HDD，设置的时间必须大于当前时间。取值格式为："yyyy-MM-dd HH:mm:ss"。如果需要不同分区具有不同自动降冷时间点，则需要执行 [ALTER TABLE ... ADD PARTITION 或 ALTER TABLE ... MODIFY PARTITION](../data-definition/ALTER_TABLE.md) 手动指定。

  **使用说明**

  * 目前 StarRocks 提供如下数据自动降冷的相关参数，对比如下：
    * `storage_cooldown_ttl`：表的属性，指定该表中分区自动降冷时间间隔，由系统自动降冷表中到达时间点（时间间隔+分区时间上界）的分区。并且表按照分区粒度自动降冷，更加灵活。
    * `storage_cooldown_time`：表的属性，指定该表的自动降冷时间点（绝对时间）。建表后也可以为不同分区配置不同时间点。
    * `storage_cooldown_second`：FE 静态参数，指定集群范围内所有表的自动降冷时延。
  * 表属性 `storage_cooldown_ttl` 或 `storage_cooldown_time` 比 FE 静态参数 `storage_cooldown_second` 优先级高。
  * 配置以上参数时，必须指定 `"storage_medium = "SSD"`。
  * 不配置以上参数时，则不进行自动降冷。
  * 执行 `SHOW PARTITIONS FROM <table_name>` 查看各个分区的自动降冷时间点。

  **限制**
  * 不支持表达式分区和 List 分区。
  * 不支持分区列为非日期类型。
  * 不支持多个分区列。
  * 不支持主键模型表。

**设置分区 Tablet 副本数**

`replication_num`：分区 Tablet 副本数。默认为 3。

```sql
PROPERTIES (
    "replication_num" = "<num>"
)
```

#### 创建表时为列添加 bloom filter 索引

如果 Engine 类型为 olap, 可以指定某列使用 bloom filter 索引。bloom filter 索引使用时有如下限制：

* 主键模型和明细模型中所有列都可以创建 Bloom filter 索引；聚合模型和更新模型中，只有维度列（即 Key 列）支持创建 Bloom filter 索引。
* 不支持为 TINYINT、FLOAT、DOUBLE 和 DECIMAL 类型的列创建 Bloom filter 索引。
* Bloom filter 索引只能提高查询条件为 `in` 和 `=` 的查询效率，值越分散效果越好。

更多信息，参见 [Bloom filter 索引](../../../using_starrocks/Bloomfilter_index.md)。

```sql
PROPERTIES (
    "bloom_filter_columns" = "k1, k2, k3"
)
```

#### 添加属性支持 Colocate Join

如果希望使用 Colocate Join 特性，需要在 properties 中指定:

``` sql
PROPERTIES (
    "colocate_with" = "table1"
)
```

详细的 Colocate Join 使用方法及应用场景请参考 [Colocate Join](/using_starrocks/Colocate_join.md) 章节。

#### 设置动态分区

如果希望使用动态分区特性，需要在 properties 中指定如下参数：

``` sql
PROPERTIES (
    "dynamic_partition.enable" = "true|false",
    "dynamic_partition.time_unit" = "DAY|WEEK|MONTH",
    "dynamic_partition.start" = "${integer_value}",
    "dynamic_partition.end" = "${integer_value}",
    "dynamic_partition.prefix" = "${string_value}",
    "dynamic_partition.buckets" = "${integer_value}"
)
```

| 参数                          | 是否必填 | 说明                                                         |
| ----------------------------- | -------- | ------------------------------------------------------------ |
| `dynamic_partition.enable`    | 否       | 开启动态分区特性，取值为 `TRUE`（默认）或 `FALSE`。       |
| `dynamic_partition.time_unit` | 是       | 动态分区的时间粒度，取值为 `DAY`、`WEEK` 或 `MONTH`。时间粒度会决定动态创建的分区名后缀格式。  <br />取值为 `DAY` 时，动态创建的分区名后缀格式为 yyyyMMdd，例如 `20200321`。<br />取值为 `WEEK` 时，动态创建的分区名后缀格式为 `yyyy_ww`，例如 `2020_13` 代表 2020 年第 13 周。<br />取值为 `MONTH` 时，动态创建的分区名后缀格式为 yyyyMM，例如 `202003`。 |
| `dynamic_partition.start`：   | 否       | 保留的动态分区的起始偏移，取值范围为负整数。根据 `dynamic_partition.time_unit` 属性的不同，以当天（周/月）为基准，分区范围在此偏移之前的分区将会被删除。比如设置为`-3`，并且`dynamic_partition.time_unit`为`day`，则表示 3 天前的分区会被删掉。<br />如果不填写，则默认为 `Integer.MIN_VALUE`，即 `-2147483648`，表示不删除历史分区。 |
| `dynamic_partition.end`       | 是       | 提前创建的分区数量，取值范围为正整数。根据 `dynamic_partition.time_unit` 属性的不同，以当天（周/月）为基准，提前创建对应范围的分区。 |
| `dynamic_partition.prefix`    | 否       | 动态分区的前缀名，默认值为 `p`。                             |
| dynamic_partition.buckets     | 否       | 动态分区的分桶数量。默认与 BUCKETS 保留字指定的分桶数量、或者 StarRocks 自动设置的分桶数量保持一致。 |

#### 设置数据压缩算法

您可以在建表时通过增加属性 `compression` 为该表指定数据压缩算法。

`compression` 有效值包括：

* `LZ4`：LZ4 算法。
* `ZSTD`：Zstandard 算法。
* `ZLIB`：zlib 算法。
* `SNAPPY`：Snappy 算法。

如不指定数据压缩算法，StarRocks 默认使用 LZ4。

关于如何选择合适的数据压缩算法，请参阅[数据压缩](../../../table_design/data_compression.md)。

#### 设置数据导入安全等级

如果您的 StarRocks 集群有多数据副本，可以在建表时在 `PROPERTIES` 中设置 `write_quorum` 参数来指定数据导入安全等级，即设置需要多少数据副本导入成功后 StarRocks 可返回导入成功。该属性从 2.5 版本开始支持。

`write_quorum` 的取值及其对应描述如下：

* `MAJORITY`：默认值。当**多数**数据副本导入成功时，StarRocks 返回导入成功，否则返回失败。
* `ONE`：当**一个**数据副本导入成功时，StarRocks 返回导入成功，否则返回失败。
* `ALL`：当**所有**数据副本导入成功时，StarRocks 返回导入成功，否则返回失败。

> **注意**
>
> * 设置较低的导入数据安全等级会增加数据不可访问甚至丢失的风险。例如，在 StarRocks 集群有两个数据副本的情况下设置 `write_quorum` 为 `ONE`，如果某个 Tablet 实际只成功导入了一个副本，而此副本所在机器后续下线，则会导致该 Tablet 中的数据因为没有存活副本而无法访问。如果服务器磁盘受损，则会导致该 Tablet 中的数据丢失。
> * 仅当所有数据副本返回导入状态后，StarRocks 才会返回导入任务状态。当有副本导入状态未知时，StarRocks 不会返回导入任务状态。导入超时的副本亦会被标记为失败。

#### 指定数据在多副本间的写入和同步方式

如果您的 StarRocks 集群有多数据副本，可以在建表时在 `PROPERTIES` 中设置 `replicated_storage` 参数来指定数据在多副本间的写入和同步方式。

* 设置为 `true`（3.0 及后续版本的默认值）表示 single leader replication，即数据只写入到主副本 (primary replica)，由主副本同步数据到从副本 (secondary replica)。该模式能有效降低多副本写入带来的 CPU 成本。该模式从 2.5 版本开始支持。
* 设置为 `false`（2.5 版本的默认值）表示 leaderless replication，即数据直接写入到多个副本，不区分主从副本。该模式 CPU 成本比较高。

默认配置在绝大部分场景下能获得更好的写入性能，如果要修改已有表的多副本写入和同步方式，可执行 ALTER TABLE 命令，举例：

```sql
ALTER TABLE example_db.my_table
SET ("replicated_storage" = "false");
```

#### 建表时批量创建多个 Rollup

语法：

``` sql
ROLLUP (rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key" = "value", ...)],...)
```

#### 为 View Delta Join 查询改写定义 Unique Key 和外键约束

要在 View Delta Join 场景中启用查询重写，您必须为 Delta Join 中的表定义 Unique Key 约束 `foreign_key_constraints` 和外键约束 `foreign_key_constraints`。详细信息，请参阅 [异步物化视图 - 基于 View Delta Join 场景改写查询](../../../using_starrocks/query_rewrite_with_materialized_views.md#view-delta-join-改写)。

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

* `child_column`：当前表中的外键列。 您可以定义多个 `child_column`。
* `catalog_name`：待 Join 表所在的数据目录名。未指定此参数时使用默认目录。
* `database_name`：待 Join 表所在的数据库名。未指定此参数时使用当前数据库。
* `parent_table_name`：待 Join 表名。
* `parent_column`：待 Join 列名，必须为相应表的 Primary Key 或 Unique Key。

> **注意**
>
> * Unique Key 约束和外键约束仅用于查询重写。导入数据时，不保证进行外键约束校验。您必须确保导入的数据满足约束条件。
> * 主键模型表的 Primary Key 或更新模型表的 Unique Key 默认是其 `unique_constraints`，您无需手动设置。
> * `foreign_key_constraints` 中的 `child_column` 必须对应另一个表的 `unique_constraints` 中的 `unique_key`。
> * `child_column` 和 `parent_column` 的数量必须一致。
> * `child_column` 和对应的 `parent_column` 的数据类型必须匹配。

#### 为 StarRocks 存算分离集群创建云原生表

为了[使用 StarRocks 存算分离集群](../../../deployment/deploy_shared_data.md#使用-starrocks-存算分离集群)，您需要通过以下 PROPERTIES 创建云原生表：

```SQL
PROPERTIES (
    "datacache.enable" = "{ true | false }",
    "datacache.partition_duration" = "<string_value>",
    "enable_async_write_back" = "{ true | false }"
)
```

* `datacache.enable`：是否启用本地磁盘缓存。默认值：`true`。

  * 当该属性设置为 `true` 时，数据会同时导入对象存储（或 HDFS）和本地磁盘（作为查询加速的缓存）。
  * 当该属性设置为 `false` 时，数据仅导入到对象存储中。

  > **说明**
  >
  > 如需启用本地磁盘缓存，必须在 BE 配置项 `storage_root_path` 中指定磁盘目录。更多信息，请参见 [BE 配置项](../../../administration/Configuration.md#be-配置项)

* `datacache.partition_duration`：热数据的有效期。当启用本地磁盘缓存时，所有数据都会导入至本地磁盘缓存中。当缓存满时，StarRocks 会从缓存中删除最近较少使用（Less recently used）的数据。当有查询需要扫描已删除的数据时，StarRocks 会检查该数据是否在有效期内。如果数据在有效期内，StarRocks 会再次将数据导入至缓存中。如果数据不在有效期内，StarRocks 不会将其导入至缓存中。该属性为字符串，您可以使用以下单位指定：`YEAR`、`MONTH`、`DAY` 和 `HOUR`，例如，`7 DAY` 和 `12 HOUR`。如果不指定，StarRocks 将所有数据都作为热数据进行缓存。

  > **说明**
  >
  > 仅当 `datacache.enable` 设置为 `true` 时，此属性可用。

* `enable_async_write_back`：是否允许数据异步写入对象存储。默认值：`false`。

  * 当该属性设置为 `true` 时，导入任务在数据写入本地磁盘缓存后立即返回成功，数据将异步写入对象存储。允许数据异步写入可以提升导入性能，但如果系统发生故障，可能会存在一定的数据可靠性风险。
  * 当该属性设置为 `false` 时，只有在数据同时写入对象存储和本地磁盘缓存后，导入任务才会返回成功。禁用数据异步写入保证了更高的可用性，但会导致较低的导入性能。

## 示例

### 创建 Hash 分桶表并根据 key 列对数据进行聚合

创建一个 olap 表，使用 Hash 分桶，使用列存，相同 key 的记录进行聚合。

``` sql
CREATE TABLE example_db.table_hash
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type" = "column");
```

> **注意**
>
> 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](../../../table_design/Data_distribution.md#确定分桶数量)。

### 创建表并设置存储介质和数据自动降冷时间

创建一个 olap 表，使用 Hash 分桶，使用列存，相同 key 的记录进行覆盖，设置初始存储介质和冷却时间。

``` sql
CREATE TABLE example_db.table_hash
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT DEFAULT "10"
)
ENGINE = olap
UNIQUE KEY(k1, k2)
DISTRIBUTED BY HASH (k1, k2)
PROPERTIES(
    "storage_type" = "column",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2025-06-04 00:00:00"
);
```

或

``` sql
CREATE TABLE example_db.table_hash
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT DEFAULT "10"
)
ENGINE = olap
PRIMARY KEY(k1, k2)
DISTRIBUTED BY HASH (k1, k2)
PROPERTIES(
    "storage_type" = "column",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2025-06-04 00:00:00"
);
```

### 创建分区表

创建一个 olap 表，使用 Range 分区，使用 Hash 分桶，默认使用列存，相同 key 的记录同时存在，设置初始存储介质和冷却时间。

使用 LESS THAN 方式按照范围划分分区：

``` sql
CREATE TABLE example_db.table_range
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE = olap
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
    "storage_cooldown_time" = "2025-06-04 00:00:00"
);
```

说明：
这个语句会将数据划分成如下 3 个分区：

``` sql
( {    MIN     },   {"2014-01-01"} )
[ {"2014-01-01"},   {"2014-06-01"} )
[ {"2014-06-01"},   {"2014-12-01"} )
```

不在这些分区范围内的数据将视为非法数据被过滤。

使用 Fixed Range 方式创建分区：

``` sql
CREATE TABLE table_range
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE = olap
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

### 创建一个 MySQL 外表

``` sql
CREATE EXTERNAL TABLE example_db.table_mysql
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE = mysql
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "8239",
    "user" = "mysql_user",
    "password" = "mysql_passwd",
    "database" = "mysql_db_test",
    "table" = "mysql_table_test"
);
```

### 创建一张含有 HLL 列的表

``` sql
CREATE TABLE example_db.example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 HLL HLL_UNION,
    v2 HLL HLL_UNION
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type" = "column");
```

### 创建一张含有 `BITMAP_UNION` 聚合类型的表

v1 和 v2 列的原始数据类型必须是 `TINYINT, SMALLINT, INT`。

``` sql
CREATE TABLE example_db.example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 BITMAP BITMAP_UNION,
    v2 BITMAP BITMAP_UNION
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type" = "column");
```

### 创建两张支持 Colocate Join 的表

创建 t1 和 t2 两个表，两表可进行 Colocate Join。两表属性中的 `colocate_with` 属性的值需保持一致。

``` sql
CREATE TABLE `t1` (
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) ENGINE = OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "colocate_with" = "t1"
);

CREATE TABLE `t2` (
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) ENGINE = OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "colocate_with" = "t1"
);
```

### 创建一个带有 bitmap 索引的表

``` sql
CREATE TABLE example_db.table_hash
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM,
    INDEX k1_idx (k1) USING BITMAP COMMENT 'xxxxxx'
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type" = "column");
```

### 创建动态分区表

 创建动态分区表需要在 FE 配置中开启 **动态分区** 功能（`"dynamic_partition.enable" = "true"`），参数可参见本文[设置动态分区](#设置动态分区)。

 该表每天提前创建 3 天的分区，并删除 3 天前的分区。例如今天为 `2020-01-08`，则会创建分区名为 `p20200108`，`p20200109`，`p20200110`，`p20200111` 的分区. 分区范围分别为:

```plain text
[types: [DATE]; keys: [2020-01-08]; ‥types: [DATE]; keys: [2020-01-09]; )
[types: [DATE]; keys: [2020-01-09]; ‥types: [DATE]; keys: [2020-01-10]; )
[types: [DATE]; keys: [2020-01-10]; ‥types: [DATE]; keys: [2020-01-11]; )
[types: [DATE]; keys: [2020-01-11]; ‥types: [DATE]; keys: [2020-01-12]; )
```

```sql
CREATE TABLE example_db.dynamic_partition
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE = olap
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
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
);
```

### 创建一个 Hive 外部表

``` SQL
CREATE EXTERNAL TABLE example_db.table_hive
(
    k1 TINYINT,
    k2 VARCHAR(50),
    v INT
)
ENGINE = hive
PROPERTIES
(
    "resource" = "hive0",
    "database" = "hive_db_name",
    "table" = "hive_table_name"
);
```

### 创建一张主键模型的表并且指定排序键

假设需要按地域、最近活跃时间实时分析用户情况，则可以将表示用户 ID 的 `user_id` 列作为主键，表示地域的 `address` 列和表示最近活跃时间的 `last_active` 列作为排序键。建表语句如下：

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

## References

* [SHOW CREATE TABLE](../data-manipulation/SHOW_CREATE_TABLE.md)
* [SHOW TABLES](../data-manipulation/SHOW_TABLES.md)
* [USE](USE.md)
* [ALTER TABLE](ALTER_TABLE.md)
* [DROP TABLE](DROP_TABLE.md)
