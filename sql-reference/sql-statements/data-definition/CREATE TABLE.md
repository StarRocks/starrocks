# CREATE TABLE

## 功能

该语句用于创建表。

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
[PROPERTIES ("key"="value", ...)]
[BROKER PROPERTIES ("key"="value", ...)]
```

## 参数说明

在指定数据库名、表名和列名等变量时，如果使用了保留关键字，必须使用反引号 (`) 包裹，否则可能会产生报错。有关 StarRocks 的保留关键字列表，请参见[关键字](../keywords.md#保留关键字)。

### **column_definition**

语法：

```sql
col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
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

**NULL | NOT NULL**：是否允许为 NULL。默认为 NULL，PRIMARY KEY 的 key 列默认为 NOT NULL。NULL 值在导入数据中用 \N 来表示。

### **index_definition**

语法：

```sql
INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] [COMMENT '']
```

建表时仅支持创建 bitmap 索引，语法如下。有关参数说明和使用限制，请参见 [Bitmap 索引](/using_starrocks/Bitmap_index.md#创建索引)。

### **ENGINE 类型**

默认为 `olap`，表示创建的是 StarRocks 内部表。

可选值：`mysql`，`elasticsearch`，`hive`，`jdbc`(2.3 及以后)，`iceberg`，`hudi`（2.2 及以后）。如果指定了可选值，则创建的是对应类型的外部表 (external table)，在建表时需要使用 CREATE EXTERNAL TABLE。更多信息，参见[外部表](../../../data_source/External_table.md)。

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

### **partition_desc**

`partition_desc` 有三种使用方式，分别为：`LESS THAN，Fixed Range，批量创建分区`。以下为不同方式的详细使用方法：

**LESS THAN**

语法：

```sql
PARTITION BY RANGE (k1, k2, ...)
(
    PARTITION partition_name1 VALUES LESS THAN MAXVALUE|("value1", "value2", ...),
    PARTITION partition_name2 VALUES LESS THAN MAXVALUE|("value1", "value2", ...)
    ...
)
```

说明：
使用指定的 key 列和指定的数值范围进行分区。

1. 分区名称仅支持字母开头，由字母、数字和下划线组成。
2. 仅支持以下类型的列作为 Range 分区列：`TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, DATETIME`。
3. 分区为左闭右开区间，首个分区的左边界为最小值。
4. NULL 值只会存放在包含 **最小值** 的分区中。当包含最小值的分区被删除后，NULL 值将无法导入。
5. 可以指定一列或多列作为分区列。如果分区值缺省，则会默认填充最小值。
6. 当分区列为单列时，才支持使用 MAXVALUE。

注意：

1. 分区一般用于时间维度的数据管理。
2. 有数据回溯需求的，可以考虑首个分区为空分区，以便后续增加分区。

示例：

1. 数据类型为 DATE 的列 `pay_dt` 为分区列，并且按天分区。

    ```sql
    PARTITION BY RANGE(pay_dt)
    (
        PARTITION p1 VALUES LESS THAN ("2021-01-02"),
        PARTITION p2 VALUES LESS THAN ("2021-01-03"),
        PARTITION p3 VALUES LESS THAN ("2021-01-04")
    )
    ```

2. 数据类型为 INT 的列 `pay_dt` 为分区列，并且按天分区。

    ```sql
    PARTITION BY RANGE(pay_dt)
    (
        PARTITION p1 VALUES LESS THAN ("20210102"),
        PARTITION p2 VALUES LESS THAN ("20210103"),
        PARTITION p3 VALUES LESS THAN ("20210104")
    )
    ```

3. 数据类型为 INT 的列 `pay_dt` 为分区列，并且按天分区，最后一个分区没有上界。

    ```sql
    PARTITION BY RANGE(pay_dt)
    (
        PARTITION p1 VALUES LESS THAN ("20210102"),
        PARTITION p2 VALUES LESS THAN ("20210103"),
        PARTITION p3 VALUES LESS THAN MAXVALUE
    )
    ```

**Fixed Range**

语法：

```sql
PARTITION BY RANGE (k1, k2, k3, ...)
(
    PARTITION partition_name1 VALUES [("k1-lower1", "k2-lower1", "k3-lower1",...), ("k1-upper1", "k2-upper1", "k3-upper1", ...)),
    PARTITION partition_name2 VALUES [("k1-lower1-2", "k2-lower1-2", ...), ("k1-upper1-2", "k2-upper1-2", "k3-upper1-2", ...))
    ...
)
```

说明：

1. Fixed Range 比 LESS THAN 相对灵活些，左右区间完全由用户自己确定。
2. 其他与 LESS THAN 保持同步。
3. 当分区列为单列时，才支持使用 MAXVALUE。

示例：

1. 数据类型为 DATE 的列 `pay_dt` 为分区列，并且按月分区。

    ```sql
    PARTITION BY RANGE (pay_dt)
    (
        PARTITION p202101 VALUES [("2021-01-01"), ("2021-02-01")),
        PARTITION p202102 VALUES [("2021-02-01"), ("2021-03-01")),
        PARTITION p202103 VALUES [("2021-03-01"), ("2021-04-01"))
    )
    ```

2. 数据类型为 INT 的列 `pay_dt` 为分区列，并且按月分区。

    ```sql
    PARTITION BY RANGE (pay_dt)
    (
        PARTITION p202101 VALUES [("20210101"), ("20210201")),
        PARTITION p202102 VALUES [("20210201"), ("20210301")),
        PARTITION p202103 VALUES [("20210301"), ("20210401"))
    )
    ```

3. 数据类型为 INT 的列 `pay_dt` 为分区列，并且按月分区，最后一个分区没有上界。

    ```sql
    PARTITION BY RANGE (pay_dt)
    (
        PARTITION p202101 VALUES [("20210101"), ("20210201")),
        PARTITION p202102 VALUES [("20210201"), ("20210301")),
        PARTITION p202103 VALUES [("20210301"), (MAXVALUE))
    )

**批量创建分区**

语法：

```sql
PARTITION BY RANGE (pay_dt) (
    START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 day)
)
```

说明：
用户可以通过给出一个 START 值、一个 END 值以及一个定义分区增量值的 EVERY 子句批量产生分区。

1. 当前分区列仅支持 **日期类型** 和 **整数类型**，分区类型需要与 EVERY 里的表达式匹配。
2. 当分区列为日期类型的时候需要指定 `INTERVAL` 关键字来表示日期间隔，目前日期仅支持 `day、week、month、year`，分区的命名规则同动态分区一样。
3. 当分区列的数据类型为 INT 时，START 值、END 值仍需要用双引号包裹。
4. 仅支持指定一列作为分区列。
5. 更详细的语法规则，请参见[批量创建分区](/table_design/Data_distribution.md#批量创建分区)。

示例：

1. 数据类型为 DATE 的列 `pay_dt` 为分区列，并且按年分区。

    ```sql
    PARTITION BY RANGE (pay_dt) (
        START ("2018-01-01") END ("2023-01-01") EVERY (INTERVAL 1 YEAR)
    )
    ```

2. 数据类型为 INT 的列 `pay_dt` 为分区列，并且按年分区。

    ```sql
    PARTITION BY RANGE (pay_dt) (
        START ("2018") END ("2023") EVERY (1)
    )
    ```

### **distribution_desc**

Hash 分桶

语法：

```sql
DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
```

说明：
使用指定的 key 列进行哈希分桶。默认分桶数为 10。`DISTRIBUTED BY` 为必填字段。有关如何确定分桶数量，请参见[确定分桶数量](/table_design/Data_distribution.md#确定分桶数量)。建议使用 Hash 分桶方式。

### **PROPERTIES**

#### 设置数据的初始存储介质、存储降冷时间和副本数

如果 ENGINE 类型为 olap, 可以在 `properties` 设置该表数据的初始存储介质 (storage_medium)、存储降冷时间 (storage_cooldown_time) 和副本数 (replication_num)。

> 注意
>
只有 "storage_medium" = "SSD" 时才可设置降冷时间，创建 SSD 属性的表需要您的集群中配置了 SSD 类型的存储路径（`storage_root_path`）。

```sql
PROPERTIES (
    "storage_medium" = "[SSD|HDD]",
    [ "storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss", ]
    [ "replication_num" = "3" ]
)
```

**storage_medium**：用于指定该分区的初始存储介质，可选择 SSD 或 HDD。

> **说明**
>
> * 从 2.4.2 版本开始，支持在未显式指定该参数的情况下，由系统自动推导存储介质。如果您在建表时未显式指定该参数，系统会根据 BE 的磁盘类型来自动推导并设定表的存储类型。推导机制如下：如果 BE 上报的存储路径 (`storage_root_path`) 都是 SSD，系统会默认该参数为 SSD；如果 BE 上报的存储路径都是 HDD，系统会默认该参数为 HDD；如果 BE 上报的存储路径两者都有，系统会默认该参数为 SSD。<!-- 从 2.4.5 版本开始，如果 BE 上报的存储路径两者都有且 FE 配置文件中设置了 `storage_cooldown_second`，系统会默认该参数为 SSD，如果未设置 `storage_cooldown_second`，系统默认为 HDD。-->
> * 当 FE 配置项 `enable_strict_storage_medium_check` 为 `true` 时，表示在建表时会严格校验 BE 上的存储介质。如果建表语句中的存储介质和 BE 的存储类型不一致，建表语句会报错 `Failed to find enough hosts with storage medium [SSD|HDD] at all backends...`。当 `enable_strict_storage_medium_check` 为 `false` 时，可以忽略该报错强行建表，但是后续可能会导致集群磁盘空间分布出现不均衡，所以强烈建议在建表时指定和集群存储介质相匹配的 `storage_medium` 属性。

**storage_cooldown_time**：当设置存储介质为 SSD 时，指定该分区在该时间点之后从 SSD 降冷到 HDD，设置的时间必须大于当前时间。

* 不显式设置该属性时，默认不进行自动降冷。
* 取值格式为："yyyy-MM-dd HH:mm:ss"

**replication_num**：指定分区的副本数。默认为 3。

说明：

* 当表为单分区表时，以上属性为表的属性。
* 当表为两级分区时，以上属性属于每一个分区。
* 如果希望不同分区有不同属性，可以通过 ADD PARTITION 或 MODIFY PARTITION 命令进行操作，具体参见[ALTER TABLE](../data-definition/ALTER%20TABLE.md)。

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

dynamic_partition.enable: 用于指定表级别的动态分区功能是否开启。默认为 true。

dynamic_partition.time_unit: 用于指定动态添加分区的时间单位，可选择为 DAY（天），WEEK(周)，MONTH（月）。

dynamic_partition.start: 用于指定向前删除多少个分区。值必须小于 0。默认为 Integer.MIN_VALUE。

dynamic_partition.end: 用于指定提前创建的分区数量。值必须大于 0。

dynamic_partition.prefix: 用于指定创建的分区名前缀，例如分区名前缀为 p，则自动创建分区名为 p20200108。

dynamic_partition.buckets: 用于指定自动创建的分区分桶数量。

#### 建表时批量创建多个 Rollup

语法：

``` sql
ROLLUP (rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key" = "value", ...)],...)
```

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
DISTRIBUTED BY HASH(k1) BUCKETS 10
PROPERTIES ("storage_type" = "column");
```

### 创建表并设置存储介质和数据降冷时间

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
DISTRIBUTED BY HASH (k1, k2) BUCKETS 10
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
DISTRIBUTED BY HASH (k1, k2) BUCKETS 10
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
DISTRIBUTED BY HASH(k2) BUCKETS 10
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
DISTRIBUTED BY HASH(k2) BUCKETS 10
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
DISTRIBUTED BY HASH(k1) BUCKETS 10
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
DISTRIBUTED BY HASH(k1) BUCKETS 10
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
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "t1"
);

CREATE TABLE `t2` (
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) ENGINE = OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
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
DISTRIBUTED BY HASH(k1) BUCKETS 10
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
DISTRIBUTED BY HASH(k2) BUCKETS 10
PROPERTIES(
    "storage_medium" = "SSD",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10"
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

## 参考文档

* [SHOW CREATE TABLE](../data-manipulation/SHOW%20CREATE%20TABLE.md)
* [SHOW TABLES](../data-manipulation/SHOW%20TABLES.md)
* [USE](USE.md)
* [ALTER TABLE](ALTER%20TABLE.md)
* [DROP TABLE](DROP%20TABLE.md)
