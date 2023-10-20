# CREATE TABLE

## Description

Create a new table in StarRocks.

## Syntax

```Plain_Text
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...]
[, index_definition1[, ndex_definition12,]])
[ENGINE = [olap|mysql|elasticsearch|hive]]
[key_desc]
[COMMENT "table comment"];
[partition_desc]
[distribution_desc]
[rollup_index]
[PROPERTIES ("key"="value", ...)]
[BROKER PROPERTIES ("key"="value", ...)]
```

## Parameters

### column_definition

Syntax:

```SQL
col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
```

Note:

```Plain_Text
col_name：Column name
col_type：Column type



Specific column information, such as types and ranges, are as follows: 



* TINYINT（1 byte）

Range ：-2^7 + 1 ~ 2^7 - 1



* SMALLINT（2 bytes）

Range ：-2^15 + 1 ~ 2^15 - 1



* INT（4 bytes）

Range ：-2^31 + 1 ~ 2^31 - 1



* BIGINT（8 bytes）

Range ：-2^63 + 1 ~ 2^63 - 1



* LARGEINT（16 bytes）

Range ：-2^127 + 1 ~ 2^127 - 1



* FLOAT（4 bytes）

Support scientific notation 



* DOUBLE（8 bytes）

Support scientific notation 



* DECIMAL[(precision, scale)] (16 bytes) 

 Default: DECIMAL(10, 0)

 precision: 1 ~ 38

 scale: 0 ~ precision

Integer part：precision - scale

Scientific notation is not supported 



* DATE（3 bytes）

Range ：0000-01-01 ~ 9999-12-31



* DATETIME（8 bytes ）

Range ：0000-01-01 00:00:00 ~ 9999-12-31 23:59:59



* CHAR[(length)]

Fixed length string. Range：1 ~ 255。default: 1



* VARCHAR[(length)]

Variable length string. Range：1 ~ 65533



* HLL (1~16385 bytes)

For HLL type, there's no need to specify length or default value. 

The length will be controlled within the system according to data aggregation. 

HLL column can only be queried or used by hll_union_agg、Hll_cardinality、hll_hash.



* BITMAP

 Bitmap type does not require specified length or default value. It represents a set of unsigned bigint numbers. The largest element could be up to 2^64 - 1.
agg_type：aggregation type. If not specified, this column is key column. 

If specified, it it value column. 



The aggregation types supported are as follows: 



* SUM、MAX、MIN、REPLACE



* HLL_UNION (only for HLL type) 



* BITMAP_UNION(only for BITMAP) 



* REPLACE_IF_NOT_NULL：This means the imported data will only be replaced when it is of non-null value. If it is of null value, StarRocks will retain the original value. 

Note: if NOT NULL is specified by REPLACE_IF_NOT_NULL column when the table was created, StarRocks will still convert the data to NULL without sending an error report to the user. With this, the user can import selected columns. 

This aggregation type applies ONLY to the aggregation model whose key_desc type is AGGREGATE KEY. 
NULL is not allowed by default. NULL value should be represented by /N in the impored data. 



Note: 

When the column of aggregation type BITMAP_UNION is imported, its original data types must be TINYINT, SMALLINT, 
```

### index_definition

Syntax:

```SQL
INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'
```

Note:

index_name: Index name

col_name: Column name

> Note:
> Currently only BITMAP index is supported and it only applies to single columns.

### ENGINE type

Default: olap. Optional: mysql, elasticsearch, and hive.

- For MySQL, properties should include:

    ```Plain_Text
    PROPERTIES (
        "host" = "mysql_server_host",
        "port" = "mysql_server_port",
        "user" = "your_user_name",
        "password" = "your_password",
        "database" = "database_name",
        "table" = "table_name"
    )
    ```

    Note:

    "table_name" in mysql should indicate the real table name. In contrast, "table_name" in CREATE TABLE statement indicates the name of this mysql table on StarRocks. They can either be different or the same.

    The aim of creating mysql tables in StarRocks is to access mysql database. StarRocks itself does not maintain or store any mysql data.

- For Elasticsearch, specify the following properties:

    ```Plain_Text
    PROPERTIES (
        "hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
        "user" = "root",
        "password" = "root",
        "index" = "tindex",
        "type" = "doc"
        )
        ```

        - `host`: the URL that is used to connect your Elasticsearch cluster. You can specify one or more URLs.
        - `user`: the account of the root user that is used to log in to your Elasticsearch cluster for which basic authentication is enabled.
        - `password`: the password of the preceding root account.
        - `index`: the index of the StarRocks table in your Elasticsearch cluster. The index name is the same as the StarRocks table name. You can set this parameter to the alias of the StarRocks table.
        - `type`: the type of index. The default value is `doc`.
    ```

- For Hive, properties should include:

    ```SQL
    PROPERTIES (
        "database" = "hive_db_name",
        "table" = "hive_table_name",
        "hive.metastore.uris" = "thrift://127.0.0.1:9083"
    )
    ```

    Here, database is the name of the corresponding database in Hive table. Table is the name of Hive table. hive.metastore.uris and Hive metastore are server addresses.

### key_desc

Syntax：

```SQL
`key_type(k1[,k2 ...])`
```

Note：

```Plain_Text
Data is sequenced in specified key columns and has different attributes for different key_type. 

Key_type supports: 

AGGREGATE KEY: Identical content in key columns will be aggregated into value columns according to the specified aggregation type. It usually applies to business scenarios such as financial statements and multi-dimensional analysis. 

UNIQUE KEY/PRIMARY KEY: Identical content in key columns will be replaced in value columns according to the import sequence. It can be applied to make addition, deletion, modification and query on key columns. 

DUPLICATE KEY: Identical content in key columns, which also exists in StarRocks at the same time. It can be used to store detailed data or data with no aggregation attributes. 

DUPLICATE KEY is set as default. Data will be sequenced according to key columns. 



Please note: 

Value columns do not need to specify aggregation types when other key_type is used to create tables with the exception of AGGREGATE KEY. 
```

### partition_desc

Partition description can be used in the following three ways:

#### LESS THAN

Syntax:

```Plain_Text
PARTITION BY RANGE (k1, k2, ...)

(
    PARTITION partition_name1 VALUES LESS THAN MAXVALUE|("value1", "value2", ...),

    PARTITION partition_name2 VALUES LESS THAN MAXVALUE|("value1", "value2", ...)

    ...
)
```

Note:

Please use specified key columns and specified value ranges for partitioning.

- Partition name only supports [A-z0-9_]
- Columns in Range partition only support the following types: TINYINT, SAMLLINT, INT, BIGINT, LARGEINT, DATE, and DATETIME.
- Partitions are left closed and right open. The left boundary of the first partition is of minimum value.
- NULL value is stored only in partitions that contain minimum values. When the partition containing the minimum value is deleted, NULL values can no longer be imported.
- Partition columns can either be single columns or multiple columns. The partition values are the default minimum values.

Please note:

- Partitions are often used for managing data related to time.
- When data backtracking is needed, you may want to consider emptying the first partition for adding partitions later when necessary.

#### Fixed Range

Syntax:

```SQL
PARTITION BY RANGE (k1, k2, k3, ...)
(
    PARTITION partition_name1 VALUES [("k1-lower1", "k2-lower1", "k3-lower1",...), ("k1-upper1", "k2-upper1", "k3-upper1", ...)),
    PARTITION partition_name2 VALUES [("k1-lower1-2", "k2-lower1-2", ...), ("k1-upper1-2", MAXVALUE, )),
    "k3-upper1-2", ...
)
```

Note:

- Fixed Range is more flexible than LESS THAN. You can customize the left and right partitions.
- Fixed Range is the same as LESS THAN in the other aspects.

#### Create partitions in bulk

Syntax

```Plain_Text
PARTITION BY RANGE (datekey) (

    START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 day)

)
```

Description

You can specify the value for `START` and `END` and the expression in `EVERY` to create partitions in bulk .

- If `datekey` supports DATE and INTEGER data type, the data type of `START`, `END`, and `EVERY` must be the same as the data type of `datekey`.
- If `datekey` only supports DATE data type, you need to use the `INTERVAL` keyword to specify the date interval. You can specify the date interval by day, week, month, or year. The naming conventions of partitions are the same as those for dynamic partitions.

For more information, see [Data Distribution](../../../table_design/Data_distribution.md#create-and-modify-partitions-in-bulk).

### distribution_des

Hash bucketing

Syntax:

```SQL
`DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]`
```

Note:

Please use specified key columns for Hash bucketing. The default bucket number is 10.

It is recommended to use Hash bucketing method.

### PROPERTIES

- If ENGINE type is olap. Users can specify storage medium, cooldown time and replica number.

```Plain_Text
PROPERTIES (
    "storage_medium" = "[SSD|HDD]",
    [ "storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss", ]
    [ "replication_num" = "3" ]
)
```

storage_medium: SSD or HDD could be specified as the initial storage media. You can specify default initial storage medium by specifying default_storage_medium=xxx through FE configuration file fe.conf. If no medium is specified, the default is HDD.

**Note**: When FE configuration item enable_strict_storage_medium_check is True and if storage medium is not set in the cluster, the statement for table creating will report an error: Failed to find enough host in all backends with storage medium is SSD|HDD.

storage_cooldown_time: When the storage medium is SSD, please specify its storage cooldown time. The default time is 30 days. Format: "yyyy-MM-dd HH:mm:ss"

replication_num: number of replicas in the specified partition. Default number: 3.

When the table has only one partition, the properties belongs to the table. When the table has two levels of partitions, the properties belong to each partition. Users can also specify different properties for different partitions through ADD ADDITION and MODIFY PARTITION statements.

- If Engine type is olap, users can specify a column to adopt bloom filter index which applies only to the condition where in and equal are query filters. More discrete values in this column will result in more precise queries. Bloom filter currently supports the key column, with the exception of the key column in TINYINT FLOAT DOUBLE type, and the value column with the aggregation method REPLACE.

```SQL
PROPERTIES (
    "bloom_filter_columns"="k1,k2,k3"
)
```

- If you want to use Colocate Join attributes, please specify it in properties.

```SQL
PROPERTIES (
    "colocate_with"="table1"
)
```

- If you want to use dynamic partition attributes, please specify it in properties.

```SQL
PROPERTIES (
    "dynamic_partition.enable" = "true|false",
    "dynamic_partition.time_unit" = "DAY|WEEK|MONTH",
    "dynamic_partition.start" = "${integer_value}",
    "dynamic_partitoin.end" = "${integer_value}",
    "dynamic_partition.prefix" = "${string_value}",
    "dynamic_partition.buckets" = "${integer_value}"
)
```

dynamic_partition.enable: It is used to specify whether dynamic partitioning at the table level is enabled. Default: true.

dynamic_partition.time_unit: It is used to specify the time unit for adding partitions dynamically. Time unit could be DAY, WEEK, MONTH.

dynamic_partition.start: It is used to specify how many partitions should be deleted. The value must be less than 0. Default: integer.Min_VAULE.

dynamic_partition.end: It is used to specify the how many partitions will be created in advance. The value must be more than 0.

dynamic_partition.prefix: It is used to specify the prefix of the created partition. For instance, if the prefix is p, the partition will be named p20200108 automatically.

dynamic_partition.buckets: It is used to specify the number of buckets automatically created in partitions.

- When building tables, Rollup can be created in bulk.

Syntax:

```SQL
ROLLUP (rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...)
```

## Examples

- Create an olap table that uses Hash bucketing and column-based storage, and that is aggregated by identical key records.

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
DISTRIBUTED BY HASH(k1) BUCKETS 10
PROPERTIES ("storage_type"="column");
```

- Create an olap table that uses Hash bucketing and column-based storage, and that is aggregated by identical key records. Also, please set the storage medium and the cooldown time.

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
DISTRIBUTED BY HASH (k1, k2) BUCKETS 10
PROPERTIES(
    "storage_type"="column",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

Or

```SQL
CREATE TABLE example_db.table_hash
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT SUM DEFAULT "10"
)
ENGINE=olap
PRIMARY KEY(k1, k2)
DISTRIBUTED BY HASH (k1, k2) BUCKETS 10
PROPERTIES(
    "storage_type"="column",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

- Create an olap table that uses Range partition, Hash bucketing and the default column-based storage. Records with the same key should exist at the same time. Also, please set the initial storage medium and the cooldown time.

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
DISTRIBUTED BY HASH(k2) BUCKETS 10
PROPERTIES(
    "storage_medium" = "SSD", "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

Note:

This statement will create 3 data partitions:

```SQL
( {    MIN     },   {"2014-01-01"} )
[ {"2014-01-01"},   {"2014-06-01"} )
[ {"2014-06-01"},   {"2014-12-01"} )
```

Data outside these ranges will be not be loaded.

Fixed Range

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
DISTRIBUTED BY HASH(k2) BUCKETS 10

PROPERTIES(
    "storage_medium" = "SSD"
);
```

- Create a mysql table.

```SQL
CREATE TABLE example_db.table_mysql
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

- Create a table that contain HLL columns.

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
DISTRIBUTED BY HASH(k1) BUCKETS 10
PROPERTIES ("storage_type"="column");
```

- Create table containing BITMAP_UNION aggregation type. (The original data type of v1 and v2 columns muse be TINYINT, SMALLINT, INT)

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
DISTRIBUTED BY HASH(k1) BUCKETS 10
PROPERTIES ("storage_type"="column");
```

- Create table t1 and t2 that support Colocate Join.

```SQL
CREATE TABLE `t1` (
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "t1"
);



CREATE TABLE `t2` (
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
    "colocate_with" = "t1"
);
```

- Create a table with bitmap index

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
DISTRIBUTED BY HASH(k1) BUCKETS 10
PROPERTIES ("storage_type"="column");
```

- Create a dynamic partition table. (The dynamic partitioning function should be turned on in FE configuration.) This table will create partitions for three days and delete those created three days ago. For example, assuming today is 2020-01-08, partitions with these names will be created: p20200108, p20200109, p20200110, p20200111. And their ranges are:

```Plain_Text
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

- Create an external table in hive.

```SQL
CREATE TABLE example_db.table_hive
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

## Keywords

CREATE, TABLE
