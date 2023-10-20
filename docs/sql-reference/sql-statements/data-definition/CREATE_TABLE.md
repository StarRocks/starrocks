# CREATE TABLE

## Description

Creates a new table in StarRocks.

## Syntax

```Plain_Text
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...]
[, index_definition1[, index_definition12,]])
[ENGINE = [olap|mysql|elasticsearch|hive|hudi|iceberg|jdbc]]
[key_desc]
[COMMENT "table comment"]
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
col_name: Column name
col_type: Column type



Specific column information, such as types and ranges: 

- DATE (3 bytes): Ranges from 0000-01-01 to 9999-12-31.
- DATETIME (8 bytes): Ranges from 0000-01-01 00:00:00 to 9999-12-31 23:59:59.
- CHAR[(length)]: Fixed length string. Range: 1 ~ 255. Default value: 1.
- VARCHAR[(length)]: A variable-length string. The default value is 1. Unit: bytes. In versions earlier than StarRocks 2.1, the value range of `length` is 1–65533. [Preview] In StarRocks 2.1 and later versions, the value range of `length` is 1–1048576.
- HLL (1~16385 bytes): For HLL type, there's no need to specify length or default value. The length will be controlled within the system according to data aggregation. HLL column can only be queried or used by [hll_union_agg](../../sql-functions/aggregate-functions/hll_union_agg.md), [Hll_cardinality](../../sql-functions/scalar-functions/hll_cardinality.md), and [hll_hash](../../sql-functions/aggregate-functions/hll_hash.md).
- BITMAP: Bitmap type does not require specified length or default value. It represents a set of unsigned bigint numbers. The largest element could be up to 2^64 - 1.

* TINYINT (1 byte)

Range: -2^7 + 1 ~ 2^7 - 1

* SMALLINT (2 bytes )

Range: -2^15 + 1 ~ 2^15 - 1

* INT (4 bytes)

Range: -2^31 + 1 ~ 2^31 - 1

* BIGINT (8 bytes)

Range: -2^63 + 1 ~ 2^63 - 1

* LARGEINT (16 bytes)

Range: -2^127 + 1 ~ 2^127 - 1

* FLOAT (4 bytes)

Support scientific notation 

* DOUBLE (8 bytes)

Support scientific notation 

* DECIMAL[(precision, scale)] (16 bytes) 

 Default value: DECIMAL(10, 0)

 precision: 1 ~ 38

 scale: 0 ~ precision

Integer part: precision - scale

Scientific notation is not supported 

* DATE (3 bytes)

Range: 0000-01-01 ~ 9999-12-31

* DATETIME (8 bytes)

Range: 0000-01-01 00:00:00 ~ 9999-12-31 23:59:59

* CHAR[(length)]

Fixed length string. Range: 1 ~ 255. Default value: 1.

* VARCHAR[(length)]

A variable-length string. The default value is 1. Unit: bytes.

- In versions earlier than StarRocks 2.1, the value range of `length` is 1–65533.
- [Preview] In StarRocks 2.1 and later versions, the value range of `length` is 1–1048576.

* HLL (1~16385 bytes)

For HLL type, there's no need to specify length or default value. 

The length will be controlled within the system according to data aggregation. 

HLL column can only be queried or used by hll_union_agg, Hll_cardinality, hll_hash.

* BITMAP

 Bitmap type does not require specified length or default value. It represents a set of unsigned bigint numbers. The largest element could be up to 2^64 - 1.
agg_type: aggregation type. If not specified, this column is key column. 

If specified, it it value column. 

The aggregation types supported are as follows: 

* SUM, MAX, MIN, REPLACE
* HLL_UNION (only for HLL type) 
* BITMAP_UNION(only for BITMAP) 

* REPLACE_IF_NOT_NULL: This means the imported data will only be replaced when it is of non-null value. If it is of null value, StarRocks will retain the original value. 

Note: if NOT NULL is specified by REPLACE_IF_NOT_NULL column when the table was created, StarRocks will still convert the data to NULL without sending an error report to the user. With this, the user can import selected columns. 

This aggregation type applies ONLY to the aggregation model whose key_desc type is AGGREGATE KEY. 
NULL is not allowed by default. NULL value should be represented by /N in the imported data. 

Note: 

When the column of aggregation type BITMAP_UNION is imported, its original data types must be TINYINT, SMALLINT, 
```

### index_definition

Syntax:

```SQL
INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'
```

You can only create bitmap indexes when you create tables. For more information about parameter descriptions and usage notes, see [Bitmap indexing](../../../using_starrocks/Bitmap_index.md#create-a-bitmap-index).

### ENGINE type

Default value: olap. If this parameter is not specified, an OLAP table (StarRocks native table) is created by default.

Optional value: mysql, elasticsearch, hive, jdbc (2.3 and later), iceberg, and hudi (2.2 and later). If you want to create an external table to query external data sources, specify `CREATE EXTERNAL TABLE` and set `ENGINE` to any of these values. You can refer to [External table](../../../data_source/External_table.md) for more information.

- For MySQL, specify the following properties:

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

    "table_name" in MySQL should indicate the real table name. In contrast, "table_name" in CREATE TABLE statement indicates the name of this mysql table on StarRocks. They can either be different or the same.

    The aim of creating MySQL tables in StarRocks is to access MySQL database. StarRocks itself does not maintain or store any MySQL data.

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

  - `hosts`: the URL that is used to connect your Elasticsearch cluster. You can specify one or more URLs.
  - `user`: the account of the root user that is used to log in to your Elasticsearch cluster for which basic authentication is enabled.
  - `password`: the password of the preceding root account.
  - `index`: the index of the StarRocks table in your Elasticsearch cluster. The index name is the same as the StarRocks table name. You can set this parameter to the alias of the StarRocks table.
  - `type`: the type of index. The default value is `doc`.

- For Hive, specify the following properties:

    ```Plain_Text
    PROPERTIES (

        "database" = "hive_db_name",
        "table" = "hive_table_name",
        "hive.metastore.uris" = "thrift://127.0.0.1:9083"
    )
    ```

    Here, database is the name of the corresponding database in Hive table. Table is the name of Hive table. hive.metastore.uris is the server address.

- For JDBC, specify the following properties:

    ```Plain_Text
    PROPERTIES (
    "resource"="jdbc0",
    "table"="dest_tbl"
    )
    ```

    `resource` is the JDBC resource name and `table` is the destination table.

- For Iceberg, specify the following properties:

   ```Plain_Text
    PROPERTIES (
    "resource" = "iceberg0", 
    "database" = "iceberg", 
    "table" = "iceberg_table"
    )
    ```

    `resource` is the Iceberg resource name. `database` is the Iceberg database. `table` is the Iceberg table.

- For Hudi, specify the following properties:

  ```Plain_Text
    PROPERTIES (
    "resource" = "hudi0", 
    "database" = "hudi", 
    "table" = "hudi_table" 
    )
    ```

### key_desc

Syntax: 

```SQL
key_type(k1[,k2 ...])
```

Data is sequenced in specified key columns and has different attributes for different key types:

- AGGREGATE KEY: Identical content in key columns will be aggregated into value columns according to the specified aggregation type. It usually applies to business scenarios such as financial statements and multi-dimensional analysis.
- UNIQUE KEY/PRIMARY KEY: Identical content in key columns will be replaced in value columns according to the import sequence. It can be applied to make addition, deletion, modification and query on key columns.
- DUPLICATE KEY: Identical content in key columns, which also exists in StarRocks at the same time. It can be used to store detailed data or data with no aggregation attributes. **DUPLICATE KEY is the default type. Data will be sequenced according to key columns.**

> **NOTE**
>
> Value columns do not need to specify aggregation types when other key_type is used to create tables with the exception of AGGREGATE KEY.

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

Use specified key columns and specified value ranges for partitioning.

- Partition name can contain only letters (A-z) and digits (0-9).
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

For more information, see [Data distribution](../../../table_design/Data_distribution.md#create-and-modify-partitions-in-bulk).

### distribution_desc

Hash bucketing

Syntax:

```SQL
`DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]`
```

Note:

Please use specified key columns for Hash bucketing. The default bucket number is 10.

It is recommended to use Hash bucketing method.

### PROPERTIES

#### Specify storage medium, storage cooldown time, replica number

- If ENGINE type is olap. Users can specify storage medium, cooldown time, and replica number.

```Plain_Text
PROPERTIES (
    "storage_medium" = "[SSD|HDD]",
    [ "storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss", ]
    [ "replication_num" = "3" ]
)
```

storage_medium: SSD or HDD can be specified as the initial storage medium.

> **Note**
>
> When the FE configuration item `enable_strict_storage_medium_check` is `True` and the storage medium is not specified, the statement for creating a table will report an error: Failed to find enough host in all backends with storage medium is SSD|HDD.

storage_cooldown_time: the storage cooldown time for a partition. If the storage medium is SSD, SSD is switched to HDD after the time specified by this parameter. Format: "yyyy-MM-dd HH:mm:ss". The specified time must be later than the current time. If this parameter is not explicitly specified, auto-cooldown is not performed by default.

replication_num: number of replicas in the specified partition. Default number: 3.

When the table has only one partition, the properties belongs to the table. When the table has two levels of partitions, the properties belong to each partition. Users can also specify different properties for different partitions through ADD ADDITION and MODIFY PARTITION statements.

#### Add bloom filter index for a column

If the Engine type is olap, you can specify a column to adopt bloom filter indexes.

The following limits apply when you use bloom filter index:

- You can create bloom filter indexes for all columns of a Duplicate Key or Primary Key table. For an Aggregate table or Unique Key table, you can only create bloom filter indexes for key columns.
- TINYINT, FLOAT, DOUBLE, and DECIMAL columns do not support creating bloom filter indexes.
- Bloom filter indexes can only improve the performance of queries that contain the `in` and `=` operators, such as `Select xxx from table where x in {}` and `Select xxx from table where column = xxx`. More discrete values in this column will result in more precise queries.

For more information, see [Bloom filter indexing](../../../using_starrocks/Bloomfilter_index.md)

```SQL
PROPERTIES (
    "bloom_filter_columns"="k1,k2,k3"
)
```

#### Use Colocate Join

If you want to use Colocate Join attributes, specify it in `properties`.

```SQL
PROPERTIES (
    "colocate_with"="table1"
)
```

#### Configure dynamic partitions

If you want to use dynamic partition attributes, please specify it in properties.

```SQL
PROPERTIES (

    "dynamic_partition.enable" = "true|false",
    "dynamic_partition.time_unit" = "DAY|WEEK|MONTH",
    "dynamic_partition.start" = "${integer_value}",
    "dynamic_partition.end" = "${integer_value}",
    "dynamic_partition.prefix" = "${string_value}",
    "dynamic_partition.buckets" = "${integer_value}"
```

dynamic_partition.enable: It is used to specify whether dynamic partitioning at the table level is enabled. Default value: true.

dynamic_partition.time_unit: It is used to specify the time unit for adding partitions dynamically. Time unit could be DAY, WEEK, MONTH.

dynamic_partition.start: It is used to specify how many partitions should be deleted. The value must be less than 0. Default value: integer.Min_VAULE.

dynamic_partition.end: It is used to specify the how many partitions will be created in advance. The value must be more than 0.

dynamic_partition.prefix: It is used to specify the prefix of the created partition. For instance, if the prefix is p, the partition will be named p20200108 automatically.

dynamic_partition.buckets: It is used to specify the number of buckets automatically created in partitions.

#### Create rollup in bulk

You can create rollup in bulk when you create a table.

Syntax:

```SQL
ROLLUP (rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...)
```

## Examples

### Create an Aggregate table that uses Hash bucketing and columnar storage

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

### Create an Aggregate table and set the storage medium and cooldown time

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

### Create a Duplicate Key table that uses Range partition, Hash bucketing, and column-based storage, and set the storage medium and cooldown time

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
    "storage_medium" = "SSD", 
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

Note:

This statement will create three data partitions:

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

### Create a MySQL external table

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

### Create a table that contains HLL columns

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

### Create a table containing BITMAP_UNION aggregation type

The original data type of `v1` and `v2` columns must be TINYINT, SMALLINT, or INT.

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

### Create two tables that support Colocate Join

```SQL
CREATE TABLE `t1` 
(
     `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) 
ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
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
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES 
(
    "colocate_with" = "t1"
);
```

### Create a table with bitmap index

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

### Create a dynamic partition table

The dynamic partitioning function must be enabled ("dynamic_partition.enable" = "true") in FE configuration. For more information, see [Configure dynamic partitions](#configure-dynamic-partitions).

This example creates partitions for the next three days and deletes partitions created three days ago. For example, if today is 2020-01-08, partitions with the following names will be created: p20200108, p20200109, p20200110, p20200111, and their ranges are:

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
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10"
);
```

### Create a Hive external table

Before you create a Hive external table, you must have created a Hive resource and database. For more information, see [External table](../../../data_source/External_table.md#hive-external-table).

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
