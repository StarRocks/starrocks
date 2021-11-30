# CREATE TABLE

## description

This table is used to create a table.

Syntax:

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...]
[, index_definition1[, ndex_definition12,]])
[ENGINE = [olap|mysql|broker|hive]]
[key_desc]
[COMMENT "table comment"];
[partition_desc]
[distribution_desc]
[rollup_index]
[PROPERTIES ("key"="value", ...)]
[BROKER PROPERTIES ("key"="value", ...)]
```

1. column_definition

    Syntax:

    ```sql
    col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
    ```

    Note:

    ```plain text
    col_name：Column name
    ```

    ```plain text
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
    
    * DOUBLE（12 bytes）
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
    ```

    ```plain text
    agg_type：aggregation type. If not specified, this column is key column. 
    If specified, it it value column. 
    
    The aggregation types supported are as follows: 
    
    * SUM、MAX、MIN、REPLACE
    
    * HLL_UNION (only for HLL type) 
    
    * BITMAP_UNION(only for BITMAP) 
    
    * REPLACE_IF_NOT_NULL：This means the imported data will only be replaced when it is of non-null value. If it is of null value, StarRocks will retain the original value. 
    Note: if NOT NULL is specified by REPLACE_IF_NOT_NULL column when the table was created, StarRocks will still convert the data to NULL without sending an error report to the user. With this, the user can import selected columns. 
    This aggregation type applies ONLY to the aggregation model whose key_desc type is AGGREGATE KEY. 
    ```

    ```plain text
    NULL is not allowed by default. NULL value should be represented by /N in the impored data. 
    
    Note: 
    When the column of aggregation type BITMAP_UNION is imported, its original data types must be TINYINT, SMALLINT, 
    ```

2. index_definition

    Syntax:

    ```sql
    INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'
    ```

    Note:

    index_name：Index name

    col_name：Column name

    > Note:
    >
    > Currently only BITMAP index is supported and it only applies to single columns.

3. ENGINE type

    Default: olap. Optional: mysql, broker,hive.

## Example

1. For mysql, properties should include:

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

    Note:

    "table_name" in mysql should indicate the real table name. In contrast, "table_name" in CREATE TABLE statement indicates the name of this mysql table on StarRocks. They can either be different or the same.

    The aim of creating mysql tables in StarRocks is to access mysql database. StarRocks itself does not maintain or store any mysql data.

2. For borker, accessing tables requires specified broker, and properties should include:

    ```sql
    PROPERTIES (
        "broker_name" = "broker_name",
        "path" = "file_path1[,file_path2]",
        "column_separator" = "value_separator"
        "line_delimiter" = "value_delimiter"
    )
    ```

    In addition, Property information required by broker, such as HDFS, should be provided and transmitted through BROKER PROPERTIES.

    ```sql
    BROKER PROPERTIES(
        "username" = "name",
        "password" = "password"
    )
    ```

    Different broker types may require different content.

    Note:

    If multiple files are in the "path", commas should be used to separate them. If the file name contains commas, please replace them with %2c. If the name contains %, replace them with %25. Currently, the content in files can be compressed into CSV, GZ, BZ2, LZ4, LZO(LZOP) formats.

3. For hive, properties should include:

    ```sql
    PROPERTIES (
        "database" = "hive_db_name",
        "table" = "hive_table_name",
        "hive.metastore.uris" = "thrift://127.0.0.1:9083"
    )
    ```

    Here, database is the name of the corresponding database in hive table. Table is the name of hive table. hive.metastore.uris and hive metastore are server addresses.  

    Note:

    Currently the external hive table can only be used for Spark Load without query functionality.

### Syntax

1. key_desc

    Syntax：

    ```sql
    `key_type(k1[,k2 ...])`
    ```

    Note：

    ```plain text
    Data is sequenced in specified key columns and has different attributes for different key_type. 
    Key_type supports: 
    AGGREGATE KEY: Identical content in key columns will be aggregated into value columns according to the specified aggregation type. It usually applies to business scenarios such as financial statements and multi-dimensional analysis. 
    UNIQUE KEY/PRIMARY KEY: Identical content in key columns will be replaced in value columns according to the import sequence. It can be applied to make addition, deletion, modification and query on key columns. 
    DUPLICATE KEY: Identical content in key columns, which also exists in StarRocks at the same time. It can be used to store detailed data or data with no aggregation attributes. 
    DUPLICATE KEY is set as default. Data will be sequenced according to key columns. 
    
    Please note: 
    Value columns do not need to specify aggregation types when other key_type is used to create tables with the exception of AGGREGATE KEY. 
    ```

2. partition_desc

    Partition description can be used in two ways.

    LESS THAN

    Syntax:

    ```sql
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
    - Columns in Range partition only supports the following types and only one partition column can be specified: TINYINT, SAMLLINT, INT, BIGINT, LARGEINT, DATE, DATETIME  
    - Partitions are left closed and right open. The left boundary of the first partition is of minimum value.
    - NULL value is stored only in partitions that contain minimum values. When the partition containing the minimum value is deleted, NULL values can no longer be imported.  
    - Partition columns can either be single columns or multiple columns. The partition values are the default minimum values.

    Please note:

    - Partitions are often used for managing data related to time.  
    - When data backtracking is needed, you may want to consider emptying the first partition for adding partitions later when necessary.

    Fixed Range

    Syntax:

    ```sql
    PARTITION BY RANGE (k1, k2, k3, ...)
    (
    PARTITION partition_name1 VALUES [("k1-lower1", "k2-lower1", "k3-lower1",...), ("k1-upper1", "k2-upper1", "k3-upper1", ...)),
    PARTITION partition_name2 VALUES [("k1-lower1-2", "k2-lower1-2", ...), ("k1-upper1-2", MAXVALUE, )),
    "k3-upper1-2", ...
    )
    ```

    Note:

    \1) Fixed Range is relatively more flexible than LESS THAN. Users can customize the left and right partitions.

    \2) Others should be kept consistent with LESS THAN.

3. distribution_des

    \1) Hash bucketing

    Syntax:

    ```sql
    `DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]`
    ```

    Note:

    Please use specified key columns for Hash bucketing. The default bucket number is 10.

    It is recommended to use Hash bucketing method.

4. PROPERTIES

    \1) If ENGINE type is olap. Users can specify storage medium, cooldown time and replica number.

    ```sql
    PROPERTIES (
        "storage_medium" = "[SSD|HDD]",
    ["storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss"],
    ["replication_num" = "3"]
    )
    ```

    storage_medium: SSD or HDD could be specified as the initial storage media. Users can specify default initial storage medium by specifying  default_storage_medium=xxx through fe configuration file fe.conf. If no medium is specified, the default is HDD.

    Note: When FE configuration item enable_strict_storage_medium_check is True and if storage medium is not set in the cluster, the statement for table creating will report an error: Failed to find enough host in all backends with storage medium is SSD|HDD.

    storage_cooldown_time: When the storage medium is SSD, please specify its storage cooldown time. The default time is 30 days. Format: "yyyy-MM-dd HH:mm:ss"

    replication_num: number of replicas in the specified partition. Default number: 3.

    When the table has only one partition, the properties belongs to the table. When the table has two levels of partitions, the properties belong to each partition. Users can also specify different properties for different partitions through ADD ADDITION and MODIFY PARTITION statements.

    \2) If Engine type is olap, users can specify a column to adopt bloom filter index which applies only to the condition where in and equal are query filters. More discrete values in this column will result in more precise queries. Bloom filter currently supports the key column, with the exception of the key column in TINYINT FLOAT DOUBLE type, and the value column with the aggregation method REPLACE.

    ```sql
    PROPERTIES (
        "bloom_filter_columns"="k1,k2,k3"
    )
    ```

    \3) If you want to use Colocate Join attributes, please specify it in properties.

    ```sql
    PROPERTIES (
        "colocate_with"="table1"
    )
    ```

    \4) If you want to use dynamic partition attributes, please specify it in properties.

    ```sql
    PROPERTIES (
        "dynamic_partition.enable" = "true|false",
        "dynamic_partition.time_unit" = "DAY|WEEK|MONTH",
        "dynamic_partition.start" = "${integer_value}",
        "dynamic_partitoin.end" = "${integer_value}",
        "dynamic_partition.prefix" = "${string_value}",
        "dynamic_partition.buckets" = "${integer_value}"
    ```

    dynamic_partition.enable: It is used to specify whether dynamic partitioning at the table level is enabled. Default: true.

    dynamic_partition.time_unit: It is used to specify the time unit for adding partitions dynamically. Time unit could be DAY， WEEK, MONTH.

    dynamic_partition.start: It is used to specify how many partitions should be deleted. The value must be less than 0. Default: integer.Min_VAULE.

    dynamic_partition.end: It is used to specify the how many partitions will be created in advance. The value must be more than 0.

    dynamic_partition.prefix: It is used to specify the prefix of the created partition. For instance, if the prefix is p, the partition will be named p20200108 automatically.

    dynamic_partition.buckets: It is used to specify the number of buckets automatically created in partitions.

    \5) When building tables, Rollup can be created in bulk.

    Syntax:

    ```sql
    ROLLUP (rollup_name (column_name1, column_name2, ...)
    [FROM from_index_name]
    [PROPERTIES ("key"="value", ...)],...)
    ```

    \6) If you want to use inmemory table attributes, please specify it in properties.

    ```sql
    PROPERTIES (
        "in_memory"="true"
    )
    ```

    When the attribute is true, StarRocks will try to cache the data and index in this table to BE memory.

## example

1. Create an olap table that uses Hash bucketing and column-based storage, and that is aggregated by identical key records.

    ```sql
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
    DISTRIBUTED BY HASH(k1) BUCKETS 32
    PROPERTIES ("storage_type"="column");
    ```

2. Create an olap table that uses Hash bucketing and column-based storage, and that is aggregated by identical key records. Also, please set the storage medium and the cooldown time

    ```sql
    CREATE TABLE example_db.table_hash
    (
        k1 BIGINT,
        k2 LARGEINT,
        v1 VARCHAR(2048) REPLACE,
        v2 SMALLINT SUM DEFAULT "10"
    )
    ENGINE=olap
    UNIQUE KEY(k1, k2)
    DISTRIBUTED BY HASH (k1, k2) BUCKETS 32
    PROPERTIES(
        "storage_type"="column"，
        "storage_medium" = "SSD",
        "storage_cooldown_time" = "2015-06-04 00:00:00"
    );
    ```

    Or

    ```sql
    CREATE TABLE example_db.table_hash
    (
        k1 BIGINT,
        k2 LARGEINT,
        v1 VARCHAR(2048) REPLACE,
        v2 SMALLINT SUM DEFAULT "10"
    )
    ENGINE=olap
    PRIMARY KEY(k1, k2)
    DISTRIBUTED BY HASH (k1, k2) BUCKETS 32
    PROPERTIES(
        "storage_type"="column"，
        "storage_medium" = "SSD",
        "storage_cooldown_time" = "2015-06-04 00:00:00"
    );
    ```

3. Create an olap table that uses Range partition, Hash bucketing and the default column-based storage. Records with the same key should exist at the same time. Also, please set the initial storage medium and the cooldown time.

    LESS THAN

    ```sql
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
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    PROPERTIES(
        "storage_medium" = "SSD", "storage_cooldown_time" = "2015-06-04 00:00:00"
    );
    ```

    Note:

    This statement will create 3 data partitions:

    ```sql
    ( {    MIN     },   {"2014-01-01"} )
    [ {"2014-01-01"},   {"2014-06-01"} )
    [ {"2014-06-01"},   {"2014-12-01"} )
    ```

    Data outside these ranges will be not be loaded.

    Fixed Range

    ```sql
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
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    PROPERTIES(
        "storage_medium" = "SSD"
    );
    ```

4. Create a mysql table.

    ```sql
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

5. Create a mysql table.

    ```sql
    CREATE TABLE example_db.example_table
    (
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 HLL HLL_UNION,
    v2 HLL HLL_UNION
    )
    ENGINE=olap
    AGGREGATE KEY(k1, k2)
    DISTRIBUTED BY HASH(k1) BUCKETS 32
    PROPERTIES ("storage_type"="column");
    ```

6. Create table containing BITMAP_UNION aggregation type. (The original data type of v1 and v2 columns muse be TINYINT, SMALLINT, INT)

    ```sql
    CREATE TABLE example_db.example_table
    (
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 BITMAP BITMAP_UNION,
    v2 BITMAP BITMAP_UNION
    )
    ENGINE=olap
    AGGREGATE KEY(k1, k2)
    DISTRIBUTED BY HASH(k1) BUCKETS 32
    PROPERTIES ("storage_type"="column");
    ```

7. Create table t1 and t2 that support Colocat Join

    ```sql
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

8. Create a table with bitmap index

    ```sql
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
    DISTRIBUTED BY HASH(k1) BUCKETS 32
    PROPERTIES ("storage_type"="column");
    ```

9. Create a dynamic partition table. (The dynamic partitioning function should be turned on in FE configuration.) This table will create partitions for three days and delete those created three days ago. For example, assuming today is 2020-01-08, partitions with these names will be created: p20200108, p20200109, p20200110, p20200111. And their ranges are:

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
    ENGINE=olap
    DUPLICATE KEY(k1, k2, k3)
    PARTITION BY RANGE (k1)
    (
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
    )
    DISTRIBUTED BY HASH(k2) BUCKETS 32
    PROPERTIES(
        "storage_medium" = "SSD",
        "dynamic_partition.time_unit" = "DAY",
        "dynamic_partition.start" = "-3",
        "dynamic_partition.end" = "3",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "32"
    );
    ```

10. Create a table with rollup index

    ```sql
    CREATE TABLE example_db.rolup_index_table
    (
    event_day DATE,
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
    )
    AGGREGATE KEY(event_day, siteid, citycode, username)
    DISTRIBUTED BY HASH(siteid) BUCKETS 10
    rollup (
    r1(event_day,siteid),
    r2(event_day,citycode),
    r3(event_day)
    )
    PROPERTIES("replication_num" = "3");

11. Create an in-memory table

     ```sql
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
     DISTRIBUTED BY HASH(k1) BUCKETS 32
     PROPERTIES ("in_memory"="true");
     ```

12. Create an external table in hive

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
         "database" = "hive_db_name",
         "table" = "hive_table_name",
         "hive.metastore.uris" = "thrift://127.0.0.1:9083"
     );
     ```

## keyword

CREATE,TABLE
