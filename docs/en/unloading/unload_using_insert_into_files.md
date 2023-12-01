---
displayed_sidebar: "English"
---

# Unload data using INSERT INTO FILES

This topic describes how to unload data from StarRocks into remote storage by using INSERT INTO FILES.

From v3.2 onwards, StarRocks supports using the table function FILES() to define a writable file in remote storage. You can then combine FILES() with the INSERT statement to unload data from StarRocks to your remote storage.

Compared to other data export methods supported by StarRocks, unloading data with INSERT INTO FILES provides a more unified, easy-to-use interface. You can unload the data directly into your remote storage using the same syntax that you used to load data from it. Moreover, this method supports storing the data files in different storage paths by extracting the values of a specified column, allowing you to manage the exported data in a partitioned layout.

> **NOTE**
>
> - Please note that unloading data with INSERT INTO FILES does not support exporting data into local file systems.
> - Currently,  INSERT INTO FILES only supports unloading data in the Parquet file format.

## Preparation

The following example creates a database `unload` and a table `sales_records` as the data objects that can be used in the following tutorial. You can use your own data instead.

```SQL
CREATE DATABASE unload;
USE unload;
CREATE TABLE sales_records(
    record_id     BIGINT,
    seller        STRING,
    store_id      INT,
    sales_time    DATETIME,
    sales_amt     DOUBLE
)
DUPLICATE KEY(record_id)
PARTITION BY date_trunc('day', sales_time)
DISTRIBUTED BY HASH(record_id);

INSERT INTO sales_records
VALUES
    (220313001,"Amy",1,"2022-03-13 12:00:00",8573.25),
    (220314002,"Bob",2,"2022-03-14 12:00:00",6948.99),
    (220314003,"Amy",1,"2022-03-14 12:00:00",4319.01),
    (220315004,"Carl",3,"2022-03-15 12:00:00",8734.26),
    (220316005,"Carl",3,"2022-03-16 12:00:00",4212.69),
    (220317006,"Bob",2,"2022-03-17 12:00:00",9515.88);
```

The table `sales_records` contains the transaction ID `record_id`, salesperson `seller`, store ID `store_id`, time `sales_time`, and sales amount `sales_amt` for each transaction. It is partitioned on a daily basis in accordance with `sales_time`.

You also need to prepare a remote storage system with write access. The following examples use an HDFS cluster with the simple authentication method enabled. For more about the remote storage systems and credential methods supported, see [SQL reference - FILES()](../sql-reference/sql-functions/table-functions/files.md).

## Unload data

INSERT INTO FILES supports unloading data into a single file or multiple files. You can further partition these data files by specifying separate storage paths for them.

When unloading data using INSERT INTO FILES, you must manually set the compression algorithm using the property `compression`. For more information on the data compression algorithm supported by StarRocks, see [Data compression](../table_design/data_compression.md).

### Unload data into multiple files

By default,  INSERT INTO FILES unloads data into multiple data files, each with a size of 1 GB. You can configure the file size using the property `max_file_size`.

The following example unloads all data rows in `sales_records` as multiple Parquet files prefixed by `data1`. The size of each file is 1 KB.

```SQL
INSERT INTO 
FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/data1",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "compression" = "lz4",
    "max_file_size" = "1KB"
)
SELECT * FROM sales_records;
```

### Unload data into multiple files under different paths

You can also partition the data files in different storage paths by extracting the values of the specified column using the property `partition_by`.

The following example unloads all data rows in `sales_records` as multiple Parquet files under the path **/unload/partitioned/** in the HDFS cluster. These files are stored in different subpaths distinguished by the values in the column `sales_time`.

```SQL
INSERT INTO 
FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/partitioned/",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "compression" = "lz4",
    "partition_by" = "sales_time"
)
SELECT * FROM sales_records;
```

### Unload data into a single file

To unload data into a single data file, you must specify the property `single` as `true`.

The following example unloads all data rows in `sales_records` as a single Parquet file prefixed by `data2`.

```SQL
INSERT INTO 
FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/data2",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "compression" = "lz4",
    "single" = "true"
)
SELECT * FROM sales_records;
```

## See also

- For more instructions on the usage of INSERT, see [SQL reference - INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md).
- For more instructions on the usage of FILES(), see [SQL reference - FILES()](../sql-reference/sql-functions/table-functions/files.md).
