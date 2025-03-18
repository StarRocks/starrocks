---
displayed_sidebar: docs
---

# Unload data using INSERT INTO FILES

This topic describes how to unload data from StarRocks into remote storage by using INSERT INTO FILES.

From v3.2 onwards, StarRocks supports using the table function FILES() to define a writable file in remote storage. You can then combine FILES() with the INSERT statement to unload data from StarRocks to your remote storage.

Compared to other data export methods supported by StarRocks, unloading data with INSERT INTO FILES provides a more unified, easy-to-use interface. You can unload the data directly into your remote storage using the same syntax that you used to load data from it. Moreover, this method supports storing the data files in different storage paths by extracting the values of a specified column, allowing you to manage the exported data in a partitioned layout.

> **NOTE**
>
> Please note that unloading data with INSERT INTO FILES does not support directly exporting data into local file systems. However, you can export the data into local files by using NFS. See [Unload to local files using NFS](#unload-to-local-files-using-nfs).

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

You also need to prepare a remote storage system with write access. The following examples export data to the following remote storages:

- An HDFS cluster with the simple authentication method enabled.
- An AWS S3 bucket using the IAM User credential.

For more about the remote storage systems and credential methods supported by FILES(), see [SQL reference - FILES()](../sql-reference/sql-functions/table-functions/files.md).

## Unload data

INSERT INTO FILES supports unloading data into a single file or multiple files. You can further partition these data files by specifying separate storage paths for them.

When unloading data using INSERT INTO FILES, you must manually set the compression algorithm using the property `compression`. For more information on the data compression algorithm supported by FILES, see [unload_data_param](../sql-reference/sql-functions/table-functions/files.md#unload_data_param).

### Unload data into multiple files

By default,  INSERT INTO FILES unloads data into multiple data files, each with a size of 1 GB. You can configure the file size using the property `target_max_file_size` (Unit: Bytes).

The following example unloads all data rows in `sales_records` as multiple Parquet files prefixed by `data1`. The size of each file is 1 KB.

:::note

Here, setting `target_max_file_size` to 1 KB is to demonstrate unloading into multiple files with a small dataset. In production environment, you are strongly advised to set this value within the range of hundreds of MB to multiple GB.

:::

- **To S3**:

```SQL
INSERT INTO 
FILES(
    "path" = "s3://mybucket/unload/data1",
    "format" = "parquet",
    "compression" = "uncompressed",
    "target_max_file_size" = "1024", -- 1KB
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy",
    "aws.s3.region" = "us-west-2"
)
SELECT * FROM sales_records;
```

- **To HDFS**:

```SQL
INSERT INTO 
FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/data1",
    "format" = "parquet",
    "compression" = "uncompressed",
    "target_max_file_size" = "1024", -- 1KB
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx"
)
SELECT * FROM sales_records;
```

### Unload data into multiple files under different paths

You can also partition the data files in different storage paths by extracting the values of the specified column using the property `partition_by`.

The following example unloads all data rows in `sales_records` as multiple Parquet files under the path **/unload/partitioned/**. These files are stored in different subpaths distinguished by the values in the column `sales_time`.

- **To S3**:

```SQL
INSERT INTO 
FILES(
    "path" = "s3://mybucket/unload/partitioned/",
    "format" = "parquet",
    "compression" = "lz4",
    "partition_by" = "sales_time",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy",
    "aws.s3.region" = "us-west-2"
)
SELECT * FROM sales_records;
```

- **To HDFS**:

```SQL
INSERT INTO 
FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/partitioned/",
    "format" = "parquet",
    "compression" = "lz4",
    "partition_by" = "sales_time",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx"
)
SELECT * FROM sales_records;
```

### Unload data into a single file

To unload data into a single data file, you must specify the property `single` as `true`.

The following example unloads all data rows in `sales_records` as a single Parquet file prefixed by `data2`.

- **To S3**:

```SQL
INSERT INTO 
FILES(
    "path" = "s3://mybucket/unload/data2",
    "format" = "parquet",
    "compression" = "lz4",
    "single" = "true",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy",
    "aws.s3.region" = "us-west-2"
)
SELECT * FROM sales_records;
```

- **To HDFS**:

```SQL
INSERT INTO 
FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/unload/data2",
    "format" = "parquet",
    "compression" = "lz4",
    "single" = "true",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx"
)
SELECT * FROM sales_records;
```

### Unload to MinIO

The parameters used for MinIO are different from those used for AWS S3.

Example:

```SQL
INSERT INTO 
FILES(
    "path" = "s3://huditest/unload/data3",
    "format" = "parquet",
    "compression" = "zstd",
    "single" = "true",
    "aws.s3.access_key" = "xxxxxxxxxx",
    "aws.s3.secret_key" = "yyyyyyyyyy",
    "aws.s3.region" = "us-west-2",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "http://minio:9000"
)
SELECT * FROM sales_records;
```

### Unload to local files using NFS

To access the files in NFS via the `file://` protocol, you need to mount a NAS device as NFS under the same directory of each BE or CN node.

Example:

```SQL
-- Unload data into CSV files.
INSERT INTO FILES(
  'path' = 'file:///home/ubuntu/csvfile/', 
  'format' = 'csv', 
  'csv.column_separator' = ',', 
  'csv.row_delimitor' = '\n'
)
SELECT * FROM sales_records;

-- Unload data into Parquet files.
INSERT INTO FILES(
  'path' = 'file:///home/ubuntu/parquetfile/',
   'format' = 'parquet'
)
SELECT * FROM sales_records;
```

## See also

- For more instructions on the usage of INSERT, see [SQL reference - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md).
- For more instructions on the usage of FILES(), see [SQL reference - FILES()](../sql-reference/sql-functions/table-functions/files.md).
