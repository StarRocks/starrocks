# EXPORT

## Description

Exports the data of a table to a specified location.

This is an asynchronous operation. The export result is returned after you submit the export task. You can use [SHOW EXPORT](../../../sql-reference/sql-statements/data-manipulation/SHOW_EXPORT.md) to view the progress of the export task.

> **NOTICE**
>
> You can export data out of StarRocks tables only as a user who has the EXPORT privilege on those StarRocks tables. If you do not have the EXPORT privilege, follow the instructions provided in [GRANT](../account-management/GRANT.md) to grant the EXPORT privilege to the user that you use to connect to your StarRocks cluster.

## Syntax

```SQL
EXPORT TABLE <table_name>
[PARTITION (<partition_name>[, ...])]
[(<column_name>[, ...])]
TO <export_path>
[opt_properties]
WITH BROKER
[broker_properties]
```

## Parameters

- `table_name`

  The name of the table. StarRocks supports exporting the data of tables whose `engine` is `olap` or `mysql`.

- `partition_name`

  The partitions from which you want to export data. By default, if you do not specify this parameter, StarRocks exports the data from all partitions of the table.

- `column_name`

  The columns from which you want to export data. The sequence of columns that you specify by using this parameter can differ from the schema of the table. By default, if you do not specify this parameter, StarRocks exports the data from all columns of the table.

- `export_path`

  The location to which you want to export the data of the table. If the location contains a path, make sure that the path ends with a slash (/). Otherwise, the part following the last slash (/) in the path will be used as the prefix to the name of the exported file. By default, `data_` is used as the file name prefix if no file name prefix is specified.

- `opt_properties`

  Optional properties that you can configure for the export task.

  Syntax:

  ```SQL
  [PROPERTIES ("<key>"="<value>", ...)]
  ```

  | **Property**     | **Description**                                              |
  | ---------------- | ------------------------------------------------------------ |
  | column_separator | The column separator that you want to use in the exported file. Default value: `\t`. |
  | line_delimiter   | The row separator that you want to use in the exported file. Default value: `\n`. |
  | load_mem_limit   | The maximum memory that is allowed for the export task on each individual BE. Unit: bytes. The default maximum memory is 2 GB. |
  | timeout          | The amount of time after which the export task times out. Unit: second. Default value: `86400`, meaning 1 day. |
  | include_query_id | Specifies whether the name of the exported file contains `query_id`. Valid values: `true` and `false`. The value `true` specifies that the file name contains `query_id`, and the value `false` specifies that the file name does not contain `query_id`. |

- `WITH BROKER`

  In v2.4 and earlier, input `WITH BROKER "<broker_name>"` to specify the broker you want to use. From v2.5 onwards, you no longer need to specify a broker, but you still need to retain the `WITH BROKER` keyword. For more information, see [Export data using EXPORT > Background information](../../../unloading/Export.md#background-information).

- `broker_properties`

  The information that is used to authenticate the source data. The authentication information varies depending on the data source. For more information, see [BROKER LOAD](../../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md).

## Examples

### Export all data of a table to HDFS

The following example exports all data of the `testTbl` table to the `hdfs://<hdfs_host>:<hdfs_port>/a/b/c/` path of an HDFS cluster:

```SQL
EXPORT TABLE testTbl 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/" 
WITH BROKER
(
        "username"="xxx",
        "password"="yyy"
);
```

### Export the data of specified partitions of a table to HDFS

The following example exports the data of two partitions, `p1` and `p2`, of the `testTbl` table to the `hdfs://<hdfs_host>:<hdfs_port>/a/b/c/` path of an HDFS cluster:

```SQL
EXPORT TABLE testTbl
PARTITION (p1,p2) 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/" 
WITH BROKER
(
        "username"="xxx",
        "password"="yyy"
);
```

### Export all data of a table to HDFS with column separator specified

The following example exports all data of the `testTbl` table to the `hdfs://<hdfs_host>:<hdfs_port>/a/b/c/` path of an HDFS cluster and specifies that commas (`,`) is used as the column separator:

```SQL
EXPORT TABLE testTbl 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/" 
PROPERTIES
(
    "column_separator"=","
) 
WITH BROKER
(
    "username"="xxx",
    "password"="yyy"
);
```

The following example exports all data of the `testTbl` table to the `hdfs://<hdfs_host>:<hdfs_port>/a/b/c/` path of an HDFS cluster and specifies that `\x01` (the default column separator supported by Hive) is used as the column separator:

```SQL
EXPORT TABLE testTbl 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/" 
PROPERTIES
(
    "column_separator"="\\x01"
) 
WITH BROKER;
```

### Export all data of a table to HDFS with file name prefix specified

The following example exports all data of the `testTbl` table to the `hdfs://<hdfs_host>:<hdfs_port>/a/b/c/` path of an HDFS cluster and specifies that `testTbl_` is used as the prefix to the name of the exported file:

```SQL
EXPORT TABLE testTbl 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/testTbl_" 
WITH BROKER;
```

### Export data to AWS S3

The following example exports all data of the `testTbl` table to the `s3-package/export/` path of an AWS S3 bucket:

```SQL
EXPORT TABLE testTbl 
TO "s3a://s3-package/export/"
WITH BROKER
(
    "aws.s3.access_key" = "xxx",
    "aws.s3.secret_key" = "yyy",
    "aws.s3.region" = "zzz"
);
```
