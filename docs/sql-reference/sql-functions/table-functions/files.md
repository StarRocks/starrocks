# FILES

## Description

Reads a data file from an external data source for direct query or loading data using [INSERT](../../sql-statements/data-manipulation/insert.md). This feature is supported from v3.1.0 onwards.

Currently, the FILES() function supports the following data sources and file formats:

- **Data sources:**

  - AWS S3

- **File formats:**

  - Parquet
  - ORC

## Syntax

```SQL
FILES("key" = "value"...)
```

## Parameters

All parameters are in the `"key" = "value"` pairs.

| **Key**           | **Description**                                              |
| ----------------- | ------------------------------------------------------------ |
| path              | The URI used to access the file. Example: `s3://testbucket/parquet/test.parquet`. |
| format            | The format of the data file. Valid values: `parquet` and `orc`. |
| aws.s3.access_key | The Access Key ID that you can use to access the Amazon S3 bucket. |
| aws.s3.secret_key | The Secret Access Key that you can use to access the Amazon S3 bucket. |
| aws.s3.region     | The region in which your AWS S3 bucket resides. Example: `us-west-1`. |

## Examples

Example 1: Query the data from the Parquet file **parquet/par-dup.parquet** within the AWS S3 bucket `inserttest`:

```Plain
MySQL > SELECT * FROM FILES(
     "path" = "s3://inserttest/parquet/par-dup.parquet",
     "format" = "parquet",
     "aws.s3.access_key" = "XXXXXXXXXX",
     "aws.s3.secret_key" = "YYYYYYYYYY",
     "aws.s3.region" = "ap-southeast-1"
);
+------+---------------------------------------------------------+
| c1   | c2                                                      |
+------+---------------------------------------------------------+
|    1 | {"1": "key", "1": "1", "111": "1111", "111": "aaaa"}    |
|    2 | {"2": "key", "2": "NULL", "222": "2222", "222": "bbbb"} |
+------+---------------------------------------------------------+
2 rows in set (22.335 sec)
```

Example 2: Insert the data rows from the Parquet file **parquet/insert_wiki_edit_append.parquet** within the AWS S3 bucket `inserttest` into the table `insert_wiki_edit`:

```Plain
MySQL > INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "ap-southeast-1"
);
Query OK, 2 rows affected (23.03 sec)
{'label':'insert_d8d4b2ee-ac5c-11ed-a2cf-4e1110a8f63b', 'status':'VISIBLE', 'txnId':'2440'}
```
