# FILES

## Description

Reads a data file from cloud storage. FILE() accesses the cloud storage with the path-related properties of the file, infers the table schema of the data in the file, and returns the data rows. You can directly query the data rows using [SELECT](../../sql-statements/data-manipulation/SELECT.md), load the data rows into an existing table using [INSERT](../../sql-statements/data-manipulation/insert.md), or create a new table and load the data rows into it using [CREATE TABLE AS SELECT](../../sql-statements/data-definition/CREATE%20TABLE%20AS%20SELECT.md). This feature is supported from v3.1.0 onwards.

Currently, the FILES() function supports the following data sources and file formats:

- **Data sources:**

  - AWS S3

- **File formats:**

  - Parquet
  - ORC

## Syntax

```SQL
FILES( data_location , data_format [, StorageCredentialParams ] )

data_location ::=
    "path" = "s3://<s3_path>"

data_format ::=
    "format" = "{parquet | orc}"
```

## Parameters

All parameters are in the `"key" = "value"` pairs.

| **Key** | **Required** | **Description**                                              |
| ------- | ------------ | ------------------------------------------------------------ |
| path    | Yes          | The URI used to access the file. Example: `s3://testbucket/parquet/test.parquet`. |
| format  | Yes          | The format of the data file. Valid values: `parquet` and `orc`. |

### StorageCredentialParams

The authentication information used by StarRocks to access your storage system.

- Use the IAM user-based authentication to access AWS S3:

  ```SQL
  "aws.s3.access_key" = "xxxxxxxxxx",
  "aws.s3.secret_key" = "yyyyyyyyyy",
  "aws.s3.region" = "<s3_region>"
  ```

  | **Key**           | **Required** | **Description**                                              |
  | ----------------- | ------------ | ------------------------------------------------------------ |
  | aws.s3.access_key | Yes          | The Access Key ID that you can use to access the Amazon S3 bucket. |
  | aws.s3.secret_key | Yes          | The Secret Access Key that you can use to access the Amazon S3 bucket. |
  | aws.s3.region     | Yes          | The region in which your AWS S3 bucket resides. Example: `us-west-2`. |

## Examples

Example 1: Query the data from the Parquet file **parquet/par-dup.parquet** within the AWS S3 bucket `inserttest`:

```Plain
MySQL > SELECT * FROM FILES(
     "path" = "s3://inserttest/parquet/par-dup.parquet",
     "format" = "parquet",
     "aws.s3.access_key" = "XXXXXXXXXX",
     "aws.s3.secret_key" = "YYYYYYYYYY",
     "aws.s3.region" = "us-west-2"
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
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (23.03 sec)
{'label':'insert_d8d4b2ee-ac5c-11ed-a2cf-4e1110a8f63b', 'status':'VISIBLE', 'txnId':'2440'}
```

Example 3: Create a table named `ctas_wiki_edit` and insert the data rows from the Parquet file **parquet/insert_wiki_edit_append.parquet** within the AWS S3 bucket `inserttest` into the table:

```Plain
MySQL > CREATE TABLE ctas_wiki_edit AS
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (22.09 sec)
{'label':'insert_1a217d70-2f52-11ee-9e4a-7a563fb695da', 'status':'VISIBLE', 'txnId':'3248'}
```
