---
displayed_sidebar: docs
---

# FILES

## Description

Reads a data file from cloud storage. FILES() accesses cloud storage with the path-related properties of the file, infers the table schema of the data in the file, and returns the data rows. You can directly query the data rows using [SELECT](../../sql-statements/table_bucket_part_index/SELECT.md), load the data rows into an existing table using [INSERT](../../sql-statements/loading_unloading/INSERT.md), or create a new table and load the data rows into it using [CREATE TABLE AS SELECT](../../sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md). This feature is supported from v3.1.0 onwards.

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
  "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
  "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "aws.s3.region" = "<s3_region>"
  ```

  | **Key**           | **Required** | **Description**                                              |
  | ----------------- | ------------ | ------------------------------------------------------------ |
  | aws.s3.access_key | Yes          | The Access Key ID that you can use to access the Amazon S3 bucket. |
  | aws.s3.secret_key | Yes          | The Secret Access Key that you can use to access the Amazon S3 bucket. |
  | aws.s3.region     | Yes          | The region in which your AWS S3 bucket resides. Example: `us-west-2`. |

<<<<<<< HEAD
## Return
=======
- Use the IAM user-based authentication to access GCS:

  ```SQL
  "fs.s3a.access.key" = "AAAAAAAAAAAAAAAAAAAA",
  "fs.s3a.secret.key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
  "fs.s3a.endpoint" = "<gcs_endpoint>"
  ```

  | **Key**           | **Required** | **Description**                                              |
  | ----------------- | ------------ | ------------------------------------------------------------ |
  | fs.s3a.access.key | Yes          | The Access Key ID that you can use to access the GCS bucket. |
  | fs.s3a.secret.key | Yes          | The Secret Access Key that you can use to access the GCS bucket.|
  | fs.s3a.endpoint   | Yes          | The endpoint that you can use to access the GCS bucket. Example: `storage.googleapis.com`. Do not specify `https` in the endpoint address. |

- Use Shared Key to access Azure Blob Storage:

  ```SQL
  "azure.blob.storage_account" = "<storage_account>",
  "azure.blob.shared_key" = "<shared_key>"
  ```

  | **Key**                    | **Required** | **Description**                                              |
  | -------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account | Yes          | The name of the Azure Blob Storage account.                  |
  | azure.blob.shared_key      | Yes          | The Shared Key that you can use to access the Azure Blob Storage account. |

#### columns_from_path

From v3.2 onwards, StarRocks can extract the value of a key/value pair from the file path as the value of a column.

```SQL
"columns_from_path" = "<column_name> [, ...]"
```

Suppose the data file **file1** is stored under a path in the format of `/geo/country=US/city=LA/`. You can specify the `columns_from_path` parameter as `"columns_from_path" = "country, city"` to extract the geographic information in the file path as the value of columns that are returned. For further instructions, see Example 4.

### Return

#### SELECT FROM FILES()
>>>>>>> 2572c1e5cb ([Doc] Forbid https in GCS endpoint (#52285))

When used with SELECT, FILES() returns the data in the file as a table.

- When querying Parquet or ORC files, you can directly specify the name of the desired columns in the SELECT statement, or specify `*` to obtain data from all columns.

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://inserttest/parquet/file2.parquet",
      "format" = "parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  )
  WHERE c1 IN (101,105);
  +------+------+---------------------+
  | c1   | c2   | c3                  |
  +------+------+---------------------+
  |  101 |    9 | 2018-05-15T18:30:00 |
  |  105 |    6 | 2018-05-15T18:30:00 |
  +------+------+---------------------+
  2 rows in set (0.29 sec)

  SELECT c1, c3 FROM FILES(
      "path" = "s3://inserttest/parquet/file2.parquet",
      "format" = "parquet",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  );
  +------+---------------------+
  | c1   | c3                  |
  +------+---------------------+
  |  101 | 2018-05-15T18:30:00 |
  |  102 | 2018-05-15T18:30:00 |
  |  103 | 2018-05-15T18:30:00 |
  |  104 | 2018-05-15T18:30:00 |
  |  105 | 2018-05-15T18:30:00 |
  |  106 | 2018-05-15T18:30:00 |
  |  107 | 2018-05-15T18:30:00 |
  |  108 | 2018-05-15T18:30:00 |
  |  109 | 2018-05-15T18:30:00 |
  |  110 | 2018-05-15T18:30:00 |
  +------+---------------------+
  10 rows in set (0.55 sec)
  ```

## Examples

Example 1: Query the data from the Parquet file **parquet/par-dup.parquet** within the AWS S3 bucket `inserttest`:

```Plain
MySQL > SELECT * FROM FILES(
     "path" = "s3://inserttest/parquet/par-dup.parquet",
     "format" = "parquet",
     "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
     "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
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
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
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
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.region" = "us-west-2"
);
Query OK, 2 rows affected (22.09 sec)
{'label':'insert_1a217d70-2f52-11ee-9e4a-7a563fb695da', 'status':'VISIBLE', 'txnId':'3248'}
```
