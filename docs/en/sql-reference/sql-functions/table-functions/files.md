---
displayed_sidebar: "English"
---

# FILES

## Description

Reads a data file from cloud storage. FILES() accesses cloud storage with the path-related properties of the file, infers the table schema of the data in the file, and returns the data rows. You can directly query the data rows using [SELECT](../../sql-statements/data-manipulation/SELECT.md), load the data rows into an existing table using [INSERT](../../sql-statements/data-manipulation/INSERT.md), or create a new table and load the data rows into it using [CREATE TABLE AS SELECT](../../sql-statements/data-definition/CREATE_TABLE_AS_SELECT.md). This feature is supported from v3.1.0 onwards.

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
  | fs.s3a.endpoint   | Yes          | The endpoint that you can use to access the GCS bucket. Example: `storage.googleapis.com`. |

- Use Shared Key to access Azure Blob Storage:

  ```SQL
  "azure.blob.storage_account" = "<storage_account>",
  "azure.blob.shared_key" = "<shared_key>"
  ```

  | **Key**                    | **Required** | **Description**                                              |
  | -------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account | Yes          | The name of the Azure Blob Storage account.                  |
  | azure.blob.shared_key      | Yes          | The Shared Key that you can use to access the Azure Blob Storage account. |

### columns_from_path

From v3.2 onwards, StarRocks can extract the value of a key/value pair from the file path as the value of a column.

```SQL
"columns_from_path" = "<column_name> [, ...]"
```

Suppose the data file **file1** is stored under a path in the format of `/geo/country=US/city=LA/`. You can specify the `columns_from_path` parameter as `"columns_from_path" = "country, city"` to extract the geographic information in the file path as the value of columns that are returned. For further instructions, see Example 4.

### unload_data_param

From v3.2 onwards, FILES() supports defining writable files in remote storage for data unloading. For detailed instructions, see [Unload data using INSERT INTO FILES](../../../unloading/unload_using_insert_into_files.md).

```sql
-- Supported from v3.2 onwards.
unload_data_param::=
    "compression" = "<compression_method>",
    "partition_by" = "<column_name> [, ...]",
    "single" = { "true" | "false" } ,
    "target_max_file_size" = "<int>"
```

| **Key**          | **Required** | **Description**                                              |
| ---------------- | ------------ | ------------------------------------------------------------ |
| compression      | Yes          | The compression method to use when unloading data. Valid values:<ul><li>`uncompressed`: No compression algorithm is used.</li><li>`gzip`: Use the gzip compression algorithm.</li><li>`snappy`: Use the SNAPPY compression algorithm.</li><li>`zstd`: Use the Zstd compression algorithm.</li><li>`lz4`: Use the LZ4 compression algorithm.</li></ul>                  |
| partition_by     | No           | The list of columns that are used to partition data files into different storage paths. Multiple columns are separated by commas (,). FILES() extracts the key/value information of the specified columns and stores the data files under the storage paths featured with the extracted key/value pair. For further instructions, see Example 5. |
| single           | No           | Whether to unload the data into a single file. Valid values:<ul><li>`true`: The data is stored in a single data file.</li><li>`false` (Default): The data is stored in multiple files if the amount of data unloaded exceeds 512 MB.</li></ul>                  |
| target_max_file_size | No           | The best-effort maximum size of each file in the batch to be unloaded. Unit: Bytes. Default value: 1073741824 (1 GB). When the size of data to be unloaded exceeds this value, the data will be divided into multiple files, and the size of each file will not significantly exceed this value. Introduced in v3.2.7. |

## Return

When used with SELECT, FILES() returns the data in the file as a table.

- When querying CSV files, you can use `$1`, `$2` ... to represent each column in the SELECT statement, or specify `*` to obtain data from all columns.

  ```SQL
  SELECT * FROM FILES(
      "path" = "s3://inserttest/csv/file1.csv",
      "format" = "csv",
      "csv.column_separator"=",",
      "csv.row_delimiter"="\n",
      "csv.enclose"='"',
      "csv.skip_header"="1",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  )
  WHERE $1 > 5;
  +------+---------+------------+
  | $1   | $2      | $3         |
  +------+---------+------------+
  |    6 | 0.34413 | 2017-11-25 |
  |    7 | 0.40055 | 2017-11-26 |
  |    8 | 0.42437 | 2017-11-27 |
  |    9 | 0.67935 | 2017-11-27 |
  |   10 | 0.22783 | 2017-11-29 |
  +------+---------+------------+
  5 rows in set (0.30 sec)

  SELECT $1, $2 FROM FILES(
      "path" = "s3://inserttest/csv/file1.csv",
      "format" = "csv",
      "csv.column_separator"=",",
      "csv.row_delimiter"="\n",
      "csv.enclose"='"',
      "csv.skip_header"="1",
      "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
      "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
      "aws.s3.region" = "us-west-2"
  );
  +------+---------+
  | $1   | $2      |
  +------+---------+
  |    1 | 0.71173 |
  |    2 | 0.16145 |
  |    3 | 0.80524 |
  |    4 | 0.91852 |
  |    5 | 0.37766 |
  |    6 | 0.34413 |
  |    7 | 0.40055 |
  |    8 | 0.42437 |
  |    9 | 0.67935 |
  |   10 | 0.22783 |
  +------+---------+
  10 rows in set (0.38 sec)
  ```

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

## Usage notes

From v3.2 onwards, FILES() further supports complex data types including ARRAY, JSON, MAP, and STRUCT in addition to basic data types.

>>>>>>> a1095b33d2 ([Doc] Add Returns to Files (#48880))
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
