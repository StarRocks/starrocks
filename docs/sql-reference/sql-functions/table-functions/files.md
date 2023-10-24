# FILES

## Description

Defines data files in remote storage.

From v3.1.0 onwards, StarRocks supports defining read-only files in remote storage using the table function FILES(). It can access remote storage with the path-related properties of the files, infers the table schema of the data in the files, and returns the data rows. You can directly query the data rows using [SELECT](../../sql-statements/data-manipulation/SELECT.md), load the data rows into an existing table using [INSERT](../../sql-statements/data-manipulation/insert.md), or create a new table and load the data rows into it using [CREATE TABLE AS SELECT](../../sql-statements/data-definition/CREATE_TABLE_AS_SELECT.md).

From v3.2.0 onwards, FILES() supports defining writable data files in remote storage. You can [use INSERT INTO FILES() to unload data from StarRocks to remote storage](../../../unloading/unload_using_insert_into_files.md).

Currently, the FILES() function supports the following data sources and file formats:

- **Data sources:**
  - HDFS
  - AWS S3
  - Google Cloud Storage
  - Other S3-compatible storage system
  - Microsoft Azure Blob Storage
- **File formats:**
  - Parquet
  - ORC (Currently not supported for unloading data)

## Syntax

```SQL
FILES( data_location , data_format [, StorageCredentialParams ] 
    [, columns_from_path ] [, schema_detect ] [, unload_data ] )

data_location ::=
    "path" = { "hdfs://<hdfs_host>:<hdfs_port>/<hdfs_path>"
             | "s3://<s3_path>" 
             | "s3a://<gcs_path>" 
             | "wasb://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
             | "wasbs://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
             }

data_format ::=
    "format" = { "parquet" | "orc" }


-- Supported from v3.2 onwards.
columns_from_path ::=
    "columns_from_path" = "<column_name> [, ...]"


-- Supported from v3.2 onwards.
schema_detect::=
    [ "schema_auto_detect_sample_rows" = "<INT>" ]
    [, "schema_auto_detect_sample_files" = "<INT>" ]

-- Supported from v3.2 onwards.
unload_data::=
    "compression" = "<compression_method>"
    [, "max_file_size" = "<file_size>" ]
    [, "partition_by" = "<column_name> [, ...]" ]
    [, "single" = { "true" | "false" } ]
```

## Parameters

All parameters are in the `"key" = "value"` pairs.

### data_location

The URI used to access the files. You can specify a path or a file.

- To access HDFS, you need to specify this parameter as:

  ```SQL
  "path" = "hdfs://<hdfs_host>:<hdfs_port>/<hdfs_path>"
  -- Example: "path" = "hdfs://127.0.0.1:9000/path/file.parquet"
  ```

- To access AWS S3:

  - If you use the S3 protocol, you need to specify this parameter as:

    ```SQL
    "path" = "s3://<s3_path>"
    -- Example: "path" = "s3://path/file.parquet"
    ```

  - If you use the S3A protocol, you need to specify this parameter as:

    ```SQL
    "path" = "s3a://<s3_path>"
    -- Example: "path" = "s3a://path/file.parquet"
    ```

- To access Google Cloud Storage, you need to specify this parameter as:

  ```SQL
  "path" = "s3q://<gcs_path>"
  -- Example: "path" = "s3a://path/file.parquet"
  ```

- To access Azure Blob Storage:

  - If your storage account allows access over HTTP, you need to specify this parameter as:

    ```SQL
    "path" = "wasb://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
    -- Example: "path" = "wasb://testcontainer@testaccount.blob.core.windows.net/path/file.parquet"
    ```
  
  - If your storage account allows access over HTTPS, you need to specify this parameter as:

    ```SQL
    "path" = "wasbs://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
    -- Example: "path" = "wasbs://testcontainer@testaccount.blob.core.windows.net/path/file.parquet"
    ```

### data_format

The format of the data file. Valid values: `parquet` and `orc`.

### StorageCredentialParams

The authentication information used by StarRocks to access your storage system.

StarRocks currently supports accessing HDFS with the simple authentication, accessing AWS S3 and GCS with the IAM user-based authentication, and accessing Azure Blob Storage with Shared Key.

- Use the simple authentication to access HDFS:

  ```SQL
  "hadoop.security.authentication" = "simple",
  "username" = "xxxxxxxxxx",
  "password" = "yyyyyyyyyy"
  ```

  | **Key**                        | **Required** | **Description**                                              |
  | ------------------------------ | ------------ | ------------------------------------------------------------ |
  | hadoop.security.authentication | No           | The authentication method. Valid value: `simple` (Default). `simple` represents simple authentication, meaning no authentication. |
  | username                       | Yes          | The username of the account that you want to use to access the NameNode of the HDFS cluster. |
  | password                       | Yes          | The password of the account that you want to use to access the NameNode of the HDFS cluster. |

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

- Use the IAM user-based authentication to access GCS:

  ```SQL
  "fs.s3a.access.key" = "xxxxxxxxxx",
  "fs.s3a.secret.key" = "yyyyyyyyyy",
  "fs.s3a.endpoint" = "<gcs_endpoint>"
  ```

  | **Key**           | **Required** | **Description**                                              |
  | ----------------- | ------------ | ------------------------------------------------------------ |
  | fs.s3a.access.key | Yes          | The Access Key ID that you can use to access the GCS bucket. |
  | fs.s3a.secret.key | Yes          | The Secret Access Key that you can use to access the GCS bucket.|
  | fs.s3a.endpoint   | Yes          | The endpoint that you can use to access the GCS bucket. Example: `storage.googleapis.com`。 |

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

### schema_detect

From v3.2 onwards, FILES() supports automatic schema detection and unionization of the same batch of data files. StarRocks first detects the schema of the data by sampling certain data rows of a random data file in the batch. Then, StarRocks unionizes the columns from all the data files in the batch.

You can configure the sampling rule using the following parameters:

- `schema_auto_detect_sample_rows`: the number of data rows to scan in each sampled data file. Range: [-1, 500]. If this parameter is set to `-1`, all data rows are scanned. 
- `schema_auto_detect_sample_files`: the number of random data files to sample in each batch. Valid values: `1` (default) and `-1`. If this parameter is set to `-1`, all data files are scanned.

After the sampling, StarRocks unionizes the columns from all the data files according to these rules:

- For columns with different column names or indices, each column is identified as an individual column, and, eventually, the union of all individual columns is returned.
- For columns with the same column name but different data types, they are identified as the same column but with a more general data type. For example, if the column `col1` in file A is INT but DECIMAL in file B, DOUBLE is used in the returned column. The STRING type can be used to unionize all data types.

If StarRocks fails to unionize all the columns, it generates a schema error report that includes the error information and all the file schemas.

> **CAUTION**
>
> All data files in a single batch must be of the same file format.

### unload_data

From v3.2 onwards, FILES() supports defining writable files in remote storage for data unloading. For detailed instructions, see [Unload data using INSERT INTO FILES](../../../unloading/unload_using_insert_into_files.md).

- `compression` (Required): The compression method to use when unloading data. Valid values:
  - `uncompressed`: No compression algorithm is used.
  - `gzip`: Use the gzip compression algorithm.
  - `brotli`: Use the Brotli compression algorithm.
  - `zstd`: Use the Zstd compression algorithm.
  - `lz4`: Use the LZ4 compression algorithm.
- `max_file_size`: The maximum size of each data file when data is unloaded into multiple files. Default value: `1GB`. Unit: B, KB, MB, GB, TB, and PB.
- `partition_by`: The list of columns that are used to partition data files into different storage paths. FILES() extracts the key/value information of the specified columns and stores the data files under the storage paths featured with the extracted key/value pair. For further instructions, see Example 5.
- `single`: Whether to unload the data into a single file.  Valid values:
  - `true`: The data is stored in a single data file.
  - `false`: The data is stored in multiple files if `max_file_size` is reached.

> **CAUTION**
>
> You cannot specify both `max_file_size` and `single`.

## Usage notes

From v3.2 onwards, FILES() further supports complex data types including ARRAY, JSON, MAP, and STRUCT in addition to basic data types.

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

Example 4: Query the data from the Parquet file **/geo/country=US/city=LA/file1.parquet** (which only contains two columns -`id` and `user`), and extract the key/value information in its path as columns returned.

```Plain
SELECT * FROM FILES(
    "path" = "hdfs://xxx.xx.xxx.xx:9000/geo/country=US/city=LA/file1.parquet",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "xxxxx",
    "password" = "xxxxx",
    "columns_from_path" = "country, city"
);
+------+---------+---------+------+
| id   | user    | country | city |
+------+---------+---------+------+
|    1 | richard | US      | LA   |
|    2 | amber   | US      | LA   |
+------+---------+---------+------+
2 rows in set (3.84 sec)
```

Example 5: Unload all data rows in `sales_records` as multiple Parquet files under the path **/unload/partitioned/** in the HDFS cluster. These files are stored in different subpaths distinguished by the values in the column `sales_time`.

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
