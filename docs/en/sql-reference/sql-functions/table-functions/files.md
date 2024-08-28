---
displayed_sidebar: docs
---

# FILES

## Description

Defines data files in remote storage.

From v3.1.0 onwards, StarRocks supports defining read-only files in remote storage using the table function FILES(). It can access remote storage with the path-related properties of the files, infers the table schema of the data in the files, and returns the data rows. You can directly query the data rows using [SELECT](../../sql-statements/table_bucket_part_index/SELECT.md), load the data rows into an existing table using [INSERT](../../sql-statements/loading_unloading/INSERT.md), or create a new table and load the data rows into it using [CREATE TABLE AS SELECT](../../sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md).

From v3.2.0 onwards, FILES() supports writing data into files in remote storage. You can [use INSERT INTO FILES() to unload data from StarRocks to remote storage](../../../unloading/unload_using_insert_into_files.md).

Currently, the FILES() function supports the following data sources and file formats:

- **Data sources:**
  - HDFS
  - AWS S3
  - Google Cloud Storage
  - Other S3-compatible storage system
  - Microsoft Azure Blob Storage
- **File formats:**
  - Parquet
  - ORC
  - CSV

## Syntax

- **Data loading**:

  ```SQL
  FILES( data_location , data_format [, schema_detect ] [, StorageCredentialParams ] [, columns_from_path ] )
  ```

- **Data unloading**:

  ```SQL
  FILES( data_location , data_format [, StorageCredentialParams ] , unload_data_param )
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
  "path" = "s3a://<gcs_path>"
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

The format of the data file. Valid values: `parquet`, `orc`, and `csv`.

You must set detailed options for specific data file formats.

#### CSV

Example for the CSV format:

```SQL
"format"="csv",
"csv.column_separator"="\\t",
"csv.enclose"='"',
"csv.skip_header"="1",
"csv.escape"="\\"
```

##### csv.column_separator

Specifies the column separator used when the data file is in CSV format. If you do not specify this parameter, this parameter defaults to `\\t`, indicating tab. The column separator you specify using this parameter must be the same as the column separator that is actually used in the data file. Otherwise, the load job will fail due to inadequate data quality.

Tasks that use Files() are submitted according to the MySQL protocol. StarRocks and MySQL both escape characters in the load requests. Therefore, if the column separator is an invisible character such as tab, you must add a backslash (`\`) preceding the column separator. For example, you must input `\\t` if the column separator is `\t`, and you must input `\\n` if the column separator is `\n`. Apache Hive™ files use `\x01` as their column separator, so you must input `\\x01` if the data file is from Hive.

> **NOTE**
>
> - For CSV data, you can use a UTF-8 string, such as a comma (,), tab, or pipe (|), whose length does not exceed 50 bytes as a text delimiter.
> - Null values are denoted by using `\N`. For example, a data file consists of three columns, and a record from that data file holds data in the first and third columns but no data in the second column. In this situation, you need to use `\N` in the second column to denote a null value. This means the record must be compiled as `a,\N,b` instead of `a,,b`. `a,,b` denotes that the second column of the record holds an empty string.

##### csv.enclose

Specifies the character that is used to wrap the field values in the data file according to RFC4180 when the data file is in CSV format. Type: single-byte character. Default value: `NONE`. The most prevalent characters are single quotation mark (`'`) and double quotation mark (`"`).

All special characters (including row separators and column separators) wrapped by using the `enclose`-specified character are considered normal symbols. StarRocks can do more than RFC4180 as it allows you to specify any single-byte character as the `enclose`-specified character.

If a field value contains an `enclose`-specified character, you can use the same character to escape that `enclose`-specified character. For example, you set `enclose` to `"`, and a field value is `a "quoted" c`. In this case, you can enter the field value as `"a ""quoted"" c"` into the data file.

##### csv.skip_header

Specifies whether to skip the first rows of the data file when the data file is in CSV format. Type: INTEGER. Default value: `0`.

In some CSV-formatted data files, the first rows at the beginning are used to define metadata such as column names and column data types. By setting the `skip_header` parameter, you can enable StarRocks to skip the first rows of the data file during data loading. For example, if you set this parameter to `1`, StarRocks skips the first row of the data file during data loading.
The first rows at the beginning in the data file must be separated by using the row separator that you specify in the load statement.

##### csv.escape

Specifies the character that is used to escape various special characters, such as row separators, column separators, escape characters, and `enclose`-specified characters, which are then considered by StarRocks to be common characters and are parsed as part of the field values in which they reside. Type: single-byte character. Default value: `NONE`. The most prevalent character is slash (`\`), which must be written as double slashes (`\\`) in SQL statements.

> **NOTE**
>
> The character specified by `escape` is applied to both inside and outside of each pair of `enclose`-specified characters.
> Two examples are as follows:
> - When you set `enclose` to `"` and `escape` to `\`, StarRocks parses `"say \"Hello world\""` into `say "Hello world"`.
> - Assume that the column separator is comma (`,`). When you set `escape` to `\`, StarRocks parses `a, b\, c` into two separate field values: `a` and `b, c`.

### schema_detect

From v3.2 onwards, FILES() supports automatic schema detection and unionization of the same batch of data files. StarRocks first detects the schema of the data by sampling certain data rows of a random data file in the batch. Then, StarRocks unionizes the columns from all the data files in the batch.

You can configure the sampling rule using the following parameters:

- `auto_detect_sample_files`: the number of random data files to sample in each batch. Range: [0, + ∞]. Default: `1`.
- `auto_detect_sample_rows`: the number of data rows to scan in each sampled data file. Range: [0, + ∞]. Default: `500`.

After the sampling, StarRocks unionizes the columns from all the data files according to these rules:

- For columns with different column names or indices, each column is identified as an individual column, and, eventually, the union of all individual columns is returned.
- For columns with the same column name but different data types, they are identified as the same column but with a general data type on a relative fine granularity level. For example, if the column `col1` in file A is INT but DECIMAL in file B, DOUBLE is used in the returned column.
  - All integer columns will be unionized as an integer type on an overall rougher granularity level.
  - Integer columns together with FLOAT type columns will be unionized as the DECIMAL type.
  - String types are used for unionizing other types.
- Generally, the STRING type can be used to unionize all data types.

You can refer to [Example 6](#example-6).

If StarRocks fails to unionize all the columns, it generates a schema error report that includes the error information and all the file schemas.

> **CAUTION**
>
> All data files in a single batch must be of the same file format.

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
  "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
  "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "aws.s3.region" = "<s3_region>"
  ```

  | **Key**           | **Required** | **Description**                                              |
  | ----------------- | ------------ | ------------------------------------------------------------ |
  | aws.s3.access_key | Yes          | The Access Key ID that you can use to access the Amazon S3 bucket. |
  | aws.s3.secret_key | Yes          | The Secret Access Key that you can use to access the Amazon S3 bucket. |
  | aws.s3.region     | Yes          | The region in which your AWS S3 bucket resides. Example: `us-west-2`. |

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

## Examples

#### Example 1

Query the data from the Parquet file **parquet/par-dup.parquet** within the AWS S3 bucket `inserttest`:

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

#### Example 2

Insert the data rows from the Parquet file **parquet/insert_wiki_edit_append.parquet** within the AWS S3 bucket `inserttest` into the table `insert_wiki_edit`:

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

#### Example 3

Create a table named `ctas_wiki_edit` and insert the data rows from the Parquet file **parquet/insert_wiki_edit_append.parquet** within the AWS S3 bucket `inserttest` into the table:

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

#### Example 4

Query the data from the Parquet file **/geo/country=US/city=LA/file1.parquet** (which only contains two columns -`id` and `user`), and extract the key/value information in its path as columns returned.

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

#### Example 5

Unload all data rows in `sales_records` as multiple Parquet files under the path **/unload/partitioned/** in the HDFS cluster. These files are stored in different subpaths distinguished by the values in the column `sales_time`.

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

#### Example 6

Automatic schema detection and Unionization.

The following example is based on two Parquet files in the S3 bucket:

- File 1 contains three columns - INT column `c1`, FLOAT column `c2`, and DATE column `c3`.

```Plain
c1,c2,c3
1,0.71173,2017-11-20
2,0.16145,2017-11-21
3,0.80524,2017-11-22
4,0.91852,2017-11-23
5,0.37766,2017-11-24
6,0.34413,2017-11-25
7,0.40055,2017-11-26
8,0.42437,2017-11-27
9,0.67935,2017-11-27
10,0.22783,2017-11-29
```

- File 2 contains three columns - INT column `c1`, INT column `c2`, and DATETIME column `c3`.

```Plain
c1,c2,c3
101,9,2018-05-15T18:30:00
102,3,2018-05-15T18:30:00
103,2,2018-05-15T18:30:00
104,3,2018-05-15T18:30:00
105,6,2018-05-15T18:30:00
106,1,2018-05-15T18:30:00
107,8,2018-05-15T18:30:00
108,5,2018-05-15T18:30:00
109,6,2018-05-15T18:30:00
110,8,2018-05-15T18:30:00
```

Use a CTAS statement to create a table named `test_ctas_parquet` and insert the data rows from the two Parquet files into the table:

```SQL
CREATE TABLE test_ctas_parquet AS
SELECT * FROM FILES(
    "path" = "s3://inserttest/parquet/*",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);
```

View the table schema of `test_ctas_parquet`:

```SQL
SHOW CREATE TABLE test_ctas_parquet\G
```

```Plain
*************************** 1. row ***************************
       Table: test_ctas_parquet
Create Table: CREATE TABLE `test_ctas_parquet` (
  `c1` bigint(20) NULL COMMENT "",
  `c2` decimal(38, 9) NULL COMMENT "",
  `c3` varchar(1048576) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c1`, `c2`)
COMMENT "OLAP"
DISTRIBUTED BY RANDOM
PROPERTIES (
"bucket_size" = "4294967296",
"compression" = "LZ4",
"replication_num" = "3"
);
```

The result shows that the `c2` column, which contains both FLOAT and INT data, is merged as a DECIMAL column, and `c3`, which contains both DATE and DATETIME data, is merged as a VARCHAR column.

The above result stays the same when the Parquet files are changed to CSV files that contain the same data:

```Plain
mysql> CREATE TABLE test_ctas_csv AS
    -> SELECT * FROM FILES(
    ->     "path" = "s3://inserttest/csv/*",
    ->     "format" = "csv",
    ->     "csv.column_separator"=",",
    ->     "csv.row_delimiter"="\n",
    ->     "csv.enclose"='"',
    ->     "csv.skip_header"="1",
    ->     "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    ->     "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    ->     "aws.s3.region" = "us-west-2"
    -> );
Query OK, 0 rows affected (30.90 sec)

mysql> SHOW CREATE TABLE test_ctas_csv\G
*************************** 1. row ***************************
       Table: test_ctas_csv
Create Table: CREATE TABLE `test_ctas_csv` (
  `c1` bigint(20) NULL COMMENT "",
  `c2` decimal(38, 9) NULL COMMENT "",
  `c3` varchar(1048576) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c1`, `c2`)
COMMENT "OLAP"
DISTRIBUTED BY RANDOM
PROPERTIES (
"bucket_size" = "4294967296",
"compression" = "LZ4",
"replication_num" = "3"
);
1 row in set (0.27 sec)
```