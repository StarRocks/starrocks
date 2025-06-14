---
displayed_sidebar: docs
---

# File external table

File external table is a special type of external table. It allows you to directly query Parquet and ORC data files in external storage systems without loading data into StarRocks. In addition, file external tables do not rely on a metastore. In the current version, StarRocks supports the following external storage systems: HDFS, Amazon S3, and other S3-compatible storage systems.

This feature is supported from StarRocks v2.5.

:::note

- From v3.1 onwards, StarRocks supports directly loading data from files on cloud storage using the [INSERT](../loading/InsertInto.md#insert-data-directly-from-files-in-an-external-source-using-files) command and the [FILES](../sql-reference/sql-functions/table-functions/files.md) function, thereby you do not need to create an external catalog or file external table first. Besides, FILES() can automatically infer the table schema of the files, greatly simplifying the process of data loading.
- The File External Table feature was designed to help with loading data into StarRocks, NOT to perform efficient queries against external systems as a normal operation. A more performant solution would be to load the data into StarRocks.

:::

## Limits

- File external tables must be created in databases within the [default_catalog](../data_source/catalog/default_catalog.md). You can run [SHOW CATALOGS](../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) to query catalogs created in the cluster.
- Only Parquet, ORC, Avro, RCFile, and SequenceFile data files are supported.
- You can only use file external tables to query data in the target data file. Data write operations such as INSERT, DELETE, and DROP are not supported.

## Prerequisites

Before you create a file external table, you must configure your StarRocks cluster so that StarRocks can access the external storage system where the target data file is stored. The configurations required for a file external table are the same as those required for a Hive catalog, except that you do not need to configure a metastore. See [Hive catalog - Integration preparations](../data_source/catalog/hive_catalog.md#integration-preparations) for more information about configurations.

## Create a database (Optional)

After connecting to your StarRocks cluster, you can create a file external table in an existing database or create a new database to manage file external tables. To query existing databases in the cluster, run [SHOW DATABASES](../sql-reference/sql-statements/Database/SHOW_DATABASES.md). Then you can run `USE <db_name>` to switch to the target database.

The syntax for creating a database is as follows.

```SQL
CREATE DATABASE [IF NOT EXISTS] <db_name>
```

## Create a file external table

After accessing the target database, you can create a file external table in this database.

### Syntax

```SQL
CREATE EXTERNAL TABLE <table_name>
(
    <col_name> <col_type> [NULL | NOT NULL] [COMMENT "<comment>"]
) 
ENGINE=file
COMMENT ["comment"]
PROPERTIES
(
    FileLayoutParams,
    StorageCredentialParams
)
```

### Parameters

| Parameter        | Required | Description                                                  |
| ---------------- | -------- | ------------------------------------------------------------ |
| table_name       | Yes      | The name of the file external table. The naming conventions are as follows:<ul><li> The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.</li><li> The name cannot exceed 64 characters in length.</li></ul> |
| col_name         | Yes      | The column name in the file external table. The column names in the file external table must be the same as those in the target data file but are not case-sensitive. The order of columns in the file external table can be different from that in the target data file. |
| col_type         | Yes      | The column type in the file external table. You need to specify this parameter based on the column type in the target data file. For more information, see [Mapping of column types](#mapping-of-column-types). |
| NULL \| NOT NULL | No       | Whether the column in the file external table is allowed to be NULL. <ul><li>NULL: NULL is allowed. </li><li>NOT NULL: NULL is not allowed.</li></ul>You must specify this modifier based on the following rules: <ul><li>If this parameter is not specified for the columns in the target data file, you can choose not to specify it for the columns in the file external table or specify NULL for the columns in the file external table.</li><li>If NULL is specified for the columns in the target data file, you can choose not to specify this parameter for the columns in the file external table or specify NULL for the columns in the file external table.</li><li>If NOT NULL is specified for the columns in the target data file, you must also specify NOT NULL for the columns in the file external table.</li></ul> |
| comment          | No       | The comment of column in the file external table.            |
| ENGINE           | Yes      | The type of engine. Set the value to file.                   |
| comment          | No       | The description of the file external table.                  |
| PROPERTIES       | Yes      | <ul><li>`FileLayoutParams`: specifies the path and format of the target file. This property is required.</li><li>`StorageCredentialParams`: specifies the authentication information required for accessing object storage systems. This property is required only for AWS S3 and other S3-compatible storage systems.</li></ul> |

#### FileLayoutParams

A set of parameters for accessing the target data file.

```SQL
"path" = "<file_path>",
"format" = "<file_format>"
"enable_recursive_listing" = "{ true | false }"
"enable_wildcards" = "{ true | false }"
```

| Parameter                | Required | Description                                                  |
| ------------------------ | -------- | ------------------------------------------------------------ |
| path                     | Yes      | The path of the data file. <ul><li>If the data file is stored in HDFS, the path format is `hdfs://<IP address of HDFS>:<port>/<path>`. The default port number is 8020. If you use the default port, you do not need to specify it.</li><li>If the data file is stored in AWS S3 or other S3-compatible storage system, the path format is `s3://<bucket name>/<folder>/`.</li></ul> Note the following rules when you enter the path: <ul><li>If you want to access all files in a path, end this parameter with a slash (`/`), such as `hdfs://x.x.x.x/user/hive/warehouse/array2d_parq/data/`. When you run a query, StarRocks traverses all data files under the path. It does not traverse data files by using recursion.</li><li>If you want to access a single file, enter a path that directly points to this file, such as `hdfs://x.x.x.x/user/hive/warehouse/array2d_parq/data`. When you run a query, StarRocks only scans this data file.</li></ul> |
| format                   | Yes      | The format of the data file. Valid values: `parquet`, `orc`, `avro`, `rctext` or `rcbinary`, and `sequence`. |
| enable_recursive_listing | No       | Specifies whether to recursively transverse all files under the current path. Default value: `true`. The value `true` specifies to recursively list subdirectories, and the value `false` specifies to ignore subdirectories. |
| enable_wildcards         | No       | Whether to support using wildcards (`*`) in `path`. Default value: `false`. For example, `2024-07-*` is to match all files with the `2024-07-` prefix. This parameter is supported from v3.1.9. |

#### StorageCredentialParams (Optional)

A set of parameters about how StarRocks integrates with the target storage system. This parameter set is **optional**.

You need to configure `StorageCredentialParams` only when the target storage system is AWS S3 or other S3-compatible storage.

For other storage systems, you can ignore `StorageCredentialParams`.

##### AWS S3

If you need to access a data file stored in AWS S3, configure the following authentication parameters in `StorageCredentialParams`.

- If you choose the instance profile-based authentication method, configure `StorageCredentialParams` as follows:

```JavaScript
"aws.s3.use_instance_profile" = "true",
"aws.s3.region" = "<aws_s3_region>"
```

- If you choose the assumed role-based authentication method, configure `StorageCredentialParams` as follows:

```JavaScript
"aws.s3.use_instance_profile" = "true",
"aws.s3.iam_role_arn" = "<ARN of your assumed role>",
"aws.s3.region" = "<aws_s3_region>"
```

- If you choose the IAM user-based authentication method, configure `StorageCredentialParams` as follows:

```JavaScript
"aws.s3.use_instance_profile" = "false",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>",
"aws.s3.region" = "<aws_s3_region>"
```

| Parameter name              | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | Specifies whether to enable the instance profile-based authentication method and the assumed role-based authentication method when you access AWS S3. Valid values: `true` and `false`. Default value: `false`. |
| aws.s3.iam_role_arn         | Yes      | The ARN of the IAM role that has privileges on your AWS S3 bucket. <br />If you use the assumed role-based authentication method to access AWS S3, you must specify this parameter. Then, StarRocks will assume this role when it accesses the target data file. |
| aws.s3.region               | Yes      | The region in which your AWS S3 bucket resides. Example: us-west-1. |
| aws.s3.access_key           | No       | The access key of your IAM user. If you use the IAM user-based authentication method to access AWS S3, you must specify this parameter. |
| aws.s3.secret_key           | No       | The secret key of your IAM user. If you use the IAM user-based authentication method to access AWS S3, you must specify this parameter.|

For information about how to choose an authentication method for accessing AWS S3 and how to configure an access control policy in the AWS IAM Console, see [Authentication parameters for accessing AWS S3](../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3).

##### S3-compatible storage

If you need to access an S3-compatible storage system, such as MinIO, configure `StorageCredentialParams` as follows to ensure a successful integration:

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

The following table describes the parameters you need to configure in `StorageCredentialParams`.

| Parameter                       | Required | Description                                                  |
| ------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl               | Yes      | Specifies whether to enable SSL connection. <br />Valid values: `true` and `false`. Default value: `true`. |
| aws.s3.enable_path_style_access | Yes      | Specifies whether to enable path-style access.<br />Valid values: `true` and `false`. Default value: `false`. For MinIO, you must set the value to `true`.<br />Path-style URLs use the following format: `https://s3.<region_code>.amazonaws.com/<bucket_name>/<key_name>`. For example, if you create a bucket named `DOC-EXAMPLE-BUCKET1` in the US West (Oregon) Region, and you want to access the `alice.jpg` object in that bucket, you can use the following path-style URL: `https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/alice.jpg`. |
| aws.s3.endpoint                 | Yes      | The endpoint used to connect to an S3-compatible storage system instead of AWS S3. |
| aws.s3.access_key               | Yes      | The access key of your IAM user.                         |
| aws.s3.secret_key               | Yes      | The secret key of your IAM user.                         |

#### Mapping of column types

The following table provides the mapping of column types between the target data file and the file external table.

| Data file   | File external table                                          |
| ----------- | ------------------------------------------------------------ |
| INT/INTEGER | INT                                                          |
| BIGINT      | BIGINT                                                       |
| TIMESTAMP   | DATETIME. <br />Note that TIMESTAMP is converted to DATETIME without a time zone based on the time zone setting of the current session and loses some of its precision. |
| STRING      | STRING                                                       |
| VARCHAR     | VARCHAR                                                      |
| CHAR        | CHAR                                                         |
| DOUBLE      | DOUBLE                                                       |
| FLOAT       | FLOAT                                                        |
| DECIMAL     | DECIMAL                                                      |
| BOOLEAN     | BOOLEAN                                                      |
| ARRAY       | ARRAY                                                        |
| MAP         | MAP                                                          |
| STRUCT      | STRUCT                                                       |

### Examples

#### HDFS

Create a file external table named `t0` to query Parquet data files stored in an HDFS path.

```SQL
USE db_example;

CREATE EXTERNAL TABLE t0
(
    name string, 
    id int
) 
ENGINE=file
PROPERTIES 
(
    "path"="hdfs://x.x.x.x:8020/user/hive/warehouse/person_parq/", 
    "format"="parquet"
);
```

#### AWS S3

Example 1: Create a file external table and use **instance profile** to access **a single Parquet file** in AWS S3.

```SQL
USE db_example;

CREATE EXTERNAL TABLE table_1
(
    name string, 
    id int
) 
ENGINE=file
PROPERTIES 
(
    "path" = "s3://bucket-test/folder1/raw_0.parquet", 
    "format" = "parquet",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.region" = "us-west-2" 
);
```

Example 2: Create a file external table and use **assumed role** to access **all the ORC files** under the target file path in AWS S3.

```SQL
USE db_example;

CREATE EXTERNAL TABLE table_1
(
    name string, 
    id int
) 
ENGINE=file
PROPERTIES 
(
    "path" = "s3://bucket-test/folder1/", 
    "format" = "orc",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.iam_role_arn" = "arn:aws:iam::51234343412:role/role_name_in_aws_iam",
    "aws.s3.region" = "us-west-2" 
);
```

Example 3: Create a file external table and use **IAM user** to access **all the ORC files** under the file path in AWS S3.

```SQL
USE db_example;

CREATE EXTERNAL TABLE table_1
(
    name string, 
    id int
) 
ENGINE=file
PROPERTIES 
(
    "path" = "s3://bucket-test/folder1/", 
    "format" = "orc",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key" = "<iam_user_access_key>",
    "aws.s3.secret_key" = "<iam_user_access_key>",
    "aws.s3.region" = "us-west-2" 
);
```

## Query a file external table

Syntax:

```SQL
SELECT <clause> FROM <file_external_table>
```

For example, to query data from the file external table `t0` created in [Examples - HDFS](#examples), run the following command:

```plain
SELECT * FROM t0;

+--------+------+
| name   | id   |
+--------+------+
| jack   |    2 |
| lily   |    1 |
+--------+------+
2 rows in set (0.08 sec)
```

## Manage file external tables

You can view the schema of the table using [DESC](../sql-reference/sql-statements/table_bucket_part_index/DESCRIBE.md) or drop the table using [DROP TABLE](../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md).
