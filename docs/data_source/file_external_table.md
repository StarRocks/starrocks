# File external table

This topic describes how to use file external tables to directly query Parquet and ORC data files in external storage systems (such as HDFS). Only StarRocks 2.5 and later versions support this feature.

## Before you begin

Before you create a file external table, configure your StarRocks cluster so that StarRocks can access the external storage system where your data files are located. StarRocks supports the following systems: HDFS, Amazon S3 (and S3-Compatible Object Storage), Alibaba Cloud Object Storage Service (OSS), and Tencent Cloud Object Storage (COS). The configurations required for a file external table are the same as that required for a Hive catalog. Therefore, see [Hive catalog - Integration preparations](../data_source/catalog/hive_catalog.md#integration-preparations) for more information about configurations.

## Procedure

### (Optional) Step 1: Create a database

You can create a file external table in an existing database or create a new database to manage file external tables. The syntax to create a database is as follows.

```SQL
CREATE DATABASE [IF NOT EXISTS] <db_name>;
```

### Step 2: Create an external table

```SQL
CREATE EXTERNAL TABLE <table_name> 
(
    <col_name> <col_type> [NULL | NOT NULL] [COMMENT "<comment>"]
) 
ENGINE=file
PROPERTIES("<key>" = "<value>");
```

> **NOTE**
>
> This statement requires no privileges for execution.

#### Parameters

| **Parameter**    | **Required** | **Description**                                              |
| ---------------- | ------------ | ------------------------------------------------------------ |
| table_name       | Yes          | The name of the file external table. The naming conventions are as follows:<ul><li>The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.</li><li>The name cannot exceed 64 characters in length.</li></ul> |
| col_name         | Yes          | The column name in the file external table. The column names in the file external table must be the same with the column names in the data file, but are not case-sensitive. The order of columns in the file external table can be inconsistent with that in the data file. |
| col_type         | Yes          | The column type in the file external table. You need to specify this parameter based on the column type in the data file. For more information, see [Mapping of column data types](#mapping-of-column-types). |
| NULL \| NOT NULL | No           | Whether the column in the file external table is allowed to be `NULL`.<ul><li>`NULL`: The column is allowed to be `NULL`.</li><li>`NOT NULL`: The column is not allowed to be `NULL`.</li></ul>You need to specify the `NULL \| NOT NULL` modifier based on the following rules:<ul><li>If the column in the data file is not specified as `NULL \| NOT NULL`, you can choose not to specify `NULL \| NOT NULL` for the column in the file external table or to specify `NULL` for the column in the file external table.</li><li>If the column in the data file is specified as `NULL`, you can choose not to specify `NULL \| NOT NULL` for the column in the file external table or to specify `NULL` for the column in the file external table.</li><li>If the column in the data file is specified as `NOT NULL`, you must specify `NOT NULL` for the column in the file external table.</li></ul> |
| comment          | No           | The comment of the column in the file external table.        |
| ENGINE           | Yes          | The type of ENGINE. Set the value of this parameter to `file`. |
| PROPERTIES       | Yes          | The properties of the file external table. For more information, see **PROPERTIES**. |

**PROPERTIES**:

You can configure the following key-value pairs for the `PROPERTIES` parameter.

| **Property** | **Required** | **Description**                                                    |
| ------- | ------------ | ------------------------------------------------------------ |
| path    | Yes          | The path of the data file.<ul><li>If the data file is stored in HDFS, the path format is `hdfs://<IP address of HDFS>:<port>/<path>`.</li><li>If the data file is stored in Amazon S3, the path format is `s3://<bucket name>/<folder>/`.</li></ul>Note the following rules when you enter the path:<ul><li>If the value of the `path` parameter ends with `'/'`, such as `hdfs://x.x.x.x/user/hive/warehouse/array2d_parq/data'/'`, StarRocks treats it as a path. When you execute a query, StarRocks traverses all data files under the path. It does not traverse data files by using recursion.</li><li>If the value of the `path` parameter does not end with `'/'`, such as `hdfs://x.x.x.x/user/hive/warehouse/array2d_parq/data`, StarRocks treats it as a single data file. When you execute a query, StarRocks only scans the data file.</li></ul> |
| format  | Yes          | The format of the data file. Only Parquet and ORC are supported. |

#### Mapping of column types

When you create a file external table, specify column types in the file external table based on the column types in the data file. The following table provides the mapping of column types.

| **Data file** | **File external table**                                      |
| ------------- | ------------------------------------------------------------ |
| INT/INTEGER   | INT                                                          |
| BIGINT        | BIGINT                                                       |
| TIMESTAMP     | DATETIME <br>Note that TIMESTAMP is converted to DATETIME without a time zone based on the time zone setting of the current session and loses some of its precision. |
| STRING        | STRING                                                       |
| VARCHAR       | VARCHAR                                                      |
| CHAR          | CHAR                                                         |
| DOUBLE        | DOUBLE                                                       |
| FLOAT         | FLOAT                                                        |
| DECIMAL       | DECIMAL                                                      |
| BOOLEAN       | BOOLEAN                                                      |
| ARRAY         | ARRAY                                                        |
| MAP          | MAP                                                          |
| STRUCT       | STRUCT                                                       |

### Step 3: Query data from data files

For example, create a file external table named `t0` to query a data file stored in HDFS.

```SQL
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

Then, you can directly query data from the data file without loading data into StarRocks.

```SQL
SELECT * FROM t0;

+--------+------+
| name   | id   |
+--------+------+
| hanhan |    2 |
| lily   |    1 |
+--------+------+
2 rows in set (0.08 sec)
```

## Examples

Example 1: Create a file external table named `table_1` and use the instance profile-based credential method to access a single Parquet file named `raw_0.parquet` under the file path `s3://bucket-test/folder1` in AWS S3.

```SQL
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

Example 2: Create a file external table named `table_1` and use the assumed role-based credential method to access all the ORC files under the file path `s3://bucket-test/folder1` in AWS S3.

```SQL
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

Example 3: Create a file external table named `table_1` and use the IAM user-based credential method to access all the ORC files under the file path `s3://bucket-test/folder1` in AWS S3.

```SQL
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
