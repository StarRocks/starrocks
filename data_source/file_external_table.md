# 文件外部表

本文介绍如何通过文件外部表 (File External Table) 直接查询外部存储系统（例如 HDFS）上的 Parquet 和 ORC 格式的数据文件。仅 StarRocks 2.5 及以上版本支持该功能。

## 前提条件

创建文件外部表前，您需要在 StarRocks 中进行相应的配置，以便能够访问数据文件所在的外部存储系统。StarRocks 当前支持的系统包括 HDFS、Amazon S3（以及兼容 S3 协议的存储对象）、阿里云对象存储 OSS 和腾讯云对象存储 COS。具体配置步骤和 Hive Catalog 相同，详细信息请参见 [Hive catalog - 准备工作](../data_source/catalog/hive_catalog.md#准备工作)。

## 操作步骤

### （可选）步骤一：创建数据库

您可以使用当前已有的数据库创建文件外部表；也可以创建一个新的数据库用来管理创建的文件外部表。创建数据库语法如下。

```SQL
CREATE DATABASE [IF NOT EXISTS] <db_name>;
```

### 步骤二：创建文件外部表

```SQL
CREATE EXTERNAL TABLE <table_name> 
(
    <col_name> <col_type> [NULL | NOT NULL] [COMMENT "<comment>"]
) 
ENGINE=FILE
PROPERTIES("<key>" = "<value>");
```

> **说明**
>
> 使用该创建语句无权限限制。

#### 参数说明

| **参数**         | **必选** | **说明**                                                     |
| ---------------- | -------- | ------------------------------------------------------------ |
| table_name       | 是       | 文件外部表名称。命名要求如下：<ul><li>必须由字母 (a-z 或 A-Z)、数字 (0-9) 或下划线 (_) 组成，且只能以字母开头。</li><li>总长度不能超过 64 个字符。</li></ul> |
| col_name         | 是       | 文件外部表列名。列名大小写不敏感，需和数据文件中的保持一致，列的顺序无需保持一致。 |
| col_type         | 是       | 文件外部表列类型，需要根据其与数据文件列类型的映射关系来填写。详细请参见[列类型映射](#列类型映射)。 |
| NULL \| NOT NULL | 否       | 文件外部表中的列是否允许为 `NULL`。<ul><li>`NULL`: 允许为 `NULL`。</li><li>`NOT NULL`: 不允许为 `NULL`。</li></ul>您需要按照如下规则指定该参数：<ul><li>如数据文件中的列没有指定 `NULL \| NOT NULL` ，则文件外部表中的列可以不指定 `NULL \| NOT NULL` 或指定为 `NULL`。</li><li>如数据文件中的列指定为 `NULL`，则文件外部表中的列可以不指定 `NULL \| NOT NULL` 或指定为 `NULL`。</li><li>如数据文件中的列指定为 `NOT NULL`，则文件外部表列必须指定为 `NOT NULL`。</li></ul> |
| comment          | 否       | 文件外部表的列备注。                                         |
| ENGINE           | 是       | ENGINE 类型，取值为 `file`。                                 |
| PROPERTIES       | 是       | 表属性。具体配置见下表 **PROPERTIES**。                      |

**PROPERTIES**

当前支持配置以下属性。

| **属性** | **必选** | **说明**                                                    |
| ------- | -------- | ------------------------------------------------------------ |
| path    | 是       | 数据文件所在的路径。<ul><li>若文件在 HDFS 上，则路径格式为 `hdfs://<HDFS的IP地址>:<端口号>/<路径>`。其中端口号默认为 `8020`，如使用默认端口号可忽略不在路径中指定。</li><li>若文件在 Amazon S3 上，则路径格式为 `s3://<bucket名称>/<folder>/`。</li></ul>填写路径时，需注意以下两点：<ul><li>如 `path` 以 `'/'` 结尾，例如 `hdfs://x.x.x.x/user/hive/warehouse/array2d_parq/data/`，则默认填写的是一个路径。查询时，StarRocks 会遍历该路径下所有文件，但不做递归遍历。</li><li>如 `path` 不以 `'/'` 结尾，例如 `hdfs://x.x.x.x/user/hive/warehouse/array2d_parq/data`，则默认填写的是单个文件，查询时会直接扫描该文件。</li></ul> |
| format  | 是       | 数据文件格式，目前仅支持 Parquet 和 ORC。                    |

#### 列类型映射

创建文件外部表时，需根据数据文件的列类型指定文件外部表的列类型，具体映射关系如下。

| **数据文件** | **文件外部表**                                               |
| ------------ | ------------------------------------------------------------ |
| INT          | INT                                                          |
| BIGINT       | BIGINT                                                       |
| TIMESTAMP    | DATETIME <br>注意， TIMESTAMP 转成 DATETIME 会损失精度，并根据当前会话设置的时区转成无时区的 DATETIME。 |
| STRING       | STRING                                                       |
| VARCHAR      | VARCHAR                                                      |
| CHAR         | CHAR                                                         |
| DOUBLE       | DOUBLE                                                       |
| FLOAT        | FLOAT                                                        |
| DECIMAL      | DECIMAL                                                      |
| BOOLEAN      | BOOLEAN                                                      |
| ARRAY        | ARRAY                                                        |
| MAP          | MAP                                                          |
| STRUCT       | STRUCT                                                       |

### 步骤三：查询数据文件

例如，创建一个文件外部表名为 `t0`，用于查询 HDFS 上的数据文件。

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

创建文件外部表后，无需导入数据，执行如下命令即可查询。

```SQL
SELECT * FROM t0;

+--------+------+
| name   | id   |
+--------+------+
| Alice  |    2 |
| lily   |    1 |
+--------+------+
2 rows in set (0.08 sec)
```

## 示例

示例 1：创建一个文件外部表名为 `table_1`，并使用 Instance Profile 进行认证和鉴权，用来访问存储在 AWS S3 路径 `s3://bucket-test/folder1` 下单个 Parquet 格式文件 `raw_0.parquet`：

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

示例 2：创建一个文件外部表名为 `table_1`，并使用 Assumed Role 进行认证和鉴权，用来访问存储在 AWS S3 路径 `s3://bucket-test/folder1` 下所有 ORC 格式文件：

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

示例 3：创建一个文件外部表名为 `table_1`，并使用 IAM User 进行认证和鉴权，用来访问存储在 AWS S3 路径 `s3://bucket-test/folder1` 下所有 ORC 格式文件：

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
