---
displayed_sidebar: docs
---

# FILES

## 功能

<<<<<<< HEAD
从云存储或 HDFS 读取数据文件。该函数根据给定的数据路径等参数读取数据，并自动根据数据文件的格式、列信息等推断出 Table Schema，最终以数据行形式返回文件中的数据。您可以通过 [SELECT](../../sql-statements/data-manipulation/SELECT.md) 直接直接查询该数据，通过 [INSERT](../../sql-statements/data-manipulation/INSERT.md) 导入数据，或通过 [CREATE TABLE AS SELECT](../../sql-statements/data-definition/CREATE_TABLE_AS_SELECT.md) 建表并导入数据。该功能自 v3.1.0 起支持。
=======

定义远程存储中的数据文件。

从 v3.1.0 版本开始，StarRocks 支持使用表函数 FILES() 在远程存储中定义只读文件。该函数根据给定的数据路径等参数读取数据，并自动根据数据文件的格式、列信息等推断出 Table Schema，最终以数据行形式返回文件中的数据。您可以通过 [SELECT](../../sql-statements/table_bucket_part_index/SELECT.md) 直接直接查询该数据，通过 [INSERT](../../sql-statements/loading_unloading/INSERT.md) 导入数据，或通过 [CREATE TABLE AS SELECT](../../sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) 建表并导入数据。

从 v3.2.0 版本开始，FILES() 写入数据至远程存储。您可以[使用 INSERT INTO FILES() 将数据从 StarRocks 导出到远程存储](../../../unloading/unload_using_insert_into_files.md)。
>>>>>>> e06217c368 ([Doc] Ref docs (#50111))

目前 FILES() 函数支持以下数据源和文件格式：

- **数据源：**
  - AWS S3
  - HDFS
- **文件格式：**
  - Parquet
  - ORC

## 语法

```SQL
FILES( data_location , data_format [, StorageCredentialParams ] )

data_location ::=
    "path" = {"s3://<s3_path>" | "hdfs://<hdfs_ip>:<hdfs_port>/<hdfs_path>"}

data_format ::=
    "format" = "{parquet | orc}"

StorageCredentialParams ::=
    { hdfs_credential | aws_s3_credential }
```

## 参数说明

所有参数均为 `"key" = "value"` 形式的参数对。

| **参数** | **必填** | **说明**                                                     |
| -------- | -------- | ------------------------------------------------------------ |
| path     | 是       | 用于访问数据文件的 URI。示例：<br />如使用 AWS S3：`s3://testbucket/parquet/test.parquet` <br />如使用 HDFS：`hdfs://<hdfs_ip>:<hdfs_port>/test/parquet/test.orc` |
| format   | 是       | 数据文件的格式。有效值：`parquet` 和 `orc`。                 |

### StorageCredentialParams

StarRocks 访问存储系统的认证配置。

StarRocks 当前仅支持通过 IAM User 认证访问 AWS S3，以及通过简单认证接入访问 HDFS 集群。

- 如果您使用 IAM User 认证访问 AWS S3：

  ```SQL
  "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
  "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "aws.s3.region" = "<s3_region>"
  ```

  | **参数**          | **必填** | **说明**                                                 |
  | ----------------- | -------- | -------------------------------------------------------- |
  | aws.s3.access_key | 是       | 用于指定访问 AWS S3 存储空间的 Access Key。              |
  | aws.s3.secret_key | 是       | 用于指定访问 AWS S3 存储空间的 Secret Key。              |
  | aws.s3.region     | 是       | 用于指定需访问的 AWS S3 存储空间的地区，如 `us-west-2`。 |

- 如果您使用简单认证接入访问 HDFS 集群：

  ```SQL
  "hadoop.security.authentication" = "simple",
  "username" = "xxxxxxxxxx",
  "password" = "yyyyyyyyyy"
  ```

  | **参数**                       | **必填** | **说明**                                                     |
  | ------------------------------ | -------- | ------------------------------------------------------------ |
  | hadoop.security.authentication | 否       | 用于指定待访问 HDFS 集群的认证方式。有效值：`simple`（默认值）。`simple` 表示简单认证，即无认证。 |
  | username                       | 是       | 用于访问 HDFS 集群中 NameNode 节点的用户名。                 |
  | password                       | 是       | 用于访问 HDFS 集群中 NameNode 节点的密码。                   |

## 返回

当与 SELECT 语句一同使用时，FILES() 函数会以表的形式返回远端存储文件中的数据。

- 当查询 Parquet 或 ORC 文件时，您可以在 SELECT 语句直接指定对应列名，或使用 `*` 查询所有列。

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

## 示例

示例一：查询 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet/par-dup.parquet** 中的数据

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

示例二：将 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet/insert_wiki_edit_append.parquet** 中的数据插入至表 `insert_wiki_edit` 中：

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

示例三：基于 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet/insert_wiki_edit_append.parquet** 中的数据创建表 `ctas_wiki_edit`：

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
