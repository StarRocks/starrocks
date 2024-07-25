---
displayed_sidebar: "Chinese"
---

# FILES

## 功能

从云存储或 HDFS 读取数据文件。该函数根据给定的数据路径等参数读取数据，并自动根据数据文件的格式、列信息等推断出 Table Schema，最终以数据行形式返回文件中的数据。您可以通过 [SELECT](../../sql-statements/data-manipulation/SELECT.md) 直接直接查询该数据，通过 [INSERT](../../sql-statements/data-manipulation/INSERT.md) 导入数据，或通过 [CREATE TABLE AS SELECT](../../sql-statements/data-definition/CREATE_TABLE_AS_SELECT.md) 建表并导入数据。该功能自 v3.1.0 起支持。

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
<<<<<<< HEAD
  "hadoop.security.authentication" = "simple",
  "username" = "xxxxxxxxxx",
  "password" = "yyyyyyyyyy"
  ```

  | **参数**                       | **必填** | **说明**                                                     |
  | ------------------------------ | -------- | ------------------------------------------------------------ |
  | hadoop.security.authentication | 否       | 用于指定待访问 HDFS 集群的认证方式。有效值：`simple`（默认值）。`simple` 表示简单认证，即无认证。 |
  | username                       | 是       | 用于访问 HDFS 集群中 NameNode 节点的用户名。                 |
  | password                       | 是       | 用于访问 HDFS 集群中 NameNode 节点的密码。                   |
=======
  "fs.s3a.access.key" = "AAAAAAAAAAAAAAAAAAAA",
  "fs.s3a.secret.key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "fs.s3a.endpoint" = "<gcs_endpoint>"
  ```

  | **参数**          | **必填** | **说明**                                                 |
  | ----------------- | -------- | -------------------------------------------------------- |
  | fs.s3a.access.key | 是       | 用于指定访问 GCS 存储空间的 Access Key。              |
  | fs.s3a.secret.key | 是       | 用于指定访问 GCS 存储空间的 Secret Key。              |
  | fs.s3a.endpoint   | 是       | 用于指定需访问的 GCS 存储空间的 Endpoint，如 `storage.googleapis.com`。 |

- 如果您使用 Shared Key 访问 Azure Blob Storage：

  ```SQL
  "azure.blob.storage_account" = "<storage_account>",
  "azure.blob.shared_key" = "<shared_key>"
  ```

  | **参数**                   | **必填** | **说明**                                                 |
  | -------------------------- | -------- | ------------------------------------------------------ |
  | azure.blob.storage_account | 是       | 用于指定 Azure Blob Storage Account 名。                  |
  | azure.blob.shared_key      | 是       | 用于指定访问 Azure Blob Storage 存储空间的 Shared Key。     |

### columns_from_path

自 v3.2 版本起，StarRocks 支持从文件路径中提取 Key/Value 对中的 Value 作为列的值。

```SQL
"columns_from_path" = "<column_name> [, ...]"
```

假设数据文件 **file1** 存储在路径 `/geo/country=US/city=LA/` 下。您可以将 `columns_from_path` 参数指定为 `"columns_from_path" = "country, city"`，以提取文件路径中的地理信息作为返回的列的值。详细使用方法请见以下示例四。

### unload_data_param

从 v3.2 版本开始，FILES() 支持在远程存储中定义可写入文件以进行数据导出。有关详细说明，请参阅[使用 INSERT INTO FILES 导出数据](../../../unloading/unload_using_insert_into_files.md)。

```sql
-- 自 v3.2 版本起支持。
unload_data_param::=
    "compression" = "<compression_method>",
    "partition_by" = "<column_name> [, ...]",
    "single" = { "true" | "false" } ,
    "target_max_file_size" = "<int>"
```

| **参数**          | **必填** | **说明**                                                          |
| ---------------- | ------------ | ------------------------------------------------------------ |
| compression      | 是          | 导出数据时要使用的压缩方法。有效值：<ul><li>`uncompressed`：不使用任何压缩算法。</li><li>`gzip`：使用 gzip 压缩算法。</li><li>`snappy`：使用 SNAPPY 压缩算法。</li><li>`zstd`：使用 Zstd 压缩算法。</li><li>`lz4`：使用 LZ4 压缩算法。</li></ul>                  |
| partition_by     | 否           | 用于将数据文件分区到不同存储路径的列，可以指定多个列。FILES() 提取指定列的 Key/Value 信息，并将数据文件存储在以对应 Key/Value 区分的子路径下。详细使用方法请见以下示例五。 |
| single           | 否           | 是否将数据导出到单个文件中。有效值：<ul><li>`true`：数据存储在单个数据文件中。</li><li>`false`（默认）：如果数据量超过 512 MB，，则数据会存储在多个文件中。</li></ul>                  |
| target_max_file_size | 否           | 分批导出时，单个文件的大致上限。单位：Byte。默认值：1073741824（1 GB）。当要导出的数据大小超过该值时，数据将被分成多个文件，每个文件的大小不会大幅超过该值。自 v3.2.7 起引入。|

## 返回

当与 SELECT 语句一同使用时，FILES() 函数会以表的形式返回远端存储文件中的数据。

- 当查询 CSV 文件时，您可以在 SELECT 语句使用 `$1`、`$2` ... 表示文件中不同的列，或使用 `*` 查询所有列。

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

## 注意事项

自 v3.2 版本起，除了基本数据类型，FILES() 还支持复杂数据类型 ARRAY、JSON、MAP 和 STRUCT。
>>>>>>> a1095b33d2 ([Doc] Add Returns to Files (#48880))

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
