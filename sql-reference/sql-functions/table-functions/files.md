# FILES

## 功能

定义远程存储中的数据文件。

从 v3.1.0 版本开始，StarRocks 支持使用表函数 FILES() 在远程存储中定义只读文件。该函数根据给定的数据路径等参数读取数据，并自动根据数据文件的格式、列信息等推断出 Table Schema，最终以数据行形式返回文件中的数据。您可以通过 [SELECT](../../sql-statements/data-manipulation/SELECT.md) 直接直接查询该数据，通过 [INSERT](../../sql-statements/data-manipulation/insert.md) 导入数据，或通过 [CREATE TABLE AS SELECT](../../sql-statements/data-definition/CREATE_TABLE_AS_SELECT.md) 建表并导入数据。

从 v3.2.0 版本开始，FILES() 支持在远程存储中定义可写入的数据文件。您可以[使用 INSERT INTO FILES() 将数据从 StarRocks 导出到远程存储](../../../unloading/unload_using_insert_into_files.md)。

目前 FILES() 函数支持以下数据源和文件格式：

- **数据源：**
  - HDFS
  - AWS S3
  - Google Cloud Storage
  - Microsoft Azure Blob Storage
- **文件格式：**
  - Parquet
  - ORC（暂不支持数据导出）

## 语法

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


-- 自 v3.2 起支持。
columns_from_path ::=
    "columns_from_path" = <column_name> [, ...]


-- 自 v3.2 起支持。
schema_detect::=
    [ "schema_auto_detect_sample_rows" = "<INT>" ]
    [, "schema_auto_detect_sample_files" = "<INT>" ]

-- 自 v3.2 起支持。
unload_data::=
    "compression" = "<compression_method>"
    [, "max_file_size" = "<file_size>" ]
    [, "partition_by" = "<column_name> [, ...]" ]
    [, "single" = { "true" | "false" } ]
```

## 参数说明

所有参数均为 `"key" = "value"` 形式的参数对。

### data_location

用于访问文件的 URI。可以指定路径或文件名。

- 要访问 HDFS，您需要将此参数指定为：

  ```SQL
  "path" = "hdfs://<hdfs_host>:<hdfs_port>/<hdfs_path>"
  -- 示例： "path" = "hdfs://127.0.0.1:9000/path/file.parquet"
  ```

- 要访问 AWS S3：

  - 如果使用 S3 协议，您需要将此参数指定为：

    ```SQL
    "path" = "s3://<s3_path>"
    -- 示例： "path" = "s3://mybucket/file.parquet"
    ```

  - 如果使用 S3A 协议，您需要将此参数指定为：

    ```SQL
    "path" = "s3a://<s3_path>"
    -- 示例： "path" = "s3a://mybucket/file.parquet"
    ```

- 要访问 Google Cloud Storage，您需要将此参数指定为：

  ```SQL
  "path" = "s3a://<gcs_path>"
  -- 示例： "path" = "s3a://mybucket/file.parquet"
  ```

- 要访问 Azure Blob Storage：

  - 如果您的存储帐户允许通过 HTTP 访问，您需要将此参数指定为：

    ```SQL
    "path" = "wasb://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
    -- 示例： "path" = "wasb://testcontainer@testaccount.blob.core.windows.net/path/file.parquet"
    ```

  - 如果您的存储帐户允许通过 HTTPS 访问，您需要将此参数指定为：

    ```SQL
    "path" = "wasbs://<container>@<storage_account>.blob.core.windows.net/<blob_path>"
    -- 示例： "path" = "wasbs://testcontainer@testaccount.blob.core.windows.net/path/file.parquet"
    ```

### data_format

数据文件的格式。有效值：`parquet` 和 `orc`。

### StorageCredentialParams

StarRocks 访问存储系统的认证配置。

StarRocks 当前仅支持通过简单认证访问 HDFS 集群，通过 IAM User 认证访问 AWS S3 以及 Google Cloud Storage，以及通过 Shared Key 访问 Azure Blob Storage。

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

- 如果您使用 IAM User 认证访问 AWS S3：

  ```SQL
  "aws.s3.access_key" = "xxxxxxxxxx",
  "aws.s3.secret_key" = "yyyyyyyyyy",
  "aws.s3.region" = "<s3_region>"
  ```

  | **参数**          | **必填** | **说明**                                                 |
  | ----------------- | -------- | -------------------------------------------------------- |
  | aws.s3.access_key | 是       | 用于指定访问 AWS S3 存储空间的 Access Key。              |
  | aws.s3.secret_key | 是       | 用于指定访问 AWS S3 存储空间的 Secret Key。              |
  | aws.s3.region     | 是       | 用于指定需访问的 AWS S3 存储空间的地区，如 `us-west-2`。 |

- 如果您使用 IAM User 认证访问 GCS：

  ```SQL
  "fs.s3a.access.key" = "xxxxxxxxxx",
  "fs.s3a.secret.key" = "yyyyyyyyyy",
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

### schema_detect

自 v3.2 版本起，FILES() 支持为批量数据文件执行自动 Schema 检测和 Union 操作。StarRocks 首先扫描同批次中随机数据文件的数据进行采样，以检测数据的 Schema。然后，StarRocks 将对同批次中所有数据文件的列进行 Union 操作。

您可以使用以下参数配置采样规则：

- `schema_auto_detect_sample_rows`：扫描每个采样数据文件中的数据行数。范围：[-1, 500]。如果将此参数设置为 `-1`，则扫描所有数据行。
- `schema_auto_detect_sample_files`：在每个批次中采样的随机数据文件数量。有效值：`1`（默认值）和 `-1`。如果将此参数设置为 `-1`，则扫描所有数据文件。

采样后，StarRocks 根据以下规则 Union 所有数据文件的列：

- 对于具有不同列名或索引的列，StarRocks 将每列识别为单独的列，最终返回所有单独列。
- 对于列名相同但数据类型不同的列，StarRocks 将这些列识别为相同的列，并为其选择一个通用的数据类型。例如，如果文件 A 中的列 `col1` 是 INT 类型，而文件 B 中的列 `col1` 是 DECIMAL 类型，则在返回的列中使用 DOUBLE 数据类型。STRING 类型可用于统一所有数据类型。

如果 StarRocks 无法统一所有列，将生成一个包含错误信息和所有文件 Schema 的错误报告。

> **注意**
>
> 单个批次中的所有数据文件必须为相同的文件格式。

### unload_data

从 v3.2 版本开始，FILES() 支持在远程存储中定义可写入文件以进行数据导出。有关详细说明，请参阅[使用 INSERT INTO FILES 导出数据](../../../unloading/unload_using_insert_into_files.md)。

- `compression`（必填）：导出数据时要使用的压缩方法。有效值：
  - `uncompressed`：不使用任何压缩算法。
  - `gzip`：使用 gzip 压缩算法。
  - `brotli`：使用 Brotli 压缩算法。
  - `zstd`：使用 Zstd 压缩算法。
  - `lz4`：使用 LZ4 压缩算法。
- `max_file_size`：当数据导出为多个文件时，每个数据文件的最大大小。默认值：`1GB`。单位：B、KB、MB、GB、TB 和 PB。
- `partition_by`：用于将数据文件分区到不同存储路径的列，可以指定多个列。FILES() 提取指定列的 Key/Value 信息，并将数据文件存储在以对应 Key/Value 区分的子路径下。详细使用方法请见以下示例五。
- `single`：是否将数据导出到单个文件中。有效值：
  - `true`：数据存储在单个数据文件中。
  - `false`：如果达到 `max_file_size`，则数据存储在多个文件中。

> **注意**
>
> 不能同时指定 `max_file_size` 和 `single`。

## 注意事项

自 v3.2 版本起，除了基本数据类型，FILES() 还支持复杂数据类型 ARRAY、JSON、MAP 和 STRUCT。

## 示例

示例一：查询 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet/par-dup.parquet** 中的数据

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

示例二：将 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet/insert_wiki_edit_append.parquet** 中的数据插入至表 `insert_wiki_edit` 中：

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

示例三：基于 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet/insert_wiki_edit_append.parquet** 中的数据创建表 `ctas_wiki_edit`：

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

示例四：查询 HDFS 集群内 Parquet 文件 **/geo/country=US/city=LA/file1.parquet** 中的数据（其中仅包含两列 - `id` 和 `user`），并提取其路径中的 Key/Value 信息作为返回的列。

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

示例五：将 `sales_records` 中的所有数据行导出为多个 Parquet 文件，存储在 HDFS 集群的路径 **/unload/partitioned/** 下。这些文件存储在不同的子路径中，这些子路径根据列 `sales_time` 中的值来区分。

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
