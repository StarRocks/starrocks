---
displayed_sidebar: docs
---

# FILES

## 功能

定义远程存储中的数据文件。

从 v3.1.0 版本开始，StarRocks 支持使用表函数 FILES() 在远程存储中定义只读文件。该函数根据给定的数据路径等参数读取数据，并自动根据数据文件的格式、列信息等推断出 Table Schema，最终以数据行形式返回文件中的数据。您可以通过 [SELECT](../../sql-statements/table_bucket_part_index/SELECT.md) 直接直接查询该数据，通过 [INSERT](../../sql-statements/loading_unloading/INSERT.md) 导入数据，或通过 [CREATE TABLE AS SELECT](../../sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md) 建表并导入数据。

从 v3.2.0 版本开始，FILES() 写入数据至远程存储。您可以[使用 INSERT INTO FILES() 将数据从 StarRocks 导出到远程存储](../../../unloading/unload_using_insert_into_files.md)。

目前 FILES() 函数支持以下数据源和文件格式：

- **数据源：**
  - HDFS
  - AWS S3
  - Google Cloud Storage
  - Microsoft Azure Blob Storage
- **文件格式：**
  - Parquet
  - ORC
  - CSV

## 语法

- **导入**:

  ```SQL
  FILES( data_location , data_format [, schema_detect ] [, StorageCredentialParams ] [, columns_from_path ] )
  ```

- **导出**:

  ```SQL
  FILES( data_location , data_format [, StorageCredentialParams ] , unload_data_param )
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

数据文件的格式。有效值：`parquet`、`orc` 和 `csv`。

特定数据文件格式需要额外参数指定细节选项。

#### CSV

CSV 格式示例：

```SQL
"format"="csv",
"csv.column_separator"="\\t",
"csv.enclose"='"',
"csv.skip_header"="1",
"csv.escape"="\\"
```

##### csv.column_separator

用于指定源数据文件中的列分隔符。如果不指定该参数，则默认列分隔符为 `\\t`，即 Tab。必须确保这里指定的列分隔符与源数据文件中的列分隔符一致；否则，导入作业会因数据质量错误而失败。

需要注意的是，Files() 任务通过 MySQL 协议提交请求，除了 StarRocks 会做转义处理以外，MySQL 协议也会做转义处理。因此，如果列分隔符是 Tab 等不可见字符，则需要在列分隔字符前面多加一个反斜线 (`\`)。例如，如果列分隔符是 `\t`，这里必须输入 `\\t`；如果列分隔符是 `\n`，这里必须输入 `\\n`。Apache Hive™ 文件的列分隔符为 `\x01`，因此，如果源数据文件是 Hive 文件，这里必须传入 `\\x01`。

> **说明**
>
> - StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (|)。
> - 空值 (null) 用 `\N` 表示。比如，数据文件一共有三列，其中某行数据的第一列、第三列数据分别为 `a` 和 `b`，第二列没有数据，则第二列需要用 `\N` 来表示空值，写作 `a,\N,b`，而不是 `a,,b`。`a,,b` 表示第二列是一个空字符串。

##### csv.enclose

根据 [RFC4180](https://www.rfc-editor.org/rfc/rfc4180)，用于指定把 CSV 文件中的字段括起来的字符。取值类型：单字节字符。默认值：`NONE`。最常用 `enclose` 字符为单引号 (`'`) 或双引号 (`"`)。

被 `enclose` 指定字符括起来的字段内的所有特殊字符（包括行分隔符、列分隔符等）均看做是普通符号。比 RFC4180 标准更进一步的是，StarRocks 提供的 `enclose` 属性支持设置任意单个字节的字符。

如果一个字段内包含了 `enclose` 指定字符，则可以使用同样的字符对 `enclose` 指定字符进行转义。例如，在设置了`enclose` 为双引号 (`"`) 时，字段值 `a "quoted" c` 在 CSV 文件中应该写作 `"a ""quoted"" c"`。

##### csv.skip_header

用于指定跳过 CSV 文件最开头的几行数据。取值类型：INTEGER。默认值：`0`。

在某些 CSV 文件里，最开头的几行数据会用来定义列名、列类型等元数据信息。通过设置该参数，可以使 StarRocks 在导入数据时忽略 CSV 文件的前面几行。例如，如果设置该参数为 `1`，则 StarRocks 会在导入数据时忽略 CSV 文件的第一行。

这里的行所使用的分隔符须与您在导入语句中所设定的行分隔符一致。

##### csv.escape

指定 CSV 文件用于转义的字符。用来转义各种特殊字符，比如行分隔符、列分隔符、转义符、`enclose` 指定字符等，使 StarRocks 把这些特殊字符当做普通字符而解析成字段值的一部分。取值类型：单字节字符。默认值：`NONE`。最常用的 `escape` 字符为斜杠 (`\`)，在 SQL 语句中应该写作双斜杠 (`\\`)。

> **说明**
>
> `escape` 指定字符同时作用于 `enclose` 指定字符的内部和外部。
> 以下为两个示例：
> - 当设置 `enclose` 为双引号 (`"`) 、`escape` 为斜杠 (`\`) 时，StarRocks 会把 `"say \"Hello world\""` 解析成一个字段值 `say "Hello world"`。
> - 假设列分隔符为逗号 (`,`) ，当设置 `escape` 为斜杠 (`\`) ，StarRocks 会把 `a, b\, c` 解析成 `a` 和 `b, c` 两个字段值。

### schema_detect

自 v3.2 版本起，FILES() 支持为批量数据文件执行自动 Schema 检测和 Union 操作。StarRocks 首先扫描同批次中随机数据文件的数据进行采样，以检测数据的 Schema。然后，StarRocks 将对同批次中所有数据文件的列进行 Union 操作。

您可以使用以下参数配置采样规则：

- `auto_detect_sample_files`：每个批次中采样的数据文件数量。范围：[0, + ∞]。默认值：`1`。
- `auto_detect_sample_rows`：每个采样数据文件中的数据扫描行数。范围：[0, + ∞]。默认值：`500`。

采样后，StarRocks 根据以下规则 Union 所有数据文件的列：

- 对于具有不同列名或索引的列，StarRocks 将每列识别为单独的列，最终返回所有单独列。
- 对于列名相同但数据类型不同的列，StarRocks 将这些列识别为相同的列，并为其选择一个相对较小的通用数据类型。例如，如果文件 A 中的列 `col1` 是 INT 类型，而文件 B 中的列 `col1` 是 DECIMAL 类型，则在返回的列中使用 DOUBLE 数据类型。
  - 所有整数列将被统一为更粗粗粒度上的整数类型。
  - 整数列与 FLOAT 类型列将统一为 DECIMAL 类型。
  - 其他类型统一为字符串类型用。
- 一般情况下，STRING 类型可用于统一所有数据类型。

您可以参考[示例六](#示例六)。

如果 StarRocks 无法统一所有列，将生成一个包含错误信息和所有文件 Schema 的错误报告。

> **注意**
>
> 单个批次中的所有数据文件必须为相同的文件格式。

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
  "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
  "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "aws.s3.region" = "<s3_region>"
  ```

  | **参数**          | **必填** | **说明**                                                 |
  | ----------------- | -------- | -------------------------------------------------------- |
  | aws.s3.access_key | 是       | 用于指定访问 AWS S3 存储空间的 Access Key。              |
  | aws.s3.secret_key | 是       | 用于指定访问 AWS S3 存储空间的 Secret Key。              |
  | aws.s3.region     | 是       | 用于指定需访问的 AWS S3 存储空间的地区，如 `us-west-2`。 |

- 如果您使用 IAM User 认证访问 GCS：

  ```SQL
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

## 示例

#### 示例一

查询 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet/par-dup.parquet** 中的数据

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

#### 示例二

将 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet/insert_wiki_edit_append.parquet** 中的数据插入至表 `insert_wiki_edit` 中：

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

#### 示例三

基于 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet/insert_wiki_edit_append.parquet** 中的数据创建表 `ctas_wiki_edit`：

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

#### 示例四

查询 HDFS 集群内 Parquet 文件 **/geo/country=US/city=LA/file1.parquet** 中的数据（其中仅包含两列 - `id` 和 `user`），并提取其路径中的 Key/Value 信息作为返回的列。

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

#### 示例五

将 `sales_records` 中的所有数据行导出为多个 Parquet 文件，存储在 HDFS 集群的路径 **/unload/partitioned/** 下。这些文件存储在不同的子路径中，这些子路径根据列 `sales_time` 中的值来区分。

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

#### 示例六

自动 Schema 检测和 Union 操作

以下示例基于 S3 桶中两个 Parquet 文件 File 1 和 File 2：

- File 1 中包含三列数据 - INT 列 `c1`、FLOAT 列 `c2` 以及 DATE 列 `c3`。

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

- File 2 中包含三列数据 - INT 列 `c1`、INT 列 `c2` 以及 DATETIME 列 `c3`。

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

使用 CTAS 语句创建表 `test_ctas_parquet` 并将两个 Parquet 文件中的数据导入表中：

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

查看 `test_ctas_parquet` 的表结构：

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

由结果可知，`c2` 列因为包含 FLOAT 和 INT 数据，被合并为 DECIMAL 列，而 `c3` 列因为包含 DATE 和 DATETIME 数据，，被合并为 VARCHAR 列。

将 Parquet 文件替换为含有同样数据的 CSV 文件，以上结果依然成立：

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