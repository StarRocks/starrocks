---
displayed_sidebar: "Chinese"
---

# CREATE PIPE

## 功能

创建一个 Pipe，用于定义一个实现数据导入的 INSERT INTO SELECT FROM FILES 语句。该命令自 3.2 版本起支持。

## 语法

```SQL
CREATE [OR REPLACE] PIPE [db_name.]<pipe_name> 
[PROPERTIES ("<key>" = "<value>"[, "<key> = <value>" ...])]
AS <INSERT_SQL>
```

## 参数说明

### db_name

Pipe 所属的数据库的名称。

### pipe_name

Pipe 的名称。该名称在 Pipe 所在的数据库内必须唯一。

> **NOTICE**
>
> 每个 Pipe 从属于一个数据库。删除 Pipe 所在的数据库后，该 Pipe 也会随之删除，并且该 Pipe 不会随数据库的恢复而恢复。

### INSERT_SQL

INSERT INTO SELECT FROM FILES 语句，用于从指定的源数据文件导入数据到目标表。

有关如何使用表函数 FILES()，参见 [FILES](../../../sql-reference/sql-functions/table-functions/files.md)。

### PROPERTIES

用于控制 Pipe 执行的一些参数。格式：`"key" = "value"`。

| 参数          | 默认值        | 参数描述                                                     |
| :------------ | :------------ | :----------------------------------------------------------- |
| AUTO_INGEST   | `TRUE`        | 是否启用自动增量导入。取值范围：`TRUE` 和 `FALSE`。`TRUE` 表示开启自动增量导入。`FALSE` 表示只导入作业启动时指定的数据文件内容，后续新增或修改的文件内容不导入。对于批量导入来说，可以将其设置为 `FALSE`。 |
| POLL_INTERVAL | `10` (second) | 自动增量导入的轮询间隔。                                     |
| BATCH_SIZE    | `1 GB`       | 导入批次大小。如果参数取值中不指定单位，则使用默认单位 Byte。 |
| BATCH_FILES   | `256`         | 导入批次文件数量。                                           |

## 示例

在当前数据库下，创建一个名为 `user_behavior_replica` 的 Pipe，用于把 `s3://starrocks-datasets/user_behavior_ten_million_rows.parquet` 中的数据导入到表 `user_behavior_replica` 中：

```SQL
CREATE PIPE user_behavior_replica
PROPERTIES
(
    "AUTO_INGEST" = "TRUE"
)
AS
INSERT INTO user_behavior_replica
SELECT * FROM FILES
(
    "path" = "s3://starrocks-datasets/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "aws.s3.region" = "us-east-1",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
); 
```

> **说明**
>
> 把上面命令示例中的 `AAA` 和 `BBB` 替换成真实有效的 Access Key 和 Secret Key 作为访问凭证。由于这里使用的数据对象对所有合法的 AWS 用户开放，因此您填入任何真实有效的 Access Key 和 Secret Key 都可以。

该示例以基于 IAM User 的认证鉴权方式为例，并假设 Parquet 源文件与 StarRocks 目标表的结构相同。有关认证方式和语句详情，参见[配置 AWS 认证信息](../../../integrations/authenticate_to_aws_resources.md)和 [FILES](../../../sql-reference/sql-functions/table-functions/files.md)。

## 相关文档

- [ALTER PIPE](../data-manipulation/CREATE_PIPE.md)
- [DROP PIPE](../data-manipulation/DROP_PIPE.md)
- [SHOW PIPES](../data-manipulation/SHOW_PIPES.md)
- [SUSPEND or RESUME PIPE](../data-manipulation/SUSPEND_or_RESUME_PIPE.md)
- [RETRY FILE](../data-manipulation/RETRY_FILE.md)
