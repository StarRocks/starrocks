
有关如何为其他远程存储创建存储卷和设置默认存储卷的更多信息，请参阅 [CREATE STORAGE VOLUME](../../sql-reference/sql-statements/Administration/CREATE_STORAGE_VOLUME.md) 和 [SET DEFAULT STORAGE VOLUME](../../sql-reference/sql-statements/Administration/SET_DEFAULT_STORAGE_VOLUME.md)。

### 创建数据库和云原生表

创建默认存储卷后，您可以使用该存储卷创建数据库和云原生表。

StarRocks 存算分离集群支持所有[表类型](../../table_design/table_types/table_types.md)。

以下示例创建数据库 `cloud_db`，并创建明细表 `detail_demo`，启用本地磁盘缓存，将热数据有效期设置为一个月，并禁用异步数据导入：

```SQL
CREATE DATABASE cloud_db;
USE cloud_db;
CREATE TABLE IF NOT EXISTS detail_demo (
    recruit_date  DATE           NOT NULL COMMENT "YYYY-MM-DD",
    region_num    TINYINT        COMMENT "range [-128, 127]",
    num_plate     SMALLINT       COMMENT "range [-32768, 32767] ",
    tel           INT            COMMENT "range [-2147483648, 2147483647]",
    id            BIGINT         COMMENT "range [-2^63 + 1 ~ 2^63 - 1]",
    password      LARGEINT       COMMENT "range [-2^127 + 1 ~ 2^127 - 1]",
    name          CHAR(20)       NOT NULL COMMENT "range char(m),m in (1-255) ",
    profile       VARCHAR(500)   NOT NULL COMMENT "upper limit value 65533 bytes",
    ispass        BOOLEAN        COMMENT "true/false")
DUPLICATE KEY(recruit_date, region_num)
DISTRIBUTED BY HASH(recruit_date, region_num)
PROPERTIES (
    "storage_volume" = "def_volume",
    "datacache.enable" = "true",
    "datacache.partition_duration" = "1 MONTH",
    "enable_async_write_back" = "false"
);
```

> **说明**
>
> 当您在 StarRocks 存算分离集群中创建数据库或云原生表时，如果未指定存储卷，StarRocks 将使用默认存储卷。

除了常规表 PROPERTIES 之外，您还需要在创建表时指定以下 PROPERTIES：

#### datacache.enable

是否启用本地磁盘缓存。默认值：`true`。

- `true`：数据会同时导入对象存储（或 HDFS）和本地磁盘（作为查询加速的缓存）。
- `false`：数据仅导入到对象存储中。

> **说明**
>
> - 在 v3.0 版本中，该参数名称为 `enable_storage_cache`。
> - 如需启用本地磁盘缓存，必须在 CN 配置项 `storage_root_path` 中指定磁盘目录。

#### datacache.partition_duration

热数据的有效期。当启用本地磁盘缓存时，所有数据都会导入至本地磁盘缓存中。当缓存满时，StarRocks 会从缓存中删除最近较少使用（Less recently used）的数据。当有查询需要扫描已删除的数据时，StarRocks 会从当前时间开始，检查该数据是否在有效期内。如果数据在有效期内，StarRocks 会再次将数据导入至缓存中。如果数据不在有效期内，StarRocks 不会将其导入至缓存中。该属性为字符串，您可以使用以下单位指定：`YEAR`、`MONTH`、`DAY` 和 `HOUR`，例如，`7 DAY` 和 `12 HOUR`。如果不指定，StarRocks 将所有数据都作为热数据进行缓存。

> **说明**
>
> - 在 v3.0 版本中，该参数名称为 `storage_cache_ttl`。
> - 仅当 `datacache.enable` 设置为 `true` 时，此属性可用。

#### enable_async_write_back

是否允许数据异步写入对象存储。默认值：`false`。

- `true`：导入任务在数据写入本地磁盘缓存后立即返回成功，数据将异步写入对象存储。允许数据异步写入可以提升导入性能，但如果系统发生故障，可能会存在一定的数据可靠性风险。
- `false`：只有在数据同时写入对象存储和本地磁盘缓存后，导入任务才会返回成功。禁用数据异步写入保证了更高的可用性，但会导致较低的导入性能。

### 查看表信息

您可以通过 `SHOW PROC "/dbs/<db_id>"` 查看特定数据库中的表的信息。详细信息，请参阅 [SHOW PROC](../../sql-reference/sql-statements/Administration/SHOW_PROC.md)。

示例：

```Plain
mysql> SHOW PROC "/dbs/xxxxx";
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+------------------------------+
| TableId | TableName   | IndexNum | PartitionColumnName | PartitionNum | State  | Type         | LastConsistencyCheckTime | ReplicaCount | PartitionType | StoragePath                  |
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+------------------------------+
| 12003   | detail_demo | 1        | NULL                | 1            | NORMAL | CLOUD_NATIVE | NULL                     | 8            | UNPARTITIONED | s3://xxxxxxxxxxxxxx/1/12003/ |
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+------------------------------+
```

StarRocks 存算分离集群中表的 `Type` 为 `CLOUD_NATIVE`。`StoragePath` 字段为表在对象存储中的路径。

### 向 StarRocks 存算分离集群导入数据

StarRocks 存算分离集群支持 StarRocks 提供的所有导入方式。详细信息，请参阅 [导入总览](../../loading/Loading_intro.md)。

### 在 StarRocks 存算分离集群查询

StarRocks 存算分离集群支持 StarRocks 提供的所有查询方式。详细信息，请参阅 [SELECT](../../sql-reference/sql-statements/data-manipulation/SELECT.md)。

> **说明**
>
> StarRocks 存算分离集群暂不支持[同步物化视图](../../using_starrocks/Materialized_view-single_table.md)。
