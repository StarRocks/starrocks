---
displayed_sidebar: "Chinese"
---

# 从 HDFS 导入

StarRocks 支持通过以下方式从 HDFS 导入数据：

- 使用 [INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md)+[`FILES()`](../sql-reference/sql-functions/table-functions/files.md) 进行同步导入。
- 使用 [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 进行异步导入。
- 使用 [Pipe](../sql-reference/sql-statements/data-manipulation/CREATE_PIPE.md) 进行持续的异步导入。

三种导入方式各有优势，具体将在下面分章节详细阐述。

一般情况下，建议您使用 INSERT+`FILES()`，更为方便易用。

但是，INSERT+`FILES()` 当前只支持 Parquet 和 ORC 文件格式。因此，如果您需要导入其他格式（如 CSV）的数据、或者需要[在导入过程中执行 DELETE 等数据变更操作](../loading/Load_to_Primary_Key_tables.md)，可以使用 Broker Load。

如果需要导入超大数据（比如超过 100 GB、特别是 1 TB 以上的数据量），建议您使用 Pipe。Pipe 会按文件数量或大小，自动对目录下的文件进行拆分，将一个大的导入作业拆分成多个较小的串行的导入任务，从而降低出错重试的代价。另外，在进行持续性的数据导入时，也推荐使用 Pipe，它能监听远端存储目录的文件变化，并持续导入有变化的文件数据。

## 准备工作

### 准备数据源

确保待导入数据已保存在 HDFS 集群。本文假设待导入的数据文件为 `/user/amber/user_behavior_ten_million_rows.parquet`。

### 查看权限

导入操作需要目标表的 INSERT 权限。如果您的 StarRocks 用户账号没有 INSERT 权限，请参考 [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 给用户赋权。

### 获取资源访问配置

可以使用简单认证方式访问 HDFS，需要您提前获取用于访问 HDFS 集群中 NameNode 节点的用户名和密码。

## 通过 INSERT+FILES() 导入

该特性从 3.1 版本起支持。当前只支持 Parquet 和 ORC 文件格式。

### INSERT+FILES() 优势

`FILES()` 会根据给定的数据路径等参数读取数据，并自动根据数据文件的格式、列信息等推断出表结构，最终以数据行的形式返回文件中的数据。

通过 `FILES()`，您可以：

- 使用 [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md) 语句直接从 HDFS 查询数据。
- 通过 [CREATE TABLE AS SELECT](../sql-reference/sql-statements/data-definition/CREATE_TABLE_AS_SELECT.md)（简称 CTAS）语句实现自动建表和导入数据。
- 手动建表，然后通过 [INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md) 导入数据。

### 操作示例

#### 通过 SELECT 直接查询数据

您可以通过 SELECT+`FILES()` 直接查询 HDFS 里的数据，从而在建表前对待导入数据有一个整体的了解，其优势包括：

- 您不需要存储数据就可以对其进行查看。
- 您可以查看数据的最大值、最小值，并确定需要使用哪些数据类型。
- 您可以检查数据中是否包含 `NULL` 值。

例如，查询保存在 HDFS 中的数据文件 `/user/amber/user_behavior_ten_million_rows.parquet`：

```SQL
SELECT * FROM FILES
(
    "path" = "hdfs://<hdfs_ip>:<hdfs_port>/user/amber/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
LIMIT 3;
```

系统返回如下查询结果：

```Plaintext
+--------+---------+------------+--------------+---------------------+
| UserID | ItemID  | CategoryID | BehaviorType | Timestamp           |
+--------+---------+------------+--------------+---------------------+
| 543711 |  829192 |    2355072 | pv           | 2017-11-27 08:22:37 |
| 543711 | 2056618 |    3645362 | pv           | 2017-11-27 10:16:46 |
| 543711 | 1165492 |    3645362 | pv           | 2017-11-27 10:17:00 |
+--------+---------+------------+--------------+---------------------+
```

> **说明**
>
> 以上返回结果中的列名是源 Parquet 文件中定义的列名。

#### 通过 CTAS 自动建表并导入数据

该示例是上一个示例的延续。该示例中，通过在 CREATE TABLE AS SELECT (CTAS) 语句中嵌套上一个示例中的 SELECT 查询，StarRocks 可以自动推断表结构、创建表、并把数据导入新建的表中。Parquet 格式的文件自带列名和数据类型，因此您不需要指定列名或数据类型。

> **说明**
>
> 使用表结构推断功能时，CREATE TABLE 语句不支持设置副本数，因此您需要在建表前把副本数设置好。例如，您可以通过如下命令设置副本数为 `1`：
>
> ```SQL
> ADMIN SET FRONTEND CONFIG ('default_replication_num' = "1");
> ```

通过如下语句创建数据库、并切换至该数据库：

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

通过 CTAS 自动创建表、并把数据文件 `/user/amber/user_behavior_ten_million_rows.parquet` 中的数据导入到新建表中：

```SQL
CREATE TABLE user_behavior_inferred AS
SELECT * FROM FILES
(
    "path" = "hdfs://<hdfs_ip>:<hdfs_port>/user/amber/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

建表完成后，您可以通过 [DESCRIBE](../sql-reference/sql-statements/Utility/DESCRIBE.md) 查看新建表的表结构：

```SQL
DESCRIBE user_behavior_inferred;
```

系统返回如下查询结果：

```Plaintext
+--------------+------------------+------+-------+---------+-------+
| Field        | Type             | Null | Key   | Default | Extra |
+--------------+------------------+------+-------+---------+-------+
| UserID       | bigint           | YES  | true  | NULL    |       |
| ItemID       | bigint           | YES  | true  | NULL    |       |
| CategoryID   | bigint           | YES  | true  | NULL    |       |
| BehaviorType | varchar(1048576) | YES  | false | NULL    |       |
| Timestamp    | varchar(1048576) | YES  | false | NULL    |       |
+--------------+------------------+------+-------+---------+-------+
```

将系统推断出来的表结构跟手动建表的表结构从以下几个方面进行对比：

- 数据类型
- 是否允许 `NULL` 值
- 定义为键的字段

在生产环境中，为更好地控制目标表的表结构、实现更高的查询性能，建议您手动创建表、指定表结构。

您可以查询新建表中的数据，验证数据已成功导入。例如：

```SQL
SELECT * from user_behavior_inferred LIMIT 3;
```

系统返回如下查询结果，表明数据已成功导入：

```Plaintext
+--------+--------+------------+--------------+---------------------+
| UserID | ItemID | CategoryID | BehaviorType | Timestamp           |
+--------+--------+------------+--------------+---------------------+
|     84 |  56257 |    1879194 | pv           | 2017-11-26 05:56:23 |
|     84 | 108021 |    2982027 | pv           | 2017-12-02 05:43:00 |
|     84 | 390657 |    1879194 | pv           | 2017-11-28 11:20:30 |
+--------+--------+------------+--------------+---------------------+
```

#### 手动建表并通过 INSERT 导入数据

在实际业务场景中，您可能需要自定义目标表的表结构，包括：

- 各列的数据类型和默认值、以及是否允许 `NULL` 值
- 定义哪些列作为键、以及这些列的数据类型
- 数据分区分桶

> **说明**
>
> 要实现高效的表结构设计，您需要深度了解表中数据的用途、以及表中各列的内容。本文不对表设计做过多赘述，有关表设计的详细信息，参见[表设计](../table_design/StarRocks_table_design.md)。

该示例主要演示如何根据源 Parquet 格式文件中数据的特点、以及目标表未来的查询用途等对目标表进行定义和创建。在创建表之前，您可以先查看一下保存在 HDFS 中的源文件，从而了解源文件中数据的特点，例如：

- 源文件中包含一个数据类型为 `datetime` 的 `Timestamp` 列，因此建表语句中也应该定义这样一个数据类型为 `datetime` 的 `Timestamp` 列。
- 源文件中的数据中没有 `NULL` 值，因此建表语句中也不需要定义任何列为允许 `NULL` 值。
- 根据查询到的数据类型，可以在建表语句中定义 `UserID` 列为排序键和分桶键。根据实际业务场景需要，您还可以定义其他列比如 `ItemID` 或者定义 `UserID` 与其他列的组合作为排序键。

通过如下语句创建数据库、并切换至该数据库：

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

通过如下语句手动创建表（建议表结构与您在 HDFS 存储的待导入数据结构一致）：

```SQL
CREATE TABLE user_behavior_declared
(
    UserID int(11),
    ItemID int(11),
    CategoryID int(11),
    BehaviorType varchar(65533),
    Timestamp datetime
)
ENGINE = OLAP 
DUPLICATE KEY(UserID)
DISTRIBUTED BY HASH(UserID)
PROPERTIES
(
    "replication_num" = "1"
);
```

建表完成后，您可以通过 INSERT INTO SELECT FROM FILES() 向表内导入数据：

```SQL
INSERT INTO user_behavior_declared
SELECT * FROM FILES
(
    "path" = "hdfs://<hdfs_ip>:<hdfs_port>/user/amber/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

导入完成后，您可以查询新建表中的数据，验证数据已成功导入。例如：

```SQL
SELECT * from user_behavior_declared LIMIT 3;
```

系统返回如下查询结果，表明数据已成功导入：

```Plaintext
+--------+---------+------------+--------------+---------------------+
| UserID | ItemID  | CategoryID | BehaviorType | Timestamp           |
+--------+---------+------------+--------------+---------------------+
|    107 | 1568743 |    4476428 | pv           | 2017-11-25 14:29:53 |
|    107 |  470767 |    1020087 | pv           | 2017-11-25 14:32:31 |
|    107 |  358238 |    1817004 | pv           | 2017-11-25 14:43:23 |
+--------+---------+------------+--------------+---------------------+
```

#### 查看导入进度

通过 `information_schema.loads` 视图查看导入作业的进度。该功能自 3.1 版本起支持。例如：

```SQL
SELECT * FROM information_schema.loads ORDER BY JOB_ID DESC;
```

如果您提交了多个导入作业，您可以通过 `LABEL` 过滤出想要查看的作业。例如：

```SQL
SELECT * FROM information_schema.loads WHERE LABEL = 'insert_0d86c3f9-851f-11ee-9c3e-00163e044958' \G
*************************** 1. row ***************************
              JOB_ID: 10214
               LABEL: insert_0d86c3f9-851f-11ee-9c3e-00163e044958
       DATABASE_NAME: mydatabase
               STATE: FINISHED
            PROGRESS: ETL:100%; LOAD:100%
                TYPE: INSERT
            PRIORITY: NORMAL
           SCAN_ROWS: 10000000
       FILTERED_ROWS: 0
     UNSELECTED_ROWS: 0
           SINK_ROWS: 10000000
            ETL_INFO:
           TASK_INFO: resource:N/A; timeout(s):300; max_filter_ratio:0.0
         CREATE_TIME: 2023-11-17 15:58:14
      ETL_START_TIME: 2023-11-17 15:58:14
     ETL_FINISH_TIME: 2023-11-17 15:58:14
     LOAD_START_TIME: 2023-11-17 15:58:14
    LOAD_FINISH_TIME: 2023-11-17 15:58:18
         JOB_DETAILS: {"All backends":{"0d86c3f9-851f-11ee-9c3e-00163e044958":[10120]},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":311710786,"InternalTableLoadRows":10000000,"ScanBytes":581574034,"ScanRows":10000000,"TaskNumber":1,"Unfinished backends":{"0d86c3f9-851f-11ee-9c3e-00163e044958":[]}}
           ERROR_MSG: NULL
        TRACKING_URL: NULL
        TRACKING_SQL: NULL
REJECTED_RECORD_PATH: NULL
```

有关 `loads` 视图提供的字段详情，参见 [Information Schema](../reference/information_schema/loads.md)。

> **NOTE**
>
> 由于 INSERT 语句是一个同步命令，因此，如果作业还在运行当中，您需要打开另一个会话来查看 INSERT 作业的执行情况。

## 通过 Broker Load 导入

作为一种异步的导入方式，Broker Load 负责建立与 HDFS 的连接、拉取数据、并将数据存储到 StarRocks 中。

当前支持 Parquet、ORC、及 CSV 三种文件格式。

### Broker Load 优势

- Broker Load 支持在导入过程中执行[数据转换](../loading/Etl_in_loading.md)、以及 [UPSERT、DELETE 等数据变更操作](../loading/Load_to_Primary_Key_tables.md)。
- Broker Load 在后台运行，客户端不需要保持连接也能确保导入作业不中断。
- Broker Load 作业默认超时时间为 4 小时，适合导入数据较大、导入运行时间较长的场景。
- 除 Parquet 和 ORC 文件格式，Broker Load 还支持 CSV 文件格式。

### 工作原理

![Broker Load 原理图](../assets/broker_load_how-to-work_zh.png)

1. 用户创建导入作业。
2. FE 生成查询计划，然后把查询计划拆分并分分配给各个 BE 执行。
3. 各个 BE 从数据源拉取数据并把数据导入到 StarRocks 中。

### 操作示例

创建 StarRocks 表，启动导入作业从 HDFS 集群拉取数据文件 `/user/amber/user_behavior_ten_million_rows.parquet` 的数据，然后验证导入过程和结果是否成功。

#### 建库建表

通过如下语句创建数据库、并切换至该数据库：

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

通过如下语句手动创建表（建议表结构与您在 HDFS 集群中存储的待导入数据结构一致）：

```SQL
CREATE TABLE user_behavior
(
    UserID int(11),
    ItemID int(11),
    CategoryID int(11),
    BehaviorType varchar(65533),
    Timestamp datetime
)
ENGINE = OLAP 
DUPLICATE KEY(UserID)
DISTRIBUTED BY HASH(UserID)
PROPERTIES
(
    "replication_num" = "1"
);
```

#### 提交导入作业

执行如下命令创建 Broker Load 作业，把数据文件 `/user/amber/user_behavior_ten_million_rows.parquet` 中的数据导入到表 `user_behavior`中：

```SQL
LOAD LABEL user_behavior
(
    DATA INFILE("hdfs://<hdfs_ip>:<hdfs_port>/user/amber/user_behavior_ten_million_rows.parquet")
    INTO TABLE user_behavior
    FORMAT AS "parquet"
 )
 WITH BROKER
(
    "hadoop.security.authentication" = "simple",
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "timeout" = "72000"
);
```

导入语句包含四个部分：

- `LABEL`：导入作业的标签，字符串类型，可用于查询导入作业的状态。
- `LOAD` 声明：包括源数据文件所在的 URI、源数据文件的格式、以及目标表的名称等作业描述信息。
- `BROKER`：连接数据源的认证信息配置。
- `PROPERTIES`：用于指定超时时间等可选的作业属性。

有关详细的语法和参数说明，参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

#### 查看导入进度

通过 `information_schema.loads` 视图查看导入作业的进度。该功能自 3.1 版本起支持。

```SQL
SELECT * FROM information_schema.loads;
```

有关 `loads` 视图提供的字段详情，参见 [Information Schema](../reference/information_schema/loads.md)。

如果您提交了多个导入作业，您可以通过 `LABEL` 过滤出想要查看的作业。例如：

```SQL
SELECT * FROM information_schema.loads WHERE LABEL = 'user_behavior';
```

例如，在下面的返回结果中，有两条关于导入作业 `user_behavior` 的记录：

- 第一条记录显示导入作业的状态为 `CANCELLED`。通过记录中的 `ERROR_MSG` 字段，可以确定导致作业出错的原因是 `listPath failed`。
- 第二条记录显示导入作业的状态为 `FINISHED`，表示作业成功。

```Plaintext
JOB_ID|LABEL                                      |DATABASE_NAME|STATE    |PROGRESS           |TYPE  |PRIORITY|SCAN_ROWS|FILTERED_ROWS|UNSELECTED_ROWS|SINK_ROWS|ETL_INFO|TASK_INFO                                           |CREATE_TIME        |ETL_START_TIME     |ETL_FINISH_TIME    |LOAD_START_TIME    |LOAD_FINISH_TIME   |JOB_DETAILS                                                                                                                                                                                                                                                    |ERROR_MSG                             |TRACKING_URL|TRACKING_SQL|REJECTED_RECORD_PATH|
------+-------------------------------------------+-------------+---------+-------------------+------+--------+---------+-------------+---------------+---------+--------+----------------------------------------------------+-------------------+-------------------+-------------------+-------------------+-------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------+------------+------------+--------------------+
 10121|user_behavior                              |mydatabase   |CANCELLED|ETL:N/A; LOAD:N/A  |BROKER|NORMAL  |        0|            0|              0|        0|        |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|2023-08-10 14:59:30|                   |                   |                   |2023-08-10 14:59:34|{"All backends":{},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":0,"InternalTableLoadRows":0,"ScanBytes":0,"ScanRows":0,"TaskNumber":0,"Unfinished backends":{}}                                                                                        |type:ETL_RUN_FAIL; msg:listPath failed|            |            |                    |
 10106|user_behavior                              |mydatabase   |FINISHED |ETL:100%; LOAD:100%|BROKER|NORMAL  | 86953525|            0|              0| 86953525|        |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|2023-08-10 14:50:15|2023-08-10 14:50:19|2023-08-10 14:50:19|2023-08-10 14:50:19|2023-08-10 14:55:10|{"All backends":{"a5fe5e1d-d7d0-4826-ba99-c7348f9a5f2f":[10004]},"FileNumber":1,"FileSize":1225637388,"InternalTableLoadBytes":2710603082,"InternalTableLoadRows":86953525,"ScanBytes":1225637388,"ScanRows":86953525,"TaskNumber":1,"Unfinished backends":{"a5|                                      |            |            |                    |
```

 导入作业完成后，您可以从表内查询数据，验证数据是否已成功导入。例如：

```SQL
SELECT * from user_behavior LIMIT 3;
```

系统返回如下查询结果，表明数据已经成功导入：

```Plaintext
+--------+--------+------------+--------------+---------------------+
| UserID | ItemID | CategoryID | BehaviorType | Timestamp           |
+--------+--------+------------+--------------+---------------------+
|     58 | 158350 |    2355072 | pv           | 2017-11-27 13:06:51 |
|     58 | 158590 |    3194735 | pv           | 2017-11-27 02:21:04 |
|     58 | 215073 |    3002561 | pv           | 2017-11-30 10:55:42 |
+--------+--------+------------+--------------+---------------------+
```

## 通过 Pipe 导入

从 3.2 版本起，StarRocks 支持使用 Pipe 以微批次的方式从 HDFS 持续导入数据，使数据在几分钟内对用户可用，同时您无需按计划手动执行导入命令。适用于大规模批量导入数据、以及持续导入数据的场景。

当前只支持 Parquet 和 ORC 文件格式。

### Pipe 优势

- **大规模分批导入，降低出错重试成本。**

  需要导入的数据文件较多、数据量大。Pipe 会按文件数量或大小，自动对目录下的文件进行拆分，将一个大的导入作业拆分成多个较小的串行的导入任务。单个文件的数据错误不会导致整个导入作业的失败。另外，Pipe 会记录每个文件的导入状态。当导入结束后，您可以修复出错的数据文件，然后重新导入修正后的数据文件即可。这有助于降低数据出错重试的代价。

- **不间断持续导入，减少人力操作成本。**

  需要将新增或变化的数据文件写入到某个文件夹下，并且新增的数据需要持续地导入到 StarRocks 中。通过 Pipe，您只需要创建一个基于 Pipe 的持续导入作业，该 Pipe 会持续监控目录下的数据文件变化，将新增或有变动的数据文件自动导入到 StarRocks 目标表中。

- **文件唯一性判断，避免重复数据导入。**

  在导入过程中，Pipe 会根据文件名和文件对应的摘要值判断数据文件是否重复。如果文件名和文件摘要值在同一个 Pipe 导入作业中已经处理过，后续导入会自动跳过已经处理过的文件。注意，HDFS 使用 `LastModifiedTime` 作为文件摘要。

导入过程中的文件状态会记录到 `information_schema.pipe_files` 视图下，您可以通过该视图查看 Pipe 导入作业下各文件的导入状态。如果该视图关联的 Pipe 作业被删除，那么该视图下相关的记录也会同步清理。

### 工作原理

![Pipe 工作原理](../assets/pipe_data_flow.png)

### Pipe 与 INSERT+FILES() 的区别

Pipe 导入操作会根据每个数据文件的大小和包含的行数，分割成一个或多个事务，导入过程中的中间结果对用户可见。INSERT+`FILES()` 导入操作是一个整体事务，导入过程中数据对用户不可见。

### 文件导入顺序

Pipe 导入操作会在内部维护一个文件队列，分批次从队列中取出对应文件进行导入。Pipe 并不能保证文件的导入顺序和文件上传顺序一致，因此可能会出现新的数据早于老的数据导入。

### 操作示例

#### 建库建表

通过如下语句创建数据库、并切换至该数据库：

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

通过如下语句手动创建表（建议表结构与您在 HDFS 存储的待导入数据结构一致）：

```SQL
CREATE TABLE user_behavior_replica
(
    UserID int(11),
    ItemID int(11),
    CategoryID int(11),
    BehaviorType varchar(65533),
    Timestamp datetime
)
ENGINE = OLAP 
DUPLICATE KEY(UserID)
DISTRIBUTED BY HASH(UserID)
PROPERTIES
(
    "replication_num" = "1"
);
```

#### 提交导入作业

执行如下命令创建 Pipe 作业，把数据文件 `/user/amber/user_behavior_ten_million_rows.parquet` 中的数据导入到表 `user_behavior_replica` 中：

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
    "path" = "hdfs://<hdfs_ip>:<hdfs_port>/user/amber/user_behavior_ten_million_rows.parquet",
    "format" = "parquet",
    "hadoop.security.authentication" = "simple",
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
); 
```

导入语句包含四个部分：

- `pipe_name`：Pipe 的名称。该名称在 Pipe 所在的数据库内必须唯一。
- `INSERT_SQL`：INSERT INTO SELECT FROM FILES 语句，用于从指定的源数据文件导入数据到目标表。
- `PROPERTIES`：用于控制 Pipe 执行的一些参数，例如 `AUTO_INGEST`、`POLL_INTERVAL`、`BATCH_SIZE` 和 `BATCH_FILES`，格式为 `"key" = "value"`。

有关详细的语法和参数说明，参见 [CREATE PIPE](../sql-reference/sql-statements/data-manipulation/CREATE_PIPE.md)。

#### 查看导入进度

- 通过 [SHOW PIPES](../sql-reference/sql-statements/data-manipulation/SHOW_PIPES.md) 查看当前数据库中的导入作业。

  ```SQL
  SHOW PIPES;
  ```

  如果您提交了多个导入作业，您可以通过 `NAME` 过滤出想要查看哪个导入作业。例如：

  ```SQL
  SHOW PIPES WHERE NAME = "user_behavior_replica" \G
  *************************** 1. row ***************************
  DATABASE_NAME: mydatabase
        PIPE_ID: 10252
      PIPE_NAME: user_behavior_replica
          STATE: RUNNING
     TABLE_NAME: mydatabase.user_behavior_replica
    LOAD_STATUS: {"loadedFiles":1,"loadedBytes":132251298,"loadingFiles":0,"lastLoadedTime":"2023-11-17 16:13:22"}
     LAST_ERROR: NULL
   CREATED_TIME: 2023-11-17 16:13:15
  1 row in set (0.00 sec)
  ```

- 通过 [`information_schema.pipes`](../reference/information_schema/pipes.md) 视图查看当前数据库中的导入作业。

  ```SQL
  SELECT * FROM information_schema.pipes;
  ```

  如果您提交了多个导入作业，您可以通过 `PIPE_NAME` 过滤出想要查看哪个导入作业。例如：

  ```SQL
  SELECT * FROM information_schema.pipes WHERE pipe_name = 'user_behavior_replica' \G
  *************************** 1. row ***************************
  DATABASE_NAME: mydatabase
        PIPE_ID: 10252
      PIPE_NAME: user_behavior_replica
          STATE: RUNNING
     TABLE_NAME: mydatabase.user_behavior_replica
    LOAD_STATUS: {"loadedFiles":1,"loadedBytes":132251298,"loadingFiles":0,"lastLoadedTime":"2023-11-17 16:13:22"}
     LAST_ERROR:
   CREATED_TIME: 2023-11-17 16:13:15
  1 row in set (0.00 sec)
  ```

#### 查看导入的文件信息

您可以通过 [`information_schema.pipe_files`](../reference/information_schema/pipe_files.md) 视图查看导入的文件信息。

```SQL
SELECT * FROM information_schema.pipe_files;
```

如果您提交了多个导入作业，您可以通过 `PIPE_NAME` 过滤出想要查看哪个作业下的文件信息。例如：

```SQL
SELECT * FROM information_schema.pipe_files WHERE pipe_name = 'user_behavior_replica' \G
*************************** 1. row ***************************
   DATABASE_NAME: mydatabase
         PIPE_ID: 10252
       PIPE_NAME: user_behavior_replica
       FILE_NAME: hdfs://172.26.195.67:9000/user/amber/user_behavior_ten_million_rows.parquet
    FILE_VERSION: 1700035418838
       FILE_SIZE: 132251298
   LAST_MODIFIED: 2023-11-15 08:03:38
      LOAD_STATE: FINISHED
     STAGED_TIME: 2023-11-17 16:13:16
 START_LOAD_TIME: 2023-11-17 16:13:17
FINISH_LOAD_TIME: 2023-11-17 16:13:22
       ERROR_MSG:
1 row in set (0.02 sec)
```

#### 管理导入作业

创建 Pipe 导入作业以后，您可以根据需要对这些作业进行修改、暂停或重新启动、删除、查询、以及尝试重新导入等操作。详情参见 [ALTER PIPE](../sql-reference/sql-statements/data-manipulation/ALTER_PIPE.md)、[SUSPEND or RESUME PIPE](../sql-reference/sql-statements/data-manipulation/SUSPEND_or_RESUME_PIPE.md)、[DROP PIPE](../sql-reference/sql-statements/data-manipulation/DROP_PIPE.md)、[SHOW PIPES](../sql-reference/sql-statements/data-manipulation/SHOW_PIPES.md)、[RETRY FILE](../sql-reference/sql-statements/data-manipulation/RETRY_FILE.md)。
