# 从 HDFS 导入

StarRocks 支持通过两种方式从 HDFS 导入大批量数据：[Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md) 和 [INSERT](../sql-reference/sql-statements/data-manipulation/insert.md)。

在 3.0 及以前版本，StarRocks 只支持 Broker Load 导入方式。Broker Load 是一种异步导入方式，即您提交导入作业以后，StarRocks 会异步地执行导入作业。您需要通过 [SHOW LOAD](../sql-reference/sql-statements/data-manipulation/SHOW%20LOAD.md) 语句或者 `curl` 命令来查看导入作业的结果。

Broker Load 能够保证单次导入事务的原子性，即单次导入的多个数据文件都成功或者都失败，而不会出现部分导入成功、部分导入失败的情况。

另外，Broker Load 还支持在导入过程中做数据转换、以及通过 UPSERT 和 DELETE 操作实现数据变更。请参见[导入过程中实现数据转换](/loading/Etl_in_loading.md)和[通过导入实现数据变更](../loading/Load_to_Primary_Key_tables.md)。

> **注意**
>
> Broker Load 操作需要目标表的 INSERT 权限。如果您的用户账号没有 INSERT 权限，请参考 [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 给用户赋权。

从 3.1 版本起，StarRocks 新增支持使用 INSERT 语句和 `FILES` 关键字直接从 HDFS 导入 Parquet 或 ORC 格式的数据文件，避免了需事先创建外部表的麻烦。参见 [INSERT > 通过 FILES 关键字直接导入外部数据文件](../loading/InsertInto.md#通过-files-函数直接导入外部数据文件)。

本文主要介绍如何使用 [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md) 从 HDFS 导入数据。

## 背景信息

在 v2.4 及以前版本，StarRocks 在执行 Broker Load 时需要借助 Broker 才能访问外部存储系统，称为“有 Broker 的导入”。导入语句中需要通过 `WITH BROKER "<broker_name>"` 来指定使用哪个 Broker。Broker 是一个独立的无状态服务，封装了文件系统接口。通过 Broker，StarRocks 能够访问和读取外部存储系统上的数据文件，并利用自身的计算资源对数据文件中的数据进行预处理和导入。

自 v2.5 起，StarRocks 在执行 Broker Load 时不需要借助 Broker 即可访问外部存储系统，称为“无 Broker 的导入”。导入语句中也不再需要指定 `broker_name`，但继续保留 `WITH BROKER` 关键字。

需要注意的是，无 Broker 的导入在数据源为 HDFS 的某些场景下会受限，例如，在多 HDFS 集群或者多 Kerberos 用户的场景。在这些场景下，可以继续采用有 Broker 的导入，需要确保至少部署了一组独立的 Broker。有关各种场景下如何指定认证方式和 HA 配置，参见 [HDFS](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md#hdfs)。

> **说明**
>
> 您可以通过 [SHOW BROKER](/sql-reference/sql-statements/Administration/SHOW%20BROKER.md) 语句来查看 StarRocks 集群中已经部署的 Broker。如果集群中没有部署 Broker，请参见[部署 Broker 节点](/deployment/deploy_broker.md)完成 Broker 部署。

## 支持的数据文件格式

Broker Load 支持如下数据文件格式：

- CSV

- Parquet

- ORC

> **说明**
>
> 对于 CSV 格式的数据，需要注意以下两点：
>
> - StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (|)。
> - 空值 (null) 用 `\N` 表示。比如，数据文件一共有三列，其中某行数据的第一列、第三列数据分别为 `a` 和 `b`，第二列没有数据，则第二列需要用 `\N` 来表示空值，写作 `a,\N,b`，而不是 `a,,b`。`a,,b` 表示第二列是一个空字符串。

## 基本原理

提交导入作业以后，FE 会生成对应的查询计划，并根据目前可用 BE 的个数和源数据文件的大小，将查询计划分配给多个 BE 执行。每个 BE 负责执行一部分导入任务。BE 在执行过程中，会从外部存储系统拉取数据，并且会在对数据进行预处理之后将数据导入到 StarRocks 中。所有 BE 均完成导入后，由 FE 最终判断导入作业是否成功。

下图展示了 Broker Load 的主要流程：

![Broker Load 原理图](/assets/broker_load_how-to-work_zh.png)

## 准备数据样例

1. 登录 HDFS 集群，在指定路径（假设为 `/user/starrocks/`）下创建两个 CSV 格式的数据文件，`file1.csv` 和 `file2.csv`。两个数据文件都包含三列，分别代表用户 ID、用户姓名和用户得分，如下所示：

   - `file1.csv`

     ```Plain
     1,Lily,21
     2,Rose,22
     3,Alice,23
     4,Julia,24
     ```

   - `file2.csv`

     ```Plain
     5,Tony,25
     6,Adam,26
     7,Allen,27
     8,Jacky,28
     ```

2. 登录 StarRocks 数据库（假设为 `test_db`），创建两张主键模型表，`table1` 和 `table2`。两张表都包含 `id`、`name` 和 `score` 三列，分别代表用户 ID、用户姓名和用户得分，主键为 `id` 列，如下所示：

   ```SQL
   CREATE TABLE `table1`
      (
          `id` int(11) NOT NULL COMMENT "用户 ID",
          `name` varchar(65533) NULL DEFAULT "" COMMENT "用户姓名",
          `score` int(11) NOT NULL DEFAULT "0" COMMENT "用户得分"
      )
          ENGINE=OLAP
          PRIMARY KEY(`id`)
          DISTRIBUTED BY HASH(`id`);
             
   CREATE TABLE `table2`
      (
          `id` int(11) NOT NULL COMMENT "用户 ID",
          `name` varchar(65533) NULL DEFAULT "" COMMENT "用户姓名",
          `score` int(11) NOT NULL DEFAULT "0" COMMENT "用户得分"
      )
          ENGINE=OLAP
          PRIMARY KEY(`id`)
          DISTRIBUTED BY HASH(`id`);
   ```

## 创建导入作业

注意，下述命令以 CSV 格式和简单认证方式为例。有关如何导入其他格式的数据、使用 Kerberos 认证方式时需要配置的参数、以及 HA 配置相关内容，参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md)。

### 导入单个数据文件到单表

#### 操作示例

通过如下语句，把数据文件 `file1.csv` 的数据导入到目标表 `table1`：

```SQL
LOAD LABEL test_db.label_brokerload_singlefile_singletable
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/file1.csv")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    (id, name, score)
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

#### 查询数据

提交导入作业以后，您需要通过 [SHOW LOAD](../sql-reference/sql-statements/data-manipulation/SHOW%20LOAD.md) 语句或者 `curl` 命令来查看导入作业的结果。参见本文“[查看导入作业](#查看导入作业)”小节。

确认导入作业成功以后，您可以使用 [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md) 语句来查询 `table1` 的数据，如下所示：

```SQL
SELECT * FROM table1;
+------+-------+-------+
| id   | name  | score |
+------+-------+-------+
|    1 | Lily  |    21 |
|    2 | Rose  |    22 |
|    3 | Alice |    23 |
|    4 | Julia |    24 |
+------+-------+-------+
4 rows in set (0.01 sec)
```

### 导入多个数据文件到单表

#### 操作示例

通过如下语句，把 HDFS 集群 `/user/starrocks/` 路径下所有数据文件（`file1.csv` 和 `file2.csv`）的数据导入到目标表 `table1`：

```SQL
LOAD LABEL test_db.label_brokerload_allfile_singletable
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/*")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    (id, name, score)
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

#### 查询数据

提交导入作业以后，您需要通过 [SHOW LOAD](../sql-reference/sql-statements/data-manipulation/SHOW%20LOAD.md) 语句或者 `curl` 命令来查看导入作业的结果。参见本文“[查看导入作业](#查看导入作业)”小节。

确认导入作业成功以后，您可以使用 [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md) 语句来查询 `table1` 的数据，如下所示：

```SQL
SELECT * FROM table1;
+------+-------+-------+
| id   | name  | score |
+------+-------+-------+
|    1 | Lily  |    21 |
|    2 | Rose  |    22 |
|    3 | Alice |    23 |
|    4 | Julia |    24 |
|    5 | Tony  |    25 |
|    6 | Adam  |    26 |
|    7 | Allen |    27 |
|    8 | Jacky |    28 |
+------+-------+-------+
4 rows in set (0.01 sec)
```

### 导入多个数据文件到多表

#### 操作示例

通过如下语句，把数据文件 `file1.csv` 和 `file2.csv` 的数据分别导入到目标表 `table1` 和 `table2`：

```SQL
LOAD LABEL test_db.label_brokerload_multiplefile_multipletable
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/file1.csv")
    INTO TABLE table1
    COLUMNS TERMINATED BY ","
    (id, name, score)
    ,
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/file2.csv")
    INTO TABLE table2
    COLUMNS TERMINATED BY ","
    (id, name, score)
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

#### 查询数据

提交导入作业以后，您需要通过 [SHOW LOAD](../sql-reference/sql-statements/data-manipulation/SHOW%20LOAD.md) 语句或者 `curl` 命令来查看导入作业的结果。参见本文“[查看导入作业](#查看导入作业)”小节。

确认导入作业成功以后，您可以使用 [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md) 语句来查询 `table1` 和 `table2` 中的数据：

1. 查询 `table1` 的数据，如下所示：

   ```SQL
   SELECT * FROM table1;
   +------+-------+-------+
   | id   | name  | score |
   +------+-------+-------+
   |    1 | Lily  |    21 |
   |    2 | Rose  |    22 |
   |    3 | Alice |    23 |
   |    4 | Julia |    24 |
   +------+-------+-------+
   4 rows in set (0.01 sec)
   ```

2. 查询 `table2` 的数据，如下所示：

   ```SQL
   SELECT * FROM table2;
   +------+-------+-------+
   | id   | name  | score |
   +------+-------+-------+
   |    5 | Tony  |    25 |
   |    6 | Adam  |    26 |
   |    7 | Allen |    27 |
   |    8 | Jacky |    28 |
   +------+-------+-------+
   4 rows in set (0.01 sec)
   ```

## 查看导入作业

Broker Load 支持通过 SHOW LOAD 语句和 `curl` 命令两种方式来查看导入作业的执行情况。

### 使用 SHOW LOAD 语句

请参见 [SHOW LOAD](/sql-reference/sql-statements/data-manipulation/SHOW%20LOAD.md)。

### 使用 curl 命令

命令语法如下：

```Bash
curl --location-trusted -u <username>:<password> \
    'http://<fe_host>:<fe_http_port>/api/<database_name>/_load_info?label=<label_name>'
```

> **说明**
>
> - 如果账号没有设置密码，这里只需要传入 `<username>:`。
> - 您可以通过 [SHOW FRONTENDS](../sql-reference/sql-statements/Administration/SHOW%20FRONTENDS.md) 命令查看 FE 节点的 IP 地址和 HTTP 端口号。

例如，可以通过如下命令查看上面 `test_db` 数据库中执行的导入作业之一 `label_brokerload_multiplefile_multipletable` 的执行情况：

```Bash
curl --location-trusted -u <username>:<password> \
    'http://<fe_host>:<fe_http_port>/api/test_db/_load_info?label=label_brokerload_multiplefile_multipletable'
```

命令执行后，以 JSON 格式返回导入作业的结果信息 `jobInfo`，如下所示：

```JSON
{"jobInfo":{"dbName":"test_db","tblNames":["table1","table2"],"label":"label_brokerload_multiplefile_multipletable","state":"FINISHED","failMsg":"","trackingUrl":""},"status":"OK","msg":"Success"}
```

`jobInfo` 中包含如下参数：

| **参数**    | **说明**                                                     |
| ----------- | ------------------------------------------------------------ |
| dbName      | 目标 StarRocks 表所在的数据库的名称。                               |
| tblNames    | 目标 StarRocks 表的名称。                        |
| label       | 导入作业的标签。                                             |
| state       | 导入作业的状态，包括：<ul><li>`PENDING`：导入作业正在等待执行中。</li><li>`QUEUEING`：导入作业正在等待执行中。</li><li>`LOADING`：导入作业正在执行中。</li><li>`PREPARED`：事务已提交。</li><li>`FINISHED`：导入作业成功。</li><li>`CANCELLED`：导入作业失败。</li></ul>请参见[异步导入](/loading/Loading_intro.md#异步导入)。 |
| failMsg     | 导入作业的失败原因。当导入作业的状态为`PENDING`，`LOADING`或`FINISHED`时，该参数值为`NULL`。当导入作业的状态为`CANCELLED`时，该参数值包括 `type` 和 `msg` 两部分：<ul><li>`type` 包括如下取值：</li><ul><li>`USER_CANCEL`：导入作业被手动取消。</li><li>`ETL_SUBMIT_FAIL`：导入任务提交失败。</li><li>`ETL-QUALITY-UNSATISFIED`：数据质量不合格，即导入作业的错误数据率超过了 `max-filter-ratio`。</li><li>`LOAD-RUN-FAIL`：导入作业在 `LOADING` 状态失败。</li><li>`TIMEOUT`：导入作业未在允许的超时时间内完成。</li><li>`UNKNOWN`：未知的导入错误。</li></ul><li>`msg` 显示有关失败原因的详细信息。</li></ul> |
| trackingUrl | 导入作业中质量不合格数据的访问地址。可以使用 `curl` 命令或 `wget` 命令访问该地址。如果导入作业中不存在质量不合格的数据，则返回空值。 |
| status      | 导入请求的状态，包括 `OK` 和 `Fail`。                        |
| msg         | HTTP 请求的错误信息。                                        |

### 取消导入作业

当导入作业状态不为 **CANCELLED** 或 **FINISHED** 时，可以通过 [CANCEL LOAD](/sql-reference/sql-statements/data-manipulation/CANCEL%20LOAD.md) 语句来取消该导入作业。

例如，可以通过以下语句，撤销 `test_db` 数据库中标签为 `label1` 的导入作业：

```SQL
CANCEL LOAD
FROM test_db
WHERE LABEL = "label1";
```

## 作业拆分与并行执行

一个 Broker Load 作业会拆分成一个或者多个子任务并行处理，一个作业的所有子任务作为一个事务整体成功或失败。作业的拆分通过 `LOAD LABEL` 语句中的 `data_desc` 参数来指定：

- 如果声明多个 `data_desc` 参数对应导入多张不同的表，则每张表数据的导入会拆分成一个子任务。

- 如果声明多个 `data_desc` 参数对应导入同一张表的不同分区，则每个分区数据的导入会拆分成一个子任务。

每个子任务还会拆分成一个或者多个实例，然后这些实例会均匀地被分配到 BE 上并行执行。实例的拆分由以下 [FE 配置](/administration/Configuration.md#配置-fe-动态参数)决定：

- `min_bytes_per_broker_scanner`：单个实例处理的最小数据量，默认为 64 MB。

- `load_parallel_instance_num`：单个 BE 上每个作业允许的并发实例数，默认为 1 个。

   可以使用如下公式计算单个子任务的实例总数：

   单个子任务的实例总数 = min（单个子任务待导入数据量的总大小/`min_bytes_per_broker_scanner`，`load_parallel_instance_num` x BE 总数）

一般情况下，一个导入作业只有一个 `data_desc`，只会拆分成一个子任务，子任务会拆分成与 BE 总数相等的实例。

## 相关配置项

[FE 配置项](../administration/Configuration.md#fe-配置项) `max_broker_load_job_concurrency` 指定了 StarRocks 集群中可以并行执行的 Broker Load 任务的最大数量。

StarRocks v2.4 及以前版本中，如果某一时间段内提交的 Broker Load 作业的任务总数超过最大数量，则超出作业会按照各自的提交时间放到队列中排队等待调度。

StarRocks v2.5 版本中，如果某一时间段内提交的 Broker Load 作业的任务总数超过最大数量，则超出的作业会按照作业创建时指定的优先级被放到队列中排队等待调度。参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md#opt_properties) 文档中的可选参数 `priority`。您可以使用 [ALTER LOAD](../sql-reference/sql-statements/data-manipulation/ALTER%20LOAD.md) 语句修改处于 **QUEUEING** 状态或者 **LOADING** 状态的 Broker Load 作业的优先级。

## 常见问题

请参见 [Broker Load 常见问题](/faq/loading/Broker_load_faq.md)。
