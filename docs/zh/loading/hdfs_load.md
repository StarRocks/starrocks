---
displayed_sidebar: "Chinese"
toc_max_heading_level: 4
keywords: ['Broker Load']
---

# 从 HDFS 导入

import InsertPrivNote from '../assets/commonMarkdown/insertPrivNote.md'

StarRocks 支持通过 [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 从 HDFS 导入数据。

作为一种异步的导入方式，Broker Load 负责建立与 GCS 的连接、拉取数据、并将数据存储到 StarRocks 中。

当前支持 Parquet、ORC、及 CSV 三种文件格式。

## Broker Load 优势

- Broker Load 在后台运行，客户端不需要保持连接也能确保导入作业不中断。
- Broker Load 作业默认超时时间为 4 小时，适合导入数据较大、导入运行时间较长的场景。
- 除 Parquet 和 ORC 文件格式，Broker Load 还支持 CSV 文件格式。

## 工作原理

![Broker Load 原理图](../assets/broker_load_how-to-work_zh.png)

1. 用户创建导入作业。
2. FE 生成查询计划，然后把查询计划拆分并分分配给各个 BE（或 CN）执行。
3. 各个 BE（或 CN）从数据源拉取数据并把数据导入到 StarRocks 中。

## 准备工作

### 准备数据源

确保待导入数据已保存在 HDFS 集群。本文假设待导入的数据文件为 `/user/amber/user_behavior_ten_million_rows.parquet`。

### 查看权限

<InsertPrivNote />

### 获取资源访问配置

可以使用简单认证方式访问 HDFS，需要您提前获取用于访问 HDFS 集群中 NameNode 节点的用户名和密码。

## 操作示例

创建 StarRocks 表，启动导入作业从 HDFS 集群拉取数据文件 `/user/amber/user_behavior_ten_million_rows.parquet` 的数据，然后验证导入过程和结果是否成功。

### 建库建表

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
DISTRIBUTED BY HASH(UserID);
```

### 提交导入作业

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

### 查看导入进度

通过 `information_schema.loads` 视图查看导入作业的进度。该功能自 3.1 版本起支持。

```SQL
SELECT * FROM information_schema.loads;
```

有关 `loads` 视图提供的字段详情，参见 [Information Schema](../sql-reference/information_schema.md#loads)。

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
