---
displayed_sidebar: "Chinese"
toc_max_heading_level: 4
---

# 从 GCS 导入

import InsertPrivNote from '../assets/commonMarkdown/insertPrivNote.md'

StarRocks 支持通过 [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 从 Google Cloud Storage（简称 GCS）导入数据。

作为一种异步的导入方式，Broker Load 负责建立与 GCS 的连接、拉取数据、并将数据存储到 StarRocks 中。

当前支持 Parquet、ORC、及 CSV 三种文件格式。

## Broker Load 优势

- Broker Load 在后台运行，客户端不需要保持连接也能确保导入作业不中断。
- Broker Load 作业默认超时时间为 4 小时，适合导入数据较大、导入运行时间较长的场景。
- 除 Parquet 和 ORC 文件格式，Broker Load 还支持 CSV 文件格式。

## 工作原理

![Broker Load 原理图](../assets/broker_load_how-to-work_zh.png)

1. 用户创建导入作业。
2. FE 生成查询计划，然后把查询计划拆分并分分配给各个 BE 执行。
3. 各个 BE 从数据源拉取数据并把数据导入到 StarRocks 中。

## 准备工作

### 准备数据源

确保待导入数据已保存在 GCS 存储桶。建议您将数据保存在与 StarRocks 集群同处一个地域（Region）的 存储桶，这样可以降低数据传输成本。

本文中，我们提供了样例数据集 `gs://starrocks-samples/user_behavior_ten_million_rows.parquet`，对所有合法的 GCP 用户开放。您只要配置真实有效的安全凭证，即可访问该数据集。

### 查看权限

<InsertPrivNote />

### 获取资源访问配置

本文的示例均使用基于 Service Account 的认证方式，您需要提前获取以下 CGS 资源信息：

- 数据所在的 GCS 存储桶
- GCS 对象键（或“对象名称”）（只在访问 GCS 存储桶中某个特定数据对象时才需要。注意，如果要访问的数据对象保存在子文件夹下，其名称可以包含前缀。）
- GCS 存储桶所在的 GCS 地域（Region）
- Google Cloud 服务账号 (Service Account) 的 `private_ key_id`、`private_key`、及 `client_email`。

有关 StarRocks 支持的其他认证方式，参见 [配置 GCS 认证信息](../integrations/authenticate_to_gcs.md)。

## 操作示例

创建 StarRocks 表，启动导入作业从 AGCS 拉取样例数据集  `gs://starrocks-samples/user_behavior_ten_million_rows.parquet` 的数据，然后验证导入过程和结果是否成功。

### 建库建表

通过如下语句创建数据库、并切换至该数据库：

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

通过如下语句手动创建表（建议表结构与您在 GCS 存储的待导入数据结构一致）：

```SQL
CREATE TABLE user_behavior
(
    UserID int(11),
    ItemID int(11),
    CategoryID int(11),
    BehaviorType varchar(65533),
    Timestamp varbinary
)
ENGINE = OLAP 
DUPLICATE KEY(UserID)
DISTRIBUTED BY HASH(UserID);
```

### 提交导入作业

执行如下命令创建 Broker Load 作业，把样例数据集 `gs://starrocks-samples/user_behavior_ten_million_rows.parquet` 中的数据导入到表 `user_behavior` 中：

```SQL
LOAD LABEL user_behavior
(
    DATA INFILE("gs://starrocks-samples/user_behavior_ten_million_rows.parquet")
    INTO TABLE user_behavior
    FORMAT AS "parquet"
 )
 WITH BROKER
 (
 
    "gcp.gcs.service_account_email" = "sampledatareader@xxxxx-xxxxxx-000000.iam.gserviceaccount.com",
    "gcp.gcs.service_account_private_key_id" = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "gcp.gcs.service_account_private_key" = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
 )
PROPERTIES
(
    "timeout" = "72000"
);
```

> **NOTE**
>
> 把上面命令示例中的访问凭证替换成您自己的访问凭证。由于这里使用的数据对象对所有合法的 GCP 用户开放，因此您填入任何真实有效的 Google Cloud 服务账号的 `private_ key_id`、`private_key`、及 `client_email` 都可以。

导入语句包含四个部分：

- `LABEL`：导入作业的标签，字符串类型，可用于查询导入作业的状态。
- `LOAD` 声明：包括源数据文件所在的 URI、源数据文件的格式、以及目标表的名称等作业描述信息。
- `BROKER`：连接数据源的认证信息配置。
- `PROPERTIES`：用于指定超时时间等可选的作业属性。

有关详细的语法和参数说明，参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

### 查看导入进度

通过 StarRocks Information Schema 库中的 [`loads`](../administration/information_schema.md#loads) 视图查看导入作业的进度。该功能自 3.1 版本起支持。

```SQL
SELECT * FROM information_schema.loads;
```

有关 `loads` 视图提供的字段详情，参见 [Information Schema](../administration/information_schema.md#loads)。

如果您提交了多个导入作业，您可以通过 `LABEL` 过滤出想要查看的作业。例如：

```SQL
SELECT * FROM information_schema.loads WHERE LABEL = 'user_behavior';
```

例如，在下面的返回结果中，有两条关于导入作业 `user_behavior` 的记录：

- 第一条记录显示导入作业的状态为 `CANCELLED`。通过记录中的 `ERROR_MSG` 字段，可以确定导致作业出错的原因是 `listPath failed`。
- 第二条记录显示导入作业的状态为 `FINISHED`，表示作业成功。

```Plain
JOB_ID|LABEL                                      |DATABASE_NAME|STATE    |PROGRESS           |TYPE  |PRIORITY|SCAN_ROWS|FILTERED_ROWS|UNSELECTED_ROWS|SINK_ROWS|ETL_INFO|TASK_INFO                                           |CREATE_TIME        |ETL_START_TIME     |ETL_FINISH_TIME    |LOAD_START_TIME    |LOAD_FINISH_TIME   |JOB_DETAILS                                                                                                                                                                                                                                                    |ERROR_MSG                             |TRACKING_URL|TRACKING_SQL|REJECTED_RECORD_PATH|
------+-------------------------------------------+-------------+---------+-------------------+------+--------+---------+-------------+---------------+---------+--------+----------------------------------------------------+-------------------+-------------------+-------------------+-------------------+-------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------+------------+------------+--------------------+
 10121|user_behavior                              |mydatabase   |CANCELLED|ETL:N/A; LOAD:N/A  |BROKER|NORMAL  |        0|            0|              0|        0|        |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|2023-08-10 14:59:30|                   |                   |                   |2023-08-10 14:59:34|{"All backends":{},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":0,"InternalTableLoadRows":0,"ScanBytes":0,"ScanRows":0,"TaskNumber":0,"Unfinished backends":{}}                                                                                        |type:ETL_RUN_FAIL; msg:listPath failed|            |            |                    |
 10106|user_behavior                              |mydatabase   |FINISHED |ETL:100%; LOAD:100%|BROKER|NORMAL  | 86953525|            0|              0| 86953525|        |resource:N/A; timeout(s):72000; max_filter_ratio:0.0|2023-08-10 14:50:15|2023-08-10 14:50:19|2023-08-10 14:50:19|2023-08-10 14:50:19|2023-08-10 14:55:10|{"All backends":{"a5fe5e1d-d7d0-4826-ba99-c7348f9a5f2f":[10004]},"FileNumber":1,"FileSize":1225637388,"InternalTableLoadBytes":2710603082,"InternalTableLoadRows":86953525,"ScanBytes":1225637388,"ScanRows":86953525,"TaskNumber":1,"Unfinished backends":{"a5|                                      |            |            |                    |
```

导入作业完成后，您可以从表内查询数据，验证数据是否已成功导入。例如：

```SQL
SELECT * from user_behavior LIMIT 3;
```

系统返回类似如下查询结果，表明数据已经成功导入：

```Plain
+--------+---------+------------+--------------+---------------------+
| UserID | ItemID  | CategoryID | BehaviorType | Timestamp           |
+--------+---------+------------+--------------+---------------------+
|    142 | 2869980 |    2939262 | pv           | 2017-11-25 03:43:22 |
|    142 | 2522236 |    1669167 | pv           | 2017-11-25 15:14:12 |
|    142 | 3031639 |    3607361 | pv           | 2017-11-25 15:19:25 |
+--------+---------+------------+--------------+---------------------+
```
