---
displayed_sidebar: "Chinese"
toc_max_heading_level: 4
---

# 从 Microsoft Azure Storage 导入

import InsertPrivNote from '../assets/commonMarkdown/insertPrivNote.md'

StarRocks 支持通过 [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 从Microsoft Azure Storage 导入数据。

作为一种异步的导入方式，Broker Load 负责建立与 Azure 的连接、拉取数据、并将数据存储到 StarRocks 中。

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

确保待导入数据已保存在您 Azure 服务账号（Service Account）下的容器（Container）。

本文中，我们提供了 Parquet 格式的样例数据集 (`user_behavior_ten_million_rows.parquet`，保存在 Azure Data Lake Storage Gen2（简称 ADLS Gen2）服务账号 (`starrocks`) 下容器 (`starrocks-container`) 的根目录里。

### 查看权限

<InsertPrivNote />

### 获取资源访问配置

本文的示例使用基于 Shared Key 的认证方式。为确保您能顺利访问存储在 ADLS Gen2 中的数据，建议您参照“[Azure Data Lake Storage Gen2 > Shared Key (access key of storage account)](../integrations/authenticate_to_azure_storage.md#基于-shared-key-认证鉴权) 中的介绍，理解该认证方式下需要配置哪些认证参数。

概括来说，如果选择基于 Shared Key 的认证方式，您需要提前获取以下资源信息：

- ADLS Gen2 存储账号的用户名
- ADLS Gen2 账号的 Shared Key（即 Access Key）

有关 StarRocks 支持的其他认证方式，参见 [Authenticate to Azure cloud storage](../integrations/authenticate_to_azure_storage.md)。

## 操作示例

创建 StarRocks 表，启动导入作业从 Azure 拉取样例数据集 `user_behavior_ten_million_rows.parquet` 的数据，然后验证导入过程和结果是否成功。

### 建库建表

登录目标 StarRocks 集群，然后通过如下语句创建数据库、并切换至该数据库：

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

通过如下语句手动创建表（建议表结构与您在 Azure 存储的待导入数据结构一致）：

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

执行如下命令创建 Broker Load 作业，把样例数据集 `user_behavior_ten_million_rows.parquet` 中的数据导入到表 `user_behavior`中：

```SQL
LOAD LABEL user_behavior
(
    DATA INFILE("abfss://starrocks-container@starrocks.dfs.core.windows.net/user_behavior_ten_million_rows.parquet")
    INTO TABLE user_behavior
    FORMAT AS "parquet"
)
WITH BROKER
(
    "azure.adls2.storage_account" = "starrocks",
    "azure.adls2.shared_key" = "xxxxxxxxxxxxxxxxxx"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

导入语句包含四个部分：

- `LABEL`：导入作业的标签，字符串类型，可用于查询导入作业的状态。
- `LOAD` 声明：包括源数据文件所在的 URI、源数据文件的格式、以及目标表的名称等作业描述信息。
- `BROKER`：连接数据源的认证信息配置。
- `PROPERTIES`：用于指定超时时间等可选的作业属性。

有关详细的语法和参数说明，参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

### 查看导入进度

通过 StarRocks Information Schema 库中的 [`loads`](../administration/information_schema.md#loads) 视图查看导入作业的进度。该功能自 3.1 版本起支持。

```SQL
SELECT * FROM information_schema.loads \G
```

如果您提交了多个导入作业，您可以通过 `LABEL` 过滤出想要查看的作业。例如：

```SQL
SELECT * FROM information_schema.loads WHERE LABEL = 'user_behavior' \G
*************************** 1. row ***************************
              JOB_ID: 10250
               LABEL: user_behavior
       DATABASE_NAME: mydatabase
               STATE: FINISHED
            PROGRESS: ETL:100%; LOAD:100%
                TYPE: BROKER
            PRIORITY: NORMAL
           SCAN_ROWS: 10000000
       FILTERED_ROWS: 0
     UNSELECTED_ROWS: 0
           SINK_ROWS: 10000000
            ETL_INFO:
           TASK_INFO: resource:N/A; timeout(s):3600; max_filter_ratio:0.0
         CREATE_TIME: 2023-12-28 16:15:19
      ETL_START_TIME: 2023-12-28 16:15:25
     ETL_FINISH_TIME: 2023-12-28 16:15:25
     LOAD_START_TIME: 2023-12-28 16:15:25
    LOAD_FINISH_TIME: 2023-12-28 16:16:31
         JOB_DETAILS: {"All backends":{"6a8ef4c0-1009-48c9-8d18-c4061d2255bf":[10121]},"FileNumber":1,"FileSize":132251298,"InternalTableLoadBytes":311710786,"InternalTableLoadRows":10000000,"ScanBytes":132251298,"ScanRows":10000000,"TaskNumber":1,"Unfinished backends":{"6a8ef4c0-1009-48c9-8d18-c4061d2255bf":[]}}
           ERROR_MSG: NULL
        TRACKING_URL: NULL
        TRACKING_SQL: NULL
REJECTED_RECORD_PATH: NULL
```

有关 `loads` 视图提供的字段详情，参见 [Information Schema](../administration/information_schema.md#loads)。

导入作业完成后，您可以从表内查询数据，验证数据是否已成功导入。例如：

```SQL
SELECT * from user_behavior LIMIT 3;
```

系统返回类似如下查询结果，表明数据已经成功导入：

```SQL
+--------+---------+------------+--------------+---------------------+
| UserID | ItemID  | CategoryID | BehaviorType | Timestamp           |
+--------+---------+------------+--------------+---------------------+
|    142 | 2869980 |    2939262 | pv           | 2017-11-25 03:43:22 |
|    142 | 2522236 |    1669167 | pv           | 2017-11-25 15:14:12 |
|    142 | 3031639 |    3607361 | pv           | 2017-11-25 15:19:25 |
+--------+---------+------------+--------------+---------------------+
```
