---
displayed_sidebar: docs
sidebar_label: 集群快照
---

# 自动化集群快照

本文介绍如何在存算分离集群上启用自动化集群快照以实现灾难恢复。

此功能从 v3.4.2 开始支持，仅在存算分离集群上可用。

## 概述

存算分离集群的灾难恢复基本思想是确保完整的集群状态（包括数据和元数据）存储在对象存储中。这样，如果集群发生故障，只要数据和元数据保持完整，就可以从对象存储中恢复。此外，云提供商提供的备份和跨区域复制等功能可以用于实现远程恢复和跨区域灾难恢复。

在存算分离集群中，CN 状态（数据）存储在对象存储中，但 FE 状态（元数据）仍然存储于本地。为了确保对象存储中包含所有用于恢复的集群状态，StarRocks 现在支持在对象存储中自动化集群快照，包括数据和元数据。

### 术语

- **集群快照**

  集群快照指的是在某一时刻的集群状态快照。它包含集群中的所有对象，如 Catalog、数据库、表、用户和权限、导入任务等。它不包括所有外部依赖对象，如 External Catalog 的配置文件和本地 UDF JAR 包。

- **自动化集群快照**

  系统自动维护一个紧随最新集群状态的快照。历史快照将在最新快照创建后立即被删除，始终只保留一个快照。目前，自动化集群快照的任务仅能由系统触发，不支持手动创建快照。

- **集群恢复**

  从快照中恢复集群。

## 使用方法

### 启用自动化集群快照

自动化集群快照默认禁用。

您需要使用以下语句启用此功能：

语法：

```SQL
ADMIN SET AUTOMATED CLUSTER SNAPSHOT ON
[STORAGE VOLUME <storage_volume_name>]
```

参数：

`storage_volume_name`: 指定用于存储快照的存储卷。如果未指定此参数，将使用默认存储卷。

每次 FE 在完成元数据 CheckPoint 后创建新的元数据 Image 时，会自动创建一个快照。快照的名称由系统生成，格式为 `automated_cluster_snapshot_{timestamp}`。

元数据快照存储在 `/{storage_volume_locations}/{service_id}/meta/image/automated_cluster_snapshot_timestamp` 下。数据快照存储在与原始数据相同的位置。

FE 配置项 `automated_cluster_snapshot_interval_seconds` 控制快照自动化周期。默认值为 1800 秒（30 分钟）。

### 禁用自动化集群快照

使用以下语句禁用自动化集群快照：

```SQL
ADMIN SET AUTOMATED CLUSTER SNAPSHOT OFF
```

一旦禁用自动化集群快照，系统将自动清除历史快照。

### 查看集群快照

您可以通过查询视图 `information_schema.cluster_snapshots` 来查看最新的集群快照和尚未删除的快照。

```SQL
SELECT * FROM information_schema.cluster_snapshots;
```

返回：

| 字段                | 描述                                                         |
| ------------------ | ------------------------------------------------------------ |
| snapshot_name      | 快照的名称。                                                 |
| snapshot_type      | 快照的类型。目前仅支持 `automated`。                         |
| created_time       | 快照创建的时间。                                             |
| fe_journal_id      | FE 日志的 ID。                                               |
| starmgr_journal_id | StarManager 日志的 ID。                                      |
| properties         | 应用于尚不可用的功能。                                       |
| storage_volume     | 存储快照的存储卷。                                           |
| storage_path       | 存储快照的存储路径。                                         |

### 查看集群快照任务

您可以查询视图 `information_schema.cluster_snapshot_jobs` 来查看集群快照的任务信息。

```SQL
SELECT * FROM information_schema.cluster_snapshot_jobs;
```

返回：

| 字段                | 描述                                                         |
| ------------------ | ------------------------------------------------------------ |
| snapshot_name      | 快照的名称。                                                 |
| job_id             | 任务的 ID。                                                  |
| created_time       | 任务创建的时间。                                             |
| finished_time      | 任务完成的时间。                                             |
| state              | 任务的状态。有效值：`INITIALIZING`、`SNAPSHOTING`、`FINISHED`、`EXPIRED`、`DELETED` 和 `ERROR`。 |
| detail_info        | 当前执行阶段的具体进度信息。                                 |
| error_message      | 任务的错误信息（如果有）。                                   |

### 恢复集群

按照以下步骤使用集群快照恢复集群。

1. **（可选）** 如果存储集群快照的存储位置（存储卷）发生了变化，必须将原始存储路径下的所有文件复制到新路径。为此，您必须修改 Leader FE 节点的 `fe/conf` 目录下的配置文件 **cluster_snapshot.yaml**。有关 **cluster_snapshot.yaml** 的模板，请参见[附录](#附录)。

2. 启动 Leader FE 节点。

   ```Bash
   ./fe/bin/start_fe.sh --cluster_snapshot --daemon
   ```

3. **清空 `meta` 目录后**，启动其他 FE 节点。

   ```Bash
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

4. **清空 `storage_root_path` 目录后**，启动 CN 节点。

   ```Bash
   ./be/bin/start_cn.sh --daemon
   ```

如果您在步骤 1 中修改了 **cluster_snapshot.yaml**，节点和存储卷将在新集群中根据文件中的信息重新配置。

## 附录

**cluster_snapshot.yaml** 模板：

```Yaml
# 要下载的用于恢复的集群快照的信息。
cluster_snapshot:
    # 快照的 URI。
    # 示例一：s3://defaultbucket/test/f7265e80-631c-44d3-a8ac-cf7cdc7adec811019/meta/image/automated_cluster_snapshot_1704038400000
    # 示例二：s3://defaultbucket/test/f7265e80-631c-44d3-a8ac-cf7cdc7adec811019/meta
    cluster_snapshot_path: <cluster_snapshot_uri>
    # 用于存储快照的存储卷名称。您必须在 `storage_volumes` 部分中定义它。
    # 注意：它必须与原始集群中的名称相同。
    storage_volume_name: my_s3_volume

# [可选] 要恢复快照的新集群的节点信息。
# 如果未指定此部分，恢复后的集群将只有 Leader FE 节点，CN 节点将保留原始集群的信息。
# 注意：请勿在此部分中指定 Leader FE 节点。

frontends:
    # FE Host。
  - host: xxx.xx.xx.x1
    # FE edit_log_port。
    edit_log_port: 9010
    # FE 节点类型。有效值：`follower`（默认）和 `observer`。
    type: follower
  - host: xxx.xx.xx.x2
    edit_log_port: 9010
    type: observer

compute_nodes:
    # CN Host。
  - host: xxx.xx.xx.x3
    # CN heartbeat_service_port。
    heartbeat_service_port: 9050
  - host: xxx.xx.xx.x4
    heartbeat_service_port: 9050

# 新集群中的存储卷信息。用于恢复克隆的快照。
# 注意：存储卷的名称必须与原始集群中的名称相同。
storage_volumes:
  # S3 兼容存储卷示例。
  - name: my_s3_volume
    type: S3
    location: s3://defaultbucket/test/
    comment: my s3 volume
    properties:
      - key: aws.s3.region
        value: us-west-2
      - key: aws.s3.endpoint
        value: https://s3.us-west-2.amazonaws.com
      - key: aws.s3.access_key
        value: xxxxxxxxxx
      - key: aws.s3.secret_key
        value: yyyyyyyyyy
  # HDFS 存储卷示例。
  - name: my_hdfs_volume
    type: HDFS
    location: hdfs://127.0.0.1:9000/sr/test/
    comment: my hdfs volume
    properties:
      - key: hadoop.security.authentication
        value: simple
      - key: username
        value: starrocks
```