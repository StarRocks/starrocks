---
keywords: ['xiugai'] 
displayed_sidebar: docs
---

# ALTER SYSTEM

## 功能

管理 StarRocks 集群中的 FE、BE、CN 和 Broker。

> **注意**
>
> 只有拥有 `cluster_admin` 角色的用户才可以执行集群管理相关操作。

## 语法和参数说明

### FE

- 添加 Follower FE。添加后，可使用 `SHOW PROC '/frontends'\G` 命令查看新增FE 的状态。

   ```SQL
    ALTER SYSTEM ADD FOLLOWER "host:edit_log_port"[, ...]
    ```

- 删除 Follower FE。

    ```SQL
    ALTER SYSTEM DROP FOLLOWER "host:edit_log_port"[, ...]
    ```

- 添加 Observer FE。添加后，可通过 `SHOW PROC '/frontends'\G` 命令查看新增FE 的状态。

    ```SQL
    ALTER SYSTEM ADD OBSERVER "host:edit_log_port"[, ...]
    ```

- 删除 Observer FE。

    ```SQL
    ALTER SYSTEM DROP OBSERVER "host:edit_log_port"[, ...]
    ```

     参数说明如下：

    | **参数**           | **必选** | **说明**                                                     |
    | ------------------ | -------- | ------------------------------------------------------------ |
    | host:edit_log_port | 是       | <ul><li>`host`：FE 机器的 FQDN 或 IP 地址。如果机器存在多个 IP 地址，则该参数取值应为 `priority_networks` 配置项下设定的唯一通信 IP 地址。</li><li>`edit_log_port`：FE 上的 BDBJE 通信端口，默认为 `9010`。</li></ul> |

- 创建 image。

    ```SQL
    ALTER SYSTEM CREATE IMAGE
    ```

    执行该语句会主动触发 Leader FE 创建新的 Image（元数据快照）文件。该操作异步执行，您可以通过查看 FE 日志文件 **fe.log** 确认操作何时开始执行及结束。`triggering a new checkpoint manually...` 表明操作开始执行，`finished save image...` 则表明 Image 创建完成。

### BE

- 添加 BE。 添加后，可通过 [SHOW BACKENDS](SHOW_BACKENDS.md) 查看新增 BE 的状态。

    ```SQL
    ALTER SYSTEM ADD BACKEND "host:heartbeat_service_port"[, ...]
    ```

- 删除 BE。如果有表是单副本且该表的部分 tablet 分布在要删除的 BE 上，则不允许删除该 BE。

    ```SQL
    ALTER SYSTEM DROP BACKEND "host:heartbeat_service_port"[, ...]
    ```

- 下线 BE。

    ```SQL
    ALTER SYSTEM DECOMMISSION BACKEND "host:heartbeat_service_port"[, ...]
    ```

    下线前，该 BE 上的数据会迁移到其他 BE 上，过程中不影响数据导入和查询。下线 BE 为异步操作，可通过 [SHOW BACKENDS](SHOW_BACKENDS.md) 语句查看是否下线成功，如下线成功，该 BE 不会在 SHOW BACKENDS 返回的信息中显示。您可以手动撤销下线操作，详情参见 [CANCEL DECOMMISSION](CANCEL_DECOMMISSION.md)。

    参数说明如下：

    | **参数**                    | **必选** | **说明**                                                     |
    | --------------------------- | -------- | ------------------------------------------------------------ |
    | host:heartbeat_service_port | 是       |<ul><li> `host`：BE 机器的 FQDN 或 IP 地址。如果机器存在多个 IP 地址，则该参数取值应为 `priority_networks` 配置项下设定的唯一通信 IP 地址。</li><li>`heartbeat_service_port`：BE 的心跳端口，用于接收来自 FE 的心跳，默认为 `9050`。</li></ul> |

### CN

- 添加 CN。 添加后，可通过 `SHOW PROC '/compute_nodes'\G` 查看新增 CN 的状态。

    ```SQL
    ALTER SYSTEM ADD COMPUTE NODE "host:heartbeat_service_port"[, ...]
    ```

- 删除 CN。

    ```SQL
    ALTER SYSTEM DROP COMPUTE NODE "host:heartbeat_service_port"[, ...]
    ```

> **说明**
>
> CN 暂不支持通过 `ALTER SYSTEM DECOMMISSION` 命令下线。

    参数说明如下：

    | **参数**                    | **必选** | **说明**                                                     |
    | --------------------------- | -------- | ------------------------------------------------------------ |
    | host:heartbeat_service_port | 是       |<ul><li> `host`：CN 机器的 FQDN 或 IP 地址。如果机器存在多个 IP 地址，则该参数取值应为 `priority_networks` 配置项下设定的唯一通信 IP 地址。</li><li>`heartbeat_service_port`：CN 的心跳端口，用于接收来自 FE 的心跳，默认为 `9050`。</li></ul> |

### Broker

- 添加 Broker。添加后，您可以使用 Broker Load 将 HDFS 或外部云存储系统中的数据导入到 StarRocks 中。详情参见[从 HDFS 导入](../../../../loading/hdfs_load.md)或[从云存储导入](../../../../loading/cloud_storage_load.md)。

    ```SQL
    ALTER SYSTEM ADD BROKER broker_name "host:port"[, ...]
    ```

    在一条 SQL 语句中，如同时添加多个 Broker（一个`host:port`为一个 Broker），那么这些 Broker 共用同一个 `broker_name`。添加后，可通过 [SHOW BROKER](SHOW_BROKER.md) 语句查看 Broker 的详细信息。

- 删除 Broker。注意如一个 Broker 上有正在执行的导入任务，那么删除该 Broker 会导致该任务中断。

  - 删除 `broker_name` 下的一个或多个 Broker。

      ```SQL
      ALTER SYSTEM DROP BROKER broker_name "host:broker_ipc_port"[, ...]
      ```

  - 删除所有名为 `broker_name` 下的 Broker。

      ```SQL
      ALTER SYSTEM DROP ALL BROKER broker_name
      ```

     参数说明如下：

    | **参数**             | **必选** | **说明**                                                     |
    | -------------------- | -------- | ------------------------------------------------------------ |
    | broker_name          | 是       | 一个 Broker 的名称或多个 Broker 共用的名称。                 |
    | host:broker_ipc_port | 是       | <ul><li>`host`：Broker 机器的 FQDN 或 IP 地址。</li><li>`broker_ipc_port`：Broker 上的 thrift server 端口，用于接受 FE 或 BE 的请求，默认为 `8000`。</li></ul> |

## 使用说明

添加和删除 FE、添加和删除 BE 以及添加和删除 Broker 均为同步操作。执行删除语句后，FE、BE 或 Broker 会直接删除，不可手动撤销该操作。

## 示例

示例一：添加一个 Follower FE。

```SQL
ALTER SYSTEM ADD FOLLOWER "x.x.x.x:9010";
```

示例二：同时删除两个 Observer FE。

```SQL
ALTER SYSTEM DROP OBSERVER "x.x.x.x:9010","x.x.x.x:9010";
```

示例三：添加一个 BE。

```SQL
ALTER SYSTEM ADD BACKEND "x.x.x.x:9050";
```

示例四：同时删除两个 BE。

```SQL
ALTER SYSTEM DROP BACKEND "x.x.x.x:9050", "x.x.x.x:9050";
```

示例五：同时下线两个 BE。

```SQL
ALTER SYSTEM DECOMMISSION BACKEND "x.x.x.x:9050", "x.x.x.x:9050";
```

示例六：同时添加两个名为 `hdfs` 的 Broker。

```SQL
ALTER SYSTEM ADD BROKER hdfs "x.x.x.x:8000", "x.x.x.x:8000";
```

示例七：删除 `amazon_s3` 下的两个 Broker。

```SQL
ALTER SYSTEM DROP BROKER amazon_s3 "x.x.x.x:8000", "x.x.x.x:8000";
```

示例八：删除 `amazon_s3` 下的所有 Broker。

```SQL
ALTER SYSTEM DROP ALL BROKER amazon_s3;
```
