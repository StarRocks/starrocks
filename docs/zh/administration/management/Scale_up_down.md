---
displayed_sidebar: docs
---

# 扩容缩容 StarRocks

本文介绍如何扩容以及缩容 StarRocks 集群。

## 扩缩容 FE 集群

StarRocks FE 节点分为 Follower 节点和 Observer 节点。Follower 节点参与选举投票和写入，Observer 节点只用来同步日志，扩展读性能。

> 注意：
>
> * 所有 FE 节点的 `http_port` 必须相同。
> * Follower FE 节点（包括 Leader 节点）的数量推荐为奇数。建议部署 3 个 Follower 节点，以组成高可用部署（HA）模式。
> * 当 FE 集群已经为高可用部署模式时（即包含 1 个 Leader 节点，2 个 Follower 节点），建议您通过增加 Observer 节点来扩展 FE 的读服务能力。

### 扩容 FE 集群

部署并启动新增 FE 节点。详细部署方式参考 [部署 StarRocks](../../deployment/deploy_manually.md)。

```bash
bin/start_fe.sh --helper "fe_leader_host:edit_log_port" --daemon
```

`fe_leader_host`： Leader FE 节点的 IP 地址。

扩容 FE 集群。您可以将新增节点设定为 Follower 或 Observer 节点。

* 将新增节点设定为 Follower 节点。

```sql
ALTER SYSTEM ADD follower "fe_host:edit_log_port";
```

* 将新增节点设定为 Observer 节点。

```sql
ALTER SYSTEM ADD observer "fe_host:edit_log_port";
```

完成后，您可以查看节点信息验证扩容是否成功。

```sql
SHOW PROC '/frontends';
```

### 缩容 FE 集群

您可以删除 Follower 或 Observer 节点。

* 删除 Follower 节点。

```sql
ALTER SYSTEM DROP follower "fe_host:edit_log_port";
```

* 删除 Observer 节点。

```sql
ALTER SYSTEM DROP observer "fe_host:edit_log_port";
```

完成后，您可以查看节点信息验证缩容是否成功。

```sql
SHOW PROC '/frontends';
```

## 扩缩容 BE 集群

StarRocks 会在后端节点 (BE) 进行横向扩展或缩减后自动执行负载均衡，而不会影响整体性能。

当您添加新的 BE 节点时，系统的 Tablet Scheduler 会检测到该新节点及其低负载。然后，它会将 Tablet 从高负载的 BE 节点迁移到新的低负载 BE 节点，以确保数据和负载在整个集群中均匀分布。

均衡过程基于为每个 BE 计算的 loadScore，该 loadScore 会同时考虑磁盘利用率和副本数量。系统的目标是将 Tablet 从 loadScore 较高的节点迁移到 loadScore 较低的节点。

您可以检查前端 (FE) 配置参数 `tablet_sched_disable_balance`，以确保自动均衡功能未被禁用（该参数默认为 false，这意味着 Tablet 均衡功能默认启用）。更多详细信息请参阅[管理副本文档](./resource_management/Replica.md)。

### 扩容 BE 集群

部署并启动新增 BE 节点。详细部署方式参考 [部署 StarRocks](../../deployment/deploy_manually.md)。

```bash
bin/start_be.sh --daemon
```

扩容 BE 集群。

```sql
ALTER SYSTEM ADD backend 'be_host:be_heartbeat_service_port';
```

完成后，您可以查看节点信息验证扩容是否成功。

```sql
SHOW PROC '/backends';
```

### 缩容 BE 集群

您可以通过 DROP 或 DECOMMISSION 的方式缩容 BE 集群。

DROP 会立刻删除 BE 节点，丢失的副本由 FE 调度补齐，而 DECOMMISSION 先保证副本补齐，然后再删除 BE 节点。建议您通过 DECOMMISSION 方式进行 BE 集群缩容。

* 通过 DECOMMISSION 的方式缩容 BE 集群。

```sql
ALTER SYSTEM DECOMMISSION backend "be_host:be_heartbeat_service_port";
```

* 通过 DROP 的方式缩容 BE 集群。

> 警告：如果您需要使用 DROP 方式删除 BE 节点，请确保系统三副本完整。

```sql
ALTER SYSTEM DROP backend "be_host:be_heartbeat_service_port";
```

完成后，您可以查看节点信息验证缩容是否成功。

```sql
SHOW PROC '/backends';
```

## 扩缩容 CN 集群

### 扩容 CN 集群

运行以下命令添加 CN 节点。

```sql
ALTER SYSTEM ADD COMPUTE NODE "cn_host:cn_heartbeat_service_port";
```

运行以下命令查看 CN 节点状态。

```sql
SHOW PROC '/compute_nodes';
```

### 缩容 CN 集群

运行以下命令删除 CN 节点。

```sql
ALTER SYSTEM DROP COMPUTE NODE "cn_host:cn_heartbeat_service_port";
```

您可以通过运行 `SHOW PROC '/compute_nodes';` 查看 CN 节点状态。
