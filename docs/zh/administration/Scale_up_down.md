---
displayed_sidebar: "Chinese"
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
> * 正常情况下，一个 FE 节点可以应对 10 至 20 台 BE 节点。建议您将 FE 集群节点数量控制在 10 个以下。通常 3 个 FE 节点即可满足绝大部分需求。

### 扩容 FE 集群

部署并启动新增 FE 节点。详细部署方式参考 [部署 StarRocks](../deployment/deploy_manually.md)。

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

BE 集群成功扩缩容后，StarRocks 会自动根据负载情况，进行数据均衡，此期间系统正常运行。

### 扩容 BE 集群

部署并启动新增 BE 节点。详细部署方式参考 [部署 StarRocks](../deployment/deploy_manually.md)。

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
