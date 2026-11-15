---
sidebar_position: 30
displayed_sidebar: docs
description: "How to add or remove nodes to scale a StarRocks cluster up or down."
products: [oss, enterprise, anywhere]
---

# 扩容和缩容

本主题介绍如何对 StarRocks 的节点进行扩容和缩容。

## FE 扩容和缩容 {/* #scale-fe-in-and-out */}

StarRocks 有两种类型的 FE 节点：Follower 和 Observer。Follower 参与选举投票和写入。Observer 仅用于同步日志并扩展读取性能。

> - 我们建议 follower FE（包括 leader）的数量为奇数，并部署其中 3 个以组成高可用（HA）模式。
> - 当 FE 处于高可用部署（1 个 leader，2 个 follower）时，建议添加 Observer FE 以获得更好的读取性能。

### FE 扩容 {/* #scale-fe-out */}

部署 FE 节点并启动服务后，运行以下命令来扩容 FE。

```sql
alter system add follower "fe_host:edit_log_port";
alter system add observer "fe_host:edit_log_port";
```

### FE 缩容 {/* #scale-fe-in */}

FE 缩容与扩容类似。运行以下命令来缩容 FE。

```sql
alter system drop follower "fe_host:edit_log_port";
alter system drop observer "fe_host:edit_log_port";
```

扩容和缩容之后，您可以通过运行 `show proc '/frontends';` 来查看节点信息。

## BE 扩容和缩容 {/* #scale-be-in-and-out */}

StarRocks 会在 BE 扩容或缩容后自动执行负载均衡，而不会影响整体性能。

当您添加新的 BE 节点时，系统的 Tablet Scheduler 会检测到新节点及其低负载情况，然后开始将 tablet 从高负载的 BE 节点迁移到新的低负载 BE 节点，以确保整个集群的数据和负载均匀分布。

均衡过程基于为每个 BE 计算出的 loadScore，该分数同时考虑了磁盘利用率和副本数量。系统旨在将 tablet 从 loadScore 较高的节点迁移到 loadScore 较低的节点。

您可以检查 FE 配置参数 `tablet_sched_disable_balance`，以确保自动均衡没有被禁用（该参数默认值为 false，这意味着默认情况下启用了 tablet 均衡）。更多详情请参阅 [管理副本文档](./resource_management/Replica.md)。

### BE 扩容 {/* #scale-be-out */}

运行以下命令来扩容 BE。

```sql
alter system add backend 'be_host:be_heartbeat_service_port';
```

运行以下命令来检查 BE 状态。

```sql
show proc '/backends';
```

### BE 缩容 {/* #scale-be-in */}

有两种方式可以缩容 BE 节点——`DROP` 和 `DECOMMISSION`。

`DROP` 会立即删除 BE 节点，丢失的副本将由 FE 调度进行补齐。`DECOMMISSION` 会先确保副本被补齐，然后再删除 BE 节点。`DECOMMISSION` 更为友好，推荐用于 BE 缩容。

两种方法的命令类似：

- `alter system decommission backend "be_host:be_heartbeat_service_port";`
- `alter system drop backend "be_host:be_heartbeat_service_port";`

删除 backend 是一项危险操作，因此在执行前需要进行两次确认

- `alter system drop backend "be_host:be_heartbeat_service_port";`

## CN 扩容和缩容 {/* #scale-cn-in-and-out */}

### CN 扩容 {/* #scale-cn-out */}

运行以下命令来扩容 CN。

```sql
ALTER SYSTEM ADD COMPUTE NODE "cn_host:cn_heartbeat_service_port";
```

运行以下命令来检查 CN 状态。

```sql
SHOW PROC '/compute_nodes';
```

### CN 缩容 {/* #scale-cn-in */}

CN 缩容与扩容类似。运行以下命令来缩容 CN。

```sql
ALTER SYSTEM DROP COMPUTE NODE "cn_host:cn_heartbeat_service_port";
```

您可以通过运行 `SHOW PROC '/compute_nodes';` 来查看节点信息。
