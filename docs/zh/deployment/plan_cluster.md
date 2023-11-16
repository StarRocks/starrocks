---
displayed_sidebar: "Chinese"
---

# 规划 StarRocks 集群

本文介绍如何为部署 StarRocks 生产集群规划资源。您需要从节点数、CPU 内核数、内存大小和存储大小的角度规划您的 StarRocks 集群。

## 节点数量

StarRocks 主要由两种类型的组件组成：FE 节点和 BE 节点。每个节点必须单独部署在物理机或虚拟机上。

### FE 节点数量

FE 节点主要负责元数据管理、客户端连接管理、查询计划和查询调度。

对于 StarRocks 生产集群，建议您至少部署**三个** Follower FE 节点，以防止单点故障。

StarRocks 通过 BDB JE 协议跨 FE 节点管理元数据。StarRocks 从所有 Follower FE 节点中选出一个 Leader FE 节点。只有 Leader FE 节点可以写入元数据，其他 Follower FE 节点只能根据 Leader FE 节点的日志更新元数据。如果 Leader FE 节点掉线，只要超过半数的 Follower FE 节点存活，StarRocks 就会重新选举出一个新的 Leader FE 节点。

如果您的应用程序会产生高并发查询请求，您可以在集群中添加 Observer FE 节点。Observer FE 节点只负责处理查询请求，不会参与 Leader FE 节点的选举。

### BE 节点数量

BE 节点负责数据存储和 SQL 执行。

对于 StarRocks 生产集群，建议您至少部署三个 BE 节点，这些节点会自动形成一个 BE 高可用集群，避免由于发生单点故障而影响数据可靠性和服务可用性。

您可以通过增加 BE 节点的数量来实现查询的高并发。

### CN 节点数量

CN 节点是 StarRocks 的可选组件，仅负责 SQL 执行。

您可以通过增加 CN 节点数量以弹性扩展计算资源，而无需改变集群中的数据分布。

## CPU 和内存

通常，FE 服务不会消耗大量的 CPU 和内存资源。建议您为每个 FE 节点分配 8 个 CPU 内核和 16 GB RAM。

与 FE 服务不同，如果您的应用程序需要在大型数据集上处理高度并发或复杂的查询，BE 服务可能会使用大量 CPU 和内存资源。因此，建议您为每个 BE 节点分配 16 个 CPU 内核和 64 GB RAM。

## 存储空间

### FE 存储

由于 FE 节点仅在其存储中维护 StarRocks 的元数据，因此在大多数场景下，每个 FE 节点只需要 100 GB 的 HDD 存储。

### BE 存储

#### 预估 BE 初始存储空间

StarRocks 集群需要的总存储空间同时受到原始数据大小、数据副本数以及使用的数据压缩算法的压缩比的影响。

您可以通过以下公式估算所有 BE 节点所需的总存储空间：

```Plain
BE 节点所需的总存储空间 = 原始数据大小 * 数据副本数/数据压缩算法压缩比

原始数据大小 = 单行数据大小 * 总数据行数
```

在 StarRocks 中，一个表中的数据首先被划分为多个分区（Partition），然后进一步被划分为多个 Tablet。Tablet 是 StarRocks 中基本数据管理逻辑单元。为保证数据的高可靠性，您可以为每个 Tablet 维护多个副本，存储于不同的 BE 节点。StarRocks 默认维护三个副本。

目前，StarRocks 支持四种数据压缩算法：zlib、Zstandard（或 zstd）、LZ4 和 Snappy（按压缩比从高至低排列）。这些数据压缩算法可以提供 3:1 到 5:1 的压缩比。

通过计算得到总存储空间后，你可以简单地将之除以集群中的 BE 节点数，估算出每个 BE 节点所需的平均存储空间。

#### 随时添加额外存储空间

如果 BE 存储空间随着原始数据的增长而耗尽，您可以通过垂直或水平扩展集群或扩展云存储以补充存储空间。

- 在 StarRocks 集群中添加新的 BE 节点

  您可以在 StarRocks 集群中添加新的 BE 节点，从而将数据重新平分至更多节点上。有关详细说明，请参阅 [扩容 StarRocks - 扩容 BE 集群](../administration/Scale_up_down.md)。

  添加新的 BE 节点后，StarRocks 会自动重新平衡数据在所有 BE 节点之间的分布。所有数据模型均支持这种自动平衡。

- 在 BE 节点上添加额外的存储卷

  您还可以在已有 BE 节点上添加额外的存储卷。有关详细说明，请参阅 [扩容 StarRocks - 扩容 BE 集群](../administration/Scale_up_down.md)。

  添加额外的存储卷后，StarRocks 会自动重新平衡所有表中的数据。

- 添加云存储空间

  如果您的 StarRocks 集群部署在云端，您可以按需扩展您的云存储。请联系您的云提供商以获取详细说明。
