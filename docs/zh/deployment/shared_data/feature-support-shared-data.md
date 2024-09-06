---
displayed_sidebar: docs
sidebar_label: "能力边界"
---

# 存算分离集群能力边界

:::tip
以下列出的所有功能均已列出其引入版本。如果您想搭建新的存算分离集群，推荐您部署 v3.2 或以上的最新小版本。
:::

## 概述

StarRocks 存算分离集群采用了存储计算分离架构，将数据存储在远程存储中，从而降低存储成本、优化资源隔离并加强集群的弹性扩展能力。

本文介绍了存算分离功能的能力边界，涵盖了部署方法、存储配置、缓存机制、Compaction、主键表功能和性能测试等。

## 部署

存算分离集群支持在物理或虚拟机上部署以及通过 Operator 在 Kubernetes 上部署。

两种部署方案均有以下限制：

- 不支持混合部署存算一体模式和存算分离模式。
- 不支持从存算一体集群转换为存算分离集群，反之亦然。
- 不支持异构部署，即集群内所有 CN 节点的硬件规格必须相同。

### StarRocks Kubernetes Operator

StarRocks 提供用于在 Kubernetes 上部署存算分离集群的 [StarRocks Kubernetes Operator](https://github.com/StarRocks/starrocks-kubernetes-operator/releases)。

您可以通过以下方法扩缩容存算分离集群：

- 手动扩缩容。
- 使用 Kubernetes HPA（Horizontal Pod Autoscaler）策略自动扩缩容。

## 存储

存算分离集群支持在 HDFS 和对象存储上构建存储卷。

### HDFS

#### Location

StarRocks 支持以下 HDFS 存储卷 Location：

- HDFS：`hdfs://<host>:<port>/`

  > **说明**
  >
  > 从 v3.2 起，存储卷支持启用 NameNode HA 模式的 HDFS 集群。

- WebHDFS（从 v3.2 起支持）：`webhdfs://<host>:<http_port>/`

- ViewFS（从 v3.2 起支持）：`viewfs://<ViewFS_cluster>/`

#### 认证

StarRocks 支持以下 HDFS 存储卷的认证方式：

- Basic

- Username（从 v3.2 起支持）

- Kerberos Ticket Cache（从 v3.2 起支持）

  > **说明**
  >
  > StarRocks 不支持自动刷新 Ticket。您需要设置 crontab 任务来刷新 Ticket。

暂不支持使用 Kerberos Keytab 和 Principal ID 进行认证。

#### 使用说明

StarRocks 支持基于 HDFS 和对象存储创建存储卷，但每个 StarRocks 实例中只允许创建一个 HDFS 存储卷。创建多个 HDFS 存储卷可能会导致 StarRocks 出现未知行为。

### 对象存储

#### Location

StarRocks 支持以下对象存储服务的存储卷：

- 兼容 S3 协议的对象存储服务：`s3://<s3_path>`
  - AWS S3
  - GCS、OSS、OBS、COS、TOS、KS3、MinIO 和 Ceph S3
- Azure Blob Storage（从 v3.1.1 起支持）：`azblob://<azblob_path>`

#### 认证

针对不同对象存储服务，StarRocks 支持以下认证方式：

- AWS S3
  - AWS SDK
  - IAM user-based Credential
  - Instance Profile
  - Assumed Role
- GCS、OSS、OBS、COS、TOS、KS3、MinIO 和 Ceph S3
  - 访问密钥
- Azure Blob Storage
  - Shared Key
  - Shared Access Signatures（SAS）

#### 分区前缀

自 v3.2.4 起，StarRocks 支持基于兼容 S3 的对象存储系统创建带有分区前缀功能的存储卷。当启用此功能时，StarRocks 会将数据打散至桶下多个分区（子路径）中。此举可以轻松提高 StarRocks 对存储桶中数据文件的读写性能。

### 存储卷

- 从 v3.1.0 起，支持使用 CREATE STORAGE VOLUME 语句创建存储卷，并推荐在后续版本中使用这种方法。
- 存算分离集群中的 `default_catalog` 使用默认存储卷进行数据持久化。您可以通过 Property `storage_volume` 为 `default_catalog` 中的数据库和表设置不同的存储卷。如未配置，该属性将以Catalog、数据库和表的顺序继承。
- 目前，存储卷只能用于存储云原生表中的数据。未来将支持外部存储管理、导入和备份等功能。

## Cache

### Cache 类型

#### File Cache

File Cache 是与存算分离集群一同支持的初始缓存机制，按 Segment 文件粒度加载缓存文件。自 v3.1.7 和 v3.2.3 起，不推荐使用 File Cache。

#### Data Cache

Data Cache 自 v3.1.7 和 v3.2.3 开始支持，用以取代早期版本中的 File Cache。Data Cache 以 Block（MB 级别）为单位，按需从远程存储中加载缓存数据。推荐您在后续版本中使用 Data Cache。该功能自 v3.2.3 起默认开启。

#### Data Cache 预热

StarRocks v3.3.0 引入了 Data Cache 预热功能，用以加速数据湖和存算分离集群中的查询。Data Cache 预热是主动填充缓存的过程。通过执行 CACHE SELECT，可以提前从远程存储主动获取所需数据。

### 配置项

- 表属性：
  - `datacache.enable`：是否启用本地磁盘缓存。默认值：`true`。
  - `datacache.partition_duration`：缓存数据的有效时长。
- BE 配置：
  - `starlet_use_star_cache`：是否启用 Data Cache。
  - `starlet_star_cache_disk_size_percent`：Data Cache 在存算分离集群中最多可使用的磁盘容量百分比。

### 能力

- 数据导入会生成本地缓存。本地缓存仅会通过缓存容量控制机制淘汰，而不受 `partition_duration` 限制。
- StarRocks 支持设置定期任务进行 Data Cache 预热。

### 限制

- StarRocks 不支持多副本缓存数据。

## Compaction

### 可观测性

#### 分区 Compaction 状态

从 v3.1.9 起，可以通过查询 `information_schema.partitions_meta` 查看分区的 Compaction 状态。

推荐监控以下关键指标：

- **AvgCS**: 分区中所有表格的平均 Compaction Score。
- **MaxCS**: 分区中所有表格的最大 Compaction Score。

#### Compaction 任务状态

从 v3.2.0 起，可以通过查询 `information_schema.be_cloud_native_compactions` 查看 Compaction 任务的状态和进度。

推荐监控以下关键指标：

- **PROGRESS**: 表格当前 Compaction 进度（以百分比表示）。
- **STATUS**: Compaction 任务的状态。如果发生任何错误，详细的错误信息会在此字段中返回。

### 取消 Compaction 任务

可以使用 CANCEL COMPACTION 语句取消特定的 Compaction 任务。

示例：

```SQL
CANCEL COMPACTION WHERE TXN_ID = 123;
```

> **说明**
>
> CANCEL COMPACTION 语句必须在 Leader FE 节点上执行。

### 手动 Compaction

从 v3.1 开始，StarRocks 支持通过 SQL 语句手动 Compaction。可以指定表或分区进行 Compaction。有关详细信息，请参阅 [手动 Compaction](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md#手动-compaction)。

## 主键表

下表列出了主键表的主要功能及其在存算分离集群中的支持状态：

| **功能**       | **支持起始版本** | **说明**                                                     |
| -------------- | ---------------- | ------------------------------------------------------------ |
| 主键表          | v3.1.0           |                                                              |
| 主键索引持久化 | v3.2.0<br />v3.1.3 | <ul><li>目前，存算分离集群支持在本地磁盘上持久化主键索引。</li><li>远程存储持久化将在未来版本中支持。</li></ul> |
| 部分更新       | v3.1.0           | 存算分离集群从 v3.1.0 开始支持行模式的部分更新，从 v3.3.1 开始支持列模式的部分更新。   |
| 条件更新       | v3.1.0           | 目前仅支持“大于”条件。                                       |
| 行列混存       | ❌                | 将在未来版本中支持。                                         |

## 查询性能

以下测试比较了禁用 Data Cache 的存算分离集群、启用 Data Cache 的存算分离集群、查询 Hive 数据的集群和存算一体集群的查询性能。

### 硬件规格

测试中使用的集群包括一个 FE 节点和五个 CN/BE 节点。硬件规格如下：

| **VM 提供商**      | 阿里云 ECS          |
| ----------------- | ------------------- |
| **FE 节点**        | 8 Core 32 GB Memory |
| **CN/BE 节点**     | 8 Core 64 GB Memory |
| **网络带宽**        | 8 Gbits/s           |
| **磁盘**           | ESSD                |

### 软件版本

StarRocks v3.3.0

### 数据集

SSB 1TB 数据集

:::note

以下性能测试中使用的数据集和查询来自于 [Star Schema Benchmark](../../benchmarking/SSB_Benchmarking.md/#test-sql-and-table-creation-statements)。

:::

### 测试结果

下表列出了十三个查询的测试结果及各集群的总和。查询延迟的单位为毫秒（ms）。

| **查询** | **禁用 Data Cache 的存算分离集群**     | **启用 Data Cache 的存算分离集群**    | **查询 Hive 数据的集群禁用 Data Cache**            | **存算一体集群**            |
| -------- | ---------------------------------- | ---------------------------------- | ----------------------------------------------- | ---------------- |
| **Q01**  | 2742                               | 858                                | 9652                                            | 3555             |
| **Q02**  | 2714                               | 704                                | 8638                                            | 3183             |
| **Q03**  | 1908                               | 658                                | 8163                                            | 2980             |
| **Q04**  | 31135                              | 8582                               | 34604                                           | 7997             |
| **Q05**  | 26597                              | 7806                               | 29183                                           | 6794             |
| **Q06**  | 21643                              | 7147                               | 24401                                           | 5602             |
| **Q07**  | 35271                              | 15490                              | 38904                                           | 19530            |
| **Q08**  | 24818                              | 7368                               | 27598                                           | 6984             |
| **Q09**  | 21056                              | 6667                               | 23587                                           | 5687             |
| **Q10**  | 2823                               | 912                                | 16663                                           | 3942             |
| **Q11**  | 50027                              | 18947                              | 52997                                           | 19636            |
| **Q12**  | 10300                              | 4919                               | 36146                                           | 8136             |
| **Q13**  | 7378                               | 3386                               | 23153                                           | 6380             |
| **SUM**  | 238412                             | 83444                              | 333689                                          | 100406           |

### 结论

- 禁用 Data Cache 但启用并行 Scan 及 I/O 合并优化的存算分离集群的查询性能是查询 Hive 数据的集群的 **1.4 倍**。
- 启用 Data Cache 和并行 Scan 及 I/O 合并优化的存算分离集群的查询性能是存算一体集群的 **1.2 倍**。

## 其他待支持功能

- 全文倒排索引
- 行列混存
- 全局字典对象
- 生成列
- 备份和恢复

