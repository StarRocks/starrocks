---
displayed_sidebar: docs
---

# 数据湖相关 FAQ

本文介绍数据湖相关的常见问题，并给出解决方案。文中介绍的很多指标，需要您通过 `set enable_profile=true` 设置来采集 SQL 查询的 Profile 并进行分析。

## HDFS 慢节点问题

### 问题描述

在访问 HDFS 上存储的数据文件时，如果发现 SQL 查询的 Profile 中 `__MAX_OF_FSIOTime` 和 `__MIN_OF_FSIOTime` 两个指标的值相差很大，说明当前环境存在 HDFS 集群某些 DataNode 节点较慢的情况。如下所示的 Profile，就是典型的 HDFS 慢节点场景：

```plaintext
 - InputStream: 0
   - AppIOBytesRead: 22.72 GB
     - __MAX_OF_AppIOBytesRead: 187.99 MB
     - __MIN_OF_AppIOBytesRead: 64.00 KB
   - AppIOCounter: 964.862K (964862)
     - __MAX_OF_AppIOCounter: 7.795K (7795)
     - __MIN_OF_AppIOCounter: 1
   - AppIOTime: 1s372ms
     - __MAX_OF_AppIOTime: 4s358ms
     - __MIN_OF_AppIOTime: 1.539ms
   - FSBytesRead: 15.40 GB
     - __MAX_OF_FSBytesRead: 127.41 MB
     - __MIN_OF_FSBytesRead: 64.00 KB
   - FSIOCounter: 1.637K (1637)
     - __MAX_OF_FSIOCounter: 12
     - __MIN_OF_FSIOCounter: 1
   - FSIOTime: 9s357ms
     - __MAX_OF_FSIOTime: 60s335ms
     - __MIN_OF_FSIOTime: 1.536ms
```

### 解决方案

当前有三种解决方案：

- 【推荐】开启 [Data Cache](../data_source/data_cache.md)。通过自动缓存远端数据到 BE（或 CN）节点，消除 HDFS 慢节点对查询的影响。
- 【推荐】缩短 HDFS 客户端和 DataNode 之间的超时时间，适合 Data Cache 不起效果的场景。
- 开启 [Hedged Read](https://hadoop.apache.org/docs/r2.8.3/hadoop-project-dist/hadoop-common/release/2.4.0/RELEASENOTES.2.4.0.html) 功能。开启后，如果当前从某个数据块读取数据比较慢，StarRocks 发起一个新的 Read 任务，与原来的 Read 任务并行，用于从目标数据块的副本上读取数据。不管哪个 Read 任务先返回结果，另外一个 Read 任务则会取消。**Hedged Read 可以加速数据读取速度，但是也会导致 Java 虚拟机（简称“JVM”）堆内存的消耗显著增加。因此，在物理机内存比较小的情况下，不建议开启 Hedged Read。**

#### 【推荐】Data Cache

参见 [Data Cache](../data_source/data_cache.md)。

#### 【推荐】缩短 HDFS 客户端和 DataNode 之间的超时时间

可以通过在 `hdfs-site.xml` 配置 `dfs.client.socket-timeout` 属性，来缩短 HDFS 客户端和 DataNode 之间的超时时间（默认超时时间是 60s，比较长）。这样，当 StarRocks 遇到一个反应缓慢的 DataNode 节点时，能够快速超时，转而向新的 DataNode 发起请求。如下例子中，配置了 5s 的超时时间：

```xml
<configuration>
  <property>
      <name>dfs.client.socket-timeout</name>
      <value>5000</value>
   </property>
</configuration>
```

#### Hedged Read

在 BE（或 CN）配置文件 `be.conf` 中通过如下参数（从 3.0 版本起支持），开启并配置 HDFS 集群的 Hedged Read 功能。

| 参数名称                                 | 默认值 | 说明                                                         |
| ---------------------------------------- | ------ | ------------------------------------------------------------ |
| hdfs_client_enable_hedged_read           | false  | 指定是否开启 Hedged Read 功能。 |
| hdfs_client_hedged_read_threadpool_size  | 128    | 指定 HDFS 客户端侧 Hedged Read 线程池的大小，即 HDFS 客户端侧允许有多少个线程用于服务 Hedged Read。该参数对应 HDFS 集群配置文件 `hdfs-site.xml` 中的 `dfs.client.hedged.read.threadpool.size` 参数。 |
| hdfs_client_hedged_read_threshold_millis | 2500   | 指定发起 Hedged Read 请求前需要等待多少毫秒。例如，假设该参数设置为 `30`，那么如果一个 Read 任务未能在 30 毫秒内返回结果，则 HDFS 客户端会立即发起一个 Hedged Read，从目标数据块的副本上读取数据。该参数对应 HDFS 集群配置文件 `hdfs-site.xml` 中的 `dfs.client.hedged.read.threshold.millis` 参数。 |

如果在 Profile 中观测到如下任一指标的值大于 `0`，则代表 Hedged Read 功能开启成功。

| 指标                            | 说明                                                         |
| ------------------------------ | ------------------------------------------------------------ |
| TotalHedgedReadOps             | 发起 Hedged Read 的次数。                                      |
| TotalHedgedReadOpsInCurThread  | 由于 Hedged Read 线程池大小限制（通过 `hdfs_client_hedged_read_threadpool_size` 配置）而无法启动新线程、只能在当前线程内触发 Hedged Read 的次数。 |
| TotalHedgedReadOpsWin          | Hedged Read 比原 Read 更早返回结果的次数。 |

## 当查询 Hive Catalog 中的表时，如何解决错误“ERROR 1064 (HY000): Type mismatches on column [is_refund], JDBC result type is Integer, please set the type to one of tinyint,smallint,int,bigint”？

此问题是由 JDBC 连接配置不正确引起的。将参数 `tinyInt1isBit=false` 添加到您的 JDBC URI 以防止此问题：

```SQL
"jdbc_uri" = "jdbc:mysql://xxx:3306?database=yl_spmibill&tinyInt1isBit=false"
```

## 为什么在 Iceberg Catalog 中无法查询到最新更新的数据（即使在刷新或重建 catalog 之后），我该如何排查？

首先检查问题是否由启用 Data Cache 引起。按照以下步骤进行验证：

1. 比较 StarRocks 和 Spark 之间扫描的数据文件：

   - 在 StarRocks 中：`select file_path, spec_id from db.table_name$files;`
   - 在 Spark 中：`select file_path, spec_id from db.table_name.files;`

2. 如果结果一致，继续通过禁用 Data Cache 并再次查询来排查问题是否仍然存在。

根本原因：更新 Iceberg 表数据是通过覆盖旧文件来实现的，这会破坏 Iceberg 的历史数据。正确的行为是在写入更新时生成新的文件名。StarRocks Data Cache 使用文件名、文件大小和修改时间来确定缓存数据是否有效。由于 Iceberg 不覆盖文件且修改时间始终为 0，StarRocks 错误地将文件视为未更改并从缓存中读取，导致查询结果过时。