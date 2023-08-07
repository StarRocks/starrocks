# 数据湖相关 FAQ

本文介绍数据湖相关的常见问题，并给出解决方案。文中介绍的很多指标，需要您通过 `set enable_profile=true` 设置来采集 SQL 查询的 Profile 并进行分析。

## HDFS 慢节点问题

### 问题描述

在访问 HDFS 上存储的数据文件时，如果发现 SQL 查询的 Profile 中 `__MAX_OF_FSIOTime` 和 `__MIN_OF_FSIOTime` 两个指标的值相差很大，说明当前环境存在 HDFS 慢节点的情况。如下所示的 Profile，就是典型的 HDFS 慢节点场景：

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

当前有两种解决方案：

- 【推荐】开启 [Data Cache](../data_source/data_cache.md)。通过自动缓存远端数据到 BE 节点，消除 HDFS 慢节点对查询的影响。
- 开启 [Hedged Read](https://hadoop.apache.org/docs/r2.8.3/hadoop-project-dist/hadoop-common/release/2.4.0/RELEASENOTES.2.4.0.html) 功能。开启后，如果当前从某个数据块读取数据比较慢，StarRocks 发起一个新的 Read 任务，与原来的 Read 任务并行，用于从目标数据块的副本上读取数据。不管哪个 Read 任务先返回结果，另外一个 Read 任务则会取消。**Hedged Read 可以加速数据读取速度，但是也会导致 Java 虚拟机（简称“JVM”）堆内存的消耗显著增加。因此，在物理机内存比较小的情况下，不建议开启 Hedged Read。**

#### 【推荐】Data Cache

参见 [Data Cache](../data_source/data_cache.md)。

#### Hedged Read

在 BE 配置文件 `be.conf` 中通过如下参数（从 3.0 版本起支持），开启并配置 HDFS 集群的 Hedged Read 功能。

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
