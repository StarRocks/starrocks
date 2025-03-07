---
displayed_sidebar: docs
---

# Data Cache 可观测性

早期版本中 Data Cache 特性缺少相关监控指标和有效的监控手段。3.3 版本对 Data Cache 的可观测性做了提升，支持通过以下几种方式来观测 Data Cache 相关指标，帮助用户清晰了解 Data Cache 的整体磁盘和内存用量，以及详细的指标。

> **注意**
>
> 从 v3.4 版本开始，数据湖外表和云原生表（存算分离）查询统一使用 Data Cache 实例。因此，除特殊说明外，以下可视化方式中默认展示的是 Data Cache 实例自身的指标，即同时包含了数据湖外表和云原生表查询的缓存使用情况。

## SQL 查询

通过 SQL 语句，可以查询集群中每个 BE 节点的 Data Cache 容量指标信息。

### SHOW BACKENDS

`DataCacheMetrics` 字段记录了 Data Cache 的磁盘用量和内存用量。

```SQL
mysql> show backends\G
*************************** 1. row ***************************
            BackendId: 10004
                   IP: XXX.XX.XX.XXX
        HeartbeatPort: 4450
               BePort: 4448
             HttpPort: 4449
             BrpcPort: 4451
        LastStartTime: 2023-12-13 20:09:30
        LastHeartbeat: 2023-12-13 20:10:43
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 48
     DataUsedCapacity: 0.000 B
        AvailCapacity: 280.103 GB
        TotalCapacity: 1.968 TB
              UsedPct: 86.10 %
       MaxDiskUsedPct: 86.10 %
               ErrMsg:
              Version: datacache-heartbeat-c68caf7
               Status: {"lastSuccessReportTabletsTime":"2023-12-13 20:10:38"}
    DataTotalCapacity: 280.103 GB
          DataUsedPct: 0.00 %
             CpuCores: 104
    NumRunningQueries: 0
           MemUsedPct: 0.00 %
           CpuUsedPct: 0.0 %
     -- highlight-start   
     DataCacheMetrics: Status: Normal, DiskUsage: 0.00GB/2.00GB, MemUsage: 0.00GB/30.46GB
     -- highlight-end
1 row in set (1.90 sec)
```

### information_schema

`information_schema` 下的 `be_datacache_metrics` 视图记录了 Data Cache 的内存和磁盘容量和用量信息。

```Bash
mysql> select * from information_schema.be_datacache_metrics;
+-------+--------+------------------+-----------------+-----------------+----------------+-----------------+----------------------------------------------------------------------------------------------+
| BE_ID | STATUS | DISK_QUOTA_BYTES | DISK_USED_BYTES | MEM_QUOTA_BYTES | MEM_USED_BYTES | META_USED_BYTES | DIR_SPACES                                                                                   |
+-------+--------+------------------+-----------------+-----------------+----------------+-----------------+----------------------------------------------------------------------------------------------+
| 10004 | Normal |       2147483648 |               0 |     32706263420 |              0 |               0 | [{"Path":"/home/disk1/datacache","QuotaBytes":2147483648}]                                   |
+-------+--------+------------------+-----------------+-----------------+----------------+-----------------+----------------------------------------------------------------------------------------------+
1 row in set (5.41 sec)
```

- `BE_ID`：BE ID。
- `STATUS`：BE 节点状态。
- `DISK_QUOTA_BYTES`：用户配置的总磁盘缓存容量。
- `DISK_USED_BYTES`：当前实际使用的磁盘缓存空间。
- `MEM_QUOTA_BYTES`：用户配置的内存缓存容量。
- `MEM_USED_BYTES`：当前实际使用的内存缓存空间。
- `META_USED_BYTES`：系统元数据占用的内存空间。
- `DIR_SPACES`：磁盘上的 cache  路径，以及对应的 cache 大小。

## 调用 API

自 v3.3.2 起，StarRocks 提供了两个 API 接口获取缓存指标，其反应的是系统不同层面的缓存状态：

- `/api/datacache/app_stat`：查询真实的缓存命中率，计算公式为 `远端读取数据量 / (远端读取数据量 + Data Cache 数据读取量)`。
- `/api/datacache/stat`：Data Cache 的底层执行状态。该接口主要用于 Data Cache 的运维和性能问题定位，并不能反应查询的真实命中率，普通用户无需关注该接口。

### 获取缓存命中指标

通过访问以下 API 接口获取：

```bash
http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/app_stat
```

返回结果如下：

```bash
{
    "hit_bytes": 4008,
    "miss_bytes": 2004,
    "hit_rate": 0.67,
    "hit_bytes_last_minute": 4008,
    "miss_bytes_last_minute": 2004,
    "hit_rate_last_minute": 0.67
}
```

| **指标**               | **说明**                                             |
| ---------------------- | ---------------------------------------------------- |
| hit_bytes              | 从缓存中读取的字节数。                               |
| miss_bytes             | 从远端读取的字节数。                                 |
| hit_rate               | 缓存命中率 `(hit_bytes / (hit_bytes + miss_bytes))`。 |
| hit_bytes_last_minute  | 最近一分钟内从缓存中读取的字节数。                   |
| miss_bytes_last_minute | 最近一分钟内从远端读取的字节数。                     |
| hit_rate_last_minute   | 最近一分钟内缓存命中率。                             |

### 获取底层 Data Cache 详细指标

通过访问以下 API 接口，可获取该节点 Data Cache 更加详细的指标。

```Bash
http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/stat
```

结果如下图所示：

![img](../_assets/data_cache_observe.png)

### 指标说明

| **指标**               | **说明**                                                     |
| ---------------------- | ------------------------------------------------------------ |
| status                 | Data Cache 实例状态，当前主要有这几种：`NORMAL`: 正常。`ABNORMAL`:  实例运行不正常，无法进行正常缓存读写，需要查看日志定位。`UPDATING`:  更新中，比如在线扩缩容过程中会出现短暂的 updating 状态。 |
| mem_quota_bytes        | 用户配置的内存缓存容量。                                     |
| mem_used_bytes         | 当前实际使用的内存缓存空间。                                 |
| mem_used_rate          | 内存缓存使用率。                                             |
| disk_quota_bytes       | 用户配置的总磁盘缓存容量。                                   |
| disk_used_bytes        | 当前实际使用的磁盘缓存空间。                                 |
| disk_used_rate         | 磁盘缓存使用率。                                             |
| disk_spaces            | 用户配置的具体的磁盘缓存信息，包括各个缓存路径和大小。       |
| meta_used_bytes        | 缓存系统元数据占用的内存空间。                               |
| hit_count              | 缓存命中次数。                                               |
| miss_count             | 缓存未命中次数。                                             |
| hit_rate               | 缓存命中率。                                                 |
| hit_bytes              | 缓存命中字节数。                                             |
| miss_bytes             | 缓存未命中字节数。                                           |
| hit_count_last_minute  | 最近一分钟内缓存命中次数。                                   |
| miss_count_last_minute | 最近一分钟内缓存未命中次数。                                 |
| hit_bytes_last_minute  | 最近一分钟内缓存命中字节数。                                 |
| miss_bytes_last_minute | 最近一分钟内缓存未命中字节数。                               |
| buffer_item_count      | 当前缓存的 Buffer 实例的数量。Buffer 实例指常见的数据缓存，例如我们从远端读取某个文件的部分原始数据，将其直接缓存到内存或者磁盘。 |
| buffer_item_bytes      | 当前缓存的 Buffer 实例所占字节数。                           |
| read_mem_bytes         | 从内存缓存中读取的字节数。                                   |
| read_disk_bytes        | 从磁盘缓存中读取的字节数。                                   |
| write_bytes            | 总共写入缓存的字节数。                                       |
| write_success_count    | 写入成功的次数。                                             |
| write_fail_count       | 写入失败的次数。                                             |
| remove_bytes           | 删除的字节数。                                               |
| remove_success_count   | 删除成功的次数。                                             |
| remove_fail_count      | 删除失败的次数。                                             |
| current_reading_count  | 当前正在执行的读缓存操作数量。                               |
| current_writing_count  | 当前正在执行的写缓存操作数量。                               |
| current_removing_count | 当前正在执行的删除缓存操作数量。                             |
