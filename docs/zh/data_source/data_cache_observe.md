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

- `/api/datacache/app_stat`：查询 Block Cache 和 Page Cache 的缓存命中率。
- `/api/datacache/stat`：Data Cache 的底层执行状态。该接口主要用于 Data Cache 的运维和性能问题定位，并不能反应查询的真实命中率，普通用户无需关注该接口。

### 获取缓存命中指标

通过访问以下 API 接口获取：

```bash
http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/app_stat
```

返回结果如下：

```bash
{
    "block_cache_hit_bytes": 1642106883,
    "block_cache_miss_bytes": 8531219739,
    "block_cache_hit_rate": 0.16,
    "block_cache_hit_bytes_last_minute": 899037056,
    "block_cache_miss_bytes_last_minute": 4163253265,
    "block_cache_hit_rate_last_minute": 0.18,
    "page_cache_hit_count": 15048,
    "page_cache_miss_count": 10032,
    "page_cache_hit_rate": 0.6,
    "page_cache_hit_count_last_minute": 10032,
    "page_cache_miss_count_last_minute": 5016,
    "page_cache_hit_rate_last_minute": 0.67
}
```

| **指标**                             | **说明**                                                                                        |
|------------------------------------|-----------------------------------------------------------------------------------------------|
| block_cache_hit_bytes              | 从 Block Cache 中读取的字节数。                                                                        |
| block_cache_miss_bytes             | 从远端读取的字节数。                                                                                    |
| block_cache_hit_rate               | Block Cache 命中率 `(block_cache_hit_bytes / (block_cache_hit_bytes + block_cache_miss_bytes))`。 |
| block_cache_hit_bytes_last_minute  | 最近一分钟内从 Block Cache 中读取的字节数。                                                                  |
| block_cache_miss_bytes_last_minute | 最近一分钟内从远端读取的字节数（Block Cache 未命中）。                                                             |
| block_cache_hit_rate_last_minute   | 最近一分钟内 Block Cache 命中率。                                                                       |
| page_cache_hit_count               | 从 Page Cache 中读取的 Page 数量。                                                                    |
| page_cache_miss_count              | Page Cache 未命中的 Page 数量。                                                                      |
| page_cache_hit_rate                | Page Cache 命中率 `(page_cache_hit_count / (page_cache_hit_count + page_cache_miss_count))`。     |
| page_cache_hit_count_last_minute   | 最近一分钟内从 Page Cache 中读取的 Page 数量。                                                              |
| page_cache_miss_count_last_minute  | 最近一分钟内未命中 Page Cache 的 Page 数量。                                                               |
| page_cache_hit_rate_last_minute    | 最近一分钟内 Page Cache 命中率。                                                                        |

### 获取底层 Data Cache 详细指标

通过访问以下 API 接口，可获取该节点 Data Cache 更加详细的指标。

```Bash
http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/stat
```

结果如下所示：

```json
{
    "page_cache_mem_quota_bytes": 10679976935,
    "page_cache_mem_used_bytes": 10663052377,
    "page_cache_mem_used_rate": 1.0,
    "page_cache_hit_count": 276890,
    "page_cache_miss_count": 153126,
    "page_cache_hit_rate": 0.64,
    "page_cache_hit_count_last_minute": 11196,
    "page_cache_miss_count_last_minute": 9982,
    "page_cache_hit_rate_last_minute": 0.53,
    "block_cache_status": "NORMAL",
    "block_cache_disk_quota_bytes": 214748364800,
    "block_cache_disk_used_bytes": 11371020288,
    "block_cache_disk_used_rate": 0.05,
    "block_cache_disk_spaces": "/disk1/sr/be/storage/datacache:107374182400;/disk2/sr/be/storage/datacache:107374182400",
    "block_cache_meta_used_bytes": 11756727,
    "block_cache_hit_count": 57707,
    "block_cache_miss_count": 2556,
    "block_cache_hit_rate": 0.96,
    "block_cache_hit_bytes": 15126253744,
    "block_cache_miss_bytes": 620687633,
    "block_cache_hit_count_last_minute": 18108,
    "block_cache_miss_count_last_minute": 2449,
    "block_cache_hit_bytes_last_minute": 4745613488,
    "block_cache_miss_bytes_last_minute": 607536783,
    "block_cache_read_disk_bytes": 15126253744,
    "block_cache_write_bytes": 11338218093,
    "block_cache_write_success_count": 43377,
    "block_cache_write_fail_count": 36394,
    "block_cache_remove_bytes": 0,
    "block_cache_remove_success_count": 0,
    "block_cache_remove_fail_count": 0,
    "block_cache_current_reading_count": 0,
    "block_cache_current_writing_count": 0,
    "block_cache_current_removing_count": 0
}
```

### 指标说明

| **指标**                             | **说明**                                                                                                                       |
|------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| page_cache_mem_quota_bytes         | 当前 Page Cache 内存上限。                                                                                                          |
| page_cache_mem_used_bytes          | 当前实际使用 Page Cache 内存。                                                                                                        |
| page_cache_mem_used_rate           | 当前 Page Cache 内存使用率。                                                                                                         |
| page_cache_hit_count               | Page Cache 命中次数。                                                                                                             |
| page_cache_miss_count              | Page Cache 未命中次数。                                                                                                            |
| page_cache_hit_rate                | Page Cache 命中率。                                                                                                              |
| page_cache_hit_count_last_minute   | Page Cache 最近一分钟的命中次数。                                                                                                       |
| page_cache_miss_count_last_minute  | Page Cache 最近一分钟的未合中次数。                                                                                                      | 
| page_cache_hit_rate_last_minute    | Page Cache 最近一分钟的命中率。                                                                                                        |
| block_cache_status                 | Block Cache 状态，当前主要有这几种：`NORMAL`: 正常。`ABNORMAL`:  实例运行不正常，无法进行正常缓存读写，需要查看日志定位。`UPDATING`:  更新中，比如在线扩缩容过程中会出现短暂的 updating 状态。 |
| block_cache_disk_quota_bytes       | 用户配置的 Block Cache 总磁盘缓存容量。                                                                                                   |
| block_cache_disk_used_bytes        | 当前 Block Cache 实际使用的磁盘缓存空间。                                                                                                  |
| block_cache_disk_used_rate         | Block Cache 磁盘缓存使用率。                                                                                                         |
| block_cache_disk_spaces            | 用户配置的 Block Cache 具体的磁盘缓存信息，包括各个缓存路径和大小。                                                                                     |
| block_cache_meta_used_bytes        | Block Cache 元数据占用的内存空间。                                                                                                      |
| block_cache_hit_count              | Block Cache 命中次数。                                                                                                            |
| block_cache_miss_count             | Block Cache 命中次数。                                                                                                            |
| block_cache_hit_rate               | Block Cache 命中率。                                                                                                             |
| block_cache_hit_bytes              | Block Cache 命中字节数。                                                                                                           |
| block_cache_miss_bytes             | Block Cache 未命中字节数。                                                                                                          |
| block_cache_hit_count_last_minute  | Block Cache 最近一分钟内命中次数。                                                                                                      |
| block_cache_miss_count_last_minute | Block Cache 最近一分钟内未命中次数。                                                                                                     |
| block_cache_hit_bytes_last_minute  | Block Cache 最近一分钟内命中字节数。                                                                                                     |
| block_cache_miss_bytes_last_minute | Block Cache 最近一分钟内未命中字节数。                                                                                                    |
| block_cache_buffer_item_count      | 当前 Block Cache 的 Buffer 实例的数量。Buffer 实例指常见的数据缓存，例如我们从远端读取某个文件的部分原始数据，将其直接缓存到内存或者磁盘。                                          |
| block_cache_buffer_item_bytes      | 当前 Block Cache 的 Buffer 实例所占字节数。                                                                                             |
| block_cache_read_disk_bytes        | 从 Block Cache 中读取的字节数。                                                                                                       |
| block_cache_write_bytes            | 总共写入 Block Cache 的字节数。                                                                                                       |
| block_cache_write_success_count    | Block Cache 写入成功的次数。                                                                                                         |
| block_cache_write_fail_count       | Block Cache 写入失败的次数。                                                                                                         |
| block_cache_remove_bytes           | Block Cache 删除的字节数。                                                                                                          |
| block_cache_remove_success_count   | Block Cache 删除成功的次数。                                                                                                         |
| block_cache_remove_fail_count      | Block Cache 删除失败的次数。                                                                                                         |
| block_cache_current_reading_count  | Block Cache 当前正在执行的读操作数量。                                                                                                    |
| block cache_current_writing_count  | Block Cache 当前正在执行的写操作数量。                                                                                                    |
| block_cache_current_removing_count | Block Cache 当前正在执行的删除操作数量。                                                                                                   |
