---
sidebar_position: 30
displayed_sidebar: docs
description: "如何使用内置可观测性指标监控 Data Cache 在 StarRocks 中的性能、使用情况和健康状态。"
products: [oss, enterprise, anywhere]
---

# Data Cache 可观测性

在早期版本中，没有丰富的指标或高效的方法来监控 [Data Cache](./data_cache.md) 的性能、使用情况和健康状况。

在 v3.3 中，StarRocks 通过提供高效的监控方法和更多指标，提升了 Data Cache 的可观测性。用户现在可以查看数据缓存的整体磁盘和内存使用情况以及相关指标，从而增强对缓存使用情况的监控。

> **注意**
>
> 从 v3.4.0 开始，对外部 catalog 和 cloud 内表（在存算分离集群中）的查询使用统一的 Data Cache 实例。因此，除非另有说明，以下方法默认展示 Data Cache 实例本身的指标，其中包括对外部 catalog 和 cloud 内表查询的缓存使用情况。

## SQL 命令 {/* #sql-commands */}

您可以运行 SQL 命令，查看每个 BE 节点上 Data Cache 的容量和使用情况。

### SHOW BACKENDS {/* #show-backends */}

`DataCacheMetrics` 字段记录了特定 BE 上 Data Cache 已使用的磁盘和内存空间。

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
     DataCacheMetrics: Status: Normal, DiskUsage: 0B/2GB, MemUsage: 0B/30.4GB
     -- highlight-end
1 row in set (1.90 sec)
```

### information_schema {/* #information_schema */}

`information_schema` 中的 `be_datacache_metrics` 视图记录了以下 Data Cache 相关信息。

```Bash
mysql> select * from information_schema.be_datacache_metrics;
+-------+--------+------------------+-----------------+-----------------+----------------+-----------------+----------------------------------------------------------------------------------------------+
| BE_ID | STATUS | DISK_QUOTA_BYTES | DISK_USED_BYTES | MEM_QUOTA_BYTES | MEM_USED_BYTES | META_USED_BYTES | DIR_SPACES                                                                                   |
+-------+--------+------------------+-----------------+-----------------+----------------+-----------------+----------------------------------------------------------------------------------------------+
| 10004 | Normal |       2147483648 |               0 |     32706263420 |              0 |               0 | [{"Path":"/home/disk1/datacache","QuotaBytes":2147483648}] |
+-------+--------+------------------+-----------------+-----------------+----------------+-----------------+----------------------------------------------------------------------------------------------+
1 row in set (5.41 sec)
```

- `BE_ID`：BE ID
- `STATUS`：BE 状态
- `DISK_QUOTA_BYTES`：用户配置的磁盘缓存容量，单位为字节
- `DISK_USED_BYTES`：已使用的磁盘缓存空间，单位为字节
- `MEM_QUOTA_BYTES`：用户配置的内存缓存容量，单位为字节
- `MEM_USED_BYTES`：已使用的内存缓存空间，单位为字节
- `META_USED_BYTES`：用于缓存元数据的空间
- `DIR_SPACES`：缓存路径及其缓存大小

## API 调用 {/* #api-call */}

自 v3.3.2 起，StarRocks 提供了两个用于获取缓存指标的 API，它们反映了不同层级的缓存状态：

- `/api/datacache/app_stat`：查询 Block Cache 和 Page Cache 的命中率。
- `/api/datacache/stat`：Data Cache 的底层执行状态。该接口主要用于 Data Cache 的维护和瓶颈定位，并不反映查询的实际命中率。普通用户无需关注此接口。

### 查看缓存命中指标 {/* #view-cache-hit-metrics */}

通过访问以下 API 接口查看缓存命中指标：

```bash
http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/app_stat
```

返回：

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

| **指标** | **描述** |
| --------------- | --------------- |
| block_cache_hit_bytes | 从 Block Cache 读取的字节数。 |
| block_cache_miss_bytes | 从远端存储读取的字节数（Block Cache 未命中）。 |
| block_cache_hit_rate | Block Cache 命中率，`(block_cache_hit_bytes / (block_cache_hit_bytes + block_cache_miss_bytes))`。 |
| block_cache_hit_bytes_last_minute | 最近一分钟内从 Block Cache 读取的字节数。 |
| block_cache_miss_bytes_last_minute | 最近一分钟内从远端存储读取的字节数。 |
| block_cache_hit_rate_last_minute | 最近一分钟内的 Block Cache 命中率。 |
| page_cache_hit_count | 从 Page Cache 中读取的 page 数量。 |
| page_cache_miss_count | Page Cache 中未命中的 page 数量。 |
| page_cache_hit_rate | Page Cache 命中率：`(page_cache_hit_count / (page_cache_hit_count + page_cache_miss_count))`。 |
| page_cache_hit_count_last_minute | 最近一分钟内从 Page Cache 中读取的 page 数量。 |
| page_cache_miss_count_last_minute | 最近一分钟内 Page Cache 中未命中的 page 数量。 |
| page_cache_hit_rate_last_minute | 最近一分钟内 Page Cache 的命中率。 |

### 查看 Data Cache 的底层执行状态 {/* #view-the-underlying-execution-state-of-data-cache */}

您可以通过访问以下 API 接口来获取 Data Cache 更详细的指标信息。

```Bash
http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/stat
```

结果如下：

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

### 指标说明 {/* #metric-description */}

| **指标**  | **说明**  |
| ---------------- | ---------------- |
| page_cache_mem_quota_bytes  | Page Cache 当前的内存限制。  |
| page_cache_mem_used_bytes  | Page Cache 当前实际使用的内存。  |
| page_cache_mem_used_rate  | Page Cache 当前的内存使用率。  |
| page_cache_hit_count  | Page Cache 命中次数。  |
| page_cache_miss_count  | Page Cache 未命中次数。  |
| page_cache_hit_rate  | Page Cache 的命中率。  |
| page_cache_hit_count_last_minute  | 最近一分钟内 Page Cache 的命中次数。  |
| page_cache_miss_count_last_minute  | 最近一分钟内 Page Cache 的未命中次数。  |
| page_cache_hit_rate_last_minute  | 最近一分钟内 Page Cache 的命中率。  |
| block_cache_status  | Block Cache 的状态，包括：`NORMAL`：实例运行正常。`ABNORMAL`：无法从缓存中读取或写入数据，需要通过日志定位问题。`UPDATING`：实例正在更新中，例如在线扩缩容时的更新状态。  |
| block_cache_disk_quota_bytes  | 用户配置的 Block Cache 磁盘缓存容量，单位为字节。  |
| block_cache_disk_used_bytes  | Block Cache 已使用的磁盘缓存空间，单位为字节。  |
| block_cache_disk_used_rate  | Block Cache 的实际磁盘缓存使用率，单位为百分比。  |
| block_cache_disk_spaces  | 用户配置的 Block Cache 磁盘缓存信息，包括每个缓存路径和缓存大小。  |
| block_cache_meta_used_bytes  | 用于缓存 Block Cache 元数据的内存空间，单位为字节。  |
| block_cache_hit_count  | Block Cache 的命中次数。  |
| block_cache_miss_count  | 缓存未命中次数。  |
| block_cache_hit_rate  | Block Cache 的命中率。  |
| block_cache_hit_bytes  | Block Cache 中命中的字节数。  |
| block_cache_miss_bytes  | Block Cache 中未命中的字节数。  |
| block_cache_hit_count_last_minute  | 最近一分钟内 Block Cache 的命中次数。 |
| block_cache_miss_count_last_minute | 最近一分钟内 Block Cache 的未命中次数。 |
| block_cache_hit_bytes_last_minute | 最近一分钟内 Block Cache 命中的类型数量。 |
| block_cache_miss_bytes_last_minute | 最近一分钟内 Block Cache 未命中的类型数量。 |
| block_cache_buffer_item_count | Block Cache 中当前 Buffer 实例的数量。Buffer 实例指的是通用数据缓存，例如从远端文件读取部分原始数据并直接将其缓存到内存或磁盘中的情况。 |
| block_cache_buffer_item_bytes | 用于在 Block Cache 中缓存 Buffer 实例的类型数量。 |
| block_cache_read_disk_bytes | 从 Block Cache 读取的字节数。 |
| block_cache_write_bytes | 写入 Block Cache 的字节数。 |
| block_cache_write_success_count | Block Cache 写入成功的次数。 |
| block_cache_write_fail_count | Block Cache 写入失败的次数。 |
| block_cache_remove_bytes | 从 Block Cache 中移除的字节数。 |
| block_cache_remove_success_count | 从 Block Cache 中成功执行移除操作的次数。 |
| block_cache_remove_fail_count | 从 Block Cache 中执行移除操作失败的次数。 |
| block_cache_current_reading_count | 当前在 Block Cache 中正在执行的读取操作数量。 |
| block_cache_current_writing_count | 当前在 Block Cache 中正在执行的写入操作数量。 |
| block_cache_current_removing_count | 当前在 Block Cache 中正在执行的移除操作数量。 |
