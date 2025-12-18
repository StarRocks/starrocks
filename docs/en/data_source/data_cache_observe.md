---
displayed_sidebar: docs
---

# Data Cache observability

In earlier versions, there are no rich metrics or efficient methods to monitor the performance, usage, and health of [Data Cache](./data_cache.md).

In v3.3, StarRocks improves the observability of Data Cache by offering efficient monitoring methods and more metrics. Users can now check the overall disk and memory usage of the data cache, and related metrics, enhancing monitoring of cache usage.

> **NOTE**
>
> From v3.4.0 onwards, queries against external catalogs and cloud-native tables (in shared-data clusters) use a unified Data Cache instance. Therefore, unless otherwise specified, the following methods default to displaying the metrics of the Data Cache instance itself, which includes the cache usage of queries against both external catalogs and cloud-native tables.

## SQL commands

You can run SQL commands to view the capacity and usage of Data Cache on each BE node.

### SHOW BACKENDS

The `DataCacheMetrics` field records the used disk and memory space of Data Cache on a specific BE.

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

The `be_datacache_metrics` view in `information_schema` records the following Data Cache-related information.

```Bash
mysql> select * from information_schema.be_datacache_metrics;
+-------+--------+------------------+-----------------+-----------------+----------------+-----------------+----------------------------------------------------------------------------------------------+
| BE_ID | STATUS | DISK_QUOTA_BYTES | DISK_USED_BYTES | MEM_QUOTA_BYTES | MEM_USED_BYTES | META_USED_BYTES | DIR_SPACES                                                                                   |
+-------+--------+------------------+-----------------+-----------------+----------------+-----------------+----------------------------------------------------------------------------------------------+
| 10004 | Normal |       2147483648 |               0 |     32706263420 |              0 |               0 | [{"Path":"/home/disk1/datacache","QuotaBytes":2147483648}] |
+-------+--------+------------------+-----------------+-----------------+----------------+-----------------+----------------------------------------------------------------------------------------------+
1 row in set (5.41 sec)
```

- `BE_ID`: the BE ID
- `STATUS`: the BE status
- `DISK_QUOTA_BYTES`: the disk cache capacity configured by users, in bytes
- `DISK_USED_BYTES`: the disk cache space that has been used, in bytes
- `MEM_QUOTA_BYTES`: the memory cache capacity configured by users, in bytes
- `MEM_USED_BYTES`: the memory cache space that has been used, in bytes
- `META_USED_BYTES`: the space used to cache metadata
- `DIR_SPACES`: the cache path and its cache size

## API call

Since v3.3.2, StarRocks provides two APIs to get cache metrics, which reflect the cache state at different levels:

- `/api/datacache/app_stat`: Query the Block Cache and Page Cache hit rates.
- `/api/datacache/stat`: the underlying execution state of Data Cache. This interface is mainly used for maintenance and bottleneck identification of Data Cache. It does not reflect the actual hit rate of the query. Common users do not need to pay attention to this interface.

### View cache hit metrics

View the cache hit metrics by accessing the following API interface:

```bash
http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/app_stat
```

Return:

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

| **Metric**                         | **Description**                                                                                     |
|------------------------------------|-----------------------------------------------------------------------------------------------------|
| block_cache_hit_bytes              | Bytes read from the block cache.                                                                    |
| block_cache_miss_bytes             | Bytes read from remote storage (Block Cache misses).                                                |
| block_cache_hit_rate               | Block Cache hit rate, `(block_cache_hit_bytes / (block_cache_hit_bytes + block_cache_miss_bytes))`. |
| block_cache_hit_bytes_last_minute  | Bytes read from the Block Cache in the last minute.                                                 |
| block_cache_miss_bytes_last_minute | Bytes read from the remote storage in the last minute.                                              |
| block_cache_hit_rate_last_minute   | Block Cache hit rate in the last minute.                                                            |
| page_cache_hit_count               | Number of pages read from the Page Cache.                                                           |
| page_cache_miss_count              | Number of pages missed in the Page Cache.                                                           |
| block_cache_hit_rate               | Page Cache hit rate: `(page_cache_hit_count / (page_cache_hit_count + page_cache_miss_count))`.     |
| page_cache_hit_count_last_minute   | Number of pages read from the Page Cache in the last minute.                                        |
| page_cache_miss_count_last_minute  | Number of pages missed in the Page Cache in the last minute.                                        |
| page_cache_hit_rate_last_minute    | Page Cache hit rate in the last minute.                                                             |

### View the underlying execution state of Data Cache

You can get more detailed metrics on the Data Cache by accessing the following API interfaces.

```Bash
http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/stat
```

The results are as follows:

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

### Metric description

| **Metric**                         | **Description**                                                                                                                                                                                                                                                        |
|------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| page_cache_mem_quota_bytes         | Current memory limit of Page Cache.                                                                                                                                                                                                                                    |
| page_cache_mem_used_bytes	         | Current actual memory used by Page Cache.                                                                                                                                                                                                                              |
| page_cache_mem_used_rate	          | Current memory usage rate of Page Cache.                                                                                                                                                                                                                               |
| page_cache_hit_count	              | Number of Page Cache hits.                                                                                                                                                                                                                                             |
| page_cache_miss_count	             | Number of Page Cache misses.                                                                                                                                                                                                                                           |
| page_cache_hit_rate	               | Hit rate of Page Cache.                                                                                                                                                                                                                                                |
| page_cache_hit_count_last_minute	  | Number of Page Cache hits in the last minute.                                                                                                                                                                                                                          |
| page_cache_miss_count_last_minute  | Number of Page Cache misses in the last minute.                                                                                                                                                                                                                        |
| page_cache_hit_rate_last_minute	   | Hit rate of Page Cache in the last minute.                                                                                                                                                                                                                             |
| block_cache_status                 | Status of the Block Cache, including:`NORMAL`: The instance runs normally.`ABNORMAL`: Data cannot be read or written into the cache. The issue must be located using logs.`UPDATING`: The instance is being updated, such as the updating state during online scaling. |
| block_cache_disk_quota_bytes       | The disk cache capacity of Block Cache configured by users, in bytes.                                                                                                                                                                                                  |
| block_cache_disk_used_bytes        | The disk cache space that has been used by Block Cache, in bytes.                                                                                                                                                                                                      |
| block_cache_disk_used_rate         | The actual disk cache usage rate of Block Cache, in percentages.                                                                                                                                                                                                       |
| block_cache_disk_spaces            | The disk cache information of Block Cache configured by users, including each cache path and the cache size.                                                                                                                                                           |
| block_cache_meta_used_bytes        | The memory space used to cache Block Cache metadata, in bytes.                                                                                                                                                                                                         |
| block_cache_hit_count              | Number of Block Cache hits.                                                                                                                                                                                                                                            |
| block_cache_miss_count             | Number of cache misses.                                                                                                                                                                                                                                                |
| block_cache_hit_rate               | Hit rate of Block Cache.                                                                                                                                                                                                                                               |
| block_cache_hit_bytes              | Number of bytes that are hit in the Block Cache.                                                                                                                                                                                                                       |
| block_cache_miss_bytes             | Number of bytes that are missed in the Block Cache.                                                                                                                                                                                                                    |
| block_cache_hit_count_last_minute  | Number of Block Cache hits in the last minute.                                                                                                                                                                                                                         |
| block_cache_miss_count_last_minute | Number of Block Cache misses in the last minute.                                                                                                                                                                                                                       |
| block_cache_hit_bytes_last_minute  | Number of types hit by Block Cache in the last minute.                                                                                                                                                                                                                 |
| block_cache_miss_bytes_last_minute | Number of types missed by Block Cache in the last minute.                                                                                                                                                                                                              |
| block_cache_buffer_item_count      | The current number of Buffer instances in the Block Cache. Buffer instances refer to common data caches, such as when reading part of the raw data from a remote file and caching the data directly in memory or on disks.                                             |
| block_cache_buffer_item_bytes      | Number of types used to cache the Buffer instance in Block Cache.                                                                                                                                                                                                      |
| block_cache_read_disk_bytes        | Number of bytes read from Block Cache.                                                                                                                                                                                                                                 |
| block_cache_write_bytes            | Number of bytes written to Block Cache.                                                                                                                                                                                                                                |
| block_cache_write_success_count    | Number of successful Block Cache writes.                                                                                                                                                                                                                               |
| block_cache_write_fail_count       | Number of failed Block Cache writes.                                                                                                                                                                                                                                   |
| block_cache_remove_bytes           | Number of bytes removed from Block Cache.                                                                                                                                                                                                                              |
| block_cache_remove_success_count   | Number of successful remove operations from Block Cache.                                                                                                                                                                                                               |
| block_cache_remove_fail_count      | Number of failed remove operations from Block Cache.                                                                                                                                                                                                                   |
| block_cache_current_reading_count  | Number of read operations currently being executed in Block Cache.                                                                                                                                                                                                     |
| block_cache_current_writing_count  | Number of write operations currently being executed in Block Cache.                                                                                                                                                                                                    |
| block_cache_current_removing_count | Number of remove operations currently being executed in Block Cache.                                                                                                                                                                                                   |
