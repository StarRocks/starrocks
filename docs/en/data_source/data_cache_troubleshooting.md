---
displayed_sidebar: docs
---

# Data Cache FAQ

This topic describes some common problems and troubleshooting steps encountered when using Data Cache to help users quickly locate related problems.

## Data Cache Enabled

###How to confirm whether the current Data Cache is successfully enabled?

You can usually determine whether Data Cache is successfully enabled in the current system by using the following methods (select one of them):

* Execute `show backends` (or `show compute nodes`) in your SQL client, check the `DataCacheMetrics` value, and confirm whether the disk or memory cache quota is greater than 0.

```SQL
mysql> show backends \G
*************************** 1. row ***************************
            BackendId: 89041
                   IP: X.X.X.X
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
        LastStartTime: 2025-05-29 14:45:37
        LastHeartbeat: 2025-05-29 19:20:32
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 10
     DataUsedCapacity: 0.000 B
        AvailCapacity: 1.438 TB
        TotalCapacity: 1.718 TB
              UsedPct: 16.27 %
       MaxDiskUsedPct: 16.27 %
               ErrMsg:
              Version: main-c15b412
               Status: {"lastSuccessReportTabletsTime":"2025-05-29 19:20:30"}
    DataTotalCapacity: 1.438 TB
          DataUsedPct: 0.00 %
             CpuCores: 8
             MemLimit: 50.559GB
    NumRunningQueries: 0
           MemUsedPct: 0.50 %
           CpuUsedPct: 0.2 %
     DataCacheMetrics: Status: Normal, DiskUsage: 44MB/1TB, MemUsage: 0B/0B
             Location:
           StatusCode: OK
1 row in set (0.00 sec)
```

In the above example, the disk cache quota of Data Cache is 1TB, and 44MB is currently used; the memory cache quota is 0B, and memory cache is not enabled.

* You can visit the `http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/stat` page to check the current Data Cache quota, hit rate and other metrics, and check whether `disk_quota_bytes` and `mem_quota_bytes` are greater than 0.

![](https://docs.starrocks.io/zh/assets/images/data_cache_observe-dba68d3bcfbe64559d30787e5d09ff43.png)

###Why Data Cache is not enabled by default?

Starting from version 3.3, BE will attempt to enable Data Cache upon startup. However, if there is insufficient remaining space on the current disk, Data Cache will not be enabled by default.

It may include the following situations:

* Percentage: The current disk usage is high.

* Remaining size: The remaining disk space is relatively small.

Therefore, when encountering the problem that Data Cache is not enabled by default, you can first check the current disk usage and increase disk capacity if necessary.

Alternatively, you can manually enable Data Cache by configuring cache quota based on the current available disk space.

```
# disable datacache automatic adjustment
datacache_auto_adjust_enable = false
# set datacache disk quota manually
datacache_disk_size = 1T
```

##Data Cache Effective

###What catalog types does Data Cache currently support?

Data Cache currently supports External Catalog types that use StarRocks Native File Reader (such as Parquet/ORC/CSV Reader), including Hive, Iceberg, Hudi, Delta Lake, Paimon, etc. Catalogs that access data based on JNI (such as JDBC Catalog) are not supported yet.

**NOTICE**：Some catalogs may select different data access methods based on certain conditions (such as file type, data status, etc.). For example, for the Paimon catalog, StarRocks may automatically choose whether to use Native File Reader or JNI to access data based on the compaction status of the current data. When JNI is used to access Paimon data, Data Cache acceleration is also not supported.

###How to confirm whether the current query hits the cache?

You can check DataCache-related metrics through the profile of this query, and check `DataCacheReadBytes` and `DataCacheReadCounter` to determine the local cache hit status.

```
 - DataCacheReadBytes: 518.73 MB
   - __MAX_OF_DataCacheReadBytes: 4.73 MB
   - __MIN_OF_DataCacheReadBytes: 16.00 KB
 - DataCacheReadCounter: 684
   - __MAX_OF_DataCacheReadCounter: 4
   - __MIN_OF_DataCacheReadCounter: 0
 - DataCacheReadTimer: 737.357us
 - DataCacheWriteBytes: 7.65 GB
   - __MAX_OF_DataCacheWriteBytes: 64.39 MB
   - __MIN_OF_DataCacheWriteBytes: 0.00
 - DataCacheWriteCounter: 7.887K (7887)
   - __MAX_OF_DataCacheWriteCounter: 65
   - __MIN_OF_DataCacheWriteCounter: 0
 - DataCacheWriteTimer: 23.467ms
   - __MAX_OF_DataCacheWriteTimer: 62.280ms
   - __MIN_OF_DataCacheWriteTimer: 0ns
```

###Data Cache has been enabled, why does the current query not hit the cache?

1. Check whether Data Cache support the current catalog type.
2. Confirm whether the current query statement meets the cache population conditions. By default, Data Cache will reject cache population of some queries through certain rules to avoid cache pollution. For details, see [Data Cache Population Rules](https://docs.starrocks.io/docs/data_source/data_cache/#population-rules)。

The `EXPLAIN VERBOSE` command can be used to check whether the current query triggers cache population.
Example:

```sql
mysql> explain verbose select col1 from hudi_table;
|   0:HudiScanNode                        |
|      TABLE: hudi_table                  |
|      partitions=3/3                     |
|      cardinality=9084                   |
|      avgRowSize=2.0                     |
|      dataCacheOptions={populate: false} |
|      cardinality: 9084                  |
+-----------------------------------------+
```

In the above example, the `populate` field of `dataCacheOptions` is `false`, so the cache will not be populated for the current query.

If you do need to cache this query, you can modify the default population behavior by setting `populate_datacache_mode='always'`.

##Data Cache Hit

###Why sometimes the same query needs to be executed multiple times to fully hit the cache?

In the current version, Data Cache uses asynchronous population by default to reduce the impact of cache population on query performance. When using asynchronous population, the system will try to cache the accessed data in the background without affecting the read performance as much as possible. Therefore, usually a single query can only populate a part of the remote data locally, and multiple executions are required to cache all the data required by the query.

You can also use the synchronous cache population by setting `enable_datacache_async_populate_mode=false`, or warm up the target data in advance by `CACHE SELECT`.

###Why is it that all the data in the current query has been cached, but there is still a few data accessed remotely?

In the current version, IO adaptation is enabled by default to optimize cache performance when disk IO load is high, which may result in a small number of requests directly accessing the remote end in some cases.

You can also turn off the IO adaptive function by setting `enable_datacache_io-adapter=false`

##Others

###How to clear the cached data?

Currently, Data Cache does not provide a direct clear interface to clear cached data, but users can choose one of the following methods to clear cached data:

* You can clean up cached data by deleting all data (including block files and meta directories) in the datacache directory on the BE/CN node, and then restarting the BE/CN node. (Recommended)

* If it is inconvenient to restart the BE/CN node, you can also indirectly clean cached data by scaling cache quota online. Assuming that the user's disk cache quota is configured to 2T earlier, you can first scale it down to 0 (the system automatically cleans up cache data), and then restore it to 2T. For example:

```SQL
UPDATE be_configs SET VALUE="0" WHERE NAME="datacache_disk_size" and BE_ID=10005;
UPDATE be_configs SET VALUE="2T" WHERE NAME="datacache_disk_size" and BE_ID=10005;
```

**NOTICE**：When cleaning cache data by online scaling method, be sure to pay attention to the `WHERE` condition in the statement to avoid accidentally damaging other irrelevant parameters or nodes.

###How to improve Data Cache performance?

Cache is essentially accessing local memory or disk instead of accessing a remote storage system. Therefore, cache performance is directly related to the local cache medium. If you find that cache access latency is high due to high disk load, you need to consider improving the performance of the local cache medium:

* Prioritize using high-performance NVME disks as cache disks.

* If high-performance disks cannot be selected, it is also possible to share the IO pressure of a single disk by adding multiple disks.

* Increase BE/CN node memory (referring to increasing machine memory, rather than increasing the Data Cache memory quota), using the operating system page cache to reduce the number of direct disk accesses and reduce disk pressure.
