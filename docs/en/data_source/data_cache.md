---
displayed_sidebar: docs
---

# Data Cache

This topic will introduce the core principles of Data Cache and how to accelerate data queries by enabling the Data Cache feature.

Data Cache is used to cache data from internal tables and external tables. This feature is enabled by default starting from version v3.3.0, and beginning with version V4.0, the memory caches and disk caches of internal tables and external tables have been uniformly integrated into the Data Cache system for management.

Data Cache consists of two components: Page Cache (memory cache) and Block Cache (disk cache).

## Principles of Page Cache

As an in-memory cache, Page Cache is responsible for storing data pages of internal and external tables after decompression. The size of these pages is not fixed. Currently, Page Cache supports caching the following types of data:

1. Data pages and index pages of internal tables
2. Footer information of external table data files
3. Partial decompressed data pages of external tables

Page Cache currently uses the LRU (Least Recently Used) strategy for data eviction.

## Principles of Block Cache

Block Cache is a disk-based cache whose primary function is to cache data files—from external tables and internal tables in compute-storage separation scenarios that are stored in remote object storage—to local disks. This reduces remote data access latency and improves query efficiency. The size of each Block is fixed.

### Background and Value of the Feature

In data lake analytics or internal table scenarios with shared-data mode, StarRocks—acting as an OLAP query engine—needs to scan data files stored in HDFS or object storage (hereinafter collectively referred to as the "external storage system"). This process faces two major performance bottlenecks:

* The more files a query needs to read, the greater the remote I/O overhead.
* In ad-hoc query scenarios, frequent access to the same data leads to redundant remote I/O consumption.

To address these issues, StarRocks has introduced the Block Cache feature since version 2.5. It splits raw data from the external storage system into multiple blocks based on a specific strategy and caches these blocks in the local BE nodes of StarRocks. By avoiding repeated retrieval of remote data, the query and analysis performance for hot data is significantly improved.

### Applicable Scenarios

- When querying data from external storage systems using external tables (excluding JDBC external tables).
- When querying StarRocks native tables in the shared-data mode.

### Core Working Mechanism

**Data Splitting and Cache Unit**

When StarRocks caches remote files, it splits the original files into blocks of equal size according to the configured strategy (a block is the minimum cache unit, and its size is customizable).

- Example: If the block size is configured as 1 MB, when querying a 128 MB Parquet file on Amazon S3, the file will be split into 128 consecutive blocks (e.g., [0, 1 MB), [1 MB, 2 MB), ..., [127 MB, 128 MB)).

Each block is assigned a globally unique cache identifier (cache key), which consists of the following three parts:

```Plain
hash(filename) + fileModificationTime + blockId
```

The description of each component is as follows:

| **Component** | **Description**|
|----|----|
|filename|The name of the data file.|
|fileModificationTime|The last modification time of the data file.|
|blockId|The ID assigned to each block by StarRocks when splitting a data file. This ID is unique within a single file but not globally unique.|

### Cache Medium

Block Cache uses the local disks of BE nodes as its storage medium, and the cache acceleration effect is directly related to disk performance:

- It is recommended to use high-performance local disks (such as NVMe disks) to minimize cache read/write latency.
- If disk performance is average, you can increase the number of disks to achieve load balancing and reduce I/O pressure on individual disks.

### Cache replacement policies

Block Cache supports two data caching and eviction strategies, with SLRU (Segmented LRU) being the default:

| **Strategy** | **Core Logic** | **Advantage** |
|----|----|----|
|LRU|Based on the "Least Recently Used" principle: evicts the blocks that have not been accessed for the longest time.|Simple to implement, suitable for scenarios with stable access patterns.|
|SLRU|Divides the cache space into an eviction segment and a protection segment, both following the LRU rule: <br>1. Data enters the eviction segment on first access; <br>2. Data in the eviction segment is promoted to the protection segment when accessed again; <br>3. Data evicted from the protection segment falls back to the eviction segment, while data evicted from the eviction segment is directly removed from the cache. |Effectively resists sudden sparse traffic, preventing "temporary data accessed only once" from directly evicting hot data in the protection segment. It offers better stability than LRU.|

## Enable Data Cache

The Data Cache feature is enabled by default. Its activation status and resource limits are controlled through configuration parameters on BE nodes. Details are as follows:

### Switch Parameters

1. Master Switch: Controls Overall Activation/Deactivation of Data Cache

- Parameter Name: datacache_enable
- Function: Serves as the master switch for the Data Cache feature, directly controlling the overall status of both Page Cache and Block Cache.
- Value Description:
  - true (default): Data Cache is fully enabled; Page Cache and Block Cache can be independently controlled via their respective sub-switches.
  - false: Data Cache is fully disabled; both Page Cache and Block Cache become ineffective.

2. Sub-Cache Switches: Independently Control Page Cache/Block Cache

| Sub-Cache Type	 |Parameter Name	|Function	| Value Description |
|-----------------|----|----|-------------------|
| Page Cache      |disable_storage_page_cache	|Controls Page Cache on/off	|false (default, enabled); true (disabled)|
| Block Cache     |block_cache_enable	|Controls Block Cache on/off	|true (default, enabled); false (disabled)|

### Resource Limit Configuration Parameters

Use the following BE node parameters to set the maximum memory and disk usage limits for Data Cache, preventing excessive resource occupation:

- datacache_mem_size: Sets the maximum memory usage limit for Data Cache (used for storing data in Page Cache).
- datacache_disk_size: Sets the maximum disk usage limit for Data Cache (used for storing data in Block Cache).

## Populate block cache

### Population rules

Since v3.3.2, in order to improve the cache hit rate of Block Cache, StarRocks populates Block Cache according to the following rules:

- The cache will not be populated for statements that are not `SELECT`, for example, `ANALYZE TABLE` and `INSERT INTO SELECT`.
- Queries that scan all partitions of a table will not populate the cache. However, if the table has only one partition, population is performed by default.
- Queries that scan all columns of a table will not populate the cache. However, if the table has only one column, population is performed by default.
- The cache will not be populated for tables that are not Hive, Paimon, Delta Lake, Hudi, or Iceberg.

### Viewing Cache Population Behavior

You can view the population behavior for a specific query with the `EXPLAIN VERBOSE` command.

Example:

```sql
mysql> explain verbose select col1 from hudi_table;
...
|   0:HudiScanNode                        |
|      TABLE: hudi_table                  |
|      partitions=3/3                     |
|      cardinality=9084                   |
|      avgRowSize=2.0                     |
|      dataCacheOptions={populate: false} |
|      cardinality: 9084                  |
+-----------------------------------------+
```

`dataCacheOptions={populate: false}` indicates that the cache will not be populated because the query will scan all partitions.

You can also fine tune the population behavior of Block Cache via the Session Variable [populate_datacache_mode](../sql-reference/System_variable.md#populate_datacache_mode).

### Population mode

Block Cache supports two modes: synchronous population and asynchronous population. You can choose between them based on your business requirements for "first query performance" and "cache efficiency":

|Population Mode|Core Logic|Pros and Cons Comparison|
|----|----|----|
|Synchronous Population	|When remote data is read for the first query, the data is immediately cached locally. Subsequent queries can directly reuse the cache.	|Pros: High cache efficiency—data caching is completed with a single query. <br>Cons: Cache operations are executed synchronously with read operations, which may increase latency for the first query.|
|Asynchronous Population	|For the first query, data reading is prioritized and completed first. Cache writing is executed asynchronously in the background, without blocking the current query process.	|Pros: Does not affect first query performance and prevents read operations from being delayed due to caching. <br>Cons: Lower cache efficiency—a single query may not fully cache all accessed data, requiring multiple queries to gradually improve cache coverage.|

From v3.3.0, asynchronous cache population is enabled by default. You can change the population mode by setting the session variable [enable_datacache_async_populate_mode](../sql-reference/System_variable.md).

### Persistence

The cached data in disks can be persistent by default, and these data can be reused after BE restarts.

## Check whether a query hits data cache

You can check whether a query hits data cache by analyzing the following metrics in the query profile:

- `DataCacheReadBytes`: the size of data that StarRocks reads directly from its memory and disks.
- `DataCacheWriteBytes`: the size of data loaded from an external storage system to StarRocks' memory and disks.
- `BytesRead`: the total amount of data that is read, including data that StarRocks reads from an external storage system, and its memory and disks.

Example 1: In this example, StarRocks reads a large amount of data (7.65 GB) from the external storage system and only a few data (518.73 MB) from the disks. This means that few block caches were hit.

```Plain
 - Table: lineorder
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
 - BufferUnplugCount: 15
   - __MAX_OF_BufferUnplugCount: 2
   - __MIN_OF_BufferUnplugCount: 0
 - BytesRead: 7.65 GB
   - __MAX_OF_BytesRead: 64.39 MB
   - __MIN_OF_BytesRead: 0.00
```

Example 2: In this example, StarRocks reads a large amount of data (46.08 GB) from data cache and no data from the external storage system, which means StarRocks reads data only from block cache.

```Plain
Table: lineitem
- DataCacheReadBytes: 46.08 GB
 - __MAX_OF_DataCacheReadBytes: 194.99 MB
 - __MIN_OF_DataCacheReadBytes: 81.25 MB
- DataCacheReadCounter: 72.237K (72237)
 - __MAX_OF_DataCacheReadCounter: 299
 - __MIN_OF_DataCacheReadCounter: 118
- DataCacheReadTimer: 856.481ms
 - __MAX_OF_DataCacheReadTimer: 1s547ms
 - __MIN_OF_DataCacheReadTimer: 261.824ms
- DataCacheWriteBytes: 0.00 
- DataCacheWriteCounter: 0
- DataCacheWriteTimer: 0ns
- BufferUnplugCount: 1.231K (1231)
 - __MAX_OF_BufferUnplugCount: 81
 - __MIN_OF_BufferUnplugCount: 35
- BytesRead: 46.08 GB
 - __MAX_OF_BytesRead: 194.99 MB
 - __MIN_OF_BytesRead: 81.25 MB
```

## I/O Adaptor

To prevent significant tail latency in disk access due to high cache disk I/O load, which can lead to negative optimization of the cache system, Data Cache provides the I/O adaptor feature. This feature routes some cache requests to remote storage when disk load is high, utilizing both local cache and remote storage to improve I/O throughput. This feature is enabled by default.

You can enable I/O Adaptor by setting the following system variable:

```SQL
SET GLOBAL enable_datacache_io_adaptor=true;
```

## Dynamic Scaling

Data Cache supports manual adjustment of cache capacity without restarting the BE process, and also supports automatic adjustment of cache capacity.

### Manual Scaling

You can modify Data Cache's memory limit or disk capacity by dynamically adjusting BE configuration items.

Examples:

```SQL
-- Adjust the Data Cache memory limit for a specific BE instance.
UPDATE be_configs SET VALUE="10G" WHERE NAME="datacache_mem_size" and BE_ID=10005;

-- Adjust the Data Cache memory ratio limit for all BE instances.
UPDATE be_configs SET VALUE="10%" WHERE NAME="datacache_mem_size";

-- Adjust the Data Cache disk limit for all BE instances.
UPDATE be_configs SET VALUE="2T" WHERE NAME="datacache_disk_size";
```

> **NOTE**
>
> - Be cautious when adjusting capacities in this way. Make sure not to omit the WHERE clause to avoid modifying irrelevant configuration items.
> - Cache capacity adjustments made this way will not be persisted and will be lost after the BE process restarts. Therefore, you can first adjust the parameters dynamically as described above, and then manually modify the BE configuration file to ensure that the changes take effect after the next restart.

### Automatic Scaling

StarRocks currently supports automatic scaling of disk capacity. If you do not specify the cache disk path and capacity limit in the BE configuration, automatic scaling is enabled by default.

You can also enable automatic scaling by adding the following configuration item to the BE configuration file and restarting the BE process:

```Plain
datacache_auto_adjust_enable=true
```

After automatic scaling is enabled:

- When the disk usage exceeds the threshold specified by the BE parameter `disk_high_level` (default value is `90`, that is, 90% of disk space), the system will automatically evict cache data to free up disk space.
- When the disk usage is consistently below the threshold specified by the BE parameter `disk_low_level` (default value is `60`, that is, 60% of disk space), and the current disk space used by Data Cache is full, the system will automatically expand the cache capacity.
- When automatically scaling the cache capacity, the system will aim to adjust the cache capacity to the level specified by the BE parameter `disk_safe_level` (default value is `80`, that is, 80% of disk space).

## Cache Sharing

Because Data Cache depends on the BE node's local disk, when the cluster are being scaled, changes in data routing can cause cache misses, which can easily lead to significant performance degradation during the elastic scaling.

Cache Sharing is used to support accessing cache data between nodes through network. During cluster scaling, if a local cache miss occurs, the system first attempts to fetch cache data from other nodes within the same cluster. Only if all caches miss will the system re-fetch data from the remote storage. This feature effectively reduces the performance jitter caused by cache invalidation during elastic scaling and ensures stable query performance.

![cache sharing workflow](../_assets/cache_sharing_workflow.png)

You can enable the Cache Sharing feature by configuring the following two items:

- Set the FE configuration item `enable_trace_historical_node` to `true`.
- Set the system variable `enable_datacache_sharing` to `true`.

In addition, you can check the following metrics in query profile to monitor Cache Sharing:

- `DataCacheReadPeerCounter`: The read count from other cache nodes.
- `DataCacheReadPeerBytes`: The bytes read from other cache nodes.
- `DataCacheReadPeerTimer`: The time used for accessing cache data from other cache nodes.

## Configurations and variables

You can configure Data Cache using the following system variables and parameters.

### System variables

- [populate_datacache_mode](../sql-reference/System_variable.md#populate_datacache_mode)
- [enable_datacache_io_adaptor](../sql-reference/System_variable.md#enable_datacache_io_adaptor)
- [enable_file_metacache](../sql-reference/System_variable.md#enable_file_metacache)
- [enable_datacache_async_populate_mode](../sql-reference/System_variable.md)
- [enable_datacache_sharing](../sql-reference/System_variable.md#enable_datacache_sharing)

### FE Parameters

- [enable_trace_historical_node](../administration/management/FE_configuration.md#enable_trace_historical_node)

### BE Parameters

- [datacache_enable](../administration/management/BE_configuration.md#datacache_enable)
- [datacache_mem_size](../administration/management/BE_configuration.md#datacache_mem_size)
- [datacache_disk_size](../administration/management/BE_configuration.md#datacache_disk_size)
- [datacache_auto_adjust_enable](../administration/management/BE_configuration.md#datacache_auto_adjust_enable)
- [datacache_disk_high_level](../administration/management/BE_configuration.md#datacache_disk_high_level)
- [datacache_disk_safe_level](../administration/management/BE_configuration.md#datacache_disk_safe_level)
- [datacache_disk_low_level](../administration/management/BE_configuration.md#datacache_disk_low_level)
- [datacache_disk_adjust_interval_seconds](../administration/management/BE_configuration.md#datacache_disk_adjust_interval_seconds)
- [datacache_disk_idle_seconds_for_expansion](../administration/management/BE_configuration.md#datacache_disk_idle_seconds_for_expansion)
- [datacache_min_disk_quota_for_adjustment](../administration/management/BE_configuration.md#datacache_min_disk_quota_for_adjustment)
- [datacache_eviction_policy](../administration/management/BE_configuration.md#datacache_eviction_policy)
- [datacache_inline_item_count_limit](../administration/management/BE_configuration.md#datacache_inline_item_count_limit)
