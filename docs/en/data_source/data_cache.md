---
displayed_sidebar: docs
---

# Data Cache

This topic describes the working principles of Data Cache and how to enable Data Cache to improve query performance on external data.

In data lake analytics, StarRocks works as an OLAP engine to scan data files stored in external storage systems, such as HDFS and Amazon S3. The I/O overhead increases as the number of files to scan increases. In addition, in some ad hoc scenarios, frequent access to the same data doubles I/O overhead.

To optimize the query performance in these scenarios, StarRocks 2.5 provides the Data Cache feature. This feature splits data in an external storage system into multiple blocks based on predefined policies and caches the data on StarRocks backends (BEs). This eliminates the need to pull data from external systems for each access request and accelerates queries and analysis on hot data. Data Cache only works when you query data from external storage systems by using external catalogs or external tables (excluding external tables for JDBC-compatible databases). It does not work when you query StarRocks native tables.

## How it works

StarRocks splits data in an external storage system into multiple blocks of the same size, and caches the data on BEs. Block is the smallest unit of data cache, which is configurable.

For example, if you set the block size to 1 MB and you want to query a Parquet file of 128 MB from Amazon S3, StarRocks splits the file into 128 blocks. The blocks are [0, 1 MB), [1 MB, 2 MB), [2 MB, 3 MB) ... [127 MB, 128 MB). StarRocks assigns a globally unique ID to each block, called a cache key. A cache key consists of the following three parts.

```Plain
hash(filename) + fileModificationTime + blockId
```

The following table provides descriptions of each part.

| **Component item** | **Description**                                              |
| ------------------ | ------------------------------------------------------------ |
| filename           | The name of the data file.                                   |
| fileModificationTime | The last modification time of the data file.                  |
| blockId            | The ID that StarRocks assigns to a block when splitting the data file. The ID is unique under the same data file but is not unique within your StarRocks cluster. |

If the query hits the [1 MB, 2 MB) block, StarRocks performs the following operations:

1. Check whether the block exists in the cache.
2. If the block exists, StarRocks reads the block from the cache. If the block does not exist, StarRocks reads the block from Amazon S3 and caches it on a BE.

After Data Cache is enabled, StarRocks caches data blocks read from external storage systems. If you do not want to cache such data blocks, run the following command:

```SQL
SET enable_populate_block_cache = false;
```

For more information about `enable_populate_block_cache`, see [System variables](../reference/System_variable.md).

## Storage media of blocks

StarRocks uses the memory and disks of BE machines to cache blocks. It supports cache solely on memory or on both the memory and disks.

If you use disks as the storage media, the cache speed is directly affected by the performance of disks. Therefore, we recommend that you use high-performance disks such as NVMe disks for data cache. If you do not have high-performance disks, you can add more disks to relieve disk I/O pressure.

## Cache replacement policies

StarRocks uses the [least recently used](https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)) (LRU) policy to cache and discard data.

- StarRocks first reads data from memory. If the data is not found in memory, StarRocks will read the data from disks and try to load the data read from disks into memory.
- Data discarded from memory will be written to disks. Data discarded from disks will be deleted.

## Enable Data Cache

Data Cache is disabled by default. To enable this feature, configure FEs and BEs in your StarRocks cluster.

### Configurations for FEs

You can enable Data Cache for FEs by using one of the following methods:

- Enable Data Cache for a given session based on your requirements.

  ```SQL
  SET enable_scan_block_cache = true;
  ```

- Enable Data Cache for all active sessions.

  ```SQL
  SET GLOBAL enable_scan_block_cache = true;
  ```

### Configurations for BEs

Add the following parameters to the **conf/be.conf** file of each BE. Then restart each BE to make the settings take effect.

| **Parameter**          | **Description**                                              | **Default value** |
| ---------------------- | ------------------------------------------------------------ | -------------------|
| block_cache_enable     | Whether to enable Data Cache.<ul><li>`true`: Data Cache is enabled.</li><li>`false`: Data Cache is disabled.</li></ul> | false |
| block_cache_disk_path  | The paths of disks. You can configure more than one disk and separate the disk paths with semicolons (;). We recommend that the number of paths you configured be the same as the number of disks of your BE machine. When the BE starts, StarRocks automatically creates a disk cache directory (the creation fails if no parent directory exists). | `${STARROCKS_HOME}/block_cache` |
| block_cache_meta_path  | The storage path of block metadata. You can leave this parameter unspecified. | `${STARROCKS_HOME}/block_cache` |
| block_cache_mem_size   | The maximum amount of data that can be cached in the memory. Unit: bytes. We recommend that you set the value of this parameter to at least 20 GB. If StarRocks reads a large amount of data from disks after Data Cache is enabled, consider increasing the value. | `2147483648`, which is 2 GB.  |
| block_cache_disk_size  | The maximum amount of data that can be cached in a single disk. Unit: bytes. For example, if you configure two disk paths for the `block_cache_disk_path` parameter and set the value of the `block_cache_disk_size` parameter to `21474836480` (20 GB), a maximum of 40 GB data can be cached in these two disks.  | `0`, which indicates that only the memory is used to cache data.  |

<<<<<<< HEAD
Examples of setting these parameters.
=======
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

`dataCacheOptions={populate: false}` indicates that the cache will not be populated because the query will scan all partitions.

You can also fine tune the population behavior of Data Cache via the Session Variable [populdate_datacache_mode](../sql-reference/System_variable.md#populate_datacache_mode).

### Population mode

StarRocks supports populating Data Cache in synchronous or asynchronous mode.

- Synchronous cache population

  In synchronous population mode, all the remote data read by the current query is cached locally. Synchronous population is efficient but may affect the performance of initial queries because it happens during data reading.

- Asynchronous cache population

  In asynchronous population mode, the system tries to cache the accessed data in the background, in order to minimize the impact on read performance. Asynchronous population can reduce the performance impact of cache population on initial reads, but the population efficiency is lower than synchronous population. Typically, a single query cannot guarantee that all the accessed data can be cached. Multiple attempts may be needed to cache all the accessed data.

From v3.3.0, asynchronous cache population is enabled by default. You can change the population mode by setting the session variable [enable_datacache_async_populate_mode](../sql-reference/System_variable.md).

## Footer Cache

In addition to caching data from files in remote storage during queries against data lakes, StarRocks also supports caching the metadata (Footer) parsed from files. Footer Cache directly caches the parsed Footer object in memory. When the same file's Footer is accessed in subsequent queries, the object descriptor can be obtained directly from the cache, avoiding repetitive parsing.

Currently, StarRocks supports caching Parquet Footer objects.

You can enable Footer Cache by setting the following system variable:

```SQL
SET GLOBAL enable_file_metacache=true;
```

> **NOTE**
>
> Footer Cache uses the memory module of the Data Cache for data caching. Therefore, you must ensure that the BE parameter `datacache_enable` is set to `true` and configure a reasonable value for `datacache_mem_size`.

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
>>>>>>> e06217c368 ([Doc] Ref docs (#50111))

```Plain

# Enable Data Cache.
block_cache_enable = true  

# Configure the disk path. Assume the BE machine is equipped with two disks.
block_cache_disk_path = /home/disk1/sr/dla_cache_data/;/home/disk2/sr/dla_cache_data/

# Set block_cache_mem_size to 2 GB.
block_cache_mem_size = 2147483648

# Set block_cache_disk_size to 1.2 TB.
block_cache_disk_size = 1288490188800
```

## Check whether a query hits data cache

You can check whether a query hits data cache by analyzing the following metrics in the query profile:

- `BlockCacheReadBytes`: the amount of data that StarRocks reads directly from its memory and disks.
- `BlockCacheWriteBytes`: the amount of data loaded from an external storage system to StarRocks' memory and disks.
- `BytesRead`: the total amount of data that is read, including data that StarRocks reads from an external storage system, its memory, and disks.

Example 1: In this example, StarRocks reads a large amount of data (7.65 GB) from the external storage system and only a few data (518.73 MB) from the memory and disks. This means that few data caches were hit.

```Plain
 - Table: lineorder
 - BlockCacheReadBytes: 518.73 MB
   - __MAX_OF_BlockCacheReadBytes: 4.73 MB
   - __MIN_OF_BlockCacheReadBytes: 16.00 KB
 - BlockCacheReadCounter: 684
   - __MAX_OF_BlockCacheReadCounter: 4
   - __MIN_OF_BlockCacheReadCounter: 0
 - BlockCacheReadTimer: 737.357us
 - BlockCacheWriteBytes: 7.65 GB
   - __MAX_OF_BlockCacheWriteBytes: 64.39 MB
   - __MIN_OF_BlockCacheWriteBytes: 0.00 
 - BlockCacheWriteCounter: 7.887K (7887)
   - __MAX_OF_BlockCacheWriteCounter: 65
   - __MIN_OF_BlockCacheWriteCounter: 0
 - BlockCacheWriteTimer: 23.467ms
   - __MAX_OF_BlockCacheWriteTimer: 62.280ms
   - __MIN_OF_BlockCacheWriteTimer: 0ns
 - BufferUnplugCount: 15
   - __MAX_OF_BufferUnplugCount: 2
   - __MIN_OF_BufferUnplugCount: 0
 - BytesRead: 7.65 GB
   - __MAX_OF_BytesRead: 64.39 MB
   - __MIN_OF_BytesRead: 0.00
```

Example 2: In this example, StarRocks reads a large amount of data (46.08 GB) from data cache and no data from the external storage system, which means StarRocks reads data only from data cache.

```Plain
Table: lineitem
- BlockCacheReadBytes: 46.08 GB
 - __MAX_OF_BlockCacheReadBytes: 194.99 MB
 - __MIN_OF_BlockCacheReadBytes: 81.25 MB
- BlockCacheReadCounter: 72.237K (72237)
 - __MAX_OF_BlockCacheReadCounter: 299
 - __MIN_OF_BlockCacheReadCounter: 118
- BlockCacheReadTimer: 856.481ms
 - __MAX_OF_BlockCacheReadTimer: 1s547ms
 - __MIN_OF_BlockCacheReadTimer: 261.824ms
- BlockCacheWriteBytes: 0.00 
- BlockCacheWriteCounter: 0
- BlockCacheWriteTimer: 0ns
- BufferUnplugCount: 1.231K (1231)
 - __MAX_OF_BufferUnplugCount: 81
 - __MIN_OF_BufferUnplugCount: 35
- BytesRead: 46.08 GB
 - __MAX_OF_BytesRead: 194.99 MB
 - __MIN_OF_BytesRead: 81.25 MB
```
<<<<<<< HEAD
=======

## Configurations and variables

You can configure Data Cache using the following system variables and BE parameters.

### System variables

- [populdate_datacache_mode](../sql-reference/System_variable.md#populate_datacache_mode)
- [enable_datacache_io_adaptor](../sql-reference/System_variable.md#enable_datacache_io_adaptor)
- [enable_file_metacache](../sql-reference/System_variable.md#enable_file_metacache)
- [enable_datacache_async_populate_mode](../sql-reference/System_variable.md)

### BE Parameters

- [datacache_enable](../administration/management/BE_configuration.md#datacache_enable)
- [datacache_disk_path](../administration/management/BE_configuration.md#datacache_disk_path)
- [datacache_meta_path](../administration/management/BE_configuration.md#datacache_meta_path)
- [datacache_mem_size](../administration/management/BE_configuration.md#datacache_mem_size)
- [datacache_disk_size](../administration/management/BE_configuration.md#datacache_disk_size)
- [datacache_auto_adjust_enable](../administration/management/BE_configuration.md#datacache_auto_adjust_enable)
- [datacache_disk_high_level](../administration/management/BE_configuration.md#datacache_disk_high_level)
- [datacache_disk_safe_level](../administration/management/BE_configuration.md#datacache_disk_safe_level)
- [datacache_disk_low_level](../administration/management/BE_configuration.md#datacache_disk_low_level)
- [datacache_disk_adjust_interval_seconds](../administration/management/BE_configuration.md#datacache_disk_adjust_interval_seconds)
- [datacache_disk_idle_seconds_for_expansion](../administration/management/BE_configuration.md#datacache_disk_idle_seconds_for_expansion)
- [datacache_min_disk_quota_for_adjustment](../administration/management/BE_configuration.md#datacache_min_disk_quota_for_adjustment)
>>>>>>> e06217c368 ([Doc] Ref docs (#50111))
