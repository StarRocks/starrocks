# Block cache

This topic describes the working principles of block cache and how to enable block cache to improve query performance on external data.

In data lake analytics, StarRocks scans data files stored in external storage systems, such as HDFS and Amazon S3. The I/O overhead of an external storage system is affected by the amount of scanned data. The larger the amount of data scanned, the more I/O overhead it causes. In addition, in some Ad Hoc (instant query) scenarios, locality of reference causes duplicate I/O overhead for an external storage system.

To optimize the query performance in these scenarios, StarRocks 2.5 and later versions provide block cache. This feature accelerates queries and analysis by caching hot data from external storage systems into blocks. Block cache only works when you query data from external storage systems by using external tables (except for external tables for JDBC-compatible databases) or external catalogs. It does not work when you query native tables stored in StarRocks.

## How it works

For example, if you query a Parquet data file of 128 MB from Amazon S3, StarRocks splits the file into multiple blocks (By default, the size of each block is 1 MB and we recommend you retain the default setting). In this case, StarRocks splits the file into 128 blocks: [0, 1 MB), [1 MB, 2 MB), [2 MB, 3 MB) ... [127 MB, 128 MB). StarRocks assigns a globally unique ID to each block, called a cache key. A cache key consists of the following three parts.

```Plain
hash(filename) + filesize + blockId
```

The following table provides descriptions of each part.

| **Component item** | **Description**                                              |
| ------------------ | ------------------------------------------------------------ |
| filename           | The name of the data file.                                   |
| filesize           | The size of the data file, such as 1MB.                      |
| blockId            | The ID that StarRocks assigns to a block when splitting the data file. The ID is unique under the same data file but is not unique within your StarRocks cluster. |

If the query hits the [1 MB, 2 MB) block, StarRocks performs the following operations:

1. Check whether the block exists in the cache.
2. If the block exists, StarRocks reads the block from the cache. If the block does not exist, StarRocks reads the block from Amazon S3 and caches it on a BE.

By default, StarRocks caches data blocks read from external storage systems. If you do not want to cache data blocks read from external storage systems, run the following command.

```SQL
SET enable_populate_block_cache = false;
```

For more information about `enable_populate_block_cache`, see [System variables](../reference/System_variable.md).

## Storage media of blocks

By default, StarRocks uses the memory of BE machines to cache blocks. It also supports using both the memory and disks as the storage media of blocks. If BE machines are equipped with disks, such as NVMe drives or SSDs, you can use both the memory and disks to cache blocks. If you use cloud storage, such as Amazon EBS, for BE machines, we recommend that you use only the memory to cache blocks.

## Cache replacement policies

In block cache, blocks are the smallest unit that StarRocks uses to cache and discard data. StarRocks uses the [least recently used](https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)) (LRU) policy to cache and discard data.

- StarRocks reads data from memory, and then from disks if the data is not found in memory. Data read from disks is loaded into memory.
- Data deleted from memory is written into disks. Data removed from disks is discarded.

## Enable block cache

Block cache is disabled by default. To enable this feature, configure FEs and BEs in your StarRocks cluster.

### Configurations for FEs

You can enable block cache for FEs by using one of the following methods:

- Enable block cache for a given session based on your requirements.

  ```SQL
  SET enable_scan_block_cache = true;
  ```

- Enable block cache for all active sessions.

  ```SQL
  SET GLOBAL enable_scan_block_cache = true;
  ```

### Configurations for BEs

Add the following parameters to the **conf/be.conf** file of each BE. Then restart each BE to make the settings take effect.

| **Parameter**          | **Description**                                              |
| ---------------------- | ------------------------------------------------------------ |
| block_cache_enable     | Whether block cache is enabled.<ul><li>`true`: Block cache is enabled.</li><li>`false`: Block cache is disabled. The value of this parameter defaults to `false`.</li></ul>To enable block cache, set the value of this parameter to `true`. |
| block_cache_disk_path  | The paths of disks. We recommend that the number of paths you configured for this parameter is the same as the number of disks of your BE machine. Multiple paths need to be separated with semicolons (;). After you add this parameter, StarRocks automatically creates a file named **cachelib_data** to cache blocks. |
| block_cache_meta_path  | The storage path of block metadata. You can customize the storage path. We recommend that you store the metadata under the **$STARROCKS_HOME** path. |
| block_cache_mem_size   | The maximum amount of data that can be cached in the memory. Unit: bytes. The default value is `2147483648`, which is 2 GB. We recommend that you set the value of this parameter to at least 20 GB. If StarRocks reads a large amount of data from disks after block cache is enabled, consider increasing the value. |
| block_cache_disk_size  | The maximum amount of data that can be cached in a single disk. For example, if you configure two disk paths for the `block_cache_disk_path` parameter and set the value of the `block_cache_disk_size` parameter as `21474836480` (20 GB), a maximum of 40 GB data can be cached in these two disks. The default value is `0`, which indicates that only the memory is used to cache data. Unit: bytes. |

Examples of setting these parameters.

```Plain
block_cache_enable = true  

# The BE machine is equipped with two disks.
block_cache_disk_path = /home/disk1/sr/dla_cache_data/;/home/disk2/sr/dla_cache_data/ 

block_cache_meta_path = /home/disk1/sr/dla_cache_meta/ 

# 1 MB 
block_cache_block_size = 1048576

# 2 GB
block_cache_mem_size = 2147483648

# 1.2 TB
block_cache_disk_size = 1288490188800
```

## Check whether a query hits block cache

You can check whether a query hits block cache by analyzing the following metrics in the query profile:

- `BlockCacheReadBytes`: indicates the amount of data that StarRocks reads directly from the memory and disks.
- `BlockCacheWriteBytes`: indicates the amount of data loaded from an external storage system to memory and disks.
- `BytesRead`: indicates the amount of data that StarRocks read from an external storage system.

Example 1: In this example, StarRocks reads a large amount of data (7.65 GB) from the external storage system and few data (518.73 MB) from the memory and disks. This means that few block caches were hit.

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

Example 2: In this example, the amount of data StarRocks reads directly from the external storage system is 0, which means StarRocks reads data only from block cache.

```Plain
- Table: lineorder
 - BlockCacheReadBytes: 7.37 GB
   - __MAX_OF_BlockCacheReadBytes: 60.73 MB
   - __MIN_OF_BlockCacheReadBytes: 16.00 KB
 - BlockCacheReadCounter: 8.571K (8571)
   - __MAX_OF_BlockCacheReadCounter: 68
   - __MIN_OF_BlockCacheReadCounter: 1
 - BlockCacheReadTimer: 174.362ms
   - __MAX_OF_BlockCacheReadTimer: 753.745ms
   - __MIN_OF_BlockCacheReadTimer: 15.840us
 - BlockCacheWriteBytes: 0.00 
 - BlockCacheWriteCounter: 0
 - BlockCacheWriteTimer: 0ns
 - BufferUnplugCount: 103
   - __MAX_OF_BufferUnplugCount: 5
   - __MIN_OF_BufferUnplugCount: 0
 - BytesRead: 0.00 
```
