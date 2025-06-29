---
displayed_sidebar: docs
sidebar_position: 20
toc_max_heading_level: 2
---

# Data Cache

From v3.1.7 and v3.2.3 onwards, StarRocks introduced Data Cache to accelerate queries in shared-data clusters, replacing File Cache in earlier versions. Data Cache loads data from remote storage in blocks (on the order of MBs) as needed, while File Cache loads entire data files each time in the background, regardless of how many data rows are actually needed.

Compared to File Cache, Data Cache has the following advantages:

- Fewer reads from object storage, meaning less cost on access to object storage (if your object storage charges based on access frequency).
- Less write pressure on local disks and CPU usage, thus less impact on other loading or query tasks (because background loading threads are no longer needed).
- Optimized cache effectiveness (because File Cache may load less frequently used data in the file).
- Optimized control over the cached data, thus avoiding overwhelmed local disks caused by excessive data failed to be evicted by File Cache.

## Enable Data Cache

From v3.4.0 onwards, StarRocks uses a unified Data Cache instance for queries against external catalogs and cloud-native tables (in shared-data clusters).

## Configure Data Cache

You can configure Data Cache using the following CN(BE) configuration items:

### Cache directory

- [storage_root_path](../../administration/management/BE_configuration.md#storage_root_path) (In shared-data clusters, this item is used to specify the root path where the cached data is stored.)

### Cache disk size

- [datacache_disk_size](../../administration/management/BE_configuration.md#datacache_disk_size)
- [starlet_star_cache_disk_size_percent](../../administration/management/BE_configuration.md#starlet_star_cache_disk_size_percent)

The disk size of the cache in a shared-data cluster takes the greater value between `datacache_disk_size` and `starlet_star_cache_disk_size_percent`.

## View Data Cache status

- Execute the following statement to view the root path that stores the cached data:

  ```SQL
  SELECT * FROM information_schema.be_configs 
  WHERE NAME LIKE "%storage_root_path%";
  ```

  Usually, the cached data is stored under the sub-path `datacache/` of your `storage_root_path`.

- Execute the following statement to view the maximum percentage of storage that Data Cache can use:

  ```SQL
  SELECT * FROM information_schema.be_configs 
  WHERE NAME LIKE "%starlet_star_cache_disk_size_percent% or %datacache_disk_size%";
  ```

## Monitor Data Cache

StarRocks provides various metrics that monitor Data Cache.

### Dashboard templates

You can download the following Grafana Dashboard templates based on your StarRocks environment:

- [Dashboard template for StarRocks shared-data cluster on virtual machines](http://starrocks-thirdparty.oss-cn-zhangjiakou.aliyuncs.com/StarRocks-Shared_data-for-vm.json)
- [Dashboard template for StarRocks shared-data cluster on Kubernetes](http://starrocks-thirdparty.oss-cn-zhangjiakou.aliyuncs.com/StarRocks-Shared_data-for-k8s.json)

### Important metrics

#### fslib read io_latency

Records the read latency of Data Cache.

#### fslib write io_latency

Records the write latency of Data Cache.

#### fslib star cache meta memory size

Records the estimated memory usage of Data Cache.

#### fslib star cache data disk size

Records the actual disk usage of Data Cache.

## Disable Data Cache

To disable Data Cache, you need to add the following configuration to the CN configuration file **cn.conf**, and restart the CN nodes:

```Properties
datacache_enable = false
storage_root_path =
```

## Clear cached data

You can clear the cached data in case of emergencies. This will not affect the original data in your remote storage. 

Follow these steps to clear the cached data on a CN node:

1. Remove the sub-directory that stores the data.

   Example:

   ```Bash
   # Suppose `storage_root_path = /data/disk1;/data/disk2`
   rm -rf /data/disk1/datacache/
   rm -rf /data/disk2/datacache/
   ```

2. Restart the CN node.

## Usage notes

- If the `datacache.enable` property is set to `false` for a cloud-native table, Data Cache will not be enabled for the table.
- If the `datacache.partition_duration` property is set to a specific time range, data beyond the time range will not be cached.
- After downgrading a shared-data cluster from v3.3 to v3.2.8 and earlier, if you want to re-use the cached data in Data Cache, you need to manually rename the Blockfile in the directory `starlet_cache` by changing the file name format from `blockfile_{n}.{version}` to `blockfile_{n}`, that is, to remove the suffix of version information. v3.2.9 and later versions are compatible with the file name format in v3.3, so you do not need to perform this operation manually. You can change the name by running the following shell script:

```Bash
#!/bin/bash

# Replace <starlet_cache_path> with the directory of Data Cache of your cluster, for example, /usr/be/storage/starlet_cache.
starlet_cache_path="<starlet_cache_path>"

for blockfile in ${starlet_cache_path}/blockfile_*; do
    if [ -f "$blockfile" ]; then
        new_blockfile=$(echo "$blockfile" | cut -d'.' -f1)
        mv "$blockfile" "$new_blockfile"
    fi
done
```

## Known issues

### High memory usage

- Description: While the cluster is running under low-load conditions, the total memory usage of the CN node is far beyond the sum of each module's memory usage.
- Identification: If the sum of `fslib star cache meta memory size` and `fslib star cache data memory size` takes a significant proportion of the total memory usage of the CN node, it might indicate this issue.
- Affected versions: v3.1.8 and earlier patch versions, and v3.2.3 and earlier patch versions
- Fixed versions: v3.1.9, v3.2.4
- Solutions: 
  - Upgrade the cluster to the fixed versions.
  - If you do not want to upgrade the cluster, you can clear the directory `${storage_root_path}/starlet_cache/star_cache/meta` of the CN nodes, and restart the nodes.

## Q&A

### Q1: Why does the Data Cache directory occupy much larger storage space (observed through `du` and `ls` commands) than the actual size of cached data?

The disk space occupied by Data Cache represents the historical peak usage and is irrelevant to the current actual cached data size. For example, if 100 GB of data is cached, the data size will become 200 GB after compaction. Then, after garbage collection (GC), the data size was reduced to 100 GB. However, the disk space occupied by Data Cache will remain at its peak of 200 GB, even though the actual cached data within is 100 GB.

### Q2: Does Data Cache automatically evict data?

No. Data Cache evicts data only when it reaches the disk usage limit (`80%` of the disk space by default). The eviction process does not delete the data; it merely marks the disk space that stores the old cache as empty. The new cache will then overwrite the old cache. Therefore, even if eviction occurs, the disk usage will not decrease and will not affect actual usage.

### Q3: Why does the disk usage remain at the configured maximum level and not decrease?

Refer to Q2. Data Cache eviction mechanism does not delete the cached data but marks the old data as overwritable. Hence, the disk usage will not decrease.

### Q4: Why does the disk usage of Data Cache remain at the same level after I dropped a table and the table files are deleted from remote storage?

Dropping a table does not trigger the deletion of data in the Data Cache. The cache of the deleted table will gradually be evicted over time based on Data Cache's LRU (Least Recently Used) logic, and this does not affect actual usage.

### Q5: Why does the actual disk usage reach 90% or more even though 80% disk capacity is configured?

The disk usage by Data Cache is accurate and will not exceed the configured limit. The excessive disk usage may be caused by:

- Log files generated during runtime.
- Core files generated by CN crashes.
- Persistent indexes of Primary Key tables (stored under `${storage_root_path}/persist/`).
- Mixed deployments of BE/CN/FE instances sharing the same disk.
- Disk issues, for example, the ext3 file system is used.

You can execute `du -h . -d 1` in the disk root directory or sub-directories to check the specific space-occupying directories, and then delete the unexpected portions. You can also reduce the disk capacity limit of Data Cache by configuring `starlet_star_cache_disk_size_percent`.

### Q6: Why is there a significant difference in disk usage of Data Cache between nodes?

It is impossible to ensure consistent cache usage across nodes due to the inherent limitations of single-node caching. As long as it does not affect query latency, differences in cache usage are acceptable. This discrepancy may be caused by:

- Different time points at which the nodes were added to the cluster.
- Differences in the number of tablets owned by different nodes.
- Differences in the size of data owned by different tablets.
- Differences in compaction and GC situations across nodes.
- Nodes experiencing crashes or Out-Of-Memory (OOM) issues.

In summary, the differences in cache usage are influenced by multiple factors.
