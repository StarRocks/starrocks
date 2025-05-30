---
displayed_sidebar: docs
---

# Data Cache常见问题

本文介绍 Data Cache 使用过程中遇到的一些常见问题和排查步骤，帮助用户快速定位相关问题。

## 缓存开启

###如何确认当前缓存是否成功开启？

通常可以通过以下几种方式（任选其一）来判断当前系统是否成功启用Data Cache：

* 在SQL客户端执行`show backends`（或者`show compute nodes`)，查看`DataCacheMetrics`指标，确认磁盘或者内存缓存的quota是否大于0。

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

在上面例子中，Data Cache中磁盘缓存quota为1TB，当前使用44MB；内存缓存quota为0B，未启用内存缓存。

* 可以通过访问`http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/stat`接口来显示当前Data Cache的quota、命中率等状态信息，检查`disk_quota_bytes`和`mem_quota_bytes`是否大于0。如图：

![](https://docs.starrocks.io/zh/assets/images/data_cache_observe-dba68d3bcfbe64559d30787e5d09ff43.png)

###为什么Data Cache没有默认开启？

从3.3版本开始，BE会尝试默认Data Cache。然而，如果当前磁盘剩余空间不足时，则不会默认开启Data Cache。

这里的剩余磁盘空间不足会包括以下情况：

* 百分比：磁盘当前使用率较高。

* 绝对值：磁盘剩余空间较小。

因此，在遇到Data Cache没有默认启用的情况下，可以先检查当前磁盘的使用情况，必要时增加磁盘容量。

另外，也可以根据当前磁盘剩余空间，通过配置缓存quota来手动启用Data Cache：

```
# disable datacache automatic adjustment
datacache_auto_adjust_enable = false
# set datacache disk quota manually
datacache_disk_size = 1T
```

##缓存生效

###Data Cache当前支持哪些Catalog类型？

Data Cache当前支持使用了StarRocks Native File Reader（如Parquet/ORC/CSV Reader）的External Catalog类型，包Hive、Iceberg、Hudi、Delta Lake、Paimon等Catalog类型，暂不支持基于JNI访问数据的Catalog（如JDBC Catalog）。

**注意**：有些Catalog可能会根据一定条件（如文件类型、数据状态等）来选择不同的数据访问方式，比如对于Paimon Catalog，可能会根据当前数据的compaction状态由StarRocks自动选择是使用Native File Reader还是JNI来访问数据，当使用JNI方式访问Paimon数据时，同样不支持Data Cache加速。

###如何确认当前查询是否命中缓存？

可以通过该查询的profile来查看DataCache相关的指标，检查`DataCacheReadBytes`和`DataCacheReadCounter`确定本地缓存读取情况，比如：

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

###缓存已经启用，为何当前查询没有命中缓存？

1. 检查Data Cache是否支持当前catalog类型。
2. 确认当前查询语句是否满足缓存填充条件。默认情况下，Data Cache会通过一定规则拒绝掉部分查询的缓存填充，避免缓存污染。详情见[Data Cache填充规则](https://docs.starrocks.io/zh/docs/data_source/data_cache/#%E5%A1%AB%E5%85%85%E8%A7%84%E5%88%99 "Data Cache填充规则")。

用户可以通过`EXPLAIN VERBOSE` 命令来确认当前语句是否触发缓存填充。
示例：

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

上面例子中`dataCacheOptions`的`populate`字段为`false`，因此，当前查询并不会进行缓存填充。

如果确实需要对该查询进行缓存，可以通过设置`populate_datacache_mode='always'`来修改默认填充行为。

##缓存命中

###为什么同一条查询需要执行许多次才能完全命中缓存？

当前版本中，Data Cache默认采用异步填充方式来减少缓存填充对查询性能影响。使用异步填充方式时，系统会尝试在尽可能不影响读取性能的前提下在后台对访问到的数据进行缓存，因此，通常单次查询只能填充一部分缓存数据到本地，需要多次执行才能缓存该查询所需全部数据。

用户也可通过设置`enable_datacache_async_populate_mode=false`来使用同步缓存方式，或者通过`CACHE SELECT`对目标数据进行提前预热。

###为什么当前查询所有数据都已经被缓存，但查询还有少量数据访问了远端？

当前版本中默认启用IO自适应来优化当磁盘IO负载较高时的缓存性能，可能会导致某些情况下少量请求直接访问远端。

用户也可以通过设置`enable_datacache_io_adaptor=false`来关闭IO自适应功能。

##其他

###如何清空当前缓存数据？

当前Data Cache并没有提供直接的clear接口来清空缓存数据，不过用户可以通过以下方式来清理缓存数据：

* 可以通过删除BE/CN节点上datacache目录下的所有数据（包括block文件和meta目录），然后重启BE/CN节点，从而清理缓存数据。（推荐）

* 如果不方便重启BE/CN节点，也可以通过在线扩缩容的方式来间接实现缓存清理，假设用户早期磁盘缓存quota配置了2T，可以先缩容为0（系统自动清理缓存数据），再恢复到2T即可。如：

```SQL
UPDATE be_configs SET VALUE="0" WHERE NAME="datacache_disk_size" and BE_ID=10005;
UPDATE be_configs SET VALUE="2T" WHERE NAME="datacache_disk_size" and BE_ID=10005;
```

**注意**：使用扩缩容方式在线清理缓存时，一定要注意语句中的WHERE条件，避免误伤到其他的无关参数或节点。

###如何提升缓存性能？

缓存本质上是通过访问本地内存或磁盘来代替远端存储系统访问，因此，缓存性能和本地缓存介质直接相关。当发现由于磁盘负载较高导致缓存访问延时较大时，需要考虑提升本地缓存介质性能：

* 尽量使用高性能NVME盘来作为缓存盘。

* 如果条件有限，没法选择高性能磁盘，也可以通过增加多块磁盘来分摊单盘IO压力。

* 增加节点内存（这里指增加机器内存，而非调大Data Cache内存quota），借助于操作系统page cache来减少直接访问磁盘的次数，降低磁盘压力。
