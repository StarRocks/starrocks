---
displayed_sidebar: docs
sidebar_position: 40
description: "Overview of memory classification and memory management strategies in StarRocks."
products: [oss, enterprise, anywhere]
---

# Memory Management

This section briefly introduces memory classification and StarRocks’ methods of managing memory.

## Memory Classification

Explanation:

|   Metric  | Name | Description |
| --- | --- | --- |
|  process   |  Total memory used of BE  | |
|  `query_pool`   |   Memory used by data querying  | Consists of two parts: memory used by the execution layer and memory used by the storage layer.|
|  load   |  Memory used by data loading    | Generally MemTable|
|  metadata   |   Metadata memory | S Schema, Tablet metadata, RowSet metadata, Column metadata, ColumnReader, IndexReader |
|  compaction   |   Multi-version memory compaction  |  compaction that happens after data import is complete |
|  clone  |   Clone memory  | Memory used by tablet clone tasks |
|  page_cache   |   BE's own PageCache   | Enabled by default since v2.4. You can disable it by setting `disable_storage_page_cache` to `true` in the BE configuration file |

## Memory-related configuration

* **BE Configuration**

| Name | Default| Description|
| --- | --- | --- |
| vector_chunk_size | 4096 | Number of chunk rows |
| mem_limit | 90% | BE process memory upper limit. You can set it as a percentage ("80%") or a physical limit ("100G"). The default value is 90% of the server's memory size. The BE uses 90% of `mem_limit` as the memory limit of the process memory tracker, which is 81% of the server's memory size by default. You need to configure this parameter if you want to deploy StarRocks with other memory-intensive services on a same server. |
| disable_storage_page_cache | false | The boolean value to control whether to disable PageCache. When PageCache is enabled, StarRocks caches the recently scanned data. PageCache can significantly improve the query performance when similar queries are repeated frequently. `true` indicates to disable PageCache. Use this item together with `storage_page_cache_limit`, you can accelerate query performance in scenarios with sufficient memory resources and much data scan. The default value of this item has been changed from `true` to `false` since StarRocks v2.4. |
| write_buffer_size | 104857600 |  The capacity limit of a single MemTable, exceeding which a disk swipe will be performed. |
| load_process_max_memory_limit_bytes | 107374182400 | The upper limit of memory resources that can be taken up by all load processes on a BE node. Its value is the smaller one between `BE process memory limit * load_process_max_memory_limit_percent / 100` (the BE process memory limit is 90% of `mem_limit`) and `load_process_max_memory_limit_bytes`. If this threshold is exceeded, a flush and backpressure will be triggered.  |
| load_process_max_memory_limit_percent | 30 | The maximum percentage of memory resources that can be taken up by all load processes on a BE node. Its value is the smaller one between `BE process memory limit * load_process_max_memory_limit_percent / 100` (the BE process memory limit is 90% of `mem_limit`) and `load_process_max_memory_limit_bytes`. If this threshold is exceeded, a flush and backpressure will be triggered. |
| max_compaction_concurrency | -1 | The maximum concurrency of compactions (both Base Compaction and Cumulative Compaction). The value -1 indicates that no limit is imposed on the concurrency. |
| cumulative_compaction_check_interval_seconds | 1 | Interval of compaction check|

* **Session variables**

| Name| Default| Description|
| --- | --- | --- |
| query_mem_limit| 0| Memory limit of a query on each BE node |
| load_mem_limit | 0| Memory limit of a single import task on each BE node. If the value is 0, no per-task limit is set, and the task is limited only by the memory limit of all load processes on the BE node (see `load_process_max_memory_limit_percent` and `load_process_max_memory_limit_bytes`).|

## View memory usage

* **`mem_tracker`**

~~~ bash
//View the overall memory statistics
<http://be_ip:be_http_port/mem_tracker>

// View fine-grained memory statistics
<http://be_ip:be_http_port/mem_tracker?type=query_pool&upper_level=3>
~~~

* **jemalloc**

~~~ bash
<http://be_ip:be_http_port/memz>
~~~

The `/memz` page displays the BE process memory limit, current process memory consumption, jemalloc allocator statistics, and update manager memory statistics. In the jemalloc section, fields such as `allocated`, `active`, `resident`, `mapped`, `retained`, and `metadata` help distinguish memory used by objects from memory retained by the allocator.

Allocator-retained memory may be higher than memory currently used by objects. Use `/mem_tracker` together with `/memz` to compare StarRocks memory trackers with process-level allocator statistics.

* **metrics**

~~~bash
curl -XGET http://be_ip:be_http_port/metrics | grep 'mem'
~~~

The value of metrics is updated every 10 seconds. It is possible to monitor some of the memory statistics with older versions.
