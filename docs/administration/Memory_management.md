# Memory Management

This section briefly introduces memory classification and StarRocks’ methods of managing memory.

## Memory Classification

Explanation:

|   Metric  | Name | Description |
| --- | --- | --- |
|  process   |  Total memory used of BE  | |
|  query\_pool   |   Memory used by data querying  | Consists of two parts: memory used by the execution layer and memory used by the storage layer.|
|  load   |  Memory used by data loading    | Generally MemTable|
|  table_meta   |   Metadata memory | S Schema, Tablet metadata, RowSet metadata, Column metadata, ColumnReader, IndexReader |
|  compaction   |   Multi-version memory compaction  |  compaction that happens after data import is complete |
|  snapshot  |   Snapshot memory  | Generally used for clone, little memory usage |
|  column_pool   |    Column pool memory   | Request to release column cache for accelerated column |
|  page_cache   |   BE's own PageCache   | The default is off, the user can turn it on by modifying the BE file |

## Memory-related configuration

* **BE Configuration**

| Name | Default| Description|  
| --- | --- | --- |
| vector_chunk_size | 4096 | Number of chunk rows |
| mem_limit | 80% | The percentage of total memory that BE can use. If BE is deployed as a standalone, there is no need to configure it. If it is deployed with other services that consume more memory, it should be configured separately. |
| disable_storage_page_cache | false | The boolean value to control if to disable PageCache. When PageCache is enabled, StarRocks caches the query results. PageCache can significantly improve the query performance when similar queries are repeated frequently. `true` indicates to disable PageCache. Use this item together with `storage_page_cache_limit`, you can accelerate query performance in scenarios with sufficient memory resources and much data scan. The value of this item has been changed from `true` to `false` since StarRocks v2.4. |
| write_buffer_size | 104857600 |  The capacity limit of a single MemTable, exceeding which a disk swipe will be performed. |
| load_process_max_memory_limit_bytes | 107374182400 | The total import memory limit `min(mem_limit * load_process_max_memory_limit_bytes, load_process_max_memory_limit_bytes)`. It is the actual available import memory threshold, reaching which a disk swipe will be triggered.  |
| load_process_max_memory_limit_percent | 30 | The total import memory limit `min(mem_limit * load_process_max_memory_limit_percent, load_process_max_memory_limit_bytes)`. It is the actual available import memory threshold, reaching which the swipe will be triggered. |
| default_load_mem_limit | 2147483648 | If the memory limit on the receiving side is reached for a single import instance, a disk swipe will be triggered. This needs to be modified with the Session variable `load_mem_limit` to take effect. |
| max_compaction_concurrency | -1 | The maximum concurrency of compactions (both Base Compaction and Cumulative Compaction). The value -1 indicates that no limit is imposed on the concurrency. |
| cumulative_compaction_check_interval_seconds | 1 | Interval of compaction check|

* **Session variables**

| Name| Default| Description|
| --- | --- | --- |
| query_mem_limit| 0| Memory limit of a query on each backend node |
| load_mem_limit | 0| Memory limit of a single import task. If the value is 0, `exec_mem_limit` will be taken|

## View memory usage

* **mem\_tracker**

~~~ bash
//View the overall memory statistics
<http://be_ip:be_http_port/mem_tracker>

// View fine-grained memory statistics
<http://be_ip:be_http_port/mem_tracker?type=query_pool&upper_level=3>
~~~

* **tcmalloc**

~~~ bash
<http://be_ip:be_http_port/memz>
~~~

~~~plain text
------------------------------------------------
MALLOC:      777276768 (  741.3 MiB) Bytes in use by application
MALLOC: +   8851890176 ( 8441.8 MiB) Bytes in page heap freelist
MALLOC: +    143722232 (  137.1 MiB) Bytes in central cache freelist
MALLOC: +     21869824 (   20.9 MiB) Bytes in transfer cache freelist
MALLOC: +    832509608 (  793.9 MiB) Bytes in thread cache freelists
MALLOC: +     58195968 (   55.5 MiB) Bytes in malloc metadata
MALLOC:   ------------
MALLOC: =  10685464576 (10190.5 MiB) Actual memory used (physical + swap)
MALLOC: +  25231564800 (24062.7 MiB) Bytes released to OS (aka unmapped)
MALLOC:   ------------
MALLOC: =  35917029376 (34253.1 MiB) Virtual address space used
MALLOC:
MALLOC:         112388              Spans in use
MALLOC:            335              Thread heaps in use
MALLOC:           8192              Tcmalloc page size
------------------------------------------------
Call ReleaseFreeMemory() to release freelist memory to the OS (via madvise()).
Bytes released to the OS take up virtual address space but no physical memory.
~~~

The memory queried by this method is accurate. However, some memory in StarRocks is reserved but not in use. TcMalloc counts the memory that is reserved, not the memory used.

Here `Bytes in use by application` refers to the memory currently in use.

* **metrics**

~~~bash
curl -XGET http://be_ip:be_http_port/metrics | grep 'mem'
curl -XGET http://be_ip:be_http_port/metrics | grep 'column_pool'
~~~

The value of metrics is updated every 10 seconds. It is possible to monitor some of the memory statistics with older versions.
