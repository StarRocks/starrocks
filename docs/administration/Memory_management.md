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
| max_compaction_concurrency | 10 | Disable the compaction|
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

* **jemalloc**

~~~ bash
http://be_ip:be_http_port/memz
~~~

~~~plain text
Mem Limit: 50.82 GB
Mem Consumption: 21.67 GB
___ Begin jemalloc statistics ___
Version: "5.2.1-0-gea6b3e973b477b8061e0076bb257dbd7f3faa756"
Build-time option settings
  config.cache_oblivious: true
  config.debug: false
  config.fill: true
  config.lazy_lock: false
  config.malloc_conf: ""
  config.opt_safety_checks: false
  config.prof: true
  config.prof_libgcc: true
  config.prof_libunwind: false
  config.stats: true
  config.utrace: false
  config.xmalloc: false
Run-time option settings
  opt.abort: false
  opt.abort_conf: false
  opt.confirm_conf: false
  opt.retain: true
  opt.dss: "secondary"
  opt.narenas: 32
  opt.percpu_arena: "percpu"
  opt.oversize_threshold: 0
  opt.metadata_thp: "auto"
  opt.background_thread: true (background_thread: true)
  opt.dirty_decay_ms: 5000 (arenas.dirty_decay_ms: 5000)
  opt.muzzy_decay_ms: 5000 (arenas.muzzy_decay_ms: 5000)
  opt.lg_extent_max_active_fit: 6
  opt.junk: "false"
  opt.zero: false
  opt.tcache: true
  opt.lg_tcache_max: 15
  opt.thp: "default"
  opt.prof: false
  opt.prof_prefix: "jeprof"
  opt.prof_active: true (prof.active: false)
  opt.prof_thread_active_init: true (prof.thread_active_init: false)
  opt.lg_prof_sample: 19 (prof.lg_sample: 0)
  opt.prof_accum: false
  opt.lg_prof_interval: -1
  opt.prof_gdump: false
  opt.prof_final: false
  opt.prof_leak: false
  opt.stats_print: false
  opt.stats_print_opts: ""
Profiling settings
  prof.thread_active_init: false
  prof.active: false
  prof.gdump: false
  prof.interval: 0
  prof.lg_sample: 0
Arenas: 32
Quantum size: 16
Page size: 4096
Maximum thread-cached size class: 32768
Number of bin size classes: 36
Number of thread-cache bin size classes: 41
Number of large size classes: 196
Allocated: 23549031352, active: 25295286272, metadata: 1065318656 (n_thp 0), resident: 26613735424, mapped: 26898759680, retained: 160729759744
Background threads: 4, num_runs: 2953297, run_interval: 553974185 ns
                           n_lock_ops (#/sec)       n_waiting (#/sec)      n_spin_acq (#/sec)  n_owner_switch (#/sec)   total_wait_ns   (#/sec)     max_wait_ns  max_n_thds
background_thread               54913       0               0       0               0       0              41       0               0         0               0           0
ctl                            219482       0               0       0               0       0               2       0               0         0               0           0
prof                                0       0               0       0               0       0               0       0               0         0               0           0
Merged arenas stats:
assigned threads: 1084
uptime: 411881355428623
dss allocation precedence: "N/A"
decaying:  time       npages       sweeps     madvises       purged
   dirty:   N/A        43823     16323126    344345699   7385082027
   muzzy:   N/A        25602     16048407    309650104   6030684663
                            allocated         nmalloc (#/sec)         ndalloc (#/sec)       nrequests   (#/sec)           nfill   (#/sec)          nflush   (#/sec)
small:                    16484017080    146894107154  356642    146752101115  356297    291279410042    707193      5718896563     13884      2713220447      6587
large:                     7065014272      2863530205    6952      2863421415    6952      4582347015     11125      2863530205      6952               0         0
total:                    23549031352    149757637359  363594    149615522530  363249    295861757057    718318      8582426768     20837      2713220447      6587
...
~~~

* **metrics**

~~~bash
curl -XGET http://be_ip:be_http_port/metrics | grep 'mem'
curl -XGET http://be_ip:be_http_port/metrics | grep 'column_pool'
~~~

The value of metrics is updated every 10 seconds. It is possible to monitor some of the memory statistics with older versions.
