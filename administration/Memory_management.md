# 内存管理

本章节简单的介绍StarRocks中的内存使用分类和基本的查看内存管理的方法

## 内存分类

解释：

|   标识  |  名称   | 说明 |
| --- | --- | --- |
|  process   |  BE 总使用内存   | |
|  query\_pool   |   查询内存   |分为两部分: (1)执行层使用内存 (2)存储层使用内存。|
|  load   |   导入内存    | 主要是 MemTable|
|  table_meta   |   元数据内存  | Schema, Tablet元数据, RowSet元数据, Column元数据, ColumnReader, IndexReader|
|  compaction   |   多版本合并内存   |  用于数据导入完成后的 Compaction|
|  snaphost   |   快照内存  |一般用于 clone, 内存使用很少 |
|  column_pool   |    Column pool内存池   |用于加速Column申请释放的列缓存 |
|  page_cache   |   BE自有PageCache   | 默认是关闭的，用户可以通过修改BE文件主动打开 |

## 内存相关的配置

* **BE 配置**

| 名称 |  默认值|   说明|  
 | --- | --- | --- |
| vector_chunk_size | 4096 | Chunk 行数 |
| tc_use_memory_min | 10737418240 | TCmalloc 最小保留内存，只有超过这个值，StarRocks才将空闲内存返还给操作系统|
| mem_limit | 80% | BE可以使用的机器总内存的比例，如果是BE单独部署的话，不需要配置，如果是和其它占用内存比较多的服务混合部署的话，要单独配置下 |
| disable_storage_page_cache | true |  是否打开StarRocks自有PageCachestorage_page_cache_limit0PageCache容量限制 |
| write_buffer_size | 104857600 |  单个MemTable内存中的容量限制超过这个限制要执行刷盘 |
| load_process_max_memory_limit_bytes | 107374182400 | 导入总内存限制min(mem_limit * load_process_max_memory_limit_percent, load_process_max_memory_limit_bytes)是实际可使用的导入内存限制到达这个限制，会触发刷盘逻辑 |
| load_process_max_memory_limit_percent | 60 | 导入总内存限制min(mem_limit * load_process_max_memory_limit_percent, load_process_max_memory_limit_bytes)是实际可使用的导入内存限制到达这个限制，会触发刷盘逻辑 |
| default_load_mem_limit | 2147483648 | 单个导入实例，接收端的内存限制到达这个限制，会触发刷盘逻辑当前，需要配合Session变量 load_mem_limit 的修改才能生效|
| max_compaction_concurrency | 10 | 表示禁用 Compaction|
| cumulative_compaction_check_interval_seconds | 1 | Compaction Check 间隔时间|

* **Session 变量**

| 名称| 默认值| 说明|
|  --- |  --- | --- |
| exec_mem_limit| 2147483648| 单个 instance 内存限制|
| load_mem_limit| 0| 单个导入任务的内存限制，如果是0,会取exec_mem_limit|

## 查看内存使用

* **mem\_tracker**

~~~ bash
// 看整体内存统计
<http://be_ip:be_web_port/mem_tracker>

// 看更细粒度的内存统计
<http://be_ip:be_web_port/mem_tracker?type=query_pool&upper_level=3>
~~~

* **tcmalloc**

~~~ bash
<http://be_ip:be_web_port/memz>
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

通过这种方法，看到的内存是准确的，但是当前StarRocks，有些内存使用是**Reserve**的，但是并没有使用，TcMalloc统计的**实际上是Reserve的内存**，而不是实际使用的内存。

这里Bytes in use by application 指的是当前使用的内存.

* **metrics**

~~~bash
curl -XGET http://be_ip:be_web_port/metrics | grep 'mem'
curl -XGET http://be_ip:be_web_port/metrics | grep 'column_pool'
~~~

metrics的值是10秒更新一次，老的版本，可以通过这种方法，监控部分内存统计。
