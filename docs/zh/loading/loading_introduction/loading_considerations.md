---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# 相关配置

本文介绍您在执行数据导入之前，需要关注的一些系统限制和参数配置。

## 内存限制

您可以通过设置参数来限制单个导入作业的内存使用，以防止导入作业占用过多内存，特别是在导入并发较高的情况下。同时，您也需要注意避免设置过小的内存使用上限，因为内存使用上限过小，导入过程中可能会因为内存使用量达到上限而频繁地将内存中的数据刷出到磁盘，进而可能影响导入效率。建议您根据具体的业务场景要求，合理地设置内存使用上限。

不同的导入方式限制内存的方式略有不同，具体请参见 [Stream Load](../../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md)、[Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md)、[Routine Load](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md)、[Spark Load](../../sql-reference/sql-statements/loading_unloading/SPARK_LOAD.md) 和 [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md)。需要注意的是，一个导入作业通常都会分布在多个 BE（或 CN）上执行，这些内存参数限制的是一个导入作业在单个 BE（或 CN）上的内存使用，而不是在整个集群上的内存使用总和。

您还可以通过设置一些参数来限制在单个 BE（或 CN）上运行的所有导入作业的总的内存使用上限。可参考下面的“[系统配置](#系统配置)”章节。

## 系统配置

本节解释对所有导入方式均适用的参数配置。

### FE 配置

您可以通过修改每个 FE 的配置文件 **fe.conf** 来设置如下参数：

- `max_load_timeout_second` 和 `min_load_timeout_second`

  设置导入超时时间的最大、最小值，单位均为秒。默认的最大超时时间为 3 天，默认的最小超时时间为 1 秒。自定义的导入超时时间不能超过这个最大、最小值范围。该参数配置适用于所有模式的导入作业。

- `desired_max_waiting_jobs`

  等待队列可以容纳的导入作业的最大个数，默认值为 1024 (2.4 及之前版本默认值为 100；2.5 及以后版本默认值变为 1024)。如果 FE 中处于 **PENDING** 状态的导入作业数目达到最大个数限制时，FE 会拒绝新的导入请求。该参数配置仅对异步执行的导入有效。

- `max_running_txn_num_per_db`

  StarRocks 集群每个数据库中正在进行的导入事务的最大个数（一个导入作业可能包含多个事务），默认值为 1000 （自 3.1 版本起，默认值由 100 变为 1000。）。当数据库中正在运行的导入事务达到最大个数限制时，后续提交的导入作业不会执行。如果是同步的导入作业，作业会被拒绝；如果是异步的导入作业，作业会在队列中等待。

  :::note
  
  所有模式的作业均包含在内、统一计数。
  
  :::

- `label_keep_max_second`

  已经完成、且处于 **FINISHED** 或 **CANCELLED** 状态的导入作业记录在 StarRocks 系统的保留时长，默认值为 3 天。该参数配置适用于所有模式的导入作业。

### BE（或 CN）配置

您可以通过修改每个 BE 的配置文件 **be.conf**（或 CN 的配置文件 **cn.conf**）来设置如下参数：

- `write_buffer_size`

  BE（或 CN）上内存块的大小阈值，默认阈值为 100 MB。导入的数据在 BE（或 CN）上会先写入一个内存块，当内存块的大小达到这个阈值以后才会写回磁盘。如果阈值过小，可能会导致 BE（或 CN）上存在大量的小文件，影响查询的性能，这时候可以适当提高这个阈值来减少文件数量。如果阈值过大，可能会导致远程过程调用（Remote Procedure Call，简称 RPC）超时，这时候可以适当地调整该参数的取值。

- `streaming_load_rpc_max_alive_time_sec`

  指定了 Writer 进程的等待超时时间，默认为 1200 秒。在导入过程中，StarRocks 会为每个 Tablet 开启一个 Writer 进程，用于接收和写入数据。如果在参数指定时间内 Writer 进程没有收到任何数据，StarRocks 系统会自动销毁这个 Writer 进程。当系统处理速度较慢时，Writer 进程可能长时间接收不到下一批次数据，导致上报 "TabletWriter add batch with unknown id" 错误。这时候可适当调大这个参数的取值。

- `load_process_max_memory_limit_bytes` 和 `load_process_max_memory_limit_percent`

  用于导入的最大内存使用量和最大内存使用百分比，用来限制单个 BE（或 CN）上所有导入作业的内存总和的使用上限。StarRocks 系统会在两个参数中取较小者，作为最终的使用上限。

  - `load_process_max_memory_limit_bytes`：指定 BE（或 CN）上最大内存使用量，默认为 100 GB。
  - `load_process_max_memory_limit_percent`：指定 BE（或 CN）上最大内存使用百分比，默认为 30%。该参数与 `mem_limit` 参数不同。`mem_limit` 参数指定的是 BE（或 CN）进程内存上限，默认硬上限为 BE（或 CN）所在机器内存的 90%，软上限为 BE（或 CN）所在机器内存的 90% x 90%。

    假设 BE（或 CN）所在机器物理内存大小为 M，则用于导入的内存上限为：`M x 90% x 90% x 30%`。

### 会话变量

您可以设置如下[会话变量](../../sql-reference/System_variable.md)：

- `query_timeout`

  用于设置查询超时时间。单位：秒。取值范围：`1` ~ `259200`。默认值：`300`，相当于 5 分钟。该变量会作用于当前连接中所有的查询语句，以及 INSERT 语句。
