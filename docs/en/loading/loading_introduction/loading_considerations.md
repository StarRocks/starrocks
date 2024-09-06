---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# Considerations

This topic describes some system limits and configurations that you need to consider before you run data loads.

## Memory limits

StarRocks provides parameters for you to limit the memory usage for each load job, thereby reducing memory consumption, especially in high concurrency scenarios. However, do not specify an excessively low memory usage limit. If the memory usage limit is excessively low, data may be frequently flushed from memory to disk because the memory usage for load jobs reaches the specified limit. We recommend that you specify a proper memory usage limit based on your business scenario.

The parameters that are used to limit memory usage vary for each loading method. For more information, see [Stream Load](../../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md), [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md), [Routine Load](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md), [Spark Load](../../sql-reference/sql-statements/loading_unloading/SPARK_LOAD.md), and [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md). Note that a load job usually runs on multiple BEs or CNs. Therefore, the parameters limit the memory usage of each load job on each involved BE or CN rather than the total memory usage of the load job on all involved BEs or CNs.

StarRocks also provides parameters for you to limit the total memory usage of all load jobs that run on each individual BE or CN. For more information, see the "[System configurations](#system-configurations)" section below.

## System configurations

This section describes some parameter configurations that are applicable to all of the loading methods provided by StarRocks.

### FE configurations

You can configure the following parameters in the configuration file **fe.conf** of each FE:

- `max_load_timeout_second` and `min_load_timeout_second`
  
  These parameters specify the maximum timeout period and minimum timeout period of each load job. The timeout periods are measured in seconds. The default maximum timeout period spans 3 days, and the default minimum timeout period spans 1 second. The maximum timeout period and minimum timeout period that you specify must fall within the range of 1 second to 3 days. These parameters are valid for both synchronous load jobs and asynchronous load jobs.

- `desired_max_waiting_jobs`
  
  This parameter specifies the maximum number of load jobs that can be held waiting in queue. The default value is **1024** (100 in v2.4 and earlier, and 1024 in v2.5 and later). When the number of load jobs in the **PENDING** state on an FE reaches the maximum number that you specify, the FE rejects new load requests. This parameter is valid only for asynchronous load jobs.

- `max_running_txn_num_per_db`
  
  This parameter specifies the maximum number of ongoing load transactions that are allowed in each database of your StarRocks cluster. A load job can contain one or more transactions. The default value is **100**. When the number of load transactions running in a database reaches the maximum number that you specify, the subsequent load jobs that you submit are not scheduled. In this situation, if you submit a synchronous load job, the job is rejected. If you submit an asynchronous load job, the job is held waiting in queue.

  :::note
  
  StarRocks counts all load jobs together and does not distinguish between synchronous load jobs and asynchronous load jobs.

  :::

- `label_keep_max_second`
  
  This parameter specifies the retention period of the history records for load jobs that have finished and are in the **FINISHED** or **CANCELLED** state. The default retention period spans 3 days. This parameter is valid for both synchronous load jobs and asynchronous load jobs.

### BE/CN configurations

You can configure the following parameters in the configuration file **be.conf** of each BE or the configuration file **cn.conf** of each CN:

- `write_buffer_size`
  
  This parameter specifies the maximum memory block size. The default size is 100 MB. The loaded data is first written to a memory block on the BE or CN. When the amount of data that is loaded reaches the maximum memory block size that you specify, the data is flushed to disk. You must specify a proper maximum memory block size based on your business scenario.

  - If the maximum memory block size is exceedingly small, a large number of small files may be generated on the BE or CN. In this case, query performance degrades. You can increase the maximum memory block size to reduce the number of files generated.
  - If the maximum memory block size is exceedingly large, remote procedure calls (RPCs) may time out. In this case, you can adjust the value of this parameter based on your business needs.

- `streaming_load_rpc_max_alive_time_sec`
  
  The waiting timeout period for each Writer process. The default value is 1200 seconds. During the data loading process, StarRocks starts a Writer process to receive data from and write data to each tablet. If a Writer process does not receive any data within the waiting timeout period that you specify, StarRocks stops the Writer process. When your StarRocks cluster processes data at low speeds, a Writer process may not receive the next batch of data within a long period of time and therefore reports a "TabletWriter add batch with unknown id" error. In this case, you can increase the value of this parameter.

- `load_process_max_memory_limit_bytes` and `load_process_max_memory_limit_percent`
  
  These parameters specify the maximum amount of memory that can be consumed for all load jobs on each individual BE or CN. StarRocks identifies the smaller memory consumption among the values of the two parameters as the final memory consumption that is allowed.

  - `load_process_max_memory_limit_bytes`: specifies the maximum memory size. The default maximum memory size is 100 GB.
  - `load_process_max_memory_limit_percent`: specifies the maximum memory usage. The default value is 30%. This parameter differs from the `mem_limit` parameter. The `mem_limit` parameter specifies the total maximum memory usage of your StarRocks cluster, and the default value is 90% x 90%.

    If the memory capacity of the machine on which the BE or CN resides is M, the maximum amount of memory that can be consumed for load jobs is calculated as follows: `M x 90% x 90% x 30%`.

### System variable configurations

You can configure the following [system variable](../../sql-reference/System_variable.md):

- `query_timeout`

  The query timeout duration. Unit: seconds. Value range: `1` to `259200`. Default value: `300`. This variable will act on all query statements in the current connection, as well as INSERT statements.
