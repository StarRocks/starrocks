# Export Overview

This topic describes how to unload and store the data from your StarRocks cluster to other storage media by using the unloading feature of StarRocks.

The unloading feature of StarRocks allows you to unload the data from specified tables or partitions by using brokers as CSV files to external cloud storage systems such as HDFS, Alibaba Cloud OSS, AWS S3, and other S3 protocol-compatible object storage services.

## Explanation of Terms

* **Broker**: StarRocks can perform file operations on remote storage via Broker.
* **Tablet**: Data partitioning. A table will be divided into one or more partitions, and each partition will be divided into multiple data shards.

## Principle

### Export Job Execution Process

After a user submits an **export job**, StarRocks counts all the Tablets involved in the job and then **groups** the Tablets. Each group generates a special query plan that reads and writes the data to the path specified by the remote storage via Broker.

The overall processing flow of the export job is as follows:

![asset](../assets/5.3.1-1.png)

The processing flow described in the above diagram consists of the following steps:

1. The user submits an export job to the FE.
2. The FE's export scheduler executes the export job in two stages.

    a. PENDING: The FE generates **one** ExportPendingTask, sends a snapshot command to the BE, takes a snapshot of all the involved tablets, and generates **multiple** query plans.

b. EXPORTING: The FE generates **one** ExportExportingTask and starts executing **one** query plan.

### Query Plan Splitting

The Export job generates multiple query plans, each of which is responsible for scanning a portion of the Tablet. The amount of data scanned by each BE is calculated by the `export_max_bytes_per_be_per_task` parameter(256M by default). Each BE has at least one tablet to scan, and the maximum amount of exported data does not exceed the value of `export_max_bytes_per_be_per_task`.

Multiple query plans for a job **execute in parallel**. The size of the job thread pool is configured via the `export_task_pool_size` parameter (defaults to 5).

### Query Plan Execution

When executed, a query plan scans multiple tablets, reads data into batches each of which contains 1024 rows, and then calls the broker to write it to the remote storage.

A query plan will automatically retry three times when encountering an error. If it still fails after three retries, the entire job fails.

StarRocks will first create a temporary directory named `__starrocks_export_tmp_921d8f80-7c9d-11eb-9342-acde48001122` in the specified remote storage path (where `921d8f80-7c9d-11eb-9342- acde48001122` is the query id of the job). The exported data is first written to this temporary directory. Each query plan will generate a file (see example below):

`lineorder_921d8f80-7c9d-11eb-9342-acde48001122_1_2_0.csv.1615471540361`

where :

* `lineorder_`: The prefix of the export file, specified by the user, with a default value of `data_`.
* `921d8f80-7c9d-11eb-9342-acde48001122`: The query id of the job, which is included by default, and can be excluded by setting `include_query_id = false`.
* `1_2_0`: The first part is the serial number of the **query plan** corresponding to the job, the second part is the serial number of the **instance** in the job, and the third part is the serial number of the **generated file** in the instance.
* `csv`: The export file format. Currently, only the csv format is supported.

#### Broker Parameters

The export job needs to access the remote storage via Broker. Each Broker needs different parameters, see the Broker documentation.

### Usage Examples

#### Submit an Export Job

Example:

~~~sql
EXPORT TABLE db1.tbl1 
PARTITION (p1,p2)
TO "hdfs://host/path/to/export/lineorder_" 
PROPERTIES
(
    "column_separator"=",",
    "exec_mem_limit"="2147483648",
    "timeout" = "3600"
)
WITH BROKER "hdfs"
(
 "username" = "user",
 "password" = "passwd"
);
~~~

If the export path is specified to a directory, you need to specify the last `/`, otherwise the last part will be used as the prefix of the export file. The default is `data_` if no prefix is specified.
The export file in the example will be generated to the export directory with the prefix `lineorder_`.

PROPERTIES are as follows:

* `column_separator`: Column separator, defaults to `\t`.
* `line_delimiter`: Row separator, defaults to `\n`.
* `exec_mem_limit`: Indicate the memory usage limit for **a query plan** on **a single BE** in an Export job. Default to 2GB, in bytes.
* `timeout`: Job timeout time. Default to 2 hours, in seconds.
* `include_query_id`: Whether to include the query id in the export file name. Default to true.

### Get Export Job Query ID

After submitting a job, you can get the query id of the exported job by `SELECT LAST_QUERY_ID()`, and you can view or cancel the job using the query id.

### View Export Job Status

After submitting a job, you can view the job status by `SHOW EXPORT`.

~~~sql
SHOW EXPORT WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122";
~~~

The export results are as follows:

~~~sql
     JobId: 14008
     State: FINISHED
  Progress: 100%
  TaskInfo: {"partitions":["*"],"exec mem limit":2147483648,"column separator":",","line delimiter":"\n","tablet num":1,"broker":"hdfs","coord num":1,"db":"default_cluster:db1","tbl":"tbl3"}
      Path: oss://bj-test/export/
CreateTime: 2019-06-25 17:08:24
 StartTime: 2019-06-25 17:08:28
FinishTime: 2019-06-25 17:08:34
   Timeout: 3600
  ErrorMsg: N/A
~~~

* JobId: the unique ID of the job
* State: the status of the job.
  * PENDING: Job is pending
  * EXPORTING: data export in progress
  * FINISHED: Job succeeded
  * CANCELLED: Job failed

* PROGRESS: The progress of the job. The progress is measured in query plans. Suppose there are 10 query plans, and 3 have been completed, then the progress is 30%.
* TaskInfo: Job information in JSON format.
  * db: Database name
  * tbl: Table name
  * partitions: The partitions to export. `*` means all partitions.
  * exec mem limit: The memory usage limit of the query, in bytes.
  * column separator: The column separator of the export file.
  * line delimiter: The line separator of the exported file.
  * tablet num: The total number of tablets involved.
  * broker: The name of the broker used.
  * coord num: The number of query plans.

* Path: Path of the export on the remote storage.
* CreateTime/StartTime/FinishTime: The creation time, start time, and end time of the job.
* Timeout: Timeout time of the job, in seconds. This time is calculated from CreateTime.
* ErrorMsg: If the job has an error, the reason for the error is displayed here.

#### Cancellation of Jobs

Examples:

~~~sql
CANCEL EXPORT WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122";
~~~

### Best Practices

#### Query Plan Splitting

The number of query plans that need to be executed for an Export job depends on the total number of tablets and the maximum amount of data a query plan can handle.
Jobs are retried according to query plans. If a query plan handles a larger amount of data, retry costs due to query plan errors (e.g., failed RPC calls to Broker, jitter in remote storage, etc.) are higher. Each query plan has at least one tablet per BE, and the amount of exported data cannot exceed the `export_max_bytes_per_be_per_task` parameter being configured.

Multiple query plans for a job are executed in parallel, and the size of the job thread pool is configured via the `export_task_pool_size` parameter, which defaults to 5.

#### exec_mem_limit

Usually the query plan of an export job has only two parts – `scan` and `export`, which does not involve much memory-intensive computation logic. Therefore  the default memory limit of 2GB can mostly meet the demand. However, in some scenarios, such as when a query plan requires too many scanned tablets on the same BE or too many data versions of the tablets, it may lead to insufficient memory. In that case, you need to modify this parameter to set a larger memory, such as 4GB, 8GB, etc.

### Notes

* It is not recommended to export a large amount of data at once. The recommended export size for an export job is tens of GB, and oversized exports can result in an increase in junk files and retry costs.
* If the table data volume is too large, it is recommended to export by partition.
* If the FE reboots or changes the leader while the export job is running, the export job will fail and require the user to resubmit.
* The `__starrocks_export_tmp_xxx` temporary directory generated by the export job will be deleted automatically after the job fails or succeeds.
* After the export run completes (successfully or unsuccessfully) and the FE reboots or changes the leader, some job information displayed by `SHOW EXPORT` will be lost.
* The export job will only export data from the base table, not from the Rollup Index.
* The export job scans the data and consumes IO resources, which may affect the query latency of the system.

### Related configurations

Mainly describes the related configurations in FE.

* `export_checker_interval_second`: The scheduling interval of the export job scheduler, default to 5 seconds. To set this parameter, you need to restart the FE.
* `export_running_job_num_limit`: The limit number of running export jobs. If exceeded, the job will wait and be in the PENDING state. Defaults to 5. Can be adjusted at runtime.
* `export_task_default_timeout_second`: Default timeout for export jobs. Defaults to 2 hours. Can be adjusted at runtime.
* `export_max_bytes_per_be_per_task`: Maximum amount of data exported per export job on each BE. This parameter is used to split export jobs for parallel processing, and is calculated based on compressed data volume with a default value of 256M.
* `export_task_pool_size`: The size of the export job thread pool. Defaults to 5.
