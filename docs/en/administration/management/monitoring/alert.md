# StarRocks cluster alarm rules and handling methods

This article introduces the alarm items that need attention and their handling methods from multiple dimensions such as business continuity, cluster availability, and machines.

The following $job_name needs to be replaced with the corresponding job name in the prometheus configuration, and $fe_leader needs to be replaced with the corresponding ip of the leader fe.

## Service pending alarm

### FE service suspended

Expression:

```sql
count(up{group="fe", job="$job_name"}) >= 3
```

Alarm description:

Please adjust the number of surviving Fe nodes according to the actual number of Fe nodes.

Processing method:

FE service is down, try to pull it up.

### BE service suspended

Expression:

```sql
node_info{type="be_node_num", job="$job_name",state="dead"} > 1
```

Alarm description:

The number of pending nodes are greater than 1.

Processing method:

There is a be service down, try to pull up.

## Machine overload alarm

### BE cpu alarm

Expression:

```sql
(1-(sum(rate(starocks_be_cpu{mode="idle", job="$job_name",instance=~".*"}[5m])) by (job, instance)) / (sum(rate(starocks_be_cpu{job="$job_name",host=~".*"}[5m])) by (job, instance))) * 100 > 90
```

Alarm description:

CPU util exceeds 90%

Processing method:

There may be large queries or a large amount of data import. First, you can get the top result and take a screenshot.

```shell
top -Hp $be_pid
```

Get the result of the perf and send it to StarRocks support for positioning.

```shell
sudo perf top -p $be_pid -g >/tmp/perf.txt #Execute for 1-2 minutes, then cancel with CTRL+C.
```

In case of emergency, in order to restore the service as soon as possible, you can try to restart the corresponding BE service (keep the stack before restarting) (emergency: BE node process monitoring continues to be abnormal and full, unable to reduce CPU usage through effective positioning measures).

### Memory alarm

Expression:

```sql
(1-node_memory_MemAvailable_bytes{instance=~".*"}/node_memory_MemTotal_bytes{instance=~".*"})*100 > 90
```

Alarm description:

Memory usage rate exceeds 90%.

Processing method:

Troubleshooting methods can refer to [memory problem troubleshooting](https://docs.starrocks.io/docs/administration/management/resource_management/Memory_management/) or get [heap profile](https://github.com/StarRocks/starrocks/pull/35322) troubleshooting.

- In case of emergency, in order to restore the service as soon as possible, you can try to restart the corresponding BE service (emergency: BE node process memory monitoring continues to be abnormal and full, and it is impossible to reduce memory usage through effective positioning).
- If other services of the mixed department affect SR, other services can be considered to be stopped in an emergency

### Disk alarm

#### Disk load alarm

Expression:

```sql
rate(node_disk_io_time_seconds_total{instance=~".*"}[1m]) * 100 > 90
```

Alarm description:

Disk load over 90%

Processing method:

If there is a node_disk_io_time_seconds_total alarm, then first confirm whether there is a change in the business, if there is, whether the change can be rolled back, maintain the resource balance conditions before, if not, consider whether it is normal business growth leads to the need to expand resources.

You can use the iotop tool to analyze and monitor disk I/O usage. Iotop has a UI similar to top, including pid, user, I/O, process and other related information.

You can also use the following command to obtain the tablets that consume IO in the current system in StarRocks, and then locate specific tasks and tables.

```sql
/*"all" indicates all services, "10" represents a collection interval of 10 seconds, and "3" signifies obtaining the top 3 results*/
admin execute on $backend_id 'System.print(ExecEnv.io_profile_and_get_topn_stats("all", 10, 3))';
```

#### Root directory space capacity alarm

Expression:

```sql
node_filesystem_free_bytes{mountpoint="/"} /1024/1024/1024 < 5
```

Alarm description:

The remaining space capacity of the root directory is less than 5g.

Processing method:

You can use the following command to analyze which directory occupies more space and clean up unnecessary files in time. Generally, directories that may occupy more space are /var,/opt,/tmp

```shell
du -sh / --max-depth=1
```

#### Data disk disk capacity alarm

Expression:

```sql
(SUM(starocks_be_disks_total_capacity{job="$job"}) by (host, path) - SUM(starocks_be_disks_avail_capacity{job="$job"}) by (host, path)) / SUM(starocks_be_disks_total_capacity{job="$job"}) by (host, path) * 100 > 90
```

Alarm description:

Disk capacity usage over 90%

Processing method:


1. Check if the amount of imported data has changed

You can pay attention to the load bytes monitoring item in the grafana monitoring. If the data import volume increases a lot, it is recommended to expand the resources.

2. Check if there is a drop operation

If there is no significant change in the amount of data imported, and the dataused and disk usage seen in show backends are inconsistent, you can check whether you have recently performed drop database, drop table, or drop partition operations (fe.audite.log).

The metadata information involved in these operations will be retained in the fe memory for 1 day (data can be recovered through recover within one day to avoid misoperation), this time may occur, disk occupancy is greater than the displayed in the show backends used space, memory retention 1 day can be adjusted by the fe parameter catalog_trash_expire_second, adjustment method

```sql
admin set frontend config ("catalog_trash_expire_second"="86400") #If persistence is required, remember to add it to the `fe.conf` file.
```

The data dropped in the fe memory after 1 day into the be trash directory ($storage_root_path/trash), the data will be retained in the trash directory for 3 days by default, this time will also appear disk occupancy is greater than the show backends displayed in the used space, trash retention time by be configuration trash_file_expire_time_sec (default 259200, 3 days, since v2.5.17, v3.0.9 and v3.1.6, the default value from 259,200 to 86,400), the adjustment method.

```shell
curl http://be_ip:be_http_port/api/update_config?trash_file_expire_time_sec=xxx #如果需要持久化需要在be.conf新增该配置
```

#### FE metadata disk capacity alarm

Expression:

```sql
node_filesystem_free_bytes{mountpoint="${meta_path}"} /1024/1024/1024 < 10
```

Alarm description:

The remaining space capacity of the FE meta directory is less than 10g.

Processing method:

You can use the following command to analyze which directory takes up more space, clean up unnecessary files in time, the meta path is the path configured meta_dir specified in fe.conf

```shell
du -sh /${meta_dir} --max-depth=1
```

If the meta directory occupies a large space, it is usually because the bdb directory occupies a large space. It may be a checkpoint failure. You can refer to [checkpoint failure alarm] (#checkpoint failure alarm) for troubleshooting. If there is no progress, contact StarRocks support personnel in time to solve it.

## Cluster service exception alarm

### Compaction exception alarm

#### Compaction failure alarm

Expression:

```sql
increase(starocks_be_engine_requests_total{job="$job_name" ,status="failed",type="cumulative_compaction"}[1m]) > 3
increase(starocks_be_engine_requests_total{job="$job_name" ,status="failed",type="base_compaction"}[1m]) > 3
```

Alarm description:

There have been three failed incremental merges or three failed base merges in the last minute.

Processing method:

Search for the following keywords in the corresponding node log to determine the tablet involved

```shell
grep -E 'compaction' be.INFO|grep failed
```

Generally, there will be the following logs

```plain text
W0924 17:52:56:537041 123639 comaction_task_cpp:193] compaction task:8482. tablet:8423674 failed.
```

You can obtain the context of the corresponding tablet, which may be caused by deleting tables or partitioning during the compaction process. There is an internal compaction retry strategy. If it is a fixed replica, you can trigger a re-clone repair by manually setting bad (provided that the table is a 3-replica).

```sql
ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "2889157", "backend_id" = "$backend_id", "status" = "bad");
```

#### High compaction pressure alarm

Expression:

```sql
starocks_fe_max_tablet_compaction_score{job="$job_name",instance="$fe_leader"} > 100
```

Alarm description:

The maximum merged score exceeds 100, indicating that the merger pressure is relatively high.

Processing method:

This is generally due to the initiation of high-frequency (once per second) write tasks, or it may be due to the initiation of more insert into values or delete from tasks. It is recommended to submit load or delete tasks every 5s +, and it is not recommended to submit delete tasks with high concurrency.

##### Alert when the number of versions exceeds the limit.

Expression:

```sql
starocks_be_max_tablet_rowset_num{job="$job_name"} > 700
```

Alarm description:

There are more than 700 versions of a tablet.

Processing method:

Check which tablets

```sql
select BE_ID,TABLET_ID from information_schema.be_tablets where NUM_ROWSET>700;
```

Take tablet 2889156 as an example

```sql
show tablet 2889156;
```

Then execute the instructions in detailcmd

```sql
SHOW PROC '/dbs/2601148/2889154/partitions/2889153/2889155/2889156';
```

![show proc replica](../../../_assets/alert_show_proc_3.png)

Under normal circumstances, as shown in the figure, all three replicas should be in the normal state, and the other indicators should be basically consistent, such as rowcount and datasize. If only one replica version exceeds 700, the following command can be used to trigger cloning from other replicas

```sql
ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "2889157", "backend_id" = "$backend_id", "status" = "bad");
```

If more than 2 copies exceed the version limit, the number of versions can be temporarily increased

```shell
#The be_host below is the IP of the BE node with a version exceeding the limit found above, and the http_port defaults to 8040.
curl -XPOST http://be_host:http_port/api/update_config?tablet_max_versions=2000（default 1000）
```

### Checkpoint failure alarm

Expression:

```sql
starocks_fe_meta_log_count{job="$job_name",instance="$fe_master"} > 100000
```

Alarm description:

If the number of fe bdb logs exceeds 100000, the default value of 50000 will be set to 0 for checkpoint and re-accumulation.

Processing method:

The reason for the alarm is that Checkpoint was not done, so we confirmed the problem by investigating the Checkpoint situation.

Check if there is a log of completed checkpoint in fe: search for `begin to generate new image: image.xxxx` in the fe.log of Leader FE. If found, it means that the image has started to be generated. Check the subsequent logs of this thread. If `checkpoint finished save image.xxxx` appears, it means that the image has been written successfully. If `an Exception occurs when generating new image file`, the generation fails. You need to check the specific error information, which involves metadata issues. The operation needs to be cautious. Please contact S ta r R o c k s support personnel for analysis.

### FE thread count is full alarm

Expression:

```sql
s ta ro c k sfe_thread_pool{job="$job_name"} > 3000
```

Alarm description:

Fe thread pool size exceeds 3000.

Processing method:

The default number of threads for Fe and BE is 4096. Generally, there are many union all queries, so reduce the execution concurrency of union all and adjust the SQL level pipeline_dop. If it is not convenient to adjust the SQL granularity, you can adjust the pipeline_dop globally, `set global pipeline_dop = 8`.

Emergency handling:

Temporary avoidance and emergency scenarios can choose to increase this parameter, thrift_server_max_worker_threads, default value 4096

```sql
admin set frontend config ("thrift_server_max_worker_threads"="8192")
```

### FE JVM high usage alarm

Expression:

```sql
sum(jvm_heap_size_bytes{job="$job_name", type="used"}) * 100 / sum(jvm_heap_size_bytes{job="$job_name", type="max"}) > 90
```

Alarm description:

Fe JVM usage rate exceeds 90%.


Processing method:

If this monitoring alarm occurs, you can manually download jmap to assist in analysis when the JVM usage rate is high. The detailed monitoring information of the current indicator is still under development, so there is no more intuitive way to see the occupation information. Perform the following operations to send the results to StarRocks support personnel for positioning.

```shell
jmap -histo[:live] $fe_pid > jmap.dump  #Adding "live" may cause the FE to restart.
```

You can restart the corresponding FE node or adjust the JVM (Xmx) to restart the Fe service if it continues to hit high without releasing.

## Business availability alarm

### Import exception alarm

#### Import failure alarm

Expression:

```sql
rate(starocks_fe_txn_failed{job="$job_name",instance="$fe_master"}[5m]) * 100 > 5
```

Alarm description:

The number of failed items imported exceeds 5%.

Processing method:

View leader fe log

Search for information related to import errors, you can search for the keyword "status: ABORTED" to view the task of import failure.

```plain text
2024-04-09 18:34:02.363+08:00 INFO (thrift-server-pool-8845163|12111749) [DatabaseTransactionMgr.abortTransaction():1279] transaction:[TransactionState. txn_id: 7398864, label: 967009-2f20a55e-368d-48cf-833a-762cf1fe07c5, db id: 10139, table id list: 155532, callback id: 967009, coordinator: FE: 192.168.2.1, transaction status: ABORTED, error replicas num: 0, replica ids: , prepare time: 1712658795053, commit time: -1, finish time: 1712658842360, total cost: 47307ms, reason: [E1008]Reached timeout=30000ms @192.168.1.1:8060 attachment: RLTaskTxnCommitAttachment [filteredRows=0, loadedRows=0, unselectedRows=0, receivedBytes=1033110486, taskExecutionTimeMs=0, taskId=TUniqueId(hi:3395895943098091727, lo:-8990743770681178171), jobId=967009, progress=KafkaProgress [partitionIdToOffset=2_1211970882|7_1211893755]]] successfully rollback
```

#### Routine load consumption delay alarm

Expression:

```sql
(sum by (job_name)(starocks_fe_routine_load_max_lag_of_partition{job="$job_name",instance="$fe_mater"})) > 300000
starocks_fe_routine_load_jobs{job="$job_name",host="$fe_mater",state="NEED_SCHEDULE"} > 3
starocks_fe_routine_load_jobs{job="$job_name",host="$fe_mater",state="PAUSED"} > 0
```

Alarm description:

The consumption delay exceeds 300,000.

The number of scheduled routine load tasks exceeds 3 or there are tasks in PAUSED state.


Processing method:

1. First, check whether the routine load task status is RUNNING

show routine load from $db; #关注State字段

2. If the routine load task status is PAUSED

Pay attention to the ReasonOfStateChanged, ErrorLogUrls or TrackingSQL returned in the previous step. Generally, the SQL corresponding to TrackingSQL can see the specific error information, such as

    ![Tracking SQL](../../../_assets/routine_load_tracking.png)

3. If the routine load task status is RUNNING

You can try to increase the task parallelism. The concurrency of a single Routine Load Job is determined by the minimum of the following four values.

```plain text
kafka_partition_num，kafka topic的分区个数
desired_concurrent_number，任务设置的并行度
alive_be_num，存活的be节点
max_routine_load_task_concurrent_num，fe的配置，默认5
```

Generally, it is necessary to adjust the parallelism of tasks or the number of topic partitions in Kafka (contact Kafka colleagues for processing). The following is the method to adjust the parallelism of tasks

```sql
ALTER ROUTINE LOAD FOR ${routine_load_jobname}
PROPERTIES
(
    "desired_concurrent_number" = "5"
);
```

#### Import more than a single DB transaction limit alarm

Expression:

```sql
sum(starocks_fe_txn_running{job="$job_name"}) by(db) > 900
```

Alarm description:

The number of imported transactions in a single db exceeds 900. Before version 3.1, the limit was 100. Please modify the alarm threshold accordingly.

Processing method:

Generally, it is due to the addition of more import tasks, and the limit of a single DB import transaction can be temporarily increased

```sql
ADMIN SET FRONTEND CONFIG ("max_running_txn_num_per_db" = "2000");
```

### Query abnormal alarm

#### Query delay alarm

Expression:

```sql
starocks_fe_query_latency_ms{job="$job_name", quantile="0.95"} > 5
```

Alarm description:

Query time P95 is greater than 5s

Processing method:

1. First, investigate the big query

Check if there are large queries that occupy a lot of machine resources during the period of abnormal monitoring indicators, causing other queries to timeout or fail.

- Editor execute `show proc '/current_queries'`; can get the specific query id and other information, in order to quickly restore the service, you can kill the longest query execution time

```sql
mysql> SHOW PROC '/current_queries';
+--------------------------------------+--------------+------------+------+-----------+----------------+----------------+------------------+----------+
| QueryId                              | ConnectionId | Database   | User | ScanBytes | ProcessRows    | CPUCostSeconds | MemoryUsageBytes | ExecTime |
+--------------------------------------+--------------+------------+------+-----------+----------------+----------------+------------------+----------+
| 7c56495f-ae8b-11ed-8ebf-00163e00accc | 4            | tpcds_100g | root | 37.88 MB  | 1075769 Rows   | 11.13 Seconds  | 146.70 MB        | 3804     |
| 7d543160-ae8b-11ed-8ebf-00163e00accc | 6            | tpcds_100g | root | 13.02 GB  | 487873176 Rows | 81.23 Seconds  | 6.37 GB          | 2090     |
+--------------------------------------+--------------+------------+------+-----------+----------------+----------------+------------------+----------+
2 rows in set (0.01 sec)
```

- Directly restart BE node with high CPU utilization

2. Check if machine resources are sufficient

At the machine resource level, confirm whether the corresponding time period Cpu, Mem, Diskio, and network traffic monitoring information are normal. If there are abnormalities in the corresponding time period, the reason for query failure or bottleneck can be confirmed through changes in peak traffic and cluster resource usage. If there are continuous abnormalities, the corresponding node can be restarted.

Emergency handling:

1. If the query fails due to an abnormal peak traffic surge, the business traffic can be urgently reduced, and the corresponding BE node can be restarted to release the backlog of queries
2. If the resource is normally full and triggers an alarm value, node expansion can be considered

#### Query failure alarm

Expression:

```
sum by (job,instance)(starocks_fe_query_err_rate{job="$job_name"}) * 100 > 10 
increase(starocks_fe_query_internal_err{job="$job_name"})[1m] >10（since v3.1.15,v3.2.11,v3.3.3）
```

Alarm description:

Query failure rate 0.1/s or 1 minute query failure 10 new

Processing method:

When triggering this monitoring indicator, we can first confirm which SQL errors have been reported through logs.

```shell
grep 'State=ERR' fe.auit.log
```

If the auditloder plugin is installed, you can find the corresponding SQL in the following way

```sql
select stmt from starocks_audit_db__.starocks_audit_tbl__ where state='ERR';
```

It should be noted that the current syntax error, timeout, and other types of SQL are also counted as starocks_fe_query_err_rate failed queries.

SQL query failure due to SR kernel exception, need to get the complete exception stack in fe.log (search for error sql in fe.log) and Query Dump contact StarRocks support colleague troubleshooting.

#### Query overload alarm

Expression:

```sql
abs((sum by (exported_job)(rate(starocks_fe_query_total{process="FE",job="$job_name"}[3m]))-sum by (exported_job)(rate(starocks_fe_query_total{process="FE",job="$job_name"}[3m] offset 1m)))/sum by (exported_job)(rate(starocks_fe_query_total{process="FE",job="$job_name"}[3m]))) * 100 > 100

abs((sum(starocks_fe_connection_total{job="$job_name"})-sum(starocks_fe_connection_total{job="$job_name"} offset 3m))/sum(starocks_fe_connection_total{job="$job_name"})) * 100 > 100
```

Alarm description:

The QPS or connection count in the last minute increased by 100% compared to the previous minute, alarming.

Processing method:

Check if the frequently occurring queries in fe.audit.log or starocks_audit_db__ starocks_audit_tbl__ meet expectations. If there are changes in normal business behavior (such as new business or changes in business data volume), it is necessary to pay attention to the machine load and timely expand the BE node.

#### User granularity connection number exceeds limit alarm

Expression:

```sql
sum(starocks_fe_connection_total{job="$job_name"}) by(user) > 90
```

Alarm description:

User granularity connection count greater than 90 alarm (user granularity connection count is supported from 3.1.16/3.2.12/3.3.4)

Processing method:

You can check whether the current number of connections meets expectations through the show processlist. You can manually kill the connections that do not meet expectations. Check whether the front-end has improper use of the business side that causes the connection to not be released for a long time. You can also automatically kill the connections that have slept for too long by adjusting the wait_timeout parameters. You can adjust the Set wait_timeout = xxx parameter in units of s.

Emergency solution:

```sql
/*Increase the connection limit for the corresponding user.*/
ALTER USER 'jack' SET PROPERTIES ("max_user_connections" = "1000");

/*Settings for versions 2.5 and earlier.*/
SET PROPERTY FOR 'jack' 'max_user_connections' = '1000'；
```

#### Schema change abnormal alarm

Expression:

```sql
increase(starocks_be_engine_requests_total{job="$job_name",type="schema_change", status="failed"}[1m]) > 1
```

Alarm description:

The number of failed schema change tasks in the last minute exceeded 1.

Processing method:

First, check whether the field Msg returned by the following command has a corresponding error message

show alter column from $db;

If not, search for the JobId context returned from the previous step in the leader fe log

1. Schema change out of memory

You can search in the be.WARNING log of the corresponding time node for `failed to process the version, failed to process the schema change. from tablet, Memory of schema change task exceeds limit information`, confirm the context, and view failed to execute schema change:

Memory overrun error log is: `fail to execute schema change: Memory of schema change task exceeds limit. DirectSchemaChange Used: 2149621304, Limit: 2147483648. You can change the limit by modifying BE config [memory_limitation_per_thread_for_schema_change]`

This error is caused by a single schema change using more than the default memory limit of 2G, which is controlled by the following parameters.

```plain text
memory_limitation_per_thread_for_schema_change     
```
Modification method

```shell
curl -XPOST http://be_host:http_port/api/update_config?memory_limitation_per_thread_for_schema_change=8     
```

2. Schema change timeout

Adding columns is a lightweight implementation, while other schema change implementations create a bunch of new tablets, rewrite the original data, and ultimately implement it through swap.

```plain text
Create replicas failed. Error: Error replicas:21539953=99583471, 21539953=99583467, 21539953=99599851     
```

Increase the Timeout for creating tablets

```sql
admin set frontend config ("tablet_create_timeout_second" = "60") (default 10)
```

Increase the number of threads to create tablets

```shell
curl -XPOST http://be_host: http_port/api/update_config? alter_tablet_worker_count = 6 (default 3)
```

3. Tablets have abnormal copies

You can search the be.WARNING log for information that the tablet is not normal.`show proc '/statistic'` can see the cluster-level UnhealthyTabletNum information

![show statistic](../../../_assets/alert_show_statistic.png)

You can further enter show proc '/statistics/Dbid' to see the number of unhealthy replicas in the specified DB

![show statistic db](../../../_assets/alert_show_statistic_db.png)

Further, the corresponding table information can be viewed by showing tablet tabletid

![show tablet](../../../_assets/alert_show_tablet.png)

Execute the content in DetailCmd to confirm the unhealthy cause

![show proc](../../../_assets/alert_show_proc.png)

Generally speaking, unhealthy and inconsistent replicas are related to high-frequency imports. You can check whether the table has a large number of real-time writes. This unhealthy or inconsistent behavior is related to the asynchronous writing progress of the three replicas. After reducing the frequency or briefly stopping the service, it will decrease. You can retry the task.

Emergency handling:
- After the task fails, you need to troubleshoot and retry through the above method.
- The online environment is strictly required to be configured as 3 replicas. If there is a replica with 1 tablet that is not normal, the command to forcibly set it to bad can be executed (provided that three replicas are guaranteed and only one replica is damaged).

#### Materialized view refresh abnormal alarm

Expression:

```sql
increase(starocks_fe_mv_refresh_total_failed_jobs[5m]) > 0
```

Alarm description:

The number of failed times to refresh the new materialized view in the last 5 minutes exceeds 1.

Processing method:

1. Which materialized views failed

```sql
select TABLE_NAME,IS_ACTIVE,INACTIVE_REASON,TASK_NAME from information_schema.materialized_views where LAST_REFRESH_STATE !=" SUCCESS"
```

2. You can try to refresh manually first.

```sql
REFRESH MATERIALIZED VIEW ${mv_name};
```

3. If the materialized view state is INACTIVE, you can try to set it to ACTIVE as follows

```sql
ALTER MATERIALIZED VIEW ${mv_name} ACTIVE;
```

4. Reasons for troubleshooting failure

```sql
SELECT * FROM information_schema.task_runs WHERE task_name ='mv-112517' \G
```
