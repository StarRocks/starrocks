# StarRocks 集群报警规则与处理办法

本文从业务持续性、集群可用性、机器等多个维度介绍需要关注的报警项及其处理办法。

以下 $job_name 需要替换为 prometheus 配置中对应的 job name，$fe_leader 需要替换为对应 leader fe 的 ip。

## 服务挂起报警

### FE 服务挂起

表达式：

```sql
count(up{group="fe", job="$job_name"}) >= 3
```

报警描述：

存活的 fe 节点个数，请根据实际 fe 节点个数调整。

处理办法：

有 FE 服务挂掉，尝试拉起。

### BE 服务挂起

表达式：

```sql
node_info{type="be_node_num", job="$job_name",state="dead"} > 1
```

报警描述：

挂起的 be 节点个数大于1。

处理办法：

有 be 服务挂掉，尝试拉起。问题排查参考 [BE Crash问题排查](https://forum.mirrorship.cn/t/topic/4930)。

## 机器过载报警

### BE cpu 报警

表达式：

```sql
(1-(sum(rate(starrocks_be_cpu{mode="idle", job="$job_name",instance=~".*"}[5m])) by (job, instance)) / (sum(rate(starrocks_be_cpu{job="$job_name",host=~".*"}[5m])) by (job, instance))) * 100 > 90
```

报警描述：

cpu util 超过90%

处理办法：

可能有大查询或者进行过大量数据导入。首先可以获取top结果并截图

```shell
top -Hp $be_pid
```

获取下perf的结果， 发送给 StarRocks 支持人员进行定位。

```shell
sudo perf top -p $be_pid -g >/tmp/perf.txt #执行1-2分钟，CTRL+C掉即可
```

紧急情况下，为尽快恢复服务，可尝试重启对应的BE服务（重启前保留stack）（紧急情况：BE节点进程监控持续异常打满，无法通过有效定位采取手段降低使用cpu）

### 内存报警

表达式：

```sql
(1-node_memory_MemAvailable_bytes{instance=~".*"}/node_memory_MemTotal_bytes{instance=~".*"})*100 > 90
```

报警描述：

内存使用率超过 90%

处理办法：

排查方法可以参考 [内存问题排查](https://forum.mirrorship.cn/t/topic/9867/27)或者获取 [heap profile](https://github.com/StarRocks/starrocks/pull/35322) 排查。

* 紧急情况下，为尽快恢复服务，可尝试重启对应的BE服务 （紧急情况：BE节点进程内存监控持续异常打满，无法通过有效定位采取手段降低使用内存）
* 如果是混部的其他服务影响了SR，紧急情况下可考虑停掉其他服务

### 磁盘报警

#### 磁盘负载报警

表达式：

```sql
表达式：

```sql
rate(node_disk_io_time_seconds_total{instance=~".*"}[1m]) * 100 > 90
```

报警描述：

磁盘负载超过 90%

处理办法：

如果存在node_disk_io_time_seconds_total的告警 那么要先确认下业务上是否有变化，如果有的话能否回滚变更，维系回之前的资源平衡条件，没有的话则考虑是否是正常的业务增长导致需要扩容资源。

可以使用 iotop 工具去分析监视磁盘I/O使用状况，iotop具有与top相似的UI，其中包括pid、user、I/O、进程等相关信息等。

也可以通过以下指令，在 starrocks 中获取当前系统中耗费IO的tablet，进而定位到具体的任务和表。

```sql
/*all表示所有服务，10表示采集10s，3表示获取top3结果*/
admin execute on $backend_id 'System.print(ExecEnv.io_profile_and_get_topn_stats("all", 10, 3))';
```

#### 根目录空间容量报警

表达式：

```sql
node_filesystem_free_bytes{mountpoint="/"} /1024/1024/1024 < 5
```

报警描述：

根目录空间容量剩余不足 5g

处理办法：

可以使用如下命令分析哪个目录空间占用较多，及时清理不需要的文件，一般可能占用空间较大的目录是/var，/opt，/tmp

```shell
du -sh / --max-depth=1
```

#### 数据盘磁盘容量报警

表达式：

```sql
(SUM(starrocks_be_disks_total_capacity{job="$job"}) by (host, path) - SUM(starrocks_be_disks_avail_capacity{job="$job"}) by (host, path)) / SUM(starrocks_be_disks_total_capacity{job="$job"}) by (host, path) * 100 > 90
```

报警描述：

磁盘容量使用率超过90%

处理办法：


1. 排查导入数据量是否有变化

可以关注 grafana 监控中的 load bytes 监控项，如果数据导入量新增许多，建议扩容资源

2. 排查是否有drop操作

如果数据导入量没有发生太大变化，show backends中看到的dataused和磁盘占用不一致，可以核对下近期是否做过drop database、drop table或者drop分区的操作（fe.audit.log）

这些操作涉及的元数据信息会在fe内存中保留1天（一天之内可以通过recover恢复数据，避免误操作），这个时候可能会出现，磁盘占用大于show backends中显示的已用空间，内存中保留1天可通过fe的参数 catalog_trash_expire_second调整，调整方式

```sql
admin set frontend config ("catalog_trash_expire_second"="86400") #如果需要持久化，记得加到fe.conf中
```

drop的数据在fe内存中过了1天后进入到了be的trash目录下($storage_root_path/trash)，该数据默认会在trash目录保留3天，这个时候也会出现磁盘占用大于show backends中显示的已用空间，trash保留时间由be的配置 trash_file_expire_time_sec（默认259200，3天，自 v2.5.17、v3.0.9 以及 v3.1.6 起，默认值由 259,200 变为 86,400。）,调整方式

```shell
curl http://be_ip:be_http_port/api/update_config?trash_file_expire_time_sec=xxx #如果需要持久化需要在be.conf新增该配置
```

#### FE 元数据磁盘容量报警

表达式：

```sql
node_filesystem_free_bytes{mountpoint="${meta_path}"} /1024/1024/1024 < 10
```

报警描述：

磁盘容量剩余不足 10g

处理办法：

可以使用如下命令分析哪个目录空间占用较多，及时清理不需要的文件，meta 路径为 fe.conf 中配置 meta_dir 指定的路径

```shell
du -sh /${meta_dir} --max-depth=1
```

如果是meta目录占用空间比较大，一般都是因为bdb目录占用比较大，可能是checkpoint失败，可参考[checkpoint失败报警](#checkpoint失败报警)排查，如果无进展，及时联系 starrocks 支持人员解决。

## 集群服务异常报警

### compaction 失败报警

#### 增量合并失败报警

表达式：

```sql
increase(starrocks_be_engine_requests_total{job="$job_name" ,status="failed",type="cumulative_compaction"}[1m]) > 3
increase(starrocks_be_engine_requests_total{job="$job_name" ,status="failed",type="base_compaction"}[1m]) > 3
```

报警描述：

最近 1 分钟有三次增量合并失败或者有三次 base 合并失败。

处理办法：

在对应 be 节点日志中搜索以下关键字，确定涉及的 tablet

```shell
grep -E 'compaction' be.INFO|grep failed
```

一般会有如下日志

```plaintext
W0924 17:52:56:537041 123639 comaction_task_cpp:193] compaction task:8482. tablet:8423674 failed.
```

可以获取下对应 tablet 的上下文，一般可能原因是 compaction 过程中进行了删表或者分区操作导致。内部有 compaction 重试策略，如果是某个固定副本，则可以通过手动 set bad 方式触发重新 clone 修复（前提是该表是3副本）。

```sql
ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "2889157", "backend_id" = "$backend_id", "status" = "bad");
```

#### compaction 压力大报警

表达式：

```sql
starrocks_fe_max_tablet_compaction_score{job="$job_name",instance="$fe_leader"} > 100
```

报警描述：

最大合并的 score 超过100，表示合并压力比较大。

处理办法：

这个一般是因为发起了高频率（每秒1次）的写入任务导致，也有可能是发起了比较多的 insert into values 或者 delete from 任务，建议每5s+提交一次load任务或者delete任务，不建议高并发提交delete任务。

#### 版本个数超限制报警

表达式：

```sql
starrocks_be_max_tablet_rowset_num{job="$job_name"} > 700
```

报警描述：

某个 be 中 tablet 最大的版本个数超过 700.


处理办法：

排查是哪些tablet

```sql
select BE_ID,TABLET_ID from information_schema.be_tablets where NUM_ROWSET>700;
```

以tablet 2889156为例

```sql
show tablet 2889156;
```

然后执行detailcmd里的指令

```sql
SHOW PROC '/dbs/2601148/2889154/partitions/2889153/2889155/2889156';
```

![show proc replica](../../../_assets/alert_show_proc_3.png)

正常情况下如图所示三副本都应该为normal状态，且其余指标基本保持一致，例如rowcount以及datasize。如果只有一个副本版本超过700，则可以通过如下指令触发从其他副本clone

```sql
ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "2889157", "backend_id" = "$backend_id", "status" = "bad");
```

如果有 2 个以上副本都超过了版本限制，可暂时调大版本个数限制

```shell
#以下be_host为上面查到版本超过限制的be节点ip，http_port默认为8040
curl -XPOST http://be_host:http_port/api/update_config?tablet_max_versions=2000（默认1000）
```

### checkpoint失败报警

表达式：

```sql
starrocks_fe_meta_log_count{job="$job_name",instance="$fe_master"} > 100000
```

报警描述：

fe bdb log 个数超过 100000，默认 50000 会进行 checkpoint 置为 0 重新累加。

处理办法：

该报警原因是没有做Checkpoint，所以我们通过排查Checkpoint的情况来确认问题：

查看fe是否有做完checkpoint的日志：在 Leader FE 的 fe.log 中搜索 begin to generate new image: image.xxxx。如果找到，则说明开始生成 image 了。检查这个线程的后续日志，如果出现 checkpoint finished save image.xxxx，则说明 image 写入成功，如果出现 Exception when generate new image file，则生成失败，需要查看具体的错误信息 ，涉及到元数据问题，操作需要谨慎，请联系 starrocks 支持人员进行分析。

### FE 线程数被打满报警

表达式：

```sql
starrocks_fe_thread_pool{job="$job_name"} > 3000
```

报警描述：

fe 线程池大小超过 3000。

处理办法：

fe和be的线程数默认是4096，一般是有比较多的union all查询导致，调低union all的执行并发，sql级别调整pipeline_dop，如果不方便调整sql粒度，可以全局调整pipeline_dop，set global pipeline_dop=8。

紧急处理：  

临时规避和紧急方案可以选择调大该参数，thrift_server_max_worker_threads，默认值4096  

```sql
admin set frontend config ("thrift_server_max_worker_threads"="8192")
```

### FE JVM 使用率高报警

表达式：

```sql
sum(jvm_heap_size_bytes{job="$job_name", type="used"}) * 100 / sum(jvm_heap_size_bytes{job="$job_name", type="max"}) > 90
```

报警描述：

fe jvm 使用率超过 90%。


处理办法：

如果出现该监控告警，jvm使用率高时可以手动打下 jmap协助分析，当前指标的详细监控信息还在开发过程中，所以没有更直观的方式可以看出占用信息，执行以下操作把结果给发送给 StarRocks 支持人员进行定位：

```shell
jmap -histo[:live] $fe_pid > jmap.dump  加live可能会导致fe重启
```

持续打高不释放的情况下可以重启下对应的FE节点或者调大jvm（Xmx）重启fe服务。

## 业务可用性报警

### 导入异常报警

#### 导入失败报警

表达式：

```sql
rate(starrocks_fe_txn_failed{job="$job_name",instance="$fe_master"}[5m]) * 100 > 5
```

报警描述：

导入失败事物个数超过 5%

处理办法：

查看 leader fe 日志
搜索导入报错的相关信息，可以搜索"status: ABORTED"关键字，查看导入失败的任务，可参考 [导入失败问题排查](https://forum.mirrorship.cn/t/topic/4923/18)。

```plain text
2024-04-09 18:34:02.363+08:00 INFO (thrift-server-pool-8845163|12111749) [DatabaseTransactionMgr.abortTransaction():1279] transaction:[TransactionState. txn_id: 7398864, label: 967009-2f20a55e-368d-48cf-833a-762cf1fe07c5, db id: 10139, table id list: 155532, callback id: 967009, coordinator: FE: 192.168.2.1, transaction status: ABORTED, error replicas num: 0, replica ids: , prepare time: 1712658795053, commit time: -1, finish time: 1712658842360, total cost: 47307ms, reason: [E1008]Reached timeout=30000ms @192.168.1.1:8060 attachment: RLTaskTxnCommitAttachment [filteredRows=0, loadedRows=0, unselectedRows=0, receivedBytes=1033110486, taskExecutionTimeMs=0, taskId=TUniqueId(hi:3395895943098091727, lo:-8990743770681178171), jobId=967009, progress=KafkaProgress [partitionIdToOffset=2_1211970882|7_1211893755]]] successfully rollback
```

#### Routine load 消费延迟报警

表达式：

```sql
(sum by (job_name)(starrocks_fe_routine_load_max_lag_of_partition{job="$job_name",instance="$fe_mater"})) > 300000
starrocks_fe_routine_load_jobs{job="$job_name",host="$fe_mater",state="NEED_SCHEDULE"} > 3
starrocks_fe_routine_load_jobs{job="$job_name",host="$fe_mater",state="PAUSED"} > 0
```

报警描述：

消费延迟超过 300000 条。

待调度的 routine load 任务个数超过3或者有处于 PAUSED 状态的任务。


处理办法：

1. 首先排查routine load任务状态是否为RUNNING

```sql
show routine load from $db; #关注State字段
```

2. 如果routine load任务状态为PAUSED

关注上一步返回的ReasonOfStateChanged、ErrorLogUrls或TrackingSQL，一般执行TrackingSQL对应的SQL可以看到具体的报错信息，例如

    ![Tracking SQL](../../../_assets/routine_load_tracking.png)

3. 如果routine load任务状态为RUNNING

可尝试调大任务并行度，单个Routine Load Job的并发度由以下四个值的最小值决定：

```plain text
kafka_partition_num，kafka topic的分区个数
desired_concurrent_number，任务设置的并行度
alive_be_num，存活的be节点
max_routine_load_task_concurrent_num，fe的配置，默认5
```

一般需要调整任务的并行度或者kafka的topic 分区个数（联系kafka的同学处理），以下是调整任务并行度的方法

```sql
ALTER ROUTINE LOAD FOR ${routine_load_jobname}
PROPERTIES
(
    "desired_concurrent_number" = "5"
);
```

#### 导入超过单个 DB 事物限制报警

表达式：

```sql
sum(starrocks_fe_txn_running{job="$job_name"}) by(db) > 900
```

报警描述：

单个 db 导入事物个数超过 900，3.1 版本之前限制为100，请对应修改报警阈值。

处理办法：

一般是因为新增较多的导入任务导致，可以临时调大单个 DB 导入事物限制

```sql
ADMIN SET FRONTEND CONFIG ("max_running_txn_num_per_db" = "2000");
```

### 查询异常报警

#### 查询延迟报警

表达式：

```sql
starrocks_fe_query_latency_ms{job="$job_name", quantile="0.95"} > 5
```

报警描述：

查询耗时 P95 大于 5s

处理办法：

1. 首先排查大查询

查看是否在监控指标异常的时间段 有部分大查询占用了大量的机器资源导致其他查询超时失败或者失败。

* 编辑器执行 show proc '/current_queries';   可以拿到具体的query id等信息 ，为了快速恢复服务，可以 kill执行时间最长的查询

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

* 直接重启 CPU 利用率很高的BE 节点

2. 查看机器资源是否充足

机器资源层面确认对应的时间段Cpu 、Mem 、Diskio、网络流量监控信息是否正常，如果有对应时间段的异常，可以通过峰值流量的变化、集群资源的使用等方式来确认查询失败的原因或者卡点。持续有异常的话可以重启对应的节点。

紧急处理：

1. 如果是异常峰值流量激增导致资源打满的查询失败 可以紧急减少业务流量  并重启对应的BE节点 释放积压的查询
2. 如果是资源正常打满触发告警值  则可以考虑进行扩容节点

#### 查询失败报警

表达式：

```sql
sum by (job,instance)(starrocks_fe_query_err_rate{job="$job_name"}) * 100 > 10 
increase(starrocks_fe_query_internal_err{job="$job_name"})[1m] >10（3.1.15,3.2.11,3.3.3 开始支持）
```

报警描述：

查询失败率0.1/s或者1分钟查询失败新增10个

处理办法：

当触发该监控指标时，我们可以先通过日志来确认有哪些sql报错：

```shell
grep 'State=ERR' fe.auit.log
```

如果安装了 auditloder 插件，则可以通过如下方式查找对应的 sql

```sql
select stmt from starrocks_audit_db__.starrocks_audit_tbl__ where state='ERR';
```

需要注意的是，当前语法错误、超时等类型的 sql 也算在 starrocks_fe_query_err_rate 失败的查询中。

由于 SR 内核异常导致查询失败的 sql ，需要获取 fe.log 中完整的异常栈（在 fe.log 中搜索报错的 sql ）和 [Query Dump](https://docs.starrocks.io/zh/docs/faq/Dump_query/) 联系 starrocks 支持的同学排查。

#### 查询过载报警

表达式：

```sql
abs((sum by (exported_job)(rate(starrocks_fe_query_total{process="FE",job="$job_name"}[3m]))-sum by (exported_job)(rate(starrocks_fe_query_total{process="FE",job="$job_name"}[3m] offset 1m)))/sum by (exported_job)(rate(starrocks_fe_query_total{process="FE",job="$job_name"}[3m]))) * 100 > 100

abs((sum(starrocks_fe_connection_total{job="$job_name"})-sum(starrocks_fe_connection_total{job="$job_name"} offset 3m))/sum(starrocks_fe_connection_total{job="$job_name"})) * 100 > 100
```

报警描述：

最近1分钟的 QPS 或 连接数环比增加 100% 报警

处理办法：

查看 fe.audit.log 或者 starrocks_audit_db__.starrocks_audit_tbl__ 中高频出现的查询是否符合预期。如果业务上有正常行为的变更（比如新上线了业务或者业务数据量有所变更），需要关注机器负载及时扩容 BE 节点。


#### 用户粒度连接数超限制报警

表达式：

```sql
sum(starrocks_fe_connection_total{job="$job_name"}) by(user) > 90
```

报警描述：

用户粒度连接数大于 90 报警 （用户粒度连接数从3.1.16/3.2.12/3.3.4开始支持）

处理办法：

可通过show processlist 查看当前连接数是否符合预期，可以手动kill掉不符合预期的连接，检查下是否前端有业务侧使用不当导致连接长时间不释放，也可通过调整wait_timeout参数去自动kill掉sleep过久的连接,可通过Set wait_timeout=xxx参数调整，单位s。

紧急解决方法：

```sql
/* 调大对应用户的连接数限制*/
ALTER USER 'jack' SET PROPERTIES ("max_user_connections" = "1000");

/*2.5及其之前的版本设置方式*/
SET PROPERTY FOR 'jack' 'max_user_connections' = '1000'；
```

### schema change 异常报警

表达式：

```sql
increase(starrocks_be_engine_requests_total{job="$job_name",type="schema_change", status="failed"}[1m]) > 1
```

报警描述：

最近一分钟失败的 schema change 任务个数超过 1

处理办法：

先排查以下命令返回的字段Msg是否有对应的对应的报错信息

```sql
show alter column from $db;
```

如果没有，则在leader fe日志中搜索上一步返回的JobId上下文

1. schema change内存不足

    可在对应时间节点的be.WARNING 日志中搜索是否存在 `failed to process the version、failed to process the schema change. from tablet  、   Memory of schema change task exceed limit`信息，确认上下文，查看`fail to execute schema change:`

    内存超限错误日志为： `fail to execute schema change: Memory of schema change task exceed limit. DirectSchemaChange Used: 2149621304, Limit: 2147483648. You can change the limit by modify BE config [memory_limitation_per_thread_for_schema_change]`

    这个报错是由于单次schema change使用的内存超过了默认的内存限制2G引起的,该默认值是由以下be参数控制的。

    ```shell
    memory_limitation_per_thread_for_schema_change
    ```

    修改方式

    ```shell
    curl -XPOST http://be_host:http_port/api/update_config?memory_limitation_per_thread_for_schema_change=8
    ```

2. Schema change 超时

    新增列是轻量级实现，其他的 schema change 实现是创建一堆新的tablet，然后将原来的数据重写，最终通过swap实现。

    ```plain text
    Create replicas failed. Error: Error replicas:21539953=99583471, 21539953=99583467, 21539953=99599851
    ```

    调大创建tablet的超时时间

    ```sql
    admin set frontend config ("tablet_create_timeout_second"="60") （默认10）
    ```

    调大创建tablet的线程数

    ```shell
    curl -XPOST http://be_host:http_port/api/update_config?alter_tablet_worker_count=6  （默认3）
    ```

3. tablet有不normal的副本

    可在be.WARNING 日志中搜索是否存在tablet is not normal信息。
    `show proc '/statistic'`可以看到集群级别的UnhealthyTabletNum 信息

    ![show statistic](../../../_assets/alert_show_statistic.png)

    可以进一步输入show proc '/statistic/Dbid'看到指定DB内的不健康副本数

    ![show statistic db](../../../_assets/alert_show_statistic_db.png)

    进一步可通过show tablet tabletid查看对应表信息

    ![show tablet](../../../_assets/alert_show_tablet.png)

    执行DetailCmd里面的内容来确认不健康的原因

    ![show proc](../../../_assets/alert_show_proc.png)

一般来讲不健康以及不一致副本和高频导入有关，可以检查该表是否有大量实时写入，这种不健康或者不一致跟3副本写入进度不同步有关，在降低频率或者短暂停掉服务后会降下去，可以再重试该任务。

紧急处理：
- 任务失败后需要通过上述的方式去排查后重试。
- 线上环境是严格要求配置成3副本的，如果存在有1个tablet是不normal的副本，可以执行强制设置成bad的命令（前提是保证三副本，只有一个副本损坏）

### 物化视图刷新异常报警

表达式：

```sql
increase(starrocks_fe_mv_refresh_total_failed_jobs[5m]) > 0
```

报警描述：

最近5分钟新增物化视图刷新失败次数超过1

处理办法：

1. 失败的是哪些物化视图

```sql
select TABLE_NAME,IS_ACTIVE,INACTIVE_REASON,TASK_NAME from information_schema.materialized_views where LAST_REFRESH_STATE !=" SUCCESS"
```

2. 可先尝试手动刷新一次

```sql
REFRESH MATERIALIZED VIEW ${mv_name};
```

3. 如果物化视图状态为INACTIVE，可通过如下方式尝试置为ACTIVE

```sql
ALTER MATERIALIZED VIEW ${mv_name} ACTIVE;
```

4. 排查失败的原因

```sql
SELECT * FROM information_schema.task_runs WHERE task_name ='mv-112517' \G
```