# 参数配置

服务启动之后，在运行的过程中可能会出现调整配置参数的情况，以满足业务需求。修改BE，FE的配置项以后，都需要重启使其生效

## FE配置项

|配置项|默认值|作用|
|---|---|---|
|log_roll_size_mb|1024|日志拆分的大小，每1G拆分一个日志|
|sys_log_dir|StarRocksFe.STARROCKS_HOME_DIR/log|日志保留的目录|
|sys_log_level|INFO|日志级别，INFO < WARNING < ERROR < FATAL|sys_log_roll_num|10|日志保留的数目|
|sys_log_verbose_modules|空字符串|日志打印的模块，写org.apache.starrocks.catalog就只打印catalog模块下的日志|
|sys_log_roll_interval|DAY|日志拆分的时间间隔|
|sys_log_delete_age|7d|日志删除的间隔|
|sys_log_roll_mode|1024|日志拆分的大小，每1G拆分一个日志|
|audit_log_dir|starrocksFe.STARROCKS_HOME_DIR/log|审计日志保留的目录|
|audit_log_roll_num|90|审计日志保留的数目|
|audit_log_modules|"slow_query", "query"|审计日志打印的模块，默认保留slow_query和query|
|qe_slow_log_ms|5000|Slow query的认定时长，默认5000ms|
|audit_log_roll_interval|DAY|审计日志拆分的时间间隔|
|audit_log_delete_age|30d|审计日志删除的间隔|
|audit_log_roll_mode|TIME-DAY|审计日志拆分的时间间隔|
|label_keep_max_second|259200|label保留时长，默认3天，保留太久会消耗很多内存|
|history_job_keep_max_second|604800|历史任务最大的保留时长，例如schema change任务，默认7天|
|label_clean_interval_second|14400|label清理的间隔|
|transaction_clean_interval_second|30|transaction清理的间隔|
|meta_dir|StarRocksFe.STARROCKS_HOME_DIR/meta|元数据保留目录|
|tmp_dir|starrocksFe.STARROCKS_HOME_DIR/temp_ddir|临时文件保存目录，例如backup/restore等进程保留的目录|
|edit_log_port|9010|FE Group(Master, Follower, Observer)之间通信用的端口|
|edit_log_roll_num|50000|Image日志拆分大小|
|meta_delay_toleration_second|300|非master节点容忍的最大元数据落后的时间|
|master_sync_policy|SYNC|Master日志刷盘的方式，默认是SYNC|
|replica_sync_policy|SYNC|Follower日志刷盘的方式，默认是SYNC|
|replica_ack_policy|SIMPLE_MAJORITY|日志被认为有效的形式，默认是多数派返回确认消息，就认为生效|
|bdbje_heartbeat_timeout_second|30|BDBJE心跳超时的间隔|
|bdbje_lock_timeout_second|1|BDBJE锁超时的间隔|
|txn_rollback_limit|100|事务回滚的上限|
|frontend_address|0.0.0.0|FE IP地址|
|priority_networks|空字符串|以CIDR形式10.10.10.0/24指定BE IP地址，适用于机器有多个IP，需要特殊指定的形式|
|metadata_failure_recovery|false|强制重置FE的元数据，慎用|
|ignore_meta_check|false|忽略元数据落后的情形|
|max_bdbje_clock_delta_ms|5000|Master与Non-master最大容忍的时钟偏移|
|http_port|8030|Http Server的端口|
|http_backlog_num|1024|HttpServer port backlog|
|thrift_backlog_num|1024|ThriftServer port backlog|
|rpc_port|9020|FE 上的 thrift server 端口|
|query_port|9030|FE 上的 mysql server 端口|
|mysql_service_nio_enabled|false|FE 连接服务的nio是否开启|
|mysql_service_io_threads_num|false|FE 连接服务线程数|
|auth_token|空字符串|Token是否自动开启|
|tablet_create_timeout_second|1|建表超时时长|
|max_create_table_timeout_second|60|建表最大超时时间|
|publish_version_timeout_second|30|版本生效的超时时间|
|publish_version_interval_ms|10|发送版本生效任务的时间间隔|
|load_straggler_wait_second|300|控制BE副本最大容忍的导入落后时长，超过这个时长就进行克隆|
|max_layout_length_per_row|100000|单行最大的长度，100KB|
|load_checker_interval_second|5|导入轮询的间隔|
|broker_load_default_timeout_second|14400|Broker Load的超时时间，默认4个小时|
|mini_load_default_timeout_second|3600|小批量导入的超时时间，默认1小时|
|insert_load_default_timeout_second|3600|Insert Into语句的超时时间，默认1小时|
|stream_load_default_timeout_second|600|StreamLoad超时时间，默认10分钟|
|max_load_timeout_second|259200|适用于所有导入，最大超时，3天|
|min_load_timeout_second|1|适用于所有导入，最小超时，1秒|
|desired_max_waiting_jobs|100|最多等待的任务，适用于所有的任务，建表、导入、schema change|
|max_running_txn_num_per_db|100|并发导入的任务数|
|async_load_task_pool_size|10|导入任务执行的线程池大小|
|tablet_delete_timeout_second|2|删除表的超时时间|
|capacity_used_percent_high_water|0.75|Backend上磁盘使用容量的度量值，超过0.75之后，尽量不在往这个tablet上发送建表，克隆的任务，直到恢复正常|
|alter_table_timeout_second|86400|Schema change超时时间|
|max_backend_down_time_second|3600|BE和FE失联之后，FE容忍BE重新加回来的最长时间|
|storage_cooldown_second|2592000|介质迁移的时间，默认30天|
|catalog_trash_expire_second|86400|删表/数据库之后，元数据在回收站中保留的时长，超过这个时长，数据就不可以在恢复|
|min_bytes_per_broker_scanner|67108864|单个实例处理的最小数据量，默认64MB|
|max_broker_concurrency|100|单个任务最大并发实例数，默认100个|
|load_parallel_instance_num|1|单个BE上并发实例数，默认1个|
|export_checker_interval_second|5|导出线程轮询间隔|
|export_running_job_num_limit|5|导出作业最大的运行数目|
|export_task_default_timeout_second|7200|导出作业超时时长，默认2小时|
|export_max_bytes_per_be_per_task|268435456|单个导出任务在单个be上导出的最大数据量，默认256M|
|export_task_pool_size|5|导出任务线程池大小，默认5个|
|consistency_check_start_time|23|FE发起副本一致性检测的起始时间|
|consistency_check_end_time|4|FE发起副本一致性检测的终止时间|
|check_consistency_default_timeout_second|600|副本一致性检测的超时时间|
|qe_max_connection|1024|FE上最多接收的连接数，适用于所有用户|
|max_conn_per_user|100|单个用户能够处理的最大连接数|
|query_colocate_join_memory_limit_penalty_factor|8|Colocte Join的内存现在|
|disable_colocate_join|false|不开启Colocate Join|
|expr_children_limit|10000|查询中in中可以涉及的数目|
|expr_depth_limit|3000|查询嵌套的层次|
|locale|zh_CN.UTF-8|字符集|
|remote_fragment_exec_timeout_ms|5000|FE发送查询规划的RPC超时时间，不涉及任务执行|
|max_query_retry_time|2|FE上查询重试的次数|
|catalog_try_lock_timeout_ms|5000|Catalog Lock获取的超时时长|
|disable_load_job|false|不接收任何导入任务，集群出问题时的止损措施|
|es_state_sync_interval_second|10|FE获取Elastic Search Index的时间|
|tablet_repair_delay_factor_second|60|FE控制进行副本修复的间隔|
|max_routine_load_job_num|100|最大的routine load作业数|
|max_routine_load_task_concurrent_num|5|每个routine load作业最大并发执行的task数|
|max_routine_load_task_num_per_be|5|每个be最大并发执行的routine load task数，需要小于等于be的配置routine_load_thread_pool_size|
|max_routine_load_batch_size|524288000|每个routine load task导入的最大数据量，默认500M|
|routine_load_task_consume_second|3|每个routine load task消费数据的最大时间，默认为3s|
|routine_load_task_timeout_second|15|每个routine load task超时时间，默认15s|

## BE 配置项

|配置项|默认值|作用|
|---|---|---|
|be_port|9060|BE 上 thrift server 的端口，用于接收来自 FE 的请求|
|brpc_port|8060|BRPC的端口，可以查看BRPC的一些网络统计信息|
|brpc_num_threads|-1|BRPC的bthreads线程数量，-1表示和cpu核数一样|
|priority_networks|空字符串|以CIDR形式10.10.10.0/24指定BE IP地址，适用于机器有多个IP，需要指定优先使用的网络|
|heartbeat_service_port|9050|心跳服务端口（thrift），用户接收来自 FE 的心跳|
|heartbeat_service_thread_count|1|心跳线程数|
|create_tablet_worker_count|3|创建tablet的线程数|
|drop_tablet_worker_count|3|删除tablet的线程数|
|push_worker_count_normal_priority|3|导入线程数，处理NORMAL优先级任务|
|push_worker_count_high_priority|3|导入线程数，处理HIGH优先级任务|
|publish_version_worker_count|2|生效版本的线程数|
|clear_transaction_task_worker_count|1|清理事务的线程数|
|alter_tablet_worker_count|3|进行schema change的线程数|
|clone_worker_count|3|克隆的线程数|
|storage_medium_migrate_count|1|介质迁移的线程数，SATA 迁移到 SSD|
|check_consistency_worker_count|1|计算tablet的校验和(checksum)|
|report_task_interval_seconds|10|汇报单个任务的间隔。建表，删除表，导入，schema change都可以被认定是任务|
|report_disk_state_interval_seconds|60|汇报磁盘状态的间隔。汇报各个磁盘的状态，以及上面的数据量等等|
|report_tablet_interval_seconds|60|汇报tablet的间隔。汇报所有的tablet的最新版本|
|alter_tablet_timeout_seconds|86400|Schema change超时时间|
|sys_log_dir|${STARROCKS_HOME}/log|存放日志的地方，包括INFO, WARNING, ERROR, FATAL等日志|
|user_function_dir|${STARROKCS_HOME}/lib/udf|UDF程序存放的地方|
|sys_log_level|INFO|日志级别，INFO < WARNING < ERROR < FATAL|
|sys_log_roll_mode|SIZE-MB-1024|日志拆分的大小，每1G拆分一个日志|
|sys_log_roll_num|10|日志保留的数目|
|sys_log_verbose_modules|*|日志打印的模块，写olap就只打印olap模块下的日志|
|sys_log_verbose_level|10|日志显示的级别，用于控制代码中VLOG开头的日志输出|
|log_buffer_level|空字符串|日志刷盘的策略，默认保持在内存中|
|num_threads_per_core|3|每个CPU core启动的线程数|
|compress_rowbatches|true|BE之间rpc通信是否压缩RowBatch，用于查询层之间的数据传输|
|serialize_batch|false|BE之间rpc通信是否序列化RowBatch，用于查询层之间的数据传输|
|status_report_interval|5|查询汇报profile的间隔，用于FE收集查询统计信息|
|doris_scanner_thread_pool_thread_num|48|存储引擎并发扫描磁盘的线程数，统一管理在线程池中|
|doris_scanner_thread_pool_queue_size|102400|存储引擎最多接收的任务数|
|doris_scan_range_row_count|524288|存储引擎拆分查询任务的粒度，512K|
|doris_scanner_queue_size|1024|存储引擎支持的扫描任务数|
|doris_scanner_row_num|16384|每个扫描线程单次执行最多返回的数据行数|
|doris_max_scan_key_num|1024|查询最多拆分的scan key数目|
|column_dictionary_key_ratio_threshold|0|字符串类型的取值比例，小于这个比例采用字典压缩算法|
|column_dictionary_key_size_threshold|0|字典压缩列大小，小于这个值采用字典压缩算法|
|memory_limitation_per_thread_for_schema_change|2|单个schema change任务允许占用的最大内存|
|max_unpacked_row_block_size|104857600|单个block最大的字节数，100MB|
|file_descriptor_cache_clean_interval|3600|文件句柄缓存清理的间隔，用于清理长期不用的文件句柄|
|disk_stat_monitor_interval|5|磁盘状态检测的间隔|
|unused_rowset_monitor_interval|30|清理过期Rowset的时间间隔|
|storage_root_path|空字符串|存储数据的目录|
|max_percentage_of_error_disk|0|磁盘错误达到一定比例，BE退出|
|default_num_rows_per_data_block|1024|每个block的数据行数|
|max_tablet_num_per_shard|1024|每个shard的tablet数目，用于划分tablet，防止单个目录下tablet子目录过多|
|pending_data_expire_time_sec|1800|存储引擎保留的未生效数据的最大时长|
|inc_rowset_expired_sec|1800|导入生效的数据，存储引擎保留的时间，用于增量克隆|
|max_garbage_sweep_interval|3600|磁盘进行垃圾清理的最大间隔|
|min_garbage_sweep_interval|180|磁盘进行垃圾清理的最小间隔|
|snapshot_expire_time_sec|172800|快照文件清理的间隔，48个小时|
|trash_file_expire_time_sec|259200|回收站清理的间隔，72个小时|
|row_nums_check|true|Compaction完成之后，前后Rowset行数对比|
|file_descriptor_cache_capacity|32768|文件句柄缓存的容量|
|min_file_descriptor_number|60000|BE进程的文件句柄limit要求的下线|
|index_stream_cache_capacity|10737418240|BloomFilter/Min/Max等统计信息缓存的容量|
|storage_page_cache_limit|20G|PageCache的容量|
|disable_storage_page_cache|false|是否开启PageCache|
|base_compaction_start_hour|20|BaseCompaction开启的时间|
|base_compaction_end_hour|7|BaseCompaction结束的时间|
|base_compaction_check_interval_seconds|60|BaseCompaction线程轮询的间隔|
|base_compaction_num_cumulative_deltas|5|BaseCompaction触发条件之一：Cumulative文件数目要达到的限制|
|base_compaction_num_threads_per_disk|1|每个磁盘BaseCompaction线程的数目|
|base_cumulative_delta_ratio|0.3|BaseCompaction触发条件之一：Cumulative文件大小达到Base文件的比例|
|base_compaction_interval_seconds_since_last_operation|86400|BaseCompaction触发条件之一：上一轮BaseCompaction距今的间隔|
|base_compaction_write_mbytes_per_sec|5|BaseCompaction写磁盘的限速|
|cumulative_compaction_check_interval_seconds|10|CumulativeCompaction线程轮询的间隔|
|min_cumulative_compaction_num_singleton_deltas|5|CumulativeCompaction触发条件之一：Singleton文件数目要达到的下限|
|max_cumulative_compaction_num_singleton_deltas|1000|CumulativeCompaction触发条件之一：Singleton文件数目要达到的上限|
|cumulative_compaction_num_threads_per_disk|1|每个磁盘CumulativeCompaction线程的数目|
|cumulative_compaction_budgeted_bytes|104857600|BaseCompaction触发条件之一：Singleton文件大小的总和限制，100MB|
|cumulative_compaction_write_mbytes_per_sec|100|CumulativeCompaction写磁盘的限速|
|min_compaction_failure_interval_sec|600|Tablet Compaction失败之后，再次被调度的间隔。|
|max_compaction_concurrency|-1|BaseCompaction + CumulativeCompaction的最大并发，-1就是没有限制|
|webserver_port|8040|Http Server端口|
|webserver_num_workers|5|Http Server线程数|
|periodic_counter_update_period_ms|500|Counter统计信息的间隔|
|load_data_reserve_hours|4|小批量导入生成的文件保留的时间|
|load_error_log_reserve_hours|48|导入数据信息保留的时长|
|number_tablet_writer_threads|16|流式导入的线程数|
|streaming_load_max_mb|10240|流式导入单个文件大小的上限|
|streaming_load_rpc_max_alive_time_sec|1200|流式导入RPC的超时时间|
|tablet_writer_rpc_timeout_sec|600|TabletWriter的超时时长|
|fragment_pool_thread_num|64|查询线程数，默认启动64个线程，后续查询请求动态创建线程|
|fragment_pool_queue_size|1024|单节点上能够处理的查询请求上限|
|enable_partitioned_hash_join|false|使用PartitionHashJoin|
|enable_partitioned_aggregation|true|使用PartitionAggregation|
|enable_token_check|true|Token开启检验|
|enable_prefetch|true|查询提前预取|
|load_process_max_memory_limit_bytes|107374182400|单节点上所有的导入线程占据的内存上限，100GB|
|load_process_max_memory_limit_percent|80|单节点上所有的导入线程占据的内存上限比例，100GB|
|sync_tablet_meta|false|存储引擎是否开sync保留到磁盘上。|
|thrift_rpc_timeout_ms|5000|Thrift超时的时长|
|txn_commit_rpc_timeout_ms|10000|Txn超时的时长|
|routine_load_thread_pool_size|10|例行导入的线程池数目|
|tablet_meta_checkpoint_min_new_rowsets_num|10|TabletMeta Checkpoint的最小Rowset数目|
|tablet_meta_checkpoint_min_interval_secs|600|TabletMeta Checkpoint线程轮询的时间间隔|
|default_rowset_type|ALPHA|存储引擎的格式，默认新ALPHA，后面会替换成BETA|
|brpc_max_body_size|209715200|BRPC最大的包容量，200MB|
|max_runnings_transactions|2000|存储引擎支持的最大事务数|
|tablet_map_shard_size|1|Tablet分组数|
|enable_bitmap_union_disk_format_with_set | False |Bitmap新存储格式，可以优化bitmap_union性能|
  
## Broker配置项

参考 [Broker load导入](../loading/BrokerLoad.md)

## 系统参数

Linux Kernel

建议3.10以上的内核。

CPU

scaling governor用于控制CPU的能耗模式，默认是on-demand模式，使用performance能耗最高，性能也最好，StarRocks部署建议采用performance模式。

~~~shell
echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
~~~

内存

* **Overcommit**

不建议使用Overcommit，
建议把 cat /proc/sys/vm/overcommit_memory 设成  1。

~~~shell
echo 1 | sudo tee /proc/sys/vm/overcommit_memory
~~~

* **Huge Pages**

禁止transparent huge pages，这个会干扰内存分配器，导致性能下降。

~~~shell
echo 'madvise' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
~~~

* **Swappiness**

关闭交换区，消除交换内存到虚拟内存时对性能的扰动。

~~~shell
echo 0 | sudo tee /proc/sys/vm/swappiness
~~~

磁盘

* **SATA**

mq-deadline 调度算法会排序和合并 I/O 请求，适合SATA磁盘。

~~~shell
echo mq-deadline | sudo tee /sys/block/vdb/queue/scheduler
~~~

* **SSD/NVME**

kyber调度算法适用于latency低的设备，例如NVME/SSD

~~~shell
echo kyber | sudo tee /sys/block/vdb/queue/scheduler
~~~

如果系统不支持kyber，建议使用none调度算法

~~~shell
echo none | sudo tee /sys/block/vdb/queue/scheduler
~~~

网络

请至少使用10 GB网络，1GB网络也能工作，但是会导致达不到预期性能。可以使用iperf测试系统带宽，确认是否是10GB网络。

文件系统

建议使用Ext4文件系统，可用相关命令进行查看挂载类型。

~~~shell
df -Th
Filesystem     Type      Size  Used Avail Use% Mounted on
/dev/vdb1      ext4     1008G  903G   55G  95% /home/disk1
~~~

其他系统配置

* **tcp abort on overflow**

~~~shell
echo 1 | sudo tee /proc/sys/net/ipv4/tcp_abort_on_overflow
~~~

* **somaxconn**

~~~shell
echo 1024 | sudo tee /proc/sys/net/core/somaxconn
~~~

系统资源

系统资源是指系统所能使用资源上限，配置在/etc/security/limits.conf，包括文件句柄、最大进程数、最大使用内存等。

* **文件句柄**

在部署的机器上运行ulimit -n 65535，把文件句柄设为65535。如果ulimit值重新登录后失效，尝试修改 /etc/ssh/sshd_config 中的 `UsePAM yes` ，然后重启sshd服务即可。

高并发配置

如果集群负载的并发度较高，建议添加以下配置

~~~shell
echo 120000 > /proc/sys/kernel/threads-max
echo 60000  > /proc/sys/vm/max_map_count
echo 200000  > /proc/sys/kernel/pid_max
~~~

* **max user processes**

ulimit -u 40960
