// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/Config.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common;

import com.starrocks.StarRocksFE;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.Replica;

public class Config extends ConfigBase {

    /**
     * The max size of one sys log and audit log
     */
    @ConfField
    public static int log_roll_size_mb = 1024; // 1 GB

    /**
     * sys_log_dir:
     * This specifies FE log dir. FE will produce 2 kinds of log files:
     * fe.log:      all logs of FE process.
     * fe.warn.log  all WARNING and ERROR log of FE process.
     * <p>
     * sys_log_level:
     * INFO, WARNING, ERROR, FATAL
     * <p>
     * sys_log_roll_num:
     * Maximal FE log files to be kept within a sys_log_roll_interval.
     * default is 10, which means there will be at most 10 log files in a day
     * <p>
     * sys_log_verbose_modules:
     * Verbose modules. VERBOSE level is implemented by log4j DEBUG level.
     * eg:
     * sys_log_verbose_modules = com.starrocks.globalStateMgr
     * This will only print debug log of files in package com.starrocks.globalStateMgr and all its sub packages.
     * <p>
     * sys_log_roll_interval:
     * DAY:  log suffix is yyyyMMdd
     * HOUR: log suffix is yyyyMMddHH
     * <p>
     * sys_log_delete_age:
     * default is 7 days, if log's last modify time is 7 days ago, it will be deleted.
     * support format:
     * 7d      7 days
     * 10h     10 hours
     * 60m     60 mins
     * 120s    120 seconds
     */
    @ConfField
    public static String sys_log_dir = StarRocksFE.STARROCKS_HOME_DIR + "/log";
    @ConfField
    public static String sys_log_level = "INFO";
    @ConfField
    public static int sys_log_roll_num = 10;
    @ConfField
    public static String[] sys_log_verbose_modules = {};
    @ConfField
    public static String sys_log_roll_interval = "DAY";
    @ConfField
    public static String sys_log_delete_age = "7d";
    @Deprecated
    @ConfField
    public static String sys_log_roll_mode = "SIZE-MB-1024";
    /**
     * Log to file by default. set to `true` if you want to log to console
     */
    @ConfField
    public static boolean sys_log_to_console = false;

    /**
     * audit_log_dir:
     * This specifies FE audit log dir.
     * Audit log fe.audit.log contains all requests with related infos such as user, host, cost, status, etc.
     * <p>
     * audit_log_roll_num:
     * Maximal FE audit log files to be kept within an audit_log_roll_interval.
     * <p>
     * audit_log_modules:
     * Slow query contains all queries which cost exceed *qe_slow_log_ms*
     * <p>
     * qe_slow_log_ms:
     * If the response time of a query exceed this threshold, it will be recorded in audit log as slow_query.
     * <p>
     * audit_log_roll_interval:
     * DAY:  log suffix is yyyyMMdd
     * HOUR: log suffix is yyyyMMddHH
     * <p>
     * audit_log_delete_age:
     * default is 30 days, if log's last modify time is 30 days ago, it will be deleted.
     * support format:
     * 7d      7 days
     * 10h     10 hours
     * 60m     60 mins
     * 120s    120 seconds
     */
    @ConfField
    public static String audit_log_dir = StarRocksFE.STARROCKS_HOME_DIR + "/log";
    @ConfField
    public static int audit_log_roll_num = 90;
    @ConfField
    public static String[] audit_log_modules = {"slow_query", "query"};
    @ConfField(mutable = true)
    public static long qe_slow_log_ms = 5000;
    @ConfField
    public static String audit_log_roll_interval = "DAY";
    @ConfField
    public static String audit_log_delete_age = "30d";

    @ConfField(mutable = true)
    public static long slow_lock_threshold_ms = 3000L;

    @ConfField(mutable = true)
    public static long slow_lock_log_every_ms = 3000L;

    @ConfField
    public static String custom_config_dir = "/conf";

    /**
     * dump_log_dir:
     * This specifies FE dump log dir.
     * Dump log fe.dump.log contains all dump information which query has Exception
     * <p>
     * dump_log_roll_num:
     * Maximal FE log files to be kept within a dump_log_roll_interval.
     * <p>
     * dump_log_modules:
     * Dump information for an abnormal query.
     * <p>
     * dump_log_roll_interval:
     * DAY:  log suffix is yyyyMMdd
     * HOUR: log suffix is yyyyMMddHH
     * <p>
     * dump_log_delete_age:
     * default is 7 days, if log's last modify time is 7 days ago, it will be deleted.
     * support format:
     * 7d      7 days
     * 10h     10 hours
     * 60m     60 mins
     * 120s    120 seconds
     */
    @ConfField
    public static String dump_log_dir = StarRocksFE.STARROCKS_HOME_DIR + "/log";
    @ConfField
    public static int dump_log_roll_num = 10;
    @ConfField
    public static String[] dump_log_modules = {"query"};
    @ConfField
    public static String dump_log_roll_interval = "DAY";
    @ConfField
    public static String dump_log_delete_age = "7d";

    /**
     * big_query_log_dir:
     * This specifies FE big query log dir.
     * Dump log fe.big_query.log contains all information about big query.
     * The structure of each log record is very similar to the audit log.
     * If the cpu cost of a query exceeds big_query_log_cpu_second_threshold,
     * or scan rows exceeds big_query_log_scan_rows_threshold,
     * or scan bytes exceeds big_query_log_scan_bytes_threshold,
     * we will consider it as a big query.
     * These thresholds are defined by the user.
     * <p>
     * big_query_log_roll_num:
     * Maximal FE log files to be kept within a big_query_log_roll_interval.
     * <p>
     * big_query_log_modules:
     * Information for all big queries.
     * <p>
     * big_query_log_roll_interval:
     * DAY:  log suffix is yyyyMMdd
     * HOUR: log suffix is yyyyMMddHH
     * <p>
     * big_query_log_delete_age:
     * default is 7 days, if log's last modify time is 7 days ago, it will be deleted.
     * support format:
     * 7d      7 days
     * 10h     10 hours
     * 60m     60 mins
     * 120s    120 seconds
     */
    @ConfField
    public static String big_query_log_dir = StarRocksFE.STARROCKS_HOME_DIR + "/log";
    @ConfField
    public static int big_query_log_roll_num = 10;
    @ConfField
    public static String[] big_query_log_modules = {"query"};
    @ConfField
    public static String big_query_log_roll_interval = "DAY";
    @ConfField
    public static String big_query_log_delete_age = "7d";

    /**
     * Log the COSTS plan, if the query is cancelled due to a crash of the backend or RpcException.
     * It is only effective when enable_collect_query_detail_info is set to false, since the plan will be recorded
     * in the query detail when enable_collect_query_detail_info is true.
     */
    @ConfField(mutable = true)
    public static boolean log_plan_cancelled_by_crash_be = true;

    /**
     * In high-concurrency scenarios, the logging of register and unregister query ID can become a bottleneck.
     * In such cases, it is possible to disable this switch.
     */
    @ConfField(mutable = true)
    public static boolean log_register_and_unregister_query_id = true;

    /**
     * Used to limit the maximum number of partitions that can be created when creating a dynamic partition table,
     * to avoid creating too many partitions at one time.
     */
    @ConfField(mutable = true)
    public static int max_dynamic_partition_num = 500;

    /**
     * plugin_dir:
     * plugin install directory
     */
    @ConfField
    public static String plugin_dir = System.getenv("STARROCKS_HOME") + "/plugins";

    @ConfField(mutable = true)
    public static boolean plugin_enable = true;

    /**
     * Labels of finished or cancelled load jobs will be removed
     * 1. after *label_keep_max_second*
     * or
     * 2. jobs total num > *label_keep_max_num*
     * The removed labels can be reused.
     * Set a short time will lower the FE memory usage.
     * (Because all load jobs' info is kept in memory before being removed)
     */
    @ConfField(mutable = true)
    public static int label_keep_max_second = 3 * 24 * 3600; // 3 days

    /**
     * for load job managed by LoadManager, such as Insert, Broker load and Spark load.
     */
    @ConfField(mutable = true)
    public static int label_keep_max_num = 1000;

    /**
     * StreamLoadTasks hold by StreamLoadMgr will be cleaned
     */
    @ConfField(mutable = true)
    public static int stream_load_task_keep_max_num = 1000;

    /**
     * StreamLoadTasks of finished or cancelled can be removed
     * 1. after *stream_load_task_keep_max_second*
     * or
     * 2. tasks total num > *stream_load_task_keep_max_num*
     */
    @ConfField(mutable = true)
    public static int stream_load_task_keep_max_second = 3 * 24 * 3600; // 3 days

    /**
     * Load label cleaner will run every *label_clean_interval_second* to clean the outdated jobs.
     */
    @ConfField
    public static int label_clean_interval_second = 4 * 3600; // 4 hours

    /**
     * For Task framework do some background operation like cleanup Task/TaskRun.
     * It will run every *task_check_interval_second* to do background job.
     */
    @ConfField
    public static int task_check_interval_second = 4 * 3600; // 4 hours

    /**
     * for task set expire time
     */
    @ConfField(mutable = true)
    public static int task_ttl_second = 24 * 3600;         // 1 day

    /**
     * for task run set expire time
     */
    @ConfField(mutable = true)
    public static int task_runs_ttl_second = 24 * 3600;     // 1 day

    /**
     * max history task num kept
     */
    @ConfField(mutable = true)
    public static int task_runs_max_history_number = 10000;

    /**
     * The max keep time of some kind of jobs.
     * like schema change job and rollup job.
     */
    @ConfField(mutable = true)
    public static int history_job_keep_max_second = 7 * 24 * 3600; // 7 days

    /**
     * the transaction will be cleaned after transaction_clean_interval_second seconds if the transaction is visible or aborted
     * we should make this interval as short as possible and each clean cycle as soon as possible
     */
    @ConfField
    public static int transaction_clean_interval_second = 30;

    // Configurations for meta data durability
    /**
     * StarRocks metadata will be saved here.
     * The storage of this dir is highly recommended as to be:
     * 1. High write performance (SSD)
     * 2. Safe (RAID)
     */
    @ConfField
    public static String meta_dir = StarRocksFE.STARROCKS_HOME_DIR + "/meta";

    /**
     * temp dir is used to save intermediate results of some process, such as backup and restore process.
     * file in this dir will be cleaned after these process is finished.
     */
    @ConfField
    public static String tmp_dir = StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir";

    /**
     * Edit log type.
     * BDB: write log to bdbje
     * LOCAL: deprecated.
     */
    @ConfField
    public static String edit_log_type = "BDB";

    /**
     * bdbje port
     */
    @ConfField
    public static int edit_log_port = 9010;

    /**
     * Leader FE will save image every *edit_log_roll_num* meta journals.
     */
    @ConfField(mutable = true)
    public static int edit_log_roll_num = 50000;

    @ConfField(mutable = true)
    public static int edit_log_write_slow_log_threshold_ms = 2000;

    /**
     * whether ignore unknown log id
     * when fe rolls back to low version, there may be log id that low version fe can not recognise
     * if set to true, fe will ignore those id
     * or fe will exit
     */
    @ConfField(mutable = true)
    public static boolean ignore_unknown_log_id = false;

    /**
     * hdfs_read_buffer_size_kb for reading hdfs
     */
    @ConfField(mutable = true)
    public static int hdfs_read_buffer_size_kb = 8192;

    /**
     * hdfs_write_buffer_size_kb for writing hdfs
     */
    @ConfField(mutable = true)
    public static int hdfs_write_buffer_size_kb = 1024;

    /**
     * expire seconds for unused file system manager
     */
    @ConfField(mutable = true, aliases = {"hdfs_file_sytem_expire_seconds"})
    public static int hdfs_file_system_expire_seconds = 300;

    /**
     * Non-master FE will stop offering service
     * if metadata delay gap exceeds *meta_delay_toleration_second*
     */
    @ConfField(mutable = true)
    public static int meta_delay_toleration_second = 300;    // 5 min

    /**
     * Leader FE sync policy of bdbje.
     * If you only deploy one Follower FE, set this to 'SYNC'. If you deploy more than 3 Follower FE,
     * you can set this and the following 'replica_sync_policy' to WRITE_NO_SYNC.
     * more info, see:
     * <a href="http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html">SyncPolicy</a>
     */
    @ConfField
    public static String master_sync_policy = "SYNC"; // SYNC, NO_SYNC, WRITE_NO_SYNC

    /**
     * Follower FE sync policy of bdbje.
     */
    @ConfField
    public static String replica_sync_policy = "SYNC"; // SYNC, NO_SYNC, WRITE_NO_SYNC

    /**
     * Replica ack policy of bdbje.
     * more info, see:
     * <a href="http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html">ReplicaAckPolicy</a>
     */
    @ConfField
    public static String replica_ack_policy = "SIMPLE_MAJORITY"; // ALL, NONE, SIMPLE_MAJORITY

    /**
     * The heartbeat timeout of bdbje between master and follower.
     * the default is 30 seconds, which is same as default value in bdbje.
     * If the network is experiencing transient problems, of some unexpected long java GC annoying you,
     * you can try to increase this value to decrease the chances of false timeouts
     */
    @ConfField
    public static int bdbje_heartbeat_timeout_second = 30;

    /**
     * The timeout for bdbje replica ack
     */
    @ConfField
    public static int bdbje_replica_ack_timeout_second = 10;

    /**
     * The lock timeout of bdbje operation
     * If there are many LockTimeoutException in FE WARN log, you can try to increase this value
     */
    @ConfField
    public static int bdbje_lock_timeout_second = 1;

    /**
     * Set the maximum acceptable clock skew between non-leader FE to Leader FE host.
     * This value is checked whenever a non-leader FE establishes a connection to leader FE via BDBJE.
     * The connection is abandoned if the clock skew is larger than this value.
     */
    @ConfField
    public static long max_bdbje_clock_delta_ms = 5000; // 5s

    /**
     * bdb je log level
     * If you want to print all levels of logs, set to ALL
     */
    @ConfField
    public static String bdbje_log_level = "INFO";

    /**
     * bdb je cleaner thread number
     */
    @ConfField
    public static int bdbje_cleaner_threads = 1;

    /**
     * The cost of replaying the replication stream as compared to the cost of
     * performing a network restore, represented as a percentage.  Specifies
     * the relative cost of using a log file as the source of transactions to
     * replay on a replica as compared to using the file as part of a network
     * restore.  This parameter is used to determine whether a cleaned log file
     * that could be used to support replay should be removed because a network
     * restore would be more efficient.  The value is typically larger than
     * 100, to represent that replay is usually more expensive than network
     * restore for a given amount of log data due to the cost of replaying
     * transactions.  If the value is 0, then the parameter is disabled, and no
     * log files will be retained based on the relative costs of replay and
     * network restore.
     *
     * <p>Note that log files are always retained if they are known to be
     * needed to support replication for electable replicas that have been in
     * contact with the master within the REP_STREAM_TIMEOUT(default is 30min) period,
     * or by any replica currently performing replication. This parameter only
     * applies to the retention of additional files that might be useful to
     * secondary nodes that are out of contact, or to electable nodes that have
     * been out of contact for longer than REP_STREAM_TIMEOUT.</p>
     *
     * <p>To disable the retention of these additional files, set this
     * parameter to zero.</p>
     * <p>
     * If the bdb dir expands, set this param to 0.
     */
    @ConfField
    public static int bdbje_replay_cost_percent = 150;

    /**
     * For the version of 5.7, bdb-je will reserve unprotected files (which can be deleted safely) as much as possible,
     * this param (default is 0 in bdb-je, which means unlimited) controls the limit of reserved unprotected files.
     */
    @ConfField
    public static long bdbje_reserved_disk_size = 512L * 1024 * 1024;

    /**
     * the max txn number which bdbje can roll back when trying to rejoin the group
     */
    @ConfField
    public static int txn_rollback_limit = 100;

    /**
     * num of thread to handle heartbeat events in heartbeat_mgr.
     */
    @ConfField
    public static int heartbeat_mgr_threads_num = 8;

    /**
     * blocking queue size to store heartbeat task in heartbeat_mgr.
     */
    @ConfField
    public static int heartbeat_mgr_blocking_queue_size = 1024;

    /**
     * num of thread to handle profile processing
     */
    @ConfField
    public static int profile_process_threads_num = 2;

    /**
     * blocking queue size to store profile process task
     */
    @ConfField
    public static int profile_process_blocking_queue_size = profile_process_threads_num * 128;

    /**
     * max num of thread to handle agent task in agent task thread-pool.
     */
    @ConfField
    public static int max_agent_task_threads_num = 4096;

    /**
     * This config will decide whether to resend agent task when create_time for agent_task is set,
     * only when current_time - create_time > agent_task_resend_wait_time_ms can ReportHandler do resend agent task
     */
    @ConfField(mutable = true)
    public static long agent_task_resend_wait_time_ms = 5000;

    /**
     * If true, FE will reset bdbje replication group(that is, to remove all electable nodes' info)
     * and is supposed to start as Leader. After reset, this node will be the only member in the cluster,
     * and the others node should be rejoin to this cluster by `Alter system add/drop follower/observer 'xxx'`;
     * Use this configuration only when the leader cannot be successfully elected
     * (Because most of the follower data has been damaged).
     */
    @ConfField
    public static String bdbje_reset_election_group = "false";

    /**
     * If the bdb data is corrupted, and you want to start the cluster only with image, set this param to true
     */
    @ConfField
    public static boolean start_with_incomplete_meta = false;

    /**
     * If true, non-leader FE will ignore the metadata delay gap between Leader FE and its self,
     * even if the metadata delay gap exceeds *meta_delay_toleration_second*.
     * Non-leader FE will still offer read service.
     * <p>
     * This is helpful when you try to stop the Leader FE for a relatively long time for some reason,
     * but still wish the non-leader FE can offer read service.
     */
    @ConfField(mutable = true)
    public static boolean ignore_meta_check = false;

    /**
     * Specified an IP for frontend, instead of the ip get by *InetAddress.getByName*.
     * This can be used when *InetAddress.getByName* get an unexpected IP address.
     * Default is "0.0.0.0", which means not set.
     * CAN NOT set this as a hostname, only IP.
     */
    @ConfField
    public static String frontend_address = "0.0.0.0";

    /**
     * Declare a selection strategy for those servers have many ips.
     * Note that there should at most one ip match this list.
     * this is a list in semicolon-delimited format, in CIDR notation, e.g. 10.10.10.0/24
     * If no ip match this rule, will choose one randomly.
     */
    @ConfField
    public static String priority_networks = "";

    /**
     * Fe http port
     * Currently, all FEs' http port must be same.
     */
    @ConfField
    public static int http_port = 8030;

    /**
     * Number of worker threads for http server to deal with http requests which may do
     * some I/O operations. If set with a non-positive value, it will use netty's default
     * value <code>DEFAULT_EVENT_LOOP_THREADS</code> which is availableProcessors * 2. The
     * default value is 0 which is same as the previous behaviour.
     * See <a href="https://github.com/netty/netty/blob/netty-4.1.16.Final/transport/src/main/java/io/netty/channel/MultithreadEventLoopGroup.java#L40">DEFAULT_EVENT_LOOP_THREADS</a>
     * for details.
     */
    @ConfField
    public static int http_worker_threads_num = 0;

    /**
     * The backlog_num for netty http server
     * When you enlarge this backlog_num, you should ensure its value larger than
     * the linux /proc/sys/net/core/somaxconn config
     */
    @ConfField
    public static int http_backlog_num = 1024;

    @ConfField
    public static int http_max_initial_line_length = 4096;

    @ConfField
    public static int http_max_header_size = 32768;

    @ConfField
    public static int http_max_chunk_size = 8192;

    /**
     * If a request takes longer than the configured time, a log will be generated to trace it.
     */
    @ConfField(mutable = true)
    public static int http_slow_request_threshold_ms = 5000;

    /**
     * When obtaining hardware information, some sensitive commands will be executed indirectly through
     * the oshi library, such as: getent passwd
     * If you have higher requirements for security, you can turn off the acquisition of this information
     */
    @ConfField(mutable = true)
    public static boolean http_web_page_display_hardware = true;

    /**
     * Whether to enable the detail metrics for http. It may be expensive
     * to get those metrics, and only enable it for debug in general.
     */
    @ConfField(mutable = true)
    public static boolean enable_http_detail_metrics = false;

    /**
     * Cluster name will be shown as the title of web page
     */
    @ConfField
    public static String cluster_name = "StarRocks Cluster";

    /**
     * FE thrift server port
     */
    @ConfField
    public static int rpc_port = 9020;

    /**
     * The connection timeout and socket timeout config for thrift server
     * The value for thrift_client_timeout_ms is set to be larger than zero to prevent
     * some hang up problems in java.net.SocketInputStream.socketRead0
     */
    @ConfField
    public static int thrift_client_timeout_ms = 5000;

    /**
     * The backlog_num for thrift server
     * When you enlarge this backlog_num, you should ensure its value larger than
     * the linux /proc/sys/net/core/somaxconn config
     */
    @ConfField
    public static int thrift_backlog_num = 1024;

    /**
     * the timeout for thrift rpc call
     */
    @ConfField(mutable = true)
    public static int thrift_rpc_timeout_ms = 10000;

    /**
     * the retry times for thrift rpc call
     */
    @ConfField(mutable = true)
    public static int thrift_rpc_retry_times = 3;

    @ConfField
    public static boolean thrift_rpc_strict_mode = true;

    // thrift rpc max body limit size. -1 means unlimited
    @ConfField
    public static int thrift_rpc_max_body_size = -1;

    // May be necessary to modify the following BRPC configurations in high concurrency scenarios.

    // The size of BRPC connection pool. It will limit the concurrency of sending requests, because
    // each request must borrow a connection from the pool.
    @ConfField
    public static int brpc_connection_pool_size = 16;

    // BRPC idle wait time (ms)
    @ConfField
    public static int brpc_idle_wait_max_time = 10000;

    /**
     * FE mysql server port
     */
    @ConfField
    public static int query_port = 9030;

    /**
     * The backlog_num for mysql nio server
     * When you enlarge this backlog_num, you should ensure its value larger than
     * the linux /proc/sys/net/core/somaxconn config
     */
    @ConfField
    public static int mysql_nio_backlog_num = 1024;

    /**
     * mysql service nio option.
     */
    @ConfField
    public static boolean mysql_service_nio_enabled = true;

    /**
     * num of thread to handle io events in mysql.
     */
    @ConfField
    public static int mysql_service_io_threads_num = 4;

    /**
     * max num of thread to handle task in mysql.
     */
    @ConfField
    public static int max_mysql_service_task_threads_num = 4096;

    /**
     * max num of thread to handle task for http sql.
     */
    @ConfField
    public static int max_http_sql_service_task_threads_num = 4096;

    /**
     * modifies the version string returned by following situations:
     * select version();
     * handshake packet version.
     * global variable version.
     */
    @ConfField(mutable = true)
    public static String mysql_server_version = "5.1.0";

    /**
     * node(FE or BE) will be considered belonging to the same StarRocks cluster if they have same cluster id.
     * Cluster id is usually a random integer generated when master FE start at first time.
     * You can also specify one.
     */
    @ConfField
    public static int cluster_id = -1;

    /**
     * If a backend is down for *max_backend_down_time_second*, a BACKEND_DOWN event will be triggered.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true)
    public static int max_backend_down_time_second = 3600; // 1h

    /**
     * If set to true, the backend will be automatically dropped after finishing decommission.
     * If set to false, the backend will not be dropped and remaining in DECOMMISSION state.
     */
    @ConfField(mutable = true)
    public static boolean drop_backend_after_decommission = true;

    // Configurations for load, clone, create table, alter table etc. We will rarely change them
    /**
     * Maximal waiting time for creating a single replica.
     * e.g.
     * if you create a table with #m tablets and #n replicas for each tablet,
     * the create table request will run at most (m * n * tablet_create_timeout_second) before timeout.
     */
    @ConfField(mutable = true)
    public static int tablet_create_timeout_second = 10;

    /**
     * minimal intervals between two publish version action
     */
    @ConfField
    public static int publish_version_interval_ms = 10;

    @ConfField(mutable = true)
    public static boolean lake_enable_batch_publish_version = false;

    @ConfField(mutable = true)
    public static int lake_batch_publish_max_version_num = 10;

    @ConfField(mutable = true)
    public static int lake_batch_publish_min_version_num = 1;

    /**
     * The thrift server max worker threads
     */
    @ConfField(mutable = true)
    public static int thrift_server_max_worker_threads = 4096;

    /**
     * If there is no thread to handle new request, the request will be pend to a queue,
     * the pending queue size is thrift_server_queue_size
     */
    @ConfField
    public static int thrift_server_queue_size = 4096;

    /**
     * Maximal wait seconds for straggler node in load
     * eg.
     * there are 3 replicas A, B, C
     * load is already quorum finished(A,B) at t1 and C is not finished
     * if (current_time - t1) > 300s, then StarRocks will treat C as a failure node
     * will call transaction manager to commit the transaction and tell transaction manager
     * that C is failed
     * <p>
     * This is also used when waiting for publish tasks
     * <p>
     * TODO this parameter is the default value for all job and the DBA could specify it for separate job
     */
    @ConfField(mutable = true)
    public static int load_straggler_wait_second = 300;

    /**
     * The load scheduler running interval.
     * A load job will transfer its state from PENDING to LOADING to FINISHED.
     * The load scheduler will transfer load job from PENDING to LOADING
     * while the txn callback will transfer load job from LOADING to FINISHED.
     * So a load job will cost at most one interval to finish when the concurrency has not reached the upper limit.
     */
    @ConfField
    public static int load_checker_interval_second = 5;

    @ConfField(mutable = true)
    public static long lock_checker_interval_second = 30;

    /**
     * Check of deadlock is time consuming. Open it only when tracking problems.
     */
    @ConfField(mutable = true)
    public static boolean lock_checker_enable_deadlock_check = false;

    /**
     * Default broker load timeout
     */
    @ConfField(mutable = true)
    public static int broker_load_default_timeout_second = 14400; // 4 hour

    /**
     * Default timeout of spark submit task and wait for yarn response
     */
    @ConfField
    public static long spark_load_submit_timeout_second = 300; // 5min

    /**
     * Maximal bytes that a single broker scanner will read.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true)
    public static long min_bytes_per_broker_scanner = 67108864L; // 64MB

    /**
     * Default insert load timeout
     */
    @ConfField(mutable = true)
    public static int insert_load_default_timeout_second = 3600; // 1 hour

    /**
     * Default stream load and streaming mini load timeout
     */
    @ConfField(mutable = true)
    public static int stream_load_default_timeout_second = 600; // 600s

    /**
     * Max stream load and streaming mini load timeout
     */
    @ConfField(mutable = true)
    public static int max_stream_load_timeout_second = 259200; // 3days

    /**
     * Max stream load load batch size
     */
    @ConfField(mutable = true)
    public static int max_stream_load_batch_size_mb = 100;

    /**
     * Stream load max txn num per BE, less than 0 means no limit
     */
    @ConfField(mutable = true)
    public static int stream_load_max_txn_num_per_be = -1;

    /**
     * Default prepared transaction timeout
     */
    @ConfField(mutable = true)
    public static int prepared_transaction_default_timeout_second = 86400; // 1day

    /**
     * Max load timeout applicable to all type of load except for stream load and lake compaction
     */
    @ConfField(mutable = true)
    public static int max_load_timeout_second = 259200; // 3days

    /**
     * Min stream load timeout applicable to all type of load
     */
    @ConfField(mutable = true)
    public static int min_load_timeout_second = 1; // 1s

    // Configurations for spark load
    /**
     * Default spark dpp version
     */
    @ConfField
    public static String spark_dpp_version = "1.0.0";
    /**
     * Default spark load timeout
     */
    @ConfField(mutable = true)
    public static int spark_load_default_timeout_second = 86400; // 1 day

    /**
     * Default spark home dir
     */
    @ConfField
    public static String spark_home_default_dir = StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x";

    /**
     * Default spark dependencies path
     */
    @ConfField
    public static String spark_resource_path = "";

    /**
     * The specified spark launcher log dir
     */
    @ConfField
    public static String spark_launcher_log_dir = sys_log_dir + "/spark_launcher_log";

    /**
     * Default yarn client path
     */
    @ConfField
    public static String yarn_client_path = StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn";

    /**
     * Default yarn config file directory
     * Each time before running the yarn command, we need to check that the
     * config file exists under this path, and if not, create them.
     */
    @ConfField
    public static String yarn_config_dir = StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config";

    /**
     * Default number of waiting jobs for routine load and version 2 of load
     * This is a desired number.
     * In some situation, such as switch the master, the current number is maybe more than desired_max_waiting_jobs
     */
    @ConfField(mutable = true)
    public static int desired_max_waiting_jobs = 1024;

    /**
     * maximum concurrent running txn num including prepare, commit txns under a single db
     * txn manager will reject coming txns
     */
    @ConfField(mutable = true)
    public static int max_running_txn_num_per_db = 1000;

    /**
     * The load task executor pool size. This pool size limits the max running load tasks.
     * Currently, it only limits the load task of broker load, pending and loading phases.
     * It should be less than 'max_running_txn_num_per_db'
     */
    @ConfField(mutable = true, aliases = {"async_load_task_pool_size"})
    public static int max_broker_load_job_concurrency = 5;

    /**
     * Same meaning as *tablet_create_timeout_second*, but used when delete a tablet.
     */
    @ConfField(mutable = true)
    public static int tablet_delete_timeout_second = 2;
    /**
     * The high water of disk capacity used percent.
     * This is used for calculating load score of a backend.
     */
    @ConfField(mutable = true)
    public static double capacity_used_percent_high_water = 0.75;
    /**
     * Maximal timeout of ALTER TABLE request. Set long enough to fit your table data size.
     */
    @ConfField(mutable = true)
    public static int alter_table_timeout_second = 86400; // 1day

    /**
     * The alter handler max worker threads
     */
    @ConfField
    public static int alter_max_worker_threads = 4;

    /**
     * The alter handler max queue size for worker threads
     */
    @ConfField
    public static int alter_max_worker_queue_size = 4096;

    /**
     * If set to true, FE will check backend available capacity by storage medium when create table
     * <p>
     * The default value should better set to true because if user
     * has a deployment with only SSD or HDD medium storage paths,
     * create an incompatible table with cause balance problem(SSD tablet cannot move to HDD path, vice versa).
     * But currently for compatible reason, we keep it to false.
     */
    @ConfField(mutable = true)
    public static boolean enable_strict_storage_medium_check = false;

    /**
     * After dropping database(table/partition), you can recover it by using RECOVER stmt.
     * And this specifies the maximal data retention time. After time, the data will be deleted permanently.
     */
    @ConfField(mutable = true)
    public static long catalog_trash_expire_second = 86400L; // 1day

    /**
     * Parallel load fragment instance num in single host
     */
    @ConfField(mutable = true)
    public static int load_parallel_instance_num = 1;

    /**
     * Export checker's running interval.
     */
    @ConfField
    public static int export_checker_interval_second = 5;
    /**
     * Limitation of the concurrency of running export jobs.
     * Default is 5.
     * 0 is unlimited
     */
    @ConfField(mutable = true)
    public static int export_running_job_num_limit = 5;
    /**
     * Limitation of the pending TaskRun queue length.
     * Default is 500.
     */
    @ConfField(mutable = true)
    public static int task_runs_queue_length = 500;
    /**
     * Limitation of the running TaskRun.
     * Default is 4.
     */
    @ConfField(mutable = true)
    public static int task_runs_concurrency = 4;

    /**
     * max num of thread to handle task runs in task runs executor thread-pool.
     */
    @ConfField
    public static int max_task_runs_threads_num = 512;

    /**
     * Default timeout of export jobs.
     */
    @ConfField(mutable = true)
    public static int export_task_default_timeout_second = 2 * 3600; // 2h
    /**
     * Max bytes of data exported by each export task on each BE.
     * Used to split the export job for parallel processing.
     * Calculated according to the size of compressed data, default is 256M.
     */
    @ConfField(mutable = true)
    public static long export_max_bytes_per_be_per_task = 268435456; // 256M
    /**
     * Size of export task thread pool, default is 5.
     */
    @ConfField
    public static int export_task_pool_size = 5;

    // Configurations for consistency check
    /**
     * Consistency checker will run from *consistency_check_start_time* to *consistency_check_end_time*.
     * Default is from 23:00 to 04:00
     */
    @ConfField
    public static String consistency_check_start_time = "23";
    @ConfField
    public static String consistency_check_end_time = "4";
    /**
     * Default timeout of a single consistency check task. Set long enough to fit your tablet size.
     */
    @ConfField(mutable = true)
    public static long check_consistency_default_timeout_second = 600; // 10 min
    @ConfField(mutable = true)
    public static long consistency_tablet_meta_check_interval_ms = 2 * 3600 * 1000L; // every 2 hours

    // Configurations for query engine
    /**
     * Maximal number of connections per FE.
     */
    @ConfField
    public static int qe_max_connection = 1024;

    /**
     * Maximal number of thread in connection-scheduler-pool.
     */
    @ConfField
    public static int max_connection_scheduler_threads_num = 4096;

    /**
     * Used to limit element num of InPredicate in delete statement.
     */
    @ConfField(mutable = true)
    public static int max_allowed_in_element_num_of_delete = 10000;

    /**
     * control materialized view
     */
    @ConfField(mutable = true)
    public static boolean enable_materialized_view = true;

    /**
     * Control whether to enable spill for all materialized views in the refresh mv.
     */
    @ConfField(mutable = true)
    public static boolean enable_materialized_view_spill = true;

    /**
     * When the materialized view fails to start FE due to metadata problems,
     * you can try to open this configuration,
     * and he can ignore some metadata exceptions.
     */
    @ConfField(mutable = true)
    public static boolean ignore_materialized_view_error = false;

    /**
     * whether backup materialized views in backing databases. If not, will skip backing materialized views.
     */
    @ConfField(mutable = true)
    public static boolean enable_backup_materialized_view = true;

    /**
     * Whether to display all task runs or only the newest task run in ShowMaterializedViews command to be
     * compatible with old version.
     */
    @ConfField(mutable = true)
    public static boolean enable_show_materialized_views_include_all_task_runs = true;

    /**
     * The smaller schedule time is, the higher frequency TaskManager schedule which means
     * materialized view need to schedule to refresh.
     * <p>
     * When schedule time is too frequent and consumes a bit slow, FE will generate too much task runs and
     * may cost a lot of FE's memory, so add a config to control the min schedule time to avoid it.
     * </p>
     */
    @ConfField(mutable = true)
    public static int materialized_view_min_refresh_interval = 60; // 1 min

    /**
     * To avoid too many related materialized view causing too much fe memory and decreasing performance, set N
     * to determine which strategy you choose:
     *  N <0      : always use non lock optimization and no copy related materialized views which
     *      may cause metadata concurrency problem but can reduce many lock conflict time and metadata memory-copy consume.
     *  N = 0    : always not use non lock optimization
     *  N > 0    : use non lock optimization when related mvs's num <= N, otherwise don't use non lock optimization
     */
    @ConfField(mutable = true)
    public static int skip_whole_phase_lock_mv_limit = 5;

    @ConfField
    public static boolean enable_udf = false;

    @ConfField(mutable = true)
    public static boolean enable_decimal_v3 = true;

    @ConfField(mutable = true)
    public static boolean enable_sql_blacklist = false;

    /**
     * If set to true, dynamic partition feature will open
     */
    @ConfField(mutable = true)
    public static boolean dynamic_partition_enable = true;

    /**
     * Decide how often to check dynamic partition
     */
    @ConfField(mutable = true)
    public static long dynamic_partition_check_interval_seconds = 600;

    /**
     * If set to true, memory tracker feature will open
     */
    @ConfField(mutable = true)
    public static boolean memory_tracker_enable = true;

    /**
     * Decide how often to track the memory usage of the FE process
     */
    @ConfField(mutable = true)
    public static long memory_tracker_interval_seconds = 60;

    /**
     * If batch creation of partitions is allowed to create half of the partitions, it is easy to generate holes.
     * By default, this is not enabled. If it is turned on, the partitions built by batch creation syntax will
     * not allow partial creation.
     */
    @ConfField(mutable = true)
    public static boolean enable_create_partial_partition_in_batch = false;

    /**
     * The number of query retries.
     * A query may retry if we encounter RPC exception and no result has been sent to user.
     * You may reduce this number to avoid Avalanche disaster.
     */
    @ConfField(mutable = true)
    public static int max_query_retry_time = 2;

    /**
     * In order not to wait too long for create table(index), set a max timeout.
     */
    @ConfField(mutable = true)
    public static int max_create_table_timeout_second = 600;

    @ConfField(mutable = true, comment = "The maximum number of replicas to create serially." +
            "If actual replica count exceeds this, replicas will be created concurrently.")
    public static int create_table_max_serial_replicas = 128;

    // Configurations for backup and restore
    /**
     * Plugins' path for BACKUP and RESTORE operations. Currently, deprecated.
     */
    @Deprecated
    @ConfField
    public static String backup_plugin_path = "/tools/trans_file_tool/trans_files.sh";

    // default timeout of backup job
    @ConfField(mutable = true)
    public static int backup_job_default_timeout_ms = 86400 * 1000; // 1 day

    // Set runtime locale when exec some cmds
    @ConfField
    public static String locale = "zh_CN.UTF-8";

    /**
     * 'storage_usage_soft_limit_percent' limit the max capacity usage percent of a Backend storage path.
     * 'storage_usage_soft_limit_left_bytes' limit the minimum left capacity of a Backend storage path.
     * If both limitations are reached, this storage path can not be chosen as tablet balance destination.
     * But for tablet recovery, we may exceed these limit for keeping data integrity as much as possible.
     */
    @ConfField(mutable = true, aliases = {"storage_high_watermark_usage_percent"})
    public static int storage_usage_soft_limit_percent = 90;
    @ConfField(mutable = true, aliases = {"storage_min_left_capacity_bytes"})
    public static long storage_usage_soft_limit_reserve_bytes = 200L * 1024 * 1024 * 1024; // 200GB

    /**
     * If capacity of disk reach the 'storage_usage_hard_limit_percent' and 'storage_usage_hard_limit_reserve_bytes',
     * the following operation will be rejected:
     * 1. load job
     * 2. restore job
     */
    @ConfField(mutable = true, aliases = {"storage_flood_stage_usage_percent"})
    public static int storage_usage_hard_limit_percent = 95;
    @ConfField(mutable = true, aliases = {"storage_flood_stage_left_capacity_bytes"})
    public static long storage_usage_hard_limit_reserve_bytes = 100L * 1024L * 1024 * 1024; // 100GB

    /**
     * update interval of tablet stat
     * All frontends will get tablet stat from all backends at each interval
     */
    @ConfField
    public static int tablet_stat_update_interval_second = 300;  // 5 min

    /**
     * The tryLock timeout configuration of globalStateMgr lock.
     * Normally it does not need to change, unless you need to test something.
     */
    @ConfField(mutable = true)
    public static long catalog_try_lock_timeout_ms = 5000; // 5 sec

    /**
     * if this is set to true
     * all pending load job will fail when call begin txn api
     * all prepare load job will fail when call commit txn api
     * all committed load job will wait to be published
     */
    @ConfField(mutable = true)
    public static boolean disable_load_job = false;

    /*
     * One master daemon thread will update database used data quota for db txn manager
     * every db_used_data_quota_update_interval_secs
     */
    @ConfField
    public static int db_used_data_quota_update_interval_secs = 300;

    /**
     * Load using hadoop cluster will be deprecated in the future.
     * Set to true to disable this kind of load.
     */
    @ConfField(mutable = true)
    public static boolean disable_hadoop_load = false;

    /**
     * the default slot number per path in tablet scheduler
     * TODO(cmy): remove this config and dynamically adjust it by clone task statistic
     */
    @ConfField(mutable = true, aliases = {"schedule_slot_num_per_path"})
    public static int tablet_sched_slot_num_per_path = 8;

    // if the number of scheduled tablets in TabletScheduler exceed max_scheduling_tablets
    // skip checking.
    @ConfField(mutable = true, aliases = {"max_scheduling_tablets"})
    public static int tablet_sched_max_scheduling_tablets = 10000;

    /**
     * if set to true, TabletScheduler will not do balance.
     */
    @ConfField(mutable = true, aliases = {"disable_balance"})
    public static boolean tablet_sched_disable_balance = false;

    /**
     * The following configuration can set to true to disable the automatic colocate tables' relocation and balance.
     * if *disable_colocate_balance* is set to true, ColocateTableBalancer will not balance colocate tables.
     */
    @ConfField(mutable = true, aliases = {"disable_colocate_balance"})
    public static boolean tablet_sched_disable_colocate_balance = false;

    /**
     * When setting to true, disable the overall balance behavior for colocate groups which treats all the groups
     * in all databases as a whole and balances the replica distribution between all of them.
     * See `ColocateBalancer.relocateAndBalanceAllGroups` for more details.
     * Notice: set `tablet_sched_disable_colocate_balance` to true will disable all the colocate balance behavior,
     * including this behavior and the per-group balance behavior. This configuration is only to disable the overall
     * balance behavior.
     */
    @ConfField(mutable = true)
    public static boolean tablet_sched_disable_colocate_overall_balance = true;

    @ConfField(mutable = true)
    public static long[] tablet_sched_colocate_balance_high_prio_backends = {};

    @ConfField(mutable = true)
    public static boolean tablet_sched_always_force_decommission_replica = false;

    /**
     * For certain deployment, like k8s pods + pvc, the replica is not lost even the
     * corresponding backend is detected as dead, because the replica data is persisted
     * on a pvc which is backed by a remote storage service, such as AWS EBS. And later,
     * k8s control place will schedule a new pod and attach the pvc to it which will
     * restore the replica to a {@link Replica.ReplicaState#NORMAL} state immediately. But normally
     * the {@link com.starrocks.clone.TabletScheduler} of Starrocks will start to schedule
     * {@link LocalTablet.TabletStatus#REPLICA_MISSING} tasks and create new replicas in a short time.
     * After new pod scheduling is completed, {@link com.starrocks.clone.TabletScheduler} has
     * to delete the redundant healthy replica which cause resource waste and may also affect
     * the loading process.
     *
     * <p>When a backend is considered to be dead, this configuration specifies how long the
     * {@link com.starrocks.clone.TabletScheduler} should wait before starting to schedule
     * {@link LocalTablet.TabletStatus#REPLICA_MISSING} tasks. It is intended to leave some time for
     * the external scheduler like k8s to handle the repair process before internal scheduler kicks in
     * or for the system administrator to restart and put the backend online in time.
     * To be noticed, it only affects the dead backend situation, the scheduler
     * may still schedule {@link LocalTablet.TabletStatus#REPLICA_MISSING} tasks because of
     * other reasons, like manually setting a replica as bad, actively decommission a backend etc.
     *
     * <p>Currently this configuration only works for non-colocate tables, for colocate tables,
     * refer to {@link Config#tablet_sched_colocate_be_down_tolerate_time_s}.
     *
     * <p>For more discussion on this issue, see
     * <a href="https://github.com/StarRocks/starrocks-kubernetes-operator/issues/49">issue49</a>
     */
    @ConfField(mutable = true)
    public static long tablet_sched_be_down_tolerate_time_s = 900; // 15 min

    /**
     * If BE is down beyond this time, tablets on that BE of colocate table will be migrated to other available BEs
     */
    @ConfField(mutable = true)
    public static long tablet_sched_colocate_be_down_tolerate_time_s = 12L * 3600L;

    // if the number of balancing tablets in TabletScheduler exceed max_balancing_tablets,
    // no more balance check
    @ConfField(mutable = true, aliases = {"max_balancing_tablets"})
    public static int tablet_sched_max_balancing_tablets = 500;

    /**
     * When create a table(or partition), you can specify its storage medium(HDD or SSD).
     * If set to SSD, this specifies the default duration that tablets will stay on SSD.
     * After that, tablets will be moved to HDD automatically.
     * You can set storage cooldown time in CREATE TABLE stmt.
     */
    @ConfField(mutable = true, aliases = {"storage_cooldown_second"})
    public static long tablet_sched_storage_cooldown_second = -1L; // won't cool down by default

    /**
     * If the tablet in scheduler queue has not been scheduled for tablet_sched_max_not_being_scheduled_interval_ms,
     * its priority will upgrade.
     * default is 15min
     */
    @ConfField(mutable = true)
    public static long tablet_sched_max_not_being_scheduled_interval_ms = 15 * 60 * 1000;

    /**
     * FOR DiskAndTabletLoadBalancer:
     * upper limit of the difference in disk usage of all backends, exceeding this threshold will cause
     * disk balance
     */
    @ConfField(mutable = true, aliases = {"balance_load_score_threshold"})
    public static double tablet_sched_balance_load_score_threshold = 0.1; // 10%

    /**
     * For DiskAndTabletLoadBalancer:
     * if all backends disk usage is lower than this threshold, disk balance will never happen
     */
    @ConfField(mutable = true, aliases = {"balance_load_disk_safe_threshold"})
    public static double tablet_sched_balance_load_disk_safe_threshold = 0.5; // 50%

    /**
     * the factor of delay time before deciding to repair tablet.
     * if priority is VERY_HIGH, repair it immediately.
     * HIGH, delay tablet_repair_delay_factor_second * 1;
     * NORMAL: delay tablet_repair_delay_factor_second * 2;
     * LOW: delay tablet_repair_delay_factor_second * 3;
     */
    @ConfField(mutable = true, aliases = {"tablet_repair_delay_factor_second"})
    public static long tablet_sched_repair_delay_factor_second = 60;

    /**
     * min_clone_task_timeout_sec and max_clone_task_timeout_sec is to limit the
     * min and max timeout of a clone task.
     * Under normal circumstances, the timeout of a clone task is estimated by
     * the amount of data and the minimum transmission speed(5MB/s).
     * But in special cases, you may need to manually set these two configs
     * to ensure that the clone task will not fail due to timeout.
     */
    @ConfField(mutable = true, aliases = {"min_clone_task_timeout_sec"})
    public static long tablet_sched_min_clone_task_timeout_sec = 3 * 60L; // 3min
    @ConfField(mutable = true, aliases = {"max_clone_task_timeout_sec"})
    public static long tablet_sched_max_clone_task_timeout_sec = 2 * 60 * 60L; // 2h

    /**
     * tablet checker's check interval in seconds
     */
    @ConfField
    public static int tablet_sched_checker_interval_seconds = 20;

    @ConfField(mutable = true)
    public static int tablet_sched_max_migration_task_sent_once = 1000;

    @ConfField(mutable = true)
    public static long tablet_sched_consecutive_full_clone_delay_sec = 180; // 3min

    @ConfField(mutable = true, comment = "How much time we should wait before dropping the tablet from BE on tablet report")
    public static long tablet_report_drop_tablet_delay_sec = 120;

    /**
     * After checked tablet_checker_partition_batch_num partitions, db lock will be released,
     * so that other threads can get the lock.
     */
    @ConfField(mutable = true)
    public static int tablet_checker_partition_batch_num = 500;

    @Deprecated
    @ConfField(mutable = true)
    public static int report_queue_size = 100;

    /**
     * If set to true, metric collector will be run as a daemon timer to collect metrics at fix interval
     */
    @ConfField
    public static boolean enable_metric_calculator = true;

    /**
     * enable replicated storage as default table engine
     */
    @ConfField(mutable = true)
    public static boolean enable_replicated_storage_as_default_engine = true;

    /**
     * enable schedule insert query by row count
     */
    @ConfField(mutable = true)
    public static boolean enable_schedule_insert_query_by_row_count = true;

    /**
     * the max concurrent routine load task num of a single routine load job
     */
    @ConfField(mutable = true)
    public static int max_routine_load_task_concurrent_num = 5;

    /**
     * the max concurrent routine load task num per BE.
     * This is to limit the num of routine load tasks sending to a BE.
     */
    @ConfField(mutable = true)
    public static int max_routine_load_task_num_per_be = 16;

    /**
     * max load size for each routine load task
     */
    @ConfField(mutable = true)
    public static long max_routine_load_batch_size = 4294967296L; // 4GB

    /**
     * consume data time for each routine load task
     */
    @ConfField(mutable = true)
    public static long routine_load_task_consume_second = 15;

    /**
     * routine load task timeout
     * should bigger than 2 * routine_load_task_consume_second
     * but can not be less than 10s because when one be down the load time will be at least 10s
     */
    @ConfField(mutable = true)
    public static long routine_load_task_timeout_second = 60;

    /**
     * kafka util request timeout
     */
    @ConfField(mutable = true)
    public static long routine_load_kafka_timeout_second = 12;

    /**
     * pulsar util request timeout
     */
    @ConfField(mutable = true)
    public static long routine_load_pulsar_timeout_second = 12;

    /**
     * it can't auto-resume routine load job as long as one of the backends is down
     */
    @ConfField(mutable = true)
    public static int max_tolerable_backend_down_num = 0;

    /**
     * a period for auto resume routine load
     */
    @ConfField(mutable = true)
    public static int period_of_auto_resume_min = 5;

    /**
     * The max number of files store in SmallFileMgr
     */
    @ConfField(mutable = true)
    public static int max_small_file_number = 100;

    /**
     * The max size of a single file store in SmallFileMgr
     */
    @ConfField(mutable = true)
    public static int max_small_file_size_bytes = 1024 * 1024; // 1MB

    /**
     * Save small files
     */
    @ConfField
    public static String small_file_dir = StarRocksFE.STARROCKS_HOME_DIR + "/small_files";

    /**
     * control rollup job concurrent limit
     */
    @ConfField(mutable = true)
    public static int max_running_rollup_job_num_per_table = 1;

    /**
     * if set to false, auth check will be disabled, in case something goes wrong with the new privilege system.
     */
    @ConfField
    public static boolean enable_auth_check = true;

    /**
     * If set to false, auth check for StarRocks external table will be disabled. The check
     * only happens on the target cluster.
     */
    @ConfField(mutable = true)
    public static boolean enable_starrocks_external_table_auth_check = true;

    /**
     * If set to true, the granularity of auth check extends to the column level
     */
    @ConfField(mutable = true)
    public static boolean authorization_enable_column_level_privilege = false;

    /**
     * The authentication_chain configuration specifies the sequence of security integrations
     * that will be used to authenticate a user. Each security integration in the chain will be
     * tried in the order they are defined until one of them successfully authenticates the user.
     * The configuration should specify a list of names of the security integrations
     * that will be used in the chain.
     * <p>
     * For example, if user specifies the value with {"ldap", "native"}, SR will first try to authenticate
     * a user whose authentication info may exist in a ldap server, if failed, SR will continue trying to
     * authenticate the user to check whether it's a native user in SR, i.e. it's created by SR and
     * its authentication info is stored in SR metadata.
     * <p>
     * For more information about security integration, you can refer to
     * {@link com.starrocks.authentication.SecurityIntegration}
     */
    @ConfField(mutable = true)
    public static String[] authentication_chain = {AUTHENTICATION_CHAIN_MECHANISM_NATIVE};

    /**
     * ldap server host for authentication_ldap_simple
     */
    @ConfField(mutable = true)
    public static String authentication_ldap_simple_server_host = "";

    /**
     * ldap server port for authentication_ldap_simple
     */
    @ConfField(mutable = true)
    public static int authentication_ldap_simple_server_port = 389;

    /**
     * users search base in ldap directory for authentication_ldap_simple
     */
    @ConfField(mutable = true)
    public static String authentication_ldap_simple_bind_base_dn = "";

    /**
     * the name of the attribute that specifies usernames in LDAP directory entries for authentication_ldap_simple
     */
    @ConfField(mutable = true)
    public static String authentication_ldap_simple_user_search_attr = "uid";

    /**
     * the root DN to search users for authentication_ldap_simple
     */
    @ConfField(mutable = true)
    public static String authentication_ldap_simple_bind_root_dn = "";

    /**
     * the root DN password to search users for authentication_ldap_simple
     */
    @ConfField(mutable = true)
    public static String authentication_ldap_simple_bind_root_pwd = "";

    // For forward compatibility, will be removed later.
    // check token when download image file.
    @ConfField
    public static boolean enable_token_check = true;

    /**
     * Cluster token used for internal authentication.
     */
    @ConfField
    public static String auth_token = "";

    /**
     * If set to true and the jar that use to authentication is loaded in fe, kerberos authentication is supported.
     */
    @ConfField(mutable = true)
    public static boolean enable_authentication_kerberos = false;

    /**
     * If kerberos authentication is enabled, the configuration must be filled.
     * like "starrocks-fe/<HOSTNAME>@STARROCKS.COM".
     * <p>
     * Service principal name (SPN) is sent to clients that attempt to authenticate using Kerberos.
     * The SPN must be present in the database managed by the KDC server, and its key file
     * needs to be exported and configured. See authentication_kerberos_service_key_tab for details.
     */
    @ConfField(mutable = true)
    public static String authentication_kerberos_service_principal = "";

    /**
     * If kerberos authentication is enabled, the configuration must be filled.
     * like "$HOME/path/to/your/starrocks-fe.keytab"
     * <p>
     * The keytab file for authenticating tickets received from clients.
     * This file must exist and contain a valid key for the SPN or authentication of clients will fail.
     * Export keytab file requires KDC administrator to operate.
     * for example: ktadd -norandkey -k /path/to/starrocks-fe.keytab starrocks-fe/<HOSTNAME>@STARROCKS.COM
     */
    @ConfField(mutable = true)
    public static String authentication_kerberos_service_key_tab = "";

    /**
     * When set to true, we cannot drop user named 'admin' or grant/revoke role to/from user named 'admin',
     * except that we're root user.
     */
    @ConfField(mutable = true)
    public static boolean authorization_enable_admin_user_protection = false;

    /**
     * When set to true, guava cache is used to cache the privilege collection
     * for a specified user.
     */
    @ConfField(mutable = true)
    public static boolean authorization_enable_priv_collection_cache = true;

    /**
     * In some cases, some tablets may have all replicas damaged or lost.
     * At this time, the data has been lost, and the damaged tablets
     * will cause the entire query to fail, and the remaining healthy tablets cannot be queried.
     * In this case, you can set this configuration to true.
     * The system will replace damaged tablets with empty tablets to ensure that the query
     * can be executed. (but at this time the data has been lost, so the query results may be inaccurate)
     */
    @ConfField(mutable = true)
    public static boolean recover_with_empty_tablet = false;

    /**
     * Limit on the number of expr children of an expr tree.
     */
    @ConfField(mutable = true)
    public static int expr_children_limit = 10000;

    @ConfField(mutable = true)
    public static long max_planner_scalar_rewrite_num = 100000;

    /**
     * statistic collect flag
     */
    @ConfField(mutable = true)
    public static boolean enable_statistic_collect = true;

    /**
     * auto statistic collect on first load flag
     */
    @ConfField(mutable = true)
    public static boolean enable_statistic_collect_on_first_load = true;

    /**
     * max await time for collect statistic for loading
     */
    @ConfField(mutable = true)
    public static long semi_sync_collect_statistic_await_seconds = 30;

    /**
     * The start time of day when auto-updates are enabled
     */
    @ConfField(mutable = true)
    public static String statistic_auto_analyze_start_time = "00:00:00";

    /**
     * The end time of day when auto-updates are enabled
     */
    @ConfField(mutable = true)
    public static String statistic_auto_analyze_end_time = "23:59:59";

    /**
     * a period of create statistics table automatically by the StatisticsMetaManager
     */
    @ConfField(mutable = true)
    public static long statistic_manager_sleep_time_sec = 60; // 60s

    /**
     * Analyze status keep time in catalog
     */
    @ConfField(mutable = true)
    public static long statistic_analyze_status_keep_second = 3 * 24 * 3600L; // 3d

    /**
     * Enable statistics collection profile
     */
    @ConfField(mutable = true)
    public static boolean enable_statistics_collect_profile = false;

    /**
     * Check expire partition statistics data when StarRocks start up
     */
    @ConfField(mutable = true)
    public static boolean statistic_check_expire_partition = true;

    /**
     * The collect thread work interval
     */
    @ConfField(mutable = true)
    public static long statistic_collect_interval_sec = 5L * 60L; // 5m

    /**
     * Num of thread to handle statistic collect(analyze command)
     */
    @ConfField(mutable = true)
    public static int statistic_analyze_task_pool_size = 3;

    /**
     * statistic collect query timeout
     */
    @ConfField(mutable = true)
    public static long statistic_collect_query_timeout = 3600; // 1h

    @ConfField
    public static long statistic_cache_columns = 100000;

    /**
     * The size of the thread-pool which will be used to refresh statistic caches
     */
    @ConfField
    public static int statistic_cache_thread_pool_size = 10;

    @ConfField
    public static int slot_manager_response_thread_pool_size = 16;

    @ConfField
    public static long statistic_dict_columns = 100000;

    /**
     * The column statistic cache update interval
     */
    @ConfField(mutable = true)
    public static long statistic_update_interval_sec = 24L * 60L * 60L;

    @ConfField(mutable = true)
    public static long statistic_collect_too_many_version_sleep = 600000; // 10min
    /**
     * Enable full statistics collection
     */
    @ConfField(mutable = true)
    public static boolean enable_collect_full_statistic = true;

    /**
     * Statistics collection threshold
     */
    @ConfField(mutable = true)
    public static double statistic_auto_collect_ratio = 0.8;

    @ConfField(mutable = true)
    public static long statistic_full_collect_buffer = 1024L * 1024 * 20; // 20MB

    // If the health in statistic_full_collect_interval is lower than this value,
    // choose collect sample statistics first
    @ConfField(mutable = true)
    public static double statistic_auto_collect_sample_threshold = 0.3;

    @ConfField(mutable = true)
    public static long statistic_auto_collect_small_table_size = 5L * 1024 * 1024 * 1024; // 5G

    @ConfField(mutable = true)
    public static long statistic_auto_collect_small_table_rows = 10000000; // 10M

    @ConfField(mutable = true)
    public static long statistic_auto_collect_small_table_interval = 0; // unit: second, default 0

    @ConfField(mutable = true)
    public static long statistic_auto_collect_large_table_interval = 3600L * 12; // unit: second, default 12h

    /**
     * Full statistics collection max data size
     */
    @ConfField(mutable = true)
    public static long statistic_max_full_collect_data_size = 100L * 1024 * 1024 * 1024; // 100G

    /**
     * Max row count in statistics collect per query
     */
    @ConfField(mutable = true)
    public static long statistic_collect_max_row_count_per_query = 5000000000L; //5 billion

    /**
     * The row number of sample collect, default 20w rows
     */
    @ConfField(mutable = true)
    public static long statistic_sample_collect_rows = 200000;

    /**
     * default bucket size of histogram statistics
     */
    @ConfField(mutable = true)
    public static long histogram_buckets_size = 64;

    /**
     * default most common value size of histogram statistics
     */
    @ConfField(mutable = true)
    public static long histogram_mcv_size = 100;

    /**
     * default sample ratio of histogram statistics
     */
    @ConfField(mutable = true)
    public static double histogram_sample_ratio = 0.1;

    /**
     * Max row count of histogram statistics
     */
    @ConfField(mutable = true)
    public static long histogram_max_sample_row_count = 10000000;

    /**
     * If set to true, Planner will try to select replica of tablet on same host as this Frontend.
     * This may reduce network transmission in following case:
     * 1. N hosts with N Backends and N Frontends deployed.
     * 2. The data has N replicas.
     * 3. High concurrency queries are sent to all Frontends evenly
     * In this case, all Frontends can only use local replicas to do the query.
     */
    @ConfField(mutable = true)
    public static boolean enable_local_replica_selection = false;

    /**
     * This will limit the max recursion depth of hash distribution pruner.
     * eg: where `a` in (5 elements) and `b` in (4 elements) and `c` in (3 elements) and `d` in (2 elements).
     * a/b/c/d are distribution columns, so the recursion depth will be 5 * 4 * 3 * 2 = 120, larger than 100,
     * So that distribution pruner will not work and just return all buckets.
     * <p>
     * Increase the depth can support distribution pruning for more elements, but may cost more CPU.
     */
    @ConfField(mutable = true)
    public static int max_distribution_pruner_recursion_depth = 100;

    /**
     * Used to limit num of partition for one batch partition clause
     */
    @ConfField(mutable = true)
    public static long max_partitions_in_one_batch = 4096;

    /**
     * Used to limit num of partition for automatic partition table automatically created
     */
    @ConfField(mutable = true)
    public static long max_automatic_partition_number = 4096;

    /**
     * enable automatic bucket for random distribution table
     */
    @ConfField(mutable = true)
    public static boolean enable_automatic_bucket = true;

    /**
     * default bucket size of automatic bucket table
     */
    @ConfField(mutable = true)
    public static long default_automatic_bucket_size = 4 * 1024 * 1024 * 1024L;

    /**
     * Used to limit num of agent task for one be. currently only for drop task.
     */
    @ConfField(mutable = true)
    public static int max_agent_tasks_send_per_be = 10000;

    /**
     * min num of thread to refresh hive meta
     */
    @ConfField
    public static int hive_meta_cache_refresh_min_threads = 50;

    /**
     * num of thread to handle hive meta load concurrency.
     */
    @ConfField
    public static int hive_meta_load_concurrency = 4;

    /**
     * The interval of lazy refreshing hive metastore cache
     */
    @ConfField
    public static long hive_meta_cache_refresh_interval_s = 3600L * 2L;

    /**
     * Hive metastore cache ttl
     */
    @ConfField
    public static long hive_meta_cache_ttl_s = 3600L * 24L;

    /**
     * Remote file's metadata from hdfs or s3 cache ttl
     */
    @ConfField
    public static long remote_file_cache_ttl_s = 3600 * 36L;

    /**
     * The maximum number of partitions to fetch from the metastore in one RPC.
     */
    @ConfField
    public static int max_hive_partitions_per_rpc = 5000;

    /**
     * The interval of lazy refreshing remote file's metadata cache
     */
    @ConfField
    public static long remote_file_cache_refresh_interval_s = 60;

    /**
     * Number of threads to load remote file's metadata concurrency.
     */
    @ConfField
    public static int remote_file_metadata_load_concurrency = 32;

    /**
     * Hive MetaStore Client socket timeout in seconds.
     */
    @ConfField
    public static long hive_meta_store_timeout_s = 10L;

    /**
     * If set to true, StarRocks will automatically synchronize hms metadata to the cache in fe.
     */
    @ConfField
    public static boolean enable_hms_events_incremental_sync = false;

    /**
     * HMS polling interval in milliseconds.
     */
    @ConfField
    public static int hms_events_polling_interval_ms = 5000;

    /**
     * Maximum number of events to poll in each RPC.
     */
    @ConfField(mutable = true)
    public static int hms_events_batch_size_per_rpc = 500;

    /**
     * If set to true, StarRocks will process events in parallel.
     */
    @ConfField(mutable = true)
    public static boolean enable_hms_parallel_process_evens = true;

    /**
     * Num of thread to process events in parallel.
     */
    @ConfField
    public static int hms_process_events_parallel_num = 4;

    /**
     * Used to split files stored in dfs such as object storage
     * or hdfs into smaller files for hive external table
     */
    @ConfField(mutable = true)
    public static long hive_max_split_size = 64L * 1024L * 1024L;

    /**
     * Enable background refresh all external tables all partitions metadata on internal catalog.
     */
    @ConfField(mutable = true)
    public static boolean enable_background_refresh_connector_metadata = true;

    /**
     * Enable background refresh all external tables all partitions metadata based on resource in internal catalog.
     */
    @ConfField
    public static boolean enable_background_refresh_resource_table_metadata = false;

    /**
     * Number of threads to refresh remote file's metadata concurrency.
     */
    @ConfField
    public static int background_refresh_file_metadata_concurrency = 4;

    /**
     * Background refresh external table metadata interval in milliseconds.
     */
    @ConfField(mutable = true)
    public static int background_refresh_metadata_interval_millis = 600000;

    /**
     * The duration of background refresh external table metadata since the table last access.
     */
    @ConfField(mutable = true)
    public static long background_refresh_metadata_time_secs_since_last_access_secs = 3600L * 24L;

    /**
     * Enable refresh hive partition statistics.
     * The `getPartitionColumnStats()` requests of hive metastore has a high latency, and some users env may return timeout.
     */
    @ConfField(mutable = true)
    public static boolean enable_refresh_hive_partitions_statistics = true;

    /**
     * size of iceberg worker pool
     */
    @ConfField(mutable = true)
    public static boolean enable_iceberg_custom_worker_thread = false;

    /**
     * size of iceberg worker pool
     */
    @ConfField(mutable = true)
    public static long iceberg_worker_num_threads = Runtime.getRuntime().availableProcessors();

    /**
     * size of iceberg table refresh pool
     */
    @ConfField(mutable = true)
    public static int iceberg_table_refresh_threads = 128;

    /**
     * interval to remove cached table in iceberg refresh cache
     */
    @ConfField(mutable = true)
    public static int iceberg_table_refresh_expire_sec = 86400;

    /**
     * iceberg metadata cache dir
     */
    @ConfField(mutable = true)
    public static String iceberg_metadata_cache_disk_path = StarRocksFE.STARROCKS_HOME_DIR + "/caches/iceberg";

    /**
     * iceberg metadata memory cache total size, default 512MB
     */
    @ConfField(mutable = true)
    public static long iceberg_metadata_memory_cache_capacity = 536870912L;

    /**
     * iceberg metadata memory cache expiration time, default 86500s
     */
    @ConfField(mutable = true)
    public static long iceberg_metadata_memory_cache_expiration_seconds = 86500;

    /**
     * enable iceberg metadata disk cache, default false
     */
    @ConfField(mutable = true)
    public static boolean enable_iceberg_metadata_disk_cache = false;

    /**
     * iceberg metadata disk cache total size, default 2GB
     */
    @ConfField(mutable = true)
    public static long iceberg_metadata_disk_cache_capacity = 2147483648L;

    /**
     * iceberg metadata disk cache expire after access
     */
    @ConfField
    public static long iceberg_metadata_disk_cache_expiration_seconds = 7L * 24L * 60L * 60L;

    /**
     * iceberg metadata cache max entry size, default 8MB
     */
    @ConfField(mutable = true)
    public static long iceberg_metadata_cache_max_entry_size = 8388608L;

    /**
     * fe will call es api to get es index shard info every es_state_sync_interval_secs
     */
    @ConfField
    public static long es_state_sync_interval_second = 10;

    /**
     * connection and socket timeout for broker client
     */
    @ConfField
    public static int broker_client_timeout_ms = 120000;

    /**
     * Unused config field, leave it here for backward compatibility
     */
    @Deprecated
    @ConfField(mutable = true)
    public static boolean vectorized_load_enable = true;

    /**
     * Enable pipeline engine load
     */
    @Deprecated
    @ConfField(mutable = true)
    public static boolean enable_pipeline_load = true;

    /**
     * Enable shuffle load
     */
    @ConfField(mutable = true)
    public static boolean enable_shuffle_load = true;

    /**
     * Eliminate shuffle load by replicated storage
     */
    @ConfField(mutable = true)
    public static boolean eliminate_shuffle_load_by_replicated_storage = true;

    /**
     * Unused config field, leave it here for backward compatibility
     */
    @Deprecated
    @ConfField(mutable = true)
    public static boolean enable_vectorized_file_load = true;

    /**
     * Whether to collect routine load process metrics.
     * Be careful to turn this on, because this will call kafka api to get the partition's latest offset.
     */
    @ConfField(mutable = true)
    public static boolean enable_routine_load_lag_metrics = false;

    @ConfField(mutable = true)
    public static boolean enable_collect_query_detail_info = false;

    /**
     *  StarRocks-manager pull queries every 1 second
     *  metrics calculate query latency every 15 second
     *  do not set cacheTime lower than these time
     */
    @ConfField(mutable = true)
    public static long query_detail_cache_time_nanosecond = 30000000000L;

    /**
     * Min lag of routine load job to show in metrics
     * Only show the routine load job whose lag is larger than min_routine_load_lag_for_metrics
     */
    @ConfField(mutable = true)
    public static long min_routine_load_lag_for_metrics = 10000;

    /**
     * The heartbeat timeout of be/broker/fe.
     * the default is 5 seconds
     */
    @ConfField(mutable = true)
    public static int heartbeat_timeout_second = 5;

    /**
     * The heartbeat retry times of be/broker/fe.
     * the default is 3
     */
    @ConfField(mutable = true)
    public static int heartbeat_retry_times = 3;

    /**
     * shared_data: means run on cloud-native
     * shared_nothing: means run on local
     * hybrid: run on both, not production ready, should only be used in test environment now.
     */
    @ConfField
    public static String run_mode = "shared_nothing";

    /**
     * empty shard group clean threshold (by create time).
     */
    @ConfField
    public static long shard_group_clean_threshold_sec = 3600L;

    /**
     * fe sync with star mgr meta interval in seconds
     */
    @ConfField
    public static long star_mgr_meta_sync_interval_sec = 600L;

    // ***********************************************************
    // * BEGIN: Cloud native meta server related configurations
    // ***********************************************************
    /**
     * Cloud native meta server rpc listen port
     */
    @ConfField
    public static int cloud_native_meta_port = 6090;
    /**
     * Whether volume can be created from conf. If it is enabled, a builtin storage volume may be created.
     */
    @ConfField
    public static boolean enable_load_volume_from_conf = true;
    // remote storage related configuration
    @ConfField(comment = "storage type for cloud native table. Available options: \"S3\", \"HDFS\", \"AZBLOB\". case-insensitive")
    public static String cloud_native_storage_type = "S3";

    // HDFS storage configuration
    /**
     * cloud native storage: hdfs storage url
     */
    @ConfField
    public static String cloud_native_hdfs_url = "";

    // AWS S3 storage configuration
    @ConfField
    public static String aws_s3_path = "";
    @ConfField
    public static String aws_s3_region = "";
    @ConfField
    public static String aws_s3_endpoint = "";

    // AWS credential configuration
    @ConfField
    public static boolean aws_s3_use_aws_sdk_default_behavior = false;
    @ConfField
    public static boolean aws_s3_use_instance_profile = false;

    @ConfField
    public static String aws_s3_access_key = "";
    @ConfField
    public static String aws_s3_secret_key = "";

    @ConfField
    public static String aws_s3_iam_role_arn = "";
    @ConfField
    public static String aws_s3_external_id = "";

    // Enables or disables SSL connections to AWS services. Not support for now
    // @ConfField
    // public static String aws_s3_enable_ssl = "true";

    // azure blob
    @ConfField
    public static String azure_blob_endpoint = "";
    @ConfField
    public static String azure_blob_path = "";

    @ConfField
    public static String azure_blob_shared_key = "";
    @ConfField
    public static String azure_blob_sas_token = "";
    @ConfField(mutable = true)
    public static int starmgr_grpc_timeout_seconds = 5;

    // ***********************************************************
    // * END: of Cloud native meta server related configurations
    // ***********************************************************

    @ConfField(mutable = true)
    public static boolean enable_experimental_rowstore = false;

    @ConfField(mutable = true)
    public static boolean enable_experimental_mv = true;

    /**
     * Whether to support colocate mv index in olap table sink, tablet sink will only send chunk once
     * if enabled to speed up the sync mv's transformation performance.
     */
    @ConfField(mutable = true)
    public static boolean enable_colocate_mv_index = true;

    /**
     * Each automatic partition will create a hidden partition, which is not displayed to the user by default.
     * Sometimes this display can be enabled to check problems.
     */
    @ConfField(mutable = true)
    public static boolean enable_display_shadow_partitions = false;

    @ConfField
    public static boolean enable_dict_optimize_routine_load = false;

    @ConfField(mutable = true)
    public static boolean enable_dict_optimize_stream_load = true;

    /**
     * If set to true, the following rules will apply to see if the password is secure upon the creation of a user.
     * 1. The length of the password should be no less than 8.
     * 2. The password should contain at least one digit, one lowercase letter, one uppercase letter
     */
    @ConfField(mutable = true)
    public static boolean enable_validate_password = false;

    /**
     * If set to false, changing the password to the previous one is not allowed.
     */
    @ConfField(mutable = true)
    public static boolean enable_password_reuse = true;
    /**
     * If set to false, when the load is empty, success is returned.
     * Otherwise, `all partitions have no load data` is returned.
     */
    @ConfField(mutable = true)
    public static boolean empty_load_as_error = true;

    /**
     * after wait quorum_publish_wait_time_ms, will do quorum publish
     * In order to avoid unnecessary CLONE, we increase the timeout as much as possible
     */
    @ConfField(mutable = true, aliases = {"quorom_publish_wait_time_ms"})
    public static int quorum_publish_wait_time_ms = 5000;

    /**
     * FE journal queue size
     * Write log will fail if queue is full
     **/
    @ConfField(mutable = true)
    public static int metadata_journal_queue_size = 1000;

    /**
     * The maximum size(key+value) of journal entity to write as a batch
     * Increase this configuration if journal queue is always full
     * TODO: set default value
     **/
    @ConfField(mutable = true)
    public static int metadata_journal_max_batch_size_mb = 10;

    /**
     * The maximum number of journal entity to write as a batch
     * Increase this configuration if journal queue is always full
     * TODO: set default value
     **/
    @ConfField(mutable = true)
    public static int metadata_journal_max_batch_cnt = 100;

    /**
     * jaeger tracing endpoint, empty thing disables tracing
     */
    @ConfField
    public static String jaeger_grpc_endpoint = "";

    @ConfField
    public static String lake_compaction_selector = "ScoreSelector";

    @ConfField
    public static String lake_compaction_sorter = "ScoreSorter";

    @ConfField(mutable = true)
    public static long lake_compaction_simple_selector_min_versions = 3;

    @ConfField(mutable = true)
    public static long lake_compaction_simple_selector_threshold_versions = 10;

    @ConfField(mutable = true)
    public static long lake_compaction_simple_selector_threshold_seconds = 300;

    @ConfField(mutable = true)
    public static double lake_compaction_score_selector_min_score = 10.0;

    @ConfField(mutable = true, comment = "-1 means calculate the value in an adaptive way. set this value to 0 " +
            "will disable compaction.")
    public static int lake_compaction_max_tasks = -1;

    @ConfField(mutable = true)
    public static int lake_compaction_history_size = 12;

    @ConfField(mutable = true)
    public static int lake_compaction_fail_history_size = 12;

    @ConfField(mutable = true, comment = "the max number of threads for lake table publishing version")
    public static int lake_publish_version_max_threads = 512;

    @ConfField(mutable = true, comment = "the max number of threads for lake table delete txnLog when enable batch publish")
    public static int lake_publish_delete_txnlog_max_threads = 16;

    /**
     * Default lake compaction txn timeout
     */
    @ConfField(mutable = true)
    public static int lake_compaction_default_timeout_second = 86400; // 1 day


    @ConfField(mutable = true, comment = "the max number of previous version files to keep")
    public static int lake_autovacuum_max_previous_versions = 0;

    @ConfField(comment = "how many partitions can autovacuum be executed simultaneously at most")
    public static int lake_autovacuum_parallel_partitions = 8;

    @ConfField(mutable = true, comment = "the minimum delay between autovacuum runs on any given partition")
    public static long lake_autovacuum_partition_naptime_seconds = 180;

    @ConfField(mutable = true, comment =
            "History versions within this time range will not be deleted by auto vacuum.\n" +
            "REMINDER: Set this to a value longer than the maximum possible execution time of queries, to avoid deletion of " +
            "versions still being accessed.\n" +
            "NOTE: Increasing this value may increase the space usage of the remote storage system.")
    public static long lake_autovacuum_grace_period_minutes = 5;

    @ConfField(mutable = true, comment =
            "time threshold in hours, if a partition has not been updated for longer than this " +
            "threshold, auto vacuum operations will no longer be triggered for that partition.\n" +
            "Only takes effect for tables in clusters with run_mode=shared_data.\n")
    public static long lake_autovacuum_stale_partition_threshold = 12;

    @ConfField(mutable = true, comment =
            "Whether enable throttling ingestion speed when compaction score exceeds the threshold.\n" +
            "Only takes effect for tables in clusters with run_mode=shared_data.")
    public static boolean lake_enable_ingest_slowdown = false;

    @ConfField(mutable = true, comment =
            "Compaction score threshold above which ingestion speed slowdown is applied.\n" +
            "NOTE: The actual effective value is the max of the configured value and " +
            "'lake_compaction_score_selector_min_score'.")
    public static long lake_ingest_slowdown_threshold = 100;

    @ConfField(mutable = true, comment =
            "Ratio to reduce ingestion speed for each point of compaction score over the threshold.\n" +
            "E.g. 0.05 ratio, 10min normal ingestion, exceed threshold by:\n" +
            " - 1 point -> Delay by 0.05 (30secs)\n" +
            " - 5 points -> Delay by 0.25 (2.5mins)")
    public static double lake_ingest_slowdown_ratio = 0.1;

    @ConfField(mutable = true, comment =
            "The upper limit for compaction score, only takes effect when lake_enable_ingest_slowdown=true.\n" +
            "When the compaction score exceeds this value, data ingestion transactions will be prevented from\n" +
            "committing. This is a soft limit, the actual compaction score may exceed the configured bound.\n" +
            "The effective value will be set to the higher of the configured value here and " +
            "lake_compaction_score_selector_min_score.\n" +
            "A value of 0 represents no limit.")
    public static long lake_compaction_score_upper_bound = 0;

    @ConfField(mutable = true)
    public static boolean enable_new_publish_mechanism = false;

    @ConfField(mutable = true)
    public static boolean enable_sync_publish = true;

    /**
     * Normally FE will quit when replaying a bad journal. This configuration provides a bypass mechanism.
     * If this was set to a positive value, FE will skip the corresponding bad journals before it quits.
     * e.g. 495501,495503
     */
    @ConfField(mutable = true)
    public static String metadata_journal_skip_bad_journal_ids = "";

    /**
     * Set this configuration to true to ignore specific operation (with IgnorableOnReplayFailed annotation) replay failures.
     */
    @ConfField
    public static boolean metadata_journal_ignore_replay_failure = false;

    /**
     * Number of profile infos reserved by `ProfileManager` for recently executed query.
     * Default value: 500
     */
    @ConfField(mutable = true)
    public static int profile_info_reserved_num = 500;

    /**
     * Number of stream load profile infos reserved by `ProfileManager` for recently executed stream load and routine load task.
     * Default value: 500
     */
    @ConfField(mutable = true)
    public static int load_profile_info_reserved_num = 500;

    /**
     * format of profile infos reserved by `ProfileManager` for recently executed query.
     * Default value: "default"
     */
    @ConfField(mutable = true)
    public static String profile_info_format = "default";

    /**
     * Max number of roles that can be granted to user including all direct roles and all parent roles
     * Used in new RBAC framework after 3.0 released
     **/
    @ConfField(mutable = true)
    public static int privilege_max_total_roles_per_user = 64;

    /**
     * Max role inheritance depth allowed. To avoid bad performance when merging privileges.
     **/
    @ConfField(mutable = true)
    public static int privilege_max_role_depth = 16;

    /**
     * ignore invalid privilege & authentication when upgraded to new RBAC privilege framework in 3.0
     */
    @ConfField(mutable = true)
    public static boolean ignore_invalid_privilege_authentications = false;

    /**
     * the keystore file path
     */
    @ConfField
    public static String ssl_keystore_location = "";

    /**
     * the password of keystore file
     */
    @ConfField
    public static String ssl_keystore_password = "";

    /**
     * the password of private key
     */
    @ConfField
    public static String ssl_key_password = "";

    /**
     * the truststore file path
     */
    @ConfField
    public static String ssl_truststore_location = "";

    /**
     * the password of truststore file
     */
    @ConfField
    public static String ssl_truststore_password = "";

    /**
     * ignore check db status when show proc '/catalog/catalog_name'
     */
    @ConfField(mutable = true)
    public static boolean enable_check_db_state = true;

    @ConfField
    public static long binlog_ttl_second = 60 * 30; // 30min

    @ConfField
    public static long binlog_max_size = Long.MAX_VALUE; // no limit

    /**
     * Enable check if the cluster is under safe mode or not
     **/
    @ConfField(mutable = true)
    public static boolean enable_safe_mode = false;

    /**
     * The safe mode checker thread work interval
     */
    @ConfField(mutable = true)
    public static long safe_mode_checker_interval_sec = 5;

    /**
     * Enable auto create tablet when creating table and add partition
     **/
    @ConfField(mutable = true)
    public static boolean enable_auto_tablet_distribution = true;

    /**
     * default size of minimum cache size of auto increment id allocation
     **/
    @ConfField(mutable = true)
    public static int auto_increment_cache_size = 100000;

    /**
     * Enable the experimental temporary table feature
     */
    @ConfField(mutable = true)
    public static boolean enable_experimental_temporary_table = false;

    @ConfField(mutable = true)
    public static long max_per_node_grep_log_limit = 500000;

    @ConfField
    public static boolean enable_execute_script_on_frontend = true;

    @ConfField(mutable = true)
    public static short default_replication_num = 3;

    /**
     * The default scheduler interval for alter jobs.
     */
    @ConfField(mutable = true)
    public static int alter_scheduler_interval_millisecond = 10000;

    /**
     * The default scheduler interval for routine loads.
     */
    @ConfField(mutable = true)
    public static int routine_load_scheduler_interval_millisecond = 10000;

    /**
     * Only when the stream load time exceeds this value,
     * the profile will be put into the profileManager
     */
    @ConfField(mutable = true)
    public static long stream_load_profile_collect_second = 10; //10s

    /**
     * If set to <= 0, means that no limitation.
     */
    @ConfField(mutable = true)
    public static int max_upload_task_per_be = 0;

    /**
     * If set to <= 0, means that no limitation.
     */
    @ConfField(mutable = true)
    public static int max_download_task_per_be = 0;

    /*
     * Using persistent index in primary key table by default when creating table.
     */
    @ConfField(mutable = true)
    public static boolean enable_persistent_index_by_default = true;

    /**
     * timeout for external table commit
     */
    @ConfField(mutable = true)
    public static int external_table_commit_timeout_ms = 10000; // 10s

    @ConfField(mutable = true)
    public static boolean enable_fast_schema_evolution = true;

    // This configuration will be removed after the shared data mode supports fast schema evolution
    @ConfField(mutable = true)
    public static boolean experimental_enable_fast_schema_evolution_in_shared_data = false;

    @ConfField(mutable = false)
    public static int pipe_listener_interval_millis = 1000;
    @ConfField(mutable = false)
    public static int pipe_scheduler_interval_millis = 1000;

    @ConfField(mutable = true)
    public static long mv_active_checker_interval_seconds = 60;

    /**
     * Whether enable to active inactive materialized views automatically by the daemon thread or not.
     */
    @ConfField(mutable = true)
    public static boolean enable_mv_automatic_active_check = true;

    /**
     * The refresh partition number when refreshing materialized view at once by default.
     */
    @ConfField(mutable = true)
    public static int default_mv_partition_refresh_number = 1;

    @ConfField(mutable = true,
            comment = "The default behavior of whether REFRESH IMMEDIATE or not, " +
                    "which would refresh the materialized view after creating")
    public static boolean default_mv_refresh_immediate = true;

    /**
     * Whether analyze the mv after refresh in async mode.
     */
    @ConfField(mutable = true)
    public static boolean mv_auto_analyze_async = true;

    /**
     * To prevent the external catalog from displaying too many entries in the grantsTo system table,
     * you can use this variable to ignore the entries in the external catalog
     */
    @ConfField(mutable = true)
    public static boolean enable_show_external_catalog_privilege = true;

    /**
     * Loading or compaction must be stopped for primary_key_disk_schedule_time
     * seconds before it can be scheduled
     */
    @ConfField(mutable = true)
    public static int primary_key_disk_schedule_time = 3600; // 1h

    @ConfField(mutable = true)
    public static String access_control = "native";

    /**
     *  Kerberos principal used for mutual authentication with ranger
     */
    @ConfField(mutable = true)
    public static String ranger_spnego_kerberos_principal = "";

    /**
     *  Kerberos keytab file used for mutual authentication with ranger
     */
    @ConfField(mutable = true)
    public static String ranger_spnego_kerberos_keytab = "";

    /**
     *  Kerberos krb5.conf configure path, default /etc/krb5.conf
     */
    @ConfField(mutable = true)
    public static String ranger_kerberos_krb5_conf = "";

    /**
     * Whether to use the unix group as the ranger authentication group
     */
    @ConfField(mutable = true)
    public static boolean ranger_user_ugi = false;

    @ConfField(mutable = true)
    public static int catalog_metadata_cache_size = 500;

    /**
     * mv plan cache expire interval in seconds
     */
    @ConfField(mutable = true)
    public static long mv_plan_cache_expire_interval_sec = 24L * 60L * 60L;

    /**
     * mv plan cache expire interval in seconds
     */
    @ConfField(mutable = true)
    public static long mv_plan_cache_max_size = 1000;

    @ConfField(mutable = true, comment = "Max materialized view rewrite cache size during one query's lifecycle " +
            "so can avoid repeating compute to reduce optimizer time in materialized view rewrite, " +
            "but may occupy some extra FE's memory. It's well-done when there are many relative " +
            "materialized views(>10) or query is complex(multi table joins).")
    public static long mv_query_context_cache_max_size = 1000;

    /**
     * Checking the connectivity of port opened by FE,
     * mainly used for checking edit log port currently.
     */
    @ConfField(mutable = true)
    public static long port_connectivity_check_interval_sec = 60;

    @ConfField(mutable = true)
    public static long port_connectivity_check_retry_times = 3;

    @ConfField(mutable = true)
    public static int port_connectivity_check_timeout_ms = 10000;

    // This limit limits the maximum size of json file to load.
    @ConfField(mutable = true)
    public static long json_file_size_limit = 4294967296L;

    @ConfField(mutable = true)
    public static boolean allow_system_reserved_names = false;

    /**
     * Whether to use LockManager to manage lock usage
     */
    public static boolean use_lock_manager = false;

    /**
     * Number of Hash of Lock Table
     */
    public static int lock_table_num = 32;

    /**
     * Whether to enable deadlock unlocking operation.
     * It is turned off by default and only deadlock detection is performed.
     * If turned on, LockManager will try to relieve the deadlock by killing the victim.
     */
    @ConfField(mutable = true)
    public static boolean enable_unlock_deadlock = false;

    /**
     * when a lock cannot be obtained, we cannot determine whether it is because the required
     * lock is being used normally or if a deadlock has occurred.
     * Therefore, based on the configuration parameter `dead_lock_detection_delay_time_ms`
     * is used to control the waiting time before deadlock detection.
     * If a lock is obtained during this period, there is no need to perform deadlock detection.
     * Avoid frequent and unnecessary deadlock detection due to lock contention
     */
    @ConfField(mutable = true)
    public static long dead_lock_detection_delay_time_ms = 3000; // 3s

    @ConfField(mutable = true)
    public static long routine_load_unstable_threshold_second = 3600;
    /**
     * dictionary cache refresh task thread pool size
     */
    @ConfField(mutable = true)
    public static int refresh_dictionary_cache_thread_num = 2;

    /*
     * Replication config
     */
    @ConfField
    public static int replication_interval_ms = 10;
    @ConfField(mutable = true)
    public static int replication_max_parallel_table_count = 100; // 100
    @ConfField(mutable = true)
    public static int replication_max_parallel_data_size_mb = 10240; // 10g
    @ConfField(mutable = true)
    public static int replication_transaction_timeout_sec = 1 * 60 * 60; // 1hour

    @ConfField(mutable = true)
    public static boolean jdbc_meta_default_cache_enable = false;

    @ConfField(mutable = true)
    public static long jdbc_meta_default_cache_expire_sec = 600L;

    // the retention time for host disconnection events
    @ConfField(mutable = true)
    public static long black_host_history_sec = 2 * 60; // 2min

    // limit for the number of host disconnections in the last {black_host_history_sec} seconds
    @ConfField(mutable = true)
    public static long black_host_connect_failures_within_time = 5;


    @ConfField(mutable = false)
    public static int jdbc_connection_pool_size = 8;

    @ConfField(mutable = false)
    public static int jdbc_minimum_idle_connections = 1;

    @ConfField(mutable = false)
    public static int jdbc_connection_idle_timeout_ms = 600000;
}
