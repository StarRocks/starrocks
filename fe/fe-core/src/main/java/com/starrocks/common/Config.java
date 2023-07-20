// This file is made available under Elastic License 2.0.
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

public class Config extends ConfigBase {

    /**
     * The max size of one sys log and audit log
     */
    @ConfField
    public static int log_roll_size_mb = 1024; // 1 GB

    /**
     * sys_log_dir:
     * This specifies FE log dir. FE will produces 2 log files:
     * fe.log:      all logs of FE process.
     * fe.warn.log  all WARNING and ERROR log of FE process.
     * <p>
     * sys_log_level:
     * INFO, WARNING, ERROR, FATAL
     * <p>
     * sys_log_roll_num:
     * Maximal FE log files to be kept within an sys_log_roll_interval.
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
     * If the response time of a query exceed this threshold, it will be recored in audit log as slow_query.
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

    /**
     * dump_log_dir:
     * This specifies FE dump log dir.
     * Dump log fe.dump.log contains all dump information which query has Exception
     * <p>
     * dump_log_roll_num:
     * Maximal FE log files to be kept within an dump_log_roll_interval.
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
     *  for task set expire time
     */
    @ConfField(mutable = true)
    public static int task_ttl_second = 3 * 24 * 3600;         // 3 day

    /**
     *  for task run set expire time
     */
    @ConfField(mutable = true)
    public static int task_runs_ttl_second = 3 * 24 * 3600;     // 3 day

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
     * StarRocks meta data will be saved here.
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
     * Master FE will save image every *edit_log_roll_num* meta journals.
     */
    @ConfField(mutable = true)
    public static int edit_log_roll_num = 50000;

    /**
     * whether ignore unknown log id
     * when fe rolls back to low version, there may be log id that low version fe can not recognise
     * if set to true, fe will ignore those id
     * or fe will exit
     */
    @ConfField(mutable = true)
    public static boolean ignore_unknown_log_id = false;

    /**
     * Non-master FE will stop offering service
     * if meta data delay gap exceeds *meta_delay_toleration_second*
     */
    @ConfField(mutable = true)
    public static int meta_delay_toleration_second = 300;    // 5 min

    /**
     * Master FE sync policy of bdbje.
     * If you only deploy one Follower FE, set this to 'SYNC'. If you deploy more than 3 Follower FE,
     * you can set this and the following 'replica_sync_policy' to WRITE_NO_SYNC.
     * more info, see: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html
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
     * more info, see: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html
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
     * Set the maximum acceptable clock skew between non-master FE to Master FE host.
     * This value is checked whenever a non-master FE establishes a connection to master FE via BDBJE.
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
     *
     * If the bdb dir expands, set this param to 0.
     */
    @ConfField
    public static int bdbje_replay_cost_percent = 150;

    /**
     * the max txn number which bdbje can rollback when trying to rejoin the group
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
     * If true, FE will reset bdbje replication group(that is, to remove all electable nodes info)
     * and is supposed to start as Master.
     * If all the electable nodes can not start, we can copy the meta data
     * to another node and set this config to true to try to restart the FE.
     */
    @ConfField
    public static String metadata_failure_recovery = "false";

    /**
     * If true, non-master FE will ignore the meta data delay gap between Master FE and its self,
     * even if the metadata delay gap exceeds *meta_delay_toleration_second*.
     * Non-master FE will still offer read service.
     * <p>
     * This is helpful when you try to stop the Master FE for a relatively long time for some reason,
     * but still wish the non-master FE can offer read service.
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
     * The backlog_num for netty http server
     * When you enlarge this backlog_num, you should ensure it's value larger than
     * the linux /proc/sys/net/core/somaxconn config
     */
    @ConfField
    public static int http_backlog_num = 1024;

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
    public static int thrift_client_timeout_ms = 0;

    /**
     * The backlog_num for thrift server
     * When you enlarge this backlog_num, you should ensure it's value larger than
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
     * When you enlarge this backlog_num, you should ensure it's value larger than
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
     * node(FE or BE) will be considered belonging to the same StarRocks cluster if they have same cluster id.
     * Cluster id is usually a random integer generated when master FE start at first time.
     * You can also sepecify one.
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
     * eg.
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

    /**
     * The thrift server max worker threads
     */
    @ConfField
    public static int thrift_server_max_worker_threads = 4096;

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

    /**
     * Default broker load timeout
     */
    @ConfField(mutable = true)
    public static int broker_load_default_timeout_second = 14400; // 4 hour

    /**
     * Maximal bytes that a single broker scanner will read.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true)
    public static long min_bytes_per_broker_scanner = 67108864L; // 64MB

    /**
     * Maximal concurrency of broker scanners.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true)
    public static int max_broker_concurrency = 100;

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
     * Max load timeout applicable to all type of load except for stream load
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
    public static int desired_max_waiting_jobs = 100;

    /**
     * maximun concurrent running txn num including prepare, commit txns under a single db
     * txn manager will reject coming txns
     */
    @ConfField(mutable = true)
    public static int max_running_txn_num_per_db = 100;

    /**
     * The load task executor pool size. This pool size limits the max running load tasks.
     * Currently, it only limits the load task of broker load, pending and loading phases.
     * It should be less than 'max_running_txn_num_per_db'
     */
    @ConfField(mutable = false)
    public static int async_load_task_pool_size = 10;

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
     * When create a table(or partition), you can specify its storage medium(HDD or SSD).
     * If set to SSD, this specifies the default duration that tablets will stay on SSD.
     * After that, tablets will be moved to HDD automatically.
     * You can set storage cooldown time in CREATE TABLE stmt.
     */
    @ConfField
    public static long storage_cooldown_second = 30 * 24 * 3600L; // 30 days

    /**
     * If set to true, FE will check backend available capacity by storage medium when create table
     * <p>
     * The default value should better set to true because if user
     * has a deployment with only SSD or HDD medium storage paths,
     * create an incompatible table will cause balance problem(SSD tablet cannot move to HDD path, vice versa).
     * But currently for compatibility reason, we keep it to false.
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
     * Default is 20.
     */
    @ConfField(mutable = true)
    public static int task_runs_concurrency = 20;
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
    @ConfField(mutable = false)
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
     * Limit on the number of expr children of an expr tree.
     * Exceed this limit may cause long analysis time while holding database read lock.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true)
    public static int expr_children_limit = 10000;
    /**
     * Limit on the depth of an expr tree.
     * Exceed this limit may cause long analysis time while holding db read lock.
     * Do not set this if you know what you are doing.
     */
    @ConfField(mutable = true)
    public static int expr_depth_limit = 3000;

    /**
     * Used to limit element num of InPredicate in delete statement.
     */
    @ConfField(mutable = true)
    public static int max_allowed_in_element_num_of_delete = 10000;

    /**
     * only limit for Row-based storage.
     * set to Integer.MAX_VALUE, cause starrocks is already Column-based storage
     */
    @ConfField(mutable = true)
    public static int max_layout_length_per_row = Integer.MAX_VALUE;

    /**
     * The multi cluster feature will be deprecated in version 0.12
     * set this config to true will disable all operations related to cluster feature, include:
     * create/drop cluster
     * add free backend/add backend to cluster/decommission cluster balance
     * change the backends num of cluster
     * link/migration db
     */
    @ConfField(mutable = true)
    public static boolean disable_cluster_feature = true;

    /**
     * control materialized view
     */
    @ConfField(mutable = true)
    public static boolean enable_materialized_view = true;

    @ConfField(mutable = true)
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
    public static int max_create_table_timeout_second = 60;

    // Configurations for backup and restore
    /**
     * Plugins' path for BACKUP and RESTORE operations. Currently deprecated.
     */
    @Deprecated
    @ConfField
    public static String backup_plugin_path = "/tools/trans_file_tool/trans_files.sh";

    // default timeout of backup job
    @ConfField(mutable = true)
    public static int backup_job_default_timeout_ms = 86400 * 1000; // 1 day

    // If use k8s deploy manager locally, set this to true and prepare the certs files
    @ConfField
    public static boolean with_k8s_certs = false;

    // Set runtime locale when exec some cmds
    @ConfField
    public static String locale = "zh_CN.UTF-8";

    /**
     * 'storage_high_watermark_usage_percent' limit the max capacity usage percent of a Backend storage path.
     * 'storage_min_left_capacity_bytes' limit the minimum left capacity of a Backend storage path.
     * If both limitations are reached, this storage path can not be chosen as tablet balance destination.
     * But for tablet recovery, we may exceed these limit for keeping data integrity as much as possible.
     */
    @ConfField(mutable = true)
    public static int storage_high_watermark_usage_percent = 85;
    @ConfField(mutable = true)
    public static long storage_min_left_capacity_bytes = 2 * 1024 * 1024 * 1024; // 2G

    /**
     * If capacity of disk reach the 'storage_flood_stage_usage_percent' and 'storage_flood_stage_left_capacity_bytes',
     * the following operation will be rejected:
     * 1. load job
     * 2. restore job
     */
    @ConfField(mutable = true)
    public static int storage_flood_stage_usage_percent = 95;
    @ConfField(mutable = true)
    public static long storage_flood_stage_left_capacity_bytes = 1 * 1024 * 1024 * 1024; // 1G

    // update interval of tablet stat
    // All frontends will get tablet stat from all backends at each interval
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
     * all pending load job will failed when call begin txn api
     * all prepare load job will failed when call commit txn api
     * all committed load job will waiting to be published
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
     * Load using hadoop cluster will be deprecated in future.
     * Set to true to disable this kind of load.
     */
    @ConfField(mutable = true)
    public static boolean disable_hadoop_load = false;

    /**
     * the factor of delay time before deciding to repair tablet.
     * if priority is VERY_HIGH, repair it immediately.
     * HIGH, delay tablet_repair_delay_factor_second * 1;
     * NORMAL: delay tablet_repair_delay_factor_second * 2;
     * LOW: delay tablet_repair_delay_factor_second * 3;
     */
    @ConfField(mutable = true)
    public static long tablet_repair_delay_factor_second = 60;

    /**
     * If BE is down beyond this time, tablets on that BE of colcoate table will be migrated to other available BEs
     */
    @ConfField(mutable = true)
    public static long tablet_sched_colocate_be_down_tolerate_time_s = 12 * 3600;

    /**
     * If the tablet in scheduler queue has not been scheduled for tablet_sched_max_not_being_scheduled_interval_ms,
     * its priority will upgrade.
     * default is 15min
     */
    @ConfField(mutable = true)
    public static long tablet_sched_max_not_being_scheduled_interval_ms = 15 * 60 * 1000;

    /**
     * the default slot number per path in tablet scheduler
     * TODO(cmy): remove this config and dynamically adjust it by clone task statistic
     */
    @ConfField(mutable = true)
    public static int schedule_slot_num_per_path = 2;

    @ConfField
    public static String tablet_balancer_strategy = "disk_and_tablet";

    /**
     * FOR BeLoadBalancer:
     * the threshold of cluster balance score, if a backend's load score is 10% lower than average score,
     * this backend will be marked as LOW load, if load score is 10% higher than average score, HIGH load
     * will be marked.
     * <p>
     * FOR DiskAndTabletLoadBalancer:
     * upper limit of the difference in disk usage of all backends, exceeding this threshold will cause
     * disk balance
     */
    @ConfField(mutable = true)
    public static double balance_load_score_threshold = 0.1; // 10%

    /**
     * For DiskAndTabletLoadBalancer:
     * if all backends disk usage is lower than this threshold, disk balance will never happen
     */
    @ConfField(mutable = true)
    public static double balance_load_disk_safe_threshold = 0.5; // 50%

    /**
     * if set to true, TabletScheduler will not do balance.
     */
    @ConfField(mutable = true)
    public static boolean disable_balance = false;

    // if the number of scheduled tablets in TabletScheduler exceed max_scheduling_tablets
    // skip checking.
    @ConfField(mutable = true)
    public static int max_scheduling_tablets = 2000;

    // if the number of balancing tablets in TabletScheduler exceed max_balancing_tablets,
    // no more balance check
    @ConfField(mutable = true)
    public static int max_balancing_tablets = 100;

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
     * the max routine load job num, including NEED_SCHEDULED, RUNNING, PAUSE
     */
    @ConfField(mutable = true)
    public static int max_routine_load_job_num = 100;

    /**
     * the max concurrent routine load task num of a single routine load job
     */
    @ConfField(mutable = true)
    public static int max_routine_load_task_concurrent_num = 5;

    /**
     * the max concurrent routine load task num per BE.
     * This is to limit the num of routine load tasks sending to a BE, and it should also less
     * than BE config 'routine_load_thread_pool_size'(default 10),
     * which is the routine load task thread pool size on BE.
     */
    @ConfField(mutable = true)
    public static int max_routine_load_task_num_per_be = 5;

    /**
     * max load size for each routine load task
     */
    @ConfField(mutable = true)
    public static long max_routine_load_batch_size = 500 * 1024 * 1024; // 500M

    /**
     * consume data time for each routine load task
     */
    @ConfField(mutable = true)
    public static long routine_load_task_consume_second = 3;

    /**
     * routine load task timeout
     * should bigger than 2 * routine_load_task_consume_second
     * but can not be less than 10s because when one be down the load time will be at least 10s
     */
    @ConfField(mutable = true)
    public static long routine_load_task_timeout_second = 15;

    /**
     * kafka util request timeout
     */
    @ConfField(mutable = true)
    public static long routine_load_kafka_timeout_second = 12;

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
     * The following 1 configs can set to true to disable the automatic colocate tables's relocate and balance.
     * if *disable_colocate_balance* is set to true, ColocateTableBalancer will not balance colocate tables.
     */
    @ConfField(mutable = true)
    public static boolean disable_colocate_balance = false;

    /**
     * If set to true, the insert stmt with processing error will still return a label to user.
     * And user can use this label to check the load job's status.
     * The default value is false, which means if insert operation encounter errors,
     * exception will be thrown to user client directly without load label.
     */
    @ConfField(mutable = true)
    public static boolean using_old_load_usage_pattern = false;

    /**
     * control rollup job concurrent limit
     */
    @ConfField(mutable = true)
    public static int max_running_rollup_job_num_per_table = 1;

    /**
     * if set to false, auth check will be disable, in case some goes wrong with the new privilege system.
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
     * the name of the attribute that specifies user names in LDAP directory entries for authentication_ldap_simple
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
     * min_clone_task_timeout_sec and max_clone_task_timeout_sec is to limit the
     * min and max timeout of a clone task.
     * Under normal circumstances, the timeout of a clone task is estimated by
     * the amount of data and the minimum transmission speed(5MB/s).
     * But in special cases, you may need to manually set these two configs
     * to ensure that the clone task will not fail due to timeout.
     */
    @ConfField(mutable = true)
    public static long min_clone_task_timeout_sec = 3 * 60; // 3min
    @ConfField(mutable = true)
    public static long max_clone_task_timeout_sec = 2 * 60 * 60; // 2h

    @ConfField(mutable = true)
    public static long max_planner_scalar_rewrite_num = 100000;

    /**
     * a period of create statistics table automatically by the StatisticsMetaManager
     */
    @ConfField(mutable = true)
    public static long statistics_manager_sleep_time_sec = 60 * 10;

    // The statistic
    @ConfField
    public static long statistic_cache_columns = 100000;

    /**
     * The collect thread work interval
     */
    @ConfField(mutable = true)
    public static long statistic_collect_interval_sec = 120 * 60;

    /**
     * The column statistic update interval
     */
    @ConfField(mutable = true)
    public static long statistic_update_interval_sec = 24 * 60 * 60;

    /**
     * The row number of sample collect, default 20w rows
     */
    @ConfField(mutable = true)
    public static long statistic_sample_collect_rows = 200000;

    /**
     * statistic collect flag
     */
    @ConfField(mutable = true)
    public static boolean enable_statistic_collect = true;

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
     * eg: where a in (5 elements) and b in (4 elements) and c in (3 elements) and d in (2 elements).
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

    @ConfField
    public static long hive_meta_cache_refresh_interval_s = 3600L * 2L;

    @ConfField
    public static long hive_meta_cache_ttl_s = 3600L * 24L;

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
     * Metastore event processor refresh table column statistic interval in seconds.
     */
    @ConfField(mutable = true)
    public static int hms_refresh_columns_statistic_interval_s = 600;

    /**
     * Used to split files stored in dfs such as object storage
     * or hdfs into smaller files for hive external table
     */
    @ConfField(mutable = true)
    public static long hive_max_split_size = 64L * 1024L * 1024L;

    /**
     * size of iceberg worker pool
     */
    @ConfField(mutable = true)
    public static boolean enable_iceberg_custom_worker_thread = false;

    /**
     * size of iceberg worker pool
     */
    @ConfField(mutable = true)
    public static long iceberg_worker_num_threads = 64;

    /**
     * fe will call es api to get es index shard info every es_state_sync_interval_secs
     */
    @ConfField
    public static long es_state_sync_interval_second = 10;

    /**
     * If set to true, StarRocks will check if the compiled and running versions of Java are compatible
     */
    @ConfField
    public static boolean check_java_version = true;

    /**
     * connection and socket timeout for broker client
     */
    @ConfField
    public static int broker_client_timeout_ms = 10000;

    /**
     * Unused config field, leave it here for backward compatibility
     */
    @Deprecated
    @ConfField(mutable = true)
    public static boolean vectorized_load_enable = true;

    /**
     * Enable pipeline engine load for insert into.
     */
    @ConfField(mutable = true)
    public static boolean enable_pipeline_load_for_insert = false;

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
     * Temporary use, it will be removed later.
     * Set true if using StarOS to manage tablets for StarRocks lake table.
     */
    @ConfField
    public static boolean use_staros = false;
    @ConfField
    public static String starmgr_address = "127.0.0.1:6090";
    @ConfField
    public static boolean integrate_staros = false;

    /**
     * default bucket number when create OLAP table without buckets info
     */
    @ConfField(mutable = true)
    public static int default_bucket_num = 10;

    @ConfField(mutable = true)
    public static boolean enable_experimental_mv = false;
  
    @ConfField
    public static boolean enable_dict_optimize_routine_load = false;

    @ConfField(mutable = true)
    public static boolean enable_dict_optimize_stream_load = false;

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
     * after wait quorom_publish_wait_time_ms, will do quorum publish
     */
    @ConfField(mutable = true)
    public static int quorom_publish_wait_time_ms = 500;
}
