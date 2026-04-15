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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/GlobalVariable.java

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

package com.starrocks.qe;

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.Version;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.encryption.KeyMgr;
import com.starrocks.system.BackendResourceStat;

import java.lang.reflect.Field;
import java.util.List;

// You can place your global variable in this class with public and VariableMgr.VarAttr annotation.
// You can get this variable from MySQL client with statement `SELECT @@variable_name`,
// and change its value through `SET variable_name = xxx`
// NOTE: If you want access your variable safe, please hold VariableMgr's lock before access.
public final class GlobalVariable {

    public static final String VERSION_COMMENT = "version_comment";
    public static final String VERSION = "version";
    public static final String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";
    public static final String LICENSE = "license";
    public static final String LANGUAGE = "language";
    public static final String INIT_CONNECT = "init_connect";
    public static final String SYSTEM_TIME_ZONE = "system_time_zone";
    public static final String QUERY_CACHE_SIZE = "query_cache_size";
    public static final String DEFAULT_ROWSET_TYPE = "default_rowset_type";
    public static final String CHARACTER_SET_DATABASE = "character_set_database";

    public static final String ENABLE_QUERY_QUEUE_SELECT = "enable_query_queue_select";
    public static final String ENABLE_QUERY_QUEUE_STATISTIC = "enable_query_queue_statistic";
    public static final String ENABLE_QUERY_QUEUE_LOAD = "enable_query_queue_load";
    public static final String ENABLE_GROUP_LEVEL_QUERY_QUEUE = "enable_group_level_query_queue";
    public static final String QUERY_QUEUE_FRESH_RESOURCE_USAGE_INTERVAL_MS =
            "query_queue_fresh_resource_usage_interval_ms";
    public static final String QUERY_QUEUE_CONCURRENCY_LIMIT = "query_queue_concurrency_limit";
    public static final String QUERY_QUEUE_DRIVER_HIGH_WATER = "query_queue_driver_high_water";
    public static final String QUERY_QUEUE_DRIVER_LOW_WATER = "query_queue_driver_low_water";
    public static final String QUERY_QUEUE_MEM_USED_PCT_LIMIT = "query_queue_mem_used_pct_limit";
    public static final String QUERY_QUEUE_CPU_USED_PERMILLE_LIMIT = "query_queue_cpu_used_permille_limit";
    public static final String QUERY_QUEUE_PENDING_TIMEOUT_SECOND = "query_queue_pending_timeout_second";
    public static final String QUERY_QUEUE_MAX_QUEUED_QUERIES = "query_queue_max_queued_queries";
    public static final String ACTIVATE_ALL_ROLES_ON_LOGIN = "activate_all_roles_on_login";
    public static final String ACTIVATE_ALL_ROLES_ON_LOGIN_V2 = "activate_all_roles_on_login_v2";
    public static final String ENABLE_TDE = "enable_tde";
    public static final String ARROW_FLIGHT_PROXY = "arrow_flight_proxy";
    public static final String ARROW_FLIGHT_PROXY_ENABLED = "arrow_flight_proxy_enabled";
    public static final String MAX_UNKNOWN_STRING_META_LENGTH = "max_unknown_string_meta_length";
    public static final String ENABLE_REDUCE_CAST_VARCHAR_LENGTH_INHERITANCE =
            "enable_reduce_cast_varchar_length_inheritance";
    public static final String ENABLE_REDUCE_CAST_VARCHAR_EXPR_SYNC_TYPE =
            "enable_reduce_cast_varchar_expr_sync_type";

    //AutoMV's MVLifecycle
    public static final String ENABLE_AUTOMV_LIFECYCLE_KEEPER = "enable_automv_lifecycle_keeper";
    public static final String AUTOMV_LIFECYCLE_INFANT_ABORTION_MAX_TIME = "automv_lifecycle_infant_abortion_max_time";
    public static final String AUTOMV_LIFECYCLE_INITIAL_REFRESH_MAX_TIME = "automv_lifecycle_initial_refresh_max_time";
    public static final String AUTOMV_LIFECYCLE_INTERNSHIP_PERIOD = "automv_lifecycle_internship_period";
    public static final String AUTOMV_LIFECYCLE_HIT_RATIO_HWM = "automv_lifecycle_hit_ratio_hwm";
    public static final String AUTOMV_LIFECYCLE_HIT_RATIO_LWM = "automv_lifecycle_hit_ratio_lwm";
    public static final String AUTOMV_LIFECYCLE_REVIVE_WAITING_MAX_TIME = "automv_lifecycle_revive_waiting_max_time";
    public static final String AUTOMV_LIFECYCLE_PERFORMANCE_EVALUATION_INTERVAL =
            "automv_lifecycle_performance_evaluation_interval";
    public static final String AUTOMV_LIFECYCLE_EXTINCTION_RETENTION_MAX_TIME =
            "automv_lifecycle_extinction_retention_max_time";

    public static final String AUTOMV_LIFECYCLE_MV_RECOMMENDATION_INTERVAL =
            "automv_lifecycle_mv_recommendation_interval";
    public static final String AUTOMV_UNPARTITIONED_MV_CARD_MAX = "automv_unpartitioned_mv_card_max";
    public static final String AUTOMV_PARTITIONED_MV_CARD_MAX = "automv_partitioned_mv_card_max";
    public static final String AUTOMV_PER_LATTICE_MV_LIMIT = "automv_per_lattice_mv_limit";
    public static final String AUTOMV_PER_LATTICE_MV_SELECTIVITY_RATIO = "automv_per_lattice_mv_selectivity_ratio";
    public static final String AUTOMV_QUERY_LATENCY_LOW_BOUND_MS = "automv_query_latency_low_bound_ms";
    public static final String AUTOMV_COLOCATE_MV_DIMENSIONS_LIMIT = "automv_colocate_mv_dimensions_limit";
    public static final String AUTOMV_PER_LATTICE_NODE_LIMIT = "automv_per_lattice_node_limit";

    public static final String AUTOMV_PREFER_RANGE_PARTITION = "automv_prefer_range_partition";

    public static final String AUTOMV_STRING_TIME_FORMATS = "automv_string_time_formats";

    public static final String AUTOMV_ENABLE_11MV_SELECTIVITY_EVALUATION = "automv_enable_11mv_selectivity_evaluation";
    public static final String AUTOMV_RECOMMENDATIONS_TASK_EXPIRE_TIME  = "automv_recommendations_task_expire_time";
    public static final String AUTOMV_RECOMMENDATIONS_TASK_PENDING_LIMIT  = "automv_recommendations_task_pending_limit";

    // cngroup
    public static final String CNGROUP_RESOURCE_USAGE_FRESH_RATIO = "cngroup_resource_usage_fresh_ratio";
    public static final String CNGROUP_LOW_WATERMARK_RUNNING_QUERY_COUNT  = "cngroup_low_watermark_running_query_count";
    public static final String CNGROUP_LOW_WATERMARK_CPU_USED_PERMILLE = "cngroup_low_watermark_cpu_used_permille";
    public static final String CNGROUP_SCHEDULE_MODE = "cngroup_schedule_mode";

    public static final String ENABLE_QUERY_HISTORY = "enable_query_history";

    public static final String QUERY_HISTORY_KEEP_SECONDS = "query_history_keep_seconds";

    public static final String QUERY_HISTORY_LOAD_INTERVAL_SECONDS = "query_history_load_interval_seconds";

    public static final String ENABLE_SPM_CAPTURE = "enable_plan_capture";

    public static final String SPM_CAPTURE_INTERVAL_SECONDS = "plan_capture_interval_seconds";

    public static final String SPM_CAPTURE_INCLUDE_TABLE_PATTERN = "plan_capture_include_pattern";

    public static final String ENABLE_TABLE_NAME_CASE_INSENSITIVE = "enable_table_name_case_insensitive";

    public static final String RUN_MODE = "run_mode";


    @VariableMgr.VarAttr(name = VERSION_COMMENT, flag = VariableMgr.READ_ONLY)
    public static String versionComment = Version.STARROCKS_VERSION + "-" + Version.STARROCKS_COMMIT_HASH;

    @VariableMgr.VarAttr(name = VERSION, flag = VariableMgr.READ_ONLY)
    public static String version = Config.mysql_server_version;

    // 0: table names are stored as specified and comparisons are case sensitive.
    // 1: table names are stored in lowercase on disk and comparisons are not case sensitive.
    // 2: table names are stored as given but compared in lowercase.
    @VariableMgr.VarAttr(name = LOWER_CASE_TABLE_NAMES, flag = VariableMgr.READ_ONLY)
    public static int lowerCaseTableNames = 0;

    @VariableMgr.VarAttr(name = LICENSE, flag = VariableMgr.READ_ONLY)
    public static String license = "Apache License 2.0";

    @VariableMgr.VarAttr(name = LANGUAGE, flag = VariableMgr.READ_ONLY)
    public static String language = "/starrocks/share/english/";

    // A string to be executed by the server for each client that connects
    @VariableMgr.VarAttr(name = INIT_CONNECT, flag = VariableMgr.GLOBAL)
    private static volatile String initConnect = "";

    // A string to be executed by the server for each client that connects
    @VariableMgr.VarAttr(name = SYSTEM_TIME_ZONE, flag = VariableMgr.READ_ONLY)
    public static String systemTimeZone = TimeUtils.getSystemTimeZone().getID();

    // The amount of memory allocated for caching query results
    @VariableMgr.VarAttr(name = QUERY_CACHE_SIZE, flag = VariableMgr.GLOBAL)
    private static volatile long queryCacheSize = 1048576;

    @VariableMgr.VarAttr(name = DEFAULT_ROWSET_TYPE, flag = VariableMgr.GLOBAL)
    public static volatile String defaultRowsetType = "alpha";

    @VariableMgr.VarAttr(name = CHARACTER_SET_DATABASE, flag = VariableMgr.GLOBAL)
    public static volatile String characterSetDatabase = "utf8";

    // Whether the Performance Schema is enabled
    // Compatible with jdbc that version > 8.0.15
    @VariableMgr.VarAttr(name = "performance_schema", flag = VariableMgr.READ_ONLY)
    private static boolean performanceSchema = false;

    /**
     * This configuration controls case sensitivity for SQL catalog/database/table names.
     * When enabled, these database object names are treated as case-insensitive.
     * IMPORTANT NOTES:
     * - This setting can ONLY be configured during the initial cluster setup via
     *   {@link Config#enable_table_name_case_insensitive} on the FE leader node
     *
     * - Once set during first initialization, this value is IMMUTABLE and will NOT
     *   be modified by any subsequent operations including:
     *   * Cluster upgrades/downgrades
     *   * Fe node restarts
     *   * Any other maintenance operations
     *
     * - During FE restart or leader failover, if the leader node's
     *   {@link Config#enable_table_name_case_insensitive} differs from the cluster's initially
     *   recorded enableTableNameCaseInsensitive value, the leader node will FAIL to start
     *
     * - Existing clusters CANNOT modify this value. it can only be configured
     *   in NEW clusters during initial setup
     */
    @VariableMgr.VarAttr(name = ENABLE_TABLE_NAME_CASE_INSENSITIVE, flag = VariableMgr.READ_ONLY)
    public static boolean enableTableNameCaseInsensitive = false;

    @VariableMgr.VarAttr(name = RUN_MODE, flag = VariableMgr.READ_ONLY)
    public static String runMode = Config.run_mode;

    /**
     * Query will be pending when BE is overloaded, if `enableQueryQueueXxx` is true.
     * <p>
     * If the number of running queries of any BE `exceeds queryQueueConcurrencyLimit`,
     * or memory usage rate of any BE exceeds `queryQueueMemUsedPctLimit`,
     * the current query will be pending or failed:
     * - if the number of pending queries in this FE exceeds `queryQueueMaxQueuedQueries`,
     * the query will be failed.
     * - otherwise, the query will be pending until all the BEs aren't overloaded anymore
     * or timeout `queryQueuePendingTimeoutSecond`.
     * <p>
     * Every BE reports at interval the resources containing the number of running queries and memory usage rate
     * to the FE leader. And the FE leader synchronizes the resource usage info to FE followers by RPC.
     * <p>
     * The queries only using schema meta will never been queued, because a MySQL client will
     * query schema meta after the connection is established.
     */
    @VariableMgr.VarAttr(name = ENABLE_QUERY_QUEUE_SELECT, flag = VariableMgr.GLOBAL)
    private static boolean enableQueryQueueSelect = false;
    @VariableMgr.VarAttr(name = ENABLE_QUERY_QUEUE_STATISTIC, flag = VariableMgr.GLOBAL)
    private static boolean enableQueryQueueStatistic = false;
    @VariableMgr.VarAttr(name = ENABLE_QUERY_QUEUE_LOAD, flag = VariableMgr.GLOBAL)
    private static boolean enableQueryQueueLoad = false;
    @VariableMgr.VarAttr(name = ENABLE_GROUP_LEVEL_QUERY_QUEUE, flag = VariableMgr.GLOBAL)
    private static boolean enableGroupLevelQueryQueue = false;
    // Use the resource usage, only when the duration from the last report is within this interval.
    @VariableMgr.VarAttr(name = QUERY_QUEUE_FRESH_RESOURCE_USAGE_INTERVAL_MS, flag = VariableMgr.GLOBAL)
    private static long queryQueueResourceUsageIntervalMs = 5000;
    // Effective iff it is positive.
    @VariableMgr.VarAttr(name = QUERY_QUEUE_CONCURRENCY_LIMIT, flag = VariableMgr.GLOBAL)
    private static int queryQueueConcurrencyLimit = 0;

    // Effective iff it is non-negative.
    @VariableMgr.VarAttr(name = QUERY_QUEUE_DRIVER_HIGH_WATER, flag = VariableMgr.GLOBAL)
    private static int queryQueueDriverHighWater = -1;

    // Effective iff it is non-negative.
    @VariableMgr.VarAttr(name = QUERY_QUEUE_DRIVER_LOW_WATER, flag = VariableMgr.GLOBAL)
    private static int queryQueueDriverLowWater = -1;

    // Effective iff it is positive.
    @VariableMgr.VarAttr(name = QUERY_QUEUE_MEM_USED_PCT_LIMIT, flag = VariableMgr.GLOBAL)
    private static double queryQueueMemUsedPctLimit = 0;
    // Effective iff it is positive.
    @VariableMgr.VarAttr(name = QUERY_QUEUE_CPU_USED_PERMILLE_LIMIT, flag = VariableMgr.GLOBAL)
    private static int queryQueueCpuUsedPermilleLimit = 0;
    @VariableMgr.VarAttr(name = QUERY_QUEUE_PENDING_TIMEOUT_SECOND, flag = VariableMgr.GLOBAL)
    private static int queryQueuePendingTimeoutSecond = 300;
    // Unlimited iff it is non-positive.
    @VariableMgr.VarAttr(name = QUERY_QUEUE_MAX_QUEUED_QUERIES, flag = VariableMgr.GLOBAL)
    private static int queryQueueMaxQueuedQueries = 1024;

    @VariableMgr.VarAttr(name = ACTIVATE_ALL_ROLES_ON_LOGIN_V2, flag = VariableMgr.GLOBAL,
            alias = ACTIVATE_ALL_ROLES_ON_LOGIN, show = ACTIVATE_ALL_ROLES_ON_LOGIN)
    private static boolean activateAllRolesOnLogin = false;

    @VariableMgr.VarAttr(name = ENABLE_TDE, flag = VariableMgr.GLOBAL | VariableMgr.READ_ONLY)
    public static boolean enableTde = KeyMgr.isEncrypted();

    // AutoMV's Lifecycle
    @VariableMgr.VarAttr(name = ENABLE_AUTOMV_LIFECYCLE_KEEPER, flag = VariableMgr.GLOBAL)
    private static boolean enableAutoMVLifecycleKeeper = false;
    @VariableMgr.VarAttr(name = AUTOMV_LIFECYCLE_INFANT_ABORTION_MAX_TIME, flag = VariableMgr.GLOBAL)
    private static long autoMVLifecycleInfantAbortionMaxTime = 3600L;
    @VariableMgr.VarAttr(name = AUTOMV_LIFECYCLE_INITIAL_REFRESH_MAX_TIME, flag = VariableMgr.GLOBAL)
    private static long autoMVLifecycleInitialRefreshMaxTime = 7200L;
    @VariableMgr.VarAttr(name = AUTOMV_LIFECYCLE_INTERNSHIP_PERIOD, flag = VariableMgr.GLOBAL)
    private static long autoMVLifecycleInternshipPeriod = 259200L;
    @VariableMgr.VarAttr(name = AUTOMV_LIFECYCLE_HIT_RATIO_HWM, flag = VariableMgr.GLOBAL)
    private static double autoMVLifecycleHitRatioHwm = 0.5;
    @VariableMgr.VarAttr(name = AUTOMV_LIFECYCLE_HIT_RATIO_LWM, flag = VariableMgr.GLOBAL)
    private static double autoMVLifecycleHitRatioLwm = 0.0;
    @VariableMgr.VarAttr(name = AUTOMV_LIFECYCLE_REVIVE_WAITING_MAX_TIME, flag = VariableMgr.GLOBAL)
    private static long autoMVLifecycleReviveWaitingMaxTime = 259200L;
    @VariableMgr.VarAttr(name = AUTOMV_LIFECYCLE_PERFORMANCE_EVALUATION_INTERVAL, flag = VariableMgr.GLOBAL)
    private static long autoMVLifecyclePerformanceEvaluationInterval = 300L;
    @VariableMgr.VarAttr(name = AUTOMV_LIFECYCLE_EXTINCTION_RETENTION_MAX_TIME, flag = VariableMgr.GLOBAL)
    private static long autoMVLifecycleExtinctionRetentionMaxTime = 604800L;

    @VariableMgr.VarAttr(name = AUTOMV_LIFECYCLE_MV_RECOMMENDATION_INTERVAL, flag = VariableMgr.GLOBAL)
    private static long autoMVLifecycleMVRecommendationInterval = 3600L;
    @VariableMgr.VarAttr(name = AUTOMV_UNPARTITIONED_MV_CARD_MAX, flag = VariableMgr.GLOBAL)
    private static double autoMVUnpartitionedMVCardMax = 10_000_000.0;
    @VariableMgr.VarAttr(name = AUTOMV_PARTITIONED_MV_CARD_MAX, flag = VariableMgr.GLOBAL)
    private static double autoMVPartitionedMVCardMax = 1_000_000_000.0;
    @VariableMgr.VarAttr(name = AUTOMV_PER_LATTICE_MV_LIMIT, flag = VariableMgr.GLOBAL)
    private static int autoMVPerLatticeMVLimit = 20;
    @VariableMgr.VarAttr(name = AUTOMV_PER_LATTICE_MV_SELECTIVITY_RATIO, flag = VariableMgr.GLOBAL)
    private static double autoMVPerLatticeMvSelectivityRatio = 0.36787944117144233;
    @VariableMgr.VarAttr(name = AUTOMV_QUERY_LATENCY_LOW_BOUND_MS)
    private static long autoMVQueryLatencyLowBoundMs = 500;

    @VariableMgr.VarAttr(name = AUTOMV_COLOCATE_MV_DIMENSIONS_LIMIT)
    private static int autoMVColocateMVDimensionsLimit = 6;

    @VariableMgr.VarAttr(name = AUTOMV_PER_LATTICE_NODE_LIMIT, flag = VariableMgr.INVISIBLE)
    private static int autoMVPerLatticeNodeLimit = 100;

    @VariableMgr.VarAttr(name = AUTOMV_PREFER_RANGE_PARTITION)
    private static boolean autoMVPreferRangePartition = true;

    @VariableMgr.VarAttr(name = AUTOMV_STRING_TIME_FORMATS)
    private static String autoMVStringTimeFormats = "%Y%m%d,%Y-%m-%d";

    @VariableMgr.VarAttr(name = AUTOMV_ENABLE_11MV_SELECTIVITY_EVALUATION)
    private static boolean autoMVEnable11mvSelectivityEvaluation = true;

    @VariableMgr.VarAttr(name = AUTOMV_RECOMMENDATIONS_TASK_EXPIRE_TIME)
    private static long autoMVRecommendationsTaskExpireTime = 24 * 3600L;

    @VariableMgr.VarAttr(name = AUTOMV_RECOMMENDATIONS_TASK_PENDING_LIMIT)
    private static long autoMVRecommendationsTaskPendingLimit = 100;

    // Arrow Flight SQL proxy endpoint. Format: "hostname:port" or "grpcs://hostname:port" for TLS.
    @VariableMgr.VarAttr(name = ARROW_FLIGHT_PROXY, flag = VariableMgr.GLOBAL)
    private static volatile String arrowFlightProxy = "";

    // Enable Arrow Flight SQL proxy mode.
    @VariableMgr.VarAttr(name = ARROW_FLIGHT_PROXY_ENABLED, flag = VariableMgr.GLOBAL)
    private static volatile boolean arrowFlightProxyEnabled = true;

    @VariableMgr.VarAttr(name = MAX_UNKNOWN_STRING_META_LENGTH, flag = VariableMgr.GLOBAL)
    private static int maxUnknownStringMetaLength = 64;

    @VariableMgr.VarAttr(name = ENABLE_REDUCE_CAST_VARCHAR_LENGTH_INHERITANCE, flag = VariableMgr.GLOBAL)
    private static boolean enableReduceCastVarcharLengthInheritance = false;

    @VariableMgr.VarAttr(name = ENABLE_REDUCE_CAST_VARCHAR_EXPR_SYNC_TYPE, flag = VariableMgr.GLOBAL)
    private static boolean enableReduceCastVarcharExprSyncType = false;
    @VariableMgr.VarAttr(name = CNGROUP_RESOURCE_USAGE_FRESH_RATIO)
    private static double cngroupResourceUsageFreshRatio = 0.5;

    @VariableMgr.VarAttr(name = CNGROUP_LOW_WATERMARK_RUNNING_QUERY_COUNT)
    private static long cngroupLowWatermarkRunningQueryCount = 8;

    @VariableMgr.VarAttr(name = CNGROUP_LOW_WATERMARK_CPU_USED_PERMILLE)
    private static long cngroupLowWatermarkCPUUsedPermille = 600;

    @VariableMgr.VarAttr(name = CNGROUP_SCHEDULE_MODE)
    private static String cngroupScheduleMode = "standard";

    @VariableMgr.VarAttr(name = ENABLE_QUERY_HISTORY, flag = VariableMgr.GLOBAL)
    public static boolean enableQueryHistory = false;

    @VariableMgr.VarAttr(name = QUERY_HISTORY_KEEP_SECONDS, flag = VariableMgr.GLOBAL)
    public static long queryHistoryKeepSeconds = 86400 * 3; // 3 days

    @VariableMgr.VarAttr(name = QUERY_HISTORY_LOAD_INTERVAL_SECONDS, flag = VariableMgr.GLOBAL)
    public static long queryHistoryLoadIntervalSeconds = 60 * 15; // 15min

    @VariableMgr.VarAttr(name = ENABLE_SPM_CAPTURE, flag = VariableMgr.GLOBAL)
    public static boolean enableSPMCapture = false;

    @VariableMgr.VarAttr(name = SPM_CAPTURE_INTERVAL_SECONDS, flag = VariableMgr.GLOBAL)
    public static long spmCaptureIntervalSeconds = 60 * 60 * 3; // 3 hour

    @VariableMgr.VarAttr(name = SPM_CAPTURE_INCLUDE_TABLE_PATTERN, flag = VariableMgr.GLOBAL)
    public static String spmCaptureIncludeTablePattern = ".*";

    public static boolean isEnableQueryHistory() {
        return enableQueryHistory;
    }

    public static boolean isEnableQueryQueueSelect() {
        return enableQueryQueueSelect;
    }

    public static void setEnableQueryQueueSelect(boolean enableQueryQueueSelect) {
        GlobalVariable.enableQueryQueueSelect = enableQueryQueueSelect;
    }

    public static boolean isEnableQueryQueueStatistic() {
        return enableQueryQueueStatistic;
    }

    public static void setEnableQueryQueueStatistic(boolean enableQueryQueueStatistic) {
        GlobalVariable.enableQueryQueueStatistic = enableQueryQueueStatistic;
    }

    public static boolean isEnableQueryQueueLoad() {
        return enableQueryQueueLoad;
    }

    public static void setEnableQueryQueueLoad(boolean enableQueryQueueLoad) {
        GlobalVariable.enableQueryQueueLoad = enableQueryQueueLoad;
    }

    public static boolean isEnableGroupLevelQueryQueue() {
        return enableGroupLevelQueryQueue;
    }

    public static void setEnableGroupLevelQueryQueue(boolean enableGroupLevelQueryQueue) {
        GlobalVariable.enableGroupLevelQueryQueue = enableGroupLevelQueryQueue;
    }

    public static long getQueryQueueResourceUsageIntervalMs() {
        return queryQueueResourceUsageIntervalMs;
    }

    public static void setQueryQueueResourceUsageIntervalMs(long queryQueueResourceUsageIntervalMs) {
        GlobalVariable.queryQueueResourceUsageIntervalMs = queryQueueResourceUsageIntervalMs;
    }

    public static boolean isQueryQueueConcurrencyLimitEffective() {
        return queryQueueConcurrencyLimit > 0;
    }

    public static int getQueryQueueConcurrencyLimit() {
        return queryQueueConcurrencyLimit;
    }

    public static void setQueryQueueConcurrencyLimit(int queryQueueConcurrencyLimit) {
        GlobalVariable.queryQueueConcurrencyLimit = queryQueueConcurrencyLimit;
    }

    public static boolean isQueryQueueDriverHighWaterEffective() {
        return queryQueueDriverHighWater >= 0;
    }

    public static int getQueryQueueDriverHighWater() {
        if (queryQueueDriverHighWater == 0) {
            return BackendResourceStat.getInstance().getAvgNumCoresOfBe() * 16;
        }
        return queryQueueDriverHighWater;
    }

    public static void setQueryQueueDriverHighWater(int queryQueueDriverHighWater) {
        GlobalVariable.queryQueueDriverHighWater = queryQueueDriverHighWater;
    }

    public static boolean isQueryQueueDriverLowWaterEffective() {
        return queryQueueDriverLowWater >= 0;
    }

    public static int getQueryQueueDriverLowWater() {
        if (queryQueueDriverLowWater == 0) {
            return BackendResourceStat.getInstance().getAvgNumCoresOfBe() * 8;
        }
        return queryQueueDriverLowWater;
    }

    public static void setQueryQueueDriverLowWater(int queryQueueDriverLowWater) {
        GlobalVariable.queryQueueDriverLowWater = queryQueueDriverLowWater;
    }

    public static boolean isQueryQueueMemUsedPctLimitEffective() {
        return queryQueueMemUsedPctLimit > 0;
    }

    public static double getQueryQueueMemUsedPctLimit() {
        return queryQueueMemUsedPctLimit;
    }

    public static void setQueryQueueMemUsedPctLimit(double queryQueueMemUsedPctLimit) {
        GlobalVariable.queryQueueMemUsedPctLimit = queryQueueMemUsedPctLimit;
    }

    public static boolean isQueryQueueCpuUsedPermilleLimitEffective() {
        return queryQueueCpuUsedPermilleLimit > 0;
    }

    public static int getQueryQueueCpuUsedPermilleLimit() {
        return queryQueueCpuUsedPermilleLimit;
    }

    public static void setQueryQueueCpuUsedPermilleLimit(int queryQueueCpuUsedPermilleLimit) {
        GlobalVariable.queryQueueCpuUsedPermilleLimit = queryQueueCpuUsedPermilleLimit;
    }

    public static int getQueryQueuePendingTimeoutSecond() {
        return queryQueuePendingTimeoutSecond;
    }

    public static void setQueryQueuePendingTimeoutSecond(int queryQueuePendingTimeoutSecond) {
        GlobalVariable.queryQueuePendingTimeoutSecond = queryQueuePendingTimeoutSecond;
    }

    public static boolean isQueryQueueMaxQueuedQueriesEffective() {
        return queryQueueMaxQueuedQueries > 0;
    }

    public static int getQueryQueueMaxQueuedQueries() {
        return queryQueueMaxQueuedQueries;
    }

    public static void setQueryQueueMaxQueuedQueries(int queryQueueMaxQueuedQueries) {
        GlobalVariable.queryQueueMaxQueuedQueries = queryQueueMaxQueuedQueries;
    }

    public static boolean isActivateAllRolesOnLogin() {
        return activateAllRolesOnLogin;
    }

    public static void setActivateAllRolesOnLogin(boolean activateAllRolesOnLogin) {
        GlobalVariable.activateAllRolesOnLogin = activateAllRolesOnLogin;
    }

    //AutoMV's Lifecycle management

    public static void setEnableAutoMVLifecycleKeeper(boolean value) {
        enableAutoMVLifecycleKeeper = value;
    }

    public static void setAutoMVLifecycleInfantAbortionMaxTime(long value) {
        autoMVLifecycleInfantAbortionMaxTime = value;
    }

    public static void setAutoMVLifecycleInitialRefreshMaxTime(long value) {
        autoMVLifecycleInitialRefreshMaxTime = value;
    }

    public static void setAutoMVLifecycleInternshipPeriod(long value) {
        autoMVLifecycleInternshipPeriod = value;
    }

    public static void setAutoMVLifecycleHitRatioHwm(double value) {
        autoMVLifecycleHitRatioHwm = value;
    }

    public static void setAutoMVLifecycleHitRatioLwm(double value) {
        autoMVLifecycleHitRatioLwm = value;
    }

    public static void setAutoMVLifecycleReviveWaitingMaxTime(long value) {
        autoMVLifecycleReviveWaitingMaxTime = value;
    }

    public static void setAutoMVLifecyclePerformanceEvaluationInterval(long value) {
        autoMVLifecyclePerformanceEvaluationInterval = value;
    }

    public static void setAutoMVLifecycleExtinctionRetentionMaxTime(long value) {
        autoMVLifecycleExtinctionRetentionMaxTime = value;
    }

    public static boolean isEnableAutoMVLifecycleKeeper() {
        return enableAutoMVLifecycleKeeper;
    }

    public static long getAutoMVLifecycleInfantAbortionMaxTime() {
        return autoMVLifecycleInfantAbortionMaxTime;
    }

    public static long getAutoMVLifecycleInitialRefreshMaxTime() {
        return autoMVLifecycleInitialRefreshMaxTime;
    }

    public static long getAutoMVLifecycleInternshipPeriod() {
        return autoMVLifecycleInternshipPeriod;
    }

    public static double getAutoMVLifecycleHitRatioHwm() {
        return autoMVLifecycleHitRatioHwm;
    }

    public static double getAutoMVLifecycleHitRatioLwm() {
        return autoMVLifecycleHitRatioLwm;
    }

    public static long getAutoMVLifecycleReviveWaitingMaxTime() {
        return autoMVLifecycleReviveWaitingMaxTime;
    }

    public static long getAutoMVLifecyclePerformanceEvaluationInterval() {
        return autoMVLifecyclePerformanceEvaluationInterval;
    }

    public static long getAutoMVLifecycleExtinctionRetentionMaxTime() {
        return autoMVLifecycleExtinctionRetentionMaxTime;
    }

    public static void setAutoMVLifecycleMVRecommendationInterval(long interval) {
        autoMVLifecycleMVRecommendationInterval = interval;
    }

    public static long getAutoMVLifecycleMVRecommendationInterval() {
        return autoMVLifecycleMVRecommendationInterval;
    }

    public static void setAutoMVUnpartitionedMVCardMax(double value) {
        autoMVUnpartitionedMVCardMax = value;
    }

    public static double getAutoMVUnpartitionedMVCardMax() {
        return autoMVUnpartitionedMVCardMax;
    }

    public static void setAutoMVPartitionedMVCardMax(double value) {
        autoMVPartitionedMVCardMax = value;
    }

    public static double getAutoMVPartitionedMVCardMax() {
        return autoMVPartitionedMVCardMax;
    }

    public static void setAutoMVPerLatticeMVLimit(int value) {
        autoMVPerLatticeMVLimit = value;
    }

    public static int getAutoMVPerLatticeMVLimit() {
        return autoMVPerLatticeMVLimit;
    }

    public static void setAutoMVPerLatticeMVSelectivityRatio(double value) {
        autoMVPerLatticeMvSelectivityRatio = value;
    }

    public static double getAutoMVPerLatticeMVSelectivityRatio() {
        return autoMVPerLatticeMvSelectivityRatio;
    }

    public static void setAutoMVQueryLatencyLowBoundMs(long lowBoundMs) {
        autoMVQueryLatencyLowBoundMs = lowBoundMs;
    }

    public static long getAutoMVQueryLatencyLowBoundMs() {
        return autoMVQueryLatencyLowBoundMs;
    }

    public static void setAutoMVColocateMVDimensionsLimit(int limit) {
        autoMVColocateMVDimensionsLimit = limit;
    }

    public static int getAutoMVColocateMVDimensionsLimit() {
        return autoMVColocateMVDimensionsLimit;
    }

    public static void setAutoMVPerLatticeNodeLimit(int limit) {
        autoMVPerLatticeMVLimit = limit;
    }

    public static int getAutoMVPerLatticeNodeLimit() {
        return autoMVPerLatticeNodeLimit;
    }

    public static void setAutoMVPreferRangePartition(boolean on) {
        autoMVPreferRangePartition = on;
    }

    public static boolean isAutoMVPreferRangePartition() {
        return autoMVPreferRangePartition;
    }

    public static void setAutoMVStringTimeFormats(String timeFmts) {
        autoMVStringTimeFormats = timeFmts;
    }

    public static String getAutoMVStringTimeFormats() {
        return autoMVStringTimeFormats;
    }

    public static void setCngroupScheduleMode(String mode) {
        cngroupScheduleMode = mode;
    }

    public static String getCngroupScheduleMode() {
        return cngroupScheduleMode;
    }

    // Don't allow create instance.

    public static void setAutoMVEnable11mvSelectivityEvaluation(boolean flag) {
        autoMVEnable11mvSelectivityEvaluation = flag;
    }

    public static boolean isAutoMVEnable11mvSelectivityEvaluation() {
        return autoMVEnable11mvSelectivityEvaluation;
    }

    public static void setAutoMVRecommendationsTaskExpireTime(long value) {
        autoMVRecommendationsTaskExpireTime = value;
    }

    public static long getAutoMVRecommendationsTaskExpireTime() {
        return autoMVRecommendationsTaskExpireTime;
    }

    public static void setAutoMVRecommendationsTaskPendingLimit(long value) {
        autoMVRecommendationsTaskPendingLimit = value;
    }

    public static long getAutoMVRecommendationsTaskPendingLimit() {
        return autoMVRecommendationsTaskPendingLimit;
    }

    public static String getArrowFlightProxy() {
        return arrowFlightProxy;
    }

    public static void setArrowFlightProxy(String arrowFlightProxy) {
        GlobalVariable.arrowFlightProxy = arrowFlightProxy;
    }

    public static boolean isArrowFlightProxyEnabled() {
        return arrowFlightProxyEnabled;
    }

    public static void setArrowFlightProxyEnabled(boolean arrowFlightProxyEnabled) {
        GlobalVariable.arrowFlightProxyEnabled = arrowFlightProxyEnabled;
    }

    public static int getMaxUnknownStringMetaLength() {
        if (maxUnknownStringMetaLength <= 0) {
            return 64;
        }
        return maxUnknownStringMetaLength;
    }

    public static void setCngroupResourceUsageFreshRatio(double value) {
        cngroupResourceUsageFreshRatio = value;
    }

    public static boolean isEnableReduceCastVarcharLengthInheritance() {
        return enableReduceCastVarcharLengthInheritance;
    }

    public static void setEnableReduceCastVarcharLengthInheritance(boolean enableReduceCastVarcharLengthInheritance) {
        GlobalVariable.enableReduceCastVarcharLengthInheritance = enableReduceCastVarcharLengthInheritance;
    }

    public static boolean isEnableReduceCastVarcharExprSyncType() {
        return enableReduceCastVarcharExprSyncType;
    }

    public static void setEnableReduceCastVarcharExprSyncType(boolean enableReduceCastVarcharExprSyncType) {
        GlobalVariable.enableReduceCastVarcharExprSyncType = enableReduceCastVarcharExprSyncType;
    }

    public static double getCngroupResourceUsageFreshRatio() {
        return cngroupResourceUsageFreshRatio;
    }

    public static void setCngroupLowWatermarkRunningQueryCount(long value) {
        cngroupLowWatermarkRunningQueryCount = value;
    }

    public static long getCngroupLowWatermarkRunningQueryCount() {
        return cngroupLowWatermarkRunningQueryCount;
    }

    public static void setCngroupLowWatermarkCPUUsedPermille(long value) {
        cngroupLowWatermarkCPUUsedPermille = value;
    }

    public static long getCngroupLowWatermarkCPUUsedPermille() {
        return cngroupLowWatermarkCPUUsedPermille;
    }

    private GlobalVariable() {

    }

    public static List<String> getAllGlobalVarNames() {
        List<String> varNames = Lists.newArrayList();
        for (Field field : GlobalVariable.class.getDeclaredFields()) {
            VariableMgr.VarAttr attr = field.getAnnotation(VariableMgr.VarAttr.class);
            if (attr == null || attr.flag() != VariableMgr.GLOBAL) {
                continue;
            }
            varNames.add(attr.name());
        }
        return varNames;
    }
}
