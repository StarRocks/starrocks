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
import com.starrocks.system.BackendCoreStat;

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
            return BackendCoreStat.getAvgNumOfHardwareCoresOfBe() * 16;
        }
        return queryQueueDriverHighWater;
    }

    public static boolean isQueryQueueDriverLowWaterEffective() {
        return queryQueueDriverLowWater >= 0;
    }

    public static int getQueryQueueDriverLowWater() {
        if (queryQueueDriverLowWater == 0) {
            return BackendCoreStat.getAvgNumOfHardwareCoresOfBe() * 8;
        }
        return queryQueueDriverLowWater;
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

    // Don't allow create instance.
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
