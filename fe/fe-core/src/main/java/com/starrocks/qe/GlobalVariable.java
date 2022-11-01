// This file is made available under Elastic License 2.0.
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

    public static final String QUERY_QUEUE_ENABLE = "query_queue_enable";
    public static final String QUERY_QUEUE_CONCURRENCY_HARD_LIMIT = "query_queue_concurrency_hard_limit";
    public static final String QUERY_QUEUE_MEM_USED_PCT_HARD_LIMIT = "query_queue_mem_used_pct_hard_limit";
    public static final String QUERY_QUEUE_PENDING_TIMEOUT_SECOND = "query_queue_pending_timeout_second";
    public static final String QUERY_QUEUE_MAX_QUEUED_QUERIES = "query_queue_max_queued_queries";

    @VariableMgr.VarAttr(name = VERSION_COMMENT, flag = VariableMgr.READ_ONLY)
    public static String versionComment = "StarRocks version " + Version.STARROCKS_VERSION;

    @VariableMgr.VarAttr(name = VERSION, flag = VariableMgr.READ_ONLY)
    public static String version = Config.mysql_server_version;

    // 0: table names are stored as specified and comparisons are case sensitive.
    // 1: table names are stored in lowercase on disk and comparisons are not case sensitive.
    // 2: table names are stored as given but compared in lowercase.
    @VariableMgr.VarAttr(name = LOWER_CASE_TABLE_NAMES, flag = VariableMgr.READ_ONLY)
    public static int lowerCaseTableNames = 0;

    @VariableMgr.VarAttr(name = LICENSE, flag = VariableMgr.READ_ONLY)
    public static String license = "Elastic License 2.0";

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
     * Query will be pending when BE is overloaded, if `queryQueueEnable` is true.
     * <p>
     * If the number of running queries of any BE `exceeds queryQueueConcurrencyHardLimit`,
     * or memory usage rate of any BE exceeds `queryQueueMemUsedPctHardLimit`,
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
    @VariableMgr.VarAttr(name = QUERY_QUEUE_ENABLE, flag = VariableMgr.GLOBAL)
    private static boolean queryQueueEnable = false;
    // Effective iff it is positive.
    @VariableMgr.VarAttr(name = QUERY_QUEUE_CONCURRENCY_HARD_LIMIT, flag = VariableMgr.GLOBAL)
    private static int queryQueueConcurrencyHardLimit = 0;
    // Effective iff it is positive.
    @VariableMgr.VarAttr(name = QUERY_QUEUE_MEM_USED_PCT_HARD_LIMIT, flag = VariableMgr.GLOBAL)
    private static double queryQueueMemUsedPctHardLimit = 0;
    @VariableMgr.VarAttr(name = QUERY_QUEUE_PENDING_TIMEOUT_SECOND, flag = VariableMgr.GLOBAL)
    private static int queryQueuePendingTimeoutSecond = 300;
    // Unlimited iff it is non-positive.
    @VariableMgr.VarAttr(name = QUERY_QUEUE_MAX_QUEUED_QUERIES, flag = VariableMgr.GLOBAL)
    private static int queryQueueMaxQueuedQueries = 1024;

    public static boolean isQueryQueueEnable() {
        return queryQueueEnable;
    }

    public static void setQueryQueueEnable(boolean queryQueueEnable) {
        GlobalVariable.queryQueueEnable = queryQueueEnable;
    }

    public static boolean isQueryQueueConcurrencyHardLimitEffective() {
        return queryQueueConcurrencyHardLimit > 0;
    }

    public static int getQueryQueueConcurrencyHardLimit() {
        return queryQueueConcurrencyHardLimit;
    }

    public static void setQueryQueueConcurrencyHardLimit(int queryQueueConcurrencyHardLimit) {
        GlobalVariable.queryQueueConcurrencyHardLimit = queryQueueConcurrencyHardLimit;
    }

    public static boolean isQueryQueueMemUsedPctHardLimitEffective() {
        return queryQueueMemUsedPctHardLimit > 0;
    }

    public static double getQueryQueueMemUsedPctHardLimit() {
        return queryQueueMemUsedPctHardLimit;
    }

    public static void setQueryQueueMemUsedPctHardLimit(double queryQueueMemUsedPctHardLimit) {
        GlobalVariable.queryQueueMemUsedPctHardLimit = queryQueueMemUsedPctHardLimit;
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
