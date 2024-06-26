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

package com.starrocks.datacache.copilot;

import com.google.common.collect.Sets;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

public class DataCacheCopilotRepo extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(DataCacheCopilotRepo.class);
    private static final int INSERT_BATCH_SIZE = 5000;
    private long lastFlushTimestamp = 0L;
    private long lastClearTimestamp = 0L;

    public DataCacheCopilotRepo() {
        super("DataCacheCopilotRepo", Config.datacache_copilot_repo_check_interval_sec * 1000);
        lastFlushTimestamp = System.currentTimeMillis();
        lastClearTimestamp = System.currentTimeMillis();
    }

    @Override
    protected void runAfterCatalogReady() {
        if (isSatisfyFlushCondition()) {
            flushStorageToBE();
        }
        if (isSatisfyClearCondition()) {
            clearStaleStats();
        }
    }

    private boolean isSatisfyFlushCondition() {
        if ((System.currentTimeMillis() - lastFlushTimestamp) >
                Config.datacache_copilot_repo_flush_interval_sec * 1000) {
            return true;
        }

        if (DataCacheCopilotStorage.getInstance().getEstimateMemorySize() >=
                Config.datacache_copilot_storage_max_flight_bytes) {
            return true;
        }
        return false;
    }

    private boolean isSatisfyClearCondition() {
        if ((System.currentTimeMillis() - lastClearTimestamp) >
                Config.datacache_copilot_repo_clean_interval_sec * 1000) {
            return true;
        }

        return false;
    }

    public void flushStorageToBE() {
        LOG.info("DataCacheCopilotRepo start to flush data");

        lastFlushTimestamp = System.currentTimeMillis();

        List<AccessLog> accessLogs = DataCacheCopilotStorage.getInstance().exportAccessLogs();

        try {
            InsertSQLBuilder insertSQLBuilder = new InsertSQLBuilder(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    StatsConstants.STATISTICS_DB_NAME,
                    DataCacheCopilotConstants.DATACACHE_COPILOT_STATISTICS_TABLE_NAME);
            // because we have insert limit 'expr_children_limit'(default value is 10,000) in FE conf, so we need to insert batch by batch
            for (AccessLog accessLog : accessLogs) {
                if (insertSQLBuilder.size() < INSERT_BATCH_SIZE) {
                    insertSQLBuilder.addAccessLog(accessLog);
                } else {
                    String insertSQL = insertSQLBuilder.build();
                    executeSQL(insertSQL);
                    insertSQLBuilder.clear();
                }
            }

            // handle remain rows in InsertSQLBuilder
            if (insertSQLBuilder.size() > 0) {
                String insertSQL = insertSQLBuilder.build();
                executeSQL(insertSQL);
            }
        } catch (Exception e) {
            LOG.warn("Failed to flush data to BE, error msg is ", e);
        }
    }

    private void clearStaleStats() {
        LOG.info("DataCacheCopilotRepo start to drop outdated statistics");
        lastClearTimestamp = System.currentTimeMillis();

        try {
            String dropSQL = DeleteSQLBuilder.buildDeleteStatsSQL();
            executeSQL(dropSQL);
        } catch (Exception e) {
            LOG.warn("Failed to drop outdated data, error msg is ", e);
        }
    }

    public List<TStatisticData> collectCopilotStatistics(String sql) {
        StatisticExecutor executor = new StatisticExecutor();
        ConnectContext context = buildConnectContext();
        return executor.executeStatisticDQL(context, sql);
    }

    private void executeSQL(String sql) throws Exception {
        LOG.debug("execute sql: {}", sql);
        buildConnectContext().executeSql(sql);
    }

    private ConnectContext buildConnectContext() {
        ConnectContext context = new ConnectContext();
        // Note: statistics query does not register query id to QeProcessorImpl::coordinatorMap,
        // but QeProcessorImpl::reportExecStatus will check query id,
        // So we must disable report query status from BE to FE
        context.getSessionVariable().setEnableProfile(false);
        // enlarge group_concat max length from 1024 -> 65535
        context.getSessionVariable().setGroupConcatMaxLen(65535);
        // context.getSessionVariable().setQueryTimeoutS((int) Config.statistic_collect_query_timeout);
        // context.getSessionVariable().setEnablePipelineEngine(true);

        // context.setStatisticsContext(true);
        // context.setDatabase(StatsConstants.STATISTICS_DB_NAME);
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        context.setQualifiedUser(UserIdentity.ROOT.getUser());
        context.setQueryId(UUIDUtil.genUUID());
        // context.setExecutionId(UUIDUtil.toTUniqueId(context.getQueryId()));
        context.setStartTime();

        return context;
    }

    private static class DeleteSQLBuilder {
        private static final String CLEAN_SQL_TEMPLATE = "DELETE FROM `%s`.`%s`.`%s` WHERE `%s` <= '%s'";

        public static String buildDeleteStatsSQL() {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String deleteTime = simpleDateFormat.format(
                    System.currentTimeMillis() - Config.datacache_copilot_statistics_keep_sec * 1000L);
            return String.format(CLEAN_SQL_TEMPLATE, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    StatsConstants.STATISTICS_DB_NAME,
                    DataCacheCopilotConstants.DATACACHE_COPILOT_STATISTICS_TABLE_NAME,
                    DataCacheCopilotConstants.ACCESS_TIME_NAME,
                    deleteTime);
        }
    }

    private static class InsertSQLBuilder {
        private final String targetCatalogName;
        private final String targetDbName;
        private final String targetTblName;
        private final List<String> values = new LinkedList<>();

        private InsertSQLBuilder(String targetCatalogName, String targetDbName, String targetTblName) {
            this.targetCatalogName = targetCatalogName;
            this.targetDbName = targetDbName;
            this.targetTblName = targetTblName;
        }

        private void addAccessLog(AccessLog accessLog) {
            String s = String.format("('%s', '%s', '%s', '%s', '%s', from_unixtime(%d, 'yyyy-MM-dd HH:mm:ss'), %d)",
                    accessLog.getCatalogName(), accessLog.getDbName(), accessLog.getTableName(),
                    accessLog.getPartitionName(), accessLog.getColumnName(), accessLog.getAccessTimeSec(),
                    accessLog.getCount());
            values.add(s);
        }

        private void clear() {
            values.clear();
        }

        private int size() {
            return values.size();
        }

        private String build() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("INSERT INTO `%s`.`%s`.`%s` VALUES ", targetCatalogName, targetDbName,
                    targetTblName));
            sb.append(String.join(", ", values));
            return sb.toString();
        }
    }
}
