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

package com.starrocks.datacache.collector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.statistic.StatsConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

public class TableAccessCollectorRepo extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(TableAccessCollectorRepo.class);
    private static final int INSERT_BATCH_SIZE = 5000;
    private long lastFlushTimestamp = 0L;
    private long lastCleanTimestamp = 0L;

    public TableAccessCollectorRepo() {
        super("TableAccessCollectorRepo", Config.table_access_collector_repo_check_interval_sec * 1000);
        lastFlushTimestamp = System.currentTimeMillis();
        lastCleanTimestamp = System.currentTimeMillis();
    }

    @Override
    protected void runAfterCatalogReady() {
        if (isSatisfyFlushCondition()) {
            flushStorageToBE();
        }
        if (isSatisfyCleanCondition()) {
            clearStaleStats();
        }
    }

    private boolean isSatisfyFlushCondition() {
        if ((System.currentTimeMillis() - lastFlushTimestamp) >
                Config.table_access_collector_flush_sec * 1000) {
            return true;
        }

        if (TableAccessCollectorStorage.getInstance().getEstimateMemorySize() >=
                Config.table_access_collector_max_flight_bytes) {
            return true;
        }
        return false;
    }

    private boolean isSatisfyCleanCondition() {
        if ((System.currentTimeMillis() - lastCleanTimestamp) >
                Config.table_access_collector_clean_sec * 1000) {
            return true;
        }

        return false;
    }

    public void flushStorageToBE() {
        LOG.info("TableAccessCollectorRepo start to flush data");

        lastFlushTimestamp = System.currentTimeMillis();

        List<AccessLog> accessLogs = TableAccessCollectorStorage.getInstance().exportAccessLogs();

        try {
            InsertSQLBuilder insertSQLBuilder = new InsertSQLBuilder();
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
        LOG.info("TableAccessCollectorRepo start to drop outdated statistics");
        lastCleanTimestamp = System.currentTimeMillis();

        try {
            String dropSQL = DeleteSQLBuilder.buildDeleteStatsSQL();
            executeSQL(dropSQL);
        } catch (Exception e) {
            LOG.warn("Failed to drop outdated data, error msg is ", e);
        }
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
        context.setStatisticsContext(true);
        context.setDatabase(StatsConstants.STATISTICS_DB_NAME);
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        context.setQualifiedUser(UserIdentity.ROOT.getUser());
        context.setQueryId(UUIDUtil.genUUID());
        context.setStartTime();
        return context;
    }

    @VisibleForTesting
    public static class DeleteSQLBuilder {
        private static final String CLEAN_SQL_TEMPLATE = "DELETE FROM `%s`.`%s`.`%s` WHERE `%s` <= '%s'";

        public static String buildDeleteStatsSQL() {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String deleteTime = simpleDateFormat.format(
                    System.currentTimeMillis() - Config.table_access_statistics_keep_sec * 1000L);
            return String.format(CLEAN_SQL_TEMPLATE, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    StatsConstants.STATISTICS_DB_NAME,
                    TableAccessCollectorConstants.TABLE_ACCESS_STATISTICS_TABLE_NAME,
                    TableAccessCollectorConstants.ACCESS_TIME_NAME,
                    deleteTime);
        }
    }

    @VisibleForTesting
    public static class InsertSQLBuilder {
        private final String targetCatalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        private final String targetDbName = StatsConstants.STATISTICS_DB_NAME;
        private final String targetTblName = TableAccessCollectorConstants.TABLE_ACCESS_STATISTICS_TABLE_NAME;
        private final List<String> values = new LinkedList<>();

        public void addAccessLog(AccessLog accessLog) {
            String s = String.format("('%s', '%s', '%s', '%s', '%s', from_unixtime(%d, 'yyyy-MM-dd HH:mm:ss'), %d)",
                    accessLog.getCatalogName(), accessLog.getDbName(), accessLog.getTableName(),
                    accessLog.getPartitionName(), accessLog.getColumnName(), accessLog.getAccessTimeSec(),
                    accessLog.getCount());
            values.add(s);
        }

        public void clear() {
            values.clear();
        }

        public int size() {
            return values.size();
        }

        public String build() {
            Preconditions.checkArgument(size() > 0, "InsertSQLBuilder must contains at least one AccessLog");
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("INSERT INTO `%s`.`%s`.`%s` VALUES ", targetCatalogName, targetDbName,
                    targetTblName));
            sb.append(String.join(", ", values));
            return sb.toString();
        }
    }
}
