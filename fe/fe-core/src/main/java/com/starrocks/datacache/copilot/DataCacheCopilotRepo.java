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
import java.util.List;
import java.util.Optional;

public class DataCacheCopilotRepo extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(DataCacheCopilotRepo.class);
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

        Optional<String> insertSQL = DataCacheCopilotStorage.getInstance().exportInsertSQL();
        if (insertSQL.isEmpty()) {
            return;
        }
        try {
            LOG.info("execute sql: {}", insertSQL.get());
            buildConnectContext().executeSql(insertSQL.get());
        } catch (Exception e) {
            LOG.warn("Failed to flush data to BE, error msg is ", e);
        }
    }

    private void clearStaleStats() {
        LOG.info("DataCacheCopilotRepo start to drop outdated statistics");
        lastClearTimestamp = System.currentTimeMillis();

        try {
            String dropSQL = SQLBuilder.buildCleanStatsSQL();
            buildConnectContext().executeSql(dropSQL);
        } catch (Exception e) {
            LOG.warn("Failed to drop outdated data, error msg is ", e);
        }
    }

    public List<TStatisticData> collectCopilotStatistics(String sql) {
        StatisticExecutor executor = new StatisticExecutor();
        ConnectContext context = buildConnectContext();
        return executor.executeStatisticDQL(context, sql);
    }

    private ConnectContext buildConnectContext() {
        ConnectContext context = new ConnectContext();
        // Note: statistics query does not register query id to QeProcessorImpl::coordinatorMap,
        // but QeProcessorImpl::reportExecStatus will check query id,
        // So we must disable report query status from BE to FE
        context.getSessionVariable().setEnableProfile(false);
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

    private static class SQLBuilder {
        private static final String CLEAN_SQL_TEMPLATE = "DELETE FROM `%s`.`%s`.`%s` WHERE `%s` <= %s";

        public static String buildCleanStatsSQL() {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String deleteTime = simpleDateFormat.format(
                    System.currentTimeMillis() / 1000 - Config.datacache_copilot_statistics_keep_sec);
            return String.format(CLEAN_SQL_TEMPLATE, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    StatsConstants.STATISTICS_DB_NAME,
                    DataCacheCopilotConstants.DATACACHE_COPILOT_STATISTICS_TABLE_NAME,
                    DataCacheCopilotConstants.ACCESS_TIME_NAME,
                    deleteTime);
        }
    }
}
