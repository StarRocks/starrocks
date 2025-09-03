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

package com.starrocks.sql.spm;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;
import com.starrocks.common.AuditLog;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.summary.QueryHistory;
import com.starrocks.summary.QueryHistoryMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SPMAutoCapturer extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(SPMAutoCapturer.class);

    private LocalDateTime lastWorkTime =
            LocalDateTime.now().minusSeconds(GlobalVariable.spmCaptureIntervalSeconds * 2);

    private ConnectContext connect;

    public SPMAutoCapturer() {
        super("SPMAutoCapturer");
    }

    @Override
    public long getInterval() {
        return GlobalVariable.spmCaptureIntervalSeconds * 100;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!GlobalVariable.enableSPMCapture) {
            return;
        }

        if (lastWorkTime.plusSeconds(GlobalVariable.spmCaptureIntervalSeconds).isAfter(LocalDateTime.now())) {
            return;
        }

        // check statistic table state
        if (!StatisticUtils.checkStatisticTables(
                List.of(StatsConstants.SPM_BASELINE_TABLE_NAME, StatsConstants.QUERY_HISTORY_TABLE_NAME))) {
            return;
        }

        // non query history data, skip
        QueryHistoryMgr historyMgr = GlobalStateMgr.getCurrentState().getQueryHistoryMgr();
        if (historyMgr.getLastLoadTime().isBefore(lastWorkTime)) {
            return;
        }

        List<QueryHistory> qhs = historyMgr.queryLastHistory(lastWorkTime);
        if (qhs.isEmpty()) {
            return;
        }

        connect = StatisticUtils.buildConnectContext();
        // sqlDigest -> plan
        List<BaselinePlan> baselines = generateBaseline(qhs);

        if (!baselines.isEmpty()) {
            SQLPlanStorage sqlPlanStorage = GlobalStateMgr.getCurrentState().getSqlPlanStorage();
            sqlPlanStorage.storeBaselinePlan(baselines);
        }
        lastWorkTime = LocalDateTime.now();
    }

    private List<BaselinePlan> generateBaseline(List<QueryHistory> histories) {
        Pattern checkPattern = Pattern.compile(GlobalVariable.spmCaptureIncludeTablePattern);

        // 1.generate plan & check pattern
        Map<String, BaselinePlan> plans = Maps.newHashMap();
        for (QueryHistory queryHistory : histories) {
            try {
                connect.changeCatalogDb(queryHistory.getDb());
            } catch (Exception e) {
                // if the db isn't exists, we just skip this query
                AuditLog.getInternalAudit().info("SPM auto capture failed, db not exists: {}", queryHistory.getDb());
                continue;
            }
            try {
                connect.setThreadLocalInfo();
                List<StatementBase> stmt = SqlParser.parse(queryHistory.getOriginSQL(), connect.getSessionVariable());
                Preconditions.checkState(stmt.size() == 1);
                Preconditions.checkState(stmt.get(0) instanceof QueryStatement);

                Map<TableName, Table> tables = AnalyzerUtils.collectAllTable(stmt.get(0));
                if (tables.size() < 2) {
                    continue;
                }

                if (!tables.keySet().stream()
                        .allMatch(t -> checkPattern.matcher(t.getDb() + "." + t.getTbl()).find())) {
                    continue;
                }

                MetadataMgr metadata = GlobalStateMgr.getCurrentState().getMetadataMgr();
                boolean allTableExists = true;
                for (TableName tableName : tables.keySet()) {
                    if (metadata.getTable(connect, tableName).isEmpty()) {
                        AuditLog.getInternalAudit()
                                .info("SPM auto capture failed, table not exists: {}", tableName.toSql());
                        allTableExists = false;
                    }
                }
                if (!allTableExists) {
                    continue;
                }

                SPMPlanBuilder builder = new SPMPlanBuilder(connect, ((QueryStatement) stmt.get(0)));
                BaselinePlan base = builder.execute();

                base.setSource(BaselinePlan.SOURCE_CAPTURE);
                base.setGlobal(true);
                base.setEnable(false);
                base.setQueryMs(queryHistory.getQueryMs());
                base.setUpdateTime(queryHistory.getDatetime());
                plans.put(queryHistory.getSqlDigest(), base);
            } catch (Exception e) {
                LOG.warn("sql plan capture failed. sql: {}", queryHistory.getOriginSQL(), e);
            }
        }

        if (plans.isEmpty()) {
            return Collections.emptyList();
        }

        // 2. get exists baselines
        SQLPlanStorage sqlPlanStorage = GlobalStateMgr.getCurrentState().getSqlPlanStorage();
        List<String> queryDigests = histories.stream().map(QueryHistory::getSqlDigest).toList();
        List<BaselinePlan> allBaselines = sqlPlanStorage.queryBaselinePlan(queryDigests, BaselinePlan.SOURCE_CAPTURE);

        // 3. remove duplicate baseline
        List<BaselinePlan> result = Lists.newArrayList();
        for (var entry : plans.entrySet()) {
            String digest = entry.getKey();
            BaselinePlan plan = entry.getValue();

            if (allBaselines.stream().anyMatch(b -> b.getBindSqlDigest().equalsIgnoreCase(digest) &&
                    b.getPlanSql().equalsIgnoreCase(plan.getPlanSql()))) {
                continue;
            }
            result.add(plan);
        }
        return result;
    }
}
