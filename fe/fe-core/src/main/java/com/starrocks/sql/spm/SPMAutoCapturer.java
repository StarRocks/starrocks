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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.summary.QueryHistory;
import com.starrocks.summary.QueryHistoryMgr;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SPMAutoCapturer extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(SPMAutoCapturer.class);

    private LocalDateTime lastWorkTime = LocalDateTime.now();

    private ConnectContext connect;

    public SPMAutoCapturer() {
        super("SPMAutoCapturer");
    }

    @Override
    public long getInterval() {
        return GlobalVariable.getSpmCaptureIntervalSeconds() * 100;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!GlobalVariable.isEnableSPMCapture()) {
            return;
        }
        
        if (lastWorkTime.plusSeconds(GlobalVariable.getSpmCaptureIntervalSeconds()).isAfter(LocalDateTime.now())) {
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

        SQLPlanStorage sqlPlanStorage = GlobalStateMgr.getCurrentState().getSqlPlanStorage();
        sqlPlanStorage.storeBaselinePlan(baselines);
        lastWorkTime = LocalDateTime.now();
    }

    private List<BaselinePlan> generateBaseline(List<QueryHistory> histories) {
        Pattern checkPattern = Pattern.compile(GlobalVariable.getSpmCaptureIncludeTablePattern());

        // 1.generate plan & check pattern
        Map<String, BaselinePlan> plans = Maps.newHashMap();
        for (QueryHistory queryHistory : histories) {
            try {
                List<StatementBase> stmt = SqlParser.parse(queryHistory.getOriginSQL(), connect.getSessionVariable());
                Preconditions.checkState(stmt.size() == 1);
                Preconditions.checkState(stmt.get(0) instanceof QueryStatement);

                Map<TableName, Table> tables = AnalyzerUtils.collectAllTable(stmt.get(0));
                if (tables.size() < 2) {
                    continue;
                }
                if (!tables.keySet().stream().allMatch(t -> {
                    String s = t.getDb() + "." + t.getTbl();
                    return checkPattern.matcher(s).find();
                })) {
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
                LOG.warn("sql plan capture failed. sql: " + queryHistory.getOriginSQL(), e);
            }
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
