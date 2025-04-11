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

package com.starrocks.summary;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.spm.SPMAst2SQLBuilder;
import com.starrocks.sql.spm.SPMPlan2SQLBuilder;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class QueryHistoryMgr {
    private static final Logger LOG = LogManager.getLogger(QueryHistoryMgr.class);

    private static final int CONVERT_BATCH_SIZE = 100;

    private static final int MAX_MEMORY_SIZE = 200 * 1024 * 1024; // 200 MB

    private static final ExecutorService EXECUTOR = ThreadPoolManager.newDaemonFixedThreadPool(
            1, Integer.MAX_VALUE,
            "query-history-executor", true);

    private final List<QueryHistory> histories = Lists.newArrayListWithCapacity(CONVERT_BATCH_SIZE + 1);

    private StringBuffer buffer = new StringBuffer();

    private int batchSize = 0;

    private LocalDateTime lastLoadTime = LocalDateTime.now();

    public synchronized void addQueryHistory(ConnectContext ctx, ExecPlan plan) {
        if (!GlobalVariable.isEnableQueryHistory()) {
            return;
        }
        if (ctx.isStatisticsConnection() || ctx.isStatisticsJob() || plan.getScanNodes().isEmpty()
                || StringUtils.containsIgnoreCase(ctx.getExecutor().getOriginStmtInString(),
                StatsConstants.INFORMATION_SCHEMA)) {
            return;
        }

        histories.add(new QueryHistory(ctx, plan));
        if (histories.size() > CONVERT_BATCH_SIZE || lastLoadTime.plusSeconds(
                GlobalVariable.queryHistoryLoadIntervalSeconds).isBefore(LocalDateTime.now())) {
            loadQueryHistory(Lists.newArrayList(histories));
            histories.clear();
        }
    }

    private void loadQueryHistory(List<QueryHistory> list) {
        EXECUTOR.submit(() -> {
            try {
                for (QueryHistory query : list) {
                    buffer.append(query.toJSON()).append(",");
                }
                batchSize += list.size();

                if (buffer.isEmpty()) {
                    return;
                }
                if (buffer.length() < MAX_MEMORY_SIZE && lastLoadTime.plusSeconds(
                        GlobalVariable.queryHistoryLoadIntervalSeconds).isAfter(LocalDateTime.now())) {
                    return;
                }
                buffer.setLength(buffer.length() - 1);
                StreamLoader loader = new StreamLoader(StatsConstants.STATISTICS_DB_NAME,
                        StatsConstants.QUERY_HISTORY_TABLE_NAME,
                        List.of("dt", "frontend", "sql_digest", "sql", "plan", "plan_costs", "query_ms", "other"));
                StreamLoader.Response response = loader.loadBatch("query_history", buffer.toString());

                if (response != null && response.status() == HttpStatus.SC_OK) {
                    LOG.debug("load query history success, batch size[{}], response[{}]", batchSize, response);
                } else {
                    LOG.warn("load query history failed, batch size[{}], response[{}]", batchSize, response);
                }

                buffer = new StringBuffer();
                lastLoadTime = LocalDateTime.now();
                batchSize = 0;
            } catch (Exception e) {
                LOG.warn("Failed to load query history.", e);
            }
        });
    }

    public void clearExpiredQueryHistory() {
        if (!GlobalVariable.isEnableQueryHistory()) {
            return;
        }

        String date =
                DateUtils.formatDateTimeUnix(LocalDateTime.now().minusSeconds(GlobalVariable.queryHistoryKeepSeconds));
        SimpleExecutor executor = new SimpleExecutor("QueryHistoryDrop", TResultSinkType.MYSQL_PROTOCAL);
        executor.executeDML(
                "DELETE FROM " + StatsConstants.STATISTICS_DB_NAME + "." + StatsConstants.QUERY_HISTORY_TABLE_NAME
                        + " WHERE dt <= '" + date + "'");
    }

    private static class QueryHistory {
        private final String originSQL;
        private final StatementBase statement;
        private final OptExpression physicalPlan;
        private final List<ColumnRefOperator> outputs;
        private final long queryMs;
        private final String datetime;
        private final Map<String, String> other = Map.of();

        private QueryHistory(ConnectContext ctx, ExecPlan plan) {
            originSQL = ctx.getExecutor().getOriginStmtInString();
            statement = ctx.getExecutor().getParsedStmt();
            physicalPlan = plan.getPhysicalPlan();
            outputs = plan.getOutputColumns();

            datetime = DateUtils.formatDateTimeUnix(LocalDateTime.now());
            queryMs = System.currentTimeMillis() - ctx.getStartTime();
        }

        private String toJSON() {
            Map<String, Object> jsonMaps = Maps.newHashMap();
            jsonMaps.put("dt", datetime);
            jsonMaps.put("frontend", GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().getHost() + ":" +
                    GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().getQueryPort());
            jsonMaps.put("sql", originSQL);
            jsonMaps.put("plan_costs", physicalPlan.getCost());
            jsonMaps.put("query_ms", queryMs);
            jsonMaps.put("other", other);

            SPMAst2SQLBuilder builder = new SPMAst2SQLBuilder(false, true);
            String sqlDigest = builder.build((QueryStatement) statement);
            SPMPlan2SQLBuilder planBuilder = new SPMPlan2SQLBuilder();
            String planSQL = planBuilder.toSQL(List.of(), physicalPlan, outputs);

            jsonMaps.put("sql_digest", sqlDigest);
            jsonMaps.put("plan", planSQL);

            return new Gson().toJson(jsonMaps);
        }
    }
}
