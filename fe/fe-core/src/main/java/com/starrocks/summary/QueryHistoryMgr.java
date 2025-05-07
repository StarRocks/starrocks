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
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class QueryHistoryMgr {
    private static final Logger LOG = LogManager.getLogger(QueryHistoryMgr.class);

    private static final int CONVERT_BATCH_SIZE = 100;

    private static final int MAX_MEMORY_SIZE = 200 * 1024 * 1024; // 200 MB

    private static final ExecutorService EXECUTOR = ThreadPoolManager.newDaemonFixedThreadPool(
            1, Integer.MAX_VALUE,
            "query-history-executor", true);

    private final List<QueryHistory> histories = Lists.newArrayListWithCapacity(CONVERT_BATCH_SIZE + 1);

    private StringBuffer buffer = new StringBuffer("[");

    private int batchSize = 0;

    private LocalDateTime lastLoadTime = LocalDateTime.now();

    public LocalDateTime getLastLoadTime() {
        return lastLoadTime;
    }

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
        if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.QUERY_HISTORY_TABLE_NAME))) {
            return;
        }

        ConnectContext ctx = StatisticUtils.buildConnectContext();
        EXECUTOR.submit(() -> {
            ctx.setThreadLocalInfo();
            for (QueryHistory query : list) {
                try {
                    buffer.append(query.toJSON()).append(",");
                } catch (Exception e) {
                    LOG.warn("Failed to convert query history {}", query.getOriginSQL(), e);
                }
            }
            try {
                batchSize += list.size();

                if (buffer.isEmpty()) {
                    return;
                }
                if (buffer.length() < MAX_MEMORY_SIZE && lastLoadTime.plusSeconds(
                        GlobalVariable.queryHistoryLoadIntervalSeconds).isAfter(LocalDateTime.now())) {
                    return;
                }
                buffer.setLength(buffer.length() - 1);
                buffer.append("]");
                StreamLoader loader = new StreamLoader(StatsConstants.STATISTICS_DB_NAME,
                        StatsConstants.QUERY_HISTORY_TABLE_NAME,
                        List.of("dt", "frontend", "db", "sql_digest", "sql", "plan", "plan_costs", "query_ms",
                                "other"));
                StreamLoader.Response response = loader.loadBatch("query_history", buffer.toString());

                if (response != null && response.status() == HttpStatus.SC_OK) {
                    LOG.debug("load query history success, batch size[{}], response[{}]", batchSize, response);
                } else {
                    LOG.warn("load query history failed, batch size[{}], response[{}]", batchSize, response);
                }

                buffer = new StringBuffer();
                buffer.append("[");
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
        if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.QUERY_HISTORY_TABLE_NAME))) {
            return;
        }

        String date =
                DateUtils.formatDateTimeUnix(LocalDateTime.now().minusSeconds(GlobalVariable.queryHistoryKeepSeconds));
        SimpleExecutor executor = new SimpleExecutor("QueryHistoryDrop", TResultSinkType.MYSQL_PROTOCAL);
        executor.executeDML(
                "DELETE FROM " + StatsConstants.STATISTICS_DB_NAME + "." + StatsConstants.QUERY_HISTORY_TABLE_NAME
                        + " WHERE dt <= '" + date + "'");
    }

}
