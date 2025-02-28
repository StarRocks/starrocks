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

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.statistic.base.PartitionSampler;
import com.starrocks.statistic.hyper.HyperQueryJob;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.FULL_STATISTICS_TABLE_NAME;

public class HyperStatisticsCollectJob extends StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(HyperStatisticsCollectJob.class);

    private final List<Long> partitionIdList;

    private final int batchRowsLimit;
    protected final List<String> sqlBuffer = Lists.newArrayList();
    protected final List<List<Expr>> rowsBuffer = Lists.newArrayList();

    public HyperStatisticsCollectJob(Database db, Table table, List<Long> partitionIdList, List<String> columnNames,
                                     List<Type> columnTypes, StatsConstants.AnalyzeType type,
                                     StatsConstants.ScheduleType scheduleType, Map<String, String> properties) {
        this(db, table, partitionIdList, columnNames, columnTypes, type, scheduleType, properties, List.of(), List.of());
    }

    public HyperStatisticsCollectJob(Database db, Table table, List<Long> partitionIdList, List<String> columnNames,
                                     List<Type> columnTypes, StatsConstants.AnalyzeType type,
                                     StatsConstants.ScheduleType scheduleType, Map<String, String> properties,
                                     List<StatsConstants.StatisticsType> statisticsTypes, List<List<String>> columnGroups) {
        super(db, table, columnNames, columnTypes, type, scheduleType, properties, statisticsTypes, columnGroups);
        this.partitionIdList = partitionIdList;
        this.batchRowsLimit = (int) Math.max(1, Config.statistic_full_collect_buffer / 33 / 1024);
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        if (table.isTemporaryTable()) {
            context.setSessionId(((OlapTable) table).getSessionId());
        }
        context.getSessionVariable().setEnableAnalyzePhasePruneColumns(true);
        context.getSessionVariable().setPipelineDop(context.getSessionVariable().getStatisticCollectParallelism());

        int splitSize = Math.max(1, batchRowsLimit / columnNames.size());
        List<HyperQueryJob> queryJobs;
        if (statisticsTypes.isEmpty()) {
            if (analyzeType == StatsConstants.AnalyzeType.FULL) {
                queryJobs = HyperQueryJob.createFullQueryJobs(context, db, table, columnNames, columnTypes,
                            partitionIdList, splitSize);
            } else {
                PartitionSampler sampler = PartitionSampler.create(table, partitionIdList, properties);
                queryJobs = HyperQueryJob.createSampleQueryJobs(context, db, table, columnNames, columnTypes,
                        partitionIdList, splitSize, sampler);
            }
        } else {
            queryJobs = HyperQueryJob.createMultiColumnQueryJobs(context, db, table, columnGroups, analyzeType,
                    statisticsTypes, properties);
        }

        long queryTotals = 0;
        long queryFailures = 0;
        long insertFailures = 0;

        for (int i = 0; i < queryJobs.size(); i++) {
            HyperQueryJob queryJob = queryJobs.get(i);
            try {
                queryJob.queryStatistics();
                rowsBuffer.addAll(queryJob.getStatisticsData());
                sqlBuffer.addAll(queryJob.getStatisticsValueSQL());

                queryTotals += queryJob.getTotals();
                queryFailures += queryJob.getFailures();
            } catch (Exception e) {
                LOG.warn("query statistics task failed in job: {}, {}", this, queryJob, e);
                throw e;
            }

            if (queryFailures > Config.statistic_full_statistics_failure_tolerance_ratio * queryTotals) {
                String message = String.format("query statistic job failed due to " +
                                "too many failed tasks: %d/%d, the last failure is %s",
                        queryFailures, queryTotals, queryJob.getLastFailure());
                LOG.warn(message, queryJob.getLastFailure());
                throw new RuntimeException(message, queryJob.getLastFailure());
            }

            try {
                flushInsertStatisticsData(context);
            } catch (Exception e) {
                insertFailures++;
                if (insertFailures > Config.statistic_full_statistics_failure_tolerance_ratio * queryJobs.size()) {
                    String message = String.format("insert statistic job failed due to " +
                                    "too many failed tasks: %d/%d, the last failure is %s",
                            insertFailures, queryJobs.size(), e);
                    LOG.warn(message, queryJob.getLastFailure());
                    throw new RuntimeException(message, queryJob.getLastFailure());
                } else {
                    LOG.warn("insert statistics task failed in job: {}, {}", this, queryJob, e);
                }
            } finally {
                rowsBuffer.clear();
                sqlBuffer.clear();
            }
            analyzeStatus.setProgress((i + 1) * 100L / queryJobs.size());
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }
    }

    private void flushInsertStatisticsData(ConnectContext context) throws Exception {
        if (rowsBuffer.isEmpty()) {
            return;
        }

        int count = 0;
        int maxRetryTimes = 5;
        StatementBase insertStmt = createInsertStmt();
        do {
            LOG.debug("statistics insert sql size:" + rowsBuffer.size());
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, insertStmt);

            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            context.setStartTime();
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                LOG.warn("Statistics insert fail | {} | Error Message [{}]", DebugUtil.printId(context.getQueryId()),
                        context.getState().getErrorMessage());
                if (StringUtils.contains(context.getState().getErrorMessage(), "Too many versions")) {
                    Thread.sleep(Config.statistic_collect_too_many_version_sleep);
                    count++;
                } else {
                    throw new DdlException(context.getState().getErrorMessage());
                }
            } else {
                return;
            }
        } while (count < maxRetryTimes);

        throw new DdlException(context.getState().getErrorMessage());
    }

    protected StatementBase createInsertStmt() {
        List<String> targetColumnNames = StatisticUtils.buildStatsColumnDef(FULL_STATISTICS_TABLE_NAME).stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());

        String sql = "INSERT INTO _statistics_.column_statistics(" + String.join(", ", targetColumnNames) +
                ") values " + String.join(", ", sqlBuffer) + ";";
        QueryStatement qs = new QueryStatement(new ValuesRelation(rowsBuffer, targetColumnNames));
        InsertStmt insert = new InsertStmt(new TableName("_statistics_", "column_statistics"), qs);
        insert.setTargetColumnNames(targetColumnNames);
        insert.setOrigStmt(new OriginStatement(sql, 0));
        return insert;
    }

    @Override
    public String toString() {
        return "HyperStatisticsCollectJob{" + "type=" + analyzeType +
                ", scheduleType=" + scheduleType +
                ", db=" + db +
                ", table=" + table +
                ", partitionIdList=" + partitionIdList +
                ", columnNames=" + columnNames +
                ", properties=" + properties +
                '}';
    }
}
