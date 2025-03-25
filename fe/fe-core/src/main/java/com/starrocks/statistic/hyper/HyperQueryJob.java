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

package com.starrocks.statistic.hyper;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.statistic.base.ColumnClassifier;
import com.starrocks.statistic.base.ColumnStats;
import com.starrocks.statistic.base.DefaultColumnStats;
import com.starrocks.statistic.base.MultiColumnStats;
import com.starrocks.statistic.base.PartitionSampler;
import com.starrocks.statistic.sample.TabletSampleManager;
import com.starrocks.thrift.TStatisticData;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.statistics.ColumnStatistic.DEFAULT_COLLECTION_SIZE;

public abstract class HyperQueryJob {
    private static final Logger LOG = LogManager.getLogger(HyperQueryJob.class);

    protected final ConnectContext context;
    protected final Database db;
    protected final Table table;
    protected final List<ColumnStats> columnStats;
    protected final List<Long> partitionIdList;

    // pipeline dop
    protected int pipelineDop;
    // result buffer
    protected List<String> sqlBuffer = Lists.newArrayList();
    protected List<List<Expr>> rowsBuffer = Lists.newArrayList();

    protected int failures = 0;
    protected int totals = 0;
    protected Throwable lastFailure;

    protected HyperQueryJob(ConnectContext context, Database db, Table table, List<ColumnStats> columnStats,
                            List<Long> partitionIdList) {
        this.context = context;
        this.db = db;
        this.table = table;
        this.columnStats = columnStats;
        this.partitionIdList = partitionIdList;
        this.pipelineDop = context.getSessionVariable().getStatisticCollectParallelism();
    }

    public void queryStatistics() {
        String tableName = StringEscapeUtils.escapeSql(db.getOriginName() + "." + table.getName());
        List<String> sqlList = buildQuerySQL();
        for (String sql : sqlList) {
            // execute sql
            List<TStatisticData> dataList = executeStatisticsQuery(sql, context);

            for (TStatisticData data : dataList) {
                Partition partition = table.getPartition(data.getPartitionId());
                if (partition == null) {
                    continue;
                }
                String partitionName = StringEscapeUtils.escapeSql(partition.getName());
                sqlBuffer.add(createInsertValueSQL(data, tableName, partitionName));
                rowsBuffer.add(createInsertValueExpr(data, tableName, partitionName));
            }
        }
    }

    protected List<String> buildQuerySQL() {
        return Collections.emptyList();
    }

    public List<List<Expr>> getStatisticsData() {
        List<List<Expr>> r = rowsBuffer;
        rowsBuffer = Lists.newArrayList();
        return r;
    }

    public List<String> getStatisticsValueSQL() {
        List<String> s = sqlBuffer;
        sqlBuffer = Lists.newArrayList();
        return s;
    }

    public int getFailures() {
        return failures;
    }

    public int getTotals() {
        return totals;
    }

    public Throwable getLastFailure() {
        return lastFailure;
    }

    protected List<TStatisticData> executeStatisticsQuery(String sql, ConnectContext context) {
        try {
            totals++;
            LOG.debug("statistics collect sql : " + sql);
            StatisticExecutor executor = new StatisticExecutor();
            // set default session variables for stats context
            setDefaultSessionVariable(context);
            return executor.executeStatisticDQL(context, sql);
        } catch (Exception e) {
            failures++;
            String message = "execute statistics query failed, sql: " + sql +  ", error: " + e.getMessage();
            LOG.error(message, e);
            lastFailure = new RuntimeException(message, e);
            return Collections.emptyList();
        } finally {
            context.setStartTime();
        }
    }

    protected String createInsertValueSQL(TStatisticData data, String tableName, String partitionName) {
        List<String> params = Lists.newArrayList();

        params.add(String.valueOf(table.getId()));
        params.add(String.valueOf(data.getPartitionId()));
        params.add("'" + StringEscapeUtils.escapeSql(data.getColumnName()) + "'");
        params.add(String.valueOf(db.getId()));
        params.add("'" + tableName + "'");
        params.add("'" + partitionName + "'");
        params.add(String.valueOf(data.getRowCount()));
        params.add(String.valueOf(data.getDataSize()));
        params.add("hll_deserialize(unhex('mockData'))");
        params.add(String.valueOf(data.getNullCount()));
        params.add("'" + data.getMax() + "'");
        params.add("'" + data.getMin() + "'");
        params.add("now()");
        params.add(String.valueOf(data.getCollectionSize() <= 0 ? DEFAULT_COLLECTION_SIZE : data.getCollectionSize()));
        return "(" + String.join(", ", params) + ")";
    }

    protected List<Expr> createInsertValueExpr(TStatisticData data, String tableName, String partitionName) {
        List<Expr> row = Lists.newArrayList();
        row.add(new IntLiteral(table.getId(), Type.BIGINT)); // table id, 8 byte
        row.add(new IntLiteral(data.getPartitionId(), Type.BIGINT)); // partition id, 8 byte
        row.add(new StringLiteral(data.getColumnName())); // column name, 20 byte
        row.add(new IntLiteral(db.getId(), Type.BIGINT)); // db id, 8 byte
        row.add(new StringLiteral(tableName)); // table name, 50 byte
        row.add(new StringLiteral(partitionName)); // partition name, 10 byte
        row.add(new IntLiteral(data.getRowCount(), Type.BIGINT)); // row count, 8 byte
        row.add(new IntLiteral((long) data.getDataSize(), Type.BIGINT)); // data size, 8 byte
        row.add(hllDeserialize(data.getHll())); // hll, 32 kB mock it now
        row.add(new IntLiteral(data.getNullCount(), Type.BIGINT)); // null count, 8 byte
        row.add(new StringLiteral(data.getMax())); // max, 200 byte
        row.add(new StringLiteral(data.getMin())); // min, 200 byte
        row.add(nowFn()); // update time, 8 byte
        row.add(new IntLiteral(data.getCollectionSize() <= 0 ? -1 : data.getCollectionSize(), Type.BIGINT)); // collection size 8 byte
        return row;
    }

    public static Expr hllDeserialize(byte[] hll) {
        String str = new String(hll, StandardCharsets.UTF_8);
        Function unhex = Expr.getBuiltinFunction("unhex", new Type[] {Type.VARCHAR},
                Function.CompareMode.IS_IDENTICAL);

        FunctionCallExpr unhexExpr = new FunctionCallExpr("unhex", Lists.newArrayList(new StringLiteral(str)));
        unhexExpr.setFn(unhex);
        unhexExpr.setType(unhex.getReturnType());

        Function fn = Expr.getBuiltinFunction("hll_deserialize", new Type[] {Type.VARCHAR},
                Function.CompareMode.IS_IDENTICAL);
        FunctionCallExpr fe = new FunctionCallExpr("hll_deserialize", Lists.newArrayList(unhexExpr));
        fe.setFn(fn);
        fe.setType(fn.getReturnType());
        return fe;
    }

    public static Expr nowFn() {
        Function fn = Expr.getBuiltinFunction(FunctionSet.NOW, new Type[] {}, Function.CompareMode.IS_IDENTICAL);
        FunctionCallExpr fe = new FunctionCallExpr("now", Lists.newArrayList());
        fe.setType(fn.getReturnType());
        return fe;
    }

    protected void setDefaultSessionVariable(ConnectContext context) {
        SessionVariable sessionVariable = context.getSessionVariable();
        // Statistics collecting is not user-specific, which means response latency is not that important.
        // Normally, if the page cache is enabled, the page cache must be full. Page cache is used for query
        // acceleration, then page cache is better filled with the user's data.
        sessionVariable.setUsePageCache(false);
        sessionVariable.setEnableMaterializedViewRewrite(false);
        // set the max task num of connector io tasks per scan operator to 4, default is 16,
        // to avoid generate too many chunk source for collect stats in BE
        sessionVariable.setConnectorIoTasksPerScanOperator(4);

        if (table.isTemporaryTable()) {
            context.setSessionId(((OlapTable) table).getSessionId());
        }
        sessionVariable.setEnableAnalyzePhasePruneColumns(true);
        sessionVariable.setPipelineDop(pipelineDop);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() +
                "{table: " + db + "." + table + ", cols: [" +
                columnStats.stream().map(ColumnStats::getColumnNameStr).collect(Collectors.joining(", ")) +
                "], pids: " + partitionIdList + '}';
    }

    public static List<HyperQueryJob> createFullQueryJobs(ConnectContext context, Database db, Table table,
                                                          List<String> columnNames, List<Type> columnTypes,
                                                          List<Long> partitionIdList, int batchLimit) {
        ColumnClassifier classifier = ColumnClassifier.of(columnNames, columnTypes, table);

        List<ColumnStats> supportedStats = classifier.getColumnStats();
        List<ColumnStats> dataCollectColumns =
                supportedStats.stream().filter(ColumnStats::supportData).collect(Collectors.toList());
        List<ColumnStats> unSupportedStats = classifier.getUnSupportCollectColumns();

        List<List<Long>> pids = Lists.partition(partitionIdList, batchLimit);
        List<HyperQueryJob> jobs = Lists.newArrayList();
        for (List<Long> pid : pids) {
            if (!dataCollectColumns.isEmpty()) {
                jobs.add(new FullQueryJob(context, db, table, dataCollectColumns, pid));
            }
            if (!unSupportedStats.isEmpty()) {
                jobs.add(new ConstQueryJob(context, db, table, unSupportedStats, pid));
            }
        }
        return jobs;
    }

    public static List<HyperQueryJob> createSampleQueryJobs(ConnectContext context, Database db, Table table,
                                                            List<String> columnNames, List<Type> columnTypes,
                                                            List<Long> partitionIdList, int batchLimit,
                                                            PartitionSampler sampler) {
        ColumnClassifier classifier = ColumnClassifier.of(columnNames, columnTypes, table);
        List<ColumnStats> supportedStats = classifier.getColumnStats();

        List<ColumnStats> metaCollectColumns =
                supportedStats.stream().filter(ColumnStats::supportMeta).collect(Collectors.toList());
        List<ColumnStats> dataCollectColumns =
                supportedStats.stream().filter(c -> !c.supportMeta() && c.supportData()).collect(Collectors.toList());
        List<ColumnStats> unSupportedStats = classifier.getUnSupportCollectColumns();

        List<List<Long>> pids = Lists.partition(partitionIdList, batchLimit);
        List<HyperQueryJob> jobs = Lists.newArrayList();
        for (List<Long> pid : pids) {
            if (!metaCollectColumns.isEmpty()) {
                jobs.add(new MetaQueryJob(context, db, table, metaCollectColumns, pid, sampler));
            }
            if (!dataCollectColumns.isEmpty()) {
                jobs.add(new SampleQueryJob(context, db, table, dataCollectColumns, pid, sampler));
            }
            if (!unSupportedStats.isEmpty()) {
                jobs.add(new ConstQueryJob(context, db, table, unSupportedStats, pid));
            }
        }
        return jobs;
    }

    public static List<HyperQueryJob> createMultiColumnQueryJobs(ConnectContext context, Database db, Table table,
                                                                 List<List<String>> columnGroups,
                                                                 StatsConstants.AnalyzeType analyzeType,
                                                                 List<StatsConstants.StatisticsType> statisticsTypes,
                                                                 Map<String, String> properties) {
        List<ColumnStats> columnStats = columnGroups.stream()
                .map(group -> group.stream()
                        .map(columnName -> {
                            Column column = table.getColumn(columnName);
                            return new DefaultColumnStats(column.getName(), column.getType(), column.getUniqueId());
                        })
                        .collect(Collectors.toList())
                )
                .map(defaultColumnStats -> new MultiColumnStats(defaultColumnStats, statisticsTypes))
                .collect(Collectors.toList());

        if (analyzeType == StatsConstants.AnalyzeType.FULL) {
            return List.of(new FullMultiColumnQueryJob(context, db, table, columnStats));
        } else {
            TabletSampleManager tabletSampleManager = TabletSampleManager.init(properties, table);
            return List.of(new SampleMultiColumnQueryJob(context, db, table, columnStats, tabletSampleManager));
        }
    }
}
