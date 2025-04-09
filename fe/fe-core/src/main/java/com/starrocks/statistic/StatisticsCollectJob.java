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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AuditLog;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.jetbrains.annotations.NotNull;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(StatisticsMetaManager.class);

    protected final Database db;
    protected final Table table;
    protected final List<String> columnNames;
    protected final List<Type> columnTypes;

    protected final StatsConstants.AnalyzeType analyzeType;

    // statistics types are empty on single column statistics jobs.
    protected List<StatsConstants.StatisticsType> statisticsTypes;
    protected final StatsConstants.ScheduleType scheduleType;
    protected final Map<String, String> properties;
    protected Priority priority;

    // for multi-column combined statistics job.
    // Using group is to support automatic collection of all predicate columns of a table in the future.
    protected final List<List<String>> columnGroups;

    // for partition first load to collect statistics with sample strategy.
    // After the partition is first imported, we cannot immediately get the tablet row count.
    // we need to wait the tabletStatMgr to sync in the background. so we collect row num for each tablet.
    // partition_id -> tablet_id -> row_count
    protected com.google.common.collect.Table<Long, Long, Long> partitionTabletRowCounts = HashBasedTable.create();

    protected StatisticsCollectJob(Database db, Table table, List<String> columnNames,
                                   StatsConstants.AnalyzeType analyzeType, StatsConstants.ScheduleType scheduleType,
                                   Map<String, String> properties) {
        this(db, table, columnNames, columnNames.stream().map(table::getColumn).map(Column::getType).collect(Collectors.toList()),
                analyzeType, scheduleType, properties, List.of(), List.of());
    }

    protected StatisticsCollectJob(Database db, Table table, List<String> columnNames, List<Type> columnTypes,
                                   StatsConstants.AnalyzeType analyzeType, StatsConstants.ScheduleType scheduleType,
                                   Map<String, String> properties) {
        this(db, table, columnNames, columnTypes, analyzeType, scheduleType, properties, List.of(), List.of());
    }

    protected StatisticsCollectJob(Database db, Table table, List<String> columnNames, List<Type> columnTypes,
                                   StatsConstants.AnalyzeType analyzeType, StatsConstants.ScheduleType scheduleType,
                                   Map<String, String> properties, List<StatsConstants.StatisticsType> statisticsTypes,
                                   List<List<String>> columnGroups) {
        this.db = db;
        this.table = table;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.analyzeType = analyzeType;
        this.scheduleType = scheduleType;
        this.properties = properties;
        this.statisticsTypes = statisticsTypes;
        this.columnGroups = columnGroups;
    }

    protected static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        // close velocity log
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
    }

    public abstract void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception;

    public String getCatalogName() {
        return InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    }

    public Database getDb() {
        return db;
    }

    public Table getTable() {
        return table;
    }

    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public StatsConstants.AnalyzeType getAnalyzeType() {
        return analyzeType;
    }

    public StatsConstants.ScheduleType getScheduleType() {
        return scheduleType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean isAnalyzeTable() {
        return CollectionUtils.isEmpty(columnNames);
    }

    public void setPriority(Priority priority) {
        this.priority = priority;
    }

    public Priority getPriority() {
        return this.priority;
    }

    public boolean isMultiColumnStatsJob() {
        return !statisticsTypes.isEmpty();
    }

    public List<StatsConstants.StatisticsType> getStatisticsTypes() {
        return statisticsTypes;
    }

    public List<List<String>> getColumnGroups() {
        return columnGroups;
    }

    abstract String getName();

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
    }

    public void setPartitionTabletRowCounts(com.google.common.collect.Table<Long, Long, Long> partitionTabletRowCounts) {
        this.partitionTabletRowCounts = partitionTabletRowCounts;
    }

    protected void collectStatisticSync(String sql, ConnectContext context) throws Exception {
        int count = 0;
        int maxRetryTimes = 5;
        do {
            context.setQueryId(UUIDUtil.genUUID());
            LOG.debug("statistics collect sql : {}", sql);
            if (Config.enable_print_sql) {
                LOG.info("Begin to execute sql, type: Statistics collect，query id:{}, sql:{}", context.getQueryId(), sql);
            }
            StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);

            // set default session variables for stats context
            setDefaultSessionVariable(context);

            context.setExecutor(executor);
            context.setStartTime();
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                LOG.warn("Statistics collect fail | Error Message [{}] | {} | SQL [{}]",
                        context.getState().getErrorMessage(), DebugUtil.printId(context.getQueryId()), sql);
                if (StringUtils.contains(context.getState().getErrorMessage(), "Too many versions")) {
                    Thread.sleep(Config.statistic_collect_too_many_version_sleep);
                    count++;
                } else {
                    throw new DdlException(context.getState().getErrorMessage());
                }
            } else {
                AuditLog.getStatisticAudit().info("statistic execute query | QueryId [{}] | SQL: {}",
                        DebugUtil.printId(context.getQueryId()), sql);
                return;
            }
        } while (count < maxRetryTimes);

        throw new DdlException(context.getState().getErrorMessage());
    }

    protected String getMinMaxFunction(Type columnType, String name, boolean isMax) {
        String fn = isMax ? "MAX" : "MIN";
        if (columnType.getPrimitiveType().isCharFamily()) {
            fn = fn + "(LEFT(" + name + ", 200))";
        } else {
            fn = fn + "(" + name + ")";
        }
        fn = "IFNULL(" + fn + ", '')";
        return fn;
    }

    protected String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
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

    public static String fullAnalyzeGetDataSize(String columnName, Type columnType) {
        if (columnType.getPrimitiveType().isCharFamily()) {
            return "IFNULL(SUM(CHAR_LENGTH(" + columnName + ")), 0)";
        }
        long typeSize = columnType.getTypeSize();
        return "COUNT(1) * " + typeSize;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StatisticsCollectJob{");
        sb.append("type=").append(analyzeType);
        sb.append(", scheduleType=").append(scheduleType);
        sb.append(", db=").append(db);
        sb.append(", table=").append(table);
        sb.append(", columnNames=").append(columnNames);
        sb.append(", properties=").append(properties);
        sb.append('}');
        return sb.toString();
    }

    public static class Priority implements Comparable<Priority> {
        public LocalDateTime tableUpdateTime;
        public LocalDateTime statsUpdateTime;
        public double healthy;

        public Priority(LocalDateTime tableUpdateTime, LocalDateTime statsUpdateTime, double healthy) {
            this.tableUpdateTime = tableUpdateTime;
            this.statsUpdateTime = statsUpdateTime;
            this.healthy = healthy;
        }

        public long statsStaleness() {
            if (statsUpdateTime != LocalDateTime.MIN) {
                Duration gap = Duration.between(statsUpdateTime, tableUpdateTime);
                // If the tableUpdate < statsUpdate, the duration can be a negative value, so normalize it to 0
                return Math.max(0, gap.getSeconds());
            } else {
                Duration gap = Duration.between(tableUpdateTime, LocalDateTime.now());
                return Math.max(0, gap.getSeconds()) + 3600;
            }
        }

        @Override
        public int compareTo(@NotNull Priority o) {
            // Lower health means higher priority
            if (healthy != o.healthy) {
                return Double.compare(healthy, o.healthy);
            }
            // Higher staleness means higher priority
            return Long.compare(o.statsStaleness(), statsStaleness());
        }
    }

    public static class ComparatorWithPriority
            implements Comparator<StatisticsCollectJob> {

        @Override
        public int compare(StatisticsCollectJob o1, StatisticsCollectJob o2) {
            if (o1.getPriority() != null && o2.getPriority() != null) {
                return o1.getPriority().compareTo(o2.getPriority());
            }
            return 0;
        }
    }
}
