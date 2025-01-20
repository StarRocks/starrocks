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

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AuditLog;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.RemoteFilesSampleStrategy;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.statistics.IRelaxDictManager;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.thrift.TStatusCode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class StatisticExecutor {
    private static final Logger LOG = LogManager.getLogger(StatisticExecutor.class);
    private static final Set<THdfsFileFormat> SUPPORTED_FORMAT = ImmutableSet.of(THdfsFileFormat.PARQUET);

    private static final Predicate<THdfsScanRange> FORMAT_CHECKER = x -> x.isSetFile_format() &&
            SUPPORTED_FORMAT.contains(x.getFile_format());

    public List<TStatisticData> queryStatisticSync(ConnectContext context, String tableUUID, Table table,
                                                   List<String> columnNames) {
        if (table == null) {
            // Statistical information query is an unlocked operation,
            // so it is possible for the table to be deleted while the code is running
            return Collections.emptyList();
        }
        List<Type> columnTypes = Lists.newArrayList();
        for (String colName : columnNames) {
            columnTypes.add(StatisticUtils.getQueryStatisticsColumnType(table, colName));
        }
        String sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL(tableUUID, columnNames, columnTypes);
        return executeStatisticDQL(context, sql);
    }

    public List<TStatisticData> queryStatisticSync(ConnectContext context, Long dbId, Long tableId,
                                                   List<String> columnNames) {
        BasicStatsMeta meta = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getTableBasicStatsMeta(tableId);
        // TODO: remove this hack
        Table table = lookupTable(dbId, tableId);
        if (table == null) {
            // Statistical information query is an unlocked operation,
            // so it is possible for the table to be deleted while the code is running
            return Collections.emptyList();
        }

        Map<String, ColumnStatsMeta> analyzedColumns = meta != null ? meta.getAnalyzedColumns() : Maps.newHashMap();
        List<ColumnStatsMeta> columnStatsMetaList = Lists.newArrayList();
        for (String name : columnNames) {
            if (meta == null || !analyzedColumns.containsKey(name)) {
                columnStatsMetaList.add(
                        new ColumnStatsMeta(name, StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.MIN));
            } else {
                columnStatsMetaList.add(analyzedColumns.get(name));
            }
        }

        return queryColumnStats(context, dbId, tableId, columnStatsMetaList, table);
    }

    private static Table lookupTable(Long dbId, Long tableId) {
        Table table = null;
        if (dbId == null) {
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
            for (Long id : dbIds) {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(id);
                if (db == null) {
                    continue;
                }
                table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
                if (table == null) {
                    continue;
                }
                break;
            }
        } else {
            Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), tableId);
        }
        return table;
    }

    private @NotNull List<TStatisticData> queryColumnStats(ConnectContext context, Long dbId, Long tableId,
                                                           List<ColumnStatsMeta> columnStatsMetaList,
                                                           Table table) {
        List<ColumnStatsMeta> columnWithFullStats =
                columnStatsMetaList.stream()
                        .filter(x -> x.getType() == StatsConstants.AnalyzeType.FULL)
                        .collect(Collectors.toList());
        List<ColumnStatsMeta> columnWithSampleStats =
                columnStatsMetaList.stream()
                        .filter(x -> x.getType() == StatsConstants.AnalyzeType.SAMPLE)
                        .collect(Collectors.toList());

        List<TStatisticData> columnStats = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(columnWithFullStats)) {
            List<String> columnNamesForStats = columnWithFullStats.stream().map(ColumnStatsMeta::getColumnName)
                    .collect(Collectors.toList());
            List<Type> columnTypesForStats = columnWithFullStats.stream()
                            .map(x -> StatisticUtils.getQueryStatisticsColumnType(table, x.getColumnName()))
                            .collect(Collectors.toList());

            String statsSql = StatisticSQLBuilder.buildQueryFullStatisticsSQL(tableId, columnNamesForStats, columnTypesForStats);
            List<TStatisticData> tStatisticData = executeStatisticDQL(context, statsSql);
            columnStats.addAll(tStatisticData);
        }
        if (CollectionUtils.isNotEmpty(columnWithSampleStats)) {
            List<String> columnNamesForStats = columnWithSampleStats.stream().map(ColumnStatsMeta::getColumnName)
                    .collect(Collectors.toList());
            if (Config.statistic_use_meta_statistics) {
                List<Type> columnTypesForStats = columnWithSampleStats.stream()
                        .map(x -> StatisticUtils.getQueryStatisticsColumnType(table, x.getColumnName()))
                        .collect(Collectors.toList());
                String statsSql = StatisticSQLBuilder.buildQueryFullStatisticsSQL(
                        tableId, columnNamesForStats, columnTypesForStats);
                List<TStatisticData> tStatisticData = executeStatisticDQL(context, statsSql);
                columnStats.addAll(tStatisticData);
            } else {
                String statsSql = StatisticSQLBuilder.buildQuerySampleStatisticsSQL(dbId, tableId, columnNamesForStats);
                List<TStatisticData> tStatisticData = executeStatisticDQL(context, statsSql);
                columnStats.addAll(tStatisticData);
            }
        }
        return columnStats;
    }

    public void dropTableStatistics(ConnectContext statsConnectCtx, Long tableIds,
                                    StatsConstants.AnalyzeType analyzeType) {
        String sql = StatisticSQLBuilder.buildDropStatisticsSQL(tableIds, analyzeType);
        LOG.debug("Expire statistic SQL: {}", sql);

        boolean result = executeDML(statsConnectCtx, sql);
        if (!result) {
            LOG.warn("Execute statistic table expire fail.");
        }
    }

    public void dropExternalTableStatistics(ConnectContext statsConnectCtx, String tableUUID) {
        String sql = StatisticSQLBuilder.buildDropExternalStatSQL(tableUUID);
        LOG.debug("Expire external statistic SQL: {}", sql);

        boolean result = executeDML(statsConnectCtx, sql);
        if (!result) {
            LOG.warn("Execute statistic table expire fail.");
        }
    }

    public void dropExternalTableStatistics(ConnectContext statsConnectCtx, String catalogName, String dbName, String tableName) {
        String sql = StatisticSQLBuilder.buildDropExternalStatSQL(catalogName, dbName, tableName);
        LOG.debug("Expire external statistic SQL: {}", sql);

        boolean result = executeDML(statsConnectCtx, sql);
        if (!result) {
            LOG.warn("Execute statistic table expire fail.");
        }
    }

    public boolean dropPartitionStatistics(ConnectContext statsConnectCtx, List<Long> pids) {
        String sql = StatisticSQLBuilder.buildDropPartitionSQL(pids);
        LOG.debug("Expire partition statistic SQL: {}", sql);
        return executeDML(statsConnectCtx, sql);
    }

    public boolean dropTableInvalidPartitionStatistics(ConnectContext statsConnectCtx, List<Long> tables,
                                                       List<Long> pids) {
        String sql = StatisticSQLBuilder.buildDropTableInvalidPartitionSQL(tables, pids);
        LOG.debug("Expire invalid partition statistic SQL: {}", sql);
        return executeDML(statsConnectCtx, sql);
    }

    public List<TStatisticData> queryHistogram(ConnectContext statsConnectCtx, Long tableId, List<String> columnNames) {
        String sql = StatisticSQLBuilder.buildQueryHistogramStatisticsSQL(tableId, columnNames);
        return executeStatisticDQL(statsConnectCtx, sql);
    }

    public List<TStatisticData> queryHistogram(ConnectContext statsConnectCtx, String tableUUID, List<String> columnNames) {
        String sql = StatisticSQLBuilder.buildQueryConnectorHistogramStatisticsSQL(tableUUID, columnNames);
        return executeStatisticDQL(statsConnectCtx, sql);
    }

    public List<TStatisticData> queryMCV(ConnectContext statsConnectCtx, String sql) {
        return executeStatisticDQL(statsConnectCtx, sql);
    }

    public void dropHistogram(ConnectContext statsConnectCtx, Long tableId, List<String> columnNames) {
        String sql = StatisticSQLBuilder.buildDropHistogramSQL(tableId, columnNames);
        boolean result = executeDML(statsConnectCtx, sql);
        if (!result) {
            LOG.warn("Execute statistic table expire fail.");
        }
    }

    public void dropExternalHistogram(ConnectContext statsConnectCtx, String tableUUID, List<String> columnNames) {
        String sql = StatisticSQLBuilder.buildDropExternalHistogramSQL(tableUUID, columnNames);
        boolean result = executeDML(statsConnectCtx, sql);
        if (!result) {
            LOG.warn("Execute external histogram statistic table expire fail.");
        }
    }

    public void dropExternalHistogram(ConnectContext statsConnectCtx, String catalogName, String dbName, String tableName,
                                      List<String> columnNames) {
        String sql = StatisticSQLBuilder.buildDropExternalHistogramSQL(catalogName, dbName, tableName, columnNames);
        boolean result = executeDML(statsConnectCtx, sql);
        if (!result) {
            LOG.warn("Execute external histogram statistic table expire fail.");
        }
    }

    // If you call this function, you must ensure that the db lock is added
    public static Pair<List<TStatisticData>, Status> queryDictSync(Long dbId, Long tableId, ColumnId columnId)
            throws Exception {
        if (dbId == -1) {
            return Pair.create(Collections.emptyList(), Status.OK);
        }

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new SemanticException("Database %s is not found", dbId);
        }

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableId);
        }

        if (!(table.isOlapOrCloudNativeTable() || table.isMaterializedView())) {
            throw new SemanticException("Table '%s' is not a OLAP table or LAKE table or Materialize View",
                    table.getName());
        }

        OlapTable olapTable = (OlapTable) table;
        long version = olapTable.getPartitions().stream().map(p -> p.getDefaultPhysicalPartition().getVisibleVersionTime())
                .max(Long::compareTo).orElse(0L);
        String columnName = MetaUtils.getColumnNameByColumnId(dbId, tableId, columnId);
        String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        String sql = "select cast(" + StatsConstants.STATISTIC_DICT_VERSION + " as Int), " +
                "cast(" + version + " as bigint), " +
                "dict_merge(" + StatisticUtils.quoting(columnName) + ") as _dict_merge_" + columnName +
                " from " + StatisticUtils.quoting(catalogName, db.getOriginName(), table.getName()) + " [_META_]";

        return executeStatisticDQLWithoutContext(sql);
    }

    private static Pair<List<TStatisticData>, Status> executeStatisticDQLWithoutContext(String sql) throws TException {
        RemoteFilesSampleStrategy strategy = new RemoteFilesSampleStrategy();
        return executeStatisticDQLWithSample(sql, strategy);
    }

    private static Pair<List<TStatisticData>, Status> executeStatisticDQLWithSample(
            String sql, RemoteFilesSampleStrategy strategy) throws TException {
        ConnectContext context = StatisticUtils.buildConnectContext();
        // The parallelism degree of low-cardinality dict collect task is uniformly set to 1 to
        // prevent collection tasks from occupying a large number of be execution threads and scan threads.
        context.getSessionVariable().setPipelineDop(1);
        context.setThreadLocalInfo();
        StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());

        ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.STATISTIC);

        assert execPlan != null;
        ScanNode scanNode = execPlan.getScanNodes().get(0);
        scanNode.setScanSampleStrategy(strategy);

        StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
        Pair<List<TResultBatch>, Status> sqlResult = executor.executeStmtWithExecPlan(context, execPlan);
        if (!sqlResult.second.ok()) {
            return Pair.create(Collections.emptyList(), sqlResult.second);
        } else {
            return Pair.create(deserializerStatisticData(sqlResult.first), sqlResult.second);
        }
    }

    public static Pair<List<TStatisticData>, Status> queryDictSync(String tableUUID, String columnName)
            throws Exception {
        return queryDictSync(tableUUID, columnName, new RemoteFilesSampleStrategy(5, FORMAT_CHECKER));
    }

    public static Pair<List<TStatisticData>, Status> queryDictSync(String tableUUID, String columnName,
                                                                   RemoteFilesSampleStrategy strategy)
            throws TException {
        List<String> names = StatisticsUtils.getTableNameByUUID(tableUUID);
        if (names.size() < 3) {
            return Pair.create(Collections.emptyList(), new Status(TStatusCode.GLOBAL_DICT_ERROR,
                    "tableUUID " + tableUUID + " error for collecting dict"));
        }
        String sql = "select cast(" + StatsConstants.STATISTIC_DICT_VERSION + " as Int), " +
                "cast(0 as bigint), " +
                "dict_merge(" + StatisticUtils.quoting(columnName) + ") from " +
                StatisticUtils.quoting(names.get(0), names.get(1), names.get(2));

        return executeStatisticDQLWithSample(sql, strategy);
    }

    public static Pair<List<TStatisticData>, Status> queryDictSync(String tableUUID, String columnName, String fileName)
            throws TException {
        RemoteFilesSampleStrategy strategy = new RemoteFilesSampleStrategy(fileName);
        return queryDictSync(tableUUID, columnName, strategy);
    }

    public static void updateDictSync(String tableUUID, String columnName, Optional<String> fileName) {
        try {
            if (fileName.isEmpty()) {
                IRelaxDictManager.getInstance().updateGlobalDict(tableUUID, columnName, Optional.empty());
                return;
            }
            Pair<List<TStatisticData>, Status> result = queryDictSync(tableUUID, columnName, fileName.get());
            if (result.second.isGlobalDictError()) {
                IRelaxDictManager.getInstance().updateGlobalDict(tableUUID, columnName, Optional.empty());
            } else if (result.second.ok()) {
                IRelaxDictManager.getInstance()
                        .updateGlobalDict(tableUUID, columnName, Optional.of(result.first.get(0)));
            }
        } catch (TException e) {
            // ignore
        } finally {
            IRelaxDictManager.getInstance().removeTemporaryInvalid(tableUUID, columnName);
        }
    }

    public List<TStatisticData> queryTableStats(ConnectContext context, Long tableId, List<Long> partitions) {
        String sql = StatisticSQLBuilder.buildQueryTableStatisticsSQL(tableId, partitions);
        return executeStatisticDQL(context, sql);
    }

    public List<TStatisticData> queryTableStats(ConnectContext context, Long tableId, Long partitionId) {
        String sql = StatisticSQLBuilder.buildQueryTableStatisticsSQL(tableId, partitionId);
        return executeStatisticDQL(context, sql);
    }

    public List<TStatisticData> queryPartitionLevelColumnNDV(ConnectContext context, long tableId,
                                                             List<Long> partitions, List<String> columns) {
        String sql = StatisticSQLBuilder.buildQueryPartitionStatisticsSQL(tableId, partitions, columns);
        return executeStatisticDQL(context, sql);
    }

    private static List<TStatisticData> deserializerStatisticData(List<TResultBatch> sqlResult) throws TException {
        List<TStatisticData> statistics = Lists.newArrayList();

        if (sqlResult.isEmpty()) {
            return statistics;
        }

        int version = sqlResult.get(0).getStatistic_version();
        if (sqlResult.stream().anyMatch(d -> d.getStatistic_version() != version)) {
            return statistics;
        }

        if (StatsConstants.STATISTIC_SUPPORTED_VERSION.contains(version)) {
            TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
            for (TResultBatch resultBatch : sqlResult) {
                for (ByteBuffer bb : resultBatch.rows) {
                    TStatisticData sd = new TStatisticData();
                    byte[] bytes = new byte[bb.limit() - bb.position()];
                    bb.get(bytes);
                    deserializer.deserialize(sd, bytes);
                    statistics.add(sd);
                }
            }
        } else {
            throw new StarRocksPlannerException("Unknown statistics type " + version, ErrorType.INTERNAL_ERROR);
        }

        return statistics;
    }

    public AnalyzeStatus collectStatistics(ConnectContext statsConnectCtx,
                                           StatisticsCollectJob statsJob,
                                           AnalyzeStatus analyzeStatus,
                                           boolean refreshAsync) {
        Database db = statsJob.getDb();
        Table table = statsJob.getTable();

        try {
            Stopwatch watch = Stopwatch.createStarted();
            statsConnectCtx.getSessionVariable().setEnableProfile(Config.enable_statistics_collect_profile);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().registerConnection(analyzeStatus.getId(), statsConnectCtx);
            // Only update running status without edit log, make restart job status is failed
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.RUNNING);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);

            statsConnectCtx.setStatisticsConnection(true);
            statsJob.collect(statsConnectCtx, analyzeStatus);
            LOG.info("execute statistics job successfully, duration={}, job={}", watch.toString(), statsJob);
        } catch (Exception e) {
            LOG.warn("execute statistics job failed: {}", statsJob, e);
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            analyzeStatus.setEndTime(LocalDateTime.now());
            analyzeStatus.setReason(e.getMessage());
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
            return analyzeStatus;
        } finally {
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().unregisterConnection(analyzeStatus.getId(), false);
        }

        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FINISH);
        analyzeStatus.setEndTime(LocalDateTime.now());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);

        // update StatisticsCache
        statsConnectCtx.setStatisticsConnection(false);
        if (statsJob.getType().equals(StatsConstants.AnalyzeType.HISTOGRAM)) {
            if (table.isNativeTableOrMaterializedView()) {
                for (String columnName : statsJob.getColumnNames()) {
                    HistogramStatsMeta histogramStatsMeta = new HistogramStatsMeta(db.getId(),
                            table.getId(), columnName, statsJob.getType(), analyzeStatus.getEndTime(),
                            statsJob.getProperties());
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().addHistogramStatsMeta(histogramStatsMeta);
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().refreshHistogramStatisticsCache(
                            histogramStatsMeta.getDbId(), histogramStatsMeta.getTableId(),
                            Lists.newArrayList(histogramStatsMeta.getColumn()), refreshAsync);
                }
            } else {
                for (String columnName : statsJob.getColumnNames()) {
                    ExternalHistogramStatsMeta histogramStatsMeta = new ExternalHistogramStatsMeta(
                            statsJob.getCatalogName(), db.getFullName(), table.getName(), columnName,
                            statsJob.getType(), analyzeStatus.getEndTime(), statsJob.getProperties());

                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().addExternalHistogramStatsMeta(histogramStatsMeta);
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().refreshConnectorTableHistogramStatisticsCache(
                            statsJob.getCatalogName(), db.getFullName(), table.getName(),
                            Lists.newArrayList(histogramStatsMeta.getColumn()), refreshAsync);
                }
            }
        } else {
            AnalyzeMgr analyzeMgr = GlobalStateMgr.getCurrentState().getAnalyzeMgr();
            if (table.isNativeTableOrMaterializedView()) {
                BasicStatsMeta basicStatsMeta = analyzeMgr.getTableBasicStatsMeta(table.getId());
                if (basicStatsMeta == null) {
                    long existUpdateRows = analyzeMgr.getExistUpdateRows(table.getId());
                    basicStatsMeta = new BasicStatsMeta(db.getId(), table.getId(),
                            statsJob.getColumnNames(), statsJob.getType(), analyzeStatus.getEndTime(),
                            statsJob.getProperties(), existUpdateRows);
                } else {
                    basicStatsMeta = basicStatsMeta.clone();
                    basicStatsMeta.setUpdateTime(analyzeStatus.getEndTime());
                    basicStatsMeta.setProperties(statsJob.getProperties());
                    basicStatsMeta.setAnalyzeType(statsJob.getType());
                    basicStatsMeta.resetDeltaRows();
                }

                for (String column : ListUtils.emptyIfNull(statsJob.getColumnNames())) {
                    ColumnStatsMeta meta =
                            new ColumnStatsMeta(column, statsJob.getType(), analyzeStatus.getEndTime());
                    basicStatsMeta.addColumnStatsMeta(meta);
                }
                analyzeMgr.addBasicStatsMeta(basicStatsMeta);
                analyzeMgr.refreshBasicStatisticsCache(
                        basicStatsMeta.getDbId(), basicStatsMeta.getTableId(), basicStatsMeta.getColumns(),
                        refreshAsync);
            } else {
                // for external table
                ExternalBasicStatsMeta externalBasicStatsMeta = analyzeMgr.getExternalTableBasicStatsMeta(
                        statsJob.getCatalogName(), db.getFullName(), table.getName());
                if (externalBasicStatsMeta == null) {
                    externalBasicStatsMeta = new ExternalBasicStatsMeta(statsJob.getCatalogName(), db.getFullName(),
                            table.getName(), Lists.newArrayList(statsJob.getColumnNames()), statsJob.getType(),
                            analyzeStatus.getEndTime(), statsJob.getProperties());
                } else {
                    externalBasicStatsMeta = externalBasicStatsMeta.clone();
                    externalBasicStatsMeta.setUpdateTime(analyzeStatus.getEndTime());
                    externalBasicStatsMeta.setProperties(statsJob.getProperties());
                    externalBasicStatsMeta.setAnalyzeType(statsJob.getType());
                    // set columns to the latest collect job's columns
                    externalBasicStatsMeta.setColumns(Lists.newArrayList(statsJob.getColumnNames()));
                }

                Set<Long> sampledPartitions = new HashSet<>();
                int allPartitionSize = -1;
                if (statsJob.getType() == StatsConstants.AnalyzeType.SAMPLE) {
                    ExternalSampleStatisticsCollectJob sampleStatsJob = (ExternalSampleStatisticsCollectJob) statsJob;
                    sampledPartitions = sampleStatsJob.getSampledPartitionsHashValue();
                    allPartitionSize = sampleStatsJob.getAllPartitionSize();
                }
                for (String column : ListUtils.emptyIfNull(statsJob.getColumnNames())) {
                    // merge sampled partitions
                    if (externalBasicStatsMeta.getColumnStatsMetaMap().containsKey(column)) {
                        sampledPartitions.addAll(externalBasicStatsMeta.getColumnStatsMeta(column).
                                getSampledPartitionsHashValue());
                    }
                    ColumnStatsMeta meta =
                            new ColumnStatsMeta(column, statsJob.getType(), analyzeStatus.getEndTime(),
                                    sampledPartitions, allPartitionSize);
                    externalBasicStatsMeta.addColumnStatsMeta(meta);
                }
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().addExternalBasicStatsMeta(externalBasicStatsMeta);
                GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                        .refreshConnectorTableBasicStatisticsCache(statsJob.getCatalogName(),
                                db.getFullName(), table.getName(), statsJob.getColumnNames(), refreshAsync);
            }
        }
        return analyzeStatus;
    }

    public List<TStatisticData> executeStatisticDQL(ConnectContext context, String sql) {
        List<TResultBatch> sqlResult = executeDQL(context, sql);
        try {
            return deserializerStatisticData(sqlResult);
        } catch (TException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    /**
     * In case of INSERT-OVERWRITE, the partition-id would change but the statistics would not. So we need to update
     * the partition id of the statistics. Since PRIMARY-KEY tables doesn't really support update key columns, we
     * choose to copy existing row but change the id of it,  then delete existing row.
     * If the second step failed before finish, the statistics cleanup procedure will handle it.
     */
    public static void overwritePartitionStatistics(ConnectContext context,
                                                    long dbId,
                                                    long tableId,
                                                    long sourcePartition,
                                                    long targetPartition) {
        List<String> sqlList =
                FullStatisticsCollectJob.buildOverwritePartitionSQL(tableId, sourcePartition, targetPartition);
        Preconditions.checkState(sqlList.size() == 2);

        // copy
        executeDML(context, sqlList.get(0));

        // delete
        executeDML(context, sqlList.get(1));

        // NOTE: why don't we refresh the statistics cache ?
        // OVERWRITE will create a new partition and delete the existing one, so next time when consulting the stats
        // cache, it would get a cache-miss so reload the cache. and also the cache of deleted partition would be
        // vacuumed by background job. so to conclude we don't need to refresh the stats cache manually
        GlobalStateMgr.getCurrentState().getStatisticStorage().overwritePartitionStatistics(
                tableId, sourcePartition, targetPartition);
    }

    private List<TResultBatch> executeDQL(ConnectContext context, String sql) {
        context.setQueryId(UUIDUtil.genUUID());
        if (Config.enable_print_sql) {
            LOG.info("Begin to execute sql, type: Statistics collect，query id:{}, sql:{}", context.getQueryId(), sql);
        }
        if (FeConstants.enableUnitStatistics) {
            return Collections.emptyList();
        }
        StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
        ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.STATISTIC);
        StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
        context.setExecutor(executor);
        context.getSessionVariable().setEnableMaterializedViewRewrite(false);
        Pair<List<TResultBatch>, Status> sqlResult = executor.executeStmtWithExecPlan(context, execPlan);
        if (!sqlResult.second.ok()) {
            throw new SemanticException("Statistics query fail | Error Message [%s] | QueryId [%s] | SQL [%s]",
                    context.getState().getErrorMessage(), DebugUtil.printId(context.getQueryId()), sql);
        } else {
            AuditLog.getStatisticAudit().info("statistic execute query | QueryId [{}] | SQL: {}",
                    DebugUtil.printId(context.getQueryId()), sql);
            return sqlResult.first;
        }
    }

    private static boolean executeDML(ConnectContext context, String sql) {
        StatementBase parsedStmt;
        try {
            parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            executor.execute();
            AuditLog.getStatisticAudit().info("statistic execute DML | QueryId [{}] | SQL: {}",
                    DebugUtil.printId(context.getQueryId()), sql);
            return true;
        } catch (Exception e) {
            LOG.warn("statistic DML fail | {} | SQL {}", DebugUtil.printId(context.getQueryId()), sql, e);
            return false;
        }
    }
}
