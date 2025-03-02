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
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.monitor.unit.ByteSizeUnit;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.statistic.columns.ColumnUsage;
import com.starrocks.statistic.columns.PredicateColumnsMgr;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL;

public class StatisticsCollectJobFactory {
    private static final Logger LOG = LogManager.getLogger(StatisticsCollectJobFactory.class);

    private StatisticsCollectJobFactory() {
    }

    /**
     * Build several statistics jobs for ANALYZE with priority
     *
     * @return jobs order by priority
     */
    public static List<StatisticsCollectJob> buildStatisticsCollectJob(NativeAnalyzeJob nativeAnalyzeJob) {
        List<StatisticsCollectJob> statsJobs = Lists.newArrayList();
        if (StatsConstants.DEFAULT_ALL_ID == nativeAnalyzeJob.getDbId()) {
            // all database
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();

            for (Long dbId : dbIds) {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
                if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                    continue;
                }

                for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                    createJob(statsJobs, nativeAnalyzeJob, db, table, null, null);
                }
            }
        } else if (StatsConstants.DEFAULT_ALL_ID == nativeAnalyzeJob.getTableId()
                && StatsConstants.DEFAULT_ALL_ID != nativeAnalyzeJob.getDbId()) {
            // all table
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(nativeAnalyzeJob.getDbId());
            if (null == db) {
                return Collections.emptyList();
            }

            for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                createJob(statsJobs, nativeAnalyzeJob, db, table, null, null);
            }
        } else {
            // database or table is null mean database/table has been dropped
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(nativeAnalyzeJob.getDbId());
            if (db == null) {
                return Collections.emptyList();
            }
            createJob(statsJobs, nativeAnalyzeJob, db, GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(db.getId(), nativeAnalyzeJob.getTableId()),
                    nativeAnalyzeJob.getColumns(), nativeAnalyzeJob.getColumnTypes());
        }

        // Put higher priority jobs at the front
        statsJobs.sort(new StatisticsCollectJob.ComparatorWithPriority());
        return statsJobs;
    }

    public static StatisticsCollectJob buildStatisticsCollectJob(Database db, Table table,
                                                                 List<Long> partitionIdList,
                                                                 List<String> columnNames,
                                                                 List<Type> columnTypes,
                                                                 StatsConstants.AnalyzeType analyzeType,
                                                                 StatsConstants.ScheduleType scheduleType,
                                                                 Map<String, String> properties) {
        if (CollectionUtils.isEmpty(columnNames)) {
            columnNames = StatisticUtils.getCollectibleColumns(table);
            columnTypes = columnNames.stream().map(col -> table.getColumn(col).getType()).collect(Collectors.toList());
        }
        // for compatibility, if columnTypes is null, we will get column types from table
        if (CollectionUtils.isEmpty(columnTypes)) {
            columnTypes = columnNames.stream().map(col -> table.getColumn(col).getType()).collect(Collectors.toList());
        }

        LOG.debug("statistics job work on table: {}, type: {}", table.getName(), analyzeType.name());
        if (analyzeType.equals(StatsConstants.AnalyzeType.SAMPLE)) {
            if (partitionIdList == null) {
                partitionIdList = table.getPartitions().stream().filter(Partition::hasData)
                        .map(Partition::getId).collect(Collectors.toList());
            }
            
            if (Config.statistic_use_meta_statistics) {
                return new HyperStatisticsCollectJob(db, table, partitionIdList, columnNames, columnTypes,
                        StatsConstants.AnalyzeType.SAMPLE, scheduleType, properties);
            } else {
                return new SampleStatisticsCollectJob(db, table, columnNames, columnTypes,
                        StatsConstants.AnalyzeType.SAMPLE, scheduleType, properties);
            }
        } else if (analyzeType.equals(StatsConstants.AnalyzeType.HISTOGRAM)) {
            return new HistogramStatisticsCollectJob(db, table, columnNames, columnTypes, scheduleType, properties);
        } else {
            if (partitionIdList == null) {
                partitionIdList = table.getPartitions().stream().filter(Partition::hasData)
                        .map(Partition::getId).collect(Collectors.toList());
            }

            if (Config.statistic_use_meta_statistics) {
                return new HyperStatisticsCollectJob(db, table, partitionIdList, columnNames, columnTypes,
                        StatsConstants.AnalyzeType.FULL, scheduleType, properties);
            } else {
                return new FullStatisticsCollectJob(db, table, partitionIdList, columnNames, columnTypes,
                        StatsConstants.AnalyzeType.FULL, scheduleType, properties);
            }
        }
    }

    public static List<StatisticsCollectJob> buildExternalStatisticsCollectJob(ExternalAnalyzeJob externalAnalyzeJob) {
        List<StatisticsCollectJob> statsJobs = Lists.newArrayList();
        if (externalAnalyzeJob.isAnalyzeAllDb()) {
            List<String> dbNames = GlobalStateMgr.getCurrentState().getMetadataMgr().
                    listDbNames(externalAnalyzeJob.getCatalogName());
            for (String dbName : dbNames) {
                Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().
                        getDb(externalAnalyzeJob.getCatalogName(), dbName);
                if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                    continue;
                }

                List<String> tableNames = GlobalStateMgr.getCurrentState().getMetadataMgr().
                        listTableNames(externalAnalyzeJob.getCatalogName(), dbName);
                for (String tableName : tableNames) {
                    Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().
                            getTable(externalAnalyzeJob.getCatalogName(), dbName, tableName);
                    if (null == table) {
                        continue;
                    }

                    createExternalAnalyzeJob(statsJobs, externalAnalyzeJob, db, table, null);
                }
            }
        } else if (externalAnalyzeJob.isAnalyzeAllTable()) {
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().
                    getDb(externalAnalyzeJob.getCatalogName(), externalAnalyzeJob.getDbName());
            if (null == db) {
                return Collections.emptyList();
            }

            List<String> tableNames = GlobalStateMgr.getCurrentState().getMetadataMgr().
                    listTableNames(externalAnalyzeJob.getCatalogName(), externalAnalyzeJob.getDbName());
            for (String tableName : tableNames) {
                Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().
                        getTable(externalAnalyzeJob.getCatalogName(), externalAnalyzeJob.getDbName(), tableName);
                if (null == table) {
                    continue;
                }

                createExternalAnalyzeJob(statsJobs, externalAnalyzeJob, db, table, null);
            }
        } else {
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().
                    getDb(externalAnalyzeJob.getCatalogName(), externalAnalyzeJob.getDbName());
            if (null == db) {
                return Collections.emptyList();
            }

            Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().
                    getTable(externalAnalyzeJob.getCatalogName(), externalAnalyzeJob.getDbName(),
                            externalAnalyzeJob.getTableName());
            if (null == table) {
                return Collections.emptyList();
            }

            createExternalAnalyzeJob(statsJobs, externalAnalyzeJob, db, table, externalAnalyzeJob.getColumns());
        }

        statsJobs.sort(new StatisticsCollectJob.ComparatorWithPriority());
        return statsJobs;
    }

    public static StatisticsCollectJob buildExternalStatisticsCollectJob(String catalogName, Database db, Table table,
                                                                         List<String> partitionNames,
                                                                         List<String> columnNames,
                                                                         StatsConstants.AnalyzeType analyzeType,
                                                                         StatsConstants.ScheduleType scheduleType,
                                                                         Map<String, String> properties) {
        List<Type> columnTypes =
                columnNames.stream().map(col -> table.getColumn(col).getType()).collect(Collectors.toList());
        return buildExternalStatisticsCollectJob(catalogName, db, table, partitionNames, columnNames, columnTypes,
                analyzeType, scheduleType, properties);
    }

    public static StatisticsCollectJob buildExternalStatisticsCollectJob(String catalogName, Database db, Table table,
                                                                         List<String> partitionNames,
                                                                         List<String> columnNames,
                                                                         List<Type> columnTypes,
                                                                         StatsConstants.AnalyzeType analyzeType,
                                                                         StatsConstants.ScheduleType scheduleType,
                                                                         Map<String, String> properties) {
        // refresh table to get latest table/partition info
        GlobalStateMgr.getCurrentState().getMetadataMgr().refreshTable(catalogName,
                db.getFullName(), table, Lists.newArrayList(), true);

        if (columnNames == null || columnNames.isEmpty()) {
            columnNames = StatisticUtils.getCollectibleColumns(table);
            columnTypes = columnNames.stream().map(col -> table.getColumn(col).getType()).collect(Collectors.toList());
        }
        // for compatibility, if columnTypes is null, we will get column types from table
        if (columnTypes == null || columnTypes.isEmpty()) {
            columnTypes = columnNames.stream().map(col -> table.getColumn(col).getType()).collect(Collectors.toList());
        }

        List<String> allPartitionNames = null;
        if (partitionNames == null) {
            partitionNames = ConnectorPartitionTraits.build(table).getPartitionNames();
            allPartitionNames = partitionNames;
        }
        if (analyzeType.equals(StatsConstants.AnalyzeType.SAMPLE)) {
            int samplePartitionSize = properties.get(StatsConstants.STATISTIC_SAMPLE_COLLECT_PARTITIONS) != null ?
                    Integer.parseInt(properties.get(StatsConstants.STATISTIC_SAMPLE_COLLECT_PARTITIONS)) :
                    Config.statistic_sample_collect_partition_size;

            samplePartitionSize = Math.min(samplePartitionSize, partitionNames.size());
            List<String> samplePartitionNames = StatisticUtils.getLatestPartitionsSample(partitionNames, samplePartitionSize);
            allPartitionNames = allPartitionNames == null ? ConnectorPartitionTraits.build(table).getPartitionNames() :
                    allPartitionNames;
            return new ExternalSampleStatisticsCollectJob(catalogName, db, table, samplePartitionNames, columnNames,
                    columnTypes, analyzeType, scheduleType, properties, allPartitionNames.size());
        }

        return new ExternalFullStatisticsCollectJob(catalogName, db, table, partitionNames, columnNames, columnTypes,
                StatsConstants.AnalyzeType.FULL, scheduleType, properties);
    }

    private static List<String> needCollectStatsColumns(ExternalBasicStatsMeta basicStatsMeta, Table table,
                                                        List<String> columnNames, LocalDateTime tableUpdateTime,
                                                        long timeInterval) {
        List<String> needCollectStatsColumns = Lists.newArrayList();
        for (String columnName : columnNames) {
            if (basicStatsMeta == null || !basicStatsMeta.getColumnStatsMetaMap().containsKey(columnName)) {
                needCollectStatsColumns.add(columnName);
            } else {
                // check stats column last update time, if last collect time is after table update time, skip this column
                LocalDateTime columnCollectStatsTime = basicStatsMeta.getColumnStatsMeta(columnName).getUpdateTime();
                if (tableUpdateTime != null) {
                    if (columnCollectStatsTime.isAfter(tableUpdateTime)) {
                        LOG.info("statistics job doesn't work on non-update table: {}, " +
                                        "last update time: {}, last collect time: {}",
                                table.getName(), tableUpdateTime, columnCollectStatsTime);
                        continue;
                    }
                }

                if (columnCollectStatsTime.plusSeconds(timeInterval).isAfter(LocalDateTime.now())) {
                    LOG.info("statistics job doesn't work on the interval table: {}, " +
                                    "last collect time: {}, interval: {}",
                            table.getName(), tableUpdateTime, timeInterval);
                    continue;
                }
                needCollectStatsColumns.add(columnName);
            }
        }
        return needCollectStatsColumns;
    }

    private static void createExternalAnalyzeJob(List<StatisticsCollectJob> allTableJobMap, ExternalAnalyzeJob job,
                                                 Database db, Table table, List<String> columnNames) {
        if (table == null) {
            return;
        }

        String regex = job.getProperties().getOrDefault(StatsConstants.STATISTIC_EXCLUDE_PATTERN, null);
        if (StringUtils.isNotBlank(regex)) {
            Pattern checkRegex = Pattern.compile(regex);
            String name = db.getFullName() + "." + table.getName();
            if (checkRegex.matcher(name).find()) {
                LOG.info("statistics job exclude pattern {}, hit table: {}", regex, name);
                return;
            }
        }

        ExternalBasicStatsMeta basicStatsMeta = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getExternalBasicStatsMetaMap()
                .get(new AnalyzeMgr.StatsMetaKey(job.getCatalogName(), db.getFullName(), table.getName()));

        if (columnNames == null || columnNames.isEmpty()) {
            columnNames = StatisticUtils.getCollectibleColumns(table);
        }
        List<String> needCollectStatsColumns;
        if (basicStatsMeta != null) {
            // check table row count
            List<ConnectorTableColumnStats> columnStatisticList =
                    GlobalStateMgr.getCurrentState().getStatisticStorage()
                            .getConnectorTableStatisticsSync(table, columnNames);
            List<ConnectorTableColumnStats> validColumnStatistics = columnStatisticList.stream().
                    filter(columnStatistic -> !columnStatistic.isUnknown()).collect(Collectors.toList());

            // use small table row count as default table row count
            long tableRowCount = Config.statistic_auto_collect_small_table_rows - 1;
            if (!validColumnStatistics.isEmpty()) {
                tableRowCount = validColumnStatistics.get(0).getRowCount();
            }

            long defaultInterval = tableRowCount < Config.statistic_auto_collect_small_table_rows ?
                    Config.statistic_auto_collect_small_table_interval :
                    Config.statistic_auto_collect_large_table_interval;

            long timeInterval = job.getProperties().get(STATISTIC_AUTO_COLLECT_INTERVAL) != null ?
                    Long.parseLong(job.getProperties().get(STATISTIC_AUTO_COLLECT_INTERVAL)) :
                    defaultInterval;

            needCollectStatsColumns = needCollectStatsColumns(basicStatsMeta, table, columnNames,
                    StatisticUtils.getTableLastUpdateTime(table), timeInterval);
        } else {
            needCollectStatsColumns = columnNames;
        }

        if (needCollectStatsColumns.isEmpty()) {
            LOG.info("There are no columns need to collect statistics for table: {}", table.getName());
            return;
        }

        List<Type> needCollectStatsColumnTypes = needCollectStatsColumns.stream().map(col ->
                table.getColumn(col).getType()).collect(Collectors.toList());

        // compute the earliest last collect time of columns
        LocalDateTime collectColumnStatsTimeMin;
        if (basicStatsMeta == null) {
            collectColumnStatsTimeMin = LocalDateTime.MIN;
        } else {
            collectColumnStatsTimeMin = needCollectStatsColumns.stream().
                    map(columnName -> {
                        if (basicStatsMeta.getColumnStatsMetaMap().containsKey(columnName)) {
                            return basicStatsMeta.getColumnStatsMeta(columnName).getUpdateTime();
                        } else {
                            return LocalDateTime.MIN;
                        }
                    }).
                    min(LocalDateTime::compareTo).orElse(LocalDateTime.MIN);
        }
        if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.FULL)) {
            createExternalFullStatsJob(allTableJobMap, collectColumnStatsTimeMin, job, db, table,
                    needCollectStatsColumns, needCollectStatsColumnTypes);
        } else {
            createExternalSampleStatsJob(allTableJobMap, collectColumnStatsTimeMin, job, db, table, needCollectStatsColumns,
                    needCollectStatsColumnTypes);
        }
    }

    private static void createExternalFullStatsJob(List<StatisticsCollectJob> allTableJobMap,
                                                   LocalDateTime statisticsUpdateTime,
                                                   ExternalAnalyzeJob job,
                                                   Database db, Table table, List<String> columnNames,
                                                   List<Type> columnTypes) {
        // get updated partitions
        Set<String> updatedPartitions = StatisticUtils.getUpdatedPartitionNames(table, statisticsUpdateTime);
        LOG.info("create external full statistics job for table: {}, partitions: {}",
                table.getName(), updatedPartitions);
        allTableJobMap.add(buildExternalStatisticsCollectJob(job.getCatalogName(), db, table,
                updatedPartitions == null ? null : Lists.newArrayList(updatedPartitions),
                columnNames, columnTypes, StatsConstants.AnalyzeType.FULL, job.getScheduleType(), Maps.newHashMap()));
    }

    private static void createExternalSampleStatsJob(List<StatisticsCollectJob> allTableJobMap,
                                                     LocalDateTime statisticsUpdateTime,
                                                     ExternalAnalyzeJob job, Database db, Table table,
                                                     List<String> columnNames, List<Type> columnTypes) {
        // get updated partitions
        Set<String> updatedPartitions = StatisticUtils.getUpdatedPartitionNames(table, statisticsUpdateTime);
        LOG.info("create external sa,ple statistics job for table: {}, partitions: {}",
                table.getName(), updatedPartitions);
        allTableJobMap.add(buildExternalStatisticsCollectJob(job.getCatalogName(), db, table,
                null, columnNames, columnTypes, StatsConstants.AnalyzeType.SAMPLE, job.getScheduleType(),
                Maps.newHashMap()));
    }

    private static void createJob(List<StatisticsCollectJob> allTableJobMap, NativeAnalyzeJob job,
                                  Database db, Table table, List<String> columnNames, List<Type> columnTypes) {
        if (table == null || !table.isNativeTableOrMaterializedView()) {
            return;
        }

        if (table instanceof OlapTable) {
            if (((OlapTable) table).getState() != OlapTable.OlapTableState.NORMAL) {
                return;
            }
        }

        if (!Config.enable_temporary_table_statistic_collect && table.isTemporaryTable()) {
            LOG.debug("statistics job doesn't work on temporary table: {}", table.getName());
            return;
        }

        if (StatisticUtils.isEmptyTable(table)) {
            return;
        }

        Map<String, String> jobProperties = job.getProperties();
        // check job exclude db.table
        String regex = jobProperties.getOrDefault(StatsConstants.STATISTIC_EXCLUDE_PATTERN, null);
        if (StringUtils.isNotBlank(regex)) {
            Pattern checkRegex = Pattern.compile(regex);
            String name = db.getFullName() + "." + table.getName();
            if (checkRegex.matcher(name).find()) {
                LOG.debug("statistics job exclude pattern {}, hit table: {}", regex, name);
                return;
            }
        }

        double healthy = 0;
        AnalyzeMgr analyzeMgr = GlobalStateMgr.getCurrentState().getAnalyzeMgr();
        BasicStatsMeta basicStatsMeta = analyzeMgr.getTableBasicStatsMeta(table.getId());
        List<HistogramStatsMeta> histogramStatsMetas = analyzeMgr.getHistogramMetaByTable(table.getId());
        boolean useBasicStats = job.getAnalyzeType() == StatsConstants.AnalyzeType.SAMPLE ||
                job.getAnalyzeType() == StatsConstants.AnalyzeType.FULL;
        LocalDateTime tableUpdateTime = StatisticUtils.getTableLastUpdateTime(table);
        LocalDateTime statsUpdateTime = LocalDateTime.MIN;
        boolean isInitJob = true;
        if (useBasicStats && basicStatsMeta != null) {
            statsUpdateTime = basicStatsMeta.getUpdateTime();
            isInitJob = basicStatsMeta.isInitJobMeta();
        } else if (!useBasicStats && CollectionUtils.isNotEmpty(histogramStatsMetas)) {
            statsUpdateTime =
                    histogramStatsMetas.stream().map(HistogramStatsMeta::getUpdateTime)
                            .min(Comparator.naturalOrder()).get();
            isInitJob = histogramStatsMetas.stream().anyMatch(HistogramStatsMeta::isInitJobMeta);
        }

        // Use predicate columns if suitable
        TableName tableName = new TableName(db.getOriginName(), table.getName());
        int numColumns = table.getColumns().size();
        List<String> predicateColNames = null;
        boolean existsPredicateColumns = false;
        boolean enablePredicateColumnStrategy = false;
        if (basicStatsMeta != null && !basicStatsMeta.isInitJobMeta() && useBasicStats &&
                Config.statistic_auto_collect_predicate_columns_threshold > 0 &&
                CollectionUtils.isEmpty(columnNames) && table.isNativeTableOrMaterializedView()) {
            List<ColumnUsage> predicateColumns = PredicateColumnsMgr.getInstance().queryPredicateColumns(tableName);
            if (CollectionUtils.isNotEmpty(predicateColumns) && predicateColumns.size() < numColumns) {
                OlapTable olap = (OlapTable) table;
                predicateColNames = predicateColumns.stream().map(x -> x.getOlapColumnName(olap)).toList();
                existsPredicateColumns = true;
                if (numColumns > Config.statistic_auto_collect_predicate_columns_threshold) {
                    enablePredicateColumnStrategy = true;
                }
            }
        }

        if (basicStatsMeta != null) {
            // 1. if the table has no update after the stats collection
            if (useBasicStats && basicStatsMeta.isUpdatedAfterLoad(tableUpdateTime)) {
                LOG.debug("statistics job doesn't work on non-update table: {}, " +
                                "last update time: {}, last collect time: {}",
                        table.getName(), tableUpdateTime, basicStatsMeta.getUpdateTime());
                return;
            }
            if (!useBasicStats && !isInitJob && statsUpdateTime.isAfter(tableUpdateTime)) {
                return;
            }

            // 2. if the table stats is healthy enough (only a small portion has been updated)
            double statisticAutoCollectRatio =
                    PropertyUtil.propertyAsDouble(jobProperties, StatsConstants.STATISTIC_AUTO_COLLECT_RATIO,
                            Config.statistic_auto_collect_ratio);

            healthy = basicStatsMeta.getHealthy();
            if (healthy > statisticAutoCollectRatio) {
                LOG.debug("statistics job doesn't work on health table: {}, healthy: {}, collect healthy limit: <{}",
                        table.getName(), healthy, statisticAutoCollectRatio);
                return;
            }

            // 3. if the stats collection is too frequent
            long sumDataSize = 0;
            for (Partition partition : table.getPartitions()) {
                if (!StatisticUtils.isPartitionStatsHealthy(table, partition, basicStatsMeta)) {
                    sumDataSize += partition.getDataSize();
                }
            }

            long defaultInterval =
                    job.getAnalyzeType() == StatsConstants.AnalyzeType.HISTOGRAM ?
                            Config.statistic_auto_collect_histogram_interval :
                            (sumDataSize > Config.statistic_auto_collect_small_table_size ?
                                    Config.statistic_auto_collect_large_table_interval :
                                    Config.statistic_auto_collect_small_table_interval);

            long timeInterval = PropertyUtil.propertyAsLong(jobProperties, STATISTIC_AUTO_COLLECT_INTERVAL, defaultInterval);

            if (!isInitJob && statsUpdateTime.plusSeconds(timeInterval).isAfter(LocalDateTime.now())) {
                LOG.debug("statistics job doesn't work on the interval table: {}, " +
                                "last collect time: {}, interval: {}, table size: {}MB",
                        table.getName(), tableUpdateTime, timeInterval, ByteSizeUnit.BYTES.toMB(sumDataSize));
                return;
            }

            // 4. frequent-update big table without predicate column, choose sample strategy to collect statistics
            if (job.getAnalyzeType() != StatsConstants.AnalyzeType.HISTOGRAM &&
                    healthy < Config.statistic_auto_collect_sample_threshold &&
                    sumDataSize > Config.statistic_auto_collect_small_table_size) {
                if (!(Config.statistic_auto_collect_use_full_predicate_column_for_sample && existsPredicateColumns &&
                        predicateColNames.size() < Config.statistic_auto_collect_max_predicate_column_size_on_sample_strategy)) {
                    LOG.debug("statistics job choose sample on real-time update table: {}" +
                                    ", last collect time: {}, current healthy: {}, full collect healthy limit: {}, " +
                                    ", update data size: {}MB, full collect healthy data size limit: <{}MB",
                            table.getName(), basicStatsMeta.getUpdateTime(), healthy,
                            Config.statistic_auto_collect_sample_threshold, ByteSizeUnit.BYTES.toMB(sumDataSize),
                            ByteSizeUnit.BYTES.toMB(Config.statistic_auto_collect_small_table_size));
                    StatisticsCollectJob.Priority priority =
                            new StatisticsCollectJob.Priority(tableUpdateTime, statsUpdateTime, healthy);
                    createSampleStatsJob(allTableJobMap, job, db, table, columnNames, columnTypes, priority);
                    return;
                } else {
                    enablePredicateColumnStrategy = true;
                }
            }
        }

        if (enablePredicateColumnStrategy && CollectionUtils.isNotEmpty(predicateColNames)) {
            columnNames = predicateColNames;
            columnTypes = columnNames.stream().map(col -> table.getColumn(col).getType()).collect(Collectors.toList());
        }

        StatisticsCollectJob.Priority priority =
                new StatisticsCollectJob.Priority(tableUpdateTime, statsUpdateTime, healthy);
        LOG.debug("statistics job work on un-health table: {}, healthy: {}, Type: {}", table.getName(), healthy,
                job.getAnalyzeType());
        if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.SAMPLE)) {
            createSampleStatsJob(allTableJobMap, job, db, table, columnNames, columnTypes, priority);
        } else if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.HISTOGRAM)) {
            createHistogramJob(allTableJobMap, job, db, table, columnNames, columnTypes, priority);
        } else if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.FULL)) {
            createFullStatsJob(allTableJobMap, job, basicStatsMeta, db, table, columnNames, columnTypes, priority);
        } else {
            throw new StarRocksPlannerException("Unknown analyze type " + job.getAnalyzeType(),
                    ErrorType.INTERNAL_ERROR);
        }
    }

    private static void createSampleStatsJob(List<StatisticsCollectJob> allTableJobMap, NativeAnalyzeJob job,
                                             Database db, Table table, List<String> columnNames,
                                             List<Type> columnTypes, StatisticsCollectJob.Priority priority) {
        Collection<Partition> partitions = table.getPartitions();
        BasicStatsMeta basicStatsMeta = GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                .getTableBasicStatsMeta(table.getId());
        List<Long> partitionIdList;
        if (basicStatsMeta != null) {
            partitionIdList = partitions.stream()
                    .filter(partition -> !StatisticUtils.isPartitionStatsHealthy(table, partition, basicStatsMeta))
                    .map(Partition::getId)
                    .collect(Collectors.toList());
        } else {
            partitionIdList = partitions.stream()
                    .filter(Partition::hasData)
                    .map(Partition::getId)
                    .collect(Collectors.toList());
        }

        StatisticsCollectJob sample = buildStatisticsCollectJob(db, table, partitionIdList, columnNames, columnTypes,
                StatsConstants.AnalyzeType.SAMPLE, job.getScheduleType(), job.getProperties());
        sample.setPriority(priority);
        allTableJobMap.add(sample);
    }

    private static void createHistogramJob(List<StatisticsCollectJob> allTableJobMap, NativeAnalyzeJob job,
                                           Database db, Table table, List<String> columnNames,
                                           List<Type> columnTypes, StatisticsCollectJob.Priority priority) {
        StatisticsCollectJob sample = buildStatisticsCollectJob(db, table, null, columnNames, columnTypes,
                StatsConstants.AnalyzeType.HISTOGRAM, job.getScheduleType(), job.getProperties());
        sample.setPriority(priority);
        allTableJobMap.add(sample);
    }

    private static void createFullStatsJob(List<StatisticsCollectJob> allTableJobMap,
                                           NativeAnalyzeJob job, BasicStatsMeta stats,
                                           Database db, Table table, List<String> columnNames, List<Type> columnTypes,
                                           StatisticsCollectJob.Priority priority) {
        StatsConstants.AnalyzeType analyzeType;
        List<Partition> partitionList = table.getPartitions().stream()
                .filter(partition -> !StatisticUtils.isPartitionStatsHealthy(table, partition, stats))
                .collect(Collectors.toList());

        long totalDataSize = partitionList.stream().mapToLong(Partition::getDataSize).sum();
        if (job.isDefaultJob() && totalDataSize > Config.statistic_max_full_collect_data_size) {
            analyzeType = StatsConstants.AnalyzeType.SAMPLE;
            LOG.debug("statistics job choose sample on table: {}, partition data size greater than config: {}",
                    table.getName(), Config.statistic_max_full_collect_data_size);
        } else {
            analyzeType = StatsConstants.AnalyzeType.FULL;
        }

        if (!partitionList.isEmpty()) {
            StatisticsCollectJob statisticsCollectJob = buildStatisticsCollectJob(db, table,
                    partitionList.stream().map(Partition::getId).collect(Collectors.toList()), columnNames, columnTypes,
                    analyzeType, job.getScheduleType(), Maps.newHashMap());
            statisticsCollectJob.setPriority(priority);
            allTableJobMap.add(statisticsCollectJob);
        }
    }
}
