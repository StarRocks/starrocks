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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
<<<<<<< HEAD
import com.starrocks.common.Config;
=======
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.monitor.unit.ByteSizeUnit;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
<<<<<<< HEAD
=======
import org.apache.commons.collections4.CollectionUtils;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
<<<<<<< HEAD
import java.util.List;
import java.util.Map;
=======
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StatisticsCollectJobFactory {
    private static final Logger LOG = LogManager.getLogger(StatisticsCollectJobFactory.class);

    private StatisticsCollectJobFactory() {
    }

<<<<<<< HEAD
    public static List<StatisticsCollectJob> buildStatisticsCollectJob(AnalyzeJob analyzeJob) {
        List<StatisticsCollectJob> statsJobs = Lists.newArrayList();
        if (StatsConstants.DEFAULT_ALL_ID == analyzeJob.getDbId()) {
            // all database
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();

            for (Long dbId : dbIds) {
                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
=======
    public static List<StatisticsCollectJob> buildStatisticsCollectJob(NativeAnalyzeJob nativeAnalyzeJob) {
        List<StatisticsCollectJob> statsJobs = Lists.newArrayList();
        if (StatsConstants.DEFAULT_ALL_ID == nativeAnalyzeJob.getDbId()) {
            // all database
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();

            for (Long dbId : dbIds) {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                    continue;
                }

<<<<<<< HEAD
                for (Table table : db.getTables()) {
                    createJob(statsJobs, analyzeJob, db, table, null);
                }
            }
        } else if (StatsConstants.DEFAULT_ALL_ID == analyzeJob.getTableId()
                && StatsConstants.DEFAULT_ALL_ID != analyzeJob.getDbId()) {
            // all table
            Database db = GlobalStateMgr.getCurrentState().getDb(analyzeJob.getDbId());
=======
                for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                    createJob(statsJobs, nativeAnalyzeJob, db, table, null, null);
                }
            }
        } else if (StatsConstants.DEFAULT_ALL_ID == nativeAnalyzeJob.getTableId()
                && StatsConstants.DEFAULT_ALL_ID != nativeAnalyzeJob.getDbId()) {
            // all table
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(nativeAnalyzeJob.getDbId());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            if (null == db) {
                return Collections.emptyList();
            }

<<<<<<< HEAD
            for (Table table : db.getTables()) {
                createJob(statsJobs, analyzeJob, db, table, null);
            }
        } else {
            // database or table is null mean database/table has been dropped
            Database db = GlobalStateMgr.getCurrentState().getDb(analyzeJob.getDbId());
            if (db == null) {
                return Collections.emptyList();
            }
            createJob(statsJobs, analyzeJob, db, db.getTable(analyzeJob.getTableId()), analyzeJob.getColumns());
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        return statsJobs;
    }

    public static StatisticsCollectJob buildStatisticsCollectJob(Database db, Table table,
                                                                 List<Long> partitionIdList,
<<<<<<< HEAD
                                                                 List<String> columns,
                                                                 StatsConstants.AnalyzeType analyzeType,
                                                                 StatsConstants.ScheduleType scheduleType,
                                                                 Map<String, String> properties) {
        if (columns == null || columns.isEmpty()) {
            columns = StatisticUtils.getCollectibleColumns(table);
=======
                                                                 List<String> columnNames,
                                                                 List<Type> columnTypes,
                                                                 StatsConstants.AnalyzeType analyzeType,
                                                                 StatsConstants.ScheduleType scheduleType,
                                                                 Map<String, String> properties) {
        if (columnNames == null || columnNames.isEmpty()) {
            columnNames = StatisticUtils.getCollectibleColumns(table);
            columnTypes = columnNames.stream().map(col -> table.getColumn(col).getType()).collect(Collectors.toList());
        }
        // for compatibility, if columnTypes is null, we will get column types from table
        if (columnTypes == null || columnTypes.isEmpty()) {
            columnTypes = columnNames.stream().map(col -> table.getColumn(col).getType()).collect(Collectors.toList());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        LOG.debug("statistics job work on table: {}, type: {}", table.getName(), analyzeType.name());
        if (analyzeType.equals(StatsConstants.AnalyzeType.SAMPLE)) {
<<<<<<< HEAD
            return new SampleStatisticsCollectJob(db, table, columns,
                    StatsConstants.AnalyzeType.SAMPLE, scheduleType, properties);
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        } else {
            if (partitionIdList == null) {
                partitionIdList = table.getPartitions().stream().filter(Partition::hasData)
                        .map(Partition::getId).collect(Collectors.toList());
            }
<<<<<<< HEAD
            return new FullStatisticsCollectJob(db, table, partitionIdList, columns,
                    StatsConstants.AnalyzeType.FULL, scheduleType, properties);
        }
    }

    public static StatisticsCollectJob buildExternalStatisticsCollectJob(String catalogName, Database db, Table table,
                                                                         List<String> columns,
                                                                         StatsConstants.AnalyzeType analyzeType,
                                                                         StatsConstants.ScheduleType scheduleType,
                                                                         Map<String, String> properties) {
        if (columns == null || columns.isEmpty()) {
            columns = StatisticUtils.getCollectibleColumns(table);
        }
        return new ExternalFullStatisticsCollectJob(catalogName, db, table, columns,
                analyzeType, scheduleType, properties);
    }

    private static void createJob(List<StatisticsCollectJob> allTableJobMap, AnalyzeJob job,
                                  Database db, Table table, List<String> columns) {
=======

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

            long timeInterval = job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL) != null ?
                    Long.parseLong(job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL)) :
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        if (table == null || !(table.isOlapOrCloudNativeTable() || table.isMaterializedView())) {
            return;
        }

        if (table instanceof OlapTable) {
            if (((OlapTable) table).getState() != OlapTable.OlapTableState.NORMAL) {
                return;
            }
        }
<<<<<<< HEAD
=======
        if (!Config.enable_temporary_table_statistic_collect && table.isTemporaryTable()) {
            LOG.debug("statistics job doesn't work on temporary table: {}", table.getName());
            return;
        }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

        if (StatisticUtils.isEmptyTable(table)) {
            return;
        }

        // check job exclude db.table
        String regex = job.getProperties().getOrDefault(StatsConstants.STATISTIC_EXCLUDE_PATTERN, null);
        if (StringUtils.isNotBlank(regex)) {
            Pattern checkRegex = Pattern.compile(regex);
            String name = db.getFullName() + "." + table.getName();
            if (checkRegex.matcher(name).find()) {
                LOG.debug("statistics job exclude pattern {}, hit table: {}", regex, name);
                return;
            }
        }

<<<<<<< HEAD
        BasicStatsMeta basicStatsMeta = GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(table.getId());
        double healthy = 0;
        LocalDateTime tableUpdateTime = StatisticUtils.getTableLastUpdateTime(table);
        if (basicStatsMeta != null) {
            if (basicStatsMeta.isUpdatedAfterLoad(tableUpdateTime)) {
=======
        AnalyzeMgr analyzeMgr = GlobalStateMgr.getCurrentState().getAnalyzeMgr();
        BasicStatsMeta basicStatsMeta = analyzeMgr.getTableBasicStatsMeta(table.getId());
        double healthy = 0;
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
                            .min(Comparator.naturalOrder())
                            .get();
            isInitJob = histogramStatsMetas.stream().anyMatch(HistogramStatsMeta::isInitJobMeta);
        }

        if (basicStatsMeta != null) {
            // 1. if the table has no update after the stats collection
            if (useBasicStats && basicStatsMeta.isUpdatedAfterLoad(tableUpdateTime)) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                LOG.debug("statistics job doesn't work on non-update table: {}, " +
                                "last update time: {}, last collect time: {}",
                        table.getName(), tableUpdateTime, basicStatsMeta.getUpdateTime());
                return;
            }
<<<<<<< HEAD

=======
            if (!useBasicStats && !isInitJob && statsUpdateTime.isAfter(tableUpdateTime)) {
                return;
            }

            // 2. if the stats collection is too frequent
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            long sumDataSize = 0;
            for (Partition partition : table.getPartitions()) {
                LocalDateTime partitionUpdateTime = StatisticUtils.getPartitionLastUpdateTime(partition);
                if (!basicStatsMeta.isUpdatedAfterLoad(partitionUpdateTime)) {
                    sumDataSize += partition.getDataSize();
                }
            }

<<<<<<< HEAD
            long defaultInterval = sumDataSize > Config.statistic_auto_collect_small_table_size ?
                    Config.statistic_auto_collect_large_table_interval :
                    Config.statistic_auto_collect_small_table_interval;
=======
            long defaultInterval =
                    job.getAnalyzeType() == StatsConstants.AnalyzeType.HISTOGRAM ?
                            Config.statistic_auto_collect_histogram_interval :
                            (sumDataSize > Config.statistic_auto_collect_small_table_size ?
                                    Config.statistic_auto_collect_large_table_interval :
                                    Config.statistic_auto_collect_small_table_interval);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

            long timeInterval = job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL) != null ?
                    Long.parseLong(job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL)) :
                    defaultInterval;

<<<<<<< HEAD
            if (!basicStatsMeta.isInitJobMeta() &&
                    basicStatsMeta.getUpdateTime().plusSeconds(timeInterval).isAfter(LocalDateTime.now())) {
=======
            if (!isInitJob && statsUpdateTime.plusSeconds(timeInterval).isAfter(LocalDateTime.now())) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                LOG.debug("statistics job doesn't work on the interval table: {}, " +
                                "last collect time: {}, interval: {}, table size: {}MB",
                        table.getName(), tableUpdateTime, timeInterval, ByteSizeUnit.BYTES.toMB(sumDataSize));
                return;
            }

<<<<<<< HEAD
=======
            // 3. if the table stats is healthy enough (only a small portion has been updated)
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            double statisticAutoCollectRatio =
                    job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_RATIO) != null ?
                            Double.parseDouble(job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_RATIO)) :
                            Config.statistic_auto_collect_ratio;

            healthy = basicStatsMeta.getHealthy();
            if (healthy > statisticAutoCollectRatio) {
                LOG.debug("statistics job doesn't work on health table: {}, healthy: {}, collect healthy limit: <{}",
                        table.getName(), healthy, statisticAutoCollectRatio);
                return;
            } else if (healthy < Config.statistic_auto_collect_sample_threshold) {
<<<<<<< HEAD
                if (sumDataSize > Config.statistic_auto_collect_small_table_size) {
                    LOG.debug("statistics job choose sample on real-time update table: {}" +
                                    ", last collect time: {}, current healthy: {}, full collect healthy limit: {}<, " +
=======
                if (job.getAnalyzeType() != StatsConstants.AnalyzeType.HISTOGRAM &&
                        sumDataSize > Config.statistic_auto_collect_small_table_size) {
                    LOG.debug("statistics job choose sample on real-time update table: {}" +
                                    ", last collect time: {}, current healthy: {}, full collect healthy limit: {}, " +
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                                    ", update data size: {}MB, full collect healthy data size limit: <{}MB",
                            table.getName(), basicStatsMeta.getUpdateTime(), healthy,
                            Config.statistic_auto_collect_sample_threshold, ByteSizeUnit.BYTES.toMB(sumDataSize),
                            ByteSizeUnit.BYTES.toMB(Config.statistic_auto_collect_small_table_size));
<<<<<<< HEAD
                    createSampleStatsJob(allTableJobMap, job, db, table, columns);
=======
                    createSampleStatsJob(allTableJobMap, job, db, table, columnNames, columnTypes);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                    return;
                }
            }
        }

        LOG.debug("statistics job work on un-health table: {}, healthy: {}, Type: {}", table.getName(), healthy,
                job.getAnalyzeType());
        if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.SAMPLE)) {
<<<<<<< HEAD
            createSampleStatsJob(allTableJobMap, job, db, table, columns);
        } else if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.FULL)) {
            if (basicStatsMeta == null || basicStatsMeta.isInitJobMeta()) {
                createFullStatsJob(allTableJobMap, job, LocalDateTime.MIN, db, table, columns);
            } else {
                createFullStatsJob(allTableJobMap, job, basicStatsMeta.getUpdateTime(), db, table, columns);
=======
            createSampleStatsJob(allTableJobMap, job, db, table, columnNames, columnTypes);
        } else if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.HISTOGRAM)) {
            createHistogramJob(allTableJobMap, job, db, table, columnNames, columnTypes);
        } else if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.FULL)) {
            if (basicStatsMeta == null || basicStatsMeta.isInitJobMeta()) {
                createFullStatsJob(allTableJobMap, job, LocalDateTime.MIN, db, table, columnNames, columnTypes);
            } else {
                createFullStatsJob(allTableJobMap, job, basicStatsMeta.getUpdateTime(), db, table, columnNames,
                        columnTypes);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        } else {
            throw new StarRocksPlannerException("Unknown analyze type " + job.getAnalyzeType(),
                    ErrorType.INTERNAL_ERROR);
        }
    }

<<<<<<< HEAD
    private static void createSampleStatsJob(List<StatisticsCollectJob> allTableJobMap, AnalyzeJob job, Database db,
                                             Table table, List<String> columns) {
        StatisticsCollectJob sample = buildStatisticsCollectJob(db, table, null, columns,
=======
    private static void createSampleStatsJob(List<StatisticsCollectJob> allTableJobMap, NativeAnalyzeJob job,
                                             Database db, Table table, List<String> columnNames,
                                             List<Type> columnTypes) {
        StatisticsCollectJob sample = buildStatisticsCollectJob(db, table, null, columnNames, columnTypes,
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                StatsConstants.AnalyzeType.SAMPLE, job.getScheduleType(), job.getProperties());
        allTableJobMap.add(sample);
    }

<<<<<<< HEAD
    private static void createFullStatsJob(List<StatisticsCollectJob> allTableJobMap,
                                           AnalyzeJob job, LocalDateTime statsLastUpdateTime,
                                           Database db, Table table, List<String> columns) {
=======
    private static void createHistogramJob(List<StatisticsCollectJob> allTableJobMap, NativeAnalyzeJob job,
                                           Database db, Table table, List<String> columnNames,
                                           List<Type> columnTypes) {
        StatisticsCollectJob sample = buildStatisticsCollectJob(db, table, null, columnNames, columnTypes,
                StatsConstants.AnalyzeType.HISTOGRAM, job.getScheduleType(), job.getProperties());
        allTableJobMap.add(sample);
    }

    private static void createFullStatsJob(List<StatisticsCollectJob> allTableJobMap,
                                           NativeAnalyzeJob job, LocalDateTime statsLastUpdateTime,
                                           Database db, Table table, List<String> columnNames, List<Type> columnTypes) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        StatsConstants.AnalyzeType analyzeType;
        List<Partition> partitionList = new ArrayList<>();
        for (Partition partition : table.getPartitions()) {
            LocalDateTime partitionUpdateTime = StatisticUtils.getPartitionLastUpdateTime(partition);
            if (statsLastUpdateTime.isBefore(partitionUpdateTime) && partition.hasData()) {
                partitionList.add(partition);
            }
        }

        if (partitionList.stream().anyMatch(p -> p.getDataSize() > Config.statistic_max_full_collect_data_size)) {
            analyzeType = StatsConstants.AnalyzeType.SAMPLE;
            LOG.debug("statistics job choose sample on table: {}, partition data size greater than config: {}",
                    table.getName(), Config.statistic_max_full_collect_data_size);
        } else {
            analyzeType = StatsConstants.AnalyzeType.FULL;
        }

        if (!partitionList.isEmpty()) {
            allTableJobMap.add(buildStatisticsCollectJob(db, table,
<<<<<<< HEAD
                    partitionList.stream().map(Partition::getId).collect(Collectors.toList()), columns, analyzeType,
                    job.getScheduleType(), Maps.newHashMap()));
=======
                    partitionList.stream().map(Partition::getId).collect(Collectors.toList()), columnNames, columnTypes,
                    analyzeType, job.getScheduleType(), Maps.newHashMap()));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
    }
}
