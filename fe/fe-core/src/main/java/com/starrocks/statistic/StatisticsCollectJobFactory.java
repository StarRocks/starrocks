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

<<<<<<< HEAD
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
=======
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.connector.ConnectorPartitionTraits;
<<<<<<< HEAD
import com.starrocks.connector.PartitionInfo;
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.monitor.unit.ByteSizeUnit;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
<<<<<<< HEAD
=======
import org.apache.commons.collections4.CollectionUtils;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

<<<<<<< HEAD
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
=======
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StatisticsCollectJobFactory {
    private static final Logger LOG = LogManager.getLogger(StatisticsCollectJobFactory.class);

    private StatisticsCollectJobFactory() {
    }

    public static List<StatisticsCollectJob> buildStatisticsCollectJob(NativeAnalyzeJob nativeAnalyzeJob) {
        List<StatisticsCollectJob> statsJobs = Lists.newArrayList();
        if (StatsConstants.DEFAULT_ALL_ID == nativeAnalyzeJob.getDbId()) {
            // all database
<<<<<<< HEAD
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();

            for (Long dbId : dbIds) {
                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
=======
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();

            for (Long dbId : dbIds) {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                    continue;
                }

<<<<<<< HEAD
                for (Table table : db.getTables()) {
=======
                for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                    createJob(statsJobs, nativeAnalyzeJob, db, table, null, null);
                }
            }
        } else if (StatsConstants.DEFAULT_ALL_ID == nativeAnalyzeJob.getTableId()
                && StatsConstants.DEFAULT_ALL_ID != nativeAnalyzeJob.getDbId()) {
            // all table
<<<<<<< HEAD
            Database db = GlobalStateMgr.getCurrentState().getDb(nativeAnalyzeJob.getDbId());
=======
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(nativeAnalyzeJob.getDbId());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            if (null == db) {
                return Collections.emptyList();
            }

<<<<<<< HEAD
            for (Table table : db.getTables()) {
=======
            for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                createJob(statsJobs, nativeAnalyzeJob, db, table, null, null);
            }
        } else {
            // database or table is null mean database/table has been dropped
<<<<<<< HEAD
            Database db = GlobalStateMgr.getCurrentState().getDb(nativeAnalyzeJob.getDbId());
            if (db == null) {
                return Collections.emptyList();
            }
            createJob(statsJobs, nativeAnalyzeJob, db, db.getTable(nativeAnalyzeJob.getTableId()),
=======
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(nativeAnalyzeJob.getDbId());
            if (db == null) {
                return Collections.emptyList();
            }
            createJob(statsJobs, nativeAnalyzeJob, db, GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(db.getId(), nativeAnalyzeJob.getTableId()),
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                    nativeAnalyzeJob.getColumns(), nativeAnalyzeJob.getColumnTypes());
        }

        return statsJobs;
    }

    public static StatisticsCollectJob buildStatisticsCollectJob(Database db, Table table,
                                                                 List<Long> partitionIdList,
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
        }

        LOG.debug("statistics job work on table: {}, type: {}", table.getName(), analyzeType.name());
        if (analyzeType.equals(StatsConstants.AnalyzeType.SAMPLE)) {
<<<<<<< HEAD
            return new SampleStatisticsCollectJob(db, table, columnNames, columnTypes,
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        } else {
            if (partitionIdList == null) {
                partitionIdList = table.getPartitions().stream().filter(Partition::hasData)
                        .map(Partition::getId).collect(Collectors.toList());
            }
<<<<<<< HEAD
            return new FullStatisticsCollectJob(db, table, partitionIdList, columnNames, columnTypes,
                    StatsConstants.AnalyzeType.FULL, scheduleType, properties);
=======

            if (Config.statistic_use_meta_statistics) {
                return new HyperStatisticsCollectJob(db, table, partitionIdList, columnNames, columnTypes,
                        StatsConstants.AnalyzeType.FULL, scheduleType, properties);
            } else {
                return new FullStatisticsCollectJob(db, table, partitionIdList, columnNames, columnTypes,
                        StatsConstants.AnalyzeType.FULL, scheduleType, properties);
            }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
                    createExternalAnalyzeJob(statsJobs, externalAnalyzeJob, db, table, null, null);
=======
                    createExternalAnalyzeJob(statsJobs, externalAnalyzeJob, db, table, null);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
                createExternalAnalyzeJob(statsJobs, externalAnalyzeJob, db, table, null, null);
=======
                createExternalAnalyzeJob(statsJobs, externalAnalyzeJob, db, table, null);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
            createExternalAnalyzeJob(statsJobs, externalAnalyzeJob, db, table, externalAnalyzeJob.getColumns(),
                    externalAnalyzeJob.getColumnTypes());
=======
            createExternalAnalyzeJob(statsJobs, externalAnalyzeJob, db, table, externalAnalyzeJob.getColumns());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }

        return statsJobs;
    }

    public static StatisticsCollectJob buildExternalStatisticsCollectJob(String catalogName, Database db, Table table,
                                                                         List<String> partitionNames,
                                                                         List<String> columnNames,
                                                                         StatsConstants.AnalyzeType analyzeType,
                                                                         StatsConstants.ScheduleType scheduleType,
                                                                         Map<String, String> properties) {
<<<<<<< HEAD
        List<Type> columnTypes = columnNames.stream().map(col -> table.getColumn(col).getType()).collect(Collectors.toList());
=======
        List<Type> columnTypes =
                columnNames.stream().map(col -> table.getColumn(col).getType()).collect(Collectors.toList());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
        if (partitionNames == null) {
            if (!table.isUnPartitioned()) {
                partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().
                        listPartitionNames(catalogName, db.getFullName(), table.getName());
            } else {
                partitionNames = ImmutableList.of(table.getName());
            }
        }
        return new ExternalFullStatisticsCollectJob(catalogName, db, table, partitionNames, columnNames, columnTypes,
                analyzeType, scheduleType, properties);
    }

    private static void createExternalAnalyzeJob(List<StatisticsCollectJob> allTableJobMap, ExternalAnalyzeJob job,
                                                 Database db, Table table, List<String> columnNames,
                                                 List<Type> columnTypes) {
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
        ExternalBasicStatsMeta basicStatsMeta = GlobalStateMgr.getCurrentAnalyzeMgr().getExternalBasicStatsMetaMap()
                .get(new AnalyzeMgr.StatsMetaKey(job.getCatalogName(), db.getFullName(), table.getName()));
        if (basicStatsMeta != null) {
            // check table last update time, if last collect time is after last update time, skip this table
            LocalDateTime statisticsUpdateTime = basicStatsMeta.getUpdateTime();
            LocalDateTime tableUpdateTime = StatisticUtils.getTableLastUpdateTime(table);
            if (tableUpdateTime != null) {
                if (statisticsUpdateTime.isAfter(tableUpdateTime)) {
                    LOG.info("statistics job doesn't work on non-update table: {}, " +
                                    "last update time: {}, last collect time: {}",
                            table.getName(), tableUpdateTime, statisticsUpdateTime);
                    return;
                }
            }

            // check table row count
            if (columnNames == null || columnNames.isEmpty()) {
                columnNames = StatisticUtils.getCollectibleColumns(table);
                columnTypes = columnNames.stream().map(col -> table.getColumn(col).getType()).collect(Collectors.toList());
            }
            List<ConnectorTableColumnStats> columnStatisticList =
                    GlobalStateMgr.getCurrentStatisticStorage().getConnectorTableStatisticsSync(table, columnNames);
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            List<ConnectorTableColumnStats> validColumnStatistics = columnStatisticList.stream().
                    filter(columnStatistic -> !columnStatistic.isUnknown()).collect(Collectors.toList());

            // use small table row count as default table row count
            long tableRowCount = Config.statistic_auto_collect_small_table_rows - 1;
            if (!validColumnStatistics.isEmpty()) {
                tableRowCount = validColumnStatistics.get(0).getRowCount();
            }
<<<<<<< HEAD
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            long defaultInterval = tableRowCount < Config.statistic_auto_collect_small_table_rows ?
                    Config.statistic_auto_collect_small_table_interval :
                    Config.statistic_auto_collect_large_table_interval;

            long timeInterval = job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL) != null ?
                    Long.parseLong(job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL)) :
                    defaultInterval;
<<<<<<< HEAD
            if (statisticsUpdateTime.plusSeconds(timeInterval).isAfter(LocalDateTime.now())) {
                LOG.info("statistics job doesn't work on the interval table: {}, " +
                                "last collect time: {}, interval: {}, table rows: {}",
                        table.getName(), tableUpdateTime, timeInterval, tableRowCount);
                return;
            }
        }

        if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.FULL)) {
            if (basicStatsMeta == null) {
                createExternalFullStatsJob(allTableJobMap, LocalDateTime.MIN, job, db, table, columnNames, columnTypes);
            } else {
                createExternalFullStatsJob(allTableJobMap, basicStatsMeta.getUpdateTime(), job, db, table, columnNames,
                        columnTypes);
            }
        } else {
            LOG.warn("Do not support analyze type: {} for external table: {}",
                    job.getAnalyzeType(), table.getName());
=======

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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
    }

    private static void createExternalFullStatsJob(List<StatisticsCollectJob> allTableJobMap,
                                                   LocalDateTime statisticsUpdateTime,
                                                   ExternalAnalyzeJob job,
                                                   Database db, Table table, List<String> columnNames,
                                                   List<Type> columnTypes) {
        // get updated partitions
<<<<<<< HEAD
        List<String> updatedPartitions = Lists.newArrayList();
        if (table.isHiveTable()) {
            HiveTable hiveTable = (HiveTable) table;
            if (!hiveTable.isUnPartitioned()) {
                List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                        hiveTable.getCatalogName(), hiveTable.getDbName(), hiveTable.getTableName());
                List<PartitionInfo> partitions = GlobalStateMgr.getCurrentState().getMetadataMgr().
                        getPartitions(hiveTable.getCatalogName(), hiveTable, partitionNames);

                Preconditions.checkState(partitions.size() == partitionNames.size());
                for (int index = 0; index < partitions.size(); index++) {
                    // for external table, we get last modified time from other system, there may be a time inconsistency
                    // between the two systems, so we add 60 seconds to make sure table update time is later than
                    // statistics update time
                    LocalDateTime partitionUpdateTime = LocalDateTime.ofInstant(
                            Instant.ofEpochSecond(partitions.get(index).getModifiedTime()).plusSeconds(60),
                            Clock.systemDefaultZone().getZone());
                    if (partitionUpdateTime.isAfter(statisticsUpdateTime)) {
                        updatedPartitions.add(partitionNames.get(index));
                    }
                }
            }
        } else if (table.isIcebergTable()) {
            if (statisticsUpdateTime != LocalDateTime.MIN) {
                ConnectorPartitionTraits.build(table).getPartitionNameWithPartitionInfo().
                        forEach((partitionName, partitionInfo) -> {
                            // for external table, we get last modified time from other system, there may be a time
                            // inconsistency between the two systems, so we add 60 seconds to make sure table update
                            // time is later than statistics update time
                            LocalDateTime partitionUpdateTime = LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(partitionInfo.getModifiedTime() / 1000).plusSeconds(60),
                                    Clock.systemDefaultZone().getZone());
                            if (partitionUpdateTime.isAfter(statisticsUpdateTime)) {
                                updatedPartitions.add(partitionName);
                            }
                        });
            }
        }
        LOG.info("create external full statistics job for table: {}, partitions: {}",
                table.getName(), updatedPartitions);
        allTableJobMap.add(buildExternalStatisticsCollectJob(job.getCatalogName(), db, table,
                updatedPartitions.isEmpty() ? null : updatedPartitions,
                columnNames, columnTypes, StatsConstants.AnalyzeType.FULL, job.getScheduleType(), Maps.newHashMap()));
    }

=======
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

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    private static void createJob(List<StatisticsCollectJob> allTableJobMap, NativeAnalyzeJob job,
                                  Database db, Table table, List<String> columnNames, List<Type> columnTypes) {
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

            long timeInterval = job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL) != null ?
                    Long.parseLong(job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL)) :
                    defaultInterval;

<<<<<<< HEAD
            if (!basicStatsMeta.isInitJobMeta() &&
                    basicStatsMeta.getUpdateTime().plusSeconds(timeInterval).isAfter(LocalDateTime.now())) {
=======
            if (!isInitJob && statsUpdateTime.plusSeconds(timeInterval).isAfter(LocalDateTime.now())) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                LOG.debug("statistics job doesn't work on the interval table: {}, " +
                                "last collect time: {}, interval: {}, table size: {}MB",
                        table.getName(), tableUpdateTime, timeInterval, ByteSizeUnit.BYTES.toMB(sumDataSize));
                return;
            }

<<<<<<< HEAD
=======
            // 3. if the table stats is healthy enough (only a small portion has been updated)
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
=======
                if (job.getAnalyzeType() != StatsConstants.AnalyzeType.HISTOGRAM &&
                        sumDataSize > Config.statistic_auto_collect_small_table_size) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                    LOG.debug("statistics job choose sample on real-time update table: {}" +
                                    ", last collect time: {}, current healthy: {}, full collect healthy limit: {}, " +
                                    ", update data size: {}MB, full collect healthy data size limit: <{}MB",
                            table.getName(), basicStatsMeta.getUpdateTime(), healthy,
                            Config.statistic_auto_collect_sample_threshold, ByteSizeUnit.BYTES.toMB(sumDataSize),
                            ByteSizeUnit.BYTES.toMB(Config.statistic_auto_collect_small_table_size));
                    createSampleStatsJob(allTableJobMap, job, db, table, columnNames, columnTypes);
                    return;
                }
            }
        }

        LOG.debug("statistics job work on un-health table: {}, healthy: {}, Type: {}", table.getName(), healthy,
                job.getAnalyzeType());
        if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.SAMPLE)) {
            createSampleStatsJob(allTableJobMap, job, db, table, columnNames, columnTypes);
<<<<<<< HEAD
=======
        } else if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.HISTOGRAM)) {
            createHistogramJob(allTableJobMap, job, db, table, columnNames, columnTypes);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        } else if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.FULL)) {
            if (basicStatsMeta == null || basicStatsMeta.isInitJobMeta()) {
                createFullStatsJob(allTableJobMap, job, LocalDateTime.MIN, db, table, columnNames, columnTypes);
            } else {
                createFullStatsJob(allTableJobMap, job, basicStatsMeta.getUpdateTime(), db, table, columnNames,
                        columnTypes);
            }
        } else {
            throw new StarRocksPlannerException("Unknown analyze type " + job.getAnalyzeType(),
                    ErrorType.INTERNAL_ERROR);
        }
    }

    private static void createSampleStatsJob(List<StatisticsCollectJob> allTableJobMap, NativeAnalyzeJob job,
                                             Database db, Table table, List<String> columnNames,
                                             List<Type> columnTypes) {
        StatisticsCollectJob sample = buildStatisticsCollectJob(db, table, null, columnNames, columnTypes,
                StatsConstants.AnalyzeType.SAMPLE, job.getScheduleType(), job.getProperties());
        allTableJobMap.add(sample);
    }

<<<<<<< HEAD
=======
    private static void createHistogramJob(List<StatisticsCollectJob> allTableJobMap, NativeAnalyzeJob job,
                                           Database db, Table table, List<String> columnNames,
                                           List<Type> columnTypes) {
        StatisticsCollectJob sample = buildStatisticsCollectJob(db, table, null, columnNames, columnTypes,
                StatsConstants.AnalyzeType.HISTOGRAM, job.getScheduleType(), job.getProperties());
        allTableJobMap.add(sample);
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    private static void createFullStatsJob(List<StatisticsCollectJob> allTableJobMap,
                                           NativeAnalyzeJob job, LocalDateTime statsLastUpdateTime,
                                           Database db, Table table, List<String> columnNames, List<Type> columnTypes) {
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
                    partitionList.stream().map(Partition::getId).collect(Collectors.toList()), columnNames, columnTypes,
                    analyzeType, job.getScheduleType(), Maps.newHashMap()));
        }
    }
}
