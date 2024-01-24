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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.ConnectorTableColumnStats;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;
import com.starrocks.monitor.unit.ByteSizeUnit;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();

            for (Long dbId : dbIds) {
                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                    continue;
                }

                for (Table table : db.getTables()) {
                    createJob(statsJobs, nativeAnalyzeJob, db, table, null);
                }
            }
        } else if (StatsConstants.DEFAULT_ALL_ID == nativeAnalyzeJob.getTableId()
                && StatsConstants.DEFAULT_ALL_ID != nativeAnalyzeJob.getDbId()) {
            // all table
            Database db = GlobalStateMgr.getCurrentState().getDb(nativeAnalyzeJob.getDbId());
            if (null == db) {
                return Collections.emptyList();
            }

            for (Table table : db.getTables()) {
                createJob(statsJobs, nativeAnalyzeJob, db, table, null);
            }
        } else {
            // database or table is null mean database/table has been dropped
            Database db = GlobalStateMgr.getCurrentState().getDb(nativeAnalyzeJob.getDbId());
            if (db == null) {
                return Collections.emptyList();
            }
            createJob(statsJobs, nativeAnalyzeJob, db, db.getTable(nativeAnalyzeJob.getTableId()), nativeAnalyzeJob.getColumns());
        }

        return statsJobs;
    }

    public static StatisticsCollectJob buildStatisticsCollectJob(Database db, Table table,
                                                                 List<Long> partitionIdList,
                                                                 List<String> columns,
                                                                 StatsConstants.AnalyzeType analyzeType,
                                                                 StatsConstants.ScheduleType scheduleType,
                                                                 Map<String, String> properties) {
        if (columns == null || columns.isEmpty()) {
            columns = StatisticUtils.getCollectibleColumns(table);
        }

        LOG.debug("statistics job work on table: {}, type: {}", table.getName(), analyzeType.name());
        if (analyzeType.equals(StatsConstants.AnalyzeType.SAMPLE)) {
            return new SampleStatisticsCollectJob(db, table, columns,
                    StatsConstants.AnalyzeType.SAMPLE, scheduleType, properties);
        } else {
            if (partitionIdList == null) {
                partitionIdList = table.getPartitions().stream().filter(Partition::hasData)
                        .map(Partition::getId).collect(Collectors.toList());
            }
            return new FullStatisticsCollectJob(db, table, partitionIdList, columns,
                    StatsConstants.AnalyzeType.FULL, scheduleType, properties);
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
                                                                         List<String> columns,
                                                                         StatsConstants.AnalyzeType analyzeType,
                                                                         StatsConstants.ScheduleType scheduleType,
                                                                         Map<String, String> properties) {
        // refresh table to get latest table/partition info
        GlobalStateMgr.getCurrentState().getMetadataMgr().refreshTable(catalogName,
                db.getFullName(), table, Lists.newArrayList(), true);

        if (columns == null || columns.isEmpty()) {
            columns = StatisticUtils.getCollectibleColumns(table);
        }

        if (partitionNames == null) {
            if (!table.isUnPartitioned()) {
                partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().
                        listPartitionNames(catalogName, db.getFullName(), table.getName());
            } else {
                partitionNames = ImmutableList.of(table.getName());
            }
        }
        return new ExternalFullStatisticsCollectJob(catalogName, db, table, partitionNames, columns,
                analyzeType, scheduleType, properties);
    }

    private static void createExternalAnalyzeJob(List<StatisticsCollectJob> allTableJobMap, ExternalAnalyzeJob job,
                                                 Database db, Table table, List<String> columns) {
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
            if (columns == null || columns.isEmpty()) {
                columns = StatisticUtils.getCollectibleColumns(table);
            }
            List<ConnectorTableColumnStats> columnStatisticList =
                    GlobalStateMgr.getCurrentState().getStatisticStorage().getConnectorTableStatisticsSync(table, columns);
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
            if (statisticsUpdateTime.plusSeconds(timeInterval).isAfter(LocalDateTime.now())) {
                LOG.info("statistics job doesn't work on the interval table: {}, " +
                                "last collect time: {}, interval: {}, table rows: {}",
                        table.getName(), tableUpdateTime, timeInterval, tableRowCount);
                return;
            }
        }

        if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.FULL)) {
            if (basicStatsMeta == null) {
                createExternalFullStatsJob(allTableJobMap, LocalDateTime.MIN, job, db, table, columns);
            } else {
                createExternalFullStatsJob(allTableJobMap, basicStatsMeta.getUpdateTime(), job, db, table, columns);
            }
        } else {
            LOG.warn("Do not support analyze type: {} for external table: {}",
                    job.getAnalyzeType(), table.getName());
        }
    }

    private static void createExternalFullStatsJob(List<StatisticsCollectJob> allTableJobMap,
                                                   LocalDateTime statisticsUpdateTime,
                                                   ExternalAnalyzeJob job,
                                                   Database db, Table table, List<String> columns) {
        // get updated partitions
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
            IcebergTable icebergTable = (IcebergTable) table;
            if (statisticsUpdateTime != LocalDateTime.MIN && !icebergTable.isUnPartitioned()) {
                updatedPartitions.addAll(IcebergPartitionUtils.getChangedPartitionNames(icebergTable.getNativeTable(),
                        statisticsUpdateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
                                - 60 * 1000L,
                        icebergTable.getNativeTable().currentSnapshot()));
            }
        }
        LOG.info("create external full statistics job for table: {}, partitions: {}",
                table.getName(), updatedPartitions);
        allTableJobMap.add(buildExternalStatisticsCollectJob(job.getCatalogName(), db, table,
                updatedPartitions.isEmpty() ? null : updatedPartitions,
                columns, StatsConstants.AnalyzeType.FULL, job.getScheduleType(), Maps.newHashMap()));
    }

    private static void createJob(List<StatisticsCollectJob> allTableJobMap, NativeAnalyzeJob job,
                                  Database db, Table table, List<String> columns) {
        if (table == null || !(table.isOlapOrCloudNativeTable() || table.isMaterializedView())) {
            return;
        }

        if (table instanceof OlapTable) {
            if (((OlapTable) table).getState() != OlapTable.OlapTableState.NORMAL) {
                return;
            }
        }

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

        BasicStatsMeta basicStatsMeta =
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().get(table.getId());
        double healthy = 0;
        LocalDateTime tableUpdateTime = StatisticUtils.getTableLastUpdateTime(table);
        if (basicStatsMeta != null) {
            if (basicStatsMeta.isUpdatedAfterLoad(tableUpdateTime)) {
                LOG.debug("statistics job doesn't work on non-update table: {}, " +
                                "last update time: {}, last collect time: {}",
                        table.getName(), tableUpdateTime, basicStatsMeta.getUpdateTime());
                return;
            }

            long sumDataSize = 0;
            for (Partition partition : table.getPartitions()) {
                LocalDateTime partitionUpdateTime = StatisticUtils.getPartitionLastUpdateTime(partition);
                if (!basicStatsMeta.isUpdatedAfterLoad(partitionUpdateTime)) {
                    sumDataSize += partition.getDataSize();
                }
            }

            long defaultInterval = sumDataSize > Config.statistic_auto_collect_small_table_size ?
                    Config.statistic_auto_collect_large_table_interval :
                    Config.statistic_auto_collect_small_table_interval;

            long timeInterval = job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL) != null ?
                    Long.parseLong(job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL)) :
                    defaultInterval;

            if (!basicStatsMeta.isInitJobMeta() &&
                    basicStatsMeta.getUpdateTime().plusSeconds(timeInterval).isAfter(LocalDateTime.now())) {
                LOG.debug("statistics job doesn't work on the interval table: {}, " +
                                "last collect time: {}, interval: {}, table size: {}MB",
                        table.getName(), tableUpdateTime, timeInterval, ByteSizeUnit.BYTES.toMB(sumDataSize));
                return;
            }

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
                if (sumDataSize > Config.statistic_auto_collect_small_table_size) {
                    LOG.debug("statistics job choose sample on real-time update table: {}" +
                                    ", last collect time: {}, current healthy: {}, full collect healthy limit: {}, " +
                                    ", update data size: {}MB, full collect healthy data size limit: <{}MB",
                            table.getName(), basicStatsMeta.getUpdateTime(), healthy,
                            Config.statistic_auto_collect_sample_threshold, ByteSizeUnit.BYTES.toMB(sumDataSize),
                            ByteSizeUnit.BYTES.toMB(Config.statistic_auto_collect_small_table_size));
                    createSampleStatsJob(allTableJobMap, job, db, table, columns);
                    return;
                }
            }
        }

        LOG.debug("statistics job work on un-health table: {}, healthy: {}, Type: {}", table.getName(), healthy,
                job.getAnalyzeType());
        if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.SAMPLE)) {
            createSampleStatsJob(allTableJobMap, job, db, table, columns);
        } else if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.FULL)) {
            if (basicStatsMeta == null || basicStatsMeta.isInitJobMeta()) {
                createFullStatsJob(allTableJobMap, job, LocalDateTime.MIN, db, table, columns);
            } else {
                createFullStatsJob(allTableJobMap, job, basicStatsMeta.getUpdateTime(), db, table, columns);
            }
        } else {
            throw new StarRocksPlannerException("Unknown analyze type " + job.getAnalyzeType(),
                    ErrorType.INTERNAL_ERROR);
        }
    }

    private static void createSampleStatsJob(List<StatisticsCollectJob> allTableJobMap, NativeAnalyzeJob job, Database db,
                                             Table table, List<String> columns) {
        StatisticsCollectJob sample = buildStatisticsCollectJob(db, table, null, columns,
                StatsConstants.AnalyzeType.SAMPLE, job.getScheduleType(), job.getProperties());
        allTableJobMap.add(sample);
    }

    private static void createFullStatsJob(List<StatisticsCollectJob> allTableJobMap,
                                           NativeAnalyzeJob job, LocalDateTime statsLastUpdateTime,
                                           Database db, Table table, List<String> columns) {
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
                    partitionList.stream().map(Partition::getId).collect(Collectors.toList()), columns, analyzeType,
                    job.getScheduleType(), Maps.newHashMap()));
        }
    }
}
