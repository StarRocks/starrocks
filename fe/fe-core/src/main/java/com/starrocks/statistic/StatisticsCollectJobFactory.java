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
import com.starrocks.common.Config;
import com.starrocks.monitor.unit.ByteSizeUnit;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
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

    public static List<StatisticsCollectJob> buildStatisticsCollectJob(AnalyzeJob analyzeJob) {
        List<StatisticsCollectJob> statsJobs = Lists.newArrayList();
        if (StatsConstants.DEFAULT_ALL_ID == analyzeJob.getDbId()) {
            // all database
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();

            for (Long dbId : dbIds) {
                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
                if (null == db || StatisticUtils.statisticDatabaseBlackListCheck(db.getFullName())) {
                    continue;
                }

                for (Table table : db.getTables()) {
                    createJob(statsJobs, analyzeJob, db, table, null);
                }
            }
        } else if (StatsConstants.DEFAULT_ALL_ID == analyzeJob.getTableId()
                && StatsConstants.DEFAULT_ALL_ID != analyzeJob.getDbId()) {
            // all table
            Database db = GlobalStateMgr.getCurrentState().getDb(analyzeJob.getDbId());
            if (null == db) {
                return Collections.emptyList();
            }

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

        BasicStatsMeta basicStatsMeta = GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(table.getId());
        double healthy = 0;
        if (basicStatsMeta != null) {
            LocalDateTime tableUpdateTime = StatisticUtils.getTableLastUpdateTime(table);
            LocalDateTime statisticsUpdateTime = basicStatsMeta.getUpdateTime();
            if (statisticsUpdateTime.isAfter(tableUpdateTime)) {
                LOG.debug("statistics job doesn't work on non-update table: {}, " +
                                "last update time: {}, last collect time: {}",
                        table.getName(), tableUpdateTime, statisticsUpdateTime);
                return;
            }

            long sumDataSize = 0;
            for (Partition partition : table.getPartitions()) {
                LocalDateTime partitionUpdateTime = StatisticUtils.getPartitionLastUpdateTime(partition);
                if (statisticsUpdateTime.isBefore(partitionUpdateTime)) {
                    sumDataSize += partition.getDataSize();
                }
            }

            long defaultInterval = sumDataSize > Config.statistic_auto_collect_small_table_size ?
                    Config.statistic_auto_collect_large_table_interval :
                    Config.statistic_auto_collect_small_table_interval;

            long timeInterval = job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL) != null ?
                    Long.parseLong(job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL)) :
                    defaultInterval;

            if (statisticsUpdateTime.plusSeconds(timeInterval).isAfter(LocalDateTime.now())) {
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
                                    ", last collect time: {}, current healthy: {}, full collect healthy limit: {}<, " +
                                    ", update data size: {}MB, full collect healthy data size limit: <{}MB",
                            table.getName(), statisticsUpdateTime, healthy,
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
            if (basicStatsMeta == null) {
                createFullStatsJob(allTableJobMap, job, LocalDateTime.MIN, db, table, columns);
            } else {
                createFullStatsJob(allTableJobMap, job, basicStatsMeta.getUpdateTime(), db, table, columns);
            }
        } else {
            throw new StarRocksPlannerException("Unknown analyze type " + job.getAnalyzeType(),
                    ErrorType.INTERNAL_ERROR);
        }
    }

    private static void createSampleStatsJob(List<StatisticsCollectJob> allTableJobMap, AnalyzeJob job, Database db,
                                             Table table, List<String> columns) {
        StatisticsCollectJob sample = buildStatisticsCollectJob(db, table, null, columns,
                StatsConstants.AnalyzeType.SAMPLE, job.getScheduleType(), job.getProperties());
        allTableJobMap.add(sample);
    }

    private static void createFullStatsJob(List<StatisticsCollectJob> allTableJobMap,
                                           AnalyzeJob job, LocalDateTime statsLastUpdateTime,
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
