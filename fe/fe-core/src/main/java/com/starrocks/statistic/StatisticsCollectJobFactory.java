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
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StatisticsCollectJobFactory {
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
            //database or table is null mean database/table has been dropped
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

    private static void createJob(List<StatisticsCollectJob> allTableJobMap, AnalyzeJob job,
                                  Database db, Table table, List<String> columns) {
        if (table == null || !(table.isOlapOrLakeTable() || table.isMaterializedView())) {
            return;
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
                return;
            }
        }

        BasicStatsMeta basicStatsMeta = GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().get(table.getId());
        if (basicStatsMeta != null) {
            if (basicStatsMeta.getType().equals(StatsConstants.AnalyzeType.SAMPLE)
                    && job.getAnalyzeType().equals(StatsConstants.AnalyzeType.FULL)
                    && table.getPartitions().stream()
                    .noneMatch(p -> p.getDataSize() > Config.statistic_max_full_collect_data_size)) {
                createFullStatsJob(allTableJobMap, job, LocalDateTime.MIN, db, table, columns);
                return;
            }

            LocalDateTime tableUpdateTime = StatisticUtils.getTableLastUpdateTime(table);
            LocalDateTime statisticsUpdateTime = basicStatsMeta.getUpdateTime();
            if (statisticsUpdateTime.isAfter(tableUpdateTime)) {
                return;
            }

            double statisticAutoCollectRatio =
                    job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_RATIO) != null ?
                            Double.parseDouble(job.getProperties().get(StatsConstants.STATISTIC_AUTO_COLLECT_RATIO)) :
                            Config.statistic_auto_collect_ratio;
            if (basicStatsMeta.getHealthy() > statisticAutoCollectRatio) {
                return;
            }
        }

        if (job.getAnalyzeType().equals(StatsConstants.AnalyzeType.SAMPLE)) {
            allTableJobMap.add(buildStatisticsCollectJob(db, table, null, columns,
                    job.getAnalyzeType(), job.getScheduleType(), job.getProperties()));
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

    private static void createFullStatsJob(List<StatisticsCollectJob> allTableJobMap,
                                           AnalyzeJob job, LocalDateTime statsLastUpdateTime,
                                           Database db, Table table, List<String> columns) {
        StatsConstants.AnalyzeType analyzeType;
        if (table.getPartitions().stream().anyMatch(
                p -> p.getDataSize() > Config.statistic_max_full_collect_data_size)) {
            analyzeType = StatsConstants.AnalyzeType.SAMPLE;
        } else {
            analyzeType = StatsConstants.AnalyzeType.FULL;
        }

        List<Partition> partitions = Lists.newArrayList(table.getPartitions());
        List<Long> partitionIdList = new ArrayList<>();
        for (Partition partition : partitions) {
            LocalDateTime partitionUpdateTime = StatisticUtils.getPartitionLastUpdateTime(partition);
            if (statsLastUpdateTime.isBefore(partitionUpdateTime) && partition.hasData()) {
                partitionIdList.add(partition.getId());
            }
        }

        if (!partitionIdList.isEmpty()) {
            allTableJobMap.add(buildStatisticsCollectJob(db, table, partitionIdList, columns,
                    analyzeType, job.getScheduleType(), Maps.newHashMap()));
        }
    }
}
