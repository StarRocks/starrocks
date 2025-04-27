// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.optimizer.statistics;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.common.Pair;
import com.starrocks.statistic.StatisticUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class ExternalMultiColumnCombinedStatsCacheLoader
        implements AsyncCacheLoader<ExternalColumnNDVStats.ExternalColumnNDVStatsKey, MultiColumnCombinedStatistics> {
    private static final Logger LOG = LogManager.getLogger(ExternalMultiColumnCombinedStatsCacheLoader.class);

    @Override
    public CompletableFuture<MultiColumnCombinedStatistics> asyncLoad(
            ExternalColumnNDVStats.ExternalColumnNDVStatsKey key, Executor executor) {
        try {
            return CompletableFuture.supplyAsync(() -> load(key, null), executor);
        } catch (Exception e) {
            LOG.error("Load multi-column combined statistics failed. key=[{}]", key, e);
            return CompletableFuture.completedFuture(new MultiColumnCombinedStatistics());
        }
    }

    @Override
    public CompletableFuture<MultiColumnCombinedStatistics> asyncReload(
            ExternalColumnNDVStats.ExternalColumnNDVStatsKey key,
            MultiColumnCombinedStatistics oldValue,
            Executor executor) {
        try {
            return CompletableFuture.supplyAsync(() -> load(key, oldValue), executor);
        } catch (Exception e) {
            LOG.error("Reload multi-column combined statistics failed. key=[{}]", key, e);
            return CompletableFuture.completedFuture(oldValue);
        }
    }

    private MultiColumnCombinedStatistics load(ExternalColumnNDVStats.ExternalColumnNDVStatsKey key,
                                               MultiColumnCombinedStatistics oldValue) {
        if (oldValue != null && !oldValue.isEmpty()) {
            return oldValue;
        }

        String tableUUID = key.getTableUUID();
        String catalogName = key.getCatalogName();
        String dbName = key.getDbName();
        String tableName = key.getTableName();
        List<Set<Long>> columnIdsList = new ArrayList<>();

        try {
            // Query all available multi-column stats from statsdb
            String sql = String.format(
                    "SELECT column_group, column_names, ndv " +
                    "FROM %s.%s " +
                    "WHERE table_uuid='%s' AND catalog_name='%s' AND db_name='%s' AND table_name='%s'",
                    com.starrocks.statistic.StatsConstants.STATISTICS_DB_NAME,
                    com.starrocks.statistic.StatsConstants.EXTERNAL_MULTI_COLUMN_STATISTICS_TABLE_NAME,
                    tableUUID, catalogName, dbName, tableName);

            List<List<String>> rows = StatisticUtils.queryStatistics(sql);

            if (rows.isEmpty()) {
                return new MultiColumnCombinedStatistics();
            }

            Multimap<Set<Long>, Long> ndvMap = HashMultimap.create();
            for (List<String> row : rows) {
                String columnGroup = row.get(0);
                String columnNames = row.get(1);
                long ndv = Long.parseLong(row.get(2));

                Set<Long> columnIds = extractColumnIds(columnNames);
                if (columnIds.isEmpty()) {
                    continue;
                }
                ndvMap.put(columnIds, ndv);
                columnIdsList.add(columnIds);
            }

            MultiColumnCombinedStatistics stats = new MultiColumnCombinedStatistics();
            for (Map.Entry<Set<Long>, Collection<Long>> entry : ndvMap.asMap().entrySet()) {
                Set<Long> columnIds = entry.getKey();
                Long ndv = entry.getValue().iterator().next();
                stats.addColumnStatistic(columnIds, ndv);
            }

            // Pre-compute combinations of column statistics
            preComputeStatistics(stats, columnIdsList);

            return stats;
        } catch (Exception e) {
            LOG.error("Load multi-column combined statistics failed. key=[{}]", key, e);
            return new MultiColumnCombinedStatistics();
        }
    }

    private Set<Long> extractColumnIds(String columnNames) {
        try {
            String[] columnIdStrings = columnNames.split(",");
            ImmutableSet.Builder<Long> columnIdsBuilder = ImmutableSet.builder();
            for (String columnIdString : columnIdStrings) {
                columnIdsBuilder.add(Long.parseLong(columnIdString));
            }
            return columnIdsBuilder.build();
        } catch (Exception e) {
            LOG.error("Failed to extract column IDs from [{}]", columnNames, e);
            return ImmutableSet.of();
        }
    }

    private void preComputeStatistics(MultiColumnCombinedStatistics stats, List<Set<Long>> columnIdsList) {
        // Calculate the powerset of column groups
        for (int i = 0; i < columnIdsList.size(); i++) {
            for (int j = i + 1; j < columnIdsList.size(); j++) {
                Set<Long> intersection = Sets.intersection(columnIdsList.get(i), columnIdsList.get(j));
                if (!intersection.isEmpty() && !stats.containsColumnSet(intersection)) {
                    // Estimate the statistics for the intersection
                    Pair<Double, Double> estimatedStats =
                            StatisticsEstimateUtils.estimateIntersectionNDV(
                                    columnIdsList.get(i),
                                    columnIdsList.get(j),
                                    intersection,
                                    stats);
                    if (estimatedStats != null) {
                        stats.addColumnStatistic(intersection, Math.round(estimatedStats.first));
                    }
                }

                Set<Long> union = Sets.union(columnIdsList.get(i), columnIdsList.get(j));
                if (!stats.containsColumnSet(union)) {
                    // Estimate the statistics for the union
                    Pair<Double, Double> estimatedStats =
                            StatisticsEstimateUtils.estimateUnionNDV(
                                    columnIdsList.get(i),
                                    columnIdsList.get(j),
                                    union,
                                    stats);
                    if (estimatedStats != null) {
                        stats.addColumnStatistic(union, Math.round(estimatedStats.first));
                    }
                }
            }
        }
    }
}
