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
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;

import java.io.DataInput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.catalog.ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX;

public class BasicStatsMeta implements Writable {
    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

    // Deprecated by columnStatsMetaMap
    // But for backward compatibility, we still need to write into this field, to make sure the behavior is still
    // correct after rollback
    @Deprecated
    @SerializedName("columns")
    private List<String> columns;

    @SerializedName("type")
    private StatsConstants.AnalyzeType type;

    @SerializedName("updateTime")
    private LocalDateTime updateTime;

    @SerializedName("properties")
    private Map<String, String> properties;

    // The old semantics indicated the increment of ingestion tasks after last statistical collect job.
    // Since manually collecting sampled job would reset it to zero, affecting the incremental information,
    // it is now changed to record the total number of rows in the table.
    // Every time data is imported, it will be appended.
    // Every time tablet stats is synchronized, it is set to the total value of the latest snapshot.
    @SerializedName("updateRows")
    private long totalRows;

    // Every time data is imported, it will be appended.
    // Every time tablet stats is synchronized, it will be reset to 0.
    @SerializedName("deltaRows")
    private long deltaRows;

    // TODO: use ColumnId
    @SerializedName("columnStats")
    private Map<String, ColumnStatsMeta> columnStatsMetaMap = Maps.newConcurrentMap();

    // Used for deserialization
    public BasicStatsMeta() {
        columnStatsMetaMap = Maps.newConcurrentMap();
    }

    public BasicStatsMeta(long dbId, long tableId, List<String> columns,
                          StatsConstants.AnalyzeType type,
                          LocalDateTime updateTime,
                          Map<String, String> properties) {
        this(dbId, tableId, columns, type, updateTime, properties, 0);
    }

    public BasicStatsMeta(long dbId, long tableId, List<String> columns,
                          StatsConstants.AnalyzeType type,
                          LocalDateTime updateTime,
                          Map<String, String> properties,
                          long totalRows) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.columns = columns;
        this.type = type;
        this.updateTime = updateTime;
        this.properties = properties;
        this.totalRows = totalRows;
    }

    public static BasicStatsMeta read(DataInput in) throws IOException {
        String s = Text.readString(in);
        return GsonUtils.GSON.fromJson(s, BasicStatsMeta.class);
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public List<String> getColumns() {
        if (MapUtils.isNotEmpty(columnStatsMetaMap)) {
            return Lists.newArrayList(columnStatsMetaMap.keySet());
        }
        // Just for compatibility, there are no columns in the old code,
        // and the columns may be null after deserialization.
        if (columns == null) {
            return Collections.emptyList();
        }
        return columns;
    }

    public StatsConstants.AnalyzeType getType() {
        return type;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Return a number within [0,1] to indicate the health of table stats, 1 means all good.
     */
    public double getHealthy() {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), tableId);

        long tableRowCount = 1L;
        long cachedTableRowCount = 1L;
        long updatePartitionRowCount = 0L;
        long updatePartitionCount = 0L;

        Map<Long, Optional<Long>> tableStatistics = GlobalStateMgr.getCurrentState().getStatisticStorage()
                .getTableStatistics(table.getId(), table.getPartitions());

        Collection<Partition> allPartitions = table.getPartitions().stream()
                .filter(p -> !(p.getName().startsWith(SHADOW_PARTITION_PREFIX)))
                .collect(Collectors.toSet());
        long totalPartitionCount = allPartitions.size();
        for (Partition partition : allPartitions) {
            tableRowCount += partition.getRowCount();
            Optional<Long> statistic = tableStatistics.getOrDefault(partition.getId(), Optional.empty());
            cachedTableRowCount += statistic.orElse(0L);

            if (!StatisticUtils.isPartitionStatsHealthy(partition, this, statistic.orElse(0L))) {
                updatePartitionCount++;
            }
        }
        updatePartitionRowCount = Math.max(1, Math.max(tableRowCount + deltaRows, totalRows) - cachedTableRowCount);

        double updateRatio;
        // 1. If none updated partitions, health is 1
        // 2. If there are few updated partitions, the health only to calculated on rows
        // 3. If there are many updated partitions, the health needs to be calculated based on partitions
        if (updatePartitionCount == 0) {
            return 1;
        } else if (updatePartitionCount < StatsConstants.STATISTICS_PARTITION_UPDATED_THRESHOLD) {
            updateRatio = (updatePartitionRowCount * 1.0) / tableRowCount;
        } else {
            double rowUpdateRatio = (updatePartitionRowCount * 1.0) / tableRowCount;
            double partitionUpdateRatio = (updatePartitionCount * 1.0) / totalPartitionCount;
            updateRatio = Math.min(rowUpdateRatio, partitionUpdateRatio);
        }
        return 1 - Math.min(updateRatio, 1.0);
    }

    public long getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(Long totalRows) {
        this.totalRows = totalRows;
    }

    public void increaseDeltaRows(Long delta) {
        totalRows += delta;
        deltaRows += delta;
    }

    public boolean isInitJobMeta() {
        return MapUtils.isNotEmpty(properties) && properties.containsKey(StatsConstants.INIT_SAMPLE_STATS_JOB);
    }

    public boolean isUpdatedAfterLoad(LocalDateTime loadTime) {
        if (isInitJobMeta()) {
            // We update the updateTime of a partition then we may do an init sample collect job, these auto init
            // sample may return a wrong healthy value which may block the auto full collect job.
            // so we return false to regard it like a manual collect job before load.
            return false;
        } else {
            return updateTime.isAfter(loadTime);
        }
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }

    public void setAnalyzeType(StatsConstants.AnalyzeType analyzeType) {
        this.type = analyzeType;
    }

    public Map<String, ColumnStatsMeta> getAnalyzedColumns() {
        Map<String, ColumnStatsMeta> deduplicate = Maps.newHashMap();
        // TODO: just for compatible, we can remove it at next version
        for (String column : ListUtils.emptyIfNull(columns)) {
            deduplicate.put(column, new ColumnStatsMeta(column, type, updateTime));
        }
        deduplicate.putAll(columnStatsMetaMap);
        return deduplicate;
    }

    public String getColumnStatsString() {
        if (MapUtils.isEmpty(columnStatsMetaMap)) {
            return "";
        }
        return columnStatsMetaMap.values().stream()
                .map(ColumnStatsMeta::simpleString).collect(Collectors.joining(","));
    }

    public void addColumnStatsMeta(ColumnStatsMeta columnStatsMeta) {
        this.columnStatsMetaMap.put(columnStatsMeta.getColumnName(), columnStatsMeta);
    }

    public void resetDeltaRows() {
        this.deltaRows = 0;
    }

    public BasicStatsMeta clone() {
        String json = GsonUtils.GSON.toJson(this);
        return GsonUtils.GSON.fromJson(json, BasicStatsMeta.class);
    }
}
