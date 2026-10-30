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

import com.google.gson.annotations.SerializedName;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Meta of column-level statistics
 */
public class ColumnStatsMeta {

    @SerializedName("columnName")
    private String columnName;

    @SerializedName("type")
    private StatsConstants.AnalyzeType type;

    @SerializedName("updateTime")
    private LocalDateTime updateTime;

    @SerializedName("sampledPartitions")
    private Set<Long> sampledPartitionsHashValue;

    @SerializedName("allPartitionSize")
    private int allPartitionSize;

    // How many partitions the collection that wrote this entry set out to read, against which
    // sampledPartitions is the number it managed to read. The two differ when a collection was cut short -
    // some queries were tolerated away, or the job failed outright after collecting part of the table - and
    // the gap is the only durable record that the column is not as covered as it was meant to be. Without
    // it a partial collection looks exactly like a complete one to the scheduler, which then skips the
    // column for as long as the table itself does not change (see
    // StatisticsCollectJobFactory#needCollectStatsColumns): on a table that is never written again, "never
    // again" is how long the missing partitions stay missing.
    //
    // 0 on entries written before this was recorded, and on entries that carry no partition set at all
    // (AnalyzeType.FULL): both mean "nothing here says the coverage is short", so they are left alone.
    // Deliberately a count and not, say, a format-specific version marker - every connector has partitions,
    // and nothing else about them needs to be understood to tell a partial collection from a whole one.
    @SerializedName("requestedPartitionCount")
    private int requestedPartitionCount;

    public ColumnStatsMeta(String columnName, StatsConstants.AnalyzeType type, LocalDateTime updateTime) {
        this(columnName, type, updateTime, new HashSet<>(), -1);
    }

    public ColumnStatsMeta(String columnName, StatsConstants.AnalyzeType type, LocalDateTime updateTime,
                           Set<Long> sampledPartitionsHashValue, int allPartitionSize) {
        this(columnName, type, updateTime, sampledPartitionsHashValue, allPartitionSize, 0);
    }

    public ColumnStatsMeta(String columnName, StatsConstants.AnalyzeType type, LocalDateTime updateTime,
                           Set<Long> sampledPartitionsHashValue, int allPartitionSize,
                           int requestedPartitionCount) {
        this.columnName = columnName;
        this.type = type;
        this.updateTime = updateTime;
        this.sampledPartitionsHashValue = sampledPartitionsHashValue;
        this.allPartitionSize = allPartitionSize;
        this.requestedPartitionCount = requestedPartitionCount;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public StatsConstants.AnalyzeType getType() {
        return type;
    }

    public void setType(StatsConstants.AnalyzeType type) {
        this.type = type;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }

    public Set<Long> getSampledPartitionsHashValue() {
        return sampledPartitionsHashValue;
    }

    public int getAllPartitionSize() {
        return allPartitionSize;
    }

    public int getRequestedPartitionCount() {
        return requestedPartitionCount;
    }

    /**
     * Whether the collection that wrote this entry left partitions of its own request uncollected, so the
     * column should be collected again even if nothing about the table has changed since.
     *
     * <p>FULL entries are complete by definition - they describe the whole table and carry no partition
     * set - and entries written before requestedPartitionCount existed say nothing either way, so both
     * answer false and keep their previous scheduling behaviour.
     */
    public boolean isCoverageIncomplete() {
        return type != StatsConstants.AnalyzeType.FULL
                && requestedPartitionCount > 0
                && sampledPartitionsHashValue != null
                && sampledPartitionsHashValue.size() < requestedPartitionCount;
    }

    public String simpleString(boolean isExternalTable) {
        if (isExternalTable && type == StatsConstants.AnalyzeType.SAMPLE && sampledPartitionsHashValue != null) {
            // The requested count is only worth showing when it says the collection fell short of it -
            // that is the one case an operator has to act on, or at least understand.
            String incomplete = isCoverageIncomplete()
                    ? String.format(",incomplete_of=%d", requestedPartitionCount) : "";
            return String.format("(%s,%s,sampled_partition_size=%d,all_partition_size=%d%s)", columnName, type,
                    sampledPartitionsHashValue.size(), allPartitionSize, incomplete);
        } else {
            return String.format("(%s,%s)", columnName, type.toString());
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ColumnStatsMeta{");
        sb.append("columnName='").append(columnName).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnStatsMeta that = (ColumnStatsMeta) o;
        return Objects.equals(columnName, that.columnName) && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, type);
    }
}
