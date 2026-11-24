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
import com.starrocks.common.io.Writable;
import com.starrocks.persist.image.ProtoDateTimes;
import com.starrocks.persist.image.ProtoEnums;
import com.starrocks.persist.image.ProtoLists;
import com.starrocks.persist.image.ProtoMaps;
import com.starrocks.persist.proto.AnalyzeTypePB;
import com.starrocks.persist.proto.MultiColumnStatsMetaPB;
import com.starrocks.persist.proto.StatisticsTypeListPB;
import com.starrocks.sql.ast.StatisticsType;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MultiColumnStatsMeta implements Writable {
    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

    @SerializedName("columnIds")
    private Set<Integer> columnIds;

    @SerializedName("analyzeType")
    private StatsConstants.AnalyzeType analyzeType;

    @SerializedName("statisticsTypes")
    private List<StatisticsType> statisticsTypes;

    @SerializedName("updateTime")
    private LocalDateTime updateTime;

    @SerializedName("properties")
    private Map<String, String> properties;

    public MultiColumnStatsMeta(long dbId, long tableId, Set<Integer> columnIds,
                                StatsConstants.AnalyzeType analyzeType, List<StatisticsType> statisticsTypes,
                                LocalDateTime updateTime, Map<String, String> properties) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.columnIds = columnIds;
        this.analyzeType = analyzeType;
        this.statisticsTypes = statisticsTypes;
        this.updateTime = updateTime;
        this.properties = properties;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Set<Integer> getColumnIds() {
        return columnIds;
    }

    public StatsConstants.AnalyzeType getAnalyzeType() {
        return analyzeType;
    }

    public List<StatisticsType> getStatsTypes() {
        return statisticsTypes;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public MultiColumnStatsMetaPB toProto() {
        MultiColumnStatsMetaPB.Builder builder = MultiColumnStatsMetaPB.newBuilder();
        builder.setDbId(dbId);
        builder.setTableId(tableId);
        if (columnIds != null) {
            builder.setColumnIds(ProtoLists.int32List(columnIds));
        }
        if (analyzeType != null) {
            builder.setAnalyzeType(ProtoEnums.toProto(analyzeType, AnalyzeTypePB.class, "ANALYZE_"));
        }
        if (statisticsTypes != null) {
            builder.setStatisticsTypes(StatisticsTypeListPB.newBuilder().addAllValues(statisticsTypes.stream()
                    .map(value -> ProtoEnums.toProto(value, StatisticsTypeListPB.Type.class))
                    .collect(Collectors.toList())).build());
        }
        if (updateTime != null) {
            builder.setUpdateTime(ProtoDateTimes.toEpochSecond(updateTime));
        }
        if (properties != null) {
            builder.setProperties(ProtoMaps.toStringMapProto(properties));
        }
        return builder.build();
    }

    // A protobuf constructor leaves the existing GSON construction path unchanged.
    private MultiColumnStatsMeta(MultiColumnStatsMetaPB pb) {
        if (pb.hasDbId()) {
            dbId = pb.getDbId();
        }
        if (pb.hasTableId()) {
            tableId = pb.getTableId();
        }
        if (pb.hasColumnIds()) {
            columnIds = new HashSet<>(ProtoLists.toIntegers(pb.getColumnIds()));
        }
        if (pb.hasAnalyzeType()) {
            analyzeType = ProtoEnums.fromProto(pb.getAnalyzeType(), StatsConstants.AnalyzeType.class, "ANALYZE_");
        }
        if (pb.hasStatisticsTypes()) {
            statisticsTypes = pb.getStatisticsTypes().getValuesList().stream()
                    .map(value -> ProtoEnums.fromProto(value, StatisticsType.class)).collect(Collectors.toList());
        }
        if (pb.hasUpdateTime()) {
            updateTime = ProtoDateTimes.fromEpochSecond(pb.getUpdateTime());
        }
        if (pb.hasProperties()) {
            properties = ProtoMaps.fromStringMapProto(pb.getProperties());
        }
    }

    public static MultiColumnStatsMeta fromProto(MultiColumnStatsMetaPB pb) {
        return new MultiColumnStatsMeta(pb);
    }
}
