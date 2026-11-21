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

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.image.ProtoDateTimes;
import com.starrocks.persist.image.ProtoEnums;
import com.starrocks.persist.image.ProtoLists;
import com.starrocks.persist.image.ProtoMaps;
import com.starrocks.persist.proto.AnalyzeTypePB;
import com.starrocks.persist.proto.ColumnStatsMetaMapPB;
import com.starrocks.persist.proto.ExternalBasicStatsMetaPB;
import org.apache.commons.collections4.MapUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ExternalBasicStatsMeta implements Writable {
    @SerializedName("catalogName")
    private String catalogName;
    @SerializedName("dbName")
    private String dbName;

    @SerializedName("tableName")
    private String tableName;

    // Deprecated by columnStatsMetaMap
    @Deprecated
    @SerializedName("columns")
    private List<String> columns;

    @SerializedName("type")
    private StatsConstants.AnalyzeType type;

    @SerializedName("updateTime")
    private LocalDateTime updateTime;

    @SerializedName("properties")
    private Map<String, String> properties;

    @SerializedName("columnStats")
    private Map<String, ColumnStatsMeta> columnStatsMetaMap = Maps.newConcurrentMap();

    // The table UUID resolved on the leader at analyze time. It is the key used by the connector
    // statistics cache, so persisting it lets followers invalidate the cache during journal replay
    // without resolving external table metadata (which may block on HMS/object storage).
    // May be null for journals written before this field was introduced (see replay fallback).
    @SerializedName("tableUUID")
    private String tableUUID;

    public ExternalBasicStatsMeta() {}

    public ExternalBasicStatsMeta(String catalogName, String dbName, String tableName, List<String> columns,
                                  StatsConstants.AnalyzeType type,
                                  LocalDateTime updateTime,
                                  Map<String, String> properties) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.columns = columns;
        this.type = type;
        this.updateTime = updateTime;
        this.properties = properties;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableUUID() {
        return tableUUID;
    }

    public void setTableUUID(String tableUUID) {
        this.tableUUID = tableUUID;
    }

    public List<String> getColumns() {
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

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }

    public void setAnalyzeType(StatsConstants.AnalyzeType analyzeType) {
        this.type = analyzeType;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public void addColumnStatsMeta(ColumnStatsMeta columnStatsMeta) {
        this.columnStatsMetaMap.put(columnStatsMeta.getColumnName(), columnStatsMeta);
    }

    public ColumnStatsMeta getColumnStatsMeta(String columnName) {
        return columnStatsMetaMap.get(columnName);
    }

    public Map<String, ColumnStatsMeta> getColumnStatsMetaMap() {
        return columnStatsMetaMap;
    }

    public String getColumnStatsString() {
        if (MapUtils.isEmpty(columnStatsMetaMap)) {
            return "";
        }

        return columnStatsMetaMap.values().stream()
                .map(c -> c.simpleString(true))
                .collect(Collectors.joining(","));
    }

    public ExternalBasicStatsMeta clone() {
        String json = GsonUtils.GSON.toJson(this);
        return GsonUtils.GSON.fromJson(json, ExternalBasicStatsMeta.class);
    }

    public ExternalBasicStatsMetaPB toProto() {
        ExternalBasicStatsMetaPB.Builder builder = ExternalBasicStatsMetaPB.newBuilder();
        if (catalogName != null) {
            builder.setCatalogName(catalogName);
        }
        if (dbName != null) {
            builder.setDbName(dbName);
        }
        if (tableName != null) {
            builder.setTableName(tableName);
        }
        if (columns != null) {
            builder.setColumns(ProtoLists.stringList(columns));
        }
        if (type != null) {
            builder.setType(ProtoEnums.toProto(type, AnalyzeTypePB.class, "ANALYZE_"));
        }
        if (updateTime != null) {
            builder.setUpdateTime(ProtoDateTimes.toEpochSecond(updateTime));
        }
        if (properties != null) {
            builder.setProperties(ProtoMaps.toStringMapProto(properties));
        }
        if (columnStatsMetaMap != null) {
            ColumnStatsMetaMapPB.Builder columns = ColumnStatsMetaMapPB.newBuilder();
            ProtoMaps.withoutNullValues(columnStatsMetaMap).forEach((key, value) ->
                    columns.putValues(key, value.toProto()));
            builder.setColumnStatsMetaMap(columns);
        }
        if (tableUUID != null) {
            builder.setTableUuid(tableUUID);
        }
        return builder.build();
    }

    // A protobuf constructor leaves the existing GSON construction path unchanged.
    private ExternalBasicStatsMeta(ExternalBasicStatsMetaPB pb) {
        if (pb.hasCatalogName()) {
            catalogName = pb.getCatalogName();
        }
        if (pb.hasDbName()) {
            dbName = pb.getDbName();
        }
        if (pb.hasTableName()) {
            tableName = pb.getTableName();
        }
        if (pb.hasColumns()) {
            columns = ProtoLists.toStrings(pb.getColumns());
        }
        if (pb.hasType()) {
            type = ProtoEnums.fromProto(pb.getType(), StatsConstants.AnalyzeType.class, "ANALYZE_");
        }
        if (pb.hasUpdateTime()) {
            updateTime = ProtoDateTimes.fromEpochSecond(pb.getUpdateTime());
        }
        if (pb.hasProperties()) {
            properties = ProtoMaps.fromStringMapProto(pb.getProperties());
        }
        if (pb.hasColumnStatsMetaMap()) {
            columnStatsMetaMap = new ConcurrentHashMap<>();
            pb.getColumnStatsMetaMap().getValuesMap().forEach((key, value) ->
                    columnStatsMetaMap.put(key, ColumnStatsMeta.fromProto(value)));
        }
        if (pb.hasTableUuid()) {
            tableUUID = pb.getTableUuid();
        }
    }

    public static ExternalBasicStatsMeta fromProto(ExternalBasicStatsMetaPB pb) {
        return new ExternalBasicStatsMeta(pb);
    }
}
