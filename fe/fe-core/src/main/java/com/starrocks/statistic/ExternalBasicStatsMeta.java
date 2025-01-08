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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.commons.collections4.MapUtils;

import java.io.DataInput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
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



    public static ExternalBasicStatsMeta read(DataInput in) throws IOException {
        String s = Text.readString(in);
        return GsonUtils.GSON.fromJson(s, ExternalBasicStatsMeta.class);
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
                .map(ColumnStatsMeta::simpleString).collect(Collectors.joining(","));
    }

    public ExternalBasicStatsMeta clone() {
        String json = GsonUtils.GSON.toJson(this);
        return GsonUtils.GSON.fromJson(json, ExternalBasicStatsMeta.class);
    }
}
