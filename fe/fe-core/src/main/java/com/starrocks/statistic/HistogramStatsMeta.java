// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.statistic;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

public class HistogramStatsMeta implements Writable {
    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

    @SerializedName("column")
    private String column;

    @SerializedName("type")
    private StatsConstants.AnalyzeType type;

    @SerializedName("updateTime")
    private LocalDateTime updateTime;

    @SerializedName("properties")
    private Map<String, String> properties;

    public HistogramStatsMeta(long dbId, long tableId, String column,
                              StatsConstants.AnalyzeType type,
                              LocalDateTime updateTime,
                              Map<String, String> properties) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.column = column;
        this.type = type;
        this.updateTime = updateTime;
        this.properties = properties;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }

    public static HistogramStatsMeta read(DataInput in) throws IOException {
        String s = Text.readString(in);
        HistogramStatsMeta histogramStatsMeta = GsonUtils.GSON.fromJson(s, HistogramStatsMeta.class);
        return histogramStatsMeta;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getColumn() {
        return column;
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
}
