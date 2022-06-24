// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
import java.util.concurrent.atomic.LongAdder;

public class BasicStatsMeta implements Writable {
    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

    @SerializedName("type")
    private Constants.AnalyzeType type;

    @SerializedName("updateTime")
    private LocalDateTime updateTime;

    @SerializedName("properties")
    private Map<String, String> properties;

    @SerializedName("healthy")
    private double healthy;

    @SerializedName("updateRows")
    private long updateRows;

    private LongAdder updateCounter;

    public BasicStatsMeta(long dbId, long tableId,
                          Constants.AnalyzeType type,
                          LocalDateTime updateTime,
                          Map<String, String> properties) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.type = type;
        this.updateTime = updateTime;
        this.properties = properties;
        this.healthy = 1;
        this.updateRows = 0;
        this.updateCounter = new LongAdder();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        updateRows = updateCounter.longValue();

        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }

    public static BasicStatsMeta read(DataInput in) throws IOException {
        String s = Text.readString(in);
        BasicStatsMeta basicStatsMeta = GsonUtils.GSON.fromJson(s, BasicStatsMeta.class);
        basicStatsMeta.updateCounter = new LongAdder();
        basicStatsMeta.updateCounter.add(basicStatsMeta.updateRows);
        return basicStatsMeta;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Constants.AnalyzeType getType() {
        return type;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public double getHealthy() {
        return healthy;
    }

    public void setHealthy(double healthy) {
        this.healthy = healthy;
    }

    public long getUpdateRows() {
        return updateRows;
    }

    public void increase(Long delta) {
        updateCounter.add(delta);
    }
}
