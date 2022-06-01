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
import java.util.concurrent.atomic.LongAdder;

public class AnalyzeMeta implements Writable {
    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

    @SerializedName("type")
    private Constants.AnalyzeType type;

    @SerializedName("updateTime")
    private LocalDateTime updateTime;

    @SerializedName("healthy")
    private double healthy;

    @SerializedName("updateRows")
    private long updateRows;

    private LongAdder updateCounter;

    public AnalyzeMeta(long dbId, long tableId,
                       Constants.AnalyzeType type,
                       LocalDateTime updateTime) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.type = type;
        this.updateTime = updateTime;
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

    public static AnalyzeMeta read(DataInput in) throws IOException {
        String s = Text.readString(in);
        AnalyzeMeta analyzeMeta = GsonUtils.GSON.fromJson(s, AnalyzeMeta.class);
        analyzeMeta.updateCounter = new LongAdder();
        analyzeMeta.updateCounter.add(analyzeMeta.updateRows);
        return analyzeMeta;
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
