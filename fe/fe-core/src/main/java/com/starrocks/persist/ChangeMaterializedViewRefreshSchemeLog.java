// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.RefreshType;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;

public class ChangeMaterializedViewRefreshSchemeLog implements Writable {
    @SerializedName(value = "MaterializedViewId")
    private long id;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "refreshType")
    private RefreshType refreshType;

    @SerializedName(value = "startTime")
    private LocalDateTime startTime;

    @SerializedName(value = "step")
    private long step;

    @SerializedName(value = "timeUnit")
    private TimestampArithmeticExpr.TimeUnit timeUnit;

    public ChangeMaterializedViewRefreshSchemeLog() {
    }

    public ChangeMaterializedViewRefreshSchemeLog(long id, long dbId, RefreshType refreshType, LocalDateTime startTime,
                                                  long step, TimestampArithmeticExpr.TimeUnit timeUnit) {
        this.id = id;
        this.dbId = dbId;
        this.refreshType = refreshType;
        this.startTime = startTime;
        this.step = step;
        this.timeUnit = timeUnit;
    }

    public long getId() {
        return id;
    }

    public long getDbId() {
        return dbId;
    }

    public RefreshType getRefreshType() {
        return refreshType;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public long getStep() {
        return step;
    }

    public TimestampArithmeticExpr.TimeUnit getTimeUnit() {
        return timeUnit;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ChangeMaterializedViewRefreshSchemeLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ChangeMaterializedViewRefreshSchemeLog.class);
    }
}
