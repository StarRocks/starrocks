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

public class ChangeMaterializedViewRefreshSchemeLog implements Writable {
    @SerializedName(value = "MaterializedViewId")
    private long id;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "refreshType")
    private RefreshType refreshType;

    @SerializedName(value = "step")
    private long step;

    @SerializedName(value = "timeUnit")
    private TimestampArithmeticExpr.TimeUnit timeUnit;

    public ChangeMaterializedViewRefreshSchemeLog() {
    }

    public ChangeMaterializedViewRefreshSchemeLog(long id, long dbId, RefreshType refreshType, long step,
                                                  TimestampArithmeticExpr.TimeUnit timeUnit) {
        this.id = id;
        this.dbId = dbId;
        this.refreshType = refreshType;
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

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        id = in.readLong();
        refreshType = RefreshType.valueOf(Text.readString(in));
        step = in.readLong();
        timeUnit = TimestampArithmeticExpr.TimeUnit.valueOf(Text.readString(in));
    }
}
