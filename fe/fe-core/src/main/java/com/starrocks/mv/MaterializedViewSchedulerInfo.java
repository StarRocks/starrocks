// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MaterializedViewSchedulerInfo implements Writable {

    @SerializedName("id")
    private Long id;

    @SerializedName("startTime")
    private LocalDateTime startTime;

    @SerializedName("period")
    private Long period;

    @SerializedName("timeUnit")
    private TimeUnit timeUnit;

    @SerializedName("jobBuilder")
    private MaterializedViewRefreshJobBuilder jobBuilder;

    private ScheduledFuture<?> future;

    public MaterializedViewSchedulerInfo(LocalDateTime startTime, Long period, TimeUnit timeUnit,
                                         MaterializedViewRefreshJobBuilder jobBuilder) {
        this.startTime = startTime;
        this.period = period;
        this.timeUnit = timeUnit;
        this.jobBuilder = jobBuilder;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public Long getPeriod() {
        return period;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public ScheduledFuture<?> getFuture() {
        return future;
    }

    public void setFuture(ScheduledFuture<?> future) {
        this.future = future;
    }

    public static MaterializedViewSchedulerInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MaterializedViewSchedulerInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
