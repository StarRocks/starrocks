// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.google.gson.annotations.SerializedName;

import java.time.LocalDateTime;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MaterializedViewSchedulerInfo {

    @SerializedName("id")
    private Long id;

    @SerializedName("startTime")
    private LocalDateTime startTime;

    @SerializedName("period")
    private Long period;

    @SerializedName("timeUnit")
    private TimeUnit timeUnit;

    private ScheduledFuture<?> future;

    public MaterializedViewSchedulerInfo(LocalDateTime startTime, Long period, TimeUnit timeUnit) {
        this.startTime = startTime;
        this.period = period;
        this.timeUnit = timeUnit;
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
}
