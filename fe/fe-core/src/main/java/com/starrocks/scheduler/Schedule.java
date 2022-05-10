// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.scheduler;

import com.google.gson.annotations.SerializedName;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class Schedule {

    @SerializedName("startTime")
    private LocalDateTime startTime;

    @SerializedName("period")
    private Long period;

    @SerializedName("timeUnit")
    private TimeUnit timeUnit;

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
}
