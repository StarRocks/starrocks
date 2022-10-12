// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.sql.optimizer.Utils;

import java.util.concurrent.TimeUnit;

public class TaskSchedule {

    // Measured in milliseconds, between the start time and midnight, January 1, 1970 UTC.
    @SerializedName("startTime")
    private long startTime;

    @SerializedName("period")
    private long period;

    @SerializedName("timeUnit")
    private TimeUnit timeUnit;

    public TaskSchedule(long startTime, long period, TimeUnit timeUnit) {
        this.startTime = startTime;
        this.period = period;
        this.timeUnit = timeUnit;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public String toString() {
        return " (START " + Utils.getDatetimeFromLong(startTime)
                + " EVERY(" + period + " " + timeUnit + "))";
    }
}
