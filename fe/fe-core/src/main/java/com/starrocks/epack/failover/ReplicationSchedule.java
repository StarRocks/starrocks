// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.time.ZonedDateTime;

public class ReplicationSchedule {
    @SerializedName(value = "schedule")
    private String schedule;

    @SerializedName(value = "scheduledTime")
    private ZonedDateTime scheduledTime;

    @SerializedName(value = "finishedTime")
    private ZonedDateTime finishedTime;

    public ReplicationSchedule() {
        this.schedule = null;
        this.scheduledTime = null;
        this.finishedTime = null;
    }

    public ReplicationSchedule(String schedule) {
        this.schedule = schedule;
        this.scheduledTime = null;
        this.finishedTime = null;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    public boolean hasScheduled() {
        return scheduledTime != null;
    }

    public boolean isFinished() {
        return finishedTime != null;
    }

    public boolean needSchedule() {
        if (hasScheduled() && !isFinished()) {
            return false;
        }
        // TODO: support period schedule and cron
        return true;
    }

    public void startSchedule() {
        Preconditions.checkState(!hasScheduled() || isFinished());
        scheduledTime = ZonedDateTime.now();
        finishedTime = null;
    }

    public void finishSchedule() {
        Preconditions.checkState(hasScheduled() || !isFinished());
        finishedTime = ZonedDateTime.now();
    }

    public ZonedDateTime getScheduledTime() {
        return scheduledTime;
    }

    public ZonedDateTime getFinishedTime() {
        return finishedTime;
    }
}
