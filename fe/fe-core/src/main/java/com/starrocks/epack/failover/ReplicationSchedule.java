// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

public class ReplicationSchedule {
    @SerializedName(value = "schedule")
    private volatile String schedule;

    @SerializedName(value = "scheduledTimeMs")
    private volatile long scheduledTimeMs;

    @SerializedName(value = "finishedTimeMs")
    private volatile long finishedTimeMs;

    public ReplicationSchedule() {
        this.schedule = null;
        this.scheduledTimeMs = 0;
        this.finishedTimeMs = 0;
    }

    public ReplicationSchedule(String schedule) {
        this.schedule = schedule;
        this.scheduledTimeMs = 0;
        this.finishedTimeMs = 0;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    public boolean isScheduled() {
        return scheduledTimeMs != 0;
    }

    public boolean isFinished() {
        return finishedTimeMs != 0;
    }

    public boolean isPending() {
        return scheduledTimeMs != 0 && finishedTimeMs == 0;
    }

    public boolean needSchedule() {
        if (isScheduled() && !isFinished()) {
            return false;
        }
        // TODO: support period schedule and cron
        return true;
    }

    public void startSchedule() {
        Preconditions.checkState(!isScheduled() || isFinished());
        scheduledTimeMs = System.currentTimeMillis();
        finishedTimeMs = 0;
    }

    public void finishSchedule() {
        Preconditions.checkState(isPending());
        finishedTimeMs = System.currentTimeMillis();
    }

    public void cancelSchedule() {
        Preconditions.checkState(isPending());
        scheduledTimeMs = 0;
    }

    public long getScheduledTimeMs() {
        return scheduledTimeMs;
    }

    public long getFinishedTimeMs() {
        return finishedTimeMs;
    }
}
