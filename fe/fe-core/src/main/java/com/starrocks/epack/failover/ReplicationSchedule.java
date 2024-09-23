// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReplicationSchedule {
    private static final Logger LOG = LogManager.getLogger(FailoverGroup.class);

    @SerializedName(value = "schedule")
    private final String schedule; // Schedule string, such as "1s", "10m", "5h", "1d"

    @SerializedName(value = "scheduledTimeMs")
    private volatile long scheduledTimeMs; // A replication started time

    @SerializedName(value = "finishedTimeMs")
    private volatile long finishedTimeMs; // A replication finished time

    @SerializedName(value = "lastScheduledTimeMs")
    private volatile long lastScheduledTimeMs; // Last replication started time

    @SerializedName(value = "lastFinishedTimeMs")
    private volatile long lastFinishedTimeMs; // Last replication finished time

    @SerializedName(value = "roundScheduledTimeMs")
    private volatile long roundScheduledTimeMs; // A replication may try multiple times, one time called a round

    @SerializedName(value = "roundFinishedTimeMs")
    private volatile long roundFinishedTimeMs; // A replication round finished time

    @SerializedName(value = "roundFinishedTimes")
    private volatile int roundFinishedTimes; // Replication round finished times

    @SerializedName(value = "forceSchedule")
    private volatile boolean forceSchedule; // Whether need to force trigger a new replication

    @SerializedName(value = "suspended")
    private volatile boolean suspended; // Whether to no longer trigger new replications

    public ReplicationSchedule() {
        this.schedule = "0s";
        this.scheduledTimeMs = 0;
        this.finishedTimeMs = 0;
        this.lastScheduledTimeMs = 0;
        this.lastFinishedTimeMs = 0;
        this.roundScheduledTimeMs = 0;
        this.roundFinishedTimeMs = 0;
        this.roundFinishedTimes = 0;
        this.forceSchedule = false;
        this.suspended = false;
    }

    public ReplicationSchedule(String schedule) throws DdlException {
        parseSchedule(schedule);
        this.schedule = schedule;
        this.scheduledTimeMs = 0;
        this.finishedTimeMs = 0;
        this.lastScheduledTimeMs = 0;
        this.lastFinishedTimeMs = 0;
        this.roundScheduledTimeMs = 0;
        this.roundFinishedTimeMs = 0;
        this.roundFinishedTimes = 0;
        this.forceSchedule = false;
        this.suspended = false;
    }

    public ReplicationSchedule(String schedule, ReplicationSchedule other) throws DdlException {
        parseSchedule(schedule);
        this.schedule = schedule;
        this.scheduledTimeMs = other.scheduledTimeMs;
        this.finishedTimeMs = other.finishedTimeMs;
        this.lastScheduledTimeMs = other.lastScheduledTimeMs;
        this.lastFinishedTimeMs = other.lastFinishedTimeMs;
        this.roundScheduledTimeMs = other.roundScheduledTimeMs;
        this.roundFinishedTimeMs = other.roundFinishedTimeMs;
        this.roundFinishedTimes = other.roundFinishedTimes;
        this.forceSchedule = other.forceSchedule;
        this.suspended = other.suspended;
    }

    public String getSchedule() {
        return schedule;
    }

    public long getScheduledTimeMs() {
        return scheduledTimeMs;
    }

    public long getFinishedTimeMs() {
        return finishedTimeMs;
    }

    public long getLastScheduledTimeMs() {
        return lastScheduledTimeMs;
    }

    public long getLastFinishedTimeMs() {
        return lastFinishedTimeMs;
    }

    public int getRoundFinishedTimes() {
        return roundFinishedTimes;
    }

    public void forceSchedule() {
        forceSchedule = true;
    }

    public boolean isSuspended() {
        return suspended;
    }

    public void suspend() {
        suspended = true;
    }

    public void resume() {
        suspended = false;
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

    public boolean isRoundScheduled() {
        return roundScheduledTimeMs != 0;
    }

    public boolean isRoundFinished() {
        return roundFinishedTimeMs != 0;
    }

    public boolean isRoundPending() {
        return roundScheduledTimeMs != 0 && roundFinishedTimeMs == 0;
    }

    public boolean needRetry() {
        return finishedTimeMs == 0 && roundFinishedTimeMs != 0;
    }

    public boolean needSchedule() {
        if (isPending()) {
            if (isRoundPending()) {
                return false;
            }
            return true; // Round finished, but need retry
        }

        if (forceSchedule) {
            LOG.info("Force start schedule replication");
            return true;
        }

        if (suspended) {
            LOG.info("Failover group is suspended");
            return false;
        }

        long nextScheduleTimeMs = getNextScheduleTimeMs();
        return System.currentTimeMillis() >= nextScheduleTimeMs;
    }

    public boolean canSchedule(long minSchedulePeriodMs) {
        if (isPending()) {
            if (isRoundPending()) {
                return false;
            }
            return true; // Round finished, but need retry
        }

        return System.currentTimeMillis() >= scheduledTimeMs + minSchedulePeriodMs;
    }

    public void startSchedule() {
        Preconditions.checkState(!isPending() || !isRoundPending());
        roundScheduledTimeMs = System.currentTimeMillis();
        roundFinishedTimeMs = 0;
        if (!isPending()) {
            lastScheduledTimeMs = scheduledTimeMs;
            lastFinishedTimeMs = finishedTimeMs;
            scheduledTimeMs = roundScheduledTimeMs;
            finishedTimeMs = 0;
            roundFinishedTimes = 0;
        }
    }

    public void finishSchedule(boolean needRetry) {
        Preconditions.checkState(isPending() && isRoundPending());
        roundFinishedTimeMs = System.currentTimeMillis();
        if (!needRetry) {
            finishedTimeMs = roundFinishedTimeMs;
        }
        ++roundFinishedTimes;
        forceSchedule = false;
    }

    public void cancelSchedule() {
        this.scheduledTimeMs = 0;
        this.finishedTimeMs = 0;
        this.lastScheduledTimeMs = 0;
        this.lastFinishedTimeMs = 0;
        this.roundScheduledTimeMs = 0;
        this.roundFinishedTimeMs = 0;
        this.roundFinishedTimes = 0;
        this.forceSchedule = false;
    }

    // TODO: Support cron expression
    private long getNextScheduleTimeMs() {
        try {
            long periodMs = parseSchedule(schedule);
            return scheduledTimeMs + periodMs;
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    private long parseSchedule(String schedule) throws DdlException {
        schedule = schedule.trim().toLowerCase();
        try {
            if (schedule.endsWith("seconds")) {
                String periodString = schedule.substring(0, schedule.length() - 7);
                return (long) (Double.parseDouble(periodString) * 1000);
            }
            if (schedule.endsWith("minutes")) {
                String periodString = schedule.substring(0, schedule.length() - 7);
                return (long) (Double.parseDouble(periodString) * 60 * 1000);
            }
            if (schedule.endsWith("hours")) {
                String periodString = schedule.substring(0, schedule.length() - 5);
                return (long) (Double.parseDouble(periodString) * 60 * 60 * 1000);
            }
            if (schedule.endsWith("days")) {
                String periodString = schedule.substring(0, schedule.length() - 4);
                return (long) (Double.parseDouble(periodString) * 24 * 60 * 60 * 1000);
            }

            if (schedule.endsWith("s")) {
                String periodString = schedule.substring(0, schedule.length() - 1);
                return (long) (Double.parseDouble(periodString) * 1000);
            }
            if (schedule.endsWith("m")) {
                String periodString = schedule.substring(0, schedule.length() - 1);
                return (long) (Double.parseDouble(periodString) * 60 * 1000);
            }
            if (schedule.endsWith("h")) {
                String periodString = schedule.substring(0, schedule.length() - 1);
                return (long) (Double.parseDouble(periodString) * 60 * 60 * 1000);
            }
            if (schedule.endsWith("d")) {
                String periodString = schedule.substring(0, schedule.length() - 1);
                return (long) (Double.parseDouble(periodString) * 24 * 60 * 60 * 1000);
            }

        } catch (Exception e) {
            ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, schedule);
        }

        ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_PARAMETER, schedule);
        return 0;
    }
}
