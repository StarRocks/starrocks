// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.alter.reshard;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Writable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TTabletReshardJobsItem;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/*
 * TabletReshardJob is for tablet splitting and merging.
 */
public abstract class TabletReshardJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(TabletReshardJob.class);

    public enum JobState {
        PENDING, // Job is created
        PREPARING, // Creating new tablets
        RUNNING, // Do tablet splitting or merging
        CLEANING, // Clean old tablets
        FINISHED, // Job is finished
        ABORTING, // Job is aborting
        ABORTED; // Job is aborted

        public boolean isFinalState() {
            return this == JobState.FINISHED || this == JobState.ABORTED;
        }
    }

    public enum JobType {
        SPLIT_TABLET,
        MERGE_TABLET
    }

    @SerializedName(value = "jobId")
    protected final long jobId;

    @SerializedName(value = "jobType")
    protected final JobType jobType;

    @SerializedName(value = "jobState")
    protected volatile JobState jobState = JobState.PENDING;

    @SerializedName(value = "createdTimeMs")
    protected final long createdTimeMs = System.currentTimeMillis();
    @SerializedName(value = "finishedTimeMs")
    protected long finishedTimeMs;
    @SerializedName(value = "stateStartedTimeMs")
    protected long stateStartedTimeMs = createdTimeMs;

    @SerializedName(value = "errorMessage")
    protected String errorMessage;

    // The warehouse this job should run its compute work (shard creation + publish) in. Set by the
    // pre-split caller to the triggering load's warehouse; null for an online split / merge (and for a
    // job journaled before this field existed), which then fall back to the background warehouse.
    // Nullable so a missing field on replay deserializes to null (background), not 0 (a real warehouse).
    // Persisted so a leader-switch re-run targets the same warehouse.
    @SerializedName(value = "warehouseId")
    protected Long warehouseId;

    public TabletReshardJob(long jobId, JobType jobType) {
        this.jobId = jobId;
        this.jobType = jobType;
    }

    public long getJobId() {
        return jobId;
    }

    public JobType getJobType() {
        return jobType;
    }

    public Long getWarehouseId() {
        return warehouseId;
    }

    /**
     * Set the warehouse this job runs its compute work in. Called by the pre-split caller (before the
     * job is journaled) with the triggering load's warehouse, so shard creation and publish run there
     * rather than the background warehouse.
     */
    public void setWarehouseId(long warehouseId) {
        this.warehouseId = warehouseId;
    }

    /**
     * Resolve the compute resource for this job's compute work: the explicitly-set warehouse when one
     * was provided (pre-split → the load's warehouse), otherwise the background warehouse (online
     * split / merge, or a job journaled before warehouseId existed).
     */
    protected ComputeResource resolveComputeResource(long tableId) {
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        return warehouseId == null
                ? warehouseMgr.getBackgroundComputeResource(tableId)
                : warehouseMgr.acquireComputeResource(warehouseId);
    }

    public JobState getJobState() {
        return jobState;
    }

    protected void setJobState(JobState jobState) {
        long currentTimeMs = System.currentTimeMillis();

        if (jobState.isFinalState()) {
            this.finishedTimeMs = currentTimeMs;
        }

        this.jobState = jobState;

        this.stateStartedTimeMs = currentTimeMs;

        GlobalStateMgr.getCurrentState().getEditLog().logUpdateTabletReshardJob(this);

        LOG.info("Tablet reshard job set job state. {}", this);
    }

    public long getCreatedTimeMs() {
        return createdTimeMs;
    }

    public long getFinishedTimeMs() {
        return finishedTimeMs;
    }

    public boolean isExpired() {
        return isDone() &&
                (System.currentTimeMillis() - finishedTimeMs) > Config.tablet_reshard_history_job_keep_max_ms;
    }

    public boolean isDone() {
        return jobState.isFinalState();
    }

    public boolean isAborted() {
        return jobState == JobState.ABORTED;
    }

    protected boolean abort(String reason) {
        if (!canAbort()) {
            LOG.warn("Tablet reshard job cannot abort. {}", this);
            return false;
        }

        errorMessage = reason;
        setJobState(JobState.ABORTING);
        return true;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void run() {
        try {
            JobState prevState = null;
            do {
                prevState = jobState;
                switch (prevState) {
                    case PENDING:
                        runPendingJob();
                        break;
                    case PREPARING:
                        runPreparingJob();
                        break;
                    case RUNNING:
                        runRunningJob();
                        break;
                    case CLEANING:
                        runCleaningJob();
                        break;
                    case FINISHED:
                        runFinishedJob();
                        break;
                    case ABORTING:
                        runAbortingJob();
                        break;
                    case ABORTED:
                        runAbortedJob();
                        break;
                    default:
                        LOG.warn("Invalid state in tablet reshard job, try to abort. {}", this);
                        abort("Invalid state: " + jobState);
                        break;
                }
            } while (jobState != prevState);
        } catch (Exception e) {
            LOG.warn("Failed to run tablet reshard job, try to abort. {}. Exception: ",
                    this, e);
            abort(e.getMessage());
        }
    }

    public void replay() {
        try {
            switch (jobState) {
                case PENDING:
                    replayPendingJob();
                    break;
                case PREPARING:
                    replayPreparingJob();
                    break;
                case RUNNING:
                    replayRunningJob();
                    break;
                case CLEANING:
                    replayCleaningJob();
                    break;
                case FINISHED:
                    replayFinishedJob();
                    break;
                case ABORTING:
                    replayAbortingJob();
                    break;
                case ABORTED:
                    replayAbortedJob();
                    break;
                default:
                    LOG.warn("Invalid state in tablet reshard job. {}", this);
                    break;
            }
        } catch (Exception e) {
            LOG.warn("Caught exception when replay tablet reshard job. {}. ", this, e);
        }
    }

    public abstract long getParallelTablets();

    public abstract long getTableId();

    /*
     * Admission-time reservation. Reserve the table for this job before it is queued in
     * TabletReshardJobMgr. Must succeed before the job becomes visible to the scheduler, so that
     * an admitted job is guaranteed runnable and never forced to abort at execution time due to an
     * unexpected table state. Throws if the table is not reservable (not NORMAL / dropped).
     */
    public abstract void init() throws StarRocksException;

    protected abstract void runPendingJob();

    protected abstract void runPreparingJob();

    protected abstract void runRunningJob();

    protected abstract void runCleaningJob();

    protected abstract void runFinishedJob();

    protected abstract void runAbortingJob();

    protected abstract void runAbortedJob();

    protected abstract boolean canAbort();

    protected abstract void replayPendingJob();

    protected abstract void replayPreparingJob();

    protected abstract void replayRunningJob();

    protected abstract void replayCleaningJob();

    protected abstract void replayFinishedJob();

    protected abstract void replayAbortingJob();

    protected abstract void replayAbortedJob();

    protected abstract void registerReshardingTabletsOnRestart();

    public abstract TTabletReshardJobsItem getInfo();
}
