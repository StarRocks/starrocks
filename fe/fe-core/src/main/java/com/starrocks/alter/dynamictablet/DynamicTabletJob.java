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

package com.starrocks.alter.dynamictablet;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.WarehouseIdleChecker;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;

/*
 * DynamicTabletJob, for dynamic tablet splitting and merging.
 * This is the base class of SplitTabletJob and MergeTabletJob
 */
public abstract class DynamicTabletJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(DynamicTabletJob.class);

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

    @SerializedName(value = "dbId")
    protected final long dbId;
    @SerializedName(value = "tableId")
    protected final long tableId;

    @SerializedName(value = "createdTimeMs")
    protected final long createdTimeMs = System.currentTimeMillis();
    @SerializedName(value = "finishedTimeMs")
    protected long finishedTimeMs;

    @SerializedName(value = "errorMessage")
    protected String errorMessage;

    @SerializedName(value = "warehouseId")
    protected final long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
    // no need to persistent
    protected ComputeResource computeResource = WarehouseManager.DEFAULT_RESOURCE;

    public DynamicTabletJob(long jobId, JobType jobType, long dbId, long tableId) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.dbId = dbId;
        this.tableId = tableId;
    }

    public long getJobId() {
        return jobId;
    }

    public JobType getJobType() {
        return jobType;
    }

    public JobState getJobState() {
        return jobState;
    }

    protected void setJobState(JobState jobState) {
        this.jobState = jobState;
        if (jobState.isFinalState()) {
            this.finishedTimeMs = System.currentTimeMillis();
        }

        GlobalStateMgr.getCurrentState().getEditLog().logUpdateDynamicTabletJob(this);
        LOG.info("Dynamic tablet job set job state. {}", this);
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getCreatedTimeMs() {
        return createdTimeMs;
    }

    public long getFinishedTimeMs() {
        return finishedTimeMs;
    }

    public boolean isExpired() {
        return isDone() &&
                (System.currentTimeMillis() - finishedTimeMs) > Config.dynamic_tablet_history_job_keep_max_ms;
    }

    public boolean isDone() {
        return jobState.isFinalState();
    }

    protected boolean abort(String reason) {
        if (!canAbort()) {
            return false;
        }

        errorMessage = reason;
        setJobState(JobState.ABORTING);
        return true;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public long getWarehouseId() {
        return warehouseId;
    }

    protected abstract void runPendingJob();

    protected abstract void runPreparingJob();

    protected abstract void runRunningJob();

    protected abstract void runCleaningJob();

    protected abstract void runAbortingJob();

    protected abstract boolean canAbort();

    public abstract long getParallelTablets();

    public abstract void replay();

    public void run() {
        try {
            getComputeResource();
        } catch (Exception e) {
            LOG.warn("Failed to acquire compute resource for dynamic tablet job, will retry. {}. Exception: ",
                    this, e);
            return;
        }

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
                        onJobDone();
                        break;
                    case ABORTING:
                        runAbortingJob();
                        break;
                    case ABORTED:
                        onJobDone();
                        break;
                    default:
                        LOG.warn("Invalid state in dynamic tablet job, try to abort. {}", this);
                        abort("Invalid state: " + jobState);
                        break;
                }
            } while (jobState != prevState);
        } catch (Exception e) {
            LOG.warn("Failed to run dynamic tablet job, try to abort. {}. Exception: ",
                    this, e);
            abort(e.getMessage());
        }
    }

    private void getComputeResource() {
        CRAcquireContext acquireContext = CRAcquireContext.of(this.warehouseId, this.computeResource);
        this.computeResource = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .acquireComputeResource(acquireContext);
    }

    private void onJobDone() {
        WarehouseIdleChecker.updateJobLastFinishTime(warehouseId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DynamicTabletJob: {");
        sb.append("job_id: ").append(jobId);
        sb.append(", job_type: ").append(jobType);
        sb.append(", job_state: ").append(jobState);
        sb.append(", db_id: ").append(dbId);
        sb.append(", table_id: ").append(tableId);
        sb.append(", created_time: ").append(TimeUtils.longToTimeString(createdTimeMs));
        if (finishedTimeMs > 0) {
            sb.append(", finished_time:").append(TimeUtils.longToTimeString(finishedTimeMs));
        }
        if (errorMessage != null) {
            sb.append(", error_message: ").append(errorMessage);
        }
        sb.append("}");
        return sb.toString();
    }

    public static DynamicTabletJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DynamicTabletJob.class);
    }
}
