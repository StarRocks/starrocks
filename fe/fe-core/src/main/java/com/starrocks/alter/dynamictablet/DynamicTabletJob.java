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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TDynamicTabletJobsItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

/*
 * DynamicTabletJob is for dynamic tablet splitting and merging.
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

    // Physical partition id -> PhysicalPartitionContext
    @SerializedName(value = "physicalPartitionContexts")
    protected final Map<Long, PhysicalPartitionContext> physicalPartitionContexts;

    public DynamicTabletJob(long jobId, JobType jobType, long dbId, long tableId,
            Map<Long, PhysicalPartitionContext> physicalPartitionContexts) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.dbId = dbId;
        this.tableId = tableId;
        this.physicalPartitionContexts = physicalPartitionContexts;
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

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (PhysicalPartitionContext physicalPartitionContext : physicalPartitionContexts.values()) {
            parallelTablets += physicalPartitionContext.getParallelTablets();
        }
        return parallelTablets;
    }

    protected abstract void runPendingJob();

    protected abstract void runPreparingJob();

    protected abstract void runRunningJob();

    protected abstract void runCleaningJob();

    protected abstract void runAbortingJob();

    protected abstract boolean canAbort();

    public abstract void replay();

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

    private void onJobDone() {
        LOG.info("Dynamic tablet job is done. {}", this);
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

    public TDynamicTabletJobsItem getInfo() {
        TDynamicTabletJobsItem item = new TDynamicTabletJobsItem();
        item.setJob_id(jobId);
        item.setDb_id(dbId);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            item.setDb_name("");
            LOG.warn("Failed to get database name for dynamic tablet job. {}", this);
        } else {
            item.setDb_name(db.getFullName());
        }

        item.setTable_id(tableId);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) {
            item.setTable_name("");
            LOG.warn("Failed to get table name for dynamic tablet job. {}", this);
        } else {
            item.setTable_name(table.getName());
        }
        item.setJob_type(jobType.name());
        item.setJob_state(jobState.name());
        item.setTransaction_id(-1L); // override in the future pr.
        item.setParallel_partitions(physicalPartitionContexts.size());
        item.setParallel_tablets(getParallelTablets());
        item.setCreated_time(createdTimeMs / 1000);
        item.setFinished_time(finishedTimeMs / 1000);
        if (errorMessage != null) {
            item.setError_message(errorMessage);
        } else {
            item.setError_message("");
        }
        return item;
    }

    public static DynamicTabletJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DynamicTabletJob.class);
    }
}
