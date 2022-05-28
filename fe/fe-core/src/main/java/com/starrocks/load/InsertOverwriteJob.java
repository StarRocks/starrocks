// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.InsertStmt;

import java.util.List;

public class InsertOverwriteJob {
    @SerializedName(value = "jobId")
    private long jobId;

    @SerializedName(value = "jobState")
    private InsertOverwriteJobState jobState;

    @SerializedName(value = "sourcePartitionNames")
    private List<String> sourcePartitionNames;

    @SerializedName(value = "newPartitionNames")
    private List<String> newPartitionNames;

    @SerializedName(value = "targetDbId")
    private long targetDbId;

    @SerializedName(value = "targetTableId")
    private long targetTableId;

    @SerializedName(value = "originalTargetPartitionIds")
    private List<Long> originalTargetPartitionIds;

    private InsertStmt insertStmt;

    public InsertOverwriteJob(long jobId, InsertStmt insertStmt, long targetDbId, long targetTableId) {
        this.jobId = jobId;
        this.insertStmt = insertStmt;
        this.originalTargetPartitionIds = insertStmt.getTargetPartitionIds();
        this.jobState = InsertOverwriteJobState.OVERWRITE_PENDING;
        this.targetDbId = targetDbId;
        this.targetTableId = targetTableId;
    }

    // used to replay InsertOverwriteJob
    public InsertOverwriteJob(long jobId, long targetDbId, long targetTableId, List<Long> targetPartitionIds) {
        this.jobId = jobId;
        this.targetDbId = targetDbId;
        this.targetTableId = targetTableId;
        this.originalTargetPartitionIds = targetPartitionIds;
        this.jobState = InsertOverwriteJobState.OVERWRITE_PENDING;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public InsertOverwriteJobState getJobState() {
        return jobState;
    }

    public void setJobState(InsertOverwriteJobState newState) {
        jobState = newState;
    }

    public List<String> getSourcePartitionNames() {
        return sourcePartitionNames;
    }

    public void setSourcePartitionNames(List<String> sourcePartitionNames) {
        this.sourcePartitionNames = sourcePartitionNames;
    }

    public List<String> getNewPartitionNames() {
        return newPartitionNames;
    }

    public void setNewPartitionNames(List<String> newPartitionNames) {
        this.newPartitionNames = newPartitionNames;
    }

    public long getTargetDbId() {
        return targetDbId;
    }

    public long getTargetTableId() {
        return targetTableId;
    }

    public List<Long> getOriginalTargetPartitionIds() {
        return originalTargetPartitionIds;
    }

    public boolean isFinished() {
        return jobState == InsertOverwriteJobState.OVERWRITE_SUCCESS
                || jobState == InsertOverwriteJobState.OVERWRITE_FAILED;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }
}
