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


package com.starrocks.load;

import com.google.gson.annotations.SerializedName;
import com.starrocks.sql.ast.InsertStmt;

import java.util.List;

public class InsertOverwriteJob {
    @SerializedName(value = "jobId")
    private long jobId;

    @SerializedName(value = "jobState")
    private InsertOverwriteJobState jobState;

    @SerializedName(value = "sourcePartitionIds")
    private List<Long> sourcePartitionIds;

    @SerializedName(value = "tmpPartitionIds")
    private List<Long> tmpPartitionIds;

    @SerializedName(value = "targetDbId")
    private long targetDbId;

    @SerializedName(value = "targetTableId")
    private long targetTableId;

    private transient InsertStmt insertStmt;

    public InsertOverwriteJob(long jobId, InsertStmt insertStmt, long targetDbId, long targetTableId) {
        this.jobId = jobId;
        this.insertStmt = insertStmt;
        this.sourcePartitionIds = insertStmt.getTargetPartitionIds();
        this.jobState = InsertOverwriteJobState.OVERWRITE_PENDING;
        this.targetDbId = targetDbId;
        this.targetTableId = targetTableId;
    }

    // used to replay InsertOverwriteJob
    public InsertOverwriteJob(long jobId, long targetDbId, long targetTableId, List<Long> sourcePartitionIds) {
        this.jobId = jobId;
        this.targetDbId = targetDbId;
        this.targetTableId = targetTableId;
        this.sourcePartitionIds = sourcePartitionIds;
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

    public List<Long> getSourcePartitionIds() {
        return sourcePartitionIds;
    }

    public void setSourcePartitionIds(List<Long> sourcePartitionIds) {
        this.sourcePartitionIds = sourcePartitionIds;
    }

    public List<Long> getTmpPartitionIds() {
        return tmpPartitionIds;
    }

    public void setTmpPartitionIds(List<Long> tmpPartitionIds) {
        this.tmpPartitionIds = tmpPartitionIds;
    }

    public long getTargetDbId() {
        return targetDbId;
    }

    public long getTargetTableId() {
        return targetTableId;
    }

    public boolean isFinished() {
        return jobState == InsertOverwriteJobState.OVERWRITE_SUCCESS
                || jobState == InsertOverwriteJobState.OVERWRITE_FAILED;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }
}
