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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/RLTaskTxnCommitAttachment.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.routineload;

import com.google.gson.annotations.SerializedName;
import com.starrocks.thrift.TRLTaskTxnCommitAttachment;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

// {"progress": "", "backendId": "", "taskSignature": "", "numOfErrorData": "",
// "numOfTotalData": "", "taskId": "", "jobId": ""}
public class RLTaskTxnCommitAttachment extends TxnCommitAttachment {

    private long jobId;
    private TUniqueId taskId;
    @SerializedName("filteredRows")
    private long filteredRows;
    @SerializedName("loadedRows")
    private long loadedRows;
    @SerializedName("unselectedRows")
    private long unselectedRows;
    @SerializedName("receivedBytes")
    private long receivedBytes;
    @SerializedName("taskExecutionTimeMs")
    private long taskExecutionTimeMs;
    @SerializedName("progress")
    private RoutineLoadProgress progress;
    @SerializedName("timestampProgress")
    private RoutineLoadProgress timestampProgress;
    private String errorLogUrl;
    private long loadedBytes;

    private Map<Long, Long> partitionRows;

    public RLTaskTxnCommitAttachment() {
        super(TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK);
    }

    public RLTaskTxnCommitAttachment(TRLTaskTxnCommitAttachment rlTaskTxnCommitAttachment) {
        super(TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK);
        this.jobId = rlTaskTxnCommitAttachment.getJobId();
        this.taskId = rlTaskTxnCommitAttachment.getId();
        this.filteredRows = rlTaskTxnCommitAttachment.getFilteredRows();
        this.loadedRows = rlTaskTxnCommitAttachment.getLoadedRows();
        this.unselectedRows = rlTaskTxnCommitAttachment.getUnselectedRows();
        this.receivedBytes = rlTaskTxnCommitAttachment.getReceivedBytes();
        this.loadedBytes = rlTaskTxnCommitAttachment.getLoadedBytes();
        this.taskExecutionTimeMs = rlTaskTxnCommitAttachment.getLoadCostMs();
        this.partitionRows = rlTaskTxnCommitAttachment.getPartitionRows();

        switch (rlTaskTxnCommitAttachment.getLoadSourceType()) {
            case KAFKA:
                this.progress = new KafkaProgress(rlTaskTxnCommitAttachment.getKafkaRLTaskProgress()
                        .getPartitionCmtOffset());
                this.timestampProgress = new KafkaProgress(rlTaskTxnCommitAttachment.getKafkaRLTaskProgress().
                        getPartitionCmtOffsetTimestamp());
                break;
            case PULSAR:
                this.progress = new PulsarProgress(rlTaskTxnCommitAttachment.getPulsarRLTaskProgress());
                this.timestampProgress = new PulsarProgress();
                break;
            default:
                break;
        }

        if (rlTaskTxnCommitAttachment.isSetErrorLogUrl()) {
            this.errorLogUrl = rlTaskTxnCommitAttachment.getErrorLogUrl();
        }
    }

    public long getJobId() {
        return jobId;
    }

    public TUniqueId getTaskId() {
        return taskId;
    }

    public long getFilteredRows() {
        return filteredRows;
    }

    public long getLoadedRows() {
        return loadedRows;
    }

    public long getUnselectedRows() {
        return unselectedRows;
    }

    public long getTotalRows() {
        return filteredRows + loadedRows + unselectedRows;
    }

    public long getReceivedBytes() {
        return receivedBytes;
    }

    public long getLoadedBytes() {
        return loadedBytes;
    }

    public long getTaskExecutionTimeMs() {
        return taskExecutionTimeMs;
    }

    public RoutineLoadProgress getProgress() {
        return progress;
    }

    public RoutineLoadProgress getTimestampProgress() {
        return timestampProgress;
    }

    public String getErrorLogUrl() {
        return errorLogUrl;
    }

    @Override
    public String toString() {
        return "RLTaskTxnCommitAttachment [filteredRows=" + filteredRows
                + ", loadedRows=" + loadedRows
                + ", partitionRows=" + partitionRows
                + ", unselectedRows=" + unselectedRows
                + ", receivedBytes=" + receivedBytes
                + ", taskExecutionTimeMs=" + taskExecutionTimeMs
                + ", taskId=" + taskId
                + ", jobId=" + jobId
                + ", progress=" + progress.toString() + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(filteredRows);
        out.writeLong(loadedRows);
        out.writeLong(unselectedRows);
        out.writeLong(receivedBytes);
        out.writeLong(taskExecutionTimeMs);
        progress.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        filteredRows = in.readLong();
        loadedRows = in.readLong();
        unselectedRows = in.readLong();
        receivedBytes = in.readLong();
        taskExecutionTimeMs = in.readLong();
        progress = RoutineLoadProgress.read(in);
    }
}
