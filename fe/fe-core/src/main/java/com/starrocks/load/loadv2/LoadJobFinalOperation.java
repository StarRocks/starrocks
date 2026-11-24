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

package com.starrocks.load.loadv2;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;
import com.starrocks.load.EtlStatus;
import com.starrocks.load.FailMsg;
import com.starrocks.persist.image.ProtoEnums;
import com.starrocks.persist.proto.LoadJobFinalOperationPB;
import com.starrocks.persist.proto.TxnCommitAttachmentPB;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TxnCommitAttachment;

/**
 * This object will be created when job finished or cancelled.
 * It is used to edit the job final state.
 */
public class LoadJobFinalOperation extends TxnCommitAttachment implements Writable {
    @SerializedName("id")
    private long id;
    @SerializedName("ls")
    private EtlStatus loadingStatus = new EtlStatus();
    @SerializedName("ps")
    private int progress;
    @SerializedName("lst")
    private long loadStartTimestamp;
    @SerializedName("ft")
    private long finishTimestamp;
    @SerializedName("js")
    private JobState jobState;
    // optional
    @SerializedName("fm")
    private FailMsg failMsg;

    public LoadJobFinalOperation() {
        super(TransactionState.LoadJobSourceType.BATCH_LOAD_JOB);
    }

    public LoadJobFinalOperation(long id, EtlStatus loadingStatus, int progress, long loadStartTimestamp,
                                 long finishTimestamp, JobState jobState, FailMsg failMsg) {
        super(TransactionState.LoadJobSourceType.BATCH_LOAD_JOB);
        this.id = id;
        this.loadingStatus = loadingStatus;
        this.progress = progress;
        this.loadStartTimestamp = loadStartTimestamp;
        this.finishTimestamp = finishTimestamp;
        this.jobState = jobState;
        this.failMsg = failMsg;
    }

    public long getId() {
        return id;
    }

    public EtlStatus getLoadingStatus() {
        return loadingStatus;
    }

    public int getProgress() {
        return progress;
    }

    public long getLoadStartTimestamp() {
        return loadStartTimestamp;
    }

    public long getFinishTimestamp() {
        return finishTimestamp;
    }

    public JobState getJobState() {
        return jobState;
    }

    public FailMsg getFailMsg() {
        return failMsg;
    }




    @Override
    public String toString() {
        return "LoadJobEndOperation{" +
                "id=" + id +
                ", loadingStatus=" + loadingStatus +
                ", progress=" + progress +
                ", loadStartTimestamp=" + loadStartTimestamp +
                ", finishTimestamp=" + finishTimestamp +
                ", jobState=" + jobState +
                ", failMsg=" + failMsg +
                '}';
    }

    @Override
    protected void fillProtoVariant(TxnCommitAttachmentPB.Builder parent) {
        LoadJobFinalOperationPB.Builder builder = LoadJobFinalOperationPB.newBuilder();
        builder.setId(id);
        if (loadingStatus != null) {
            builder.setLoadingStatus(loadingStatus.toProto());
        }
        builder.setProgress(progress);
        builder.setLoadStartTimestamp(loadStartTimestamp);
        builder.setFinishTimestamp(finishTimestamp);
        if (jobState != null) {
            builder.setJobState(ProtoEnums.toProto(jobState, LoadJobFinalOperationPB.JobState.class));
        }
        if (failMsg != null) {
            builder.setFailMsg(failMsg.toProto());
        }
        parent.setKind(TxnCommitAttachmentPB.Kind.LOAD_JOB_FINAL_OPERATION).setLoadJobFinalOperation(builder);
    }

    @Override
    protected void fillFromVariantProto(TxnCommitAttachmentPB parent) {
        LoadJobFinalOperationPB pb = parent.getLoadJobFinalOperation();
        if (pb.hasId()) {
            id = pb.getId();
        }
        if (pb.hasLoadingStatus()) {
            loadingStatus = EtlStatus.fromProto(pb.getLoadingStatus());
        }
        if (pb.hasProgress()) {
            progress = pb.getProgress();
        }
        if (pb.hasLoadStartTimestamp()) {
            loadStartTimestamp = pb.getLoadStartTimestamp();
        }
        if (pb.hasFinishTimestamp()) {
            finishTimestamp = pb.getFinishTimestamp();
        }
        if (pb.hasJobState()) {
            jobState = ProtoEnums.fromProto(pb.getJobState(), JobState.class);
        } else {
            jobState = null;
        }
        if (pb.hasFailMsg()) {
            failMsg = FailMsg.fromProto(pb.getFailMsg());
        } else {
            failMsg = null;
        }
    }
}
