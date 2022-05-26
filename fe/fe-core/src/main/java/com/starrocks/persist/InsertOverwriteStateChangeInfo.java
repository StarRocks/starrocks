// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.load.InsertOverwriteJobState;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class InsertOverwriteStateChangeInfo implements Writable {
    @SerializedName(value = "jobId")
    private long jobId;
    @SerializedName(value = "fromState")
    private InsertOverwriteJobState fromState;
    @SerializedName(value = "toState")
    private InsertOverwriteJobState toState;
    @SerializedName(value = "sourcePartitionName")
    private List<String> sourcePartitionNames;
    @SerializedName(value = "newPartitionNames")
    private List<String> newPartitionNames;

    public InsertOverwriteStateChangeInfo(long jobId, InsertOverwriteJobState fromState,
                                          InsertOverwriteJobState toState,
                                          List<String> sourcePartitionNames, List<String> newPartitionNames) {
        this.jobId = jobId;
        this.fromState = fromState;
        this.toState = toState;
        this.sourcePartitionNames = sourcePartitionNames;
        this.newPartitionNames = newPartitionNames;
    }

    public long getJobId() {
        return jobId;
    }

    public InsertOverwriteJobState getFromState() {
        return fromState;
    }

    public InsertOverwriteJobState getToState() {
        return toState;
    }

    public List<String> getSourcePartitionNames() {
        return sourcePartitionNames;
    }

    public List<String> getNewPartitionsName() {
        return newPartitionNames;
    }

    @Override
    public String toString() {
        return "InsertOverwriteStateChangeInfo{" +
                "jobId=" + jobId +
                ", fromState=" + fromState +
                ", toState=" + toState +
                ", sourcePartitionNames=" + sourcePartitionNames +
                ", newPartitionNames=" + newPartitionNames +
                '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static InsertOverwriteStateChangeInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, InsertOverwriteStateChangeInfo.class);
    }
}
