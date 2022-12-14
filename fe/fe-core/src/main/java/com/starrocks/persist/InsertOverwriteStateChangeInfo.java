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

    @SerializedName(value = "sourcePartitionIds")
    private List<Long> sourcePartitionIds;

    @SerializedName(value = "tmpPartitionIds")
    private List<Long> tmpPartitionIds;

    public InsertOverwriteStateChangeInfo(long jobId, InsertOverwriteJobState fromState,
                                          InsertOverwriteJobState toState,
                                          List<Long> sourcePartitionIds, List<Long> tmpPartitionIds) {
        this.jobId = jobId;
        this.fromState = fromState;
        this.toState = toState;
        this.sourcePartitionIds = sourcePartitionIds;
        this.tmpPartitionIds = tmpPartitionIds;
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

    public List<Long> getSourcePartitionIds() {
        return sourcePartitionIds;
    }

    public List<Long> getTmpPartitionIds() {
        return tmpPartitionIds;
    }

    @Override
    public String toString() {
        return "InsertOverwriteStateChangeInfo{" +
                "jobId=" + jobId +
                ", fromState=" + fromState +
                ", toState=" + toState +
                ", sourcePartitionIds=" + sourcePartitionIds +
                ", tmpPartitionIds=" + tmpPartitionIds +
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
