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
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
public class CreateInsertOverwriteJobLog implements Writable {
    @SerializedName(value = "jobId")
    private long jobId;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "tableId")
    private long tableId;

    @SerializedName(value = "targetPartitionIds")
    private List<Long> targetPartitionIds;

    public CreateInsertOverwriteJobLog(long jobId, long dbId, long tableId, List<Long> targetPartitionIds) {
        this.jobId = jobId;
        this.dbId = dbId;
        this.tableId = tableId;
        this.targetPartitionIds = targetPartitionIds;
    }

    public long getJobId() {
        return jobId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public List<Long> getTargetPartitionIds() {
        return targetPartitionIds;
    }

    @Override
    public String toString() {
        return "CreateInsertOverwriteJobInfo{" +
                "jobId=" + jobId +
                ", dbId=" + dbId +
                ", tableId=" + tableId +
                ", targetPartitionIds=" + targetPartitionIds +
                '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CreateInsertOverwriteJobLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CreateInsertOverwriteJobLog.class);
    }
}
