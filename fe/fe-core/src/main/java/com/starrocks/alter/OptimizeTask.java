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


package com.starrocks.alter;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

public class OptimizeTask extends Task {

    @SerializedName("partitionName")
    private String partitionName;

    @SerializedName("tempPartitionName")
    private String tempPartitionName;

    @SerializedName("optimizeTaskState")
    private Constants.TaskRunState optimizeTaskState = Constants.TaskRunState.PENDING;

    @SerializedName("lastVersion")
    private long lastVersion;

    @SerializedName("partitionNames")
    private List<String> partitionNames;

    @SerializedName("tempPartitionNames")
    private List<String> tempPartitionNames;

    public OptimizeTask(String name) {
        super(name);
    }

    public List<String> getTempPartitionNames() {
        return tempPartitionNames;
    }

    public void setTempPartitionNames(List<String> tempPartitionNames) {
        this.tempPartitionNames = tempPartitionNames;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getTempPartitionName() {
        return tempPartitionName;
    }

    public void setTempPartitionName(String tempPartitionName) {
        this.tempPartitionName = tempPartitionName;
    }

    public Constants.TaskRunState getOptimizeTaskState() {
        return this.optimizeTaskState;
    }

    public long getLastVersion() {
        return lastVersion;
    }

    public void setLastVersion(long version) {
        this.lastVersion = version;
    }

    public void setOptimizeTaskState(Constants.TaskRunState state) {
        this.optimizeTaskState = state;
    }

    public static Task read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, OptimizeTask.class);
    }



    @Override
    public String toString() {
        return super.toString() +
                " OptimizeTask{" +
                "partitionName='" + partitionName + '\'' +
                ", tempPartitionName='" + tempPartitionName + '\'' +
                ", optimizeTaskState=" + optimizeTaskState +
                ", lastVersion=" + lastVersion;
    }
}
