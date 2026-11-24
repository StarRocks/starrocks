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
import com.starrocks.persist.image.ProtoEnums;
import com.starrocks.persist.image.ProtoLists;
import com.starrocks.persist.proto.OptimizeTaskPB;
import com.starrocks.persist.proto.TaskRunStatePB;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;

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

    @Override
    public String toString() {
        return super.toString() +
                " OptimizeTask{" +
                "partitionName='" + partitionName + '\'' +
                ", tempPartitionName='" + tempPartitionName + '\'' +
                ", optimizeTaskState=" + optimizeTaskState +
                ", lastVersion=" + lastVersion;
    }

    /** Image v3 form. */
    public OptimizeTaskPB toProto() {
        OptimizeTaskPB.Builder builder = OptimizeTaskPB.newBuilder()
                .setTask(toTaskCoreProto())
                .setLastVersion(lastVersion);
        if (partitionName != null) {
            builder.setPartitionName(partitionName);
        }
        if (tempPartitionName != null) {
            builder.setTempPartitionName(tempPartitionName);
        }
        if (optimizeTaskState != null) {
            builder.setOptimizeTaskState(ProtoEnums.toProto(optimizeTaskState, TaskRunStatePB.class, "TASK_RUN_STATE_"));
        }
        if (partitionNames != null) {
            builder.setPartitionNames(ProtoLists.stringList(partitionNames));
        }
        if (tempPartitionNames != null) {
            builder.setTempPartitionNames(ProtoLists.stringList(tempPartitionNames));
        }
        return builder.build();
    }

    public static OptimizeTask fromProto(OptimizeTaskPB pb) {
        // The only constructor; GSON allocates the object without one, so the field initialisers running here is
        // the one difference, and they set the defaults a fresh task has.
        OptimizeTask task = new OptimizeTask(pb.getTask().hasName() ? pb.getTask().getName() : null);
        task.fillFromTaskCoreProto(pb.getTask());
        if (pb.hasPartitionName()) {
            task.partitionName = pb.getPartitionName();
        }
        if (pb.hasTempPartitionName()) {
            task.tempPartitionName = pb.getTempPartitionName();
        }
        if (pb.hasOptimizeTaskState()) {
            task.optimizeTaskState = ProtoEnums.fromProto(pb.getOptimizeTaskState(), Constants.TaskRunState.class,
                    "TASK_RUN_STATE_");
        }
        task.lastVersion = pb.getLastVersion();
        if (pb.hasPartitionNames()) {
            task.partitionNames = ProtoLists.toStrings(pb.getPartitionNames());
        }
        if (pb.hasTempPartitionNames()) {
            task.tempPartitionNames = ProtoLists.toStrings(pb.getTempPartitionNames());
        }
        return task;
    }
}
