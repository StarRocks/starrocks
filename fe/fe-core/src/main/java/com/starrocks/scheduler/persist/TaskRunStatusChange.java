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


package com.starrocks.scheduler.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaskRunStatusChange implements Writable {

    @SerializedName("taskId")
    private long taskId;

    @SerializedName("queryId")
    private String queryId;

    @SerializedName("finishTime")
    private long finishTime;

    @SerializedName("fromStatus")
    Constants.TaskRunState fromStatus;

    @SerializedName("toStatus")
    Constants.TaskRunState toStatus;

    @SerializedName("errorCode")
    private int errorCode;

    @SerializedName("errorMessage")
    private String errorMessage;

    @SerializedName("extraMessage")
    private String extraMessage;

    public TaskRunStatusChange(long taskId, TaskRunStatus status,
                               Constants.TaskRunState fromStatus,
                               Constants.TaskRunState toStatus) {
        this.taskId = taskId;
        this.queryId = status.getQueryId();
        this.fromStatus = fromStatus;
        this.toStatus = toStatus;
        this.finishTime = status.getFinishTime();
        if (toStatus == Constants.TaskRunState.FAILED) {
            errorCode = status.getErrorCode();
            errorMessage = status.getErrorMessage();
        }
        this.extraMessage = status.getExtraMessage();
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public Constants.TaskRunState getFromStatus() {
        return fromStatus;
    }

    public void setFromStatus(Constants.TaskRunState fromStatus) {
        this.fromStatus = fromStatus;
    }

    public Constants.TaskRunState getToStatus() {
        return toStatus;
    }

    public void setToStatus(Constants.TaskRunState toStatus) {
        this.toStatus = toStatus;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public String getExtraMessage() {
        return extraMessage;
    }

    public void setExtraMessage(String extraMessage) {
        this.extraMessage = extraMessage;
    }

    public static TaskRunStatusChange read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TaskRunStatusChange.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public String toString() {
        return "TaskRunStatus{" +
                "queryId='" + queryId + '\'' +
                ", taskId='" + taskId + '\'' +
                ", finishTime=" + finishTime +
                ", fromStatus=" + fromStatus +
                ", toStatus=" + toStatus +
                ", errorCode=" + errorCode +
                ", errorMessage='" + errorMessage + '\'' +
                ", extraMessage=" + getExtraMessage() +
                '}';
    }
}
