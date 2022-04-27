// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.google.gson.annotations.SerializedName;
import com.starrocks.statistic.Constants;

import java.time.LocalDateTime;

public class MaterializedViewRefreshTask  implements IMaterializedViewRefreshTask {

    @SerializedName("startTime")
    protected LocalDateTime startTime;

    @SerializedName("endTime")
    protected LocalDateTime endTime;

    @SerializedName("status")
    protected Constants.MaterializedViewTaskStatus status = Constants.MaterializedViewTaskStatus.PENDING;

    @SerializedName("errMsg")
    protected String errMsg;

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public Constants.MaterializedViewTaskStatus getStatus() {
        return status;
    }

    public void setStatus(Constants.MaterializedViewTaskStatus status) {
        this.status = status;
    }

    @Override
    public void beginTask() {
        startTime = LocalDateTime.now();
    }

    @Override
    public void runTask() throws Exception {}

    @Override
    public void finishTask() {
        endTime = LocalDateTime.now();
    }

    @Override
    public IMaterializedViewRefreshTask cloneTask() {
        return new MaterializedViewPartitionRefreshTask();
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }
}
