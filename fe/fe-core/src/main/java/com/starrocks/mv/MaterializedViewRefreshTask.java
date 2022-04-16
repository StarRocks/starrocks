// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.google.gson.annotations.SerializedName;
import com.starrocks.statistic.Constants;

import java.time.LocalDateTime;

public class MaterializedViewRefreshTask {

    @SerializedName("startTime")
    protected LocalDateTime startTime;

    @SerializedName("endTime")
    protected LocalDateTime endTime;

    @SerializedName("status")
    protected Constants.MaterializedViewTaskStatus status = Constants.MaterializedViewTaskStatus.PENDING;

    @SerializedName("retryTime")
    protected int retryTime = 0;

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

    public int getRetryTime() {
        return retryTime;
    }

    public void setRetryTime(int retryTime) {
        this.retryTime = retryTime;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }
}
