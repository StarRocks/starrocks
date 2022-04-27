// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.statistic.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MaterializedViewRefreshJobStatusChange implements Writable {

    @SerializedName("mvTableId")
    private long mvTableId;

    @SerializedName("jobId")
    private long jobId;

    @SerializedName("fromStatus")
    Constants.MaterializedViewJobStatus fromStatus;

    @SerializedName("toStatus")
    Constants.MaterializedViewJobStatus toStatus;

    public MaterializedViewRefreshJobStatusChange(long mvTableId, long jobId,
                                                  Constants.MaterializedViewJobStatus fromStatus,
                                                  Constants.MaterializedViewJobStatus toStatus) {
        this.mvTableId = mvTableId;
        this.jobId = jobId;
        this.fromStatus = fromStatus;
        this.toStatus = toStatus;
    }

    public long getMvTableId() {
        return mvTableId;
    }

    public void setMvTableId(long mvTableId) {
        this.mvTableId = mvTableId;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public Constants.MaterializedViewJobStatus getFromStatus() {
        return fromStatus;
    }

    public void setFromStatus(Constants.MaterializedViewJobStatus fromStatus) {
        this.fromStatus = fromStatus;
    }

    public Constants.MaterializedViewJobStatus getToStatus() {
        return toStatus;
    }

    public void setToStatus(Constants.MaterializedViewJobStatus toStatus) {
        this.toStatus = toStatus;
    }

    public static MaterializedViewRefreshJobStatusChange read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MaterializedViewRefreshJobStatusChange.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

}
