// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DropComputeNodeLog implements Writable {

    @SerializedName(value = "computeNodeId")
    private long computeNodeId;

    public DropComputeNodeLog(long computeNodeId) {
        this.computeNodeId = computeNodeId;
    }

    public long getComputeNodeId() {
        return computeNodeId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static DropComputeNodeLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), DropComputeNodeLog.class);
    }
}
