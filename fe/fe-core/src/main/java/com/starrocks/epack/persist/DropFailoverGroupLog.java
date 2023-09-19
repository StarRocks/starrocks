// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DropFailoverGroupLog implements Writable {
    @SerializedName(value = "failoverGroupId")
    private long failoverGroupId;

    public DropFailoverGroupLog(long failoverGroupId) {
        this.failoverGroupId = failoverGroupId;
    }

    public long getFailoverGroupId() {
        return failoverGroupId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static DropFailoverGroupLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DropFailoverGroupLog.class);
    }
}
