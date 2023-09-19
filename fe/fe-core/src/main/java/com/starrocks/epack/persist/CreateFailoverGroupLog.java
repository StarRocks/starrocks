// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CreateFailoverGroupLog implements Writable {
    @SerializedName(value = "failoverGroup")
    private FailoverGroup failoverGroup;

    public CreateFailoverGroupLog(FailoverGroup failoverGroup) {
        this.failoverGroup = failoverGroup;
    }

    public FailoverGroup getFailoverGroup() {
        return failoverGroup;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CreateFailoverGroupLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CreateFailoverGroupLog.class);
    }
}
