// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.privilege.RowAccessPolicyContext;
import com.starrocks.epack.privilege.TableUID;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ApplyOrRevokeRowAccessPolicyLog implements Writable {
    @SerializedName(value = "t")
    public TableUID table;
    @SerializedName(value = "r")
    private RowAccessPolicyContext rowAccessPolicyContext;

    public ApplyOrRevokeRowAccessPolicyLog(TableUID table,
                                           RowAccessPolicyContext rowAccessPolicyContext) {
        this.table = table;
        this.rowAccessPolicyContext = rowAccessPolicyContext;
    }

    public TableUID getTable() {
        return table;
    }

    public RowAccessPolicyContext getRowAccessPolicyContext() {
        return rowAccessPolicyContext;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static ApplyOrRevokeRowAccessPolicyLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ApplyOrRevokeRowAccessPolicyLog.class);
    }
}
