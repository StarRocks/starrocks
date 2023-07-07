// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.privilege.MaskingPolicyContext;
import com.starrocks.epack.privilege.TableUID;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ApplyOrRevokeMaskingPolicyLog implements Writable {
    @SerializedName(value = "t")
    public TableUID table;
    @SerializedName(value = "c")
    public String columnName;
    @SerializedName(value = "m")
    private MaskingPolicyContext columnMaskingPolicyContext;

    public ApplyOrRevokeMaskingPolicyLog(TableUID table, String columnName,
                                         MaskingPolicyContext columnMaskingPolicyContext) {
        this.table = table;
        this.columnName = columnName;
        this.columnMaskingPolicyContext = columnMaskingPolicyContext;
    }

    public TableUID getTable() {
        return table;
    }

    public String getColumnName() {
        return columnName;
    }

    public MaskingPolicyContext getColumnMaskingPolicyContext() {
        return columnMaskingPolicyContext;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static ApplyOrRevokeMaskingPolicyLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ApplyOrRevokeMaskingPolicyLog.class);
    }
}
