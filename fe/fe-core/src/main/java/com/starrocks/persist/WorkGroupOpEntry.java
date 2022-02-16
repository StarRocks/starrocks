// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.WorkGroup;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TWorkGroupOp;
import com.starrocks.thrift.TWorkGroupOpType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// WorkGroupEntry is used by EditLog to persist WorkGroupOp in replicated log
public class WorkGroupOpEntry implements Writable {
    @SerializedName(value = "workgroup")
    WorkGroup workgroup;
    @SerializedName(value = "opType")
    private TWorkGroupOpType opType;

    public WorkGroupOpEntry(TWorkGroupOpType opType, WorkGroup workGroup) {
        this.opType = opType;
        this.workgroup = workGroup;
    }

    public static WorkGroupOpEntry read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, WorkGroupOpEntry.class);
    }

    public WorkGroup getWorkgroup() {
        return workgroup;
    }

    public void setWorkgroup(WorkGroup workgroup) {
        this.workgroup = workgroup;
    }

    public TWorkGroupOpType getOpType() {
        return opType;
    }

    public void setOpType(TWorkGroupOpType opType) {
        this.opType = opType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public TWorkGroupOp toThrift() {
        TWorkGroupOp op = new TWorkGroupOp();
        op.setWorkgroup(workgroup.toThrift());
        op.setOp_type(opType);
        return op;
    }
}
