// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TWorkGroupOp;
import com.starrocks.thrift.TWorkGroupOpType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// ResourceGroupEntry is used by EditLog to persist ResourceGroupOp in replicated log
public class ResourceGroupOpEntry implements Writable {
    @SerializedName(value = "workgroup")
    ResourceGroup resourceGroup;
    @SerializedName(value = "opType")
    private TWorkGroupOpType opType;

    public ResourceGroupOpEntry(TWorkGroupOpType opType, ResourceGroup resourceGroup) {
        this.opType = opType;
        this.resourceGroup = resourceGroup;
    }

    public static ResourceGroupOpEntry read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ResourceGroupOpEntry.class);
    }

    public ResourceGroup getResourceGroup() {
        return resourceGroup;
    }

    public void setResourceGroup(ResourceGroup resourceGroup) {
        this.resourceGroup = resourceGroup;
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
        op.setWorkgroup(resourceGroup.toThrift());
        op.setOp_type(opType);
        return op;
    }
}
