// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class MultiEraseTableInfo implements Writable {

    @SerializedName(value = "tableIds")
    private List<Long> tableIds;

    public MultiEraseTableInfo(List<Long> tableIds) {
        this.tableIds = tableIds;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static MultiEraseTableInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, MultiEraseTableInfo.class);
    }

    public List<Long> getTableIds() {
        return tableIds;
    }
}
