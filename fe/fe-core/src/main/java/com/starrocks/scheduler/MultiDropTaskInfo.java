// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class MultiDropTaskInfo implements Writable {

    @SerializedName("taskNameList")
    List<String> taskNameList;

    public MultiDropTaskInfo(List<String> taskNameList) {
        this.taskNameList = taskNameList;
    }

    public List<String> getTaskNameList() {
        return taskNameList;
    }

    public static MultiDropTaskInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), MultiDropTaskInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
