// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class DropTasksLog implements Writable {

    @SerializedName("taskIdList")
    List<Long> taskIdList;

    public DropTasksLog(List<Long> taskIdList) {
        this.taskIdList = taskIdList;
    }

    public List<Long> getTaskIdList() {
        return taskIdList;
    }

    public static DropTasksLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), DropTasksLog.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
