// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;


public class UpdateProgressInfo implements Writable {
    // taskId -> progress
    @SerializedName("taskRunProgressMap")
    private Map<Long, Integer> taskRunProgressMap;

    public UpdateProgressInfo(Map<Long, Integer> taskRunProgressMap) {
        this.taskRunProgressMap = taskRunProgressMap;
    }

    public Map<Long, Integer> getTaskRunProgressMap() {
        return taskRunProgressMap;
    }

    public void setTaskRunProgressMap(Map<Long, Integer> taskRunPrgressMap) {
        this.taskRunProgressMap = taskRunPrgressMap;
    }

    public static UpdateProgressInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, UpdateProgressInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
