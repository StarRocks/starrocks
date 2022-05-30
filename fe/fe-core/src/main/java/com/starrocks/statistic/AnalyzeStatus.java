// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;

public class AnalyzeStatus implements Writable {
    @SerializedName("id")
    private long id;

    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

    @SerializedName("type")
    private Constants.AnalyzeType type;

    @SerializedName("scheduleType")
    private Constants.ScheduleType scheduleType;

    @SerializedName("workTime")
    private LocalDateTime workTime;

    public AnalyzeStatus(long id, long dbId, long tableId,
                         Constants.AnalyzeType type,
                         Constants.ScheduleType scheduleType,
                         LocalDateTime workTime) {
        this.id = id;
        this.dbId = dbId;
        this.tableId = tableId;
        this.type = type;
        this.scheduleType = scheduleType;
        this.workTime = workTime;
    }

    public long getId() {
        return id;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }

    public static AnalyzeStatus read(DataInput in) throws IOException {
        String s = Text.readString(in);
        return GsonUtils.GSON.fromJson(s, AnalyzeStatus.class);
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public Constants.AnalyzeType getType() {
        return type;
    }

    public Constants.ScheduleType getScheduleType() {
        return scheduleType;
    }

    public LocalDateTime getWorkTime() {
        return workTime;
    }
}
