// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.automv.qe;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.DateType;
import com.starrocks.type.StringType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class RecommendationsTaskStatus implements Writable {
    private static final ShowResultSetMetaData TASK_METADATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("TaskName", StringType.STRING))
            .addColumn(new Column("Tunespace", StringType.STRING))
            .addColumn(new Column("ResultTable", StringType.STRING))
            .addColumn(new Column("StartTime", DateType.DATETIME))
            .addColumn(new Column("EndTime", DateType.DATETIME))
            .addColumn(new Column("LastTime", StringType.STRING))
            .addColumn(new Column("Status", StringType.STRING))
            .addColumn(new Column("ErrorMsg", StringType.STRING))
            .build();
    @SerializedName(value = "taskName")
    private String taskName;
    @SerializedName(value = "tunespace")
    private String tunespace;
    @SerializedName(value = "resultTable")
    private String resultTable;
    @SerializedName(value = "startTime")
    private Long startTime;
    @SerializedName(value = "endTime")
    private Long endTime;
    @SerializedName(value = "status")
    private Status status;
    @SerializedName(value = "errorMsg")
    private String errorMsg;
    private transient ShowResultSet result;

    public RecommendationsTaskStatus(String taskName, String tunespace, String resultTable) {
        this.taskName = taskName;
        this.tunespace = tunespace;
        this.resultTable = resultTable;
        this.startTime = System.currentTimeMillis();
        this.status = Status.PENDING;
    }

    public static RecommendationsTaskStatus read(DataInput input) throws IOException {
        String s = Text.readString(input);
        return GsonUtils.GSON.fromJson(s, RecommendationsTaskStatus.class);
    }

    private String formatDuration(long startTime, long endTime) {
        Duration duration = Duration.between(new Timestamp(startTime).toInstant(), new Timestamp(endTime).toInstant());
        long hours = duration.toHours();
        StringBuilder sb = new StringBuilder();
        if (hours > 0) {
            sb.append(hours).append("h");
            sb.append(duration.toMinutesPart()).append("min");
        } else if (duration.toMinutes() > 0) {
            sb.append(duration.toMinutes()).append("min");
            sb.append(duration.toSecondsPart()).append("s");
        } else {
            sb.append(duration.toSeconds()).append("s");
            sb.append(duration.toMillisPart()).append("ms");
        }
        return sb.toString();
    }

    public ShowResultSet toShowResultSet() {
        List<String> result = Lists.newArrayList();
        result.add(taskName);
        result.add(tunespace);
        result.add(resultTable);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        result.add(sdf.format(startTime));
        if (endTime != null) {
            result.add(sdf.format(endTime));
            result.add(formatDuration(startTime, endTime));
        } else {
            result.add("");
            result.add(formatDuration(startTime, System.currentTimeMillis()));
        }
        result.add(status.name());
        result.add(Optional.ofNullable(errorMsg).orElse(""));
        return new ShowResultSet(TASK_METADATA, ImmutableList.of(result));
    }

    public String getTunespace() {
        return tunespace;
    }

    public void setTunespace(String tunespace) {
        this.tunespace = tunespace;
    }

    public String getResultTable() {
        return resultTable;
    }

    public void setResultTable(String resultTable) {
        this.resultTable = resultTable;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public boolean isPending() {
        return status.equals(Status.PENDING);
    }

    public ShowResultSet getResult() {
        return result;
    }

    public void setResult(ShowResultSet result) {
        this.result = result;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public void persist() {
        EditLogEPack editLog = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
        editLog.logRecommendationsTaskStatusChange(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecommendationsTaskStatus that = (RecommendationsTaskStatus) o;
        return Objects.equals(taskName, that.taskName) && Objects.equals(tunespace, that.tunespace) &&
                Objects.equals(resultTable, that.resultTable) &&
                Objects.equals(startTime, that.startTime) && Objects.equals(endTime, that.endTime) &&
                status == that.status && Objects.equals(errorMsg, that.errorMsg) &&
                Objects.equals(result, that.result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskName, tunespace, resultTable, startTime, endTime, status, errorMsg, result);
    }

    @Override
    public String toString() {
        return "RecommendationsTaskStatus{" +
                "taskName='" + taskName + '\'' +
                ", tunespace='" + tunespace + '\'' +
                ", resultTable='" + resultTable + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", status=" + status +
                ", errorMsg='" + errorMsg + '\'' +
                ", result=" + result +
                '}';
    }

    public enum Status {
        PENDING,
        SUCCESS,
        ERROR,
        EXPIRED
    }
}