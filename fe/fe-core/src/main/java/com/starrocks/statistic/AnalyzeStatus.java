// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.MetaUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AnalyzeStatus implements Writable {
    @SerializedName("id")
    private long id;

    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

    @SerializedName("columns")
    private List<String> columns;

    @SerializedName("type")
    private StatsConstants.AnalyzeType type;

    @SerializedName("scheduleType")
    private StatsConstants.ScheduleType scheduleType;

    @SerializedName("properties")
    private Map<String, String> properties;

    @SerializedName("status")
    private StatsConstants.ScheduleStatus status;

    @SerializedName("startTime")
    private LocalDateTime startTime;

    @SerializedName("endTime")
    private LocalDateTime endTime;

    @SerializedName("reason")
    private String reason;

    @SerializedName("progress")
    private long progress;

    public AnalyzeStatus(long id, long dbId, long tableId, List<String> columns,
                         StatsConstants.AnalyzeType type,
                         StatsConstants.ScheduleType scheduleType,
                         Map<String, String> properties,
                         LocalDateTime startTime) {
        this.id = id;
        this.dbId = dbId;
        this.tableId = tableId;
        this.columns = columns;
        this.type = type;
        this.scheduleType = scheduleType;
        this.properties = properties;
        this.startTime = startTime;
    }

    public long getId() {
        return id;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public List<String> getColumns() {
        return columns;
    }

    public StatsConstants.AnalyzeType getType() {
        return type;
    }

    public StatsConstants.ScheduleType getScheduleType() {
        return scheduleType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public void setStatus(StatsConstants.ScheduleStatus status) {
        this.status = status;
    }

    public StatsConstants.ScheduleStatus getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public long getProgress() {
        return progress;
    }

    public void setProgress(long progress) {
        this.progress = progress;
    }

    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Table", ScalarType.createVarchar(20)))
            .addColumn(new Column("Op", ScalarType.createVarchar(20)))
            .addColumn(new Column("Msg_type", ScalarType.createVarchar(20)))
            .addColumn(new Column("Msg_text", ScalarType.createVarchar(200)))
            .build();

    public ShowResultSet toShowResult() {
        String dbName;
        if (dbId == StatsConstants.DEFAULT_ALL_ID) {
            dbName = "*";
        } else {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            dbName = db.getOriginName();
        }
        String tableName;
        if (tableId == StatsConstants.DEFAULT_ALL_ID) {
            tableName = "*";
        } else {
            try {
                tableName = MetaUtils.getTable(dbId, tableId).getName();
            } catch (SemanticException e) {
                tableName = "<tableId : " + tableId + ">";
                status = StatsConstants.ScheduleStatus.FAILED;
                reason = e.getMessage();
            }
        }

        String op = "unknown";
        if (type.equals(StatsConstants.AnalyzeType.HISTOGRAM)) {
            op = "histogram";
        } else if (type.equals(StatsConstants.AnalyzeType.FULL)) {
            op = "analyze";
        } else if (type.equals(StatsConstants.AnalyzeType.SAMPLE)) {
            op = "sample";
        }

        String msgType;
        String msgText;
        if (status.equals(StatsConstants.ScheduleStatus.FAILED)) {
            msgType = "error";
            msgText = reason;
        } else {
            msgType = "status";
            msgText = "OK";
        }

        List<List<String>> rows = new ArrayList<>();
        rows.add(Lists.newArrayList(dbName + "." + tableName, op, msgType, msgText));
        return new ShowResultSet(META_DATA, rows);
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
}
