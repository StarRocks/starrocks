// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.Constants.AnalyzeType;
import com.starrocks.statistic.Constants.ScheduleStatus;
import com.starrocks.statistic.Constants.ScheduleType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class AnalyzeJob implements Writable {
    public static final String PROP_UPDATE_INTERVAL_SEC_KEY = "update_interval_sec";
    public static final String PROP_SAMPLE_COLLECT_ROWS_KEY = "sample_collect_rows";

    public static final List<String> NUMBER_PROP_KEY_LIST = ImmutableList.<String>builder()
            .add(PROP_UPDATE_INTERVAL_SEC_KEY)
            .add(PROP_SAMPLE_COLLECT_ROWS_KEY).build();

    public static final long DEFAULT_ALL_ID = -1;

    @SerializedName("id")
    private long id;

    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

    // Empty is all column
    @SerializedName("columns")
    private List<String> columns;

    @SerializedName("type")
    private AnalyzeType type;

    @SerializedName("scheduleType")
    private ScheduleType scheduleType;

    @SerializedName("properties")
    private Map<String, String> properties;

    @SerializedName("status")
    private ScheduleStatus status;

    @SerializedName("workTime")
    private LocalDateTime workTime;

    @SerializedName("reason")
    private String reason;

    public AnalyzeJob() {
        dbId = DEFAULT_ALL_ID;
        tableId = DEFAULT_ALL_ID;
        columns = Lists.newArrayList();
        type = AnalyzeType.SAMPLE;
        scheduleType = ScheduleType.ONCE;
        properties = Maps.newHashMap();
        status = ScheduleStatus.PENDING;
        workTime = LocalDateTime.MIN;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public AnalyzeType getType() {
        return type;
    }

    public void setType(AnalyzeType type) {
        this.type = type;
    }

    public LocalDateTime getWorkTime() {
        return workTime;
    }

    public void setWorkTime(LocalDateTime workTime) {
        this.workTime = workTime;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public long getUpdateIntervalSec() {
        return Long.parseLong(properties
                .getOrDefault(PROP_UPDATE_INTERVAL_SEC_KEY, String.valueOf(Config.statistic_update_interval_sec)));
    }

    public long getSampleCollectRows() {
        return Long.parseLong(properties
                .getOrDefault(PROP_SAMPLE_COLLECT_ROWS_KEY, String.valueOf(Config.statistic_sample_collect_rows)));
    }

    public ScheduleType getScheduleType() {
        return scheduleType;
    }

    public void setScheduleType(ScheduleType scheduleType) {
        this.scheduleType = scheduleType;
    }

    public ScheduleStatus getStatus() {
        return status;
    }

    public void setStatus(ScheduleStatus status) {
        this.status = status;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public List<String> showAnalyzeJobs() throws MetaNotFoundException {
        List<String> row = Lists.newArrayList("", "ALL", "ALL", "ALL", "", "", "", "", "", "");

        row.set(0, String.valueOf(id));
        if (DEFAULT_ALL_ID != dbId) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);

            if (db == null) {
                throw new MetaNotFoundException("No found database: " + dbId);
            }

            row.set(1, db.getFullName());

            if (DEFAULT_ALL_ID != tableId) {
                Table table = db.getTable(tableId);

                if (table == null) {
                    throw new MetaNotFoundException("No found table: " + tableId);
                }

                row.set(2, table.getName());

                if (null != columns && !columns.isEmpty()
                        && (columns.size() != table.getBaseSchema().size())) {
                    String str = String.join(",", columns);
                    if (str.length() > 100) {
                        row.set(3, str.substring(0, 100) + "...");
                    } else {
                        row.set(3, str);
                    }
                }
            }
        }

        row.set(4, type.name());
        row.set(5, scheduleType.name());
        row.set(6, properties == null ? "{}" : properties.toString());
        row.set(7, status.name());
        if (LocalDateTime.MIN.equals(workTime)) {
            row.set(8, "None");
        } else {
            row.set(8, workTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        }

        if (null != reason) {
            row.set(9, reason);
        }

        return row;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }

    public static AnalyzeJob read(DataInput in) throws IOException {
        String s = Text.readString(in);
        return GsonUtils.GSON.fromJson(s, AnalyzeJob.class);
    }

    @Override
    public String toString() {
        return "AnalyzeJob{" +
                "id=" + id +
                ", dbId=" + dbId +
                ", tableId=" + tableId +
                ", columns=" + columns +
                ", type=" + type +
                ", scheduleType=" + scheduleType +
                ", properties=" + properties +
                ", status=" + status +
                ", workTime=" + workTime +
                ", reason='" + reason + '\'' +
                '}';
    }

}
