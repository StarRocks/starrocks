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


package com.starrocks.statistic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.image.ProtoDateTimes;
import com.starrocks.persist.image.ProtoEnums;
import com.starrocks.persist.image.ProtoLists;
import com.starrocks.persist.image.ProtoMaps;
import com.starrocks.persist.proto.AnalyzeScheduleStatusPB;
import com.starrocks.persist.proto.AnalyzeScheduleTypePB;
import com.starrocks.persist.proto.AnalyzeStatusCorePB;
import com.starrocks.persist.proto.AnalyzeStatusPB;
import com.starrocks.persist.proto.AnalyzeTypePB;
import com.starrocks.persist.proto.NativeAnalyzeStatusPB;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NativeAnalyzeStatus implements AnalyzeStatus, Writable {
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

    private boolean cancelled;

    @VisibleForTesting
    protected NativeAnalyzeStatus() {
    }

    public NativeAnalyzeStatus(long id, long dbId, long tableId, List<String> columns,
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

    @Override
    public long getId() {
        return id;
    }

    @Override
    public boolean isNative() {
        return true;
    }

    @Override
    public long getDbId() {
        return dbId;
    }

    @Override
    public long getTableId() {
        return tableId;
    }

    @Override
    public String getCatalogName() {
        return InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    }

    @Override
    public String getDbName() throws MetaNotFoundException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("No found database: " + dbId);
        }
        return db.getOriginName();
    }

    @Override
    public String getTableName() throws MetaNotFoundException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("No found database: " + dbId);
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (table == null) {
            throw new MetaNotFoundException("No found table: " + tableId);
        }
        return table.getName();
    }

    @Override
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public StatsConstants.AnalyzeType getType() {
        return type;
    }

    @Override
    public StatsConstants.ScheduleType getScheduleType() {
        return scheduleType;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public LocalDateTime getStartTime() {
        return startTime;
    }

    @Override
    public LocalDateTime getEndTime() {
        return endTime;
    }

    @Override
    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    @Override
    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    @Override
    public void setStatus(StatsConstants.ScheduleStatus status) {
        this.status = status;
    }

    @Override
    public StatsConstants.ScheduleStatus getStatus() {
        return status;
    }

    @Override
    public String getReason() {
        return reason;
    }

    @Override
    public void setReason(String reason) {
        this.reason = reason;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    @Override
    public long getProgress() {
        return progress;
    }

    @Override
    public void setProgress(long progress) {
        this.progress = progress;
    }

    @Override
    public ShowResultSet toShowResult() {
        String dbName;
        if (dbId == StatsConstants.DEFAULT_ALL_ID) {
            dbName = "*";
        } else {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            dbName = db.getOriginName();
        }
        String tableName;
        if (tableId == StatsConstants.DEFAULT_ALL_ID) {
            tableName = "*";
        } else {
            try {
                Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
                if (table == null) {
                    throw new SemanticException("Table %s is not found", tableId);
                }
                tableName = table.getName();
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
    public AnalyzeStatusPB toProto() {
        AnalyzeStatusCorePB.Builder core = AnalyzeStatusCorePB.newBuilder();
        core.setId(id);
        if (columns != null) {
            core.setColumns(ProtoLists.stringList(columns));
        }
        if (type != null) {
            core.setType(ProtoEnums.toProto(type, AnalyzeTypePB.class, "ANALYZE_"));
        }
        if (scheduleType != null) {
            core.setScheduleType(ProtoEnums.toProto(scheduleType, AnalyzeScheduleTypePB.class, "SCHEDULE_"));
        }
        if (properties != null) {
            core.setProperties(ProtoMaps.toStringMapProto(properties));
        }
        if (status != null) {
            core.setStatus(ProtoEnums.toProto(status, AnalyzeScheduleStatusPB.class, "STATUS_"));
        }
        if (startTime != null) {
            core.setStartTime(ProtoDateTimes.toEpochSecond(startTime));
        }
        if (endTime != null) {
            core.setEndTime(ProtoDateTimes.toEpochSecond(endTime));
        }
        if (reason != null) {
            core.setReason(reason);
        }
        core.setProgress(progress);
        NativeAnalyzeStatusPB.Builder identity = NativeAnalyzeStatusPB.newBuilder();
        identity.setDbId(dbId);
        identity.setTableId(tableId);
        return AnalyzeStatusPB.newBuilder().setKind(AnalyzeStatusPB.Kind.NATIVE)
                .setCore(core).setNative(identity).build();
    }

    private NativeAnalyzeStatus(AnalyzeStatusPB pb) {
        Preconditions.checkArgument(pb.getKind() == AnalyzeStatusPB.Kind.NATIVE && pb.hasCore()
                && pb.hasNative(), "missing or mismatched NativeAnalyzeStatus payload");
        AnalyzeStatusCorePB core = pb.getCore();
        if (core.hasId()) {
            id = core.getId();
        }
        if (core.hasColumns()) {
            columns = ProtoLists.toStrings(core.getColumns());
        }
        if (core.hasType()) {
            type = ProtoEnums.fromProto(core.getType(), StatsConstants.AnalyzeType.class, "ANALYZE_");
        }
        if (core.hasScheduleType()) {
            scheduleType = ProtoEnums.fromProto(core.getScheduleType(), StatsConstants.ScheduleType.class, "SCHEDULE_");
        }
        if (core.hasProperties()) {
            properties = ProtoMaps.fromStringMapProto(core.getProperties());
        }
        if (core.hasStatus()) {
            status = ProtoEnums.fromProto(core.getStatus(), StatsConstants.ScheduleStatus.class, "STATUS_");
        }
        if (core.hasStartTime()) {
            startTime = ProtoDateTimes.fromEpochSecond(core.getStartTime());
        }
        if (core.hasEndTime()) {
            endTime = ProtoDateTimes.fromEpochSecond(core.getEndTime());
        }
        if (core.hasReason()) {
            reason = core.getReason();
        }
        if (core.hasProgress()) {
            progress = core.getProgress();
        }
        NativeAnalyzeStatusPB identity = pb.getNative();
        if (identity.hasDbId()) {
            dbId = identity.getDbId();
        }
        if (identity.hasTableId()) {
            tableId = identity.getTableId();
        }
    }

    public static NativeAnalyzeStatus fromProto(AnalyzeStatusPB pb) {
        return new NativeAnalyzeStatus(pb);
    }
}
