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

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.image.ProtoDateTimes;
import com.starrocks.persist.image.ProtoEnums;
import com.starrocks.persist.image.ProtoLists;
import com.starrocks.persist.image.ProtoMaps;
import com.starrocks.persist.proto.AnalyzeJobCorePB;
import com.starrocks.persist.proto.AnalyzeJobPB;
import com.starrocks.persist.proto.AnalyzeScheduleStatusPB;
import com.starrocks.persist.proto.AnalyzeScheduleTypePB;
import com.starrocks.persist.proto.AnalyzeTypePB;
import com.starrocks.persist.proto.ExternalAnalyzeJobPB;
import com.starrocks.persist.proto.TypeListPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants.AnalyzeType;
import com.starrocks.statistic.StatsConstants.ScheduleStatus;
import com.starrocks.statistic.StatsConstants.ScheduleType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeProtoConverter;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExternalAnalyzeJob implements AnalyzeJob, Writable {
    @SerializedName("id")
    private long id;

    @SerializedName("catalogName")
    private String catalogName;

    @SerializedName("dbName")
    private String dbName;

    @SerializedName("tableName")
    private String tableName;

    // Empty is all column
    @SerializedName("columns")
    private List<String> columns;

    @SerializedName("columnTypes")
    private List<Type> columnTypes;

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

    public ExternalAnalyzeJob(String catalogName, String dbName, String tableName, List<String> columnNames,
                              List<Type> columnTypes, AnalyzeType type,
                              ScheduleType scheduleType, Map<String, String> properties, ScheduleStatus status,
                              LocalDateTime workTime) {
        this.id = -1;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.columns = columnNames;
        this.columnTypes = columnTypes;
        this.type = type;
        this.scheduleType = scheduleType;
        this.properties = properties;
        this.status = status;
        this.workTime = workTime;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public boolean isNative() {
        return false;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public void setId(long id) {
        this.id = id;
    }

    @Override
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public AnalyzeType getAnalyzeType() {
        return type;
    }

    @Override
    public ScheduleType getScheduleType() {
        return scheduleType;
    }

    @Override
    public LocalDateTime getWorkTime() {
        return workTime;
    }

    @Override
    public void setWorkTime(LocalDateTime workTime) {
        this.workTime = workTime;
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
    public ScheduleStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(ScheduleStatus status) {
        this.status = status;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean isAnalyzeAllDb() {
        return dbName == null;
    }

    @Override
    public boolean isAnalyzeAllTable() {
        return tableName == null;
    }

    @Override
    public List<StatisticsCollectJob> instantiateJobs() {
        return StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(this);
    }

    @Override
    public void run(ConnectContext statsConnectContext, StatisticExecutor statisticExecutor,
                    List<StatisticsCollectJob> jobs) {
        setStatus(StatsConstants.ScheduleStatus.RUNNING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithoutLog(this);

        AnalyzeStatus firstFailure = null;
        for (StatisticsCollectJob statsJob : jobs) {
            if (!StatisticAutoCollector.checkoutAnalyzeTime()) {
                break;
            }
            AnalyzeStatus analyzeStatus = new ExternalAnalyzeStatus(GlobalStateMgr.getCurrentState().getNextId(),
                    statsJob.getCatalogName(), statsJob.getDb().getFullName(), statsJob.getTable().getName(),
                    statsJob.getTable().getUUID(), statsJob.getColumnNames(), statsJob.getAnalyzeType(),
                    statsJob.getScheduleType(), statsJob.getProperties(), LocalDateTime.now());
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);

            statisticExecutor.collectStatistics(statsConnectContext, statsJob, analyzeStatus, true, true /* resetWarehouse */);
            if (firstFailure == null && analyzeStatus.getStatus().equals(StatsConstants.ScheduleStatus.FAILED)) {
                firstFailure = analyzeStatus;
            }
            if (analyzeStatus.isCancelled()) {
                break;
            }
        }

        if (firstFailure == null) {
            setStatus(ScheduleStatus.FINISH);
        } else {
            setStatus(ScheduleStatus.FAILED);
            setReason(firstFailure.getReason());
        }
        setWorkTime(LocalDateTime.now());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithLog(this);
    }

    @Override
    public String toString() {
        return "ExternalAnalyzeJob{" +
                "id=" + id +
                ", dbName=" + dbName +
                ", tableName=" + tableName +
                ", columns=" + columns +
                ", type=" + type +
                ", scheduleType=" + scheduleType +
                ", properties=" + properties +
                ", status=" + status +
                ", workTime=" + workTime +
                ", reason='" + reason + '\'' +
                '}';
    }

    @Override
    public AnalyzeJobPB toProto() {
        AnalyzeJobCorePB.Builder core = AnalyzeJobCorePB.newBuilder();
        core.setId(id);
        if (columns != null) {
            core.setColumns(ProtoLists.stringList(columns));
        }
        if (columnTypes != null) {
            core.setColumnTypes(TypeListPB.newBuilder().addAllValues(columnTypes.stream()
                    .map(TypeProtoConverter::toProto).collect(Collectors.toList())).build());
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
        if (workTime != null) {
            core.setWorkTime(ProtoDateTimes.toEpochSecond(workTime));
        }
        if (reason != null) {
            core.setReason(reason);
        }
        ExternalAnalyzeJobPB.Builder identity = ExternalAnalyzeJobPB.newBuilder();
        if (catalogName != null) {
            identity.setCatalogName(catalogName);
        }
        if (dbName != null) {
            identity.setDbName(dbName);
        }
        if (tableName != null) {
            identity.setTableName(tableName);
        }
        return AnalyzeJobPB.newBuilder().setKind(AnalyzeJobPB.Kind.EXTERNAL)
                .setCore(core).setExternal(identity).build();
    }

    private ExternalAnalyzeJob(AnalyzeJobPB pb) {
        Preconditions.checkArgument(pb.getKind() == AnalyzeJobPB.Kind.EXTERNAL && pb.hasCore()
                && pb.hasExternal(), "missing or mismatched ExternalAnalyzeJob payload");
        AnalyzeJobCorePB core = pb.getCore();
        if (core.hasId()) {
            id = core.getId();
        }
        if (core.hasColumns()) {
            columns = ProtoLists.toStrings(core.getColumns());
        }
        if (core.hasColumnTypes()) {
            columnTypes = core.getColumnTypes().getValuesList().stream()
                    .map(TypeProtoConverter::fromProto).collect(Collectors.toList());
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
        if (core.hasWorkTime()) {
            workTime = ProtoDateTimes.fromEpochSecond(core.getWorkTime());
        }
        if (core.hasReason()) {
            reason = core.getReason();
        }
        ExternalAnalyzeJobPB identity = pb.getExternal();
        if (identity.hasCatalogName()) {
            catalogName = identity.getCatalogName();
        }
        if (identity.hasDbName()) {
            dbName = identity.getDbName();
        }
        if (identity.hasTableName()) {
            tableName = identity.getTableName();
        }
    }

    public static ExternalAnalyzeJob fromProto(AnalyzeJobPB pb) {
        return new ExternalAnalyzeJob(pb);
    }
}
