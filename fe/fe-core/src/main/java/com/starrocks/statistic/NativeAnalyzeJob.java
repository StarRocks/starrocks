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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.MetaNotFoundException;
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
import com.starrocks.persist.proto.NativeAnalyzeJobPB;
import com.starrocks.persist.proto.TypeListPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants.AnalyzeType;
import com.starrocks.statistic.StatsConstants.ScheduleStatus;
import com.starrocks.statistic.StatsConstants.ScheduleType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeProtoConverter;
import org.apache.commons.collections.CollectionUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatisticAutoCollector.DEFAULT_JOB_FLAG;

public class NativeAnalyzeJob implements AnalyzeJob, Writable {

    @SerializedName("id")
    private long id;

    @SerializedName("dbId")
    private long dbId;

    @SerializedName("tableId")
    private long tableId;

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

    public NativeAnalyzeJob(long dbId, long tableId, List<String> columns, List<Type> columnTypes, AnalyzeType type,
                            ScheduleType scheduleType, Map<String, String> properties, ScheduleStatus status,
                            LocalDateTime workTime) {
        this.id = -1;
        this.dbId = dbId;
        this.tableId = tableId;
        this.columns = columns;
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
        return true;
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
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            throw new MetaNotFoundException("No found table: " + tableId);
        }
        return table.getName();
    }

    @Override
    public void setId(long id) {
        this.id = id;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
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

    public void setType(AnalyzeType type) {
        this.type = type;
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
        return dbId == StatsConstants.DEFAULT_ALL_ID;
    }

    @Override
    public boolean isAnalyzeAllTable() {
        return tableId == StatsConstants.DEFAULT_ALL_ID;
    }

    public boolean isDefaultJob() {
        return isAnalyzeAllDb() && isAnalyzeAllTable() && getScheduleType() == ScheduleType.SCHEDULE &&
                getProperties() != null && getProperties().containsKey(DEFAULT_JOB_FLAG);
    }

    @Override
    public List<StatisticsCollectJob> instantiateJobs() {
        return StatisticsCollectJobFactory.buildStatisticsCollectJob(this);
    }

    @Override
    public void run(ConnectContext statsConnectContext, StatisticExecutor statisticExecutor,
                    List<StatisticsCollectJob> jobs) {
        if (CollectionUtils.isEmpty(jobs)) {
            return;
        }
        setStatus(StatsConstants.ScheduleStatus.RUNNING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithoutLog(this);

        AnalyzeStatus firstFailure = null;
        for (StatisticsCollectJob statsJob : jobs) {
            if (!StatisticAutoCollector.checkoutAnalyzeTime()) {
                break;
            }
            AnalyzeStatus analyzeStatus = new NativeAnalyzeStatus(GlobalStateMgr.getCurrentState().getNextId(),
                    statsJob.getDb().getId(), statsJob.getTable().getId(), statsJob.getColumnNames(), statsJob.getAnalyzeType(),
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
            setReason("");
        } else {
            setStatus(ScheduleStatus.FAILED);
            setReason(firstFailure.getReason());
        }
        setWorkTime(LocalDateTime.now());
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithLog(this);
    }

    @Override
    public String toString() {
        return "NativeAnalyzeJob{" +
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
        NativeAnalyzeJobPB.Builder identity = NativeAnalyzeJobPB.newBuilder();
        identity.setDbId(dbId);
        identity.setTableId(tableId);
        return AnalyzeJobPB.newBuilder().setKind(AnalyzeJobPB.Kind.NATIVE)
                .setCore(core).setNative(identity).build();
    }

    private NativeAnalyzeJob(AnalyzeJobPB pb) {
        Preconditions.checkArgument(pb.getKind() == AnalyzeJobPB.Kind.NATIVE && pb.hasCore()
                && pb.hasNative(), "missing or mismatched NativeAnalyzeJob payload");
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
        NativeAnalyzeJobPB identity = pb.getNative();
        if (identity.hasDbId()) {
            dbId = identity.getDbId();
        }
        if (identity.hasTableId()) {
            tableId = identity.getTableId();
        }
    }

    public static NativeAnalyzeJob fromProto(AnalyzeJobPB pb) {
        return new NativeAnalyzeJob(pb);
    }
}
