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

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants.AnalyzeType;
import com.starrocks.statistic.StatsConstants.ScheduleStatus;
import com.starrocks.statistic.StatsConstants.ScheduleType;
import org.apache.commons.collections.CollectionUtils;

import java.io.DataInput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

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
        return isAnalyzeAllDb() && isAnalyzeAllTable() && getScheduleType() == ScheduleType.SCHEDULE;
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

        boolean hasFailedCollectJob = false;
        for (StatisticsCollectJob statsJob : jobs) {
            if (!StatisticAutoCollector.checkoutAnalyzeTime()) {
                break;
            }
            AnalyzeStatus analyzeStatus = new NativeAnalyzeStatus(GlobalStateMgr.getCurrentState().getNextId(),
                    statsJob.getDb().getId(), statsJob.getTable().getId(), statsJob.getColumnNames(), statsJob.getAnalyzeType(),
                    statsJob.getScheduleType(), statsJob.getProperties(), LocalDateTime.now());
            analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FAILED);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);

            statisticExecutor.collectStatistics(statsConnectContext, statsJob, analyzeStatus, true);
            if (analyzeStatus.getStatus().equals(StatsConstants.ScheduleStatus.FAILED)) {
                setStatus(StatsConstants.ScheduleStatus.FAILED);
                setWorkTime(LocalDateTime.now());
                setReason(analyzeStatus.getReason());
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithLog(this);
                hasFailedCollectJob = true;
                break;
            }
        }

        if (!hasFailedCollectJob) {
            setStatus(ScheduleStatus.FINISH);
            setWorkTime(LocalDateTime.now());
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithLog(this);
        }
    }

    public static NativeAnalyzeJob read(DataInput in) throws IOException {
        String s = Text.readString(in);
        return GsonUtils.GSON.fromJson(s, NativeAnalyzeJob.class);
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
}
