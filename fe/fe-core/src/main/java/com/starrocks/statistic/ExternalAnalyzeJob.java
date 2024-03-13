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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants.AnalyzeType;
import com.starrocks.statistic.StatsConstants.ScheduleStatus;
import com.starrocks.statistic.StatsConstants.ScheduleType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

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

    public ExternalAnalyzeJob(String catalogName, String dbName, String tableName, List<String> columns, AnalyzeType type,
                              ScheduleType scheduleType, Map<String, String> properties, ScheduleStatus status,
                              LocalDateTime workTime) {
        this.id = -1;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.columns = columns;
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
    public void run(ConnectContext statsConnectContext, StatisticExecutor statisticExecutor) {
        setStatus(StatsConstants.ScheduleStatus.RUNNING);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithoutLog(this);
        List<StatisticsCollectJob> statisticsCollectJobList =
                StatisticsCollectJobFactory.buildExternalStatisticsCollectJob(this);

        boolean hasFailedCollectJob = false;
        for (StatisticsCollectJob statsJob : statisticsCollectJobList) {
            AnalyzeStatus analyzeStatus = new ExternalAnalyzeStatus(GlobalStateMgr.getCurrentState().getNextId(),
                    statsJob.getCatalogName(), statsJob.getDb().getFullName(), statsJob.getTable().getName(),
                    statsJob.getTable().getUUID(), statsJob.getColumns(), statsJob.getType(), statsJob.getScheduleType(),
                    statsJob.getProperties(), LocalDateTime.now());
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
            setStatus(StatsConstants.ScheduleStatus.PENDING);
            setWorkTime(LocalDateTime.now());
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().updateAnalyzeJobWithLog(this);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String s = GsonUtils.GSON.toJson(this);
        Text.writeString(out, s);
    }

    public static ExternalAnalyzeJob read(DataInput in) throws IOException {
        String s = Text.readString(in);
        return GsonUtils.GSON.fromJson(s, ExternalAnalyzeJob.class);
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
}
