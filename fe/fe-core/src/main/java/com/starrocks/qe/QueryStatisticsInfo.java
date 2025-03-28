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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/ProcService.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.proc.CurrentQueryInfoProvider;
import com.starrocks.common.util.QueryStatisticsFormatter;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.service.FrontendOptions;
import com.starrocks.thrift.TQueryStatisticsInfo;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryStatisticsInfo {
    private long queryStartTime;
    private String feIp;
    private String queryId;
    private String connId;
    private String db;
    private String user;
    private long cpuCostNs;
    private long scanBytes;
    private long scanRows;
    private long memUsageBytes;
    private long spillBytes;
    private long execTime;
    private String wareHouseName;
    private String customQueryId;
    private String resourceGroupName;

    public QueryStatisticsInfo() {
    }

    public QueryStatisticsInfo(long queryStartTime, String feIp, String queryId, String connId, String db, String user,
                               long cpuCostNs, long scanBytes, long scanRows, long memUsageBytes, long spillBytes,
                               long execTime, String wareHouseName, String customQueryId, String resourceGroupName) {
        this.queryStartTime = queryStartTime;
        this.feIp = feIp;
        this.queryId = queryId;
        this.connId = connId;
        this.db = db;
        this.user = user;
        this.cpuCostNs = cpuCostNs;
        this.scanBytes = scanBytes;
        this.scanRows = scanRows;
        this.memUsageBytes = memUsageBytes;
        this.spillBytes = spillBytes;
        this.execTime = execTime;
        this.wareHouseName = wareHouseName;
        this.customQueryId = customQueryId;
        this.resourceGroupName = resourceGroupName;
    }

    public long getQueryStartTime() {
        return queryStartTime;
    }

    public String getFeIp() {
        return feIp;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getConnId() {
        return connId;
    }

    public String getDb() {
        return db;
    }

    public String getUser() {
        return user;
    }

    public long getCpuCostNs() {
        return cpuCostNs;
    }

    public long getScanBytes() {
        return scanBytes;
    }

    public long getScanRows() {
        return scanRows;
    }

    public long getMemUsageBytes() {
        return memUsageBytes;
    }

    public long getSpillBytes() {
        return spillBytes;
    }

    public long getExecTime() {
        return execTime;
    }

    public String getWareHouseName() {
        return wareHouseName;
    }

    public String getResourceGroupName() {
        return resourceGroupName;
    }

    public String getCustomQueryId() {
        return customQueryId;
    }

    public QueryStatisticsInfo withQueryStartTime(long queryStartTime) {
        this.queryStartTime = queryStartTime;
        return this;
    }

    public QueryStatisticsInfo withFeIp(String feIp) {
        this.feIp = feIp;
        return this;
    }

    public QueryStatisticsInfo withQueryId(String queryId) {
        this.queryId = queryId;
        return this;
    }

    public QueryStatisticsInfo withConnId(String connId) {
        this.connId = connId;
        return this;
    }

    public QueryStatisticsInfo withDb(String db) {
        this.db = db;
        return this;
    }

    public QueryStatisticsInfo withUser(String user) {
        this.user = user;
        return this;
    }

    public QueryStatisticsInfo withCpuCostNs(long cpuCostNs) {
        this.cpuCostNs = cpuCostNs;
        return this;
    }

    public QueryStatisticsInfo withScanBytes(long scanBytes) {
        this.scanBytes = scanBytes;
        return this;
    }

    public QueryStatisticsInfo withScanRows(long scanRows) {
        this.scanRows = scanRows;
        return this;
    }

    public QueryStatisticsInfo withMemUsageBytes(long memUsageBytes) {
        this.memUsageBytes = memUsageBytes;
        return this;
    }

    public QueryStatisticsInfo withSpillBytes(long spillBytes) {
        this.spillBytes = spillBytes;
        return this;
    }

    public QueryStatisticsInfo withExecTime(long execTime) {
        this.execTime = execTime;
        return this;
    }

    public QueryStatisticsInfo withWareHouseName(String warehouseName) {
        this.wareHouseName = warehouseName;
        return this;
    }

    public QueryStatisticsInfo withResourceGroupName(String resourceGroupName) {
        this.resourceGroupName = resourceGroupName;
        return this;
    }

    public QueryStatisticsInfo withCustomQueryId(String customQueryId) {
        this.customQueryId = customQueryId;
        return this;
    }

    public TQueryStatisticsInfo toThrift() {
        return new TQueryStatisticsInfo()
                .setQueryStartTime(queryStartTime)
                .setFeIp(feIp)
                .setQueryId(queryId)
                .setConnId(connId)
                .setDb(db)
                .setUser(user)
                .setCpuCostNs(cpuCostNs)
                .setScanBytes(scanBytes)
                .setScanRows(scanRows)
                .setMemUsageBytes(memUsageBytes)
                .setSpillBytes(spillBytes)
                .setExecTime(execTime)
                .setWareHouseName(wareHouseName)
                .setCustomQueryId(customQueryId)
                .setResourceGroupName(resourceGroupName);
    }

    public static QueryStatisticsInfo fromThrift(TQueryStatisticsInfo tinfo) {
        return new QueryStatisticsInfo()
                .withQueryStartTime(tinfo.getQueryStartTime())
                .withFeIp(tinfo.getFeIp())
                .withQueryId(tinfo.getQueryId())
                .withConnId(tinfo.getConnId())
                .withDb(tinfo.getDb())
                .withUser(tinfo.getUser())
                .withScanBytes(tinfo.getScanBytes())
                .withScanRows(tinfo.getScanRows())
                .withMemUsageBytes(tinfo.getMemUsageBytes())
                .withSpillBytes(tinfo.getSpillBytes())
                .withCpuCostNs(tinfo.getCpuCostNs())
                .withExecTime(tinfo.getExecTime())
                .withWareHouseName(tinfo.getWareHouseName())
                .withCustomQueryId(tinfo.getCustomQueryId())
                .withResourceGroupName(tinfo.getResourceGroupName());
    }

    public List<String> formatToList() {
        final List<String> values = Lists.newArrayList();
        values.add(TimeUtils.longToTimeString(this.getQueryStartTime()));
        values.add(this.getFeIp());
        values.add(this.getQueryId());
        values.add(this.getConnId());
        values.add(this.getDb());
        values.add(this.getUser());
        values.add(QueryStatisticsFormatter.getBytes(this.getScanBytes()));
        values.add(QueryStatisticsFormatter.getRowsReturned(this.getScanRows()));
        values.add(QueryStatisticsFormatter.getBytes(this.getMemUsageBytes()));
        values.add(QueryStatisticsFormatter.getBytes(this.getSpillBytes()));
        values.add(QueryStatisticsFormatter.getSecondsFromNano(this.getCpuCostNs()));
        values.add(QueryStatisticsFormatter.getSecondsFromMilli(this.getExecTime()));
        values.add(this.getWareHouseName());
        values.add(this.getCustomQueryId());
        values.add(this.getResourceGroupName());
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryStatisticsInfo that = (QueryStatisticsInfo) o;
        return queryStartTime == that.queryStartTime && Objects.equals(feIp, that.feIp) &&
                Objects.equals(queryId, that.queryId) && Objects.equals(connId, that.connId) &&
                Objects.equals(db, that.db) && Objects.equals(user, that.user) && cpuCostNs == that.cpuCostNs &&
                scanBytes == that.scanBytes && scanRows == that.scanRows && memUsageBytes == that.memUsageBytes &&
                spillBytes == that.spillBytes && execTime == that.execTime &&
                Objects.equals(wareHouseName, that.wareHouseName) && Objects.equals(customQueryId, that.customQueryId) &&
                Objects.equals(resourceGroupName, that.resourceGroupName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryStartTime, feIp, queryId, connId, db, user, cpuCostNs, scanBytes, scanRows, memUsageBytes,
                spillBytes, execTime, wareHouseName, customQueryId, resourceGroupName);
    }

    @Override
    public String toString() {
        return "QueryStatisticsInfo{" +
                "queryStartTime='" + queryStartTime + '\'' +
                ", feIp=" + feIp +
                ", queryId=" + queryId +
                ", connId=" + connId +
                ", db=" + db +
                ", user=" + user +
                ", cpuCostNs=" + cpuCostNs +
                ", scanRows=" + scanBytes +
                ", memUsageBytes=" + memUsageBytes +
                ", spillBytes=" + spillBytes +
                ", execTime=" + execTime +
                ", wareHouseName=" + wareHouseName +
                ", customQueryId=" + customQueryId +
                ", resourceGroupName=" + resourceGroupName +
                '}';
    }

    public static List<QueryStatisticsInfo> makeListFromMetricsAndMgrs() throws AnalysisException {
        final Map<String, QueryStatisticsItem> statistic =
                QeProcessorImpl.INSTANCE.getQueryStatistics();
        final List<QueryStatisticsInfo> sortedRowData = Lists.newArrayList();

        final CurrentQueryInfoProvider provider = new CurrentQueryInfoProvider();
        final Map<String, CurrentQueryInfoProvider.QueryStatistics> statisticsMap
                = provider.getQueryStatistics(statistic.values());
        final List<QueryStatisticsItem> sorted =
                statistic.values().stream()
                        .sorted(Comparator.comparingLong(QueryStatisticsItem::getQueryStartTime))
                        .collect(Collectors.toList());
        for (QueryStatisticsItem item : sorted) {
            final CurrentQueryInfoProvider.QueryStatistics statistics = statisticsMap.get(item.getQueryId());

            QueryStatisticsInfo info = new QueryStatisticsInfo()
                    .withQueryStartTime(item.getQueryStartTime())
                    .withFeIp(FrontendOptions.getLocalHostAddress())
                    .withQueryId(item.getQueryId())
                    .withConnId(item.getConnId())
                    .withDb(item.getDb())
                    .withUser(item.getUser())
                    .withExecTime(item.getQueryExecTime())
                    .withWareHouseName(item.getWarehouseName())
                    .withCustomQueryId(item.getCustomQueryId())
                    .withResourceGroupName(item.getResourceGroupName());
            if (statistics != null) {
                info.withScanBytes(statistics.getScanBytes())
                        .withScanRows(statistics.getScanRows())
                        .withMemUsageBytes(statistics.getMemUsageBytes())
                        .withSpillBytes(statistics.getSpillBytes())
                        .withCpuCostNs(statistics.getCpuCostNs());
            }
            sortedRowData.add(info);
        }

        return sortedRowData;
    }
}
