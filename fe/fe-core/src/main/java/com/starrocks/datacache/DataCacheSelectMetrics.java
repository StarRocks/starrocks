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

package com.starrocks.datacache;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.monitor.unit.TimeValue;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DataCacheSelectMetrics {
    private static final ShowResultSetMetaData SIMPLE_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("STATUS", ScalarType.createVarcharType()))
            .addColumn(new Column("ALREADY_CACHED_SIZE", ScalarType.createVarcharType()))
            .addColumn(new Column("WRITE_CACHE_SIZE", ScalarType.createVarcharType()))
            .addColumn(new Column("AVG_WRITE_CACHE_TIME", ScalarType.createVarcharType()))
            .addColumn(new Column("TOTAL_CACHE_USAGE", ScalarType.createVarcharType()))
            .build();

    private static final ShowResultSetMetaData VERBOSE_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("BE_IP", ScalarType.createVarcharType()))
            .addColumn(new Column("STATUS", ScalarType.createVarcharType()))
            .addColumn(new Column("ALREADY_CACHED_SIZE", ScalarType.createVarcharType()))
            .addColumn(new Column("AVG_READ_CACHE_TIME", ScalarType.createVarcharType()))
            .addColumn(new Column("WRITE_CACHE_SIZE", ScalarType.createVarcharType()))
            .addColumn(new Column("AVG_WRITE_CACHE_TIME", ScalarType.createVarcharType()))
            .addColumn(new Column("TOTAL_CACHE_USAGE", ScalarType.createVarcharType()))
            .build();

    private final Map<Long, LoadDataCacheMetrics> beMetrics = new HashMap<>();

    public void updateLoadDataCacheMetrics(long backendId, LoadDataCacheMetrics metrics) {
        beMetrics.merge(backendId, metrics, LoadDataCacheMetrics::mergeMetrics);
    }

    public ShowResultSet getShowResultSet(boolean isVerbose) {
        if (isVerbose) {
            return getVerboseShowResultSet();
        } else {
            return getSimpleShowResultSet();
        }
    }

    public Map<Long, LoadDataCacheMetrics> getBeMetrics() {
        return beMetrics;
    }

    private ShowResultSet getVerboseShowResultSet() {
        final SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<List<String>> rows = Lists.newArrayList();
        for (Map.Entry<Long, LoadDataCacheMetrics> entry : beMetrics.entrySet()) {
            List<String> row = Lists.newArrayList();
            LoadDataCacheMetrics metrics = entry.getValue();

            long backendId = entry.getKey();
            Backend backend = clusterInfoService.getBackend(backendId);
            if (backend == null) {
                row.add("N/A");
            } else {
                row.add(backend.getIP());
            }

            row.add("SUCCESS");

            row.add(metrics.getReadBytes().toString());
            row.add(new TimeValue(metrics.getReadTimeNs().getNanos() / metrics.getCount(),
                    TimeUnit.NANOSECONDS).toString());
            row.add(metrics.getWriteBytes().toString());
            row.add(new TimeValue(metrics.getWriteTimeNs().getNanos() / metrics.getCount(),
                    TimeUnit.NANOSECONDS).toString());
            row.add(String.format("%.2f%%", metrics.getLastDataCacheMetrics().getCacheUsage() * 100));

            rows.add(row);
        }
        return new ShowResultSet(VERBOSE_META_DATA, rows);
    }

    private ShowResultSet getSimpleShowResultSet() {
        long alreadyCachedSize = 0;
        long writeCacheSize = 0;
        long writeCacheTime = 0;
        long totalCount = 0;
        long totalCacheSize = 0;
        long totalUsedCacheSize = 0;

        for (Map.Entry<Long, LoadDataCacheMetrics> entry : beMetrics.entrySet()) {
            LoadDataCacheMetrics metrics = entry.getValue();
            alreadyCachedSize += metrics.getReadBytes().getBytes();
            writeCacheSize += metrics.getWriteBytes().getBytes();
            writeCacheTime += metrics.getWriteTimeNs().getNanos();
            totalCount += metrics.getCount();
            totalCacheSize += metrics.getLastDataCacheMetrics().getDiskQuotaBytes().getBytes() +
                    metrics.getLastDataCacheMetrics().getMemQuoteBytes().getBytes();
            totalUsedCacheSize += metrics.getLastDataCacheMetrics().getDiskUsedBytes().getBytes() +
                    metrics.getLastDataCacheMetrics().getMemUsedBytes().getBytes();
        }

        List<List<String>> rows = Lists.newArrayList();
        List<String> row = Lists.newArrayList();
        rows.add(row);

        row.add("SUCCESS");
        row.add(new ByteSizeValue(alreadyCachedSize).toString());
        row.add(new ByteSizeValue(writeCacheSize).toString());

        // get avg write cache time
        long avgWriteCacheTime = writeCacheTime / totalCount;
        row.add(new TimeValue(avgWriteCacheTime, TimeUnit.NANOSECONDS).toString());

        row.add(String.format("%.2f%%", ((double) totalUsedCacheSize / totalCacheSize) * 100));
        return new ShowResultSet(SIMPLE_META_DATA, rows);
    }

    @Override
    public String toString() {
        long alreadyCachedSize = 0;
        long readCacheTime = 0;
        long writeCacheSize = 0;
        long writeCacheTime = 0;
        long totalCount = 0;
        long totalCacheSize = 0;
        long totalUsedCacheSize = 0;

        for (Map.Entry<Long, LoadDataCacheMetrics> entry : beMetrics.entrySet()) {
            LoadDataCacheMetrics metrics = entry.getValue();
            alreadyCachedSize += metrics.getReadBytes().getBytes();
            readCacheTime += metrics.getReadTimeNs().getNanos();
            writeCacheSize += metrics.getWriteBytes().getBytes();
            writeCacheTime += metrics.getWriteTimeNs().getNanos();
            totalCount += metrics.getCount();
            totalCacheSize += metrics.getLastDataCacheMetrics().getDiskQuotaBytes().getBytes() +
                    metrics.getLastDataCacheMetrics().getMemQuoteBytes().getBytes();
            totalUsedCacheSize += metrics.getLastDataCacheMetrics().getDiskUsedBytes().getBytes() +
                    metrics.getLastDataCacheMetrics().getMemUsedBytes().getBytes();
        }

        // get avg read/write cache time
        long avgReadCacheTime = readCacheTime / totalCount;
        long avgWriteCacheTime = writeCacheTime / totalCount;

        return String.format(
                "AlreadyCachedSize: %s, AvgReadCacheTime: %s, WriteCacheSize: %s, AvgWriteCacheTime: %s, TotalCacheUsage: %.2f%%",
                new ByteSizeValue(alreadyCachedSize), new TimeValue(avgReadCacheTime, TimeUnit.NANOSECONDS),
                new ByteSizeValue(writeCacheSize), new TimeValue(avgWriteCacheTime, TimeUnit.NANOSECONDS),
                ((double) totalUsedCacheSize / totalCacheSize) * 100);
    }
}
