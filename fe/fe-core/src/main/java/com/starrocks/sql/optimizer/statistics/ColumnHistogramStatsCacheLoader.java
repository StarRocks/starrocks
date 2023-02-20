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

package com.starrocks.sql.optimizer.statistics;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

public class ColumnHistogramStatsCacheLoader implements AsyncCacheLoader<ColumnStatsCacheKey, Optional<Histogram>> {
    private static final Logger LOG = LogManager.getLogger(ColumnBasicStatsCacheLoader.class);
    private final StatisticExecutor statisticExecutor = new StatisticExecutor();

    @Override
    public @NonNull
    CompletableFuture<Optional<Histogram>> asyncLoad(@NonNull ColumnStatsCacheKey cacheKey,
                                                     @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();
                List<TStatisticData> statisticData =
                        queryHistogramStatistics(connectContext, cacheKey.tableId, Lists.newArrayList(cacheKey.column));
                // check TStatisticData is not empty, There may be no such column Statistics in BE
                if (!statisticData.isEmpty()) {
                    return Optional.of(convert2Histogram(statisticData.get(0)));
                } else {
                    return Optional.empty();
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Map<@NonNull ColumnStatsCacheKey, @NonNull Optional<Histogram>>> asyncLoadAll(
            @NonNull Iterable<? extends @NonNull ColumnStatsCacheKey> keys, @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            Map<ColumnStatsCacheKey, Optional<Histogram>> result = new HashMap<>();
            try {
                long tableId = -1;
                List<String> columns = new ArrayList<>();
                for (ColumnStatsCacheKey key : keys) {
                    tableId = key.tableId;
                    columns.add(key.column);
                    result.put(key, Optional.empty());
                }
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();

                List<TStatisticData> histogramStatsDataList = queryHistogramStatistics(connectContext, tableId, columns);
                for (TStatisticData histogramStatsData : histogramStatsDataList) {
                    Histogram histogram = convert2Histogram(histogramStatsData);
                    result.put(new ColumnStatsCacheKey(histogramStatsData.tableId, histogramStatsData.columnName),
                            Optional.of(histogram));
                }

                return result;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Optional<Histogram>> asyncReload(
            @NonNull ColumnStatsCacheKey key, @NonNull Optional<Histogram> oldValue,
            @NonNull Executor executor) {
        return asyncLoad(key, executor);
    }

    public List<TStatisticData> queryHistogramStatistics(ConnectContext context, long tableId, List<String> column) {
        return statisticExecutor.queryHistogram(context, tableId, column);
    }

    private Histogram convert2Histogram(TStatisticData statisticData) throws AnalysisException {
        Database db = GlobalStateMgr.getCurrentState().getDb(statisticData.dbId);
        MetaUtils.checkDbNullAndReport(db, String.valueOf(statisticData.dbId));
        Table table = db.getTable(statisticData.tableId);
        if (!(table instanceof OlapTable)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, statisticData.tableId);
        }
        Column column = table.getColumn(statisticData.columnName);
        if (column == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, statisticData.columnName);
        }

        List<Bucket> buckets = convertBuckets(statisticData.histogram, column.getType());
        Map<String, Long> mcv = convertMCV(statisticData.histogram);
        return new Histogram(buckets, mcv);
    }

    private List<Bucket> convertBuckets(String histogramString, Type type) {
        JsonObject jsonObject = JsonParser.parseString(histogramString).getAsJsonObject();

        JsonElement jsonElement = jsonObject.get("buckets");
        if (jsonElement.isJsonNull()) {
            return Collections.emptyList();
        }

        JsonArray histogramObj = (JsonArray) jsonElement;
        List<Bucket> buckets = Lists.newArrayList();
        for (int i = 0; i < histogramObj.size(); ++i) {
            JsonArray bucketJsonArray = histogramObj.get(i).getAsJsonArray();

            double low;
            double high;
            if (type.isDate()) {
                low = (double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                        bucketJsonArray.get(0).getAsString(), DateUtils.DATE_FORMATTER_UNIX));
                high = (double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                        bucketJsonArray.get(1).getAsString(), DateUtils.DATE_FORMATTER_UNIX));
            } else if (type.isDatetime()) {
                low = (double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                        bucketJsonArray.get(0).getAsString(), DateUtils.DATE_TIME_FORMATTER_UNIX));
                high = (double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                        bucketJsonArray.get(1).getAsString(), DateUtils.DATE_TIME_FORMATTER_UNIX));
            } else {
                low = Double.parseDouble(bucketJsonArray.get(0).getAsString());
                high = Double.parseDouble(bucketJsonArray.get(1).getAsString());
            }

            Bucket bucket = new Bucket(low, high,
                    Long.parseLong(bucketJsonArray.get(2).getAsString()),
                    Long.parseLong(bucketJsonArray.get(3).getAsString()));
            buckets.add(bucket);
        }
        return buckets;
    }

    private Map<String, Long> convertMCV(String histogramString) {
        JsonObject jsonObject = JsonParser.parseString(histogramString).getAsJsonObject();
        JsonElement jsonElement = jsonObject.get("mcv");
        if (jsonElement.isJsonNull()) {
            return Collections.emptyMap();
        }

        JsonArray histogramObj = (JsonArray) jsonElement;
        Map<String, Long> mcv = new HashMap<>();
        for (int i = 0; i < histogramObj.size(); ++i) {
            JsonArray bucketJsonArray = histogramObj.get(i).getAsJsonArray();
            mcv.put(bucketJsonArray.get(0).getAsString(), Long.parseLong(bucketJsonArray.get(1).getAsString()));
        }
        return mcv;
    }
}
