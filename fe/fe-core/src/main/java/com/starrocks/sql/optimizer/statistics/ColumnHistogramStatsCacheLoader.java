// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.statistics;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

public class ColumnHistogramStatsCacheLoader implements AsyncCacheLoader<ColumnStatsCacheKey, Optional<Histogram>> {
    private static final Logger LOG = LogManager.getLogger(ColumnBasicStatsCacheLoader.class);
    private final StatisticExecutor statisticExecutor = new StatisticExecutor();

    @Override
    public @NonNull CompletableFuture<Optional<Histogram>> asyncLoad(@NonNull ColumnStatsCacheKey cacheKey,
                                                                     @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<TStatisticData> statisticData =
                        queryHistogramStatistics(cacheKey.tableId, Lists.newArrayList(cacheKey.column));
                // check TStatisticData is not empty, There may be no such column Statistics in BE

                if (!statisticData.isEmpty()) {
                    List<Bucket> buckets = convert(statisticData.get(0).histogram);
                    Histogram histogram = new Histogram(buckets);
                    return Optional.of(histogram);
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
                }
                List<TStatisticData> histogramStatsDataList = queryHistogramStatistics(tableId, columns);
                for (TStatisticData histogramStatsData : histogramStatsDataList) {
                    List<Bucket> buckets = convert(histogramStatsData.histogram);
                    Histogram histogram = new Histogram(buckets);
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

    public List<TStatisticData> queryHistogramStatistics(long tableId, List<String> column) throws Exception {
        //TODO: return statisticExecutor.queryHistogram(tableId, column);
        return null;
    }

    List<Bucket> convert(String histogramString) {
        JsonObject jsonObject = JsonParser.parseString(histogramString).getAsJsonObject();
        JsonArray histogramObj = jsonObject.getAsJsonArray("buckets");

        List<Bucket> buckets = Lists.newArrayList();
        for (int i = 0; i < histogramObj.size(); ++i) {
            JsonArray bucketJsonArray = histogramObj.get(i).getAsJsonArray();
            Bucket bucket = new Bucket(bucketJsonArray.get(0).getAsString(),
                    bucketJsonArray.get(1).getAsString(),
                    Long.parseLong(bucketJsonArray.get(2).getAsString()),
                    Long.parseLong(bucketJsonArray.get(3).getAsString()));
            buckets.add(bucket);
        }
        return buckets;
    }
}
