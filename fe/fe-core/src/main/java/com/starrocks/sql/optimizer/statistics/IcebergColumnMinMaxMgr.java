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
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.memory.estimate.Estimator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class IcebergColumnMinMaxMgr implements IMinMaxStatsMgr, MemoryTrackable {
    private record CacheKey(ColumnIdentifier id, StatsVersion version) {}

    private static final Logger LOG = LogManager.getLogger(IcebergColumnMinMaxMgr.class);

    private static final SimpleExecutor SIMPLE_EXECUTOR =
            new SimpleExecutor("LakeColumnMinMaxMgr", TResultSinkType.HTTP_PROTOCAL);

    private static final IcebergColumnMinMaxMgr INSTANCE = new IcebergColumnMinMaxMgr();

    private final AsyncLoadingCache<CacheKey, Optional<ColumnMinMax>> cache = Caffeine.newBuilder()
            .maximumSize(Config.statistic_dict_columns)
            .executor(ThreadPoolManager.getStatsCacheThreadPoolForLake())
            .buildAsync(new CacheLoader());

    static IcebergColumnMinMaxMgr getInstance() {
        return INSTANCE;
    }

    @Override
    public Optional<ColumnMinMax> getStats(ColumnIdentifier identifier, StatsVersion version) {
        if (!ConnectContext.get().getSessionVariable().isEnableMinMaxOptimization()) {
            return Optional.empty();
        }

        try {
            CompletableFuture<Optional<ColumnMinMax>> result = cache.get(new CacheKey(identifier, version));
            if (result.isDone()) {
                return result.get();
            }
        } catch (Exception e) {
            LOG.warn("Failed to get MinMax for column: {}, version: {}", identifier, version, e);
        }
        return Optional.empty();
    }

    @Override
    public void removeStats(ColumnIdentifier identifier, StatsVersion version) {
        throw new SemanticException("Iceberg column min/max stats unsupported the method");
    }

    @Override
    public Map<String, Long> estimateCount() {
        return Map.of("IcebergMinMax", (long) cache.asMap().size());
    }

    @Override
    public long estimateSize() {
        return Estimator.estimate(cache.asMap(), 20);
    }

    private static final class CacheLoader implements AsyncCacheLoader<CacheKey, Optional<ColumnMinMax>> {
        @Override
        public @NonNull CompletableFuture<Optional<ColumnMinMax>> asyncLoad(@NonNull CacheKey key,
                                                                            @NonNull Executor executor) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    List<String> names = StatisticsUtils.getTableNameByUUID(key.id.getTableUuid());
                    if (names.size() < 3) {
                        LOG.warn("tableUUID {} error for collecting min/max", key.id.getTableUuid());
                        return Optional.empty();
                    }

                    String columnName = key.id.getColumnName().toSql(true);
                    String tableName = StatisticUtils.quoting(names.get(0), names.get(1), names.get(2));

                    String sql = "select min(" + columnName + ") as min, max(" + columnName + ") as max " +
                            " from " + tableName + " VERSION AS OF " + key.version.getVersion();

                    ConnectContext context = SIMPLE_EXECUTOR.createConnectContext();
                    context.getSessionVariable().setPipelineDop(1);
                    context.getSessionVariable().setIsEnableMinMaxOptimization(true);
                    List<TResultBatch> result = SIMPLE_EXECUTOR.executeDQL(sql, context);
                    if (result.isEmpty()) {
                        return Optional.empty();
                    }
                    Preconditions.checkState(result.size() == 1);
                    return Optional.of(toMinMax(result.get(0)));
                } catch (Exception e) {
                    // ignore
                }
                return Optional.empty();
            }, executor);
        }

        private static ColumnMinMax toMinMax(TResultBatch result) {
            ByteBuffer buffer = result.getRows().get(0);
            ByteBuf copied = Unpooled.copiedBuffer(buffer);
            String jsonString = copied.toString(Charset.defaultCharset());
            JsonElement obj = JsonParser.parseString(jsonString);
            JsonArray data = obj.getAsJsonObject().get("data").getAsJsonArray();
            return new ColumnMinMax(data.get(0).getAsString(), data.get(1).getAsString());
        }
    }

}
