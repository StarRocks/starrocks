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
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.memory.estimate.Estimator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.rule.tree.prunesubfield.SubfieldAccessPathNormalizer;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
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
import java.util.stream.Collectors;

public class ColumnMinMaxMgr implements IMinMaxStatsMgr, MemoryTrackable {
    private record CacheValue(ColumnMinMax minMax, StatsVersion version) {}

    private static final Logger LOG = LogManager.getLogger(ColumnMinMaxMgr.class);

    private static final SimpleExecutor STMT_EXECUTOR =
            new SimpleExecutor("ColumnMinMaxMgr", TResultSinkType.HTTP_PROTOCAL);

    private static final ColumnMinMaxMgr INSTANCE = new ColumnMinMaxMgr();

    private final AsyncLoadingCache<ColumnIdentifier, Optional<CacheValue>> cache = Caffeine.newBuilder()
            .maximumSize(Config.statistic_dict_columns)
            .executor(ThreadPoolManager.getStatsCacheThread())
            .buildAsync(new CacheLoader());

    // Throttle: last wall-clock millis a MetaScan collection was triggered per column. Bounds how often
    // min/max is re-collected via a [_META_] MetaScan (which opens every segment's metadata and contends
    // on the segment metadata cache). Bounded + LRU-evicting so idle columns cannot accumulate.
    private final Cache<ColumnIdentifier, Long> lastCollectMs = Caffeine.newBuilder()
            .maximumSize(Config.statistic_dict_columns)
            .build();

    static ColumnMinMaxMgr getInstance() {
        return INSTANCE;
    }

    @Override
    public Optional<ColumnMinMax> getStats(ColumnIdentifier identifier, StatsVersion version) {
<<<<<<< HEAD
        CompletableFuture<Optional<CacheValue>> future = cache.get(identifier);
        if (future.isDone()) {
=======
        if (!REPLAY_MINMAX.isEmpty()) {
            ColumnMinMax replay = REPLAY_MINMAX.get(identifier);
            if (replay != null) {
                return Optional.of(replay);
            }
        }
        // Peek without triggering a load, so re-collection is gated by the throttle below. A stale entry
        // (data version moved past it) is dropped and never served -- serving it would fold min()/max()
        // to a value that no longer exists -- so throttling only ever costs the optimization (the caller
        // falls back to normal execution), never correctness.
        boolean needCollect;
        CompletableFuture<Optional<CacheValue>> future = cache.getIfPresent(identifier);
        if (future == null) {
            needCollect = true;
        } else if (!future.isDone()) {
            needCollect = false;
        } else {
            needCollect = false;
>>>>>>> 31671d7 ([BugFix] Throttle per-column min/max stats collection (#78544))
            try {
                Optional<CacheValue> cacheValue = future.get();
                if (cacheValue.isPresent()) {
                    CacheValue value = cacheValue.get();
                    if (value.version().getVersion() >= version.getVersion()) {
                        return Optional.of(value.minMax());
                    }
                    // Drop only the exact stale future we inspected: a concurrent query may have already
                    // replaced it with a fresh load, and unconditionally evicting that (while the throttle
                    // blocks us from reloading) would leave the column with no cached entry for a whole
                    // interval. asMap().remove(key, value) is a lock-free CAS that no-ops if it moved on.
                    cache.asMap().remove(identifier, future);
                    needCollect = true;
                }
                // else: cached "not collectible" (e.g. non-numeric column) -- keep it, do not rescan.
            } catch (Exception e) {
                LOG.warn("Failed to get MinMax for column: {}, version: {}", identifier, version, e);
            }
        }
        // Trigger a fresh [_META_] MetaScan only when the per-column throttle interval has elapsed;
        // otherwise skip the optimization for now. Bounds how often segment metadata is re-scanned for a
        // frequently loaded column.
        if (needCollect && shouldCollect(identifier)) {
            cache.get(identifier);
        }
        return Optional.empty();
    }

    // True if enough time has elapsed since the last collection trigger for this column (or throttling is
    // disabled via min_max_stats_collect_interval_sec <= 0). Stamps "now" as the trigger time on success.
    @VisibleForTesting
    boolean shouldCollect(ColumnIdentifier identifier) {
        int intervalSec = Config.min_max_stats_collect_interval_sec;
        if (intervalSec <= 0) {
            return true;
        }
        long now = System.currentTimeMillis();
        Long last = lastCollectMs.getIfPresent(identifier);
        if (last != null && now - last < intervalSec * 1000L) {
            return false;
        }
        lastCollectMs.put(identifier, now);
        return true;
    }

    @Override
    public void removeStats(ColumnIdentifier identifier, StatsVersion version) {
        // skip dictionary operator in checkpoint thread
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        if (!cache.asMap().containsKey(identifier)) {
            // If the identifier is already in the cache, we do not need to update it.
            return;
        }
        Optional<ColumnMinMax> minMax = getStats(identifier, version);
        if (minMax.isPresent()) {
            cache.synchronous().invalidate(identifier);
        }
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("ColumnMinMax", (long) cache.asMap().size());
    }

    @Override
    public long estimateSize() {
        return Estimator.estimate(cache.asMap(), 20);
    }

    @VisibleForTesting
    public static String genMinMaxSql(String catalogName, Database db, OlapTable olapTable, Column column) {
        List<String> pieces = SubfieldAccessPathNormalizer.parseSimpleJsonPath(column.getColumnId().getId());
        String columnName;
        if (pieces.size() == 1) {
            columnName = column.getName();
        } else {
            String path = pieces.stream().skip(1).collect(Collectors.joining("."));
            String jsonFunc;
            Type type = column.getType();
            if (type.equals(IntegerType.BIGINT)) {
                jsonFunc = "get_json_int";
            } else if (type.isStringType()) {
                jsonFunc = "get_json_string";
            } else if (type.equals(FloatType.DOUBLE)) {
                jsonFunc = "get_json_double";
            } else {
                throw new IllegalStateException("unsupported json field type: " + column.getType());
            }
            columnName = String.format("%s(%s, '%s')", jsonFunc, StatisticUtils.quoting(column.getName()), path);
        }
        String sql = "select min(" + columnName + ") as min, max(" + columnName + ") as max"
                + " from " +
                StatisticUtils.quoting(catalogName, db.getOriginName(), olapTable.getName())
                + "[_META_];";
        return sql;
    }

    protected static final class CacheLoader implements AsyncCacheLoader<ColumnIdentifier, Optional<CacheValue>> {

        @Override
        public @NonNull CompletableFuture<Optional<CacheValue>> asyncLoad(@NonNull ColumnIdentifier key,
                                                                          @NonNull Executor executor) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Pair<Database, Table> pair = getDatabaseAndTable(key.getTableId());
                    Database db = pair.first;
                    OlapTable olapTable = (OlapTable) pair.second;
                    Column column = olapTable.getColumn(key.getColumnName());
                    if (column == null || (!column.getType().isNumericType() && !column.getType().isDate())) {
                        return Optional.empty();
                    }
                    // We need the aggregated result, so this constraint is not satisfied.
                    if (column.isAggregated()) {
                        return Optional.empty();
                    }

                    long version = olapTable.getPartitions().stream().flatMap(p -> p.getSubPartitions().stream()).map(
                            PhysicalPartition::getVisibleVersionTime).max(Long::compareTo).orElse(0L);
                    String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
                    String sql = genMinMaxSql(catalogName, db, olapTable, column);

                    ConnectContext context = STMT_EXECUTOR.createConnectContext();
                    context.getSessionVariable().setPipelineDop(1);
                    List<TResultBatch> result = STMT_EXECUTOR.executeDQL(sql, context);
                    if (result.isEmpty()) {
                        return Optional.empty();
                    }
                    Preconditions.checkState(result.size() == 1);
                    ColumnMinMax minMax = toMinMax(result.get(0));
                    if (minMax.minValue() == null || minMax.maxValue() == null) {
                        return Optional.empty();
                    }
                    return Optional.of(new CacheValue(minMax, new StatsVersion(version, version)));
                } catch (Exception e) {
                    LOG.warn("get MinMax for column: {}, error: {}", key, e.getMessage(), e);
                }
                return Optional.empty();
            }, executor);
        }

        private static ColumnMinMax toMinMax(TResultBatch result) {
            ByteBuffer buffer = result.getRows().get(0);
            ByteBuf copied = Unpooled.copiedBuffer(buffer);
            String jsonString = copied.toString(Charset.defaultCharset());
            JsonElement obj = JsonParser.parseString(jsonString);
            JsonElement dataElement = obj.getAsJsonObject().get("data");
            if (dataElement == null || dataElement.isJsonNull() || !dataElement.isJsonArray()) {
                throw new IllegalStateException("Invalid 'data' field");
            }
            JsonArray data = dataElement.getAsJsonArray();

            JsonElement minElement = data.get(0);
            JsonElement maxElement = data.get(1);

            String min = minElement.isJsonNull() ? null : minElement.getAsString();
            String max = maxElement.isJsonNull() ? null : maxElement.getAsString();

            return new ColumnMinMax(min, max);
        }
    }

    public static Pair<Database, Table> getDatabaseAndTable(long tableId) {
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
        Database db = null;
        Table table = null;
        for (Long id : dbIds) {
            db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(id);
            if (db == null) {
                continue;
            }
            table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            if (table == null) {
                continue;
            }
            break;
        }

        if (table == null) {
            throw new SemanticException("Table %s is not found", tableId);
        }

        if (!table.isOlapOrCloudNativeTable() && !table.isMaterializedView()) {
            throw new SemanticException("Table '%s' is not a OLAP table or LAKE table or Materialize View",
                    table.getName());
        }
        return Pair.create(db, table);
    }
}