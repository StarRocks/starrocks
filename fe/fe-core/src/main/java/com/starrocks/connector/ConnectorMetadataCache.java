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

package com.starrocks.connector;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.AbstractCache;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongBiFunction;

/**
 * The connector metadata caches that grow with the data rather than with the number of tables: Iceberg manifests'
 * data and delete files, built delete file indexes and partition maps, and Delta Lake checkpoint and log files.
 *
 * <p>By default each catalog keeps a private {@link Store} per kind, sized by the catalog's ratio properties, so N
 * catalogs may hold N times the budget. A kind whose global FE ratio ({@code *_cache_global_memory_ratio}) is set
 * above 0 instead lives in one store shared by all catalogs and bounded by that ratio of the FE heap, however many
 * catalogs there are. Either way a catalog reads and writes through its own scope; a scope keeps its catalog's TTL,
 * and its entries expire as soon as the catalog is dropped. The table and database caches stay per catalog.
 */
public final class ConnectorMetadataCache {
    private static final Logger LOG = LogManager.getLogger(ConnectorMetadataCache.class);

    public enum Kind {
        DATA_FILES,
        DELETE_FILES,
        DELETE_FILE_INDEX,
        PARTITIONS,
        DELTA_CHECKPOINTS,
        DELTA_JSON_LOGS
    }

    public record Key(String scope, Kind kind, Object key) {
    }

    // Weighs one entry (key, value) of a kind; registered once per kind.
    private static final Map<Kind, ToLongBiFunction<Object, Object>> WEIGHERS = new ConcurrentHashMap<>();
    // scope -> TTL in seconds; a scope that is gone expires its entries at once
    private static final Map<String, Long> SCOPE_TTL_SEC = new ConcurrentHashMap<>();
    private static final AtomicLong SCOPE_ID = new AtomicLong();
    // kind -> the store all catalogs share for it; created on first use when the kind's global ratio is above 0
    private static final Map<Kind, Store> SHARED = new ConcurrentHashMap<>();
    // how long the per-scope statistics of a shared store are reused, so a memory tracker pass over N catalogs walks
    // the store once instead of N times
    private static final long SHARED_STATS_REUSE_NANOS = TimeUnit.SECONDS.toNanos(1);

    private ConnectorMetadataCache() {
    }

    static double globalRatio(Kind kind) {
        return switch (kind) {
            case DATA_FILES -> Config.iceberg_data_file_cache_global_memory_ratio;
            // the index has the same budget as the delete files, in a store of its own, as per catalog
            case DELETE_FILES, DELETE_FILE_INDEX -> Config.iceberg_delete_file_cache_global_memory_ratio;
            case PARTITIONS -> Config.iceberg_partition_cache_global_memory_ratio;
            case DELTA_CHECKPOINTS -> Config.deltalake_checkpoint_meta_cache_global_memory_ratio;
            case DELTA_JSON_LOGS -> Config.deltalake_json_meta_cache_global_memory_ratio;
        };
    }

    // The store all catalogs share for the kind, or null when the kind is cached per catalog (its global ratio is 0).
    public static Store shared(Kind kind) {
        double ratio = globalRatio(kind);
        if (ratio <= 0) {
            return null;
        }
        return SHARED.computeIfAbsent(kind,
                k -> new Store(Math.round(Runtime.getRuntime().maxMemory() * ratio), true));
    }

    // A store of one catalog only, sized by the catalog's ratio property of the kind.
    public static Store newPrivate(long maximumWeight) {
        return new Store(Math.max(0L, maximumWeight), false);
    }

    // The store all catalogs share for the kind when its global ratio is set, otherwise a private one sized by the
    // catalog's ratio property of the kind.
    public static Store storeOf(Kind kind, double catalogRatio) {
        Store shared = shared(kind);
        return shared != null ? shared : newPrivate(Math.round(Runtime.getRuntime().maxMemory() * catalogRatio));
    }

    public static void registerWeigher(Kind kind, ToLongBiFunction<Object, Object> weigher) {
        WEIGHERS.putIfAbsent(kind, weigher);
    }

    // A new scope for a catalog instance. The id is unique per instance, so a catalog dropped and created again
    // under the same name never reads what the old one cached.
    public static String register(String catalogName, long ttlSec) {
        String scope = catalogName + "#" + SCOPE_ID.incrementAndGet();
        SCOPE_TTL_SEC.put(scope, ttlSec);
        return scope;
    }

    // Drops the scope's entries from the given stores; anything left behind in a store expires at once.
    public static void unregister(String scope, Set<Store> stores) {
        SCOPE_TTL_SEC.remove(scope);
        for (Store store : stores) {
            store.invalidateIf(scope, (kind, key) -> true);
        }
    }

    private static int weigh(Key key, Object value) {
        ToLongBiFunction<Object, Object> weigher = WEIGHERS.get(key.kind());
        long size;
        try {
            size = weigher == null ? 0L : weigher.applyAsLong(key.key(), value);
        } catch (RuntimeException e) {
            // runs on Caffeine's write path, so it must not throw; charge a nominal size instead
            LOG.warn("Failed to weigh connector metadata cache entry {}", key, e);
            size = 1024L;
        }
        if (size > Integer.MAX_VALUE) {
            LOG.warn("Connector metadata cache entry {} is too large: {} bytes", key, size);
            return Integer.MAX_VALUE;
        }
        return (int) Math.max(0L, size);
    }

    /**
     * A weight-bounded cache of entries keyed by (scope, kind, key).
     */
    public static final class Store {
        // An async cache, so that a load in flight is a future in the cache: the loading thread fills it outside the
        // cache's compute (a load reads remote storage and must not hold the cache's internal locks), concurrent
        // callers of the key wait for and share its value or failure, a failed or null load is dropped, and an
        // invalidation drops a load in flight instead of letting it publish afterwards.
        private final AsyncCache<Key, Object> async;
        private final Cache<Key, Object> cache;
        private final boolean shared;
        private volatile StatsSnapshot stats;

        private Store(long maximumWeight, boolean shared) {
            this.shared = shared;
            this.async = Caffeine.newBuilder()
                    .maximumWeight(maximumWeight)
                    .weigher((Weigher<Key, Object>) ConnectorMetadataCache::weigh)
                    .expireAfter(new ScopeTtlExpiry())
                    // the per-catalog manifest caches logged their removals; keep that diagnostic
                    .removalListener((Key key, Object value, RemovalCause cause) -> {
                        if (key != null && LOG.isDebugEnabled()) {
                            LOG.debug("Connector metadata cache removal: scope={}, kind={}, key={}, items={}, cause={}",
                                    key.scope(), key.kind(), key.key(), itemCount(value), cause);
                        }
                    })
                    .buildAsync();
            this.cache = async.synchronous();
        }

        @VisibleForTesting
        public Cache<Key, Object> cache() {
            return cache;
        }

        // The cached value, or the loader's value, loaded by one caller per key at a time in its own thread and
        // shared with the callers waiting for it; cached unless it is null or the load fails.
        public Object get(String scope, Kind kind, Object key, Function<Object, Object> loader) {
            CompletableFuture<Object> mine = new CompletableFuture<>();
            CompletableFuture<Object> future = async.get(new Key(scope, kind, key), (k, executor) -> mine);
            if (future == mine) {
                Object value;
                try {
                    value = loader.apply(key);
                } catch (Throwable e) {
                    mine.completeExceptionally(e);
                    throw e;
                }
                mine.complete(value);
                return value;
            }
            try {
                return future.join();
            } catch (CompletionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException runtime) {
                    throw runtime;
                }
                if (cause instanceof Error error) {
                    throw error;
                }
                throw e;
            }
        }

        public Object getIfPresent(String scope, Kind kind, Object key) {
            return cache.getIfPresent(new Key(scope, kind, key));
        }

        public void put(String scope, Kind kind, Object key, Object value) {
            cache.put(new Key(scope, kind, key), value);
        }

        public void invalidate(String scope, Kind kind, Object key) {
            cache.invalidate(new Key(scope, kind, key));
        }

        // Drops the entries of a scope and kind whose keys match.
        public void invalidateIf(String scope, Kind kind, Predicate<Object> keyMatches) {
            invalidateIf(scope, (k, key) -> k == kind && keyMatches.test(key));
        }

        // Drops the entries of a scope whose kind and key match, in one pass.
        public void invalidateIf(String scope, BiPredicate<Kind, Object> matches) {
            cache.asMap().keySet().removeIf(k -> k.scope().equals(scope) && matches.test(k.kind(), k.key()));
        }

        // The entries of one kind in a scope, a snapshot.
        public Map<Object, Object> entries(String scope, Kind kind) {
            Map<Object, Object> result = new HashMap<>();
            cache.asMap().forEach((k, v) -> {
                if (k.scope().equals(scope) && k.kind() == kind) {
                    result.put(k.key(), v);
                }
            });
            return result;
        }

        // The weight the store charges a scope's entries of the given kinds.
        public long weightedSize(String scope, Set<Kind> kinds) {
            // weights are applied by Caffeine's maintenance, which may still be pending
            cache.cleanUp();
            Policy.Eviction<Key, Object> eviction = cache.policy().eviction().orElse(null);
            if (eviction == null) {
                return 0L;
            }
            long size = 0;
            for (Key key : cache.asMap().keySet()) {
                if (key.scope().equals(scope) && kinds.contains(key.kind())) {
                    size += eviction.weightOf(key).orElse(0);
                }
            }
            return size;
        }

        // The count, items and weight of a scope's entries of a kind. A private store holds one catalog, so it is
        // walked each time; a shared store is walked once for all scopes and the result reused briefly.
        public Stats stats(String scope, Kind kind) {
            Map<String, Map<Kind, Stats>> byScope;
            if (!shared) {
                byScope = computeStats();
            } else {
                StatsSnapshot snapshot = stats;
                if (snapshot == null || System.nanoTime() - snapshot.nanos() > SHARED_STATS_REUSE_NANOS) {
                    synchronized (this) {
                        snapshot = stats;
                        if (snapshot == null || System.nanoTime() - snapshot.nanos() > SHARED_STATS_REUSE_NANOS) {
                            // timestamped once computed, so a long scan does not use up its own reuse window
                            Map<String, Map<Kind, Stats>> computed = computeStats();
                            snapshot = new StatsSnapshot(System.nanoTime(), computed);
                            stats = snapshot;
                        }
                    }
                }
                byScope = snapshot.byScope();
            }
            return byScope.getOrDefault(scope, Map.of()).getOrDefault(kind, Stats.EMPTY);
        }

        private static long itemCount(Object value) {
            return value instanceof Collection<?> c ? c.size() : value instanceof Map<?, ?> m ? m.size() : 1L;
        }

        private Map<String, Map<Kind, Stats>> computeStats() {
            // weights are applied by Caffeine's maintenance, which may still be pending
            cache.cleanUp();
            Policy.Eviction<Key, Object> eviction = cache.policy().eviction().orElse(null);
            Map<String, Map<Kind, Stats>> result = new HashMap<>();
            cache.asMap().forEach((k, v) -> {
                long weight = eviction == null ? 0L : eviction.weightOf(k).orElse(0);
                long items = itemCount(v);
                result.computeIfAbsent(k.scope(), s -> new EnumMap<>(Kind.class))
                        .merge(k.kind(), new Stats(1, items, weight), Stats::plus);
            });
            return result;
        }

        @VisibleForTesting
        public long maximum() {
            return cache.policy().eviction().map(Policy.Eviction::getMaximum).orElse(0L);
        }

        @VisibleForTesting
        public long totalWeightedSize() {
            cache.cleanUp();
            return cache.policy().eviction().map(e -> e.weightedSize().orElse(0L)).orElse(0L);
        }
    }

    // entries: cached keys; items: the files, partitions or rows in their values; weight: what eviction charges them
    public record Stats(long entries, long items, long weight) {
        static final Stats EMPTY = new Stats(0, 0, 0);

        Stats plus(Stats other) {
            return new Stats(entries + other.entries, items + other.items, weight + other.weight);
        }
    }

    private record StatsSnapshot(long nanos, Map<String, Map<Kind, Stats>> byScope) {
    }

    private static final class ScopeTtlExpiry implements Expiry<Key, Object> {
        private static long ttlNanos(Key key) {
            Long ttlSec = SCOPE_TTL_SEC.get(key.scope());
            if (ttlSec == null) {
                return 0L;
            }
            return ttlSec < 0 ? Long.MAX_VALUE : TimeUnit.SECONDS.toNanos(ttlSec);
        }

        @Override
        public long expireAfterCreate(Key key, Object value, long currentTime) {
            return ttlNanos(key);
        }

        // expire after write: a put restarts the clock, a read does not
        @Override
        public long expireAfterUpdate(Key key, Object value, long currentTime, long currentDuration) {
            return ttlNanos(key);
        }

        @Override
        public long expireAfterRead(Key key, Object value, long currentTime, long currentDuration) {
            return currentDuration;
        }
    }

    /**
     * One scope's entries of one kind in a store, as the Caffeine Cache the planning code reads and writes. Only the
     * per-key operations are supported: the planning code never walks the cache.
     */
    public static final class ScopedCache<K, V> implements Cache<K, V> {
        private final Store store;
        private final String scope;
        private final Kind kind;

        public ScopedCache(Store store, String scope, Kind kind) {
            this.store = store;
            this.scope = scope;
            this.kind = kind;
        }

        @SuppressWarnings("unchecked")
        @Override
        public V getIfPresent(Object key) {
            return (V) store.getIfPresent(scope, kind, key);
        }

        @SuppressWarnings("unchecked")
        @Override
        public V get(K key, Function<? super K, ? extends V> mappingFunction) {
            return (V) store.get(scope, kind, key, k -> mappingFunction.apply((K) k));
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<K, V> getAllPresent(Iterable<?> keys) {
            Map<K, V> result = new HashMap<>();
            for (Object key : keys) {
                V value = getIfPresent(key);
                if (value != null) {
                    result.put((K) key, value);
                }
            }
            return result;
        }

        @Override
        public void put(K key, V value) {
            store.put(scope, kind, key, value);
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> map) {
            map.forEach(this::put);
        }

        @Override
        public void invalidate(Object key) {
            store.invalidate(scope, kind, key);
        }

        @Override
        public void invalidateAll(Iterable<?> keys) {
            for (Object key : keys) {
                invalidate(key);
            }
        }

        @Override
        public void invalidateAll() {
            store.invalidateIf(scope, kind, key -> true);
        }

        @Override
        public long estimatedSize() {
            return store.stats(scope, kind).entries();
        }

        @Override
        public CacheStats stats() {
            return CacheStats.empty();
        }

        @Override
        public ConcurrentMap<K, V> asMap() {
            throw new UnsupportedOperationException("a scoped view of a connector metadata cache store");
        }

        @Override
        public void cleanUp() {
            store.cache.cleanUp();
        }

        @Override
        public Policy<K, V> policy() {
            throw new UnsupportedOperationException("a scoped view of a connector metadata cache store");
        }

        // all keys of this view, for tests
        @SuppressWarnings("unchecked")
        @VisibleForTesting
        public List<K> keys() {
            return store.entries(scope, kind).keySet().stream().map(k -> (K) k).toList();
        }
    }

    /**
     * One scope's entries of one kind in a store, as the Guava Cache the Delta Lake kernel handlers read and write.
     * A load runs outside the store's compute, is shared by the callers of the key waiting for it, and its failure is
     * reported as Guava does.
     */
    public static final class ScopedGuavaCache<K, V> extends AbstractCache<K, V> {
        private final Store store;
        private final String scope;
        private final Kind kind;

        public ScopedGuavaCache(Store store, String scope, Kind kind) {
            this.store = store;
            this.scope = scope;
            this.kind = kind;
        }

        @SuppressWarnings("unchecked")
        @Override
        public V getIfPresent(Object key) {
            return (V) store.getIfPresent(scope, kind, key);
        }

        @SuppressWarnings("unchecked")
        @Override
        public V get(K key, Callable<? extends V> loader) throws ExecutionException {
            try {
                return (V) store.get(scope, kind, key, k -> {
                    try {
                        return loader.call();
                    } catch (RuntimeException | Error e) {
                        throw e;
                    } catch (Exception e) {
                        throw new LoadFailure(e);
                    }
                });
            } catch (LoadFailure e) {
                throw new ExecutionException(e.getCause());
            } catch (Error e) {
                throw new ExecutionError(e);
            } catch (RuntimeException e) {
                throw new UncheckedExecutionException(e);
            }
        }

        @Override
        public void put(K key, V value) {
            store.put(scope, kind, key, value);
        }

        @Override
        public void invalidate(Object key) {
            store.invalidate(scope, kind, key);
        }

        @Override
        public void invalidateAll() {
            store.invalidateIf(scope, kind, key -> true);
        }

        @Override
        public long size() {
            return store.stats(scope, kind).entries();
        }

        @Override
        public void cleanUp() {
            store.cache.cleanUp();
        }

        // all keys of this view, for tests
        @SuppressWarnings("unchecked")
        @VisibleForTesting
        public List<K> keys() {
            return store.entries(scope, kind).keySet().stream().map(k -> (K) k).toList();
        }

        private static final class LoadFailure extends RuntimeException {
            LoadFailure(Exception cause) {
                super(cause);
            }
        }
    }
}
