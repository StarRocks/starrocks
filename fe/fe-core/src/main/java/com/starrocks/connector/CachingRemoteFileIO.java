// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.starrocks.connector.exception.StarRocksConnectorException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingRemoteFileIO implements RemoteFileIO {
    public static final long NEVER_EVICT = -1;
    public static final long NEVER_REFRESH = -1;
    private final RemoteFileIO fileIO;
    private final LoadingCache<RemotePathKey, List<RemoteFileDesc>> cache;

    protected CachingRemoteFileIO(RemoteFileIO fileIO,
                               Executor executor,
                               long expireAfterWriteSec,
                               long refreshIntervalSec,
                               long maxSize) {
        this.fileIO = fileIO;
        this.cache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                .build(asyncReloading(CacheLoader.from(this::loadRemoteFiles), executor));
    }

    public static CachingRemoteFileIO createCatalogLevelInstance(RemoteFileIO fileIO, Executor executor,
                                                        long expireAfterWrite, long refreshInterval, long maxSize) {
        return new CachingRemoteFileIO(fileIO, executor, expireAfterWrite, refreshInterval, maxSize);

    }

    public static CachingRemoteFileIO createQueryLevelInstance(RemoteFileIO fileIO, long maxSize) {
        return new CachingRemoteFileIO(
                fileIO,
                newDirectExecutorService(),
                NEVER_EVICT,
                NEVER_REFRESH,
                maxSize);
    }

    public Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey) {
        try {
            return ImmutableMap.of(pathKey, cache.getUnchecked(pathKey));
        } catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), StarRocksConnectorException.class);
            throw e;
        }
    }

    public List<RemoteFileDesc> loadRemoteFiles(RemotePathKey pathKey) {
        return fileIO.getRemoteFiles(pathKey).get(pathKey);
    }

    public Map<RemotePathKey, List<RemoteFileDesc>> getPresentRemoteFiles(List<RemotePathKey> paths) {
        if (fileIO instanceof CachingRemoteFileIO) {
            return ((CachingRemoteFileIO) fileIO).getPresentRemoteFiles(paths);
        } else {
            return cache.getAllPresent(paths);
        }
    }

    public List<RemotePathKey> getPresentPathKeyInCache(String basePath, boolean isRecursive) {
        return cache.asMap().keySet().stream()
                .filter(pathKey -> pathKey.approximateMatchPath(basePath, isRecursive))
                .collect(Collectors.toList());
    }

    public void updateRemoteFiles(RemotePathKey pathKey) {
        cache.put(pathKey, loadRemoteFiles(pathKey));
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteSec, long refreshSec, long maximumSize) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteSec >= 0) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteSec, SECONDS);
        }

        if (refreshSec > 0 && expiresAfterWriteSec > refreshSec) {
            cacheBuilder.refreshAfterWrite(refreshSec, SECONDS);
        }

        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }
}
