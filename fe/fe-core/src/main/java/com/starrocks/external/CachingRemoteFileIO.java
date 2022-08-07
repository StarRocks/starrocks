package com.starrocks.external;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.starrocks.external.hive.RemoteFileDesc;
import com.starrocks.external.hive.RemotePathKey;
import com.starrocks.external.hive.StarRocksConnectorException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingRemoteFileIO extends RemoteFileIO {
    public static final long NOT_EVICT = -1;
    private final RemoteFileIO fileIO;
    private LoadingCache<RemotePathKey, List<RemoteFileDesc>> cache;

    public CachingRemoteFileIO(RemoteFileIO fileIO,
                               Executor executor,
                               long expireAfterWriteSec,
                               long refreshIntervalSec,
                               long maxSize) {
        this.fileIO = fileIO;
        this.cache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                .build(asyncReloading(CacheLoader.from(this::loadRemoteFiles), executor));
    }

    public static CachingRemoteFileIO cachingRemoteFileIO(RemoteFileIO remoteFileIO, Executor executor,
                                                            long expireAfterWrite, long refreshInterval, long maxSize) {
        return new CachingRemoteFileIO(remoteFileIO, executor, expireAfterWrite, refreshInterval, maxSize);
    }

    public static CachingRemoteFileIO memoizeRemoteIO(RemoteFileIO fileIO, long maxSize) {
        return new CachingRemoteFileIO(
                fileIO,
                newDirectExecutorService(),
                NOT_EVICT,
                NOT_EVICT,
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

    public Map<RemotePathKey, List<RemoteFileDesc>> getPresentRemoteFiles(List<RemotePathKey> paths) {
        return cache.getAllPresent(paths);
    }

    private List<RemoteFileDesc> loadRemoteFiles(RemotePathKey pathKey) {
        return fileIO.getRemoteFiles(pathKey).get(pathKey);
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteSec, long refreshSec, long maximumSize) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteSec >= 0) {
            cacheBuilder = cacheBuilder.expireAfterWrite(expiresAfterWriteSec, SECONDS);
        }

        if (refreshSec > 0 && expiresAfterWriteSec > refreshSec) {
            cacheBuilder = cacheBuilder.refreshAfterWrite(refreshSec, SECONDS);
        }

        cacheBuilder = cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }
}
