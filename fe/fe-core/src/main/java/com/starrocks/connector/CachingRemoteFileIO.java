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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.util.SizeEstimator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingRemoteFileIO implements RemoteFileIO {
    private static final Logger LOG = LogManager.getLogger(CachingRemoteFileIO.class);

    public static final long NEVER_EVICT = -1;
    public static final long NEVER_REFRESH = -1;
    private final RemoteFileIO fileIO;
    private final LoadingCache<RemotePathKey, List<RemoteFileDesc>> cache;

    protected CachingRemoteFileIO(RemoteFileIO fileIO,
                                  Executor executor,
                                  long expireAfterWriteSec,
                                  long refreshIntervalSec,
                                  double cacheMemorySizeRatio) {
        this.fileIO = fileIO;
        long cacheMemSize = Math.round(Runtime.getRuntime().maxMemory() * cacheMemorySizeRatio);
        this.cache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec)
                .executor(executor)
                .maximumWeight(cacheMemSize)
                .weigher((RemotePathKey key, List<RemoteFileDesc> value) -> {
                    long size = Math.toIntExact(SizeEstimator.estimate(key));
                    if (!value.isEmpty()) {
                        size += 1L * SizeEstimator.estimate(value.get(0)) * value.size();
                    }
                    if (size > Integer.MAX_VALUE) {
                        size = Integer.MAX_VALUE;
                        LOG.debug("size is larger than max integer, use max integer as weight");
                    }
                    return (int) size;
                })
                .removalListener((RemotePathKey key, List<RemoteFileDesc> value, RemovalCause cause) -> {
                    LOG.debug(String.format("Key=%s, Value.size=%d, Cause=%s",
                            key,
                            value != null ? value.size() : 0,
                            cause));
                })
                .build(key -> fileIO.getRemoteFiles(key).get(key));
    }

    public static CachingRemoteFileIO createCatalogLevelInstance(RemoteFileIO fileIO, Executor executor,
                                                        long expireAfterWrite, long refreshInterval, double memSizeRatio) {
        return new CachingRemoteFileIO(fileIO, executor, expireAfterWrite, refreshInterval, memSizeRatio);
    }

    public static CachingRemoteFileIO createQueryLevelInstance(RemoteFileIO fileIO, double memSizeRatio) {
        return new CachingRemoteFileIO(
                fileIO,
                newDirectExecutorService(),
                NEVER_EVICT,
                NEVER_REFRESH,
                memSizeRatio);
    }

    public Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey) {
        return getRemoteFiles(pathKey, true);
    }

    public Map<RemotePathKey, List<RemoteFileDesc>> getRemoteFiles(RemotePathKey pathKey, boolean useCache) {
        try {
            if (!useCache) {
                invalidatePartition(pathKey);
            }
            return ImmutableMap.of(pathKey, cache.get(pathKey));
        } catch (UncheckedExecutionException e) {
            LOG.error("Error occurred when getting remote files from cache", e);
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
        if (fileIO instanceof CachingRemoteFileIO) {
            ((CachingRemoteFileIO) fileIO).updateRemoteFiles(pathKey);
        } else {
            cache.put(pathKey, loadRemoteFiles(pathKey));
        }
    }

    public synchronized void invalidateAll() {
        cache.invalidateAll();
    }

    public void invalidatePartition(RemotePathKey pathKey) {
        // fileIO is a CachingRemoteFileIO instance means that the current level is query level metadata,
        // otherwise it's catalog level metadata. Both of two level metadata should be invalidated.
        if (fileIO instanceof CachingRemoteFileIO) {
            ((CachingRemoteFileIO) fileIO).invalidatePartition(pathKey);
            cache.invalidate(pathKey);
        } else {
            cache.invalidate(pathKey);
        }
    }

    private static Caffeine<Object, Object> newCacheBuilder(long expiresAfterWriteSec, long refreshSec) {
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
        if (expiresAfterWriteSec >= 0) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteSec, SECONDS);
        }

        if (refreshSec > 0 && expiresAfterWriteSec > refreshSec) {
            cacheBuilder.refreshAfterWrite(refreshSec, SECONDS);
        }

        return cacheBuilder;
    }

    @Override
    public FileStatus[] getFileStatus(Path... files) throws IOException {
        return fileIO.getFileStatus(files);
    }
}
