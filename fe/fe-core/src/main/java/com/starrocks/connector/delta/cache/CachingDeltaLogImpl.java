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
package com.starrocks.connector.delta.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.common.Config;
import com.starrocks.connector.delta.CachingDeltaLakeMetadata;
import com.starrocks.connector.delta.StarRocksDeltaLakeException;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.Snapshot;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.CommitInfo;
import io.delta.standalone.actions.Metadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CachingDeltaLogImpl implements DeltaLog {
    private static final Logger LOG = LogManager.getLogger(CachingDeltaLogImpl.class);

    private final Configuration hadoopConf;
    private final Path path;

    private final CachingDeltaLakeMetadata caching;
    private final DeltaLakeTableName deltaTbl;

    private final Snapshot snapshot;

    private static final Cache<DeltaLakeTableName, String> DELTA_TABLE_LOCK = CacheBuilder.newBuilder()
            .expireAfterAccess(Config.delta_cache_expired_time_seconds, TimeUnit.SECONDS)
            .maximumSize(1000)
            .build();

    private static final Map<String, Future<Boolean>> UPDATING_FUTURE = new ConcurrentHashMap<>();
    private static final Map<String, STATE> STATE_MAP = new ConcurrentHashMap<>();

    private static final Lock LOCK = new ReentrantLock();

    private static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(
            Config.delta_asyn_refresh_cache_thread_core_size,
            Config.delta_asyn_refresh_cache_thread_max_size, 0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("delta-disk-caching-persist-%d").build());

    public CachingDeltaLogImpl(Configuration hadoopConf,
                               CachingDeltaLakeMetadata chaching,
                               DeltaLakeTableName deltaTbl) {
        this.hadoopConf = hadoopConf;
        this.path = new Path(deltaTbl.getDataPath());
        this.caching = chaching;
        this.deltaTbl = deltaTbl;

        this.snapshot = new CachingDeltaSnapshotImpl(deltaTbl, chaching);

    }

    @Override
    public Snapshot snapshot() {
        if (STATE_MAP.containsKey(deltaTbl.toString())) {
            STATE state = STATE_MAP.get(deltaTbl.toString());
            if (null != state) {
                switch (state) {
                    case FAIL:
                        UPDATING_FUTURE.remove(deltaTbl.toString());
                        STATE_MAP.remove(deltaTbl.toString());
                        throw new StarRocksDeltaLakeException("failed to update the " + deltaTbl + " table cache");
                    case SUCCESS:
                        UPDATING_FUTURE.remove(deltaTbl.toString());
                        STATE_MAP.remove(deltaTbl.toString());
                        break;
                    case UPDATING:
                        break;
                }
            }
        }
        return snapshot;
    }

    @Override
    public Snapshot update() {
        Future<Boolean> cacheTask = submitUpdateCacheTask(deltaTbl, hadoopConf, caching);
        try {
            if (!cacheTask.get()) {
                LOG.error("failed to update the {} table cache ", deltaTbl.toString());
            }
        } catch (Exception e) {
            LOG.error("failed to update the {} table cache ", deltaTbl.toString());
            throw new StarRocksDeltaLakeException("failed to update cache ", e);
        } finally {
            STATE_MAP.remove(deltaTbl.toString());
            UPDATING_FUTURE.remove(deltaTbl.toString());
        }

        return snapshot;
    }

    @Override
    public Snapshot getSnapshotForVersionAsOf(long version) {
        throw new UnsupportedOperationException("get snapshots by version is not supported");
    }

    @Override
    public Snapshot getSnapshotForTimestampAsOf(long timestamp) {

        throw new UnsupportedOperationException("get snapshots by timestamp is not supported");
    }

    @Override
    public OptimisticTransaction startTransaction() {
        throw new UnsupportedOperationException("write is not supported");
    }

    @Override
    public CommitInfo getCommitInfoAt(long version) {

        throw new UnsupportedOperationException("get commitInfo by version is not supported");
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    public Iterator<VersionLog> getChanges(long startVersion, boolean failOnDataLoss) {
        throw new UnsupportedOperationException("get changes by version is not supported");
    }

    @Override
    public long getVersionBeforeOrAtTimestamp(long timestamp) {
        throw new UnsupportedOperationException("get version at before timestamp is not supported");
    }

    @Override
    public long getVersionAtOrAfterTimestamp(long timestamp) {
        throw new UnsupportedOperationException("get version at after timestamp is not supported");
    }

    @Override
    public boolean tableExists() {
        if (null == caching) {
            return false;
        }
        return snapshot.getVersion() >= 0;
    }

    public static DeltaLog forTable(Configuration hadoopConf,
                                    CachingDeltaLakeMetadata caching,
                                    DeltaLakeTableName deltaTbl) throws IOException {

        // Non-partitioned tables are not cached
        if (isDeltaTable(deltaTbl.getDataPath(), hadoopConf)) {
            return checkUpdateCache(hadoopConf, caching, deltaTbl);
        } else {
            return new CachingDeltaLogImpl(hadoopConf, null, null);
        }

    }

    private static boolean isNeedUpdateCache(Configuration hadoopConf,
                                             CachingDeltaLakeMetadata caching,
                                             DeltaLakeTableName deltaTbl) {
        // Get latest snapshot
        Long nativeSnapshotVersion = caching.getCacheSnapshotVersion(deltaTbl);
        if (null != nativeSnapshotVersion) {
            try {
                long lastSnapshotVersion = caching.getLastSnapshotVersion(deltaTbl, hadoopConf);
                if (lastSnapshotVersion == nativeSnapshotVersion) {
                    return false;
                }
            } catch (Exception e) {
                LOG.error("get last snapshot version fail of {}", deltaTbl, e);
                throw new IllegalStateException(String.format("Cannot checkpoint a non-exist table %s Did you manually " +
                        "delete files in the _delta_log directory?", deltaTbl.toString()));
            }
        }
        return true;
    }

    private static boolean isNonPartitionTable(CachingDeltaLakeMetadata caching,
                                               DeltaLakeTableName deltaTbl) {
        try {
            Metadata metadata = caching.getMetadata(deltaTbl);
            if (null != metadata && metadata.getPartitionColumns().size() == 0) {
                return true;
            }
        } catch (Exception e) {
            LOG.info("cache not find the metadata of {} {}", deltaTbl, e.getMessage());
        }
        return false;
    }

    private static DeltaLog checkUpdateCache(Configuration hadoopConf,
                                             CachingDeltaLakeMetadata caching,
                                             DeltaLakeTableName deltaTbl) {

        DeltaLog deltaLog;
        if (isNonPartitionTable(caching, deltaTbl)) {
            deltaLog = DeltaLog.forTable(hadoopConf, deltaTbl.getDataPath());
            caching.putCacheTableMetadata(deltaLog.snapshot(), deltaTbl);
            caching.invalidateDao(deltaTbl);
            return deltaLog;
        }

        if (isNeedUpdateCache(hadoopConf, caching, deltaTbl)) {
            String lockString;
            try {
                lockString = DELTA_TABLE_LOCK.get(deltaTbl, deltaTbl::toString);
            } catch (ExecutionException e) {
                LOG.error("get deltalake table:{} lock tail", deltaTbl, e);
                throw new StarRocksDeltaLakeException(e.getMessage());
            }
            synchronized (lockString) {
                if (isNeedUpdateCache(hadoopConf, caching, deltaTbl)) {
                    updateCache(hadoopConf, caching, deltaTbl);
                }
                deltaLog = new CachingDeltaLogImpl(hadoopConf, caching, deltaTbl);
            }
        } else {
            deltaLog = new CachingDeltaLogImpl(hadoopConf, caching, deltaTbl);
        }
        return deltaLog;
    }

    private static void updateCache(Configuration hadoopConf, CachingDeltaLakeMetadata caching,
                                    DeltaLakeTableName deltaTbl) {
        Future<Boolean> cacheTask = submitUpdateCacheTask(deltaTbl, hadoopConf, caching);
        if (caching.loaderCacheTable(deltaTbl) == null) {
            try {
                if (!cacheTask.get()) {
                    throw new Exception("deltaLake table:" + deltaTbl);
                }
            } catch (Exception e) {
                LOG.error("failed to update the {} table cache", deltaTbl.toString(), e);
                throw new StarRocksDeltaLakeException("failed to update cache", e);
            } finally {
                STATE_MAP.remove(deltaTbl.toString());
                UPDATING_FUTURE.remove(deltaTbl.toString());
            }
        }

    }


    private static void update(Configuration hadoopConf, CachingDeltaLakeMetadata caching,
                               DeltaLakeTableName deltaTbl) throws Exception {
        long startTime = System.currentTimeMillis();
        DeltaLog forTable = DeltaLog.forTable(hadoopConf, deltaTbl.getDataPath());
        Snapshot snapshot = forTable.snapshot();
        Metadata metadata = snapshot.getMetadata();
        if (metadata.getPartitionColumns().size() > 0) {
            caching.refreshTable(forTable.snapshot(), deltaTbl);
        }
        //schema cache mem
        LOG.info("update local cache take time {} (ms):", (System.currentTimeMillis() - startTime));
    }


    private static boolean isDeltaTable(String path, Configuration hadoopConf) throws IOException {
        Path logPath = new Path(path, "_delta_log");
        FileSystem fs = FileSystem.get(logPath.toUri(), hadoopConf);
        try {
            return fs.exists(logPath);
        } catch (IOException e) {
            throw new IOException(String.format("get _delta_log exception for %s",
                    path), e);
        }

    }

    private static Future<Boolean> submitUpdateCacheTask(DeltaLakeTableName deltaTbl,
                                                         Configuration hadoopConf,
                                                         CachingDeltaLakeMetadata caching) {
        Future<Boolean> cacheTask;
        LOCK.lock();
        try {
            if (!UPDATING_FUTURE.containsKey(deltaTbl.toString())) {
                cacheTask = EXECUTOR.submit(() -> {
                    try {
                        STATE_MAP.put(deltaTbl.toString(), STATE.UPDATING);
                        update(hadoopConf, caching, deltaTbl);
                    } catch (Exception e) {
                        STATE_MAP.put(deltaTbl.toString(), STATE.FAIL);
                        LOG.error("update local cache failed ", e);
                        return false;
                    }
                    STATE_MAP.put(deltaTbl.toString(), STATE.SUCCESS);
                    return true;
                });
                UPDATING_FUTURE.put(deltaTbl.toString(), cacheTask);
            } else {
                cacheTask = UPDATING_FUTURE.get(deltaTbl.toString());
            }
        } finally {
            LOCK.unlock();
        }
        return cacheTask;
    }


    enum STATE {
        SUCCESS,
        FAIL,
        UPDATING
    }
}