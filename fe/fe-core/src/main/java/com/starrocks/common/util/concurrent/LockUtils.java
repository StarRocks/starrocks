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

package com.starrocks.common.util.concurrent;

import com.google.gson.JsonObject;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Extract some functions from {@link com.starrocks.catalog.Database},
 * because we want to monitor the stats of lock/unlock action, these static
 * methods can apply locks of other objects, like tables.
 */
public class LockUtils {
    private static final Logger LOG = LogManager.getLogger(LockUtils.class);

    private enum LockType {
        DB_READ_LOCK,
        DB_READ_TRY_LOCK,
        DB_WRITE_LOCK,
        DB_WRITE_TRY_LOCK
    }

    public static class SlowLockLogStats {
        private long lastSlowLockLogTime = 0L;

        public void updateLastSlowLockLogTime(long newTime) {
            lastSlowLockLogTime = newTime;
        }

        public long getLastSlowLockLogTime() {
            return lastSlowLockLogTime;
        }
    }

    private static void logSlowLockEventIfNeeded(long startMs, LockType type,
                                                 long objectId, String objectName,
                                                 JsonObject beforeLockInfo, boolean acquired,
                                                 QueryableReentrantReadWriteLock rwLock,
                                                 SlowLockLogStats slowLockLogStats) {
        long endMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
        if (endMs - startMs > Config.slow_lock_threshold_ms &&
                endMs > slowLockLogStats.getLastSlowLockLogTime() + Config.slow_lock_log_every_ms) {
            slowLockLogStats.updateLastSlowLockLogTime(endMs);
            JsonObject slowLockInfoJObj = new JsonObject();
            slowLockInfoJObj.add("beforeLockInfo", beforeLockInfo);
            JsonObject afterLockInfo = acquired ? getLockInfoWithCurrStack(rwLock) : getLockInfoWithOwnerStack(rwLock);
            slowLockInfoJObj.add("afterLockInfo", afterLockInfo);

            // using json style instead of multi-line style, because for some logging collection
            // system, it's hard to collect and format those log
            LOG.warn("slow lock. type: {}, obj id: {}, obj name: {}, waited for: {}ms, info: {}",
                    type, objectId, objectName, endMs - startMs, slowLockInfoJObj);
        }
    }

    private static void logTryLockFailure(LockType type,
                                          long timeout, TimeUnit unit,
                                          long objectId, String objectName,
                                          JsonObject beforeLockInfo, JsonObject afterLockInfo) {
        JsonObject slowLockInfoJObj = new JsonObject();
        slowLockInfoJObj.add("beforeLockInfo", beforeLockInfo);
        slowLockInfoJObj.add("afterLockInfo", afterLockInfo);
        LOG.warn("try lock failed. type: {}, timeout: {}, obj id: {}, obj name: {}, info: {}",
                type, unit.toMillis(timeout), objectId, objectName, slowLockInfoJObj);
    }

    private static void lock(QueryableReentrantReadWriteLock rwLock,
                             LockType type,
                             long objectId, String objectName,
                             SlowLockLogStats slowLockLogStats) {
        long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);

        JsonObject beforeLockInfo = getLockInfoWithOwnerStack(rwLock);
        switch (type) {
            case DB_READ_LOCK:
                rwLock.sharedLock();
                break;
            case DB_WRITE_LOCK:
                rwLock.exclusiveLock();
                break;
            default:
                throw new UnsupportedOperationException("unknown lock type: " + type);
        }

        logSlowLockEventIfNeeded(startMs, type, objectId, objectName,
                beforeLockInfo, true, rwLock, slowLockLogStats);
    }

    private static boolean tryLock(QueryableReentrantReadWriteLock rwLock,
                                   long timeout, TimeUnit unit,
                                   LockType type,
                                   long objectId, String objectName,
                                   SlowLockLogStats slowLockLogStats) throws InterruptedException {
        long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);

        JsonObject beforeLockInfo = getLockInfoWithOwnerStack(rwLock);
        boolean lockingResult;
        switch (type) {
            case DB_READ_TRY_LOCK:
                lockingResult = rwLock.trySharedLock(timeout, unit);
                break;
            case DB_WRITE_TRY_LOCK:
                lockingResult = rwLock.tryExclusiveLock(timeout, unit);
                break;
            default:
                throw new UnsupportedOperationException("unknown lock type: " + type);
        }


        if (lockingResult) {
            logSlowLockEventIfNeeded(startMs, type,
                    objectId, objectName, beforeLockInfo, true, rwLock, slowLockLogStats);
        } else {
            // log try lock failure
            JsonObject afterLockInfo = getLockInfoWithOwnerStack(rwLock);
            logTryLockFailure(type, timeout, unit, objectId, objectName, beforeLockInfo, afterLockInfo);
        }

        return lockingResult;
    }

    public static void dbReadLock(QueryableReentrantReadWriteLock rwLock,
                                  long objectId, String objectName,
                                  SlowLockLogStats slowLockLogStats) {
        LockUtils.lock(rwLock, LockUtils.LockType.DB_READ_LOCK, objectId, objectName, slowLockLogStats);
    }

    public static boolean tryDbReadLock(QueryableReentrantReadWriteLock rwLock,
                                        long timeout, TimeUnit unit,
                                        long objectId, String objectName,
                                        SlowLockLogStats slowLockLogStats) throws InterruptedException {
        return LockUtils.tryLock(rwLock, timeout, unit,
                LockType.DB_READ_TRY_LOCK, objectId, objectName, slowLockLogStats);
    }

    public static void dbWriteLock(QueryableReentrantReadWriteLock rwLock,
                                   long objectId, String objectName,
                                   SlowLockLogStats slowLockLogStats) {
        LockUtils.lock(rwLock, LockType.DB_WRITE_LOCK, objectId, objectName, slowLockLogStats);
    }

    public static boolean tryDbWriteLock(QueryableReentrantReadWriteLock rwLock,
                                         long timeout, TimeUnit unit,
                                         long objectId, String objectName,
                                         SlowLockLogStats slowLockLogStats) throws InterruptedException {
        return LockUtils.tryLock(rwLock, timeout, unit,
                LockType.DB_WRITE_TRY_LOCK, objectId, objectName, slowLockLogStats);
    }

    private static JsonObject getLockInfoWithOwnerStack(QueryableReentrantReadWriteLock rwLock) {
        return rwLock.getLockInfoToJson(null);
    }

    private static JsonObject getLockInfoWithCurrStack(QueryableReentrantReadWriteLock rwLock) {
        return rwLock.getLockInfoToJson(Thread.currentThread());
    }
}
