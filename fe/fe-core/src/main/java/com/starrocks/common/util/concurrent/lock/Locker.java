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

package com.starrocks.common.util.concurrent.lock;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.util.LogUtil;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.concurrent.QueryableReentrantReadWriteLock;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Locker {
    private static final Logger LOG = LogManager.getLogger(Locker.class);

    /* The rid of the lock that this locker is waiting for. */
    private Long waitingForRid;

    /* The LockType corresponding to waitingFor. */
    private LockType waitingForType;

    /* The thread that created this locker */
    private final String threadName;
    private final long threadID;

    /* The thread stack that created this locker */
    private final String stackTrace;

    private final Map<Long, Long> lastSlowLockLogTimeMap = new HashMap<>();

    public Locker() {
        this.waitingForRid = null;
        this.waitingForType = null;
        /* Save the thread used to create the locker and thread stack. */
        this.threadID = Thread.currentThread().getId();
        this.threadName = Thread.currentThread().getName();
        this.stackTrace = getStackTrace();
    }

    /**
     * Attempt to acquire a lock of 'lockType' on resourceId
     *
     * @param rid      The resource id to lock
     * @param lockType Then lock type requested
     * @param timeout  milliseconds to time out after if lock couldn't be obtained.
     *                 0 means block indefinitely.
     * @throws LockTimeoutException    when the transaction time limit was exceeded.
     * @throws NotSupportLockException when not support param or operation
     */
    public void lock(long rid, LockType lockType, long timeout) throws IllegalLockStateException {
        if (timeout < 0) {
            throw new NotSupportLockException("lock timeout value cannot be less than 0");
        }

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        lockManager.lock(rid, this, lockType, timeout);
    }

    public void lock(long rid, LockType lockType) throws IllegalLockStateException {
        this.lock(rid, lockType, 0);
    }

    /**
     * Release lock
     *
     * @param rid The resource id of the lock to release.
     * @throws IllegalMonitorStateException – if the current thread does not hold this lock
     */
    public void release(long rid, LockType lockType) {
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        lockManager.release(rid, this, lockType);
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void lockDatabase(Database database, LockType lockType) {
        if (Config.lock_manager_enabled) {
            Preconditions.checkNotNull(database);
            try {
                lock(database.getId(), lockType, 0);
            } catch (IllegalLockStateException e) {
                ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
            }
        } else {
            if (lockType.isWriteLock()) {
                QueryableReentrantReadWriteLock rwLock = database.getRwLock();
                long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
                String threadDump = getOwnerInfo(rwLock.getOwner());
                rwLock.exclusiveLock();
                logSlowLockEventIfNeeded(startMs, "writeLock", threadDump, database.getId(), database.getFullName());
            } else {
                QueryableReentrantReadWriteLock rwLock = database.getRwLock();
                long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
                String threadDump = getOwnerInfo(rwLock.getOwner());
                rwLock.sharedLock();
                logSlowLockEventIfNeeded(startMs, "readLock", threadDump, database.getId(), database.getFullName());
            }
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public boolean tryLockDatabase(Database database, LockType lockType, long timeout) {
        if (Config.lock_manager_enabled) {
            Preconditions.checkNotNull(database);
            try {
                lock(database.getId(), lockType, timeout);
                return true;
            } catch (LockTimeoutException e) {
                return false;
            } catch (IllegalLockStateException e) {
                ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
                return false;
            }
        } else {
            Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));

            QueryableReentrantReadWriteLock rwLock = database.getRwLock();
            try {
                long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
                String threadDump = getOwnerInfo(rwLock.getOwner());

                boolean result;
                if (lockType.isWriteLock()) {
                    result = rwLock.tryExclusiveLock(timeout, TimeUnit.MILLISECONDS);
                } else {
                    result = rwLock.trySharedLock(timeout, TimeUnit.MILLISECONDS);
                }

                if (!result) {
                    logTryLockFailureEvent(lockType.toString(), threadDump);
                    return false;
                } else {
                    logSlowLockEventIfNeeded(startMs, "try" + lockType, threadDump, database.getId(),
                            database.getFullName());
                    return true;
                }
            } catch (InterruptedException e) {
                LOG.warn("failed to try " + lockType + " lock at db[" + database.getId() + "]", e);
                return false;
            }
        }
    }

    private void logTryLockFailureEvent(String type, String threadDump) {
        LOG.warn("try db lock failed. type: {}, current {}", type, threadDump);
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public boolean lockAndCheckExist(Database database, LockType lockType) {
        lockDatabase(database, lockType);
        if (database.getExist()) {
            return true;
        } else {
            unLockDatabase(database, lockType);
            return false;
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void unLockDatabase(Database database, LockType lockType) {
        if (Config.lock_manager_enabled) {
            Preconditions.checkNotNull(database);
            release(database.getId(), lockType);
        } else {
            if (lockType.isWriteLock()) {
                QueryableReentrantReadWriteLock rwLock = database.getRwLock();
                rwLock.exclusiveUnlock();
            } else {
                QueryableReentrantReadWriteLock rwLock = database.getRwLock();
                rwLock.sharedUnlock();
            }
        }
    }

    private String getOwnerInfo(Thread owner) {
        if (owner == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("owner id: ").append(owner.getId()).append(", owner name: ")
                .append(owner.getName()).append(", owner stack: ").append(Util.dumpThread(owner, 50));
        return sb.toString();
    }

    private void logSlowLockEventIfNeeded(long startMs, String type, String threadDump, Long databaseId,
                                          String fullQualifiedName) {
        long endMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
        Long lastSlowLockLogTime = lastSlowLockLogTimeMap.getOrDefault(databaseId, 0L);
        if (endMs - startMs > Config.slow_lock_threshold_ms &&
                endMs > lastSlowLockLogTime + Config.slow_lock_log_every_ms) {
            lastSlowLockLogTime = endMs;
            lastSlowLockLogTimeMap.put(databaseId, lastSlowLockLogTime);
            LOG.warn("slow db lock. type: {}, db id: {}, db name: {}, wait time: {}ms, " +
                            "former {}, current stack trace: {}", type, databaseId, fullQualifiedName, endMs - startMs,
                    threadDump, LogUtil.getCurrentStackTrace());
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public boolean isWriteLockHeldByCurrentThread(Database database) {
        if (Config.lock_manager_enabled) {
            return true;
        } else {
            return database.getRwLock().isWriteLockHeldByCurrentThread();
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void lockTablesWithIntensiveDbLock(Database database, List<Long> tableList, LockType lockType) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        List<Long> tableListClone = new ArrayList<>(tableList);

        if (Config.lock_manager_enabled) {
            Preconditions.checkState(!tableListClone.isEmpty());

            try {
                if (lockType == LockType.WRITE) {
                    this.lock(database.getId(), LockType.INTENTION_EXCLUSIVE, 0);
                } else {
                    this.lock(database.getId(), LockType.INTENTION_SHARED, 0);
                }

                Collections.sort(tableListClone);
                for (Long rid : tableListClone) {
                    this.lock(rid, lockType, 0);
                }
            } catch (IllegalLockStateException e) {
                ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
            }
        } else {
            //Fallback to db lock
            lockDatabase(database, lockType);
        }
    }

    public boolean tryLockTablesWithIntensiveDbLock(Database database, List<Long> tableList, LockType lockType, long timeout) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        List<Long> tableListClone = new ArrayList<>(tableList);

        if (Config.lock_manager_enabled) {
            Preconditions.checkState(!tableListClone.isEmpty());

            try {
                if (lockType == LockType.WRITE) {
                    this.lock(database.getId(), LockType.INTENTION_EXCLUSIVE, timeout);
                } else {
                    this.lock(database.getId(), LockType.INTENTION_SHARED, timeout);
                }
            } catch (IllegalLockStateException e) {
                return false;
            }

            List<Long> ridLockedList = new ArrayList<>();
            try {
                Collections.sort(tableListClone);
                for (Long rid : tableListClone) {
                    this.lock(rid, lockType, timeout);
                    ridLockedList.add(rid);
                }

                return true;
            } catch (IllegalLockStateException e) {
                if (lockType == LockType.WRITE) {
                    release(database.getId(), LockType.INTENTION_EXCLUSIVE);
                } else {
                    release(database.getId(), LockType.INTENTION_SHARED);
                }

                for (Long rid : ridLockedList) {
                    release(rid, lockType);
                }
                return false;
            }
        } else {
            //Fallback to db lock
            return tryLockDatabase(database, lockType, timeout);
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void unLockTablesWithIntensiveDbLock(Database database, List<Long> tableList, LockType lockType) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        List<Long> tableListClone = new ArrayList<>(tableList);

        if (Config.lock_manager_enabled) {
            if (lockType == LockType.WRITE) {
                this.release(database.getId(), LockType.INTENTION_EXCLUSIVE);
            } else {
                this.release(database.getId(), LockType.INTENTION_SHARED);
            }
            Collections.sort(tableListClone);
            for (Long rid : tableListClone) {
                this.release(rid, lockType);
            }
        } else {
            //Fallback to db lock
            unLockDatabase(database, lockType);
        }
    }

    public Long getWaitingForRid() {
        return waitingForRid;
    }

    public LockType getWaitingForType() {
        return waitingForType;
    }

    public Long getThreadID() {
        return threadID;
    }

    void setWaitingFor(Long rid, LockType type) {
        waitingForRid = rid;
        waitingForType = type;
    }

    void clearWaitingFor() {
        waitingForRid = null;
        waitingForType = null;
    }

    private String getStackTrace() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        StackTraceElement element = stackTrace[3];
        int lastIdx = element.getClassName().lastIndexOf(".");
        return element.getClassName().substring(lastIdx + 1) + "." + element.getMethodName() + "():" + element.getLineNumber();
    }

    @Override
    public String toString() {
        return ("(" + threadName + "|" + threadID) + ")" + " [" + stackTrace + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Locker locker = (Locker) o;
        return threadID == locker.threadID;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(threadID);
    }
}
