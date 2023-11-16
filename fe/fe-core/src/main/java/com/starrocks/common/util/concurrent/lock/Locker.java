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
import com.starrocks.common.util.concurrent.LockUtils;
import com.starrocks.common.util.concurrent.LockUtils.SlowLockLogStats;
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

    private final Map<Long, SlowLockLogStats> resourceIdToSlowLockLogStats = new HashMap<>();

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

    // --------------- Database locking API ---------------

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void lockDatabase(Database database, LockType lockType) {
        if (Config.lock_manager_enabled) {
            assert database != null;
            try {
                lock(database.getId(), lockType, 0);
            } catch (IllegalLockStateException e) {
                ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
            }
        } else {
            QueryableReentrantReadWriteLock rwLock = database.getRwLock();
            resourceIdToSlowLockLogStats.putIfAbsent(database.getId(), new SlowLockLogStats());
            if (lockType.isWriteLock()) {
                LockUtils.dbWriteLock(rwLock, database.getId(), database.getFullName(),
                        getSlowLockLogStatsByResourceId(database.getId()));
            } else {
                LockUtils.dbReadLock(rwLock, database.getId(), database.getFullName(),
                        getSlowLockLogStatsByResourceId(database.getId()));
            }
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public boolean lockDatabaseAndCheckExist(Database database, LockType lockType) {
        resourceIdToSlowLockLogStats.putIfAbsent(database.getId(), new SlowLockLogStats());
        if (lockType.isWriteLock()) {
            LockUtils.dbWriteLock(database.getRwLock(), database.getId(),
                    database.getFullName(), getSlowLockLogStatsByResourceId(database.getId()));
            return checkExistenceInLock(database, false);
        } else {
            LockUtils.dbReadLock(database.getRwLock(), database.getId(),
                    database.getFullName(), getSlowLockLogStatsByResourceId(database.getId()));
            return checkExistenceInLock(database, true);
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public boolean tryLockDatabase(Database database, LockType lockType, long timeout, TimeUnit unit) {
        if (Config.lock_manager_enabled) {
            assert database != null;
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
            Preconditions.checkArgument(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
            boolean acquired = false;
            QueryableReentrantReadWriteLock rwLock = database.getRwLock();
            resourceIdToSlowLockLogStats.putIfAbsent(database.getId(), new SlowLockLogStats());
            try {
                if (lockType.isWriteLock()) {
                    acquired = LockUtils.tryDbWriteLock(rwLock, timeout, unit, database.getId(),
                            database.getFullName(), getSlowLockLogStatsByResourceId(database.getId()));
                } else {
                    acquired = LockUtils.tryDbReadLock(rwLock, timeout, unit, database.getId(),
                            database.getFullName(), getSlowLockLogStatsByResourceId(database.getId()));
                }
            } catch (InterruptedException e) {
                LOG.warn("failed to try {} lock on db[{}-{}]",
                        lockType, database.getFullName(), database.getId(), e);
                Thread.currentThread().interrupt();
            }
            return acquired;
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void unLockDatabase(Database database, LockType lockType) {
        if (Config.lock_manager_enabled) {
            assert database != null;
            release(database.getId(), lockType);
        } else {
            QueryableReentrantReadWriteLock rwLock = database.getRwLock();
            if (lockType.isWriteLock()) {
                rwLock.exclusiveUnlock();
            } else {
                rwLock.sharedUnlock();
            }
            resourceIdToSlowLockLogStats.remove(database.getId());
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public boolean isDbWriteLockHeldByCurrentThread(Database database) {
        if (Config.lock_manager_enabled) {
            return true;
        } else {
            return database.getRwLock().isWriteLockHeldByCurrentThread();
        }
    }

    private boolean checkExistenceInLock(Database database, boolean isInReadLock) {
        if (database.isExist()) {
            return true;
        } else {
            if (isInReadLock) {
                unLockDatabase(database, LockType.READ);
            } else {
                unLockDatabase(database, LockType.WRITE);
            }
            return false;
        }
    }

    private SlowLockLogStats getSlowLockLogStatsByResourceId(long resourceId) {
        return resourceIdToSlowLockLogStats.get(resourceId);
    }

    // --------------- Table locking API ---------------

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void lockTablesWithIntensiveDbLock(Database database, List<Long> tableList, LockType lockType) {
        assert lockType == LockType.WRITE || lockType == LockType.READ;
        if (Config.lock_manager_enabled) {
            assert !tableList.isEmpty();

            try {
                if (lockType == LockType.WRITE) {
                    this.lock(database.getId(), LockType.INTENTION_EXCLUSIVE, 0);
                } else if (lockType == LockType.READ) {
                    this.lock(database.getId(), LockType.INTENTION_SHARED, 0);
                }

                Collections.sort(tableList);
                for (Long rid : tableList) {
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

    public boolean tryLockTablesWithIntensiveDbLock(Database database, List<Long> tableList, LockType lockType,
                                                    long timeout, TimeUnit unit) {
        assert lockType == LockType.WRITE || lockType == LockType.READ;
        if (Config.lock_manager_enabled) {
            assert !tableList.isEmpty();

            try {
                if (lockType == LockType.WRITE) {
                    this.lock(database.getId(), LockType.INTENTION_EXCLUSIVE, timeout);
                } else if (lockType == LockType.READ) {
                    this.lock(database.getId(), LockType.INTENTION_SHARED, timeout);
                }
            } catch (IllegalLockStateException e) {
                return false;
            }

            List<Long> ridLockedList = new ArrayList<>();
            try {
                Collections.sort(tableList);
                for (Long rid : tableList) {
                    this.lock(rid, lockType, timeout);
                    ridLockedList.add(rid);
                }

                return true;
            } catch (IllegalLockStateException e) {
                if (lockType == LockType.WRITE) {
                    release(database.getId(), LockType.INTENTION_EXCLUSIVE);
                } else if (lockType == LockType.READ) {
                    release(database.getId(), LockType.INTENTION_SHARED);
                }

                for (Long rid : ridLockedList) {
                    release(rid, lockType);
                }
                return false;
            }
        } else {
            //Fallback to db lock
            return tryLockDatabase(database, lockType, timeout, unit);
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void unLockTablesWithIntensiveDbLock(Database database, List<Long> tableList, LockType lockType) {
        if (Config.lock_manager_enabled) {
            if (lockType == LockType.WRITE) {
                this.release(database.getId(), LockType.INTENTION_EXCLUSIVE);
            } else if (lockType == LockType.READ) {
                this.release(database.getId(), LockType.INTENTION_SHARED);
            }
            Collections.sort(tableList);
            for (Long rid : tableList) {
                this.release(rid, lockType);
            }
        } else {
            //Fallback to db lock
            unLockDatabase(database, lockType);
        }
    }
}
