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
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Locker implements Cloneable {
    /* The rid of the lock that this locker is waiting for. */
    private Long waitingForRid;

    /* The LockType corresponding to waitingFor. */
    private LockType waitingForType;

    /* The thread that created this locker */
    private final String threadName;
    private final long threadID;

    /* The thread stack that created this locker */
    private final String stackTrace;

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
        if (Config.use_lock_manager) {
            assert database != null;
            try {
                lock(database.getId(), lockType, 0);
            } catch (IllegalLockStateException e) {
                ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
            }
        } else {
            if (lockType.isWriteLock()) {
                database.writeLock();
            } else {
                database.readLock();
            }
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public boolean tryLockDatabase(Database database, LockType lockType, long timeout) {
        if (Config.use_lock_manager) {
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
            if (lockType.isWriteLock()) {
                return database.tryWriteLock(timeout, TimeUnit.MILLISECONDS);
            } else {
                return database.tryReadLock(timeout, TimeUnit.MILLISECONDS);
            }
        }
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
        if (Config.use_lock_manager) {
            assert database != null;
            release(database.getId(), lockType);
        } else {
            if (lockType.isWriteLock()) {
                database.writeUnlock();
            } else {
                database.readUnlock();
            }
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public boolean isWriteLockHeldByCurrentThread(Database database) {
        if (Config.use_lock_manager) {
            return true;
        } else {
            return database.isWriteLockHeldByCurrentThread();
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void lockTablesWithIntensiveDbLock(Database database, List<Long> tableList, LockType lockType) {
        assert lockType == LockType.WRITE || lockType == LockType.READ;
        if (Config.use_lock_manager) {
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

    public boolean tryLockTablesWithIntensiveDbLock(Database database, List<Long> tableList, LockType lockType, long timeout) {
        assert lockType == LockType.WRITE || lockType == LockType.READ;
        if (Config.use_lock_manager) {
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
            return tryLockDatabase(database, lockType, timeout);
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void unLockTables(Database database, List<Long> tableList, LockType lockType) {
        if (Config.use_lock_manager) {
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
