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

import com.google.api.client.util.Lists;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Locker {
    private static final Logger LOG = LogManager.getLogger(Locker.class);

    /* The rid of the lock that this locker is waiting for. */
    private Long waitingForRid;

    /* The LockType corresponding to waitingFor. */
    private LockType waitingForType;

    /* The time when the current Locker starts to request for the lock. */
    private long lockRequestTimeMs;

    //private final String lockerStackTrace;
    private final long threadId;
    private final String threadName;

    /* The thread that request lock. */
    private final Thread lockerThread;

    // The queryId that request lock.
    // maybe null if not query
    private UUID queryId;

    public Locker() {
        this.waitingForRid = null;
        this.waitingForType = null;

        /* Save the thread used to create the locker and thread stack. */
        this.lockerThread = Thread.currentThread();
        this.threadId = lockerThread.getId();
        this.threadName = lockerThread.getName();
    }

    public Locker(UUID queryId) {
        this();
        this.queryId = queryId;
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
    public void lock(long rid, LockType lockType, long timeout) throws LockException {
        if (timeout < 0) {
            throw new NotSupportLockException("lock timeout value cannot be less than 0");
        }

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        lockManager.lock(rid, this, lockType, timeout);
    }

    public void lock(long rid, LockType lockType) throws LockException {
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
        try {
            lockManager.release(rid, this, lockType);
        } catch (LockException e) {
            throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
        }
    }

    public void setQueryId(UUID queryID) {
        this.queryId = queryID;
    }

    public UUID getQueryId() {
        return queryId;
    }

    // --------------- Database locking API ---------------

    /**
     * Acquire the database lock of the given type through {@link LockManager}.
     */
    public void lockDatabase(Long dbId, LockType lockType) {
        Preconditions.checkState(dbId != null);
        try {
            lock(dbId, lockType, 0);
        } catch (LockException e) {
            throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
        }
    }

    /**
     * Acquire the database lock and return whether the database still exists after the lock is held.
     */
    public boolean lockDatabaseAndCheckExist(Database database, LockType lockType) {
        lockDatabase(database.getId(), lockType);
        return checkExistenceInLock(database, lockType);
    }

    /**
     * Lock table with intensive db lock and check db's existence.
     */
    public boolean lockTableAndCheckDbExist(Database database, long tableId, LockType lockType) {
        lockTableWithIntensiveDbLock(database.getId(), tableId, lockType);
        return checkExistenceInLock(database, tableId, lockType);
    }

    private boolean checkExistenceInLock(Database database, long tableId, LockType lockType) {
        if (database.isExist()) {
            return true;
        } else {
            unLockTablesWithIntensiveDbLock(database.getId(), ImmutableList.of(tableId), lockType);
            return false;
        }
    }

    /**
     * Try to acquire the database lock of the given type through {@link LockManager} within the timeout.
     */
    public boolean tryLockDatabase(Long dbId, LockType lockType, long timeout, TimeUnit unit) {
        Preconditions.checkState(dbId != null);
        try {
            lock(dbId, lockType, timeout);
            return true;
        } catch (LockTimeoutException e) {
            return false;
        } catch (LockException e) {
            throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
        }
    }

    /**
     * Release the database lock of the given type through {@link LockManager}.
     */
    public void unLockDatabase(Long dbId, LockType lockType) {
        Preconditions.checkState(dbId != null);
        release(dbId, lockType);
    }

    /**
     * With {@link LockManager}, db write-lock ownership is tracked per rid rather than per thread,
     * so this thread-affinity check always reports true.
     */
    public boolean isDbWriteLockHeldByCurrentThread(Database database) {
        return true;
    }

    private boolean checkExistenceInLock(Database database, LockType lockType) {
        if (database.isExist()) {
            return true;
        } else {
            unLockDatabase(database.getId(), lockType);
            return false;
        }
    }

    // --------------- Table locking API ---------------

    /**
     * Acquire an intention lock on the database followed by the locks on the given tables.
     */
    public void lockTablesWithIntensiveDbLock(Long dbId, List<Long> tableList, LockType lockType) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        List<Long> tableListClone = new ArrayList<>(tableList);
        LockType intentionType = (lockType == LockType.WRITE)
                ? LockType.INTENTION_EXCLUSIVE
                : LockType.INTENTION_SHARED;
        boolean dbLockHeld = false;
        List<Long> ridLockedList = new ArrayList<>();
        boolean success = false;
        try {
            this.lock(dbId, intentionType, 0);
            dbLockHeld = true;

            Collections.sort(tableListClone);
            for (Long rid : tableListClone) {
                this.lock(rid, lockType, 0);
                ridLockedList.add(rid);
            }
            success = true;
        } catch (LockException e) {
            throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
        } finally {
            // Roll back any locks already acquired on partial failure
            // (LockException, deadlock victim, or any unchecked exception)
            // so a stranded DB intention or table lock does not block
            // subsequent DB-WRITE operations indefinitely.
            // rollbackPartialIntensiveLock releases tables before DB intention
            // to preserve hierarchical-lock invariants.
            if (!success) {
                rollbackPartialIntensiveLock(dbId, intentionType, dbLockHeld, ridLockedList, lockType);
            }
        }
    }

    /**
     * Best-effort rollback of a partial intensive-lock acquisition. Releases each
     * already-acquired lock and swallows any errors raised by individual release
     * calls so that the original LockException (the actual cause of the rollback)
     * surfaces to the caller instead of being masked by a release-side error.
     */
    private void rollbackPartialIntensiveLock(Long dbId, LockType intentionType,
                                              boolean dbLockHeld, List<Long> ridLockedList, LockType tableLockType) {
        for (Long rid : ridLockedList) {
            try {
                this.release(rid, tableLockType);
            } catch (Exception releaseEx) {
                LOG.warn("Failed to release table lock {} during partial-acquire rollback", rid, releaseEx);
            }
        }
        if (dbLockHeld) {
            try {
                this.release(dbId, intentionType);
            } catch (Exception releaseEx) {
                LOG.warn("Failed to release DB intention lock {} during partial-acquire rollback", dbId, releaseEx);
            }
        }
    }

    /**
     * No need to release lock explicitly, it will be released automatically when the locker failed.
     */
    public boolean tryLockTablesWithIntensiveDbLock(Long dbId, List<Long> tableList, LockType lockType,
                                                    long timeout, TimeUnit unit) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        List<Long> tableListClone = new ArrayList<>(tableList);
        LockType intentionType = (lockType == LockType.WRITE)
                ? LockType.INTENTION_EXCLUSIVE
                : LockType.INTENTION_SHARED;
        boolean dbLockHeld = false;
        List<Long> ridLockedList = new ArrayList<>();
        boolean success = false;
        try {
            this.lock(dbId, intentionType, timeout);
            dbLockHeld = true;

            Collections.sort(tableListClone);
            for (Long rid : tableListClone) {
                this.lock(rid, lockType, timeout);
                ridLockedList.add(rid);
            }
            success = true;
            return true;
        } catch (LockException e) {
            return false;
        } finally {
            // Roll back any partial state on LockException (success == false) or
            // on any unchecked exception that propagates out before success was
            // set; otherwise the DB intention or already-acquired table locks
            // would leak. rollbackPartialIntensiveLock releases tables before DB
            // intention to preserve hierarchical-lock invariants.
            if (!success) {
                rollbackPartialIntensiveLock(dbId, intentionType, dbLockHeld, ridLockedList, lockType);
            }
        }
    }

    /**
     * Release the table locks and the database intention lock acquired by the intensive-db-lock API.
     */
    public void unLockTablesWithIntensiveDbLock(Long dbId, List<Long> tableList, LockType lockType) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        List<Long> tableListClone = new ArrayList<>(tableList);
        // Release in reverse of acquire order: tables first, then DB intention.
        // Releasing the DB intention while still holding table locks would leave
        // a window for a DROP-DATABASE-style X-on-DB to slip in and invalidate
        // the table the caller is still operating on.
        Collections.sort(tableListClone);
        for (Long rid : tableListClone) {
            this.release(rid, lockType);
        }
        if (lockType == LockType.WRITE) {
            this.release(dbId, LockType.INTENTION_EXCLUSIVE);
        } else {
            this.release(dbId, LockType.INTENTION_SHARED);
        }
    }

    public void unLockTableWithIntensiveDbLock(Long dbId, Long tableId, LockType lockType) {
        unLockTablesWithIntensiveDbLock(dbId, ImmutableList.of(tableId), lockType);
    }

    /**
     * Lock table with intensive db lock.
     *
     * @param dbId     db for intensive db lock
     * @param tableId  table to be locked
     * @param lockType lock type
     */
    public void lockTableWithIntensiveDbLock(Long dbId, Long tableId, LockType lockType) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        LockType intentionType = (lockType == LockType.WRITE)
                ? LockType.INTENTION_EXCLUSIVE
                : LockType.INTENTION_SHARED;
        boolean dbLockHeld = false;
        try {
            this.lock(dbId, intentionType, 0);
            dbLockHeld = true;
            this.lock(tableId, lockType, 0);
        } catch (LockException e) {
            // Roll back the DB intention lock if step 1 succeeded but the
            // table-lock acquisition failed (e.g. deadlock victim, interrupt).
            // Otherwise the caller would leak an IS/IX lock on the DB.
            rollbackPartialIntensiveLock(dbId, intentionType, dbLockHeld, Collections.emptyList(), lockType);
            throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
        }
    }

    /**
     * Try to lock a database and a table id with intensive db lock.
     *
     * @return try if try lock success, false otherwise.
     */
    public boolean tryLockTableWithIntensiveDbLock(Long dbId, Long tableId, LockType lockType, long timeout,
                                                   TimeUnit unit) {
        return tryLockTablesWithIntensiveDbLock(dbId, ImmutableList.of(tableId), lockType, timeout, unit);
    }

    /**
     * Try to lock multi database and tables with intensive db lock.
     *
     * @return try if try lock success, false otherwise.
     */
    public boolean tryLockTableWithIntensiveDbLock(LockParams lockParams, LockType lockType, long timeout,
                                                   TimeUnit unit) {
        boolean isLockSuccess = false;
        List<Database> lockedDbs = Lists.newArrayList();
        Map<Long, Database> dbs = lockParams.getDbs();
        Map<Long, Set<Long>> tables = lockParams.getTables();
        try {
            for (Map.Entry<Long, Set<Long>> entry : tables.entrySet()) {
                Database database = dbs.get(entry.getKey());
                if (!tryLockTablesWithIntensiveDbLock(database.getId(), new ArrayList<>(entry.getValue()),
                        lockType, timeout, unit)) {
                    return false;
                }
                lockedDbs.add(database);
            }
            isLockSuccess = true;
        } finally {
            if (!isLockSuccess) {
                for (Database database : lockedDbs) {
                    unLockTablesWithIntensiveDbLock(database.getId(), new ArrayList<>(tables.get(database.getId())),
                            lockType);
                }
            }
        }
        return true;
    }

    /**
     * Unlock db and tables with intensive db lock.
     */
    public void unLockTableWithIntensiveDbLock(LockParams params, LockType lockType) {
        Map<Long, Database> dbs = params.getDbs();
        Map<Long, Set<Long>> tables = params.getTables();
        for (Map.Entry<Long, Set<Long>> entry : tables.entrySet()) {
            Database database = dbs.get(entry.getKey());
            List<Long> tableIds = new ArrayList<>(entry.getValue());
            this.unLockTablesWithIntensiveDbLock(database.getId(), tableIds, lockType);
        }
    }

    public Long getWaitingForRid() {
        return waitingForRid;
    }

    public LockType getWaitingForType() {
        return waitingForType;
    }

    void setWaitingFor(Long rid, LockType type) {
        waitingForRid = rid;
        waitingForType = type;
    }

    void clearWaitingFor() {
        waitingForRid = null;
        waitingForType = null;
    }

    public Thread getLockerThread() {
        return lockerThread;
    }

    public long getThreadId() {
        return threadId;
    }

    public String getThreadName() {
        return threadName;
    }

    public long getLockRequestTimeMs() {
        return lockRequestTimeMs;
    }

    public void setLockRequestTimeMs(long lockRequestTimeMs) {
        this.lockRequestTimeMs = lockRequestTimeMs;
    }

    @Override
    public String toString() {
        return "(" + threadName + "|" + threadId + ")";
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
        return threadId == locker.getThreadId();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(threadId);
    }
}
