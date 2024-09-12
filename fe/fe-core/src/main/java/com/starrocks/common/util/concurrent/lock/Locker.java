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
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.util.concurrent.LockUtils;
import com.starrocks.common.util.concurrent.QueryableReentrantReadWriteLock;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Locker {
    private static final Logger LOG = LogManager.getLogger(Locker.class);

    /* The rid of the lock that this locker is waiting for. */
    private Long waitingForRid;

    /* The LockType corresponding to waitingFor. */
    private LockType waitingForType;

    /* The time when the current Locker starts to request for the lock. */
    private long lockRequestTimeMs;

    /* The thread stack that created this locker. */
    private final String lockerStackTrace;

    /* The thread that request lock. */
    private final Thread lockerThread;

    public Locker() {
        this.waitingForRid = null;
        this.waitingForType = null;
        /* Save the thread used to create the locker and thread stack. */
        this.lockerThread = Thread.currentThread();
        this.lockerStackTrace = getStackTrace(this.lockerThread);
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
        LOG.debug(this + " | LockManager request lock : rid " + rid + ", lock type " + lockType);
        lockManager.lock(rid, this, lockType, timeout);
        LOG.debug(this + " | LockManager acquire lock : rid " + rid + ", lock type " + lockType);
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
        LOG.debug(this + " | LockManager release lock : rid " + rid + ", lock type " + lockType);
        try {
            lockManager.release(rid, this, lockType);
        } catch (LockException e) {
            throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
        }
    }

    // --------------- Database locking API ---------------

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */

    public void lockDatabase(Long dbId, LockType lockType) {
        if (Config.lock_manager_enabled) {
            Preconditions.checkState(dbId != null);
            try {
                lock(dbId, lockType, 0);
            } catch (LockException e) {
                throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
            }
        } else {
            Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(dbId);
            if (database == null) {
                // Database has been dropped
                return;
            }
            QueryableReentrantReadWriteLock rwLock = database.getRwLock();
            if (lockType.isWriteLock()) {
                LockUtils.dbWriteLock(rwLock, database.getId(), database.getFullName(), database.getSlowLockLogStats());
            } else {
                LockUtils.dbReadLock(rwLock, database.getId(), database.getFullName(), database.getSlowLockLogStats());
            }
        }
    }

    public void lockDatabaseWithIntendLock(Database database, LockType lockType) {
        if (Config.lock_manager_enabled) {
            Preconditions.checkState(database != null);
            try {
                lock(database.getId(), lockType, 0);
            } catch (LockException e) {
                throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
            }
        } else {
            QueryableReentrantReadWriteLock rwLock = database.getRwLock();
            if (lockType == LockType.INTENTION_EXCLUSIVE) {
                LockUtils.dbWriteLock(rwLock, database.getId(), database.getFullName(), database.getSlowLockLogStats());
            } else {
                LockUtils.dbReadLock(rwLock, database.getId(), database.getFullName(), database.getSlowLockLogStats());
            }
        }
    }

    public void lockTable(Table table, LockType lockType) {
        if (Config.lock_manager_enabled) {
            Preconditions.checkState(table != null);
            try {
                lock(table.getId(), lockType, 0);
            } catch (LockException e) {
                throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
            }
        } else {
            //
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public boolean lockDatabaseAndCheckExist(Database database, LockType lockType) {
        if (Config.lock_manager_enabled) {
            lockDatabase(database.getId(), lockType);
        } else {
            if (lockType.isWriteLock()) {
                LockUtils.dbWriteLock(database.getRwLock(), database.getId(),
                        database.getFullName(), database.getSlowLockLogStats());
            } else {
                LockUtils.dbReadLock(database.getRwLock(), database.getId(),
                        database.getFullName(), database.getSlowLockLogStats());
            }
        }
        return checkExistenceInLock(database, lockType);
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public boolean tryLockDatabase(Long dbId, LockType lockType, long timeout, TimeUnit unit) {
        if (Config.lock_manager_enabled) {
            Preconditions.checkState(dbId != null);
            try {
                lock(dbId, lockType, timeout);
                return true;
            } catch (LockTimeoutException e) {
                return false;
            } catch (LockException e) {
                throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
            }
        } else {
            Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(dbId);
            if (database == null) {
                // Database has been dropped
                return true;
            }
            Preconditions.checkArgument(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
            boolean acquired = false;
            QueryableReentrantReadWriteLock rwLock = database.getRwLock();
            try {
                if (lockType.isWriteLock()) {
                    acquired = LockUtils.tryDbWriteLock(rwLock, timeout, unit, database.getId(),
                            database.getFullName(), database.getSlowLockLogStats());
                } else {
                    acquired = LockUtils.tryDbReadLock(rwLock, timeout, unit, database.getId(),
                            database.getFullName(), database.getSlowLockLogStats());
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
     * FYI: should deduplicate dbs before call this api.
     * lock databases in ascending order of id.
     *
     * @param dbs:      databases to be locked
     * @param lockType: lock type
     */
    public void lockDatabases(List<Database> dbs, LockType lockType) {
        if (dbs == null) {
            return;
        }
        dbs.sort(Comparator.comparingLong(Database::getId));
        for (Database db : dbs) {
            lockDatabase(db.getId(), lockType);
        }
    }

    /**
     * FYI: should deduplicate dbs before call this api.
     *
     * @param dbs:      databases to be locked
     * @param lockType: lock type
     */
    public void unlockDatabases(List<Database> dbs, LockType lockType) {
        if (dbs == null) {
            return;
        }
        for (Database db : dbs) {
            unLockDatabase(db.getId(), lockType);
        }
    }

    /**
     * Try to lock databases in ascending order of id.
     *
     * @return: true if all databases are locked successfully, false otherwise.
     */
    public boolean tryLockDatabases(List<Database> dbs, LockType lockType, long timeout, TimeUnit unit) {
        if (dbs == null) {
            return false;
        }
        dbs.sort(Comparator.comparingLong(Database::getId));
        List<Database> lockedDbs = Lists.newArrayList();
        boolean isLockSuccess = false;
        try {
            for (Database db : dbs) {
                if (!tryLockDatabase(db.getId(), lockType, timeout, unit)) {
                    return false;
                }
                lockedDbs.add(db);
            }
            isLockSuccess = true;
        } finally {
            if (!isLockSuccess) {
                lockedDbs.stream().forEach(t -> unLockDatabase(t.getId(), lockType));
            }
        }
        return isLockSuccess;
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void unLockDatabase(Long dbId, LockType lockType) {
        if (Config.lock_manager_enabled) {
            Preconditions.checkState(dbId != null);
            release(dbId, lockType);
        } else {
            Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(dbId);
            if (database == null) {
                // Database has been dropped
                return;
            }
            QueryableReentrantReadWriteLock rwLock = database.getRwLock();
            if (lockType.isWriteLock()) {
                rwLock.exclusiveUnlock();
            } else {
                rwLock.sharedUnlock();
            }
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
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void lockTablesWithIntensiveDbLock(Long dbId, List<Long> tableList, LockType lockType) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        List<Long> tableListClone = new ArrayList<>(tableList);
        if (Config.lock_manager_enabled && Config.lock_manager_enable_using_fine_granularity_lock) {
            try {
                if (lockType == LockType.WRITE) {
                    this.lock(dbId, LockType.INTENTION_EXCLUSIVE, 0);
                } else {
                    this.lock(dbId, LockType.INTENTION_SHARED, 0);
                }

                Collections.sort(tableListClone);
                for (Long rid : tableListClone) {
                    this.lock(rid, lockType, 0);
                }
            } catch (LockException e) {
                throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
            }
        } else {
            //Fallback to db lock
            lockDatabase(dbId, lockType);
        }
    }

    public boolean tryLockTablesWithIntensiveDbLock(Long dbId, List<Long> tableList, LockType lockType,
                                                    long timeout, TimeUnit unit) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        List<Long> tableListClone = new ArrayList<>(tableList);
        if (Config.lock_manager_enabled && Config.lock_manager_enable_using_fine_granularity_lock) {
            try {
                if (lockType == LockType.WRITE) {
                    this.lock(dbId, LockType.INTENTION_EXCLUSIVE, timeout);
                } else {
                    this.lock(dbId, LockType.INTENTION_SHARED, timeout);
                }
            } catch (LockException e) {
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
            } catch (LockException e) {
                if (lockType == LockType.WRITE) {
                    release(dbId, LockType.INTENTION_EXCLUSIVE);
                } else {
                    release(dbId, LockType.INTENTION_SHARED);
                }

                for (Long rid : ridLockedList) {
                    release(rid, lockType);
                }
                return false;
            }
        } else {
            // Fallback to db lock
            return tryLockDatabase(dbId, lockType, timeout, unit);
        }
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void unLockTablesWithIntensiveDbLock(Long dbId, List<Long> tableList, LockType lockType) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        List<Long> tableListClone = new ArrayList<>(tableList);
        if (Config.lock_manager_enabled && Config.lock_manager_enable_using_fine_granularity_lock) {
            if (lockType == LockType.WRITE) {
                this.release(dbId, LockType.INTENTION_EXCLUSIVE);
            } else {
                this.release(dbId, LockType.INTENTION_SHARED);
            }
            Collections.sort(tableListClone);
            for (Long rid : tableListClone) {
                this.release(rid, lockType);
            }
        } else {
            //Fallback to db lock
            unLockDatabase(dbId, lockType);
        }
    }

    /**
     * Lock database and table with intensive db lock.
     *
     * @param database database for intensive db lock
     * @param table    table to be locked
     * @param lockType lock type
     * @return true if database exits, false otherwise
     */
    public boolean lockDatabaseAndCheckExist(Database database, Table table, LockType lockType) {
        return lockDatabaseAndCheckExist(database, table.getId(), lockType);
    }

    /**
     * Lock database and table with intensive db lock.
     */
    public boolean lockDatabaseAndCheckExist(Database database, long tableId, LockType lockType) {
        if (Config.lock_manager_enabled) {
            lockTableWithIntensiveDbLock(database.getId(), tableId, lockType);
            return checkExistenceInLock(database, tableId, lockType);
        } else {
            if (lockType.isWriteLock()) {
                LockUtils.dbWriteLock(database.getRwLock(), database.getId(),
                        database.getFullName(), database.getSlowLockLogStats());
            } else {
                LockUtils.dbReadLock(database.getRwLock(), database.getId(),
                        database.getFullName(), database.getSlowLockLogStats());
            }
            return checkExistenceInLock(database, lockType);
        }
    }

    private boolean checkExistenceInLock(Database database, long tableId, LockType lockType) {
        if (database.isExist()) {
            return true;
        } else {
            unLockTablesWithIntensiveDbLock(database.getId(), ImmutableList.of(tableId), lockType);
            return false;
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
        if (Config.lock_manager_enabled && Config.lock_manager_enable_using_fine_granularity_lock) {
            try {
                if (lockType == LockType.WRITE) {
                    this.lock(dbId, LockType.INTENTION_EXCLUSIVE, 0);
                } else {
                    this.lock(dbId, LockType.INTENTION_SHARED, 0);
                }
                this.lock(tableId, lockType, 0);
            } catch (LockException e) {
                throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
            }
        } else {
            //Fallback to db lock
            lockDatabase(dbId, lockType);
        }
    }

    /**
     * Try lock database and table id with intensive db lock.
     *
     * @return try if try lock success, false otherwise.
     */
    public boolean tryLockTableWithIntensiveDbLock(Long dbId, Long tableId, LockType lockType, long timeout, TimeUnit unit) {
        boolean isLockSuccess = false;
        try {
            if (!tryLockTablesWithIntensiveDbLock(dbId, ImmutableList.of(tableId), lockType, timeout, unit)) {
                return false;
            }
            isLockSuccess = true;
        } finally {
            if (!isLockSuccess) {
                unLockTablesWithIntensiveDbLock(dbId, ImmutableList.of(tableId), lockType);
            }
        }
        return true;
    }

    /**
     * Try lock database and tables with intensive db lock.
     *
     * @return try if try lock success, false otherwise.
     */
    public boolean tryLockTableWithIntensiveDbLock(LockParams lockParams, LockType lockType, long timeout, TimeUnit unit) {
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
                    unLockTablesWithIntensiveDbLock(database.getId(), new ArrayList<>(tables.get(database.getId())), lockType);
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
        Locker locker = new Locker();
        for (Map.Entry<Long, Set<Long>> entry : tables.entrySet()) {
            Database database = dbs.get(entry.getKey());
            List<Long> tableIds = new ArrayList<>(entry.getValue());
            locker.unLockTablesWithIntensiveDbLock(database.getId(), tableIds, lockType);
        }
    }

    public Long getWaitingForRid() {
        return waitingForRid;
    }

    public LockType getWaitingForType() {
        return waitingForType;
    }

    public Long getThreadID() {
        return lockerThread.getId();
    }

    public String getThreadName() {
        return lockerThread.getName();
    }

    void setWaitingFor(Long rid, LockType type) {
        waitingForRid = rid;
        waitingForType = type;
    }

    void clearWaitingFor() {
        waitingForRid = null;
        waitingForType = null;
    }

    private String getStackTrace(Thread thread) {
        StackTraceElement[] stackTrace = thread.getStackTrace();
        StackTraceElement element = stackTrace[3];
        int lastIdx = element.getClassName().lastIndexOf(".");
        return element.getClassName().substring(lastIdx + 1) + "." + element.getMethodName() + "():" + element.getLineNumber();
    }

    public String getLockerStackTrace() {
        return lockerStackTrace;
    }

    public Thread getLockerThread() {
        return lockerThread;
    }

    public long getLockRequestTimeMs() {
        return lockRequestTimeMs;
    }

    public void setLockRequestTimeMs(long lockRequestTimeMs) {
        this.lockRequestTimeMs = lockRequestTimeMs;
    }

    @Override
    public String toString() {
        return ("(" + lockerThread.getName() + "|" + lockerThread.getId()) + ")" + " [" + lockerStackTrace + "]";
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
        return lockerThread.getId() == locker.lockerThread.getId();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(lockerThread.getId());
    }
}
