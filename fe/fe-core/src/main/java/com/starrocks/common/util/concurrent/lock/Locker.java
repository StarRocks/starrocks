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
import com.starrocks.common.util.LogUtil;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.concurrent.QueryableReentrantReadWriteLock;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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

    //private final String lockerStackTrace;
    private final long threadId;
    private final String threadName;

    /* The thread that request lock. */
    private final Thread lockerThread;

    private final Map<Long, Long> lastSlowLockLogTimeMap = new HashMap<>();

    public Locker() {
        this.waitingForRid = null;
        this.waitingForType = null;

        /* Save the thread used to create the locker and thread stack. */
        this.lockerThread = Thread.currentThread();
        this.threadId = lockerThread.getId();
        this.threadName = lockerThread.getName();
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

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void lockDatabase(Database database, LockType lockType) {
        if (Config.lock_manager_enabled) {
            Preconditions.checkNotNull(database);
            try {
                lock(database.getId(), lockType, 0);
            } catch (LockException e) {
                throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
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
            } catch (LockException e) {
                throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
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
            lockDatabase(db, lockType);
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
            unLockDatabase(db, lockType);
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
                            "former: {}, current stack trace: {}", type, databaseId, fullQualifiedName, endMs - startMs,
                    threadDump, LogUtil.getCurrentStackTrace());
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
        long milliTimeout = timeout;
        if (!unit.equals(TimeUnit.MILLISECONDS)) {
            milliTimeout = TimeUnit.MILLISECONDS.convert(Duration.of(timeout, unit.toChronoUnit()));
        }
        try {
            for (Database db : dbs) {
                if (!tryLockDatabase(db, lockType, milliTimeout)) {
                    return false;
                }
                lockedDbs.add(db);
            }
            isLockSuccess = true;
        } finally {
            if (!isLockSuccess) {
                lockedDbs.stream().forEach(t -> unLockDatabase(t, lockType));
            }
        }
        return isLockSuccess;
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

    private boolean checkExistenceInLock(Database database, LockType lockType) {
        if (database.isExist()) {
            return true;
        } else {
            unLockDatabase(database, lockType);
            return false;
        }
    }

    // --------------- Table locking API ---------------

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void lockTablesWithIntensiveDbLock(Database database, List<Long> tableList, LockType lockType) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        List<Long> tableListClone = new ArrayList<>(tableList);
        if (Config.lock_manager_enabled && Config.lock_manager_enable_using_fine_granularity_lock) {
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
            } catch (LockException e) {
                throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
            }
        } else {
            //Fallback to db lock
            lockDatabase(database, lockType);
        }
    }

    /**
     * No need to release lock explicitly, it will be released automatically when the locker failed.
     */
    public boolean tryLockTablesWithIntensiveDbLock(Database database, List<Long> tableList, LockType lockType,
                                                    long timeout, TimeUnit unit) {
        long timeoutMillis = timeout;
        if (!unit.equals(TimeUnit.MILLISECONDS)) {
            timeoutMillis = TimeUnit.MILLISECONDS.convert(Duration.of(timeout, unit.toChronoUnit()));
        }
        return tryLockTablesWithIntensiveDbLock(database, tableList, lockType, timeoutMillis);
    }

    public boolean tryLockTablesWithIntensiveDbLock(Database database, List<Long> tableList, LockType lockType, long timeout) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        List<Long> tableListClone = new ArrayList<>(tableList);
        if (Config.lock_manager_enabled && Config.lock_manager_enable_using_fine_granularity_lock) {
            try {
                if (lockType == LockType.WRITE) {
                    this.lock(database.getId(), LockType.INTENTION_EXCLUSIVE, timeout);
                } else {
                    this.lock(database.getId(), LockType.INTENTION_SHARED, timeout);
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
        if (Config.lock_manager_enabled && Config.lock_manager_enable_using_fine_granularity_lock) {
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
            lockTableWithIntensiveDbLock(database, tableId, lockType);
            return checkExistenceInLock(database, tableId, lockType);
        } else {
            //Fallback to db lock
            lockDatabase(database, lockType);
            return checkExistenceInLock(database, lockType);
        }
    }

    private boolean checkExistenceInLock(Database database, long tableId, LockType lockType) {
        if (database.isExist()) {
            return true;
        } else {
            unLockTablesWithIntensiveDbLock(database, ImmutableList.of(tableId), lockType);
            return false;
        }
    }

    public void unLockTableWithIntensiveDbLock(Database database, Table table, LockType lockType) {
        unLockTablesWithIntensiveDbLock(database, ImmutableList.of(table.getId()), lockType);
    }

    public void unLockDatabase(Database database, Long tableId, LockType lockType) {
        unLockTablesWithIntensiveDbLock(database, ImmutableList.of(tableId), lockType);
    }

    /**
     * Lock table with intensive db lock.
     *
     * @param database db for intensive db lock
     * @param tableId  table to be locked
     * @param lockType lock type
     */
    public void lockTableWithIntensiveDbLock(Database database, Long tableId, LockType lockType) {
        Preconditions.checkState(lockType.equals(LockType.READ) || lockType.equals(LockType.WRITE));
        if (Config.lock_manager_enabled && Config.lock_manager_enable_using_fine_granularity_lock) {
            try {
                if (lockType == LockType.WRITE) {
                    this.lock(database.getId(), LockType.INTENTION_EXCLUSIVE, 0);
                } else {
                    this.lock(database.getId(), LockType.INTENTION_SHARED, 0);
                }
                this.lock(tableId, lockType, 0);
            } catch (LockException e) {
                throw ErrorReportException.report(ErrorCode.ERR_LOCK_ERROR, e.getMessage());
            }
        } else {
            //Fallback to db lock
            lockDatabase(database, lockType);
        }
    }

    /**
     * Try to lock a database and a table id with intensive db lock.
     *
     * @return try if try lock success, false otherwise.
     */
    public boolean tryLockTableWithIntensiveDbLock(Database db, Table table, LockType lockType, long timeout, TimeUnit unit) {
        return tryLockTableWithIntensiveDbLock(db, table.getId(), lockType, timeout, unit);
    }

    /**
     * Try lock database and table id with intensive db lock.
     *
     * @return try if try lock success, false otherwise.
     */
    public boolean tryLockTableWithIntensiveDbLock(Database db, Long tableId, LockType lockType, long timeout, TimeUnit unit) {
        return tryLockTablesWithIntensiveDbLock(db, ImmutableList.of(tableId), lockType, timeout, unit);
    }

    /**
     * Try to lock multi database and tables with intensive db lock.
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
                if (!tryLockTablesWithIntensiveDbLock(database, new ArrayList<>(entry.getValue()),
                        lockType, timeout, unit)) {
                    return false;
                }
                lockedDbs.add(database);
            }
            isLockSuccess = true;
        } finally {
            if (!isLockSuccess) {
                for (Database database : lockedDbs) {
                    unLockTablesWithIntensiveDbLock(database, new ArrayList<>(tables.get(database.getId())), lockType);
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
            locker.unLockTablesWithIntensiveDbLock(database, tableIds, lockType);
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
