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

package com.starrocks.meta.lock;

import com.starrocks.catalog.Database;
import com.starrocks.common.Config;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Locker {

    public Locker() {
    }

    /**
     * Attempt to acquire a lock of 'lockType' on resourceId
     *
     * @param resourceId The resource id to lock
     * @param lockType   Then lock type requested
     * @param timeout    milliseconds to time out after if lock couldn't be obtained.
     *                   0 means block indefinitely.
     * @throws LockTimeoutException when the transaction time limit was exceeded.
     */
    public void lock(long resourceId, LockType lockType, long timeout) {
        throw new NotSupportLockException();
    }

    /**
     * Release lock
     *
     * @param resourceId The resource id of the lock to release.
     */
    public void release(long resourceId, LockType lockType) {
        throw new NotSupportLockException();
    }

    /**
     * Before the new version of LockManager is fully enabled, it is used to be compatible with the original db lock logic.
     */
    public void lockDatabase(Database database, LockType lockType) {
        if (Config.use_lock_manager) {
            assert database != null;
            lock(database.getId(), lockType, 0);
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
    public boolean isReadLockHeldByCurrentThread(Database database) {
        if (Config.use_lock_manager) {
            return true;
        } else {
            return database.isReadLockHeldByCurrentThread();
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
    public void lockTables(Database database, List<Long> tableList, LockType lockType) {
        if (Config.use_lock_manager) {
            assert !tableList.isEmpty();
            if (lockType == LockType.WRITE) {
                this.lock(database.getId(), LockType.INTENTION_EXCLUSIVE, 0);
            } else if (lockType == LockType.READ) {
                this.lock(database.getId(), LockType.INTENTION_SHARED, 0);
            }

            Collections.sort(tableList);
            for (Long rid : tableList) {
                this.lock(rid, lockType, 0);
            }
        } else {
            //Fallback to db lock
            lockDatabase(database, lockType);
        }
    }

    public boolean tryLockTables(Database database, List<Long> tableList, LockType lockType, long timeout) {
        if (Config.use_lock_manager) {
            assert !tableList.isEmpty();
            try {
                if (lockType == LockType.WRITE) {
                    this.lock(database.getId(), LockType.INTENTION_EXCLUSIVE, timeout);
                } else if (lockType == LockType.READ) {
                    this.lock(database.getId(), LockType.INTENTION_SHARED, timeout);
                }

                Collections.sort(tableList);
                for (Long rid : tableList) {
                    this.lock(rid, lockType, timeout);
                }

                return true;
            } catch (Exception e) {
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
}
