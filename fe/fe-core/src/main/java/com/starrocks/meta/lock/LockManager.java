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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LockManager {
    private final int lockTablesSize;
    private final Object[] lockTableMutexes;
    private final Map<Long, Lock>[] lockTables;

    public LockManager() {
        lockTablesSize = 32;
        lockTableMutexes = new Object[lockTablesSize];
        lockTables = new Map[lockTablesSize];
        for (int i = 0; i < lockTablesSize; i++) {
            lockTableMutexes[i] = new Object();
            lockTables[i] = new HashMap<>();
        }
    }

    /**
     * Attempt to acquire a lock of 'lockType' on rid
     *
     * @param rid      The resource id to lock
     * @param locker   The Locker to lock this on behalf of.
     * @param lockType Then lock type requested
     * @param timeout  milliseconds to time out after if lock couldn't be obtained. 0 means block indefinitely.
     * @throws LockTimeoutException when the transaction time limit was exceeded.
     */

    public void lock(long rid, Locker locker, LockType lockType, long timeout) {
        int lockTableIdx = getLockTableIndex(rid);
        LockGrantType lockGrantType;

        synchronized (lockTableMutexes[lockTableIdx]) {
            Map<Long, Lock> lockTable = lockTables[lockTableIdx];
            Lock lock = lockTable.get(rid);

            if (lock == null) {
                lock = new LightWeightLock();
                lockTable.put(rid, lock);
            } else if (lock instanceof LightWeightLock) {
                LightWeightLock lightWeightLock = (LightWeightLock) lock;
                if (lightWeightLock.getOwner() != locker) {
                    lock = new MultiUserLock(lightWeightLock.getLockHolder());
                    lockTable.put(rid, lock);
                }
            }

            lockGrantType = lock.lock(locker, lockType);
        }

        if (lockGrantType == LockGrantType.EXISTING
                || lockGrantType == LockGrantType.PROMOTION
                || lockGrantType == LockGrantType.NEW) {
            return;
        }

        synchronized (locker) {
            if (isOwner(rid, locker, lockType)) {
                return;
            }

            assert (lockGrantType == LockGrantType.WAIT_NEW || lockGrantType == LockGrantType.WAIT_PROMOTION);

            try {
                locker.wait(timeout);
            } catch (InterruptedException ie) {
                throw new LockTimeoutException();
            }

            if (isOwner(rid, locker, lockType)) {
                return;
            } else {
                synchronized (lockTableMutexes[lockTableIdx]) {
                    Map<Long, Lock> lockTable = lockTables[lockTableIdx];
                    Lock lock = lockTable.get(rid);
                    lock.removeWaiter(locker);
                }

                throw new LockTimeoutException();
            }
        }
    }

    public boolean release(long rid, Locker locker) {
        Set<Locker> newOwners;

        int lockTableIdx = getLockTableIndex(rid);
        synchronized (lockTableMutexes[lockTableIdx]) {
            Map<Long, Lock> lockTable = lockTables[lockTableIdx];
            Lock lock = lockTable.get(rid);
            if (lock == null) {
                return false;
            }

            newOwners = lock.release(locker);

            if (lock.waiterNum() == 0 && lock.ownerNum() == 0) {
                lockTable.remove(rid);
            }
        }

        if (newOwners != null && newOwners.size() > 0) {
            for (Locker notifyLocker : newOwners) {
                synchronized (notifyLocker) {
                    notifyLocker.notifyAll();
                }
            }
        }

        return true;
    }

    boolean isOwner(long rid, Locker locker, LockType lockType) {
        int lockTableIndex = getLockTableIndex(rid);
        synchronized (lockTableMutexes[lockTableIndex]) {
            final Map<Long, Lock> lockTable = lockTables[lockTableIndex];
            final Lock lock = lockTable.get(rid);
            return lock != null && lock.isOwner(locker, lockType);
        }
    }

    private int getLockTableIndex(long rid) {
        return (((int) rid) & 0x7fffffff) % lockTablesSize;
    }
}
