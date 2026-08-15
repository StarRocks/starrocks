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
package com.starrocks.common.lock;

import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.common.util.concurrent.lock.YieldableLock;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class YieldableLockTest {
    private static final long DB_ID = 1L;
    private static final long TABLE_ID = 2L;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
    }

    @Test
    public void testDatabaseScopeLifecycle() {
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();

        YieldableLock lock = YieldableLock.lockDatabase(DB_ID, LockType.READ);
        Assertions.assertTrue(lockManager.isOwner(DB_ID, new Locker(), LockType.READ));

        lock.refresh();
        Assertions.assertTrue(lockManager.isOwner(DB_ID, new Locker(), LockType.READ));

        lock.close();
        Assertions.assertFalse(lockManager.isOwner(DB_ID, new Locker(), LockType.READ));

        // close() is idempotent, but yielding a closed scope is a programming error
        lock.close();
        Assertions.assertThrows(IllegalStateException.class, lock::refresh);
        Assertions.assertThrows(IllegalStateException.class, () -> lock.sleepUnlocked(1));
    }

    @Test
    public void testTableScopeLifecycle() throws InterruptedException {
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();

        YieldableLock lock = YieldableLock.lockTableWithIntensiveDbLock(DB_ID, TABLE_ID, LockType.READ);
        Assertions.assertTrue(lockManager.isOwner(DB_ID, new Locker(), LockType.INTENTION_SHARED));
        Assertions.assertTrue(lockManager.isOwner(TABLE_ID, new Locker(), LockType.READ));

        lock.sleepUnlocked(1);
        Assertions.assertTrue(lockManager.isOwner(DB_ID, new Locker(), LockType.INTENTION_SHARED));
        Assertions.assertTrue(lockManager.isOwner(TABLE_ID, new Locker(), LockType.READ));

        lock.close();
        Assertions.assertFalse(lockManager.isOwner(DB_ID, new Locker(), LockType.INTENTION_SHARED));
        Assertions.assertFalse(lockManager.isOwner(TABLE_ID, new Locker(), LockType.READ));
    }

    @Test
    public void testHeldTimeExcludesYieldedWindows() throws InterruptedException {
        YieldableLock lock = YieldableLock.lockDatabase(DB_ID, LockType.READ);
        lock.sleepUnlocked(150);
        lock.close();
        // Only the brief held segments before and after the yield count, never the
        // sleep itself.
        Assertions.assertTrue(lock.getHeldTimeNs() > 0);
        Assertions.assertTrue(lock.getHeldTimeMs() < 150);
        Assertions.assertEquals(0, lock.getCurrentHoldTimeMs());
        // After close() the total is stable.
        Assertions.assertEquals(lock.getHeldTimeNs(), lock.getHeldTimeNs());
    }

    @Test
    public void testSleepUnlockedReacquiresOnInterrupt() {
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();

        try (YieldableLock lock = YieldableLock.lockTableWithIntensiveDbLock(DB_ID, TABLE_ID, LockType.READ)) {
            Thread.currentThread().interrupt();
            Assertions.assertThrows(InterruptedException.class, () -> lock.sleepUnlocked(10000));
            // The locks must have been re-acquired before the interrupt propagated.
            Assertions.assertTrue(lockManager.isOwner(DB_ID, new Locker(), LockType.INTENTION_SHARED));
            Assertions.assertTrue(lockManager.isOwner(TABLE_ID, new Locker(), LockType.READ));
        }
        Assertions.assertFalse(lockManager.isOwner(DB_ID, new Locker(), LockType.INTENTION_SHARED));
        Assertions.assertFalse(lockManager.isOwner(TABLE_ID, new Locker(), LockType.READ));
    }
}
