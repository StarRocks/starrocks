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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.util.concurrent.lock.IllegalLockStateException;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLockInterface {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.dead_lock_detection_delay_time_ms = 0;
        Config.use_lock_manager = true;
        Config.enable_unlock_deadlock = true;
    }

    @After
    public void tearDown() {
        Config.use_lock_manager = false;
        Config.enable_unlock_deadlock = false;
    }

    @Test
    public void testLockDatabase() {
        long rid = 1L;
        Database database = new Database(rid, "db");
        Locker locker = new Locker();
        locker.lockDatabase(database, LockType.READ);

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assert.assertTrue(lockManager.isOwner(rid, locker, LockType.READ));
        Assert.assertThrows(ErrorReportException.class, () -> locker.lockDatabase(database, LockType.WRITE));

        locker.unLockDatabase(database, LockType.READ);
        Assert.assertFalse(lockManager.isOwner(rid, locker, LockType.READ));
    }

    @Test
    public void testTryLockDatabase() {
        long rid = 1L;
        Database database = new Database(rid, "db");
        Locker locker = new Locker();
        Assert.assertTrue(locker.tryLockDatabase(database, LockType.READ, 10));
        Config.enable_unlock_deadlock = false;
        Assert.assertFalse(locker.tryLockDatabase(database, LockType.WRITE, 10));
        Config.enable_unlock_deadlock = true;
        Assert.assertThrows(ErrorReportException.class, () -> locker.lockDatabase(database, LockType.WRITE));
    }

    @Test
    public void testLockAndCheckExist() {
        long rid = 1L;
        Database database = new Database(rid, "db");
        database.setExist(false);
        Locker locker = new Locker();
        Assert.assertFalse(locker.lockAndCheckExist(database, LockType.READ));
    }

    @Test
    public void testIsWriteLockHeldByCurrentThread() {
        long rid = 1L;
        Database database = new Database(rid, "db");
        Locker locker = new Locker();
        Assert.assertTrue(locker.isWriteLockHeldByCurrentThread(database));
    }

    @Test
    public void testLockTablesWithIntensiveDbLock() {
        long rid = 1L;
        Database database = new Database(rid, "db");

        long rid2 = 2L;

        long rid3 = 3L;

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(database, Lists.newArrayList(rid2, rid3), LockType.READ);

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assert.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertTrue(lockManager.isOwner(rid3, locker, LockType.READ));

        Assert.assertThrows(ErrorReportException.class, () ->
                locker.lockTablesWithIntensiveDbLock(database, Lists.newArrayList(rid2, rid3), LockType.WRITE));
    }

    @Test
    public void testTryLockTablesWithIntensiveDbLock() throws IllegalLockStateException {
        long rid = 1L;
        Database database = new Database(rid, "db");

        long rid2 = 2L;

        long rid3 = 3L;

        Locker locker = new Locker();
        Assert.assertTrue(locker.tryLockTablesWithIntensiveDbLock(database,
                Lists.newArrayList(rid2, rid3), LockType.READ, 10));

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assert.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertTrue(lockManager.isOwner(rid3, locker, LockType.READ));


        locker.unLockTables(database, Lists.newArrayList(rid2, rid3), LockType.READ);
        Assert.assertFalse(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assert.assertFalse(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertFalse(lockManager.isOwner(rid3, locker, LockType.READ));

        locker.lock(rid2, LockType.READ);
        Assert.assertFalse(locker.tryLockTablesWithIntensiveDbLock(database,
                Lists.newArrayList(rid2, rid3), LockType.WRITE, 10));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertFalse(lockManager.isOwner(rid, locker, LockType.INTENTION_EXCLUSIVE));
        Assert.assertFalse(lockManager.isOwner(rid2, locker, LockType.WRITE));
        Assert.assertFalse(lockManager.isOwner(rid3, locker, LockType.WRITE));
    }
}
