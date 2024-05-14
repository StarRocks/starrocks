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
import com.starrocks.common.util.concurrent.QueryableReentrantReadWriteLock;
import com.starrocks.common.util.concurrent.lock.IllegalLockStateException;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestLockInterface {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.slow_lock_threshold_ms = 0;
        Config.lock_manager_enabled = true;
        Config.lock_manager_enable_resolve_deadlock = true;
    }

    @After
    public void tearDown() {
        Config.lock_manager_enabled = false;
        Config.lock_manager_enable_resolve_deadlock = false;
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
        Assert.assertTrue(locker.tryLockDatabase(database, LockType.READ, 10, TimeUnit.MILLISECONDS));
        Config.lock_manager_enable_resolve_deadlock = false;
        Assert.assertFalse(locker.tryLockDatabase(database, LockType.WRITE, 10, TimeUnit.MILLISECONDS));
        Config.lock_manager_enable_resolve_deadlock = true;
        Assert.assertThrows(ErrorReportException.class, () -> locker.lockDatabase(database, LockType.WRITE));
    }

    @Test
    public void testLockAndCheckExist() {
        long rid = 1L;
        Database database = new Database(rid, "db");
        database.setExist(false);
        Locker locker = new Locker();
        Assert.assertFalse(locker.lockDatabaseAndCheckExist(database, LockType.READ));
    }

    @Test
    public void testIsWriteLockHeldByCurrentThread() {
        long rid = 1L;
        Database database = new Database(rid, "db");
        Locker locker = new Locker();
        Assert.assertTrue(locker.isDbWriteLockHeldByCurrentThread(database));
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
                Lists.newArrayList(rid2, rid3), LockType.READ, 10, TimeUnit.MILLISECONDS));

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assert.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertTrue(lockManager.isOwner(rid3, locker, LockType.READ));

        locker.unLockTablesWithIntensiveDbLock(database, Lists.newArrayList(rid2, rid3), LockType.READ);
        Assert.assertFalse(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assert.assertFalse(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertFalse(lockManager.isOwner(rid3, locker, LockType.READ));

        locker.lock(rid2, LockType.READ);
        Assert.assertFalse(locker.tryLockTablesWithIntensiveDbLock(database,
                Lists.newArrayList(rid2, rid3), LockType.WRITE, 10, TimeUnit.MILLISECONDS));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertFalse(lockManager.isOwner(rid, locker, LockType.INTENTION_EXCLUSIVE));
        Assert.assertFalse(lockManager.isOwner(rid2, locker, LockType.WRITE));
        Assert.assertFalse(lockManager.isOwner(rid3, locker, LockType.WRITE));
    }

    @Test
    public void testReentrantReadWriteLock() {
        long rid = 1L;
        Database database = new Database(rid, "db");
        Locker locker = new Locker();
        Config.lock_manager_enabled = false;

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean tryExclusiveLock(long timeout, TimeUnit unit) throws InterruptedException {
                return false;
            }
        };

        Assert.assertFalse(locker.tryLockDatabase(database, LockType.WRITE, 10, TimeUnit.MILLISECONDS));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean trySharedLock(long timeout, TimeUnit unit) throws InterruptedException {
                return false;
            }
        };

        Assert.assertFalse(locker.tryLockDatabase(database, LockType.READ, 10, TimeUnit.MILLISECONDS));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean tryExclusiveLock(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException();
            }
        };

        Assert.assertFalse(locker.tryLockDatabase(database, LockType.WRITE, 10, TimeUnit.MILLISECONDS));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean trySharedLock(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException();
            }
        };

        Assert.assertFalse(locker.tryLockDatabase(database, LockType.READ, 10, TimeUnit.MILLISECONDS));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean tryExclusiveLock(long timeout, TimeUnit unit) throws InterruptedException {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // ignore
                }
                return true;
            }
        };

        Assert.assertTrue(locker.tryLockDatabase(database, LockType.WRITE, 10, TimeUnit.MILLISECONDS));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean trySharedLock(long timeout, TimeUnit unit) throws InterruptedException {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // ignore
                }
                return true;
            }
        };

        Assert.assertTrue(locker.tryLockDatabase(database, LockType.READ, 10, TimeUnit.MILLISECONDS));

        Config.lock_manager_enabled = true;
    }

    @Test
    public void testReentrantReadWriteTryLock() {
        List<Database> dbs = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            dbs.add(new Database(i, "db" + i));
        }
        Locker locker = new Locker();
        Config.lock_manager_enabled = false;

        {
            Assert.assertTrue(locker.tryLockDatabases(dbs, LockType.WRITE, 10, TimeUnit.MILLISECONDS));
            Assert.assertTrue(locker.tryLockDatabases(dbs, LockType.WRITE, 10, TimeUnit.MILLISECONDS));
            locker.unlockDatabases(dbs, LockType.WRITE);
            locker.unlockDatabases(dbs, LockType.WRITE);
        }

        {
            new MockUp<Locker>() {
                @Mock
                public boolean tryLockDatabase(Database database, LockType lockType, long timeout, TimeUnit unit) {
                    if (database.getFullName().equalsIgnoreCase("db5")) {
                        return false;
                    }
                    QueryableReentrantReadWriteLock rwLock = database.getRwLock();
                    rwLock.exclusiveLock();
                    return true;
                }
            };
            Assert.assertFalse(locker.tryLockDatabases(dbs, LockType.WRITE, 10, TimeUnit.MILLISECONDS));
        }

        Config.lock_manager_enabled = true;
    }
}
