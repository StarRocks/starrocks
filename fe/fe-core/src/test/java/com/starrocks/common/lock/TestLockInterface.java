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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.QueryableReentrantReadWriteLock;
import com.starrocks.common.util.concurrent.lock.LockException;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockParams;
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
        Config.lock_manager_enabled = true;
        Config.lock_manager_enable_using_fine_granularity_lock = true;
        Config.lock_manager_enable_resolve_deadlock = true;
    }

    @After
    public void tearDown() {
        Config.lock_manager_enabled = false;
        Config.lock_manager_enable_using_fine_granularity_lock = false;
        Config.lock_manager_enable_resolve_deadlock = false;
    }

    @Test
    public void testLockAndCheckExist1() {
        long rid = 1L;
        Database database = new Database(rid, "db");
        database.setExist(false);
        Locker locker = new Locker();
        Assert.assertFalse(locker.lockAndCheckExist(database, LockType.READ));
    }

    @Test
    public void testLockAndCheckExist2() {
        long rid = 1L;
        long rid2 = 2L;
        Database database = new Database(rid, "db");
        database.setExist(false);
        Locker locker = new Locker();
        Assert.assertFalse(locker.lockDatabaseAndCheckExist(database, rid2, LockType.READ));
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

        locker.lockTablesWithIntensiveDbLock(database, Lists.newArrayList(rid2, rid3), LockType.WRITE);
    }

    @Test
    public void testTryLockTablesWithIntensiveDbLock1() throws LockException {
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

        locker.unLockTablesWithIntensiveDbLock(database, Lists.newArrayList(rid2, rid3), LockType.READ);
        Assert.assertFalse(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assert.assertFalse(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertFalse(lockManager.isOwner(rid3, locker, LockType.READ));

        locker.lock(rid2, LockType.READ);
        Assert.assertTrue(locker.tryLockTablesWithIntensiveDbLock(database,
                Lists.newArrayList(rid2, rid3), LockType.WRITE, 10));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_EXCLUSIVE));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.WRITE));
        Assert.assertTrue(lockManager.isOwner(rid3, locker, LockType.WRITE));
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

        Assert.assertFalse(locker.tryLockDatabase(database, LockType.WRITE, 10));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean trySharedLock(long timeout, TimeUnit unit) throws InterruptedException {
                return false;
            }
        };

        Assert.assertFalse(locker.tryLockDatabase(database, LockType.READ, 10));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean tryExclusiveLock(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException();
            }
        };

        Assert.assertFalse(locker.tryLockDatabase(database, LockType.WRITE, 10));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean trySharedLock(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException();
            }
        };

        Assert.assertFalse(locker.tryLockDatabase(database, LockType.READ, 10));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean tryExclusiveLock(long timeout, TimeUnit unit) throws InterruptedException {
                Thread.sleep(100);
                return true;
            }
        };

        Assert.assertTrue(locker.tryLockDatabase(database, LockType.WRITE, 10));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean trySharedLock(long timeout, TimeUnit unit) throws InterruptedException {
                Thread.sleep(100);
                return true;
            }
        };

        Assert.assertTrue(locker.tryLockDatabase(database, LockType.READ, 10));

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
                public boolean tryLockDatabase(Database database, LockType lockType, long timeout) {
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

    @Test
    public void testTryLockTablesWithIntensiveDbLock2() throws LockException {
        long rid = 1L;
        Database database = new Database(rid, "db");
        long rid2 = 2L;
        Locker locker = new Locker();
        Assert.assertTrue(locker.tryLockTableWithIntensiveDbLock(database, rid2, LockType.READ, 10, TimeUnit.MILLISECONDS));
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assert.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));

        locker.unLockTablesWithIntensiveDbLock(database, ImmutableList.of(rid2), LockType.READ);
        Assert.assertFalse(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assert.assertFalse(lockManager.isOwner(rid2, locker, LockType.READ));

        locker.lock(rid2, LockType.READ);
        Assert.assertTrue(locker.tryLockTableWithIntensiveDbLock(database, rid2, LockType.WRITE, 10, TimeUnit.MILLISECONDS));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_EXCLUSIVE));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.WRITE));
    }

    @Test
    public void testTryLockTablesWithIntensiveDbLock3() throws LockException {
        long rid = 1L;
        long rid2 = 2L;
        long rid3 = 3L;
        Database database = new Database(rid, "db");

        LockParams params = new LockParams();
        params.add(database, rid2);
        params.add(database, rid3);

        Locker locker = new Locker();
        Assert.assertTrue(locker.tryLockTableWithIntensiveDbLock(params, LockType.READ, 10, TimeUnit.MILLISECONDS));

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assert.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertTrue(lockManager.isOwner(rid3, locker, LockType.READ));

        locker.unLockTableWithIntensiveDbLock(params, LockType.READ);
        Assert.assertFalse(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assert.assertFalse(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertFalse(lockManager.isOwner(rid3, locker, LockType.READ));

        locker.lock(rid2, LockType.READ);
        Assert.assertTrue(locker.tryLockTableWithIntensiveDbLock(params, LockType.WRITE, 10, TimeUnit.MILLISECONDS));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assert.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_EXCLUSIVE));
        Assert.assertTrue(lockManager.isOwner(rid2, locker, LockType.WRITE));
        Assert.assertTrue(lockManager.isOwner(rid3, locker, LockType.WRITE));
    }
}
