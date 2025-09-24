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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.QueryableReentrantReadWriteLock;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class TestLockInterface {
    @BeforeEach
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.lock_manager_enabled = true;
        Config.lock_manager_enable_resolve_deadlock = true;
    }

    @AfterEach
    public void tearDown() {
        Config.lock_manager_enabled = false;
        Config.lock_manager_enable_resolve_deadlock = false;
    }

    @Test
    public void testLockAndCheckExist2() {
        long rid = 1L;
        long rid2 = 2L;
        Database database = new Database(rid, "db");
        database.setExist(false);
        Locker locker = new Locker();
        Assertions.assertFalse(locker.lockTableAndCheckDbExist(database, rid2, LockType.READ));
    }

    @Test
    public void testIsWriteLockHeldByCurrentThread() {
        long rid = 1L;
        Database database = new Database(rid, "db");
        Locker locker = new Locker();
        Assertions.assertTrue(locker.isDbWriteLockHeldByCurrentThread(database));
    }

    @Test
    public void testLockTablesWithIntensiveDbLock() {
        long rid = 1L;
        Database database = new Database(rid, "db");

        long rid2 = 2L;

        long rid3 = 3L;

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(database.getId(), Lists.newArrayList(rid2, rid3), LockType.READ);

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assertions.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assertions.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assertions.assertTrue(lockManager.isOwner(rid3, locker, LockType.READ));

        locker.lockTablesWithIntensiveDbLock(database.getId(), Lists.newArrayList(rid2, rid3), LockType.WRITE);
    }

    @Test
    public void testTryLockTablesWithIntensiveDbLock1() throws LockException {
        long rid = 1L;
        Database database = new Database(rid, "db");

        long rid2 = 2L;

        long rid3 = 3L;

        Locker locker = new Locker();
        Assertions.assertTrue(locker.tryLockTablesWithIntensiveDbLock(database.getId(),
                Lists.newArrayList(rid2, rid3), LockType.READ, 10, TimeUnit.MILLISECONDS));

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assertions.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assertions.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assertions.assertTrue(lockManager.isOwner(rid3, locker, LockType.READ));

        locker.unLockTablesWithIntensiveDbLock(database.getId(), Lists.newArrayList(rid2, rid3), LockType.READ);
        Assertions.assertFalse(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assertions.assertFalse(lockManager.isOwner(rid2, locker, LockType.READ));
        Assertions.assertFalse(lockManager.isOwner(rid3, locker, LockType.READ));

        locker.lock(rid2, LockType.READ);
        Assertions.assertTrue(locker.tryLockTablesWithIntensiveDbLock(database.getId(),
                Lists.newArrayList(rid2, rid3), LockType.WRITE, 10, TimeUnit.MILLISECONDS));
        Assertions.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assertions.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_EXCLUSIVE));
        Assertions.assertTrue(lockManager.isOwner(rid2, locker, LockType.WRITE));
        Assertions.assertTrue(lockManager.isOwner(rid3, locker, LockType.WRITE));
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

        Assertions.assertFalse(locker.tryLockDatabase(database.getId(), LockType.WRITE, 10, TimeUnit.MILLISECONDS));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean trySharedLock(long timeout, TimeUnit unit) throws InterruptedException {
                return false;
            }
        };

        Assertions.assertFalse(locker.tryLockDatabase(database.getId(), LockType.READ, 10, TimeUnit.MILLISECONDS));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean tryExclusiveLock(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException();
            }
        };

        Assertions.assertFalse(locker.tryLockDatabase(database.getId(), LockType.WRITE, 10, TimeUnit.MILLISECONDS));

        new MockUp<QueryableReentrantReadWriteLock>() {
            @Mock
            public boolean trySharedLock(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException();
            }
        };

        Assertions.assertFalse(locker.tryLockDatabase(database.getId(), LockType.READ, 10, TimeUnit.MILLISECONDS));

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

        Assertions.assertTrue(locker.tryLockDatabase(database.getId(), LockType.WRITE, 10, TimeUnit.MILLISECONDS));

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

        Assertions.assertTrue(locker.tryLockDatabase(database.getId(), LockType.READ, 10, TimeUnit.MILLISECONDS));

        Config.lock_manager_enabled = true;
    }

    @Test
    public void testTryLockTablesWithIntensiveDbLock2() throws LockException {
        long rid = 1L;
        Database database = new Database(rid, "db");
        long rid2 = 2L;
        Locker locker = new Locker();
        Assertions.assertTrue(locker.tryLockTableWithIntensiveDbLock(database.getId(),
                rid2, LockType.READ, 10, TimeUnit.MILLISECONDS));
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assertions.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assertions.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));

        locker.unLockTablesWithIntensiveDbLock(database.getId(), ImmutableList.of(rid2), LockType.READ);
        Assertions.assertFalse(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assertions.assertFalse(lockManager.isOwner(rid2, locker, LockType.READ));

        locker.lock(rid2, LockType.READ);
        Assertions.assertTrue(locker.tryLockTableWithIntensiveDbLock(database.getId(),
                rid2, LockType.WRITE, 10, TimeUnit.MILLISECONDS));
        Assertions.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assertions.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_EXCLUSIVE));
        Assertions.assertTrue(lockManager.isOwner(rid2, locker, LockType.WRITE));
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
        Assertions.assertTrue(locker.tryLockTableWithIntensiveDbLock(params, LockType.READ, 10, TimeUnit.MILLISECONDS));

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assertions.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assertions.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assertions.assertTrue(lockManager.isOwner(rid3, locker, LockType.READ));

        locker.unLockTableWithIntensiveDbLock(params, LockType.READ);
        Assertions.assertFalse(lockManager.isOwner(rid, locker, LockType.INTENTION_SHARED));
        Assertions.assertFalse(lockManager.isOwner(rid2, locker, LockType.READ));
        Assertions.assertFalse(lockManager.isOwner(rid3, locker, LockType.READ));

        locker.lock(rid2, LockType.READ);
        Assertions.assertTrue(locker.tryLockTableWithIntensiveDbLock(params, LockType.WRITE, 10, TimeUnit.MILLISECONDS));
        Assertions.assertTrue(lockManager.isOwner(rid2, locker, LockType.READ));
        Assertions.assertTrue(lockManager.isOwner(rid, locker, LockType.INTENTION_EXCLUSIVE));
        Assertions.assertTrue(lockManager.isOwner(rid2, locker, LockType.WRITE));
        Assertions.assertTrue(lockManager.isOwner(rid3, locker, LockType.WRITE));
    }
}
