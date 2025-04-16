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
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.util.concurrent.lock.LockException;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.common.util.concurrent.lock.NotSupportLockException;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestLockException {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
    }

    /**
     * Shared lock blocks exclusive lock
     */
    @Test
    public void testTimeout() throws InterruptedException {
        long rid = 1L;

        TestLocker locker1 = new TestLocker();
        Future<LockResult> resultFuture1 = locker1.lock(rid, LockType.READ);
        LockTestUtils.assertLockSuccess(resultFuture1);

        TestLocker locker2 = new TestLocker();
        Future<LockResult> resultFuture2 = locker2.lock(rid, LockType.WRITE, 1);
        Thread.sleep(3);
        LockTestUtils.assertLockFail(resultFuture2, LockTimeoutException.class);

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assert.assertTrue(lockManager.isOwner(rid, locker1.getLocker(), LockType.READ));
        Assert.assertFalse(lockManager.isOwner(rid, locker1.getLocker(), LockType.WRITE));
        Assert.assertFalse(lockManager.isOwner(rid, locker2.getLocker(), LockType.READ));
        Assert.assertFalse(lockManager.isOwner(rid, locker2.getLocker(), LockType.WRITE));
    }

    @Test
    public void testTimeoutWithIS() throws InterruptedException {
        long rid = 1L;

        TestLocker locker1 = new TestLocker();
        Future<LockResult> resultFuture1 = locker1.lock(rid, LockType.INTENTION_SHARED);
        LockTestUtils.assertLockSuccess(resultFuture1);

        TestLocker locker2 = new TestLocker();
        Future<LockResult> resultFuture3 = locker2.lock(rid, LockType.WRITE, 1);
        Thread.sleep(3);
        LockTestUtils.assertLockFail(resultFuture3, LockTimeoutException.class);

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assert.assertTrue(lockManager.isOwner(rid, locker1.getLocker(), LockType.INTENTION_SHARED));
        Assert.assertFalse(lockManager.isOwner(rid, locker2.getLocker(), LockType.WRITE));
    }

    @Test
    public void testTimeoutWithIX() throws InterruptedException {
        long rid = 1L;

        TestLocker locker1 = new TestLocker();
        Future<LockResult> resultFuture1 = locker1.lock(rid, LockType.INTENTION_EXCLUSIVE);
        LockTestUtils.assertLockSuccess(resultFuture1);

        TestLocker locker2 = new TestLocker();
        Future<LockResult> resultFuture3 = locker2.lock(rid, LockType.WRITE, 1);
        Thread.sleep(3);
        LockTestUtils.assertLockFail(resultFuture3, LockTimeoutException.class);

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assert.assertTrue(lockManager.isOwner(rid, locker1.getLocker(), LockType.INTENTION_EXCLUSIVE));
        Assert.assertFalse(lockManager.isOwner(rid, locker2.getLocker(), LockType.WRITE));
    }

    @Test
    public void testTimeoutError() {
        long rid = 1L;

        TestLocker locker1 = new TestLocker();
        Future<LockResult> resultFuture1 = locker1.lock(rid, LockType.READ, -1);
        LockTestUtils.assertLockFail(resultFuture1, NotSupportLockException.class);
    }

    @Test
    public void testLockException() {
        new MockUp<LockManager>() {
            @Mock
            public void lock(long rid, Locker locker, LockType lockType, long timeout) throws LockException {
                throw new NotSupportLockException("");
            }
        };

        Database db = new Database(1, "db");
        Locker locker = new Locker();
        Assert.assertThrows(ErrorReportException.class, () -> locker.lockDatabase(db.getId(), LockType.READ));
        Assert.assertThrows(ErrorReportException.class, () -> locker.tryLockDatabase(db.getId(), LockType.READ,
                10000, TimeUnit.MILLISECONDS));

        Assert.assertThrows(ErrorReportException.class, () -> locker.lockTablesWithIntensiveDbLock(
                1L, Lists.newArrayList(2L), LockType.READ));
        Assert.assertThrows(ErrorReportException.class, () -> locker.lockTableWithIntensiveDbLock(
                1L, 2L, LockType.READ));
        Assert.assertFalse(locker.tryLockTablesWithIntensiveDbLock(
                db.getId(), Lists.newArrayList(2L), LockType.READ, 10000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testReleaseException() throws LockException {
        new MockUp<LockManager>() {
            @Mock
            public void release(long rid, Locker locker, LockType lockType) throws LockException {
                throw new NotSupportLockException("");
            }
        };

        Locker locker = new Locker();
        locker.lock(1, LockType.READ);
        Assert.assertThrows(ErrorReportException.class, () -> locker.release(1, LockType.READ));
    }
}
