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
package com.starrocks.meta;

import com.starrocks.common.Config;
import com.starrocks.meta.lock.LockManager;
import com.starrocks.meta.lock.LockTimeoutException;
import com.starrocks.meta.lock.LockType;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;

import static com.starrocks.meta.LockTestUtils.assertLockFail;
import static com.starrocks.meta.LockTestUtils.assertLockSuccess;

public class TestException {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.dead_lock_detection_delay_time_ms = 0;
    }

    /**
     * Shared lock blocks exclusive lock
     */
    @Test
    public void testTimeout() throws InterruptedException {
        long rid = 1L;

        TestLocker locker1 = new TestLocker();
        Future<LockResult> resultFuture1 = locker1.lock(rid, LockType.READ);
        assertLockSuccess(resultFuture1);

        TestLocker locker2 = new TestLocker();
        Future<LockResult> resultFuture2 = locker2.lock(rid, LockType.WRITE, 1);
        Thread.sleep(3);
        assertLockFail(resultFuture2, LockTimeoutException.class);

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
        assertLockSuccess(resultFuture1);

        TestLocker locker2 = new TestLocker();
        Future<LockResult> resultFuture3 = locker2.lock(rid, LockType.WRITE, 1);
        Thread.sleep(3);
        assertLockFail(resultFuture3, LockTimeoutException.class);

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assert.assertTrue(lockManager.isOwner(rid, locker1.getLocker(), LockType.INTENTION_SHARED));
        Assert.assertFalse(lockManager.isOwner(rid, locker2.getLocker(), LockType.WRITE));
    }

    @Test
    public void testTimeoutWithIX() throws InterruptedException {
        long rid = 1L;

        TestLocker locker1 = new TestLocker();
        Future<LockResult> resultFuture1 = locker1.lock(rid, LockType.INTENTION_EXCLUSIVE);
        assertLockSuccess(resultFuture1);

        TestLocker locker2 = new TestLocker();
        Future<LockResult> resultFuture3 = locker2.lock(rid, LockType.WRITE, 1);
        Thread.sleep(3);
        assertLockFail(resultFuture3, LockTimeoutException.class);

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        Assert.assertTrue(lockManager.isOwner(rid, locker1.getLocker(), LockType.INTENTION_EXCLUSIVE));
        Assert.assertFalse(lockManager.isOwner(rid, locker2.getLocker(), LockType.WRITE));
    }
}
