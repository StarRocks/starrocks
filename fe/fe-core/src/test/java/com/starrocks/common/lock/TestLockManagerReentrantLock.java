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

import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockHolder;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;

public class TestLockManagerReentrantLock {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.lock_manager_dead_lock_detection_delay_time_ms = 0;
    }

    @Test
    public void testClone() {
        Locker locker = new Locker();
        LockHolder lockHolder = new LockHolder(locker, LockType.WRITE);
        LockHolder clone = lockHolder.clone();
        System.out.println(clone);
    }

    @Test
    public void testSLockReentrant() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker1.lock(rid, LockType.READ));
        LockTestUtils.assertLockSuccess(testLocker1.lock(rid, LockType.READ));
        LockTestUtils.assertLockSuccess(testLocker1.release(rid, LockType.READ));
        LockTestUtils.assertLockSuccess(testLocker1.release(rid, LockType.READ));
        LockTestUtils.assertLockFail(testLocker1.release(rid, LockType.READ), IllegalMonitorStateException.class);
    }

    @Test
    public void testXLockReentrant() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        LockTestUtils.assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        LockTestUtils.assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        LockTestUtils.assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        LockTestUtils.assertLockFail(testLocker1.release(rid, LockType.WRITE), IllegalMonitorStateException.class);
    }

    @Test
    public void testXAfterS() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        LockTestUtils.assertLockSuccess(testLocker1.lock(rid, LockType.READ));
        LockTestUtils.assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        LockTestUtils.assertLockSuccess(testLocker1.release(rid, LockType.READ));

        long ri2 = 2L;
        TestLocker testLocker2 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker2.lock(ri2, LockType.WRITE));
        LockTestUtils.assertLockSuccess(testLocker2.lock(ri2, LockType.READ));
        LockTestUtils.assertLockSuccess(testLocker2.lock(ri2, LockType.READ));
        LockTestUtils.assertLockSuccess(testLocker2.release(ri2, LockType.WRITE));
        LockTestUtils.assertLockSuccess(testLocker2.release(ri2, LockType.READ));
        LockTestUtils.assertLockSuccess(testLocker2.lock(ri2, LockType.READ));

        long ri3 = 3L;
        TestLocker testLocker3 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker3.lock(ri3, LockType.WRITE));
        LockTestUtils.assertLockSuccess(testLocker3.lock(ri3, LockType.READ));
        LockTestUtils.assertLockSuccess(testLocker3.release(ri3, LockType.READ));
        LockTestUtils.assertLockSuccess(testLocker3.release(ri3, LockType.WRITE));
    }

    /**
     * Lock reentrant is not restricted by fair lock queuing
     */
    @Test
    public void testReentrantWait() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker1.lock(rid, LockType.READ));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> lockerFuture2 = testLocker2.lock(rid, LockType.WRITE);
        LockTestUtils.assertLockWait(lockerFuture2);

        TestLocker testLocker3 = new TestLocker();
        Future<LockResult> lockerFuture3 = testLocker3.lock(rid, LockType.READ);
        LockTestUtils.assertLockWait(lockerFuture3);

        LockTestUtils.assertLockSuccess(testLocker1.lock(rid, LockType.READ));
    }
}
