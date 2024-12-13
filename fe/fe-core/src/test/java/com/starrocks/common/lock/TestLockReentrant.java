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

import com.starrocks.common.util.concurrent.lock.LockHolder;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;

import static com.starrocks.common.lock.LockTestUtils.assertLockSuccess;
import static com.starrocks.common.lock.LockTestUtils.assertLockWait;

public class TestLockReentrant {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
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
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));
        assertLockSuccess(testLocker1.release(rid, LockType.READ));
        assertLockSuccess(testLocker1.release(rid, LockType.READ));
        LockTestUtils.assertLockFail(testLocker1.release(rid, LockType.READ), IllegalMonitorStateException.class);
    }

    @Test
    public void testXLockReentrant() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        LockTestUtils.assertLockFail(testLocker1.release(rid, LockType.WRITE), IllegalMonitorStateException.class);
    }

    @Test
    public void testXLockReentrant2() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        //acquire LightWeightLock
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        //acquire MultiUserLock
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        //Verify that the same write lock is acquired normally on MultiUserLock
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));

        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));

        LockTestUtils.assertLockFail(testLocker1.release(rid, LockType.WRITE), IllegalMonitorStateException.class);
    }

    @Test
    public void testXLockReentrant3() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        //acquire LightWeightLock
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        //acquire MultiUserLock
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        //Verify that the same write lock is acquired normally on MultiUserLock
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));

        assertLockSuccess(testLocker1.lock(rid, LockType.READ));
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));

        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));

        assertLockSuccess(testLocker1.release(rid, LockType.READ));
        assertLockSuccess(testLocker1.release(rid, LockType.READ));

        LockTestUtils.assertLockFail(testLocker1.release(rid, LockType.WRITE), IllegalMonitorStateException.class);
        LockTestUtils.assertLockFail(testLocker1.release(rid, LockType.READ), IllegalMonitorStateException.class);
    }

    @Test
    public void testXLockReentrant4() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));
        // The write lock can be increased normally because there is a reference count for the write lock
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));

        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        LockTestUtils.assertLockFail(testLocker1.release(rid, LockType.WRITE), IllegalMonitorStateException.class);

        // Unlike Java Reentrant Lock, lock can be upgraded
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
    }

    @Test
    public void testXAfterS() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));
        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.release(rid, LockType.READ));

        long ri2 = 2L;
        TestLocker testLocker2 = new TestLocker();
        assertLockSuccess(testLocker2.lock(ri2, LockType.WRITE));
        assertLockSuccess(testLocker2.lock(ri2, LockType.READ));
        assertLockSuccess(testLocker2.lock(ri2, LockType.READ));
        assertLockSuccess(testLocker2.release(ri2, LockType.WRITE));
        assertLockSuccess(testLocker2.release(ri2, LockType.READ));
        assertLockSuccess(testLocker2.lock(ri2, LockType.READ));

        long ri3 = 3L;
        TestLocker testLocker3 = new TestLocker();
        assertLockSuccess(testLocker3.lock(ri3, LockType.WRITE));
        assertLockSuccess(testLocker3.lock(ri3, LockType.READ));
        assertLockSuccess(testLocker3.release(ri3, LockType.READ));
        assertLockSuccess(testLocker3.release(ri3, LockType.WRITE));
    }

    /**
     * Lock reentrant is not restricted by fair lock queuing
     */
    @Test
    public void testReentrantWait() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> lockerFuture2 = testLocker2.lock(rid, LockType.WRITE);
        assertLockWait(lockerFuture2);

        TestLocker testLocker3 = new TestLocker();
        Future<LockResult> lockerFuture3 = testLocker3.lock(rid, LockType.READ);
        assertLockWait(lockerFuture3);

        assertLockSuccess(testLocker1.lock(rid, LockType.READ));
    }

    @Test
    public void testISLockReentrant() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));
        assertLockSuccess(testLocker1.release(rid, LockType.INTENTION_SHARED));
        assertLockSuccess(testLocker1.release(rid, LockType.INTENTION_SHARED));
    }

    @Test
    public void testISLockReentrantWithOtherOwner() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));

        TestLocker testLocker2 = new TestLocker();
        assertLockSuccess(testLocker2.lock(rid, LockType.READ));

        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));
    }

    @Test
    public void testIXLockReentrant() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_EXCLUSIVE));
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_EXCLUSIVE));
        assertLockSuccess(testLocker1.release(rid, LockType.INTENTION_EXCLUSIVE));
        assertLockSuccess(testLocker1.release(rid, LockType.INTENTION_EXCLUSIVE));
    }


}
