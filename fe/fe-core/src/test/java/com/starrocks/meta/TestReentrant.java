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
import com.starrocks.meta.lock.LockType;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;

import static com.starrocks.meta.LockTestUtils.assertLockFail;
import static com.starrocks.meta.LockTestUtils.assertLockSuccess;
import static com.starrocks.meta.LockTestUtils.assertLockWait;

public class TestReentrant {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.dead_lock_detection_delay_time_ms = 0;
    }

    @Test
    public void testSLockReentrant() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));
        assertLockSuccess(testLocker1.release(rid, LockType.READ));
        assertLockSuccess(testLocker1.release(rid, LockType.READ));
        assertLockFail(testLocker1.release(rid, LockType.READ), "Attempt to unlock lock, not locked by current locker");
    }

    @Test
    public void testXLockReentrant() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        assertLockFail(testLocker1.release(rid, LockType.WRITE), "Attempt to unlock lock, not locked by current locker");
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
}
