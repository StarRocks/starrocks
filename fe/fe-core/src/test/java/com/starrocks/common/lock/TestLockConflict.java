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
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;

import static com.starrocks.common.lock.LockTestUtils.assertLockFail;
import static com.starrocks.common.lock.LockTestUtils.assertLockSuccess;
import static com.starrocks.common.lock.LockTestUtils.assertLockWait;

public class TestLockConflict {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.lock_manager_dead_lock_detection_delay_time_ms = 0;
    }

    /**
     * Shared lock blocks exclusive lock
     */
    @Test
    public void testSBlockX() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));

        TestLocker testLocker2 = new TestLocker();
        assertLockWait(testLocker2.lock(rid, LockType.WRITE));
    }

    /**
     * Shared lock blocks exclusive lock,
     * after the shared lock is released, the exclusive lock can acquire the lock
     */
    @Test
    public void testSRelease() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> lockTaskResultFuture = testLocker2.lock(rid, LockType.WRITE);
        assertLockWait(lockTaskResultFuture);

        assertLockSuccess(testLocker1.release(rid, LockType.READ));
        assertLockSuccess(lockTaskResultFuture);
    }

    /**
     * Shared locks block exclusive locks. When an exclusive lock blocks, subsequent shared locks are also blocked.
     */
    @Test
    public void testSBlockMultiX() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));

        TestLocker testLocker2 = new TestLocker();
        assertLockWait(testLocker2.lock(rid, LockType.WRITE));

        TestLocker testLocker3 = new TestLocker();
        assertLockWait(testLocker3.lock(rid, LockType.WRITE));
    }

    /**
     * The shared lock blocks the exclusive lock, but the shared lock before the exclusive lock can be grant
     */
    @Test
    public void testMultiSBlockX() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> locker2Result = testLocker2.lock(rid, LockType.READ);
        assertLockSuccess(locker2Result);

        TestLocker testLocker3 = new TestLocker();
        Future<LockResult> locker3Result = testLocker3.lock(rid, LockType.WRITE);
        assertLockWait(locker3Result);

        assertLockSuccess(testLocker1.release(rid, LockType.READ));
        assertLockSuccess(locker2Result);
        assertLockWait(locker3Result);
    }

    /**
     * Shared locks block exclusive locks. When there are shared locks and exclusive locks queued at the same time,
     * and the exclusive locks are ranked at the front,
     * the fairness principle is used to release the locks at the front first.
     */
    @Test
    public void testXBlockSuqS() {
        long rid = 1L;

        TestLocker locker1 = new TestLocker();
        assertLockSuccess(locker1.lock(rid, LockType.READ));

        TestLocker locker2 = new TestLocker();
        Future<LockResult> locker2Future = locker2.lock(rid, LockType.WRITE);
        assertLockWait(locker2Future);

        TestLocker locker3 = new TestLocker();
        Future<LockResult> locker3Future = locker3.lock(rid, LockType.READ);
        assertLockWait(locker3Future);

        assertLockSuccess(locker1.release(rid, LockType.READ));
        assertLockSuccess(locker2Future);
        assertLockWait(locker3Future);
    }

    /**
     * Release non-existent locks without unexpected exceptions
     */
    @Test
    public void testReleaseNotLock() {
        TestLocker locker = new TestLocker();
        Future<LockResult> resultFuture = locker.release(1L, LockType.READ);
        assertLockFail(resultFuture, IllegalMonitorStateException.class);
    }

    @Test
    public void testCantReleaseLockBelongOtherLocker() {
        TestLocker locker1 = new TestLocker();
        Future<LockResult> resultFuture1 = locker1.lock(1L, LockType.READ);
        assertLockSuccess(resultFuture1);

        TestLocker locker2 = new TestLocker();
        Future<LockResult> resultFuture2 = locker2.release(1L, LockType.READ);
        assertLockFail(resultFuture2, IllegalMonitorStateException.class);

        long rid2 = 2L;
        TestLocker locker3 = new TestLocker();
        assertLockSuccess(locker3.lock(rid2, LockType.READ));
        assertLockSuccess(locker3.release(rid2, LockType.READ));

        TestLocker locker4 = new TestLocker();
        assertLockSuccess(locker4.lock(rid2, LockType.READ));
        assertLockFail(locker3.release(rid2, LockType.READ), IllegalMonitorStateException.class);
    }

    @Test
    public void testReleaseMultiTimes() {
        TestLocker locker = new TestLocker();
        assertLockSuccess(locker.lock(1L, LockType.READ));
        assertLockSuccess(locker.release(1L, LockType.READ));
        assertLockFail(locker.release(1L, LockType.READ), IllegalMonitorStateException.class);
    }

    @Test
    public void testReleaseErrorType() {
        TestLocker locker = new TestLocker();
        assertLockSuccess(locker.lock(1L, LockType.READ));
        assertLockFail(locker.release(1L, LockType.WRITE), IllegalMonitorStateException.class);
        assertLockSuccess(locker.release(1L, LockType.READ));
    }
}
