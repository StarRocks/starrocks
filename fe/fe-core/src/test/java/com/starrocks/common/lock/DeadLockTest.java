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
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.server.GlobalStateMgr;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;

import static com.starrocks.common.lock.LockTestUtils.assertDeadLock;
import static com.starrocks.common.lock.LockTestUtils.assertLockSuccess;
import static com.starrocks.common.lock.LockTestUtils.assertLockWait;

public class DeadLockTest {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.lock_manager_dead_lock_detection_delay_time_ms = 0;
        Config.lock_manager_enable_unlock_deadlock = true;
    }

    @After
    public void tearDown() {
        Config.lock_manager_enable_unlock_deadlock = false;
    }

    @Test
    public void test1() throws InterruptedException {
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(1L, LockType.READ));
        assertDeadLock(
                Lists.newArrayList(testLocker1),
                Lists.newArrayList(new Pair<>(1L, LockType.READ)),
                Lists.newArrayList(testLocker1.lock(1L, LockType.WRITE)));

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        System.out.println(lockManager.dumpLock());
    }

    @Test
    public void test2() throws InterruptedException {
        long rid1 = 1L;
        TestLocker testLocker1 = new TestLocker();
        Future<LockResult> testLockerFuture1 = testLocker1.lock(rid1, LockType.WRITE);
        assertLockSuccess(testLockerFuture1);

        long rid2 = 2L;
        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> testLockerFuture2 = testLocker2.lock(rid2, LockType.WRITE);
        assertLockSuccess(testLockerFuture2);

        Future<LockResult> testLockerFuture3 = testLocker1.lock(rid2, LockType.READ);
        assertLockWait(testLockerFuture3);

        Future<LockResult> testLockerFuture4 = testLocker2.lock(rid1, LockType.READ);

        assertDeadLock(Lists.newArrayList(testLocker1, testLocker2),
                Lists.newArrayList(new Pair<>(rid1, LockType.WRITE), new Pair<>(rid2, LockType.WRITE)),
                Lists.newArrayList(testLockerFuture3, testLockerFuture4));
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        System.out.println(lockManager.dumpLock());
    }
}
