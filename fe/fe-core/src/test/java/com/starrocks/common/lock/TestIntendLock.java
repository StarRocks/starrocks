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

public class TestIntendLock {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.lock_manager_dead_lock_detection_delay_time_ms = 0;
    }

    /**
     * Intend Shared Lock conflict with other lock type
     */
    @Test
    public void testISConflict() {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));

        TestLocker testLocker2 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker2.lock(rid, LockType.INTENTION_SHARED));

        long rid2 = 2L;
        TestLocker testLocker3 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker3.lock(rid2, LockType.INTENTION_SHARED));

        TestLocker testLocker4 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker4.lock(rid2, LockType.INTENTION_EXCLUSIVE));

        long rid3 = 3L;
        TestLocker testLocker5 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker5.lock(rid3, LockType.INTENTION_SHARED));

        TestLocker testLocker6 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker6.lock(rid3, LockType.READ));

        long rid4 = 4L;
        TestLocker testLocker7 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker7.lock(rid4, LockType.INTENTION_SHARED));

        TestLocker testLocker8 = new TestLocker();
        LockTestUtils.assertLockWait(testLocker8.lock(rid4, LockType.WRITE));
    }

    /**
     * Intend Exclusive Lock conflict with other lock type
     */
    @Test
    public void testIXConflict() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_EXCLUSIVE));

        TestLocker testLocker2 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker2.lock(rid, LockType.INTENTION_SHARED));

        long rid2 = 2L;
        TestLocker testLocker3 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker3.lock(rid2, LockType.INTENTION_EXCLUSIVE));

        TestLocker testLocker4 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker4.lock(rid2, LockType.INTENTION_EXCLUSIVE));

        long rid3 = 3L;
        TestLocker testLocker5 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker5.lock(rid3, LockType.INTENTION_EXCLUSIVE));

        TestLocker testLocker6 = new TestLocker();
        LockTestUtils.assertLockWait(testLocker6.lock(rid3, LockType.READ));

        long rid4 = 4L;
        TestLocker testLocker7 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker7.lock(rid4, LockType.INTENTION_EXCLUSIVE));

        TestLocker testLocker8 = new TestLocker();
        LockTestUtils.assertLockWait(testLocker8.lock(rid4, LockType.WRITE));
    }

    /**
     * Shared Lock conflict with other lock type
     */
    @Test
    public void testSConflict() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker1.lock(rid, LockType.READ));

        TestLocker testLocker2 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker2.lock(rid, LockType.INTENTION_SHARED));

        long rid2 = 2L;
        TestLocker testLocker3 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker3.lock(rid2, LockType.READ));

        TestLocker testLocker4 = new TestLocker();
        LockTestUtils.assertLockWait(testLocker4.lock(rid2, LockType.INTENTION_EXCLUSIVE));

        long rid3 = 3L;
        TestLocker testLocker5 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker5.lock(rid3, LockType.READ));

        TestLocker testLocker6 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker6.lock(rid3, LockType.READ));

        long rid4 = 4L;
        TestLocker testLocker7 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker7.lock(rid4, LockType.READ));

        TestLocker testLocker8 = new TestLocker();
        LockTestUtils.assertLockWait(testLocker8.lock(rid4, LockType.WRITE));
    }

    /**
     * Exclusive Lock conflict with other lock type
     */
    @Test
    public void testXConflict() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));

        TestLocker testLocker2 = new TestLocker();
        LockTestUtils.assertLockWait(testLocker2.lock(rid, LockType.INTENTION_SHARED));

        long rid2 = 2L;
        TestLocker testLocker3 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker3.lock(rid2, LockType.WRITE));

        TestLocker testLocker4 = new TestLocker();
        LockTestUtils.assertLockWait(testLocker4.lock(rid2, LockType.INTENTION_EXCLUSIVE));

        long rid3 = 3L;
        TestLocker testLocker5 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker5.lock(rid3, LockType.WRITE));

        TestLocker testLocker6 = new TestLocker();
        LockTestUtils.assertLockWait(testLocker6.lock(rid3, LockType.READ));

        long rid4 = 4L;
        TestLocker testLocker7 = new TestLocker();
        LockTestUtils.assertLockSuccess(testLocker7.lock(rid4, LockType.WRITE));

        TestLocker testLocker8 = new TestLocker();
        LockTestUtils.assertLockWait(testLocker8.lock(rid4, LockType.WRITE));
    }
}
