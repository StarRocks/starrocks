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

import com.starrocks.common.util.concurrent.lock.LockInfo;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;

import static com.starrocks.common.lock.LockTestUtils.assertLockSuccess;
import static com.starrocks.common.lock.LockTestUtils.assertLockWait;

public class ReleaseTest {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
    }

    @Test
    public void testReleaseInvoke() throws Exception {
        long rid = 1L;

        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> f2 = testLocker2.lock(rid, LockType.READ);
        assertLockWait(f2);

        TestLocker testLocker3 = new TestLocker();
        Future<LockResult> f3 = testLocker3.lock(rid, LockType.READ);
        assertLockWait(f3);

        TestLocker testLocker4 = new TestLocker();
        Future<LockResult> f4 = testLocker4.lock(rid, LockType.READ);
        assertLockWait(f4);

        assertLockSuccess(testLocker1.release(rid, LockType.WRITE));
        LockTestUtils.assertLockSuccess(f2);
        LockTestUtils.assertLockSuccess(f3);
        LockTestUtils.assertLockSuccess(f4);

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        LockInfo lockInfo = lockManager.dumpLockManager().get(0);
        Assert.assertEquals(1, lockInfo.getRid().longValue());
        Assert.assertEquals(3, lockInfo.getOwners().size());
        Assert.assertEquals(0, lockInfo.getWaiters().size());
    }
}
