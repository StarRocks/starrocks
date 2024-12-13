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

import com.starrocks.catalog.system.sys.SysFeLocks;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockException;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TFeLocksReq;
import com.starrocks.thrift.TFeLocksRes;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;

import static com.starrocks.common.lock.LockTestUtils.assertLockSuccess;
import static com.starrocks.common.lock.LockTestUtils.assertLockWait;

public class SysLockTest {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.lock_manager_enabled = true;
    }

    @Test
    public void testListLocks() throws TException, LockException {
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();

        Locker locker = new Locker();
        lockManager.lock(1L, locker, LockType.READ, 0);

        TFeLocksReq req = new TFeLocksReq();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setUser("root");
        authInfo.setUser_ip("localhost");
        req.setAuth_info(authInfo);

        TFeLocksRes res = SysFeLocks.listLocks(req, false);
        Assert.assertEquals(String.valueOf(1), res.getItems().get(0).lock_object);
    }

    @Test
    public void testListLocksWaiter() throws TException {
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

        TFeLocksReq req = new TFeLocksReq();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setUser("root");
        authInfo.setUser_ip("localhost");
        req.setAuth_info(authInfo);
        TFeLocksRes res = SysFeLocks.listLocks(req, false);
        Assert.assertFalse(res.getItems().get(0).getWaiter_list().isEmpty());
    }
}
