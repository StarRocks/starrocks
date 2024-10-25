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
import com.starrocks.common.util.concurrent.lock.NotSupportLockException;
import com.starrocks.server.GlobalStateMgr;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;

import static com.starrocks.common.lock.LockTestUtils.assertLockFail;
import static com.starrocks.common.lock.LockTestUtils.assertLockSuccess;
import static com.starrocks.common.lock.LockTestUtils.assertLockWait;

public class TestLockUpAndDownUpgrade {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.lock_manager_enable_resolve_deadlock = true;
    }

    @After
    public void tearDown() {
        Config.lock_manager_enable_resolve_deadlock = false;
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   request S lock on 1      |                          |
     * |   request X lock on 1      |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testReadUpgrade() {
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(1L, LockType.READ));
        assertLockSuccess(testLocker1.lock(1L, LockType.WRITE));

        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        lockManager.isOwner(1L, testLocker1.getLocker(), LockType.READ);
        lockManager.isOwner(1L, testLocker1.getLocker(), LockType.WRITE);
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   request S lock on 1      |                          |
     * |                            |   request S lock on 1    |
     * |   request X lock on 1      |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testRequestXLockWithOtherSOwner() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));

        TestLocker testLocker2 = new TestLocker();
        assertLockSuccess(testLocker2.lock(rid, LockType.READ));

        assertLockWait(testLocker1.lock(rid, LockType.WRITE));
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   request S lock on 1      |                          |
     * |                            |   request S lock on 1    |
     * |   request IS lock on 1     |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testRequestISLockWithOtherSOwner() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));

        TestLocker testLocker2 = new TestLocker();
        assertLockSuccess(testLocker2.lock(rid, LockType.READ));

        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   request S lock on 1      |                          |
     * |                            |   request IS lock on 1   |
     * |   request IS lock on 1     |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testRequestISLockWithOtherISOwner() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));

        TestLocker testLocker2 = new TestLocker();
        assertLockSuccess(testLocker2.lock(rid, LockType.INTENTION_SHARED));

        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   request S lock on 1      |                          |
     * |                            |   request IS lock on 1   |
     * |   request S lock on 1      |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testRequestSLockWithOtherISOwner() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.READ));

        TestLocker testLocker2 = new TestLocker();
        assertLockSuccess(testLocker2.lock(rid, LockType.INTENTION_SHARED));

        assertLockSuccess(testLocker1.lock(rid, LockType.READ));
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   request X lock on 1      |                          |
     * |                            |   request S lock on 1    |
     * |   request S lock on 1      |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testXDowngradeToS() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> lockerFuture2 = testLocker2.lock(rid, LockType.READ);
        assertLockWait(lockerFuture2);

        assertLockSuccess(testLocker1.lock(rid, LockType.READ));
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   request X lock on 1      |                          |
     * |                            |   request S lock on 1    |
     * |   request IS lock on 1     |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testXDowngradeToIS() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> lockerFuture2 = testLocker2.lock(rid, LockType.READ);
        assertLockWait(lockerFuture2);

        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   request X lock on 1      |                          |
     * |                            |   request S lock on 1    |
     * |   request IS lock on 1     |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testXDowngradeToIX() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.WRITE));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> lockerFuture2 = testLocker2.lock(rid, LockType.READ);
        assertLockWait(lockerFuture2);

        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_EXCLUSIVE));
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   acquire IS lock on 1     |                          |
     * |                            |   acquire IS lock on 1   |
     * |   acquire S lock on 1      |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testISUpgradeToS() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));

        TestLocker testLocker2 = new TestLocker();
        assertLockSuccess(testLocker2.lock(rid, LockType.INTENTION_SHARED));

        assertLockFail(testLocker1.lock(rid, LockType.READ), NotSupportLockException.class);
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   acquire IS lock on 1     |                          |
     * |                            |   acquire IS lock on 1   |
     * |   wait X lock on 1         |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testISUpgradeToX() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));

        TestLocker testLocker2 = new TestLocker();
        assertLockSuccess(testLocker2.lock(rid, LockType.INTENTION_SHARED));

        assertLockFail(testLocker1.lock(rid, LockType.WRITE), NotSupportLockException.class);
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   acquire IS lock on 1     |                          |
     * |                            |   acquire IS lock on 1   |
     * |   acquire IX lock on 1     |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testISUpgradeToIX() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> lockerFuture2 = testLocker2.lock(rid, LockType.INTENTION_SHARED);
        assertLockSuccess(lockerFuture2);

        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_EXCLUSIVE));
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   acquire IS lock on 1     |                          |
     * |                            |   acquire S lock on 1    |
     * |   acquire S lock on 1      |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testISUpgradeToSWithOtherSOwner() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));

        TestLocker testLocker2 = new TestLocker();
        assertLockSuccess(testLocker2.lock(rid, LockType.READ));

        assertLockFail(testLocker1.lock(rid, LockType.READ), NotSupportLockException.class);
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   acquire IS lock on 1     |                          |
     * |                            |   acquire S lock on 1    |
     * |   wait X lock on 1         |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testISUpgradeToXWithOtherSOwner() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));

        TestLocker testLocker2 = new TestLocker();
        assertLockSuccess(testLocker2.lock(rid, LockType.READ));

        assertLockFail(testLocker1.lock(rid, LockType.WRITE), NotSupportLockException.class);
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   acquire IS lock on 1     |                          |
     * |                            |   acquire S lock on 1    |
     * |   wait IX lock on 1        |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testISUpgradeToIXWithOtherSOwner() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_SHARED));

        TestLocker testLocker2 = new TestLocker();
        assertLockSuccess(testLocker2.lock(rid, LockType.READ));

        assertLockWait(testLocker1.lock(rid, LockType.INTENTION_EXCLUSIVE));
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   request IX lock on 1     |                          |
     * |                            |   request IS lock on 1   |
     * |   request S lock on 1      |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testIXUpgradeToSWithOtherISOwner() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_EXCLUSIVE));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> lockerFuture2 = testLocker2.lock(rid, LockType.INTENTION_SHARED);
        assertLockSuccess(lockerFuture2);

        assertLockFail(testLocker1.lock(rid, LockType.READ), NotSupportLockException.class);
    }


    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   request IX lock on 1     |                          |
     * |                            |   request IS lock on 1   |
     * |   request X lock on 1      |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testIXUpgradeToXWithOtherISOwner() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_EXCLUSIVE));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> lockerFuture2 = testLocker2.lock(rid, LockType.INTENTION_SHARED);
        assertLockSuccess(lockerFuture2);

        assertLockFail(testLocker1.lock(rid, LockType.WRITE), NotSupportLockException.class);
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   acquire IX lock on 1     |                          |
     * |                            |   acquire IX lock on 1   |
     * |   wait S lock on 1         |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testIXUpgradeToSWithOtherIXOwner() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_EXCLUSIVE));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> lockerFuture2 = testLocker2.lock(rid, LockType.INTENTION_EXCLUSIVE);
        assertLockSuccess(lockerFuture2);

        assertLockFail(testLocker1.lock(rid, LockType.READ), NotSupportLockException.class);
    }

    /**
     * |----------------------------|--------------------------|
     * |           locker1          |         locker2          |
     * |-------------------------------------------------------|
     * |   acquire IX lock on 1     |                          |
     * |                            |   acquire IX lock on 1   |
     * |   wait S lock on 1         |                          |
     * |-------------------------------------------------------|
     */
    @Test
    public void testIXUpgradeToXWithOtherIXOwner() {
        long rid = 1L;
        TestLocker testLocker1 = new TestLocker();
        assertLockSuccess(testLocker1.lock(rid, LockType.INTENTION_EXCLUSIVE));

        TestLocker testLocker2 = new TestLocker();
        Future<LockResult> lockerFuture2 = testLocker2.lock(rid, LockType.INTENTION_EXCLUSIVE);
        assertLockSuccess(lockerFuture2);

        assertLockFail(testLocker1.lock(rid, LockType.WRITE), NotSupportLockException.class);
    }
}
