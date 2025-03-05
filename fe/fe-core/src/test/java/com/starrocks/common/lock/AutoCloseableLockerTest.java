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
import com.starrocks.catalog.Database;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Before;
import org.junit.Test;
import org.wildfly.common.Assert;

public class AutoCloseableLockerTest {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
    }

    /**
     * Intend Shared Lock conflict with other lock type
     */
    @Test
    public void testAutoClose() {
        Database db = new Database(0, "db");
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();

        try (AutoCloseableLock ignore = new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(1L), LockType.WRITE)) {
            Assert.assertTrue(lockManager.isOwner(1, new Locker(), LockType.WRITE));
        }
        Assert.assertFalse(lockManager.isOwner(1, new Locker(), LockType.WRITE));

        try (AutoCloseableLock ignore = new AutoCloseableLock(db.getId(), Lists.newArrayList(1L), LockType.WRITE)) {
            Assert.assertTrue(lockManager.isOwner(1, new Locker(), LockType.WRITE));
        }
        Assert.assertFalse(lockManager.isOwner(1, new Locker(), LockType.WRITE));
    }

    @Test
    public void testExec() {
        Database db = new Database(0, "db");
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();

        try (AutoCloseableLock ignore = new AutoCloseableLock(new Locker(), db.getId(), null, LockType.WRITE)) {
            //test
        } catch (NullPointerException e) {
            Assert.assertFalse(lockManager.isOwner(1, new Locker(), LockType.WRITE));
        }
        Assert.assertFalse(lockManager.isOwner(1, new Locker(), LockType.WRITE));
    }
}
