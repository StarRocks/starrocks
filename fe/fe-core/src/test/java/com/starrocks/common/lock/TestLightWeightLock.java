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
import com.starrocks.common.util.concurrent.lock.LightWeightLock;
import com.starrocks.common.util.concurrent.lock.LockHolder;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.wildfly.common.Assert;

import java.util.ArrayList;
import java.util.Set;

public class TestLightWeightLock {
    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.lock_manager_dead_lock_detection_delay_time_ms = 0;
        Config.lock_manager_enabled = true;
        Config.lock_manager_enable_resolve_deadlock = true;
    }

    @After
    public void tearDown() {
        Config.lock_manager_enabled = false;
        Config.lock_manager_enable_resolve_deadlock = false;
    }

    @Test
    public void testLock() {
        LightWeightLock lock = new LightWeightLock();

        Locker locker = new Locker();
        Assert.assertFalse(lock.isOwner(locker, LockType.READ));

        lock.lock(locker, LockType.READ);
        Assert.assertTrue(lock.isOwner(locker, LockType.READ));

        Set<LockHolder> clone = lock.cloneOwners();
        LockHolder cloneLocker = new ArrayList<>(clone).get(0);

        Set<LockHolder> owner = lock.getOwners();
        LockHolder lockHolder = new ArrayList<>(owner).get(0);

        Assert.assertTrue(System.identityHashCode(cloneLocker) != System.identityHashCode(lockHolder));
    }
}
