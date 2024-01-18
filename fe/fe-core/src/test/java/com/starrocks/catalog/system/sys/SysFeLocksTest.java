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

package com.starrocks.catalog.system.sys;

import com.starrocks.catalog.Database;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TFeLocksItem;
import com.starrocks.thrift.TFeLocksReq;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import java.lang.Thread.State;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SysFeLocksTest {

    @Test
    public void testListLocks() throws TException {
        TFeLocksReq req = new TFeLocksReq();
        TAuthInfo auth = new TAuthInfo();
        auth.setUser("root");
        auth.setUser_ip("127.0.0.1");
        req.setAuth_info(auth);

        var res = SysFeLocks.listLocks(req, false);
        assertTrue(StringUtils.isNotEmpty(res.toString()));
    }

    @Test
    public void testResolveLockItem() throws InterruptedException {
        Database db = new Database(1, "test_lock");

        // empty lock
        {
            TFeLocksItem item = SysFeLocks.resolveLockInfo(db);
            assertEquals("TFeLocksItem(lock_type:DATABASE, lock_object:test_lock, granted:false, waiter_list:[])",
                    item.toString());
        }

        // exclusive owner
        {
            db.writeLock();
            TFeLocksItem item = SysFeLocks.resolveLockInfo(db);

            assertEquals("EXCLUSIVE", item.getLock_mode());
            assertTrue(item.isGranted());
            assertTrue(item.getStart_time() > 0);
            assertTrue(item.getHold_time_ms() >= 0);
            assertEquals("[]", item.getWaiter_list());

            // add a waiter
            Thread waiter = new Thread(() -> {
                db.writeLock();
                db.writeUnlock();
            }, "waiter");
            waiter.start();

            while (waiter.getState() != State.WAITING) {
                Thread.sleep(1000);
            }

            item = SysFeLocks.resolveLockInfo(db);
            assertEquals(String.format("[{\"threadId\":%d,\"threadName\":\"%s\"}]", waiter.getId(), waiter.getName()),
                    item.getWaiter_list());

            db.writeUnlock();
        }

        // shared lock
        {
            db.readLock();
            TFeLocksItem item = SysFeLocks.resolveLockInfo(db);

            assertEquals("SHARED", item.getLock_mode());
            assertTrue(item.isGranted());
            assertTrue(item.getStart_time() > 0);
            assertTrue(item.getHold_time_ms() >= 0);
            assertEquals("[]", item.getWaiter_list());

            // add a waiter
            Thread waiter = new Thread(() -> {
                db.writeLock();
                db.writeUnlock();
            }, "waiter");
            waiter.start();

            // 1. start waiter (blocked)
            // 2. waiter acquired the write lock
            // 3. two threads share the lock
            // 4. two threads release the lock

            while (waiter.getState() != State.WAITING) {
                Thread.sleep(1000);
            }

            item = SysFeLocks.resolveLockInfo(db);
            assertEquals(String.format("[{\"threadId\":%d,\"threadName\":\"%s\"}]", waiter.getId(), waiter.getName()),
                    item.getWaiter_list());
            db.readUnlock();
        }
    }

}