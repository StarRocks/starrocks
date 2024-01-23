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
import com.starrocks.thrift.TFeLocksRes;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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

        TFeLocksRes res = SysFeLocks.listLocks(req, false);
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
            AtomicInteger state = new AtomicInteger(0);
            Thread waiter = new Thread(() -> {
                state.set(1);
                db.writeLock();
                db.writeUnlock();
                ;
            }, "waiter");
            waiter.start();

            while (state.get() != 1) {
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
            AtomicInteger state = new AtomicInteger(0);
            Function<Integer, Void> awaitState = (expected) -> {
                while (state.get() != expected) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                return null;
            };
            Thread waiter = new Thread(() -> {
                state.set(1);
                db.writeLock();
                db.writeUnlock();
            }, "waiter");
            waiter.start();

            // 1. start waiter (blocked)
            // 2. waiter acquired the write lock
            // 3. two threads share the lock
            // 4. two threads release the lock

            awaitState.apply(1);
            item = SysFeLocks.resolveLockInfo(db);
            assertEquals(String.format("[{\"threadId\":%d,\"threadName\":\"%s\"}]", waiter.getId(), waiter.getName()),
                    item.getWaiter_list());
            db.readUnlock();
        }
    }

}