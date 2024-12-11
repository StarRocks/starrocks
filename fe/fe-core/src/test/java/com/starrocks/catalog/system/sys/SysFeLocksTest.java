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
<<<<<<< HEAD
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TFeLocksItem;
import com.starrocks.thrift.TFeLocksReq;
import com.starrocks.thrift.TFeLocksRes;
=======
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TFeLocksItem;
import com.starrocks.thrift.TFeLocksReq;
import mockit.Expectations;
import mockit.Mocked;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

<<<<<<< HEAD
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
=======
import java.lang.Thread.State;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

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

<<<<<<< HEAD
        TFeLocksRes res = SysFeLocks.listLocks(req, false);
=======
        var res = SysFeLocks.listLocks(req, false);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        assertTrue(StringUtils.isNotEmpty(res.toString()));
    }

    @Test
<<<<<<< HEAD
    public void testResolveLockItem() throws InterruptedException {
=======
    public void testResolveLockItem(@Mocked GlobalStateMgr globalStateMgr, @Mocked MetadataMgr metadataMgr)
            throws InterruptedException {
        Config.lock_manager_enabled = false;

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        Database db = new Database(1, "test_lock");

        // empty lock
        {
            TFeLocksItem item = SysFeLocks.resolveLockInfo(db);
            assertEquals("TFeLocksItem(lock_type:DATABASE, lock_object:test_lock, granted:false, waiter_list:[])",
                    item.toString());
        }

<<<<<<< HEAD
        // exclusive owner
        {
            db.writeLock();
=======
        new Expectations(metadataMgr) {
            {
                globalStateMgr.getMetadataMgr();
                minTimes = 0;
                result = metadataMgr;

                metadataMgr.getDb(anyLong);
                result = db;
            }
        };

        // exclusive owner
        {
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.WRITE);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            TFeLocksItem item = SysFeLocks.resolveLockInfo(db);

            assertEquals("EXCLUSIVE", item.getLock_mode());
            assertTrue(item.isGranted());
            assertTrue(item.getStart_time() > 0);
            assertTrue(item.getHold_time_ms() >= 0);
            assertEquals("[]", item.getWaiter_list());

            // add a waiter
<<<<<<< HEAD
            AtomicInteger state = new AtomicInteger(0);
            Thread waiter = new Thread(() -> {
                state.set(1);
                db.writeLock();
                db.writeUnlock();
                ;
            }, "waiter");
            waiter.start();

            while (state.get() != 1) {
=======
            Thread waiter = new Thread(() -> {
                locker.lockDatabase(db.getId(), LockType.WRITE);
                locker.unLockDatabase(db.getId(), LockType.WRITE);
            }, "waiter");
            waiter.start();

            while (waiter.getState() != State.WAITING) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                Thread.sleep(1000);
            }

            item = SysFeLocks.resolveLockInfo(db);
            assertEquals(String.format("[{\"threadId\":%d,\"threadName\":\"%s\"}]", waiter.getId(), waiter.getName()),
                    item.getWaiter_list());

<<<<<<< HEAD
            db.writeUnlock();
=======
            locker.unLockDatabase(db.getId(), LockType.WRITE);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        // shared lock
        {
<<<<<<< HEAD
            db.readLock();
=======
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            TFeLocksItem item = SysFeLocks.resolveLockInfo(db);

            assertEquals("SHARED", item.getLock_mode());
            assertTrue(item.isGranted());
            assertTrue(item.getStart_time() > 0);
            assertTrue(item.getHold_time_ms() >= 0);
            assertEquals("[]", item.getWaiter_list());

            // add a waiter
<<<<<<< HEAD
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
=======
            Thread waiter = new Thread(() -> {
                locker.lockDatabase(db.getId(), LockType.WRITE);
                locker.unLockDatabase(db.getId(), LockType.WRITE);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }, "waiter");
            waiter.start();

            // 1. start waiter (blocked)
            // 2. waiter acquired the write lock
            // 3. two threads share the lock
            // 4. two threads release the lock

<<<<<<< HEAD
            awaitState.apply(1);
            item = SysFeLocks.resolveLockInfo(db);
            assertEquals(String.format("[{\"threadId\":%d,\"threadName\":\"%s\"}]", waiter.getId(), waiter.getName()),
                    item.getWaiter_list());
            db.readUnlock();
        }
=======
            while (waiter.getState() != State.WAITING) {
                Thread.sleep(1000);
            }

            item = SysFeLocks.resolveLockInfo(db);
            assertEquals(String.format("[{\"threadId\":%d,\"threadName\":\"%s\"}]", waiter.getId(), waiter.getName()),
                    item.getWaiter_list());
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        Config.lock_manager_enabled = true;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

}