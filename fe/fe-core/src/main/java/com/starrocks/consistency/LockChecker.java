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

package com.starrocks.consistency;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.concurrent.QueryableReentrantReadWriteLock;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class LockChecker extends FrontendDaemon {

    private static final Logger LOG = LogManager.getLogger(LockChecker.class);

    public LockChecker() {
        super("DeadlockChecker", 1000 * Config.lock_checker_interval_second);
    }

    @Override
    protected void runAfterCatalogReady() {
        checkDeadlocks();
        checkSlowLocks();

        setInterval(Config.lock_checker_interval_second * 1000);
    }

    private void checkSlowLocks() {
        Map<String, Database> dbs = GlobalStateMgr.getCurrentState().getFullNameToDb();
        JsonArray dbLocks = new JsonArray();
        for (Database db : dbs.values()) {
            boolean hasSlowLock = false;
            JsonObject ownerInfo = new JsonObject();
            QueryableReentrantReadWriteLock lock = db.getLock();
            // holder information
            Thread exclusiveLockThread = lock.getOwner();
            List<Long> sharedLockThreadIds = lock.getSharedLockThreadIds();
            if (exclusiveLockThread != null) {
                long lockStartTime = db.getLock().getExclusiveLockTime();
                if (lockStartTime > 0L && System.currentTimeMillis() - lockStartTime > Config.slow_lock_threshold_ms) {
                    hasSlowLock = true;
                    ownerInfo.addProperty("lockState", "writeLocked");
                    ownerInfo.addProperty("lockHoldTime", (System.currentTimeMillis() - lockStartTime) + " ms");
                    ownerInfo.addProperty("dumpThread", Util.dumpThread(exclusiveLockThread, 50));
                }
            } else if (sharedLockThreadIds.size() > 0) {
                StringBuilder infos = new StringBuilder();
                int slowReadLockCnt = 0;
                for (long threadId : sharedLockThreadIds) {
                    long lockStartTime = lock.getSharedLockTime(threadId);
                    if (lockStartTime > 0L && System.currentTimeMillis() - lockStartTime > Config.slow_lock_threshold_ms) {
                        hasSlowLock = true;
                        ThreadInfo threadInfo = ManagementFactory.getThreadMXBean().getThreadInfo(threadId, 50);
                        infos.append("lockHoldTime: ").append(System.currentTimeMillis() - lockStartTime).append(" ms;");
                        infos.append(Util.dumpThread(threadInfo, 50)).append(";");
                        slowReadLockCnt++;
                    }
                }
                if (slowReadLockCnt > 0) {
                    ownerInfo.addProperty("lockState", "readLocked");
                    ownerInfo.addProperty("slowReadLockCount", slowReadLockCnt);
                    ownerInfo.addProperty("dumpThreads", infos.toString());
                }
            }

            if (hasSlowLock) {
                ownerInfo.addProperty("lockDbName", db.getFullName());
                // waiters
                Collection<Thread> waiters = lock.getQueuedThreads();
                JsonArray waiterIds = new JsonArray();
                for (Thread th : CollectionUtils.emptyIfNull(waiters)) {
                    if (th != null) {
                        JsonObject waiter = new JsonObject();
                        waiter.addProperty("threadId", th.getId());
                        waiter.addProperty("threadName", th.getName());
                        waiterIds.add(waiter);
                    }
                }
                ownerInfo.add("lockWaiters", waiterIds);
                dbLocks.add(ownerInfo);
            }
        }

        if (!dbLocks.isEmpty()) {
            LOG.info("slow db locks: {}", dbLocks.toString());
        } else {
            LOG.debug("no slow db locks");
        }
    }

    private void checkDeadlocks() {
        if (Config.lock_checker_enable_deadlock_check) {
            ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
            long[] ids = tmx.findDeadlockedThreads();
            if (ids != null) {
                for (long id : ids) {
                    LOG.info("deadlock thread: {}", Util.dumpThread(tmx.getThreadInfo(id, 50), 50));
                }
            }
        }
    }
}
