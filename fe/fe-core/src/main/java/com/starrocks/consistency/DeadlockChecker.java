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
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.QueryableReentrantReadWriteLock;
import com.starrocks.common.util.Util;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeadlockChecker extends LeaderDaemon {

    private static final Logger LOG = LogManager.getLogger(DeadlockChecker.class);

    public DeadlockChecker() {
        super("DeadlockChecker", 1000 * Config.deadlock_checker_interval_second);
    }

    @Override
    protected void runAfterCatalogReady() {
        checkDbLocks();
        checkDeadlocks();
        checkSlowLock();

        setInterval(Config.deadlock_checker_interval_second * 1000);
    }

    private void checkDbLocks() {
        Map<String, Database> dbs = GlobalStateMgr.getCurrentState().getFullNameToDb();
        JsonArray dbLocks = new JsonArray();
        for (Database db : dbs.values()) {
            boolean useful = false;
            JsonObject ownerInfo = new JsonObject();
            String name = db.getFullName();
            ownerInfo.addProperty("lockDbName", name);

            QueryableReentrantReadWriteLock lock = db.getLock();

            // holder information
            Thread owner = lock.getOwner();
            List<ThreadInfo> sharedLockThreads = lock.getSharedLockThreads();
            if (owner != null) {
                useful = true;
                ownerInfo.addProperty("ownerThreadName", owner.getName());
                ownerInfo.addProperty("ownerThreadId", owner.getId());
                if (Config.deadlock_checker_print_detail_info) {
                    ownerInfo.addProperty("stack", Util.dumpThread(owner, 50));
                }
                if (lock.isWriteLocked()) {
                    ownerInfo.addProperty("lockState", "writeLocked");
                }
            } else if (sharedLockThreads.size() > 0) {
                useful = true;
                ownerInfo.addProperty("lockState", "readLocked");
                ownerInfo.addProperty("readLockCount", sharedLockThreads.size());
                StringBuilder infos = new StringBuilder();
                for (ThreadInfo threadInfo : sharedLockThreads) {
                    infos.append("name: ").append(threadInfo.getThreadName())
                            .append("id: ").append(threadInfo.getThreadId()).append(", ");
                }
                ownerInfo.addProperty("threadInfo", infos.toString());
                if (Config.deadlock_checker_print_detail_info) {
                    StringBuilder sb = new StringBuilder();
                    // print 5 thread stacks at most.
                    for (int i = 0; i < 5 && i < sharedLockThreads.size(); i++) {
                        sb.append(Util.dumpThread(sharedLockThreads.get(i), 50)).append("\n");
                    }
                    ownerInfo.addProperty("stacks", sb.toString());
                }
            }

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
            if (!waiterIds.isEmpty()) {
                useful = true;
                ownerInfo.add("lockWaiters", waiterIds);
            }

            if (useful) {
                dbLocks.add(ownerInfo);
            }
        }

        if (!dbLocks.isEmpty()) {
            LOG.info("dbLocks: {}", dbLocks.toString());
        } else {
            LOG.debug("no db locks held");
        }
    }

    private void checkDeadlocks() {
        ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
        long[] ids = tmx.findDeadlockedThreads();
        if (ids != null) {
            LOG.info("deadlock threads: {}", ids);
        }
    }

    private void checkSlowLock() {
        Map<String, Database> dbs = GlobalStateMgr.getCurrentState().getFullNameToDb();
        Map<QueryableReentrantReadWriteLock, Thread> lockOwnerMap = new HashMap<>();

        for (Database db : dbs.values()) {
            QueryableReentrantReadWriteLock lock = db.getLock();
            Thread owner = lock.getOwner();
            if (owner != null) {
                lockOwnerMap.put(lock, owner);
            }
        }
        if (MapUtils.isEmpty(lockOwnerMap)) {
            return;
        }

        // sleep 5s and check whether the lock is still held
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            LOG.warn("check slow lock failed", e);
            return;
        }

        for (Map.Entry<QueryableReentrantReadWriteLock, Thread> entry : lockOwnerMap.entrySet()) {
            Thread currentOwner = entry.getKey().getOwner();
            if (currentOwner != null && currentOwner.getId() == entry.getValue().getId()) {
                String stack = Arrays.toString(currentOwner.getStackTrace()).replace(',', '\n');
                LOG.warn("thread {}-{} hold the lock {} too long, with waiters: [{}], stack: {}",
                        currentOwner.getId(), currentOwner.getName(),
                        entry.getKey(), entry.getKey().getQueuedThreads(),
                        stack);
            }
        }
    }

}
