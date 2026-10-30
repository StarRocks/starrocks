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
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.LogUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Collection;

public class LockChecker extends FrontendDaemon {

    private static final Logger LOG = LogManager.getLogger(LockChecker.class);
    private static final int DEFAULT_STACK_RESERVE_LEVELS = 20;

    public LockChecker() {
        super("deadlock-checker", 1000 * Config.lock_checker_interval_second);
    }

    @Override
    protected void runAfterCatalogReady() {
        checkDeadlocks();

        setInterval(Config.lock_checker_interval_second * 1000);
    }

    public static JsonArray getLockWaiterInfoJsonArray(Collection<Thread> waiters) {
        return getLockWaiterInfoJsonArray(waiters, 0);
    }

    /**
     * Build a JSON array of waiter thread info, capped at {@code cap} entries when {@code cap > 0}.
     * If the source list contains more than {@code cap} non-null entries, a trailer object
     * {@code {"omitted": "remain N waiters omitted"}} is appended so the total count is still
     * visible. {@code cap <= 0} disables the cap and serializes every waiter.
     */
    public static JsonArray getLockWaiterInfoJsonArray(Collection<Thread> waiters, int cap) {
        JsonArray waiterInfos = new JsonArray();
        int emitted = 0;
        int totalNonNull = 0;
        for (Thread th : CollectionUtils.emptyIfNull(waiters)) {
            if (th == null) {
                continue;
            }
            totalNonNull++;
            if (cap > 0 && emitted >= cap) {
                continue;
            }
            JsonObject waiter = new JsonObject();
            waiter.addProperty("threadId", th.getId());
            waiter.addProperty("threadName", th.getName());
            waiterInfos.add(waiter);
            emitted++;
        }
        if (cap > 0 && totalNonNull > cap) {
            JsonObject omitted = new JsonObject();
            omitted.addProperty("omitted", "remain " + (totalNonNull - cap) + " waiters omitted");
            waiterInfos.add(omitted);
        }
        return waiterInfos;
    }

    private void checkDeadlocks() {
        if (Config.lock_checker_enable_deadlock_check) {
            ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
            long[] ids = tmx.findDeadlockedThreads();
            if (ids != null) {
                for (long id : ids) {
                    LOG.info("deadlock thread: {}", LogUtil.getStackTraceToJsonArray(
                            tmx.getThreadInfo(id, 50),
                            0,
                            DEFAULT_STACK_RESERVE_LEVELS));
                }
            }
        }
    }
}
