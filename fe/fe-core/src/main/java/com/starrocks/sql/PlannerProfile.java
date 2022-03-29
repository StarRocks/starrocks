// This file is made available under Elastic License 2.0.

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.qe.ConnectContext;

import java.util.HashMap;

public class PlannerProfile {

    public static class ScopedTimer implements AutoCloseable {
        private long startTime = 0;
        private long currentThreadId = 0;
        private long totalTime = 0;

        public void start() {
            Preconditions.checkState(currentThreadId == 0);
            currentThreadId = Thread.currentThread().getId();
            startTime = System.currentTimeMillis();
        }

        public void close() {
            Preconditions.checkState(currentThreadId == Thread.currentThread().getId());
            currentThreadId = 0;
            totalTime += (System.currentTimeMillis() - startTime);
        }

        public long getTotalTime() {
            return totalTime;
        }
    }

    private HashMap<String, ScopedTimer> timers;

    public PlannerProfile() {
        timers = new HashMap<>();
    }

    private synchronized ScopedTimer getOrCreateScopedTimer(String name) {
        if (timers.containsKey(name)) {
            return timers.get(name);
        }
        ScopedTimer t = new ScopedTimer();
        timers.put(name, t);
        return t;
    }

    private static PlannerProfile DEFAULT_INSTANCE = new PlannerProfile();

    public static ScopedTimer getScopedTimer(String name) {
        // to avoid null.
        PlannerProfile p = DEFAULT_INSTANCE;
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            p = ctx.getPlannerProfile();
        }
        ScopedTimer t = p.getOrCreateScopedTimer(name);
        t.start();
        return t;
    }

    public void build(RuntimeProfile p) {

    }
}
