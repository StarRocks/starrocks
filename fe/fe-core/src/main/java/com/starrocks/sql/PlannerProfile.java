// This file is made available under Elastic License 2.0.

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.qe.ConnectContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * To timing a function or a piece of code, you could
 * ```
 * try(ScopedTimer _ = PlannerProfile.ScopedTimer("ComponentA.CompoenentB.FunctionC") {
 * // function or code.
 * }
 * ```
 * It's worth to note that, "." in `ComponentA.CompoenentB.FunctionC` is preserved for representing hierarchy,
 * So better not put use "." in CompoentA or CompoentB or FunctionC. And in profile, the metric looks like
 * - ComponentA
 * - ComoennentB
 * - FunctionC: 100ms / 10
 * It means FunctionC has executed 10 times, and 100ms in total.
 */

public class PlannerProfile {
    private ConnectContext ctx;

    public static class ScopedTimer implements AutoCloseable {
        private long startTime = 0;
        private volatile long currentThreadId = 0;
        private long totalTime = 0;
        private int totalCount = 0;
        // possible to record p99?

        public void start() {
            Preconditions.checkState(currentThreadId == 0);
            currentThreadId = Thread.currentThread().getId();
            startTime = System.currentTimeMillis();
        }

        public void close() {
            Preconditions.checkState(currentThreadId == Thread.currentThread().getId());
            currentThreadId = 0;
            totalTime += (System.currentTimeMillis() - startTime);
            totalCount += 1;
        }

        public long getTotalTime() {
            return totalTime;
        }

        public int getTotalCount() {
            return totalCount;
        }
    }

    private final Map<String, ScopedTimer> timers = new ConcurrentHashMap<>();
    private final Map<String, Long> counters = new ConcurrentHashMap<>();

    public PlannerProfile() {
    }

    public void init(ConnectContext ctx) {
        this.ctx = ctx;
    }

    private ScopedTimer getOrCreateScopedTimer(String name) {
        return timers.computeIfAbsent(name, (key) -> new ScopedTimer());
    }

    private static final PlannerProfile DEFAULT_INSTANCE = new PlannerProfile();

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

    public static void addCounter(String name, long value) {
        // to avoid null.
        PlannerProfile p = DEFAULT_INSTANCE;
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            p = ctx.getPlannerProfile();
        }
        p.counters.put(name, p.counters.getOrDefault(name, 0L) + value);
    }

    private RuntimeProfile getRuntimeProfile(RuntimeProfile parent, Map<String, RuntimeProfile> cache,
                                             String prefix) {
        if (cache.containsKey(prefix)) {
            return cache.get(prefix);
        }
        String[] ss = prefix.split("\\.");
        StringBuilder sb = new StringBuilder();
        RuntimeProfile p = parent;
        for (String s : ss) {
            sb.append(s);
            sb.append('.');
            String tmp = sb.toString();
            if (!cache.containsKey(tmp)) {
                RuntimeProfile sp = new RuntimeProfile(s);
                p.addChild(sp);
                cache.put(tmp, sp);
            }
            p = cache.get(tmp);
        }
        return p;
    }

    private static String getKeyPrefix(String key) {
        String prefix = "";
        int index = key.lastIndexOf('.');
        if (index != -1) {
            prefix = key.substring(0, index + 1);
        }
        return prefix;
    }

    public void buildTimers(RuntimeProfile parent) {

        Map<String, RuntimeProfile> profilers = new HashMap<>();
        profilers.put("", parent);

        List<String> keys = new ArrayList<>(timers.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            String prefix = getKeyPrefix(key);
            String name = key.substring(prefix.length());
            RuntimeProfile p = getRuntimeProfile(parent, profilers, prefix);
            ScopedTimer t = timers.get(key);
            p.addInfoString(name, String.format("%dms / %d", t.getTotalTime(), t.getTotalCount()));
        }

        keys = new ArrayList<>(counters.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            String prefix = getKeyPrefix(key);
            String name = key.substring(prefix.length());
            RuntimeProfile p = getRuntimeProfile(parent, profilers, prefix);
            Long value = counters.get(key);
            p.addInfoString(name, String.format("%d", value));
        }
    }

    public void build(RuntimeProfile parent) {
        buildTimers(parent);
    }

    public void reset() {
        timers.clear();
    }
}
