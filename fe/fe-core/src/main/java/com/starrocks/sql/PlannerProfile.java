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

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.connector.RemoteFileOperations.HMS_PARTITIONS_REMOTE_FILES;
import static com.starrocks.connector.hive.HiveMetastoreOperations.BACKGROUND_THREAD_NAME_PREFIX;

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
    private static final Logger LOG = LogManager.getLogger(PlannerProfile.class);

    public static class ScopedTimer implements AutoCloseable {
        private volatile long currentThreadId = 0;
        private PlannerProfile profile;
        private final String name;
        private final int level;
        private final int order;
        private final AtomicInteger count = new AtomicInteger(0);
        private int totalCount = 0;
        // possible to record p99?

        private final Stopwatch watch = Stopwatch.createUnstarted();

        public ScopedTimer(PlannerProfile profile, String name) {
            this.profile = profile;
            this.name = name;
            this.level = profile.levels.getAndIncrement();
            this.order = profile.orders.getAndIncrement();
        }

        public void start() {
            if (count.getAndIncrement() == 0) {
                Preconditions.checkState(currentThreadId == 0);
                currentThreadId = Thread.currentThread().getId();
                watch.start();
            } else {
                Preconditions.checkState(currentThreadId == Thread.currentThread().getId());
            }

            totalCount++;
        }

        public void close() {
            Preconditions.checkState(currentThreadId == Thread.currentThread().getId());
            if (count.decrementAndGet() != 0) {
                return;
            }
            if (profile != null) {
                profile.levels.decrementAndGet();
                profile = null;
            }
            currentThreadId = 0;
            watch.stop();

            printBackgroundLog();
        }

        private void printBackgroundLog() {
            String threadName = Thread.currentThread().getName();
            if (threadName.startsWith(BACKGROUND_THREAD_NAME_PREFIX)) {
                LOG.info("Get partitions or partition statistics cost time: {}", this.getTotalTime());
            }
        }

        public long getTotalTime() {
            return watch.elapsed(TimeUnit.MICROSECONDS) / 1000;
        }

        public int getTotalCount() {
            return totalCount;
        }
    }

    private final Map<String, ScopedTimer> timers = new ConcurrentHashMap<>();
    private final Map<String, String> customProperties = new ConcurrentHashMap<>();
    private final Map<String, Long> timePoint = new ConcurrentHashMap<>();
    private final AtomicInteger orders = new AtomicInteger();
    private final AtomicInteger levels = new AtomicInteger();
    private final Stopwatch timing = Stopwatch.createStarted();

    private ScopedTimer getOrCreateScopedTimer(String name) {
        return timers.computeIfAbsent(name, key -> {
            timePoint.put(name, timing.elapsed(TimeUnit.MICROSECONDS) / 1000);
            return new ScopedTimer(this, name);
        });
    }

    public static ScopedTimer getScopedTimer(String name) {
        // to avoid null.
        PlannerProfile p = new PlannerProfile();
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            p = ctx.getPlannerProfile();
        }
        ScopedTimer t = p.getOrCreateScopedTimer(name);
        t.start();
        return t;
    }

    public static void addCustomProperties(String name, String value) {
        PlannerProfile p = new PlannerProfile();
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            p = ctx.getPlannerProfile();
        }
        if (name.equals(HMS_PARTITIONS_REMOTE_FILES) && p.customProperties.containsKey(HMS_PARTITIONS_REMOTE_FILES)) {
            int currentSize = Integer.parseInt(p.customProperties.get(HMS_PARTITIONS_REMOTE_FILES));
            int addedSize = Integer.parseInt(value);
            p.customProperties.put(name, String.valueOf(currentSize + addedSize));
        } else {
            p.customProperties.put(name, value);
        }

        String threadName = Thread.currentThread().getName();
        if (threadName.startsWith(BACKGROUND_THREAD_NAME_PREFIX)) {
            LOG.info("Background collect hive column statistics profile: [{}:{}]", name, value);
        }
    }

    public Map<String, ScopedTimer> getTimers() {
        return timers;
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
    }

    public void buildCustomProperties(RuntimeProfile parent) {
        Map<String, RuntimeProfile> profilers = new HashMap<>();
        profilers.put("", parent);

        List<String> keys = new ArrayList<>(customProperties.keySet());
        Collections.sort(keys);

        for (String key : keys) {
            String prefix = getKeyPrefix(key);
            String name = key.substring(prefix.length());
            RuntimeProfile p = getRuntimeProfile(parent, profilers, prefix);
            String value = customProperties.get(key);
            p.addInfoString(name, value);
        }
    }

    public void build(RuntimeProfile parent) {
        buildTimers(parent);
        buildCustomProperties(parent);
    }

    public void reset() {
        timers.clear();
        customProperties.clear();
    }

    private static Long getTime(String prefix, Map<String, PlannerProfile.ScopedTimer> times) {
        if (times.containsKey(prefix)) {
            return times.get(prefix).getTotalTime();
        } else {
            return 0L;
        }
    }

    private static String print(String name, long point, ScopedTimer timer) {
        return String.format("%8dms|", point) + String.join("", Collections.nCopies(timer.level, "    "))
                + "-- " + name + "[" + timer.getTotalCount() + "] " + timer.getTotalTime() + "ms\n";
    }

    public static String printPlannerTimeCost(PlannerProfile profile) {
        StringBuilder trace = new StringBuilder();
        Map<String, PlannerProfile.ScopedTimer> times = profile.getTimers();

        times.entrySet().stream().sorted(Comparator.comparingInt(o -> o.getValue().order)).forEach(
                entry -> trace.append(print(entry.getKey(), profile.timePoint.get(entry.getKey()), entry.getValue())));
        return trace.toString();
    }
}
