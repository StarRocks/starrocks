// This file is made available under Elastic License 2.0.

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.StatementBase;
import org.apache.hadoop.util.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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
    private ConnectContext ctx;

    public static class ScopedTimer implements AutoCloseable {
        private volatile long currentThreadId = 0;
        private int totalCount = 0;
        // possible to record p99?

        private final Stopwatch watch = Stopwatch.createUnstarted();

        public void start() {
            Preconditions.checkState(currentThreadId == 0);
            currentThreadId = Thread.currentThread().getId();
            watch.start();
        }

        public void close() {
            Preconditions.checkState(currentThreadId == Thread.currentThread().getId());
            currentThreadId = 0;
            totalCount += 1;
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

    public static class LogTracer {
        private final String name;
        // To avoid logs too large, restrict the logs' size for each tracer.
        private static final int MAX_LOG_SIZE = 128;
        // Container to store the trace logs.
        private final List<String> logs = Collections.synchronizedList(Lists.newLinkedList());

        public LogTracer(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void log(String content) {
            if (logs.size() >= MAX_LOG_SIZE) {
                return;
            }
            this.logs.add(content);
        }

        public List<String> getLogs() {
            return this.logs;
        }

        public String toString(String name) {
            StringBuilder sb = new StringBuilder();
            sb.append("-- [TRACE: " + name + "]\n");
            this.logs.stream().forEach(log -> sb.append("   " + log + "\n"));
            return sb.toString();
        }
    }

    private final Map<String, ScopedTimer> timers = new ConcurrentHashMap<>();
    private final Map<String, LogTracer> tracers = Collections.synchronizedMap(Maps.newLinkedHashMap());
    private final Map<String, String> customProperties = new ConcurrentHashMap<>();

    public PlannerProfile() {
    }

    public void init(ConnectContext ctx) {
        this.ctx = ctx;
    }

    private ScopedTimer getOrCreateScopedTimer(String name) {
        return timers.computeIfAbsent(name, (key) -> new ScopedTimer());
    }

    public static ScopedTimer getScopedTimer(String name) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null || ctx.getPlannerProfile() == null ||
                (ctx.getExplainLevel() != StatementBase.ExplainLevel.OPTIMIZER &&
                        !ctx.getSessionVariable().isEnableProfile())) {
            return null;
        }

        PlannerProfile p = ctx.getPlannerProfile();
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
        tracers.clear();
        customProperties.clear();
    }

    private static Long getTime(String prefix, Map<String, PlannerProfile.ScopedTimer> times) {
        if (times.containsKey(prefix)) {
            return times.get(prefix).getTotalTime();
        } else {
            return 0L;
        }
    }

    private static String print(String name, long time, int step) {
        return String.join("", Collections.nCopies(step, "    ")) + "-- " + name + " " + time + "ms" + "\n";
    }

    public static String printPlannerTimer(PlannerProfile profile) {
        StringBuilder trace = new StringBuilder();
        Map<String, PlannerProfile.ScopedTimer> times = profile.getTimers();

        trace.append(print("Total", getTime("Total", times), 0));
        trace.append(print("Parser", getTime("Parser", times), 1));
        trace.append(print("Analyzer", getTime("Analyzer", times), 1));
        trace.append(print("Optimizer", getTime("Optimizer", times), 1));
        trace.append(print("Optimizer.RuleBaseOptimize",
                getTime("Optimizer.RuleBaseOptimize", times), 2));
        trace.append(print("Optimizer.CostBaseOptimize",
                getTime("Optimizer.CostBaseOptimize", times), 2));
        trace.append(print("Optimizer.PhysicalRewrite",
                getTime("Optimizer.PhysicalRewrite", times), 2));
        trace.append(print("Optimizer.preprocessMvs",
                getTime("Optimizer.preprocessMvs", times), 2));
        trace.append(print("ExecPlanBuild", getTime("ExecPlanBuild", times), 1));

        return trace.toString();
    }

    ///// LOG TRACER /////
    private LogTracer getOrCreateLogTracer(String name) {
        return tracers.computeIfAbsent(name, key -> new LogTracer(name));
    }

    public Map<String, LogTracer> getTracers() {
        return tracers;
    }

    /**
     * Callers should take care the result of this method may be null because tracer is only
     * enabled when trace command or profile is enabled.
     *
     * @param name : tracer's name to display.
     * @return
     */
    public static LogTracer getLogTracer(String name) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null || ctx.getPlannerProfile() == null ||
                ctx.getExplainLevel() != StatementBase.ExplainLevel.REWRITE) {
            return null;
        }

        PlannerProfile p = ctx.getPlannerProfile();
        return p.getOrCreateLogTracer(name);
    }

    public static String printPlannerTrace(PlannerProfile profile) {
        StringBuilder sb = new StringBuilder();
        Map<String, LogTracer> traces = profile.getTracers();
        traces.entrySet().stream()
                .forEach(entry ->
                        sb.append(entry.getValue().toString(entry.getKey())));
        return sb.toString();
    }
}
