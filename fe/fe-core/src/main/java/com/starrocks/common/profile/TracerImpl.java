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

package com.starrocks.common.profile;

import com.google.common.base.Stopwatch;
import com.starrocks.common.util.RuntimeProfile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

class TracerImpl extends Tracer {
    private final Stopwatch tracerCost = Stopwatch.createUnstarted();
    private final Stopwatch timing;
    private final TimeWatcher watcher;
    private final VarTracer varTracer;
    private final LogTracer logTracer;

    public TracerImpl(Stopwatch timing, TimeWatcher watcher, VarTracer vars, LogTracer logTracer) {
        this.timing = timing;
        this.watcher = watcher;
        this.varTracer = vars;
        this.logTracer = logTracer;
    }

    private long timePoint() {
        return timing.elapsed(TimeUnit.MILLISECONDS);
    }

    public Timer watchScope(String name) {
        tracerCost.start();
        Timer t = watcher.scope(timePoint(), name);
        tracerCost.stop();
        return t;
    }

    public void log(String event) {
        tracerCost.start();
        logTracer.log(timePoint(), event);
        tracerCost.stop();
    }

    public void log(String event, Object... args) {
        tracerCost.start();
        logTracer.log(timePoint(), event, args);
        tracerCost.stop();
    }

    @Override
    // lazy log, use it if you want to avoid construct log string when log is disabled
    public void log(Function<Object[], String> func, Object... args) {
        tracerCost.start();
        logTracer.log(timePoint(), func, args);
        tracerCost.stop();
    }

    public void record(String name, String value) {
        tracerCost.start();
        varTracer.record(timePoint(), name, value);
        tracerCost.stop();
    }

    public void count(String name, int count) {
        tracerCost.start();
        varTracer.count(timePoint(), name, count);
        tracerCost.stop();
    }

    public List<Var<?>> getAllVars() {
        return varTracer.getAllVars();
    }

    public String printScopeTimer() {
        StringBuilder sb = new StringBuilder();
        long fixed = String.valueOf(timePoint()).length();
        String fixedString = "%" + fixed + "dms|";
        for (Timer timer : watcher.getAllTimerWithOrder()) {
            sb.append(String.format(fixedString, timer.getFirstTimePoint()));
            sb.append(timer);
            sb.append("\n");
        }

        printCosts(sb);
        return sb.toString();
    }

    private void printCosts(StringBuilder sb) {
        sb.append("Tracer Cost: ");
        sb.append(tracerCost.elapsed(TimeUnit.MICROSECONDS));
        sb.append("us");
    }

    public String printTiming() {
        Map<Long, String> timings = new TreeMap<>();
        for (Timer timer : watcher.getAllTimerWithOrder()) {
            timings.put(timer.getFirstTimePoint(), "watchScope: " + timer.name());
        }
        for (LogTracer.LogEvent log : logTracer.getLogs()) {
            timings.put(log.getTimePoint(), "log: " + log.getLog());
        }
        for (Var<?> var : varTracer.getAllVars()) {
            timings.put(var.getTimePoint(), "record: " + var.getName());
        }
        long fixed = String.valueOf(timePoint()).length();
        String fixedString = "%" + fixed + "dms|";
        StringBuilder sb = new StringBuilder();
        timings.forEach((k, v) -> {
            sb.append(String.format(fixedString, k));
            sb.append(" ");
            sb.append(v);
            sb.append("\n");
        });

        printCosts(sb);
        return sb.toString();
    }

    public String printVars() {
        StringBuilder sb = new StringBuilder();
        long fixed = String.valueOf(timePoint()).length();
        String fixedString = "%" + fixed + "dms|";
        for (Var<?> var : varTracer.getAllVarsWithOrder()) {
            sb.append(String.format(fixedString, var.getTimePoint()));
            sb.append(" ");
            sb.append(var);
            sb.append("\n");
        }

        printCosts(sb);
        return sb.toString();
    }

    public String printLogs() {
        StringBuilder sb = new StringBuilder();
        long fixed = String.valueOf(timePoint()).length();
        String fixedString = "%" + fixed + "dms|";
        for (LogTracer.LogEvent log : logTracer.getLogs()) {
            sb.append(String.format(fixedString, log.getTimePoint()));
            sb.append("    ");
            sb.append(log.getLog());
            sb.append("\n");
        }

        printCosts(sb);
        return sb.toString();
    }

    // ----------------- runtime profile -----------------
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
        for (Timer timer : watcher.getAllTimerWithOrder()) {
            parent.addInfoString(timer.toString(), "");
        }
    }

    public void buildVars(RuntimeProfile parent) {
        Map<String, RuntimeProfile> profilers = new HashMap<>();
        profilers.put("", parent);
        for (Var<?> var : varTracer.getAllVarsWithOrder()) {
            String prefix = getKeyPrefix(var.name);
            String name = var.name.substring(prefix.length());
            RuntimeProfile p = getRuntimeProfile(parent, profilers, prefix);
            p.addInfoString(name, var.value.toString());
        }
    }

    public void toRuntimeProfile(RuntimeProfile parent) {
        buildTimers(parent);
        buildVars(parent);
    }

}
