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
import com.starrocks.qe.ConnectContext;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.function.Function;

public class Tracers {
    public enum Mode {
        NONE, LOGS, VARS, TIMER, TIMING,
    }

    public enum Module {
        NONE, ALL, BASE, OPTIMIZER, SCHEDULER, ANALYZE, MV, EXTERNAL,
    }

    private static final Tracer EMPTY_TRACER = new Tracer() {
    };

    private static final ThreadLocal<Tracers> THREAD_LOCAL = ThreadLocal.withInitial(Tracers::new);

    // [empty tracer, real tracer]
    private final Tracer[] allTracer = new Tracer[] {EMPTY_TRACER, EMPTY_TRACER};

    // mark enable module
    private int moduleMask = 0;

    // mark enable mode
    private int modeMask = 0;

    private boolean isCommandLog = false;

    private Tracer tracer(Module module, Mode mode) {
        // need return real tracer when mode && module enable
        // enable mode is `modeMask |= 1 << mode.ordinal()`, check mode is `(modeMask >> mode.ordinal()) & 1`, so
        // when enable mode will return allTracer[1], disable will return allTracer[0]
        return allTracer[(modeMask >> mode.ordinal()) & (moduleMask >> module.ordinal() & 1)];
    }

    public static void register(ConnectContext context) {
        Tracers tracers = THREAD_LOCAL.get();
        tracers.isCommandLog = StringUtils.equalsIgnoreCase("command", context.getSessionVariable().getTraceLogMode());
        LogTracer logTracer = tracers.isCommandLog ? new CommandLogTracer() : new FileLogTracer();
        tracers.allTracer[0] = EMPTY_TRACER;
        tracers.allTracer[1] = new TracerImpl(Stopwatch.createStarted(), new TimeWatcher(), new VarTracer(), logTracer);
    }

    public static void register() {
        // default register FileLogTracer
        Tracers tracers = THREAD_LOCAL.get();
        LogTracer logTracer = new FileLogTracer();
        tracers.allTracer[0] = EMPTY_TRACER;
        tracers.allTracer[1] = new TracerImpl(Stopwatch.createStarted(), new TimeWatcher(), new VarTracer(), logTracer);
    }

    public static void init(ConnectContext context, Mode mode, String moduleStr) {
        Tracers tracers = THREAD_LOCAL.get();
        boolean enableProfile =
                context.getSessionVariable().isEnableProfile() || context.getSessionVariable().isEnableBigQueryProfile();
        boolean checkMV = context.getSessionVariable().isEnableMaterializedViewRewriteOrError();

        Module module = getTraceModule(moduleStr);
        if (Module.NONE == module || null == module) {
            tracers.moduleMask = 0;
        }
        if (Mode.NONE == mode || null == mode) {
            tracers.modeMask = 0;
        }
        if (enableProfile) {
            tracers.moduleMask |= 1 << Module.BASE.ordinal();
            tracers.moduleMask |= 1 << Module.EXTERNAL.ordinal();
            tracers.moduleMask |= 1 << Module.SCHEDULER.ordinal();

            tracers.modeMask |= 1 << Mode.TIMER.ordinal();
            tracers.modeMask |= 1 << Mode.VARS.ordinal();
        }
        if (checkMV) {
            tracers.moduleMask |= 1 << Module.MV.ordinal();

            tracers.modeMask |= 1 << Mode.VARS.ordinal();
        }
        if (Module.ALL == module) {
            tracers.moduleMask = Integer.MAX_VALUE;
        } else if (Module.NONE != module && null != module) {
            tracers.moduleMask |= 1 << Module.BASE.ordinal();
            tracers.moduleMask |= 1 << module.ordinal();
        }

        if (Mode.TIMING == mode) {
            tracers.modeMask = Integer.MAX_VALUE;
        } else if (Mode.NONE != mode && null != mode) {
            tracers.modeMask |= 1 << mode.ordinal();
        }
    }

    public static void close() {
        THREAD_LOCAL.remove();
    }

    private static Module getTraceModule(String str) {
        try {
            if (str != null) {
                return Module.valueOf(str.toUpperCase());
            }
        } catch (Exception e) {
            return Module.NONE;
        }
        return Module.NONE;
    }

    public static Timer watchScope(String name) {
        Tracers tracers = THREAD_LOCAL.get();
        return tracers.tracer(Module.BASE, Mode.TIMER).watchScope(name);
    }

    public static Timer watchScope(Module module, String name) {
        Tracers tracers = THREAD_LOCAL.get();
        return tracers.tracer(module, Mode.TIMER).watchScope(name);
    }

    public static void log(Module module, String log) {
        Tracers tracers = THREAD_LOCAL.get();
        tracers.tracer(module, Mode.LOGS).log(log);
    }

    public static void log(Module module, String log, Object... args) {
        Tracers tracers = THREAD_LOCAL.get();
        tracers.tracer(module, Mode.LOGS).log(log, args);
    }

    // lazy log, use it if you want to avoid construct log string when log is disabled
    public static void log(Module module, Function<Object[], String> func, Object... args) {
        Tracers tracers = THREAD_LOCAL.get();
        tracers.tracer(module, Mode.LOGS).log(func, args);
    }

    public static void log(String log, Object... args) {
        Tracers tracers = THREAD_LOCAL.get();
        tracers.tracer(Module.BASE, Mode.TIMER).log(log, args);
    }

    public static void record(Module module, String name, String value) {
        Tracers tracers = THREAD_LOCAL.get();
        tracers.tracer(module, Mode.VARS).record(name, value);
    }

    public static void count(Module module, String name, int count) {
        Tracers tracers = THREAD_LOCAL.get();
        tracers.tracer(module, Mode.VARS).count(name, count);
    }

    public static List<Var<?>> getAllVars() {
        Tracers tracers = THREAD_LOCAL.get();
        return tracers.allTracer[1].getAllVars();
    }

    public static String printScopeTimer() {
        Tracers tracers = THREAD_LOCAL.get();
        return tracers.allTracer[1].printScopeTimer();
    }

    public static String printTiming() {
        Tracers tracers = THREAD_LOCAL.get();
        return tracers.allTracer[1].printTiming();
    }

    public static String printVars() {
        Tracers tracers = THREAD_LOCAL.get();
        return tracers.allTracer[1].printVars();
    }

    public static String printLogs() {
        Tracers tracers = THREAD_LOCAL.get();
        return tracers.allTracer[1].printLogs();
    }

    public static void toRuntimeProfile(RuntimeProfile profile) {
        Tracers tracers = THREAD_LOCAL.get();
        tracers.allTracer[1].toRuntimeProfile(profile);
    }
}
