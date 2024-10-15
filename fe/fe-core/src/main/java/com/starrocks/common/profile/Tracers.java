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
        NONE, LOGS, VARS, TIMER, TIMING, REASON
    }

    public enum Module {
        NONE, ALL, BASE, OPTIMIZER, SCHEDULER, ANALYZE, MV, EXTERNAL, PARSER
    }

    private static final Tracer EMPTY_TRACER = new Tracer() {
    };

    private static final ThreadLocal<Tracers> THREAD_LOCAL = ThreadLocal.withInitial(Tracers::new);

    // [empty tracer, real tracer]
    private final Tracer[] allTracer = new Tracer[] {EMPTY_TRACER, EMPTY_TRACER};

    // mark enable module, default enable parser module
    private int moduleMask = 1 << Module.PARSER.ordinal();

    // mark enable mode, default enable timer mode
    private int modeMask = 1 << Mode.TIMER.ordinal();

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
        LogTracer reasonTracer = tracers.isCommandLog ? new CommandLogTracer() : new FileLogTracer();
        tracers.allTracer[0] = EMPTY_TRACER;
        tracers.allTracer[1] = new TracerImpl(Stopwatch.createStarted(), new TimeWatcher(), new VarTracer(), logTracer,
                reasonTracer);
    }

    public static void register() {
        // default register FileLogTracer
        Tracers tracers = THREAD_LOCAL.get();
        LogTracer logTracer = new FileLogTracer();
        LogTracer reasonTracer = new FileLogTracer();
        tracers.allTracer[0] = EMPTY_TRACER;
        tracers.allTracer[1] = new TracerImpl(Stopwatch.createStarted(), new TimeWatcher(), new VarTracer(), logTracer,
                reasonTracer);
    }

    // for record metrics in parallel
    public static Tracers get() {
        return THREAD_LOCAL.get();
    }

    /**
     * Init tracer with context and mode.
     * @param context connect context
     * @param mode tracer mode
     * @param moduleStr tracer module
     */
    public static void init(ConnectContext context, Mode mode, String moduleStr) {
        boolean enableProfile =
                context.getSessionVariable().isEnableProfile() || context.getSessionVariable().isEnableBigQueryProfile();
        boolean checkMV = context.getSessionVariable().isEnableMaterializedViewRewriteOrError();
        Module module = getTraceModule(moduleStr);
        init(mode, module, enableProfile, checkMV);
    }

    /**
     * Init tracer with params.
     * @param mode tracer mode
     * @param module tracer module
     * @param enableProfile enable profile or not
     * @param checkMV check mv or not
     */
    public static void init(Mode mode, Module module, boolean enableProfile, boolean checkMV) {
        Tracers tracers = THREAD_LOCAL.get();
        // reset all mark
        tracers.moduleMask = 0;
        tracers.modeMask = 0;
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
            tracers.moduleMask |= 1 << Module.MV.ordinal();

            tracers.modeMask |= 1 << Mode.TIMER.ordinal();
            tracers.modeMask |= 1 << Mode.VARS.ordinal();
            tracers.modeMask |= 1 << Mode.REASON.ordinal();
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

    public static boolean isSetTraceMode(Mode e) {
        Tracers tracers = THREAD_LOCAL.get();
        return (tracers.modeMask & 1 << e.ordinal()) != 0;
    }

    public static boolean isSetTraceModule(Module m) {
        Tracers tracers = THREAD_LOCAL.get();
        return (tracers.moduleMask & 1 << m.ordinal()) != 0;
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

    public static Timer watchScope(Tracers tracers, Module module, String name) {
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

    public static void reasoning(Module module, String reason, Object... args) {
        Tracers tracers = THREAD_LOCAL.get();
        tracers.tracer(module, Mode.REASON).reason(reason, args);
    }

    public static void record(String name, String value) {
        record(Module.BASE, name, value);
    }

    public static void record(Module module, String name, String value) {
        Tracers tracers = THREAD_LOCAL.get();
        tracers.tracer(module, Mode.VARS).record(name, value);
    }

    public static void record(Tracers tracers, Module module, String name, String value) {
        tracers.tracer(module, Mode.VARS).record(name, value);
    }

    public static void count(Module module, String name, long count) {
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

    public static String printReasons() {
        Tracers tracers = THREAD_LOCAL.get();
        return tracers.allTracer[1].printReasons();
    }

    public static void toRuntimeProfile(RuntimeProfile profile) {
        Tracers tracers = THREAD_LOCAL.get();
        tracers.allTracer[1].toRuntimeProfile(profile);
    }

    public static String getTrace(Mode mode) {
        switch (mode) {
            case TIMER:
                return Tracers.printScopeTimer();
            case VARS:
                return Tracers.printVars();
            case TIMING:
                return Tracers.printTiming();
            case LOGS:
                return Tracers.printLogs();
            case REASON:
                return Tracers.printReasons();
            default:
                return "";
        }
    }
}
