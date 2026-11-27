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

package com.starrocks.utframe;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Records who killed a test JVM through {@code System.exit} / {@code Runtime.exit}.
 *
 * <p>When a forked test JVM exits this way, Maven only reports "The forked VM terminated without properly saying
 * goodbye", with no hint of where the exit came from. This shutdown hook looks for a thread whose stack contains
 * {@code java.lang.Shutdown.exit}, i.e. the thread that is still inside {@code System.exit} waiting for the
 * shutdown hooks to finish. If it finds one, it dumps the stacks of all threads to stderr and to
 * {@code target/surefire-reports/exit-dump-<pid>-<timestamp>.txt} (the directory can be changed with
 * {@value #DUMP_DIR_PROPERTY}), with every stderr line starting with {@value #OUTPUT_PREFIX}.
 *
 * <p>Normal shutdowns write nothing: when the JVM ends because no non-daemon thread is left, no thread is in
 * {@code Shutdown.exit}; and the exits the test runners make after the tests are done (surefire's ForkedBooter,
 * the Gradle test worker, the IntelliJ runner) are ignored. A SIGTERM also goes through {@code Shutdown.exit},
 * from the signal handler thread, and is dumped, which shows what the JVM was doing when it was killed.
 */
public final class ExitTraceHook {
    static final String DUMP_DIR_PROPERTY = "starrocks.ut.exit.dump.dir";
    static final String DEFAULT_DUMP_DIR = "target/surefire-reports";
    static final String OUTPUT_PREFIX = "[ExitTraceHook] ";

    // exits made by these callers are the test runner finishing normally
    private static final String[] IGNORED_EXIT_CALLER_PREFIXES = {
            "org.apache.maven.surefire.",
            "worker.org.gradle.",
            "org.gradle.",
            "com.intellij.rt.",
    };

    private static final AtomicBoolean INSTALLED = new AtomicBoolean(false);
    // unique ids of the tests and containers that have started but not finished, kept by HangingTestWatchdog
    private static final Set<String> RUNNING_TESTS = ConcurrentHashMap.newKeySet();

    private ExitTraceHook() {
    }

    /**
     * Installs the shutdown hook once per JVM; later calls do nothing.
     */
    public static void install() {
        if (!INSTALLED.compareAndSet(false, true)) {
            return;
        }
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(ExitTraceHook::onShutdown, "exit-trace-hook"));
        } catch (IllegalStateException e) {
            // the JVM is already shutting down
        }
    }

    static void testStarted(String uniqueId) {
        RUNNING_TESTS.add(uniqueId);
    }

    static void testFinished(String uniqueId) {
        RUNNING_TESTS.remove(uniqueId);
    }

    private static void onShutdown() {
        try {
            long pid = ProcessHandle.current().pid();
            long now = System.currentTimeMillis();
            String dump = buildDump(Thread.getAllStackTraces(), pid, now, new ArrayList<>(RUNNING_TESTS));
            if (dump == null) {
                return;
            }
            StringBuilder out = new StringBuilder();
            dump.lines().forEach(line -> out.append(OUTPUT_PREFIX).append(line).append('\n'));
            System.err.print(out);
            System.err.flush();

            File dir = new File(System.getProperty(DUMP_DIR_PROPERTY, DEFAULT_DUMP_DIR));
            File file = new File(dir, "exit-dump-" + pid + "-" + now + ".txt");
            try {
                Files.createDirectories(dir.toPath());
                Files.writeString(file.toPath(), dump, StandardCharsets.UTF_8);
                System.err.println(OUTPUT_PREFIX + "thread dump written to " + file.getAbsolutePath());
            } catch (IOException | RuntimeException e) {
                System.err.println(OUTPUT_PREFIX + "failed to write " + file.getAbsolutePath() + ": " + e);
            }
            System.err.flush();
        } catch (Throwable t) {
            // never let the hook break the shutdown
        }
    }

    /**
     * Returns the frame that called {@code System.exit} / {@code Runtime.exit} in the given stack, or null if the
     * stack is not inside {@code java.lang.Shutdown.exit}. For an exit from a signal handler this is the JDK frame
     * that handles the signal.
     */
    static StackTraceElement findExitCaller(StackTraceElement[] stack) {
        if (stack == null) {
            return null;
        }
        for (int i = 0; i < stack.length; i++) {
            if ("java.lang.Shutdown".equals(stack[i].getClassName()) && "exit".equals(stack[i].getMethodName())) {
                int j = i + 1;
                while (j < stack.length && isExitFrame(stack[j])) {
                    j++;
                }
                return j < stack.length ? stack[j] : stack[i];
            }
        }
        return null;
    }

    private static boolean isExitFrame(StackTraceElement frame) {
        String cls = frame.getClassName();
        return "java.lang.Shutdown".equals(cls) || "java.lang.Runtime".equals(cls) || "java.lang.System".equals(cls);
    }

    static boolean isIgnoredExitCaller(StackTraceElement caller) {
        for (String prefix : IGNORED_EXIT_CALLER_PREFIXES) {
            if (caller.getClassName().startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds the dump text, or returns null if no thread is exiting the JVM through {@code Shutdown.exit}, or the
     * exit was made by the test runner itself.
     */
    static String buildDump(Map<Thread, StackTraceElement[]> stacks, long pid, long timestampMillis,
                            Collection<String> runningTests) {
        List<Map.Entry<Thread, StackTraceElement[]>> exiting = new ArrayList<>();
        boolean unexpected = false;
        for (Map.Entry<Thread, StackTraceElement[]> entry : stacks.entrySet()) {
            StackTraceElement caller = findExitCaller(entry.getValue());
            if (caller != null) {
                exiting.add(entry);
                if (!isIgnoredExitCaller(caller)) {
                    unexpected = true;
                }
            }
        }
        if (!unexpected) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("JVM exit through System.exit/Runtime.exit, pid=").append(pid).append(", time=")
                .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(timestampMillis)))
                .append('\n');
        sb.append("running tests:");
        if (runningTests.isEmpty()) {
            sb.append(" (unknown)\n");
        } else {
            sb.append('\n');
            runningTests.stream().sorted().forEach(id -> sb.append('\t').append(id).append('\n'));
        }
        for (Map.Entry<Thread, StackTraceElement[]> entry : exiting) {
            sb.append("exit called by thread \"").append(entry.getKey().getName()).append("\" at ")
                    .append(findExitCaller(entry.getValue())).append('\n');
        }
        sb.append("thread dump:\n");
        stacks.entrySet().stream()
                .sorted(Comparator.comparing((Map.Entry<Thread, StackTraceElement[]> e) -> e.getKey().getName())
                        .thenComparingLong(e -> e.getKey().getId()))
                .forEach(e -> appendThread(sb, e.getKey(), e.getValue()));
        return sb.toString();
    }

    private static void appendThread(StringBuilder sb, Thread thread, StackTraceElement[] stack) {
        sb.append('"').append(thread.getName()).append("\" id=").append(thread.getId())
                .append(thread.isDaemon() ? " daemon" : "").append(' ').append(thread.getState()).append('\n');
        if (stack != null) {
            for (StackTraceElement frame : stack) {
                sb.append("\tat ").append(frame).append('\n');
            }
        }
        sb.append('\n');
    }
}
