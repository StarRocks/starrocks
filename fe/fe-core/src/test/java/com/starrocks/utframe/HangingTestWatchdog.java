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

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Prints the stacks of all threads when a test JVM makes no progress for a while.
 *
 * <p>The JUnit timeout in junit-platform.properties only interrupts the test thread and then waits for it to
 * return (JUnit 5.8 has no separate_thread mode, so that setting is ignored). A test blocked where it does not
 * respond to interrupts runs until CI kills the job, and nothing shows where it is stuck.
 *
 * <p>This listener is registered through META-INF/services. When no test or container starts or finishes for
 * {@value #THRESHOLD_PROPERTY} seconds (default {@value #DEFAULT_THRESHOLD_SECONDS}, longer than the 300 s JUnit
 * timeout), it prints the running tests and all thread stacks to stderr, once per stall, each line starting with
 * {@value #OUTPUT_PREFIX}. A value of 0 or less disables it.
 */
public class HangingTestWatchdog implements TestExecutionListener {
    static final String THRESHOLD_PROPERTY = "starrocks.ut.hang.dump.seconds";
    static final long DEFAULT_THRESHOLD_SECONDS = 360;
    // starts every line of the output, so that a build can pick the lines out of the test output; the Gradle
    // build of fe-core forwards them to its log, since it does not show the test output streams
    static final String OUTPUT_PREFIX = "[HangingTestWatchdog] ";

    private final long thresholdNanos;
    private final long checkIntervalSeconds;
    // unique id -> start time in nanos, of the tests and containers that have started but not finished
    private final Map<String, Long> running = new ConcurrentHashMap<>();
    // guards lastProgressNanos and dumpedForCurrentStall, so that a callback's progress() cannot interleave
    // with check() deciding and claiming a dump; otherwise check() could overwrite the reset of a new stall
    private final Object stallLock = new Object();
    private long lastProgressNanos = System.nanoTime();
    private boolean dumpedForCurrentStall = false;
    private ScheduledExecutorService checker;

    public HangingTestWatchdog() {
        this(Long.getLong(THRESHOLD_PROPERTY, DEFAULT_THRESHOLD_SECONDS));
    }

    HangingTestWatchdog(long thresholdSeconds) {
        this.thresholdNanos = TimeUnit.SECONDS.toNanos(thresholdSeconds);
        this.checkIntervalSeconds = Math.max(1, Math.min(30, thresholdSeconds / 4));
    }

    @Override
    public synchronized void testPlanExecutionStarted(TestPlan testPlan) {
        if (thresholdNanos <= 0 || checker != null) {
            return;
        }
        checker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "hanging-test-watchdog");
            // must not keep the test JVM alive
            thread.setDaemon(true);
            return thread;
        });
        progress();
        checker.scheduleWithFixedDelay(this::check, checkIntervalSeconds, checkIntervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    public synchronized void testPlanExecutionFinished(TestPlan testPlan) {
        if (checker != null) {
            checker.shutdownNow();
            checker = null;
        }
        running.clear();
    }

    @Override
    public void executionStarted(TestIdentifier testIdentifier) {
        running.put(testIdentifier.getUniqueId(), System.nanoTime());
        progress();
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
        running.remove(testIdentifier.getUniqueId());
        progress();
    }

    @Override
    public void executionSkipped(TestIdentifier testIdentifier, String reason) {
        progress();
    }

    private void progress() {
        synchronized (stallLock) {
            lastProgressNanos = System.nanoTime();
            dumpedForCurrentStall = false;
        }
    }

    void check() {
        long now;
        long stalledNanos;
        Map<String, Long> runningSnapshot;
        synchronized (stallLock) {
            now = System.nanoTime();
            stalledNanos = now - lastProgressNanos;
            if (dumpedForCurrentStall || running.isEmpty() || stalledNanos < thresholdNanos) {
                return;
            }
            dumpedForCurrentStall = true;
            runningSnapshot = new HashMap<>(running);
        }
        // collect the stacks and print outside the lock, so that a slow dump never blocks test callbacks
        StringBuilder sb = new StringBuilder();
        sb.append("no test started or finished for ")
                .append(TimeUnit.NANOSECONDS.toSeconds(stalledNanos)).append(" s, running:\n");
        runningSnapshot.forEach((id, startNanos) -> sb.append("\t").append(id).append(" (")
                .append(TimeUnit.NANOSECONDS.toSeconds(now - startNanos)).append(" s)\n"));
        sb.append("thread dump:\n").append(MockedFrontend.dumpAllThreads());
        StringBuilder out = new StringBuilder();
        sb.toString().lines().forEach(line -> out.append(OUTPUT_PREFIX).append(line).append('\n'));
        System.err.print(out);
        System.err.flush();
    }
}
