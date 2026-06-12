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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for the parallel iceberg prepare / background scan-range deploy crash.
 *
 * <p>Those paths surface metrics onto a single query-level {@link Tracers} from multiple threads
 * at the same time. The {@code TimeWatcher} scope stack used by {@code watchScope} is
 * single-threaded, so driving it concurrently corrupted shared state
 * ({@code ConcurrentModificationException}) and leaked the shared overhead stopwatch
 * ({@code "This stopwatch is already running"}). The fix switched those sites to {@code count}/
 * {@code record}, which mutate only {@code VarTracer} under {@code TracerImpl}'s monitor. These
 * tests lock in that those two paths are safe and correct under concurrent use of a shared tracer.
 */
public class TracersConcurrentTest {

    @BeforeEach
    public void setUp() {
        Tracers.register();
        Tracers.enableTraceModule(Tracers.Module.EXTERNAL);
        Tracers.enableTraceMode(Tracers.Mode.VARS);
    }

    @AfterEach
    public void tearDown() {
        Tracers.close();
    }

    @Test
    public void testConcurrentCountIsSafeAndAccumulates() throws Exception {
        final Tracers shared = Tracers.get();
        final int threads = 16;
        final int perThread = 10000;

        List<Throwable> errors = new CopyOnWriteArrayList<>();
        List<Future<?>> futures = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        Tracers.count(shared, Tracers.Module.EXTERNAL, "concurrent.count", 1);
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            }));
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
        for (Future<?> f : futures) {
            f.get();
        }

        assertTrue(errors.isEmpty(), "concurrent count threw: " + errors);
        // synchronized add => no lost updates
        assertEquals((long) threads * perThread, findVar("concurrent.count"));
    }

    @Test
    public void testConcurrentRecordIsSafe() throws Exception {
        final Tracers shared = Tracers.get();
        final int threads = 16;
        final int perThread = 2000;

        List<Throwable> errors = new CopyOnWriteArrayList<>();
        List<Future<?>> futures = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        Tracers.record(shared, Tracers.Module.EXTERNAL, "rec." + tid + "." + i, "v");
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            }));
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
        for (Future<?> f : futures) {
            f.get();
        }

        assertTrue(errors.isEmpty(), "concurrent record threw: " + errors);
        // every distinct name must be present, none lost or corrupted
        assertEquals(threads * perThread, Tracers.getAllVars().size());
    }

    private long findVar(String name) {
        // getAllVars() reads the tracer registered on this (test) thread, i.e. the shared instance.
        for (Var<?> var : Tracers.getAllVars()) {
            if (name.equals(var.getName())) {
                return ((Number) var.getValue()).longValue();
            }
        }
        throw new IllegalStateException("var not found: " + name);
    }
}
