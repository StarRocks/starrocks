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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VarTracerTest {
    @Test
    void testCounterSnapshotDoesNotChangeAfterPublication() {
        VarTracer vars = new VarTracer();
        vars.count(1, "files", 2);
        List<Var<?>> snapshot = vars.getAllVarsWithOrder();
        vars.count(2, "files", 3);
        assertEquals(2L, snapshot.get(0).getValue());
        assertEquals(5L, vars.getAllVars().get(0).getValue());
        assertEquals(1L, snapshot.get(0).getTimePoint());
    }

    @Test
    @Timeout(30)
    void testForkedWritersDuringProfileSnapshots() throws Exception {
        VarTracer vars = new VarTracer();
        Tracer owner = new TracerImpl(Stopwatch.createStarted(), new TimeWatcher(), vars,
                new FileLogTracer(), new FileLogTracer());
        int updates = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(3);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<?>> writers = new ArrayList<>();
            for (int w = 0; w < 2; w++) {
                int writerId = w;
                Tracer fork = owner.fork(false);
                writers.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < updates; i++) {
                        fork.record("DELTA_LAKE.closed-" + writerId + "-" + i, "true");
                        fork.count("DELTA_LAKE.files", 1);
                    }
                    return null;
                }));
            }
            Future<?> snapshots = executor.submit(() -> {
                start.await();
                for (int i = 0; i < updates; i++) {
                    List<Var<?>> snapshot = vars.getAllVarsWithOrder();
                    for (int j = 1; j < snapshot.size(); j++) {
                        assertTrue(snapshot.get(j - 1).getTimePoint() <= snapshot.get(j).getTimePoint());
                    }
                    owner.getAllVars();
                    owner.toRuntimeProfile(new RuntimeProfile("query"));
                }
                return null;
            });
            start.countDown();
            for (Future<?> writer : writers) {
                writer.get(20, TimeUnit.SECONDS);
            }
            snapshots.get(20, TimeUnit.SECONDS);
            List<Var<?>> result = vars.getAllVars();
            assertEquals(2 * updates + 1, result.size());
            assertEquals(2L * updates, result.stream().filter(v -> v.getName().equals("DELTA_LAKE.files"))
                    .findFirst().orElseThrow().getValue());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
