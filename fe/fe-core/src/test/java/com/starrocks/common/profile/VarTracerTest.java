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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class VarTracerTest {

    @Test
    @Timeout(30)
    public void testCountersSharedByForkedTracers() throws Exception {
        int workers = 8;
        int incrementsPerWorker = 1000;
        String counterName = "DeltaLake.listFrom.s3Files";
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        try {
            // A fresh shared map exercises counter creation as well as concurrent increments.
            for (int iteration = 0; iteration < 20; iteration++) {
                Tracers.register();
                Tracers.init(Tracers.Mode.VARS, Tracers.Module.EXTERNAL, false, false);
                Tracers owner = Tracers.get();
                CyclicBarrier start = new CyclicBarrier(workers + 1);
                List<Future<?>> futures = new ArrayList<>();
                for (int worker = 0; worker < workers; worker++) {
                    Tracers fork = owner.fork(true);
                    futures.add(executor.submit(() -> {
                        Tracers.set(fork);
                        try {
                            start.await(10, TimeUnit.SECONDS);
                            for (int increment = 0; increment < incrementsPerWorker; increment++) {
                                Tracers.count(Tracers.Module.EXTERNAL, counterName, 1);
                            }
                        } finally {
                            Tracers.close();
                        }
                        return null;
                    }));
                }
                start.await(10, TimeUnit.SECONDS);
                for (Future<?> future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }

                List<Var<?>> snapshot = Tracers.getAllVars();
                Assertions.assertEquals(1, snapshot.size());
                Assertions.assertNotNull(snapshot.get(0));
                Assertions.assertEquals(counterName, snapshot.get(0).getName());
                Assertions.assertEquals(Long.valueOf((long) workers * incrementsPerWorker),
                        snapshot.get(0).getValue());
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            Tracers.close();
        }
    }

    @Test
    @Timeout(30)
    public void testSnapshotsDuringConcurrentWrites() throws Exception {
        VarTracer tracer = new VarTracer();
        int workers = 4;
        int variablesPerWorker = 500;
        CyclicBarrier start = new CyclicBarrier(workers + 1);
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int worker = 0; worker < workers; worker++) {
                int workerId = worker;
                futures.add(executor.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    for (int variable = 0; variable < variablesPerWorker; variable++) {
                        String name = workerId + "." + variable;
                        tracer.record(variable, "record." + name, name);
                        tracer.count(variable, "counter." + name, 1);
                    }
                    return null;
                }));
            }
            start.await(10, TimeUnit.SECONDS);
            for (int snapshot = 0; snapshot < 200; snapshot++) {
                Assertions.assertTrue(tracer.getAllVars().stream().allMatch(Objects::nonNull));
                Assertions.assertTrue(tracer.getAllVarsWithOrder().stream().allMatch(Objects::nonNull));
            }
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }

            int expectedVariables = workers * variablesPerWorker * 2;
            Assertions.assertEquals(expectedVariables, tracer.getAllVars().size());
            List<Var<?>> ordered = tracer.getAllVarsWithOrder();
            Assertions.assertEquals(expectedVariables, ordered.size());
            for (int index = 1; index < ordered.size(); index++) {
                Assertions.assertTrue(ordered.get(index - 1).getTimePoint() <= ordered.get(index).getTimePoint());
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSameTimePointKeepsRecordOrder() {
        VarTracer tracer = new VarTracer();
        List<String> names = List.of("g.Table", "g.Requests", "g.IOTime", "g.Retries", "g.Slowest",
                "a", "z", "m", "b", "y");
        for (String name : names) {
            tracer.record(7, name, "v");
        }
        // recorded again later, all at one new time point: still in the order first recorded
        for (String name : names) {
            tracer.record(9, name, "v2");
        }
        Assertions.assertEquals(names,
                tracer.getAllVarsWithOrder().stream().map(v -> v.name).collect(Collectors.toList()));
    }
}
