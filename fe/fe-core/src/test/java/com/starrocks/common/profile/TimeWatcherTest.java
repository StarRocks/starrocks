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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimeWatcherTest {
    @Test
    @Timeout(30)
    void testSnapshotsDuringLateWorkerMerges() throws Exception {
        TimeWatcher owner = new TimeWatcher();
        int batches = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<?> worker = executor.submit(() -> {
                start.await();
                for (int i = 0; i < batches; i++) {
                    TimeWatcher detached = owner.fork(false);
                    try (Timer parent = detached.scope(i * 2_000_000L, "batch-" + i);
                            Timer child = detached.scope(i * 2_000_000L + 1_000_000L, "read")) {
                        // Each completed worker contributes a new row and a new scope for the same timer.
                    }
                    owner.mergeFrom(detached);
                }
                return null;
            });
            Future<?> snapshots = executor.submit(() -> {
                start.await();
                for (int i = 0; i < batches; i++) {
                    List<Timer> timers = owner.getAllTimerWithOrder();
                    for (int j = 1; j < timers.size(); j++) {
                        assertTrue(timers.get(j - 1).getFirstTimePoint() <= timers.get(j).getFirstTimePoint());
                    }
                    owner.getTimer("read").ifPresent(timer -> assertEquals("read", timer.name()));
                }
                return null;
            });
            start.countDown();
            worker.get(20, TimeUnit.SECONDS);
            snapshots.get(20, TimeUnit.SECONDS);
            assertEquals(batches * 2, owner.getAllTimerWithOrder().size());
            assertTrue(owner.getTimer("read").isPresent());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
