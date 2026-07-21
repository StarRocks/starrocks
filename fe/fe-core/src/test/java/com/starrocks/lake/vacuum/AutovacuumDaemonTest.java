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

package com.starrocks.lake.vacuum;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AutovacuumDaemonTest {

    // On leader demotion onStopped() must drain the vacuum pool to termination (the re-activation
    // cleanliness gate relies on isRunning quiescence, consistent with the other LeaderDaemons),
    // dereference it so getExecutorService() rebuilds a fresh pool on re-election, and clear the
    // leader-session vacuuming reservations. Same-package field access, no reflection.
    @Test
    public void testOnStoppedDrainsPoolAndClearsLeaderSessionState() throws Exception {
        AutovacuumDaemon daemon = new AutovacuumDaemon();
        ThreadPoolExecutor pool =
                new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        daemon.executorService = pool;
        daemon.vacuumingPartitions.add(1L);
        daemon.vacuumingPartitions.add(2L);

        CountDownLatch started = new CountDownLatch(1);
        pool.execute(() -> {
            started.countDown();
            try {
                // Interruptible work: onStopped()'s shutdownNow() must unblock it so the drain terminates.
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException ignored) {
                // respond to shutdownNow()
            }
        });
        Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));

        daemon.onStopped();

        Assertions.assertTrue(pool.isTerminated(), "pool must be drained to termination");
        Assertions.assertNull(daemon.executorService, "executor must be dereferenced for lazy rebuild");
        Assertions.assertTrue(daemon.vacuumingPartitions.isEmpty(), "reservations must be cleared");
    }
}
