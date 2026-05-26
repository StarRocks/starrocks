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

import com.starrocks.common.Config;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class AutovacuumDaemonTest {

    @Test
    public void testOnStoppedAwaitsExecutorTermination() {
        // onStopped() shutdownNow()'s the vacuum executor and then awaits termination so a
        // subsequent start() on the new leader does not race a still-alive vacuum worker
        // against a freshly created pool. isTerminated() must be true once onStopped()
        // returns.
        AutovacuumDaemon daemon = new AutovacuumDaemon();
        BlockingThreadPoolExecutorService executor = daemon.executorService;

        daemon.onStopped();

        Assertions.assertTrue(executor.isShutdown(), "executor must be shutdown");
        Assertions.assertTrue(executor.isTerminated(),
                "executor must be terminated after onStopped() awaits drain");
    }

    @Test
    public void testStartRebuildsExecutorAfterOnStopped() {
        // After onStopped() drains the previous pool, start() must rebuild it so a re-elected
        // leader can submit vacuum tasks without RejectedExecutionException.
        AutovacuumDaemon daemon = new AutovacuumDaemon();
        BlockingThreadPoolExecutorService originalExecutor = daemon.executorService;

        daemon.onStopped();
        Assertions.assertTrue(originalExecutor.isTerminated());

        daemon.start();
        try {
            Assertions.assertNotSame(originalExecutor, daemon.executorService,
                    "executor must be rebuilt on re-election");
            Assertions.assertFalse(daemon.executorService.isShutdown(),
                    "rebuilt executor must accept new submissions");
        } finally {
            daemon.setStop();
        }
    }

    @Test
    public void testOnStoppedLogsWhenExecutorRefusesToTerminate() {
        // When a vacuum worker ignores shutdownNow's interrupt long enough to outlast the
        // drain budget, onStopped must log a warning and proceed rather than block forever.
        // Covers the LOG.warn / catch branches in onStopped's awaitTermination.
        AutovacuumDaemon daemon = new AutovacuumDaemon();
        BlockingThreadPoolExecutorService stuck =
                BlockingThreadPoolExecutorService.newInstance(1, 0, 1, TimeUnit.HOURS, "autovacuum-stuck-test");
        stuck.execute(() -> {
            long deadline = System.currentTimeMillis() + 2000L;
            while (System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {
                    // simulate uninterruptible vacuum work
                }
            }
        });
        daemon.executorService = stuck;
        int oldTimeout = Config.leader_demotion_drain_timeout_sec;
        Config.leader_demotion_drain_timeout_sec = 1;
        try {
            daemon.onStopped();
            // Drain timed out; pool is still shutdown and reference retained for the
            // restart guard.
            Assertions.assertTrue(stuck.isShutdown());
        } finally {
            Config.leader_demotion_drain_timeout_sec = oldTimeout;
            stuck.shutdownNow();
        }
    }

    @Test
    public void testStartRefusesToRestartBeforeExecutorTerminates() {
        // Mirror of BatchWriteMgr / AlterHandler restart guard. If a previous executor is
        // shutdown but has not yet terminated (in-flight task ignoring interrupt), start()
        // must throw IllegalStateException rather than spinning up a fresh pool against the
        // same metadata.
        AutovacuumDaemon daemon = new AutovacuumDaemon();
        BlockingThreadPoolExecutorService blockedExecutor =
                BlockingThreadPoolExecutorService.newInstance(1, 0, 1, TimeUnit.HOURS, "autovacuum-blocked-test");
        blockedExecutor.execute(() -> {
            try {
                Thread.sleep(30_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        blockedExecutor.shutdown();
        daemon.executorService = blockedExecutor;

        try {
            Assertions.assertThrows(IllegalStateException.class, daemon::start);
        } finally {
            blockedExecutor.shutdownNow();
        }
    }
}
