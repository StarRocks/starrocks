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

import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FullVacuumDaemonTest {

    @Test
    public void testOnStoppedAwaitsExecutorTermination() {
        // onStopped() shutdownNow()'s the vacuum executor and then awaits termination so a
        // subsequent start() on the new leader does not race a still-alive full-vacuum worker
        // against a freshly created pool. isTerminated() must be true once onStopped() returns.
        FullVacuumDaemon daemon = new FullVacuumDaemon();
        BlockingThreadPoolExecutorService executor = daemon.executorService;

        daemon.onStopped();

        Assertions.assertTrue(executor.isShutdown(), "executor must be shutdown");
        Assertions.assertTrue(executor.isTerminated(),
                "executor must be terminated after onStopped() awaits drain");
    }

    @Test
    public void testStartRebuildsExecutorAfterOnStopped() {
        // After onStopped() drains the previous pool, start() must rebuild it so a re-elected
        // leader can submit full-vacuum tasks without RejectedExecutionException.
        FullVacuumDaemon daemon = new FullVacuumDaemon();
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
}
