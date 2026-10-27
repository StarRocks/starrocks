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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.util.LeaderDaemon;
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

    // checkAndClearResetPartition is the consume-once gate for the lake_vacuum_reset_partition_ids recovery
    // hatch: it must return true exactly once per listed id -- matching AND removing it in the same step,
    // trimming whitespace, and leaving the other ids intact -- so a wedged pass is nudged once instead of
    // reset every round.
    @Test
    public void testCheckAndClearResetPartitionConsumesEachIdOnce() {
        String saved = Config.lake_vacuum_reset_partition_ids;
        try {
            // Empty config: fast path, nothing matches.
            Config.lake_vacuum_reset_partition_ids = "";
            Assertions.assertFalse(AutovacuumDaemon.checkAndClearResetPartition(1L));

            // Multiple ids (with surrounding whitespace): matching one consumes only it, leaves the rest.
            Config.lake_vacuum_reset_partition_ids = "10; 20 ;30";
            Assertions.assertTrue(AutovacuumDaemon.checkAndClearResetPartition(20L));
            Assertions.assertEquals("10;30", Config.lake_vacuum_reset_partition_ids);

            // Consume-once: the same id no longer matches on a second round, and the list is unchanged.
            Assertions.assertFalse(AutovacuumDaemon.checkAndClearResetPartition(20L));
            Assertions.assertEquals("10;30", Config.lake_vacuum_reset_partition_ids);

            // A partition not listed never matches and never mutates the list.
            Assertions.assertFalse(AutovacuumDaemon.checkAndClearResetPartition(99L));
            Assertions.assertEquals("10;30", Config.lake_vacuum_reset_partition_ids);

            // Consuming the remaining ids drains the list to empty.
            Assertions.assertTrue(AutovacuumDaemon.checkAndClearResetPartition(10L));
            Assertions.assertTrue(AutovacuumDaemon.checkAndClearResetPartition(30L));
            Assertions.assertEquals("", Config.lake_vacuum_reset_partition_ids);
        } finally {
            Config.lake_vacuum_reset_partition_ids = saved;
        }
    }

    // The reset hatch is leader-only, but ADMIN SET FRONTEND CONFIG broadcasts the value to every live FE and
    // only the leader consumes it, so a follower would hold the id until it is elected and then fire a stale,
    // long-forgotten reset. start() must drop whatever was inherited at the beginning of a leader session --
    // and must NOT wipe an id an admin set against a daemon that is already serving.
    @Test
    public void testStartDiscardsInheritedResetRequests() {
        String saved = Config.lake_vacuum_reset_partition_ids;
        // No GlobalStateMgr in this unit test: skip the ready/lease handshake so start() only exercises the
        // discard and the worker lifecycle.
        AutovacuumDaemon daemon = new AutovacuumDaemon() {
            @Override
            protected void runOneCycle() {
            }
        };
        try {
            Config.lake_vacuum_reset_partition_ids = "10;20";
            daemon.start();
            Assertions.assertEquals("", Config.lake_vacuum_reset_partition_ids,
                    "an inherited request must not survive into this leader session");

            // Already running: a redundant start() is a no-op and must leave this session's request alone.
            Config.lake_vacuum_reset_partition_ids = "30";
            daemon.start();
            Assertions.assertEquals("30", Config.lake_vacuum_reset_partition_ids,
                    "a request set on the serving leader must survive a redundant start()");
        } finally {
            daemon.setStop();
            // Leave nothing in LeaderDaemon.RUNNING_INSTANCES: the re-activation gate reads it process-wide
            // and exits the JVM on a straggler, which would take down unrelated tests in the same run.
            LeaderDaemon.awaitQuiesced(Lists.<LeaderDaemon>newArrayList(daemon), 30000);
            Config.lake_vacuum_reset_partition_ids = saved;
        }
    }
}
