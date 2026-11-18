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

package com.starrocks.journal.bdbje;

import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.utilint.RepUtils;
import com.starrocks.common.Config;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BDBClockSkewTest {
    @TempDir
    Path directory;

    @Test
    @Timeout(60)
    void testFirstHandshakeWithReplicaClockAhead() throws Exception {
        checkFirstHandshake(45_000);
    }

    @Test
    @Timeout(60)
    void testFirstHandshakeWithReplicaClockBehind() throws Exception {
        checkFirstHandshake(-45_000);
    }

    @Test
    @Timeout(60)
    void testFirstHandshakeWithSynchronizedClocks() throws Exception {
        checkFirstHandshake(0);
    }

    private ReplicationConfig replicationConfig(String name, String address, String helper) {
        ReplicationConfig config = new ReplicationConfig("clockSkewTest", name, address);
        config.setHelperHosts(helper);
        config.setMaxClockDelta(Config.max_bdbje_clock_delta_ms, TimeUnit.MILLISECONDS);
        config.setConsistencyPolicy(new NoConsistencyRequiredPolicy());
        config.setConfigParam(ReplicationConfig.ENV_SETUP_TIMEOUT, "30 s");
        return config;
    }

    private void checkFirstHandshake(int skewMs) throws Exception {
        long maxDeltaMs = Config.max_bdbje_clock_delta_ms;
        if (skewMs != 0) {
            assertTrue(Math.abs((long) skewMs) > maxDeltaMs,
                    "Injected skew must exceed max_bdbje_clock_delta_ms");
        }
        String leaderAddress = "127.0.0.1:" + UtFrameUtils.findValidPort();
        String replicaAddress = "127.0.0.1:" + UtFrameUtils.findValidPort();
        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setAllowCreate(true);
        environmentConfig.setTransactional(true);
        environmentConfig.setConfigParam(EnvironmentConfig.FREE_DISK, "0");
        environmentConfig.setConfigParam(EnvironmentConfig.RESERVED_DISK, "0");

        ReplicatedEnvironment leader = new ReplicatedEnvironment(
                Files.createDirectory(directory.resolve("leader")).toFile(),
                replicationConfig("leader", leaderAddress, leaderAddress), environmentConfig);
        ReplicatedEnvironment replica = null;
        AtomicReference<RepImpl> replicaImpl = new AtomicReference<>();
        try {
            assertEquals(ReplicatedEnvironment.State.MASTER, leader.getState());
            // Only inject the replica's handshake clock. Connections, protocol exchanges, clock-delta
            // validation, exceptions and environment invalidation all use real BDB code. Do not alter
            // the JVM/host clock or RepImpl's global clock-skew setting.
            RepUtils.Clock replicaClock = new RepUtils.Clock(skewMs);
            new MockUp<RepNode>() {
                @Mock
                public RepUtils.Clock getClock(Invocation invocation) {
                    RepNode node = invocation.getInvokedInstance();
                    if ("replica".equals(node.getNameIdPair().getName())) {
                        replicaImpl.set(node.getRepImpl());
                        return replicaClock;
                    }
                    return invocation.proceed();
                }
            };
            Path replicaHome = Files.createDirectory(directory.resolve("replica"));
            ReplicationConfig replicaConfig = replicationConfig("replica", replicaAddress, leaderAddress);
            if (skewMs == 0) {
                replica = new ReplicatedEnvironment(replicaHome.toFile(), replicaConfig, environmentConfig);
                assertEquals(ReplicatedEnvironment.State.REPLICA, replica.getState());
                assertTrue(replica.isValid());
                assertNotNull(replicaImpl.get());
            } else {
                EnvironmentFailureException failure = assertThrows(EnvironmentFailureException.class,
                        () -> new ReplicatedEnvironment(replicaHome.toFile(), replicaConfig, environmentConfig));
                assertEquals(EnvironmentFailureReason.HANDSHAKE_ERROR, failure.getReason());
                assertTrue(failure.getMessage().contains("Clock delta"), failure.getMessage());
                RepImpl failedEnvironment = replicaImpl.get();
                assertNotNull(failedEnvironment);
                // The failing constructor may already close its handle. Check the actual underlying
                // invalidation record, not just whether that handle has been closed.
                assertTrue(failedEnvironment.wasInvalidated());
                assertFalse(failedEnvironment.isValid());
                assertEquals(EnvironmentFailureReason.HANDSHAKE_ERROR,
                        failedEnvironment.getInvalidatingException().getReason());
            }
        } finally {
            try {
                if (replica != null) {
                    replica.close();
                }
            } finally {
                leader.close();
            }
        }
    }
}
