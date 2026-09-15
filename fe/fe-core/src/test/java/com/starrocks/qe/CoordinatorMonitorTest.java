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


package com.starrocks.qe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.thrift.TUniqueId;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CoordinatorMonitorTest {

    @Test
    public void testDeadBackendAndComputeNodeChecker(@Mocked DefaultCoordinator coord1,
                                                     @Mocked DefaultCoordinator coord2,
                                                     @Mocked DefaultCoordinator coord3) throws InterruptedException {
        int prevHeartbeatTimeout = Config.heartbeat_timeout_second;
        Config.heartbeat_timeout_second = 1;

        try {
            List<DefaultCoordinator> coordinators = ImmutableList.of(coord1, coord2, coord3);

            final QeProcessor qeProcessor = QeProcessorImpl.INSTANCE;
            Pair<PPlanFragmentCancelReason, String> coord1Cancel = new Pair<>(null, null);

            CountDownLatch cancelInvocationLatch = new CountDownLatch(2);
            new Expectations(qeProcessor, coord1, coord2, coord3) {
                {
                    qeProcessor.getCoordinators();
                    result = coordinators;
                }

                {
                    coord1.getQueryId();
                    result = new TUniqueId(0xaabbccddL, 0xaabbccddL);
                    minTimes = 0;
                }

                {
                    coord2.getQueryId();
                    result = new TUniqueId(0xddccbbaaL, 0xddccbbaaL);
                    minTimes = 0;
                }

                {
                    coord3.getQueryId();
                    result = new TUniqueId(0xccbbddaaL, 0xccddbbaaL);
                    minTimes = 0;
                }

                {
                    coord1.isUsingBackend(anyLong);
                    result = new mockit.Delegate<Boolean>() {
                        boolean isUsingBackend(Long backendID) {
                            return 0L == backendID;
                        }
                    };
                }

                {
                    coord2.isUsingBackend(anyLong);
                    result = new mockit.Delegate<Boolean>() {
                        boolean isUsingBackend(Long backendID) {
                            return 2L == backendID;
                        }
                    };
                }

                {
                    coord3.isUsingBackend(anyLong);
                    result = new mockit.Delegate<Boolean>() {
                        boolean isUsingBackend(Long backendID) {
                            return 3L == backendID;
                        }
                    };
                }

                {
                    coord1.cancel((PPlanFragmentCancelReason) any, anyString);
                    result = new mockit.Delegate<Boolean>() {
                        void cancel(PPlanFragmentCancelReason cancelReason, String cancelledMessage) {
                            cancelInvocationLatch.countDown();
                            coord1Cancel.first = cancelReason;
                            coord1Cancel.second = cancelledMessage;
                        }
                    };
                    times = 1;
                }

                {
                    coord2.cancel((PPlanFragmentCancelReason) any, anyString);
                    times = 0;
                }

                {
                    coord3.cancel((PPlanFragmentCancelReason) any, anyString);
                    result = new mockit.Delegate<Boolean>() {
                        void cancel(PPlanFragmentCancelReason cancelReason, String cancelledMessage) {
                            cancelInvocationLatch.countDown();
                        }
                    };
                    times = 1;
                }
            };

            CoordinatorMonitor.getInstance().start();

            // Set node#0,1,3 to dead, and stay node#2 alive.
            // coord1 and coord3 will be cancelled, and coord2 will be still alive.
            CoordinatorMonitor.getInstance().addDeadBackend(0L);
            CoordinatorMonitor.getInstance().addDeadBackend(1L);
            CoordinatorMonitor.getInstance().addDeadBackend(3L);

            // Wait until invoking coord1.cancel and coord3.cancel once or timeout.
            Assertions.assertTrue(cancelInvocationLatch.await(5, TimeUnit.SECONDS));

            Assertions.assertEquals(PPlanFragmentCancelReason.INTERNAL_ERROR, coord1Cancel.first);
            Assertions.assertEquals(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR, coord1Cancel.second);
        } finally {
            Config.heartbeat_timeout_second = prevHeartbeatTimeout;
        }
    }

    // The checker is the only thing that cancels queries on dead nodes and it is never restarted, so it has to
    // survive whatever a single coordinator does: a QueryInfo registered without a coordinator (MV maintenance
    // jobs do that), a coordinator whose check throws, and a coordinator whose cancel() blocks on the coordinator
    // lock because it is stuck deploying to the very node that just died. None of these may delay or prevent
    // the cancellation of the other queries, and the checker must still process the next dead node afterwards.
    @Test
    public void testCheckerSurvivesBrokenCoordinatorsAndBlockedCancel(@Mocked DefaultCoordinator throwing,
                                                                     @Mocked DefaultCoordinator blocking,
                                                                     @Mocked DefaultCoordinator healthy)
            throws InterruptedException {
        final QeProcessor qeProcessor = QeProcessorImpl.INSTANCE;
        List<Coordinator> coordinators = Lists.newArrayList(null, throwing, blocking, healthy);

        CountDownLatch blockingEntered = new CountDownLatch(1);
        CountDownLatch releaseBlocking = new CountDownLatch(1);
        AtomicInteger healthyCancels = new AtomicInteger();
        CountDownLatch healthyCancelledOnce = new CountDownLatch(1);

        new Expectations(qeProcessor, throwing, blocking, healthy) {
            {
                qeProcessor.getCoordinators();
                result = coordinators;
                minTimes = 0;

                throwing.getQueryId();
                result = new TUniqueId(1L, 1L);
                minTimes = 0;
                blocking.getQueryId();
                result = new TUniqueId(2L, 2L);
                minTimes = 0;
                healthy.getQueryId();
                result = new TUniqueId(3L, 3L);
                minTimes = 0;

                throwing.isUsingBackend(anyLong);
                result = new RuntimeException("isUsingBackend blew up");
                minTimes = 0;
                blocking.isUsingBackend(anyLong);
                result = true;
                minTimes = 0;
                healthy.isUsingBackend(anyLong);
                result = true;
                minTimes = 0;

                // Holds "the coordinator lock" until the test releases it, like deliverExecFragments() waiting
                // on an exec_plan_fragment RPC to a node that is gone.
                blocking.cancel((PPlanFragmentCancelReason) any, anyString);
                result = new mockit.Delegate<Void>() {
                    void cancel(PPlanFragmentCancelReason cancelReason, String cancelledMessage)
                            throws InterruptedException {
                        blockingEntered.countDown();
                        releaseBlocking.await(30, TimeUnit.SECONDS);
                    }
                };
                minTimes = 0;

                healthy.cancel((PPlanFragmentCancelReason) any, anyString);
                result = new mockit.Delegate<Void>() {
                    void cancel(PPlanFragmentCancelReason cancelReason, String cancelledMessage) {
                        healthyCancels.incrementAndGet();
                        healthyCancelledOnce.countDown();
                    }
                };
                minTimes = 0;
            }
        };

        CoordinatorMonitor.getInstance().start();
        CoordinatorMonitor.getInstance().addDeadBackend(42L);

        Assertions.assertTrue(blockingEntered.await(5, TimeUnit.SECONDS), "blocking coordinator never cancelled");
        // Still blocked in cancel(); the healthy coordinator must be cancelled regardless, and soon.
        Assertions.assertTrue(healthyCancelledOnce.await(5, TimeUnit.SECONDS),
                "a blocked cancel() delayed cancelling the other queries");

        // The checker thread must have survived the null and the throwing coordinator: another dead node gets
        // processed too.
        CoordinatorMonitor.getInstance().addDeadBackend(43L);
        long deadline = System.currentTimeMillis() + 5_000;
        while (healthyCancels.get() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        Assertions.assertTrue(healthyCancels.get() >= 2, "checker stopped processing dead nodes");

        releaseBlocking.countDown();
    }
}
