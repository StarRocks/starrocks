// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.Config;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.thrift.TUniqueId;
import mockit.Expectations;
import org.junit.Test;

import java.util.List;

public class CoordinatorSchedulerTest {

    @Test
    public void testDeadBackendAndComputeNodeChecker() throws InterruptedException {
        int prevHeartbeatTimeout = Config.heartbeat_timeout_second;
        Config.heartbeat_timeout_second = 1;

        try {
            // Prepare coordinators.
            ConnectContext connCtx = new ConnectContext();
            connCtx.setExecutionId(new TUniqueId(0, 0));
            Coordinator coord1 = new Coordinator(connCtx, null, null, null);
            Coordinator coord2 = new Coordinator(connCtx, null, null, null);
            Coordinator coord3 = new Coordinator(connCtx, null, null, null);
            List<Coordinator> coordinators = ImmutableList.of(coord1, coord2, coord3);

            final QeProcessor qeProcessor = QeProcessorImpl.INSTANCE;

            new Expectations(qeProcessor, coord1, coord2, coord3) {
                {
                    qeProcessor.getCoordinators();
                    result = coordinators;
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
                    coord1.cancel((PPlanFragmentCancelReason) any);
                    times = 1;
                }

                {
                    coord2.cancel((PPlanFragmentCancelReason) any);
                    times = 1;
                }

                {
                    coord3.cancel((PPlanFragmentCancelReason) any);
                    times = 0;
                }
            };

            CoordinatorScheduler.getInstance().start();

            // Set node#0,1,2 to dead, and stay node#3 alive.
            // coord1 and coord2 will be cancelled, and coord3 will be still alive.
            CoordinatorScheduler.getInstance().addDeadBackend(0L);
            CoordinatorScheduler.getInstance().addDeadBackend(1L);
            CoordinatorScheduler.getInstance().addDeadBackend(2L);

            Thread.sleep(3 * 1000L);
        } finally {
            Config.heartbeat_timeout_second = prevHeartbeatTimeout;
        }
    }
}
