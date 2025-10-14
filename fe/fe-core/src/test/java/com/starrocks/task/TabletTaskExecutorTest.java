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

package com.starrocks.task;

import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TabletTaskExecutorTest {

    @Test
    public void testWaitForFinishedTimeoutWithExistingNode(@Mocked GlobalStateMgr globalStateMgr) {
        long backendId = 10001L;
        long tabletId = 20001L;
        String expectedHost = "192.168.1.100";

        // Mock Backend node
        Backend backend = new Backend(backendId, expectedHost, 9050);
        backend.setBePort(9060);
        backend.setAlive(true);

        SystemInfoService systemInfoService = new SystemInfoService();
        NodeMgr nodeMgr = new NodeMgr();

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                if (nodeId == backendId) {
                    return backend;
                }
                return null;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfoService;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }

            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }

            @Mock
            public boolean isLeaderTransferred() {
                return false;
            }
        };

        // Create a timed-out CountDownLatch (count > 0 indicates unfinished tasks)
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(1);
        countDownLatch.addMark(backendId, tabletId);

        // Call waitForFinished method, expecting DdlException to be thrown
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            Deencapsulation.invoke(TabletTaskExecutor.class, "waitForFinished",
                    countDownLatch, 1L);
        });

        // Verify error message contains node host
        String errorMessage = exception.getMessage();
        Assertions.assertTrue(errorMessage.contains(expectedHost),
                "Error message should contain node host: " + expectedHost + ", actual error message: " + errorMessage);
        Assertions.assertTrue(errorMessage.contains(String.valueOf(tabletId)),
                "Error message should contain tabletId: " + tabletId);
        Assertions.assertTrue(errorMessage.contains("Table creation timed out"),
                "Error message should contain timeout message");
    }

    @Test
    public void testWaitForFinishedTimeoutWithNonExistingNode(@Mocked GlobalStateMgr globalStateMgr) {
        long backendId = 10002L;
        long tabletId = 20002L;

        SystemInfoService systemInfoService = new SystemInfoService();
        NodeMgr nodeMgr = new NodeMgr();

        // Mock SystemInfoService, return null to indicate node does not exist
        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                return null;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfoService;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }

            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }

            @Mock
            public boolean isLeaderTransferred() {
                return false;
            }
        };

        // Create a timed-out CountDownLatch
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(1);
        countDownLatch.addMark(backendId, tabletId);

        // Call waitForFinished method, expecting DdlException to be thrown
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            Deencapsulation.invoke(TabletTaskExecutor.class, "waitForFinished",
                    countDownLatch, 1L);
        });

        // Verify error message contains "N/A"
        String errorMessage = exception.getMessage();
        Assertions.assertTrue(errorMessage.contains("N/A"),
                "When node does not exist, error message should contain 'N/A', actual error message: " + errorMessage);
        Assertions.assertTrue(errorMessage.contains(String.valueOf(tabletId)),
                "Error message should contain tabletId: " + tabletId);
        Assertions.assertTrue(errorMessage.contains("Table creation timed out"),
                "Error message should contain timeout message");
    }

    @Test
    public void testWaitForFinishedTimeoutWithMultipleTablets(@Mocked GlobalStateMgr globalStateMgr) {
        long backendId1 = 10001L;
        long backendId2 = 10002L;
        long tabletId1 = 20001L;
        long tabletId2 = 20002L;
        long tabletId3 = 20003L;
        String host1 = "192.168.1.100";

        Backend backend1 = new Backend(backendId1, host1, 9050);
        backend1.setAlive(true);

        SystemInfoService systemInfoService = new SystemInfoService();
        NodeMgr nodeMgr = new NodeMgr();

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                if (nodeId == backendId1) {
                    return backend1;
                }
                return null; // backendId2 does not exist
            }
        };

        // Mock NodeMgr
        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                return systemInfoService;
            }
        };

        // Mock GlobalStateMgr
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }

            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }

            @Mock
            public boolean isLeaderTransferred() {
                return false;
            }
        };

        // Create CountDownLatch with multiple unfinished tasks
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(3);
        countDownLatch.addMark(backendId1, tabletId1);
        countDownLatch.addMark(backendId2, tabletId2);
        countDownLatch.addMark(backendId2, tabletId3);

        // Call waitForFinished method
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            Deencapsulation.invoke(TabletTaskExecutor.class, "waitForFinished",
                    countDownLatch, 1L);
        });

        // Verify error message
        String errorMessage = exception.getMessage();
        Assertions.assertTrue(errorMessage.contains("(3/3)"),
                "Error message should show the number of unfinished tasks");
        Assertions.assertTrue(errorMessage.contains(host1) && errorMessage.contains("N/A"),
                "Error message should contain node information");
    }

    @Test
    public void testWaitForFinishedSuccess(@Mocked GlobalStateMgr globalStateMgr) throws Exception {
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(1);
        countDownLatch.addMark(10001L, 20001L);

        // Simulate task completion
        countDownLatch.markedCountDown(10001L, 20001L);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }

            @Mock
            public boolean isLeaderTransferred() {
                return false;
            }
        };

        // Call waitForFinished method, should not throw exception
        Assertions.assertDoesNotThrow(() -> {
            Deencapsulation.invoke(TabletTaskExecutor.class, "waitForFinished",
                    countDownLatch, 10L);
        });
    }
}