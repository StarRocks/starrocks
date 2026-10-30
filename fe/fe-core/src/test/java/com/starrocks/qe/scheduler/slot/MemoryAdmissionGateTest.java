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

package com.starrocks.qe.scheduler.slot;

import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class MemoryAdmissionGateTest {
    private static final long GB = 1024L * 1024 * 1024;

    private double prevMemUsedPctLimit;

    @BeforeEach
    public void before() {
        prevMemUsedPctLimit = GlobalVariable.getQueryQueueMemUsedPctLimit();
    }

    @AfterEach
    public void after() {
        GlobalVariable.setQueryQueueMemUsedPctLimit(prevMemUsedPctLimit);
    }

    /**
     * An alive node that has just reported its memory usage, so its report is fresh.
     */
    private static ComputeNode reportedNode(long id, long memUsedBytes, long memLimitBytes) {
        ComputeNode node = new ComputeNode(id, "127.0.0.1", 9050 + (int) id);
        node.setAlive(true);
        node.setMemLimitBytes(memLimitBytes);
        node.updateResourceUsage(0, memUsedBytes, 0);
        assertThat(node.isResourceUsageFresh()).isTrue();
        return node;
    }

    @Test
    public void testDisabledWithoutLimit() {
        GlobalVariable.setQueryQueueMemUsedPctLimit(0);
        SlotManager slotManager = new SlotManager(new ResourceUsageMonitor());
        MemoryAdmissionGate.Status status =
                MemoryAdmissionGate.evaluate(WarehouseManager.DEFAULT_WAREHOUSE_ID, slotManager);
        assertThat(status.getState()).isEqualTo(MemoryAdmissionGate.State.DISABLED);
        assertThat(status.isEnabled()).isFalse();
        assertThat(status.isBlocked()).isFalse();
        assertThat(status.getLimit()).isZero();
        assertThat(status.toString()).contains("DISABLED");

        // a negative value is not effective either, as under Query Queue V1
        GlobalVariable.setQueryQueueMemUsedPctLimit(-1);
        assertThat(MemoryAdmissionGate.evaluate(WarehouseManager.DEFAULT_WAREHOUSE_ID, slotManager).getState())
                .isEqualTo(MemoryAdmissionGate.State.DISABLED);
    }

    @Test
    public void testAnyNodeReachingTheLimitBlocks() {
        // Three nodes with 64 GB of memory each at 62.5%, 81.25% and 78.125%.
        List<ComputeNode> nodes = List.of(
                reportedNode(1, 40 * GB, 64 * GB), reportedNode(2, 52 * GB, 64 * GB), reportedNode(3, 50 * GB, 64 * GB));

        MemoryAdmissionGate.Status status = MemoryAdmissionGate.evaluate(0.8, nodes);
        assertThat(status.getState()).isEqualTo(MemoryAdmissionGate.State.BLOCK);
        assertThat(status.isEnabled()).isTrue();
        assertThat(status.getLimit()).isEqualTo(0.8);
        assertThat(status.getMaxMemUsedPct()).isEqualTo(0.8125);
        assertThat(status.getHottestNodeId()).isEqualTo(2);
        assertThat(status.getHottestMemUsedBytes()).isEqualTo(52 * GB);
        assertThat(status.getHottestMemLimitBytes()).isEqualTo(64 * GB);
        assertThat(status.getNumNodes()).isEqualTo(3);
        assertThat(status.getNumStaleNodes()).isZero();
        assertThat(status.toString()).contains("state=BLOCK", "hottestNodeId=2");

        // the same nodes stay under a higher limit
        assertThat(MemoryAdmissionGate.evaluate(0.85, nodes).getState()).isEqualTo(MemoryAdmissionGate.State.ALLOW);

        // once the hottest node drops under the limit, the gate lets queries through again
        nodes.get(1).updateResourceUsage(0, 48 * GB, 0);
        status = MemoryAdmissionGate.evaluate(0.8, nodes);
        assertThat(status.getState()).isEqualTo(MemoryAdmissionGate.State.ALLOW);
        assertThat(status.getHottestNodeId()).isEqualTo(3);
    }

    @Test
    public void testReachingTheLimitExactlyBlocks() {
        // The same comparison as ComputeNode#isResourceOverloaded under Query Queue V1.
        MemoryAdmissionGate.Status status =
                MemoryAdmissionGate.evaluate(0.75, List.of(reportedNode(1, 48 * GB, 64 * GB)));
        assertThat(status.getMaxMemUsedPct()).isEqualTo(0.75);
        assertThat(status.getState()).isEqualTo(MemoryAdmissionGate.State.BLOCK);
    }

    @Test
    public void testDeadNodeDoesNotTakePart() {
        ComputeNode dead = reportedNode(2, 60 * GB, 64 * GB);
        dead.setAlive(false);
        MemoryAdmissionGate.Status status =
                MemoryAdmissionGate.evaluate(0.8, List.of(reportedNode(1, 32 * GB, 64 * GB), dead));
        assertThat(status.getNumNodes()).isEqualTo(1);
        assertThat(status.getHottestNodeId()).isEqualTo(1);
        assertThat(status.getState()).isEqualTo(MemoryAdmissionGate.State.ALLOW);
    }

    @Test
    public void testStaleNodeDoesNotTakePart() {
        ComputeNode stale = new ComputeNode(2, "127.0.0.1", 9052);
        stale.setAlive(true);
        stale.setMemLimitBytes(64 * GB);
        // Alive but no resource usage report yet, so the report is not fresh, as after a restart of the leader.
        assertThat(stale.isResourceUsageFresh()).isFalse();

        MemoryAdmissionGate.Status status =
                MemoryAdmissionGate.evaluate(0.8, List.of(reportedNode(1, 32 * GB, 64 * GB), stale));
        assertThat(status.getNumNodes()).isEqualTo(2);
        assertThat(status.getNumStaleNodes()).isEqualTo(1);
        assertThat(status.getHottestNodeId()).isEqualTo(1);
        assertThat(status.getState()).isEqualTo(MemoryAdmissionGate.State.ALLOW);
    }

    @Test
    public void testNoFreshNodeAdmits() {
        // Right after the leader starts no node has reported, and on a follower no node ever does.
        ComputeNode node = new ComputeNode(1, "127.0.0.1", 9051);
        node.setAlive(true);
        MemoryAdmissionGate.Status status = MemoryAdmissionGate.evaluate(0.8, List.of(node));
        assertThat(status.getHottestNodeId()).isEqualTo(-1);
        assertThat(status.getMaxMemUsedPct()).isZero();
        assertThat(status.getNumStaleNodes()).isEqualTo(1);
        assertThat(status.getState()).isEqualTo(MemoryAdmissionGate.State.ALLOW);

        // no node at all
        assertThat(MemoryAdmissionGate.evaluate(0.8, List.of()).getState()).isEqualTo(MemoryAdmissionGate.State.ALLOW);
    }

    @Test
    public void testNodeWithoutMemoryLimitCountsAsZero() {
        // A node whose heartbeat has not brought its memory limit yet reports a usage of 0, as getMemUsedPct() does.
        ComputeNode noLimit = reportedNode(1, 60 * GB, 0);
        MemoryAdmissionGate.Status status = MemoryAdmissionGate.evaluate(0.5, List.of(noLimit));
        assertThat(status.getMaxMemUsedPct()).isZero();
        assertThat(status.getState()).isEqualTo(MemoryAdmissionGate.State.ALLOW);
    }

    @Test
    public void testSharedDataUsesTheWarehouseNodes() {
        ComputeNode inWarehouse = reportedNode(11, 60 * GB, 64 * GB);
        new MockUp<RunMode>() {
            @Mock
            public static boolean isSharedNothingMode() {
                return false;
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public List<Long> getAllComputeNodeIds(long warehouseId) {
                if (warehouseId == 300L) {
                    throw new IllegalStateException("warehouse 300 does not exist");
                }
                if (warehouseId == 400L) {
                    throw ErrorReportException.report(ErrorCode.ERR_WAREHOUSE_SUSPENDED, "name: wh400");
                }
                return warehouseId == 100L ? List.of(11L, 12L) : List.of();
            }
        };
        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                // Node 12 is listed by the warehouse but no longer known to the cluster.
                return nodeId == 11L ? inWarehouse : null;
            }
        };

        assertThat(MemoryAdmissionGate.getWarehouseNodes(100L)).containsExactly(inWarehouse);
        assertThat(MemoryAdmissionGate.getWarehouseNodes(200L)).isEmpty();
        // A warehouse that cannot be resolved has no node, instead of failing admission.
        assertThat(MemoryAdmissionGate.getWarehouseNodes(300L)).isEmpty();
        // A suspended warehouse has no node either.
        assertThat(MemoryAdmissionGate.getWarehouseNodes(400L)).isEmpty();
        assertThat(MemoryAdmissionGate.getMaxNodeMemUsedPct(400L)).isZero();

        // the usage is reported whether or not a limit is set
        assertThat(MemoryAdmissionGate.getMaxNodeMemUsedPct(100L)).isEqualTo(0.9375);
        assertThat(MemoryAdmissionGate.getMaxNodeMemUsedPct(200L)).isZero();

        GlobalVariable.setQueryQueueMemUsedPctLimit(0.9);
        SlotManager slotManager = new SlotManager(new ResourceUsageMonitor());
        assertThat(MemoryAdmissionGate.evaluate(100L, slotManager).getState())
                .isEqualTo(MemoryAdmissionGate.State.BLOCK);
        assertThat(MemoryAdmissionGate.evaluate(300L, slotManager).getState())
                .isEqualTo(MemoryAdmissionGate.State.ALLOW);
    }
}
