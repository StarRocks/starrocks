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

package com.starrocks.load.batchwrite;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.epack.warehouse.WarehouseManagerEPack;
import com.starrocks.epack.warehouse.cngroup.CNGroupResource;
import com.starrocks.epack.warehouse.cngroup.CNGroupResourceProvider;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.planner.LoadScanNode;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Mock;
import mockit.MockUp;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_BATCH_WRITE_INTERVAL_MS;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_BATCH_WRITE_PARALLEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A merge commit job moves to another CN group of its warehouse when no compute node of its own CN group can
 * coordinate loads. The CN groups are faked: acquisitions get the groups listed in {@link #ACQUIRED_GROUPS},
 * {@link #AVAILABLE_NODES} says which nodes of a group can coordinate loads, both for the coordinator backend
 * assigner and for the check in {@link BatchWriteMgr}, and {@link #DISABLED_GROUPS} are the groups that DISABLE
 * CNGROUP has disabled.
 */
public class MergeCommitCnGroupSwitchTest extends BatchWriteTestBase {

    private static final long GROUP_A = 101;
    private static final long GROUP_B = 102;
    private static final long GROUP_C = 103;

    // CN group id -> the nodes of the group that can coordinate loads
    private static final Map<Long, List<ComputeNode>> AVAILABLE_NODES = new ConcurrentHashMap<>();
    // The CN groups that acquisitions get, in order; the last one repeats
    private static final List<Long> ACQUIRED_GROUPS = new CopyOnWriteArrayList<>();
    private static final AtomicInteger NUM_ACQUISITIONS = new AtomicInteger();
    private static final Set<Long> DISABLED_GROUPS = ConcurrentHashMap.newKeySet();

    private int oldAssignerScheduleIntervalMs;
    private BatchWriteMgr batchWriteMgr;
    private TableId tableId;
    private StreamLoadKvParams params;
    private BatchWriteId batchWriteId;

    @BeforeEach
    public void setup() {
        AVAILABLE_NODES.clear();
        AVAILABLE_NODES.put(GROUP_A, List.of(allNodes.get(0), allNodes.get(1)));
        AVAILABLE_NODES.put(GROUP_B, List.of(allNodes.get(2), allNodes.get(3)));
        AVAILABLE_NODES.put(GROUP_C, List.of(allNodes.get(4)));
        ACQUIRED_GROUPS.clear();
        NUM_ACQUISITIONS.set(0);
        DISABLED_GROUPS.clear();

        new MockUp<LoadScanNode>() {
            @Mock
            public static List<ComputeNode> getAvailableComputeNodes(ComputeResource computeResource) {
                return AVAILABLE_NODES.getOrDefault(computeResource.getWorkerGroupId(), List.of());
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                int index = NUM_ACQUISITIONS.getAndIncrement();
                if (ACQUIRED_GROUPS.isEmpty()) {
                    return group(GROUP_A);
                }
                return group(ACQUIRED_GROUPS.get(Math.min(index, ACQUIRED_GROUPS.size() - 1)));
            }

            @Mock
            public boolean isComputeResourceDisabled(ComputeResource computeResource) {
                return DISABLED_GROUPS.contains(computeResource.getWorkerGroupId());
            }
        };
        new MockUp<WarehouseManagerEPack>() {
            @Mock
            public boolean isComputeResourceDisabled(ComputeResource computeResource) {
                return DISABLED_GROUPS.contains(computeResource.getWorkerGroupId());
            }
        };
        new MockUp<CNGroupResourceProvider>() {
            @Mock
            public List<ComputeResource> getComputeResources(Warehouse warehouse) {
                return allGroups();
            }
        };
        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public List<ComputeResource> getComputeResources(Warehouse warehouse) {
                return allGroups();
            }
        };

        // Let the assigner's periodical check take unavailable nodes off the jobs within about a second
        oldAssignerScheduleIntervalMs = Config.merge_commit_be_assigner_schedule_interval_ms;
        Config.merge_commit_be_assigner_schedule_interval_ms = 1000;
        batchWriteMgr = new BatchWriteMgr();
        batchWriteMgr.start();

        tableId = new TableId(DB_NAME_1, TABLE_NAME_1_1);
        params = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "1000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
            }});
        batchWriteId = new BatchWriteId(tableId, params);
    }

    @AfterEach
    public void tearDown() {
        // Stopping the manager stops its assigner and thread pool too, so no thread of this test runs into the next
        if (batchWriteMgr != null) {
            batchWriteMgr.setStop();
            Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> !batchWriteMgr.isRunning());
        }
        Config.merge_commit_be_assigner_schedule_interval_ms = oldAssignerScheduleIntervalMs;
    }

    @Test
    public void testSwitchCnGroupOnCoordinatorBackendRequest() {
        ACQUIRED_GROUPS.addAll(List.of(GROUP_A, GROUP_B));
        Set<Long> groupANodes = nodeIds(AVAILABLE_NODES.get(GROUP_A));
        Set<Long> groupBNodes = nodeIds(AVAILABLE_NODES.get(GROUP_B));

        RequestCoordinatorBackendResult result1 =
                batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT);
        assertTrue(result1.isOk());
        assertEquals(groupANodes, nodeIds(result1.getValue()));
        MergeCommitJob oldJob = batchWriteMgr.getJob(tableId, params);
        assertEquals(GROUP_A, oldJob.getComputeResource().getWorkerGroupId());

        long numSwitches = MergeCommitMetricRegistry.getInstance().getJobCnGroupSwitchNum();
        makeGroupUnavailable(GROUP_A, oldJob);
        RequestCoordinatorBackendResult result2 =
                batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT);
        assertTrue(result2.isOk(), result2.getStatus().toString());
        assertEquals(groupBNodes, nodeIds(result2.getValue()));

        MergeCommitJob newJob = batchWriteMgr.getJob(tableId, params);
        assertNotSame(oldJob, newJob);
        assertNotEquals(oldJob.getId(), newJob.getId());
        assertEquals(GROUP_B, newJob.getComputeResource().getWorkerGroupId());
        assertEquals(oldJob.getWarehouseName(), newJob.getWarehouseName());
        assertEquals(oldJob.getBatchWriteParallel(), newJob.getBatchWriteParallel());
        assertEquals(oldJob.getBatchWriteIntervalMs(), newJob.getBatchWriteIntervalMs());
        assertEquals(1, batchWriteMgr.numJobs());
        assertFalse(batchWriteMgr.getCoordinatorBackendAssigner().getBackends(oldJob.getId()).isPresent());
        assertEquals(numSwitches + 1, MergeCommitMetricRegistry.getInstance().getJobCnGroupSwitchNum());

        // Later requests go to the new job without moving it again
        RequestCoordinatorBackendResult result3 =
                batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT);
        assertTrue(result3.isOk());
        assertEquals(groupBNodes, nodeIds(result3.getValue()));
        assertSame(newJob, batchWriteMgr.getJob(tableId, params));
        assertEquals(numSwitches + 1, MergeCommitMetricRegistry.getInstance().getJobCnGroupSwitchNum());
    }

    @Test
    public void testSwitchCnGroupOnLoadRequest() {
        // The tasks need not run here
        new MockUp<MergeCommitTask>() {
            @Mock
            public void run() {
            }
        };
        // An acquisition would move the job to group C, but the node asking for the load is in group B
        ACQUIRED_GROUPS.addAll(List.of(GROUP_A, GROUP_C));
        ComputeNode nodeOfGroupA = AVAILABLE_NODES.get(GROUP_A).get(0);
        ComputeNode nodeOfGroupB = AVAILABLE_NODES.get(GROUP_B).get(0);
        Set<Long> groupBNodes = nodeIds(AVAILABLE_NODES.get(GROUP_B));
        long oldWorkerGroupId = nodeOfGroupB.getWorkerGroupId();
        nodeOfGroupB.setWorkerGroupId(GROUP_B);
        try {
            RequestLoadResult result1 = batchWriteMgr.requestLoad(
                    tableId, params, UserIdentity.ROOT, nodeOfGroupA.getId(), nodeOfGroupA.getHost());
            assertTrue(result1.isOk());
            MergeCommitJob oldJob = batchWriteMgr.getJob(tableId, params);
            assertNotNull(oldJob.getTask(result1.getValue()));

            makeGroupUnavailable(GROUP_A, oldJob);
            // A node of another group asks for a load, as when the HTTP request went to it. A task cannot run on
            // a node outside the CN group of its job, so the job moves to the group of that node.
            RequestLoadResult result2 = batchWriteMgr.requestLoad(
                    tableId, params, UserIdentity.ROOT, nodeOfGroupB.getId(), nodeOfGroupB.getHost());
            assertTrue(result2.isOk(), result2.getStatus().toString());

            MergeCommitJob newJob = batchWriteMgr.getJob(tableId, params);
            assertNotSame(oldJob, newJob);
            assertEquals(GROUP_B, newJob.getComputeResource().getWorkerGroupId());
            MergeCommitTask task = newJob.getTask(result2.getValue());
            assertNotNull(task);
            assertEquals(groupBNodes, task.getBackendIds());
            // The task of the old job stays with the old job
            assertNotNull(oldJob.getTask(result1.getValue()));
            assertNull(newJob.getTask(result1.getValue()));
        } finally {
            nodeOfGroupB.setWorkerGroupId(oldWorkerGroupId);
        }
    }

    @Test
    public void testLoadRequestFromTheUnusableGroupMovesTheJobWithoutRetry() {
        new MockUp<MergeCommitTask>() {
            @Mock
            public void run() {
            }
        };
        ACQUIRED_GROUPS.addAll(List.of(GROUP_A, GROUP_C));
        ComputeNode nodeOfGroupA = AVAILABLE_NODES.get(GROUP_A).get(0);
        ComputeNode nodeOfGroupC = AVAILABLE_NODES.get(GROUP_C).get(0);
        long oldWorkerGroupId = nodeOfGroupA.getWorkerGroupId();
        nodeOfGroupA.setWorkerGroupId(GROUP_A);
        try {
            assertTrue(batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT).isOk());
            MergeCommitJob oldJob = batchWriteMgr.getJob(tableId, params);

            // The node asking for the load is in the job's own group, which has no node to coordinate loads any
            // more (the node may be alive but in the blocklist). The job moves to the group an acquisition gives,
            // but the request is not retried there: its task would have to run on a node outside that group.
            makeGroupUnavailable(GROUP_A, oldJob);
            RequestLoadResult result1 = batchWriteMgr.requestLoad(
                    tableId, params, UserIdentity.ROOT, nodeOfGroupA.getId(), nodeOfGroupA.getHost());
            assertEquals(TStatusCode.SERVICE_UNAVAILABLE, result1.getStatus().getStatus_code());
            MergeCommitJob newJob = batchWriteMgr.getJob(tableId, params);
            assertNotSame(oldJob, newJob);
            assertEquals(GROUP_C, newJob.getComputeResource().getWorkerGroupId());
            assertEquals(0, newJob.numRunningLoads());

            // A node of the new group gets a task of its own group
            RequestLoadResult result2 = batchWriteMgr.requestLoad(
                    tableId, params, UserIdentity.ROOT, nodeOfGroupC.getId(), nodeOfGroupC.getHost());
            assertTrue(result2.isOk(), result2.getStatus().toString());
            assertEquals(Set.of(nodeOfGroupC.getId()), newJob.getTask(result2.getValue()).getBackendIds());
        } finally {
            nodeOfGroupA.setWorkerGroupId(oldWorkerGroupId);
        }
    }

    @Test
    public void testLoadRequestFromTheOldCnGroupGetsNoTaskOfTheNewJob() {
        new MockUp<MergeCommitTask>() {
            @Mock
            public void run() {
            }
        };
        ACQUIRED_GROUPS.addAll(List.of(GROUP_A, GROUP_B));
        ComputeNode nodeOfGroupA = AVAILABLE_NODES.get(GROUP_A).get(0);
        ComputeNode nodeOfGroupB = AVAILABLE_NODES.get(GROUP_B).get(0);
        Set<Long> groupBNodes = nodeIds(AVAILABLE_NODES.get(GROUP_B));
        long oldWorkerGroupIdOfA = nodeOfGroupA.getWorkerGroupId();
        long oldWorkerGroupIdOfB = nodeOfGroupB.getWorkerGroupId();
        nodeOfGroupA.setWorkerGroupId(GROUP_A);
        nodeOfGroupB.setWorkerGroupId(GROUP_B);
        try {
            assertTrue(batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT).isOk());
            MergeCommitJob oldJob = batchWriteMgr.getJob(tableId, params);
            AVAILABLE_NODES.put(GROUP_A, List.of());
            MergeCommitJob newJob = batchWriteMgr.switchCnGroupIfUnavailable(batchWriteId, oldJob, null);
            assertNotNull(newJob);
            assertEquals(GROUP_B, newJob.getComputeResource().getWorkerGroupId());
            mockRunMode(RunMode.SHARED_DATA);
            long numSwitches = MergeCommitMetricRegistry.getInstance().getJobCnGroupSwitchNum();

            // The node of group A is alive but cannot coordinate loads, as when it is in the blocklist, and asks for
            // a load on the new job, as for a request redirected to it before the move or a client writing to it
            // directly. A task of the new job cannot run on that node, so the request gets no task.
            RequestLoadResult result1 = batchWriteMgr.requestLoad(
                    tableId, params, UserIdentity.ROOT, nodeOfGroupA.getId(), nodeOfGroupA.getHost());
            assertEquals(TStatusCode.SERVICE_UNAVAILABLE, result1.getStatus().getStatus_code());
            assertTrue(result1.getStatus().getError_msgs().get(0).contains("is not in CN group"),
                    result1.getStatus().toString());
            assertEquals(0, newJob.numRunningLoads());
            assertSame(newJob, batchWriteMgr.getJob(tableId, params));
            assertEquals(numSwitches, MergeCommitMetricRegistry.getInstance().getJobCnGroupSwitchNum());

            // The nodes of group B get a task of their own group, and the node of group A does not join it
            RequestLoadResult result2 = batchWriteMgr.requestLoad(
                    tableId, params, UserIdentity.ROOT, nodeOfGroupB.getId(), nodeOfGroupB.getHost());
            assertTrue(result2.isOk(), result2.getStatus().toString());
            assertEquals(groupBNodes, newJob.getTask(result2.getValue()).getBackendIds());
            RequestLoadResult result3 = batchWriteMgr.requestLoad(
                    tableId, params, UserIdentity.ROOT, nodeOfGroupA.getId(), nodeOfGroupA.getHost());
            assertEquals(TStatusCode.SERVICE_UNAVAILABLE, result3.getStatus().getStatus_code());
            assertEquals(1, newJob.numRunningLoads());
            assertEquals(groupBNodes, newJob.getTask(result2.getValue()).getBackendIds());
        } finally {
            nodeOfGroupA.setWorkerGroupId(oldWorkerGroupIdOfA);
            nodeOfGroupB.setWorkerGroupId(oldWorkerGroupIdOfB);
        }
    }

    @Test
    public void testLoadRequestFromAnotherCnGroupGetsNoTask() {
        new MockUp<MergeCommitTask>() {
            @Mock
            public void run() {
            }
        };
        ACQUIRED_GROUPS.add(GROUP_A);
        ComputeNode nodeOfGroupC = AVAILABLE_NODES.get(GROUP_C).get(0);
        long oldWorkerGroupId = nodeOfGroupC.getWorkerGroupId();
        nodeOfGroupC.setWorkerGroupId(GROUP_C);
        try {
            assertTrue(batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT).isOk());
            MergeCommitJob job = batchWriteMgr.getJob(tableId, params);
            mockRunMode(RunMode.SHARED_DATA);
            int numAcquisitions = NUM_ACQUISITIONS.get();

            // A client writes to a node of group C directly while the job is on group A, which can coordinate
            // loads. The request gets no task, and the job stays on group A.
            RequestLoadResult result = batchWriteMgr.requestLoad(
                    tableId, params, UserIdentity.ROOT, nodeOfGroupC.getId(), nodeOfGroupC.getHost());
            assertEquals(TStatusCode.SERVICE_UNAVAILABLE, result.getStatus().getStatus_code());
            assertEquals(0, job.numRunningLoads());
            assertSame(job, batchWriteMgr.getJob(tableId, params));
            assertEquals(numAcquisitions, NUM_ACQUISITIONS.get());
        } finally {
            nodeOfGroupC.setWorkerGroupId(oldWorkerGroupId);
        }
    }

    @Test
    public void testLoadRequestFromAnotherCnGroupInSharedNothingMode() {
        new MockUp<MergeCommitTask>() {
            @Mock
            public void run() {
            }
        };
        ACQUIRED_GROUPS.add(GROUP_A);
        ComputeNode nodeOfGroupC = AVAILABLE_NODES.get(GROUP_C).get(0);
        Set<Long> expectedNodes = new HashSet<>(nodeIds(AVAILABLE_NODES.get(GROUP_A)));
        expectedNodes.add(nodeOfGroupC.getId());
        long oldWorkerGroupId = nodeOfGroupC.getWorkerGroupId();
        nodeOfGroupC.setWorkerGroupId(GROUP_C);
        try {
            assertTrue(batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT).isOk());
            MergeCommitJob job = batchWriteMgr.getJob(tableId, params);
            mockRunMode(RunMode.SHARED_NOTHING);

            // A task in shared-nothing mode can run on any backend, so the requesting node joins it as before
            RequestLoadResult result = batchWriteMgr.requestLoad(
                    tableId, params, UserIdentity.ROOT, nodeOfGroupC.getId(), nodeOfGroupC.getHost());
            assertTrue(result.isOk(), result.getStatus().toString());
            assertEquals(expectedNodes, job.getTask(result.getValue()).getBackendIds());
        } finally {
            nodeOfGroupC.setWorkerGroupId(oldWorkerGroupId);
        }
    }

    @Test
    public void testSwitchSkipsADisabledCnGroup() {
        new MockUp<MergeCommitTask>() {
            @Mock
            public void run() {
            }
        };
        // Group B is disabled while its nodes stay alive, and an acquisition would move the job there
        ACQUIRED_GROUPS.addAll(List.of(GROUP_A, GROUP_B));
        DISABLED_GROUPS.add(GROUP_B);
        ComputeNode nodeOfGroupB = AVAILABLE_NODES.get(GROUP_B).get(0);
        Set<Long> groupCNodes = nodeIds(AVAILABLE_NODES.get(GROUP_C));
        long oldWorkerGroupId = nodeOfGroupB.getWorkerGroupId();
        nodeOfGroupB.setWorkerGroupId(GROUP_B);
        try {
            assertTrue(batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT).isOk());
            MergeCommitJob oldJob = batchWriteMgr.getJob(tableId, params);
            makeGroupUnavailable(GROUP_A, oldJob);

            // A node of group B asks for a load. Neither its group nor the acquired one takes the job, which moves
            // to group C, and the request is not retried there.
            RequestLoadResult result = batchWriteMgr.requestLoad(
                    tableId, params, UserIdentity.ROOT, nodeOfGroupB.getId(), nodeOfGroupB.getHost());
            assertEquals(TStatusCode.SERVICE_UNAVAILABLE, result.getStatus().getStatus_code());
            MergeCommitJob newJob = batchWriteMgr.getJob(tableId, params);
            assertNotSame(oldJob, newJob);
            assertEquals(GROUP_C, newJob.getComputeResource().getWorkerGroupId());
            assertEquals(0, newJob.numRunningLoads());

            // Requests redirected by the FE get the nodes of group C
            RequestCoordinatorBackendResult backends =
                    batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT);
            assertTrue(backends.isOk(), backends.getStatus().toString());
            assertEquals(groupCNodes, nodeIds(backends.getValue()));
        } finally {
            nodeOfGroupB.setWorkerGroupId(oldWorkerGroupId);
        }
    }

    @Test
    public void testNoSwitchWhileOtherNodesOfTheGroupAreAvailable() {
        ACQUIRED_GROUPS.addAll(List.of(GROUP_A, GROUP_B));
        assertTrue(batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT).isOk());
        MergeCommitJob job = batchWriteMgr.getJob(tableId, params);
        int numAcquisitions = NUM_ACQUISITIONS.get();

        // Group A still has nodes to coordinate loads, and the assigner gives them to the job
        assertNull(batchWriteMgr.switchCnGroupIfUnavailable(batchWriteId, job, null));
        assertSame(job, batchWriteMgr.getJob(tableId, params));
        assertEquals(numAcquisitions, NUM_ACQUISITIONS.get());
    }

    @Test
    public void testNoSwitchWithoutAnotherUsableCnGroup() {
        // Acquisitions keep getting group A, whose nodes may still count as alive
        ACQUIRED_GROUPS.add(GROUP_A);
        assertTrue(batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT).isOk());
        MergeCommitJob job = batchWriteMgr.getJob(tableId, params);
        long numSwitches = MergeCommitMetricRegistry.getInstance().getJobCnGroupSwitchNum();

        AVAILABLE_NODES.put(GROUP_B, List.of());
        AVAILABLE_NODES.put(GROUP_C, List.of());
        makeGroupUnavailable(GROUP_A, job);
        RequestCoordinatorBackendResult result =
                batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT);
        assertFalse(result.isOk());
        assertEquals(TStatusCode.SERVICE_UNAVAILABLE, result.getStatus().getStatus_code());
        assertTrue(result.getStatus().getError_msgs().get(0).contains("Can't find available backends"),
                result.getStatus().toString());
        assertSame(job, batchWriteMgr.getJob(tableId, params));
        assertEquals(numSwitches, MergeCommitMetricRegistry.getInstance().getJobCnGroupSwitchNum());

        // Once group C has a node again, a later check moves the job there
        AVAILABLE_NODES.put(GROUP_C, List.of(allNodes.get(4)));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).until(
                () -> batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT).isOk());
        MergeCommitJob newJob = batchWriteMgr.getJob(tableId, params);
        assertNotSame(job, newJob);
        assertEquals(GROUP_C, newJob.getComputeResource().getWorkerGroupId());
        assertEquals(numSwitches + 1, MergeCommitMetricRegistry.getInstance().getJobCnGroupSwitchNum());
    }

    @Test
    public void testSwitchWhenAcquisitionGivesTheSameCnGroup() {
        // Acquisitions keep getting group A, so the job moves to the only other group with a node: B
        ACQUIRED_GROUPS.add(GROUP_A);
        AVAILABLE_NODES.put(GROUP_C, List.of());
        assertTrue(batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT).isOk());
        MergeCommitJob job = batchWriteMgr.getJob(tableId, params);

        AVAILABLE_NODES.put(GROUP_A, List.of());
        MergeCommitJob newJob = batchWriteMgr.switchCnGroupIfUnavailable(batchWriteId, job, null);
        assertNotNull(newJob);
        assertEquals(GROUP_B, newJob.getComputeResource().getWorkerGroupId());
        assertSame(newJob, batchWriteMgr.getJob(tableId, params));
        RequestCoordinatorBackendResult result =
                batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT);
        assertTrue(result.isOk());
        assertEquals(nodeIds(AVAILABLE_NODES.get(GROUP_B)), nodeIds(result.getValue()));
    }

    @Test
    public void testRequestOnAMovedJobRetriesOnItsReplacement() {
        ACQUIRED_GROUPS.addAll(List.of(GROUP_A, GROUP_B, GROUP_C));
        assertTrue(batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT).isOk());
        MergeCommitJob oldJob = batchWriteMgr.getJob(tableId, params);

        AVAILABLE_NODES.put(GROUP_A, List.of());
        MergeCommitJob newJob = batchWriteMgr.switchCnGroupIfUnavailable(batchWriteId, oldJob, null);
        assertNotNull(newJob);
        assertEquals(GROUP_B, newJob.getComputeResource().getWorkerGroupId());

        // A request that got the old job before it was replaced retries on the new one and moves nothing
        int numAcquisitions = NUM_ACQUISITIONS.get();
        long numSwitches = MergeCommitMetricRegistry.getInstance().getJobCnGroupSwitchNum();
        assertSame(newJob, batchWriteMgr.switchCnGroupIfUnavailable(batchWriteId, oldJob, null));
        assertEquals(numAcquisitions, NUM_ACQUISITIONS.get());
        assertEquals(numSwitches, MergeCommitMetricRegistry.getInstance().getJobCnGroupSwitchNum());
        assertSame(newJob, batchWriteMgr.getJob(tableId, params));
        assertEquals(1, batchWriteMgr.numJobs());
    }

    @Test
    public void testCnGroupCheckIsClaimedOncePerInterval() {
        ACQUIRED_GROUPS.add(GROUP_A);
        assertTrue(batchWriteMgr.requestCoordinatorBackends(tableId, params, UserIdentity.ROOT).isOk());
        MergeCommitJob job = batchWriteMgr.getJob(tableId, params);

        long now = 10_000L;
        assertTrue(job.tryClaimCnGroupCheck(now));
        assertFalse(job.tryClaimCnGroupCheck(now));
        assertFalse(job.tryClaimCnGroupCheck(now + MergeCommitJob.CN_GROUP_CHECK_INTERVAL_MS - 1));
        assertTrue(job.tryClaimCnGroupCheck(now + MergeCommitJob.CN_GROUP_CHECK_INTERVAL_MS));
        assertFalse(job.tryClaimCnGroupCheck(now + MergeCommitJob.CN_GROUP_CHECK_INTERVAL_MS));
    }

    // Takes every node of the group out of service, and waits until the assigner's periodical check has taken
    // them off the job, as when the nodes die or enter the blocklist
    private void makeGroupUnavailable(long groupId, MergeCommitJob job) {
        AVAILABLE_NODES.put(groupId, List.of());
        CoordinatorBackendAssigner assigner = batchWriteMgr.getCoordinatorBackendAssigner();
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(
                () -> assigner.getBackends(job.getId()).map(List::isEmpty).orElse(false));
    }

    // Makes RunMode report the mode to the test thread for the rest of the test, while background threads keep the
    // real one. The CN group of a node asking for a load is checked only in shared-data mode.
    private static void mockRunMode(RunMode runMode) {
        Thread testThread = Thread.currentThread();
        RunMode realRunMode = RunMode.getCurrentRunMode();
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return Thread.currentThread() == testThread ? runMode : realRunMode;
            }
        };
    }

    private static ComputeResource group(long groupId) {
        return CNGroupResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID, groupId);
    }

    private static List<ComputeResource> allGroups() {
        return new ArrayList<>(List.of(group(GROUP_A), group(GROUP_B), group(GROUP_C)));
    }

    private static Set<Long> nodeIds(List<ComputeNode> nodes) {
        return nodes.stream().map(ComputeNode::getId).collect(Collectors.toSet());
    }
}
