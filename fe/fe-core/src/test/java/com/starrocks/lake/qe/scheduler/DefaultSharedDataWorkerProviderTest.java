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

package com.starrocks.lake.qe.scheduler;

import com.google.api.client.util.Lists;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.qe.ColocatedBackendSelector;
import com.starrocks.qe.FragmentScanRangeAssignment;
import com.starrocks.qe.HostBlacklist;
import com.starrocks.qe.NormalBackendSelector;
import com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultSharedDataWorkerProviderTest {
    private Map<Long, ComputeNode> id2Backend;
    private Map<Long, ComputeNode> id2ComputeNode;
    private Map<Long, ComputeNode> id2AllNodes;
    private DefaultSharedDataWorkerProvider.Factory factory;

    private static <C extends ComputeNode> Map<Long, C> genWorkers(long startId, long endId,
                                                                   Supplier<C> factory) {
        Map<Long, C> res = new HashMap<>();
        for (long i = startId; i < endId; i++) {
            C worker = factory.get();
            worker.setId(i);
            worker.setAlive(true);
            worker.setHost("host#" + i);
            worker.setBePort(80);
            res.put(i, worker);
        }
        return res;
    }

    @BeforeClass
    public static void setUpTestSuite() {
        SimpleScheduler.getHostBlacklist().disableAutoUpdate();
    }

    @Before
    public void setUp() {
        // clear the block list
        SimpleScheduler.getHostBlacklist().clear();
        factory = new DefaultSharedDataWorkerProvider.Factory();

        // Generate mock Workers
        // BE, 1-10
        id2Backend = genWorkers(1, 11, Backend::new);
        // CN, 11-15
        id2ComputeNode = genWorkers(11, 16, ComputeNode::new);
        // all nodes
        id2AllNodes = Maps.newHashMap(id2Backend);
        id2AllNodes.putAll(id2ComputeNode);

        // Setup MockUp
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        new Expectations(warehouseManager) {
            {
                warehouseManager.getAllComputeNodeIds(anyLong);
                result = Lists.newArrayList(id2AllNodes.keySet());
                minTimes = 0;
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                ComputeNode node = id2ComputeNode.get(nodeId);
                if (node == null) {
                    node = id2Backend.get(nodeId);
                }
                return node;
            }
        };
    }

    private WorkerProvider newWorkerProvider() {
        return factory.captureAvailableWorkers(
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(), true,
                -1, ComputationFragmentSchedulingPolicy.COMPUTE_NODES_ONLY,
                WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    private static void testUsingWorkerHelper(WorkerProvider workerProvider, Long workerId) {
        Assert.assertTrue(workerProvider.isWorkerSelected(workerId));
        Assert.assertTrue(workerProvider.getSelectedWorkerIds().contains(workerId));
    }

    private List<Long> prepareNodeAliveAndBlock(SystemInfoService sysInfo, HostBlacklist blockList) {
        // for every even number of worker, take in turn to set to alive=false and inBlock=true.
        // [0:alive=false, 1, 2:inBlock=true, 3, 4:alive=false, ...]
        List<Long> availList = Lists.newArrayList(id2AllNodes.keySet());
        boolean flip = true;
        for (int i = 0; i < id2AllNodes.size(); i += 2) {
            ComputeNode node = sysInfo.getBackendOrComputeNode(i);
            if (node != null) {
                if (flip) {
                    node.setAlive(false);
                } else {
                    blockList.add(node.getId());
                }
                flip = !flip;
                availList.remove(node.getId());
            }
        }
        return availList;
    }

    @Test
    public void testCaptureAvailableWorkers() {
        long deadBEId = 1L;
        long deadCNId = 11L;
        long inBlacklistBEId = 3L;
        long inBlacklistCNId = 13L;

        HostBlacklist blockList = SimpleScheduler.getHostBlacklist();

        blockList.add(inBlacklistBEId);
        blockList.add(inBlacklistCNId);
        id2Backend.get(deadBEId).setAlive(false);
        id2ComputeNode.get(deadCNId).setAlive(false);

        Set<Long> nonAvailableWorkerId = ImmutableSet.of(deadBEId, deadCNId, inBlacklistBEId, inBlacklistCNId);
        WorkerProvider workerProvider = newWorkerProvider();

        Optional<Long> maxId = id2Backend.keySet().stream().max(Comparator.naturalOrder());
        Assert.assertFalse(maxId.isEmpty());
        for (long id : id2AllNodes.keySet()) {
            ComputeNode worker = workerProvider.getWorkerById(id);
            if (nonAvailableWorkerId.contains(id)) {
                Assert.assertNull(worker);
            } else {
                Assert.assertEquals(id, worker.getId());
                if (id <= maxId.get()) {
                    Assert.assertTrue(worker instanceof Backend);
                } else {
                    Assert.assertFalse(worker instanceof Backend);
                }
            }
        }
    }

    @Test
    public void testSelectWorker() throws StarRocksException {
        HostBlacklist blockList = SimpleScheduler.getHostBlacklist();
        SystemInfoService sysInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

        List<Long> availList = prepareNodeAliveAndBlock(sysInfo, blockList);
        WorkerProvider provider = newWorkerProvider();

        // intend to iterate the id out of the actual range.
        for (long id = -1; id < id2AllNodes.size() + 5; id++) {
            if (availList.contains(id)) {
                provider.selectWorker(id);
                testUsingWorkerHelper(provider, id);
            } else {
                long finalId = id;
                Assert.assertThrows(NonRecoverableException.class, () -> provider.selectWorker(finalId));
            }
        }
    }

    @Test
    public void testGetAllWorkers() {
        HostBlacklist blockList = SimpleScheduler.getHostBlacklist();
        SystemInfoService sysInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

        List<Long> availList = prepareNodeAliveAndBlock(sysInfo, blockList);
        WorkerProvider provider = newWorkerProvider();
        // allWorkers returns only available workers
        Collection<ComputeNode> allWorkers = provider.getAllWorkers();

        Assert.assertEquals(availList.size(), allWorkers.size());
        for (ComputeNode node : allWorkers) {
            Assert.assertTrue(availList.contains(node.getId()));
        }
        List<Long> allWorkerIds = allWorkers.stream().map(ComputeNode::getId).collect(Collectors.toList());
        for (long availId : availList) {
            Assert.assertTrue(allWorkerIds.contains(availId));
        }
        // strictly the same
        List<Long> allAvailNodeIds = provider.getAllAvailableNodes();
        Assert.assertEquals(allWorkerIds, allAvailNodeIds);
    }

    private static void testSelectNextWorkerHelper(WorkerProvider workerProvider,
                                                   Map<Long, ComputeNode> id2Worker)
            throws StarRocksException {
        Set<Long> selectedWorkers = new HashSet<>(id2Worker.size());
        for (int i = 0; i < id2Worker.size(); i++) {
            long workerId = workerProvider.selectNextWorker();
            Assert.assertFalse(selectedWorkers.contains(workerId));
            selectedWorkers.add(workerId);
            testUsingWorkerHelper(workerProvider, workerId);
        }
    }

    @Test
    public void testSelectNextWorker() throws StarRocksException {
        HostBlacklist blockList = SimpleScheduler.getHostBlacklist();
        blockList.clear();
        SystemInfoService sysInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

        List<Long> availList = prepareNodeAliveAndBlock(sysInfo, blockList);
        { // test backend nodes only
            ImmutableMap.Builder<Long, ComputeNode> builder = new ImmutableMap.Builder<>();
            for (long backendId : id2Backend.keySet()) {
                if (availList.contains(backendId)) {
                    builder.put(backendId, id2Backend.get(backendId));
                }
            }
            ImmutableMap<Long, ComputeNode> availableId2ComputeNode = builder.build();
            WorkerProvider workerProvider =
                    new DefaultSharedDataWorkerProvider(ImmutableMap.copyOf(id2Backend), availableId2ComputeNode);
            testSelectNextWorkerHelper(workerProvider, availableId2ComputeNode);
        }

        { // test compute nodes only
            ImmutableMap.Builder<Long, ComputeNode> builder = new ImmutableMap.Builder<>();
            for (long backendId : id2ComputeNode.keySet()) {
                if (availList.contains(backendId)) {
                    builder.put(backendId, id2ComputeNode.get(backendId));
                }
            }
            ImmutableMap<Long, ComputeNode> availableId2ComputeNode = builder.build();
            WorkerProvider workerProvider =
                    new DefaultSharedDataWorkerProvider(ImmutableMap.copyOf(id2ComputeNode), availableId2ComputeNode);
            testSelectNextWorkerHelper(workerProvider, availableId2ComputeNode);
        }

        { // test both backends and compute nodes
            ImmutableMap.Builder<Long, ComputeNode> builder = new ImmutableMap.Builder<>();
            for (long backendId : id2AllNodes.keySet()) {
                if (availList.contains(backendId)) {
                    builder.put(backendId, id2AllNodes.get(backendId));
                }
            }
            ImmutableMap<Long, ComputeNode> availableId2ComputeNode = builder.build();
            WorkerProvider workerProvider =
                    new DefaultSharedDataWorkerProvider(ImmutableMap.copyOf(id2AllNodes), availableId2ComputeNode);
            testSelectNextWorkerHelper(workerProvider, availableId2ComputeNode);
        }

        { // test no available worker to select
            WorkerProvider workerProvider =
                    new DefaultSharedDataWorkerProvider(ImmutableMap.copyOf(id2AllNodes), ImmutableMap.of());

            Exception e = Assert.assertThrows(NonRecoverableException.class, workerProvider::selectNextWorker);
            Assert.assertEquals(
                    "Compute node not found. Check if any compute node is down. nodeId: -1 " +
                            "compute node: [host#1 alive: true, available: false, inBlacklist: false] " +
                            "[host#2 alive: false, available: false, inBlacklist: false] " +
                            "[host#3 alive: true, available: false, inBlacklist: false] " +
                            "[host#4 alive: true, available: false, inBlacklist: true] " +
                            "[host#5 alive: true, available: false, inBlacklist: false] " +
                            "[host#6 alive: false, available: false, inBlacklist: false] " +
                            "[host#7 alive: true, available: false, inBlacklist: false] " +
                            "[host#8 alive: true, available: false, inBlacklist: true] " +
                            "[host#9 alive: true, available: false, inBlacklist: false] " +
                            "[host#10 alive: false, available: false, inBlacklist: false] " +
                            "[host#11 alive: true, available: false, inBlacklist: false] " +
                            "[host#12 alive: true, available: false, inBlacklist: true] " +
                            "[host#13 alive: true, available: false, inBlacklist: false] " +
                            "[host#14 alive: false, available: false, inBlacklist: false] " +
                            "[host#15 alive: true, available: false, inBlacklist: false] ",
                    e.getMessage());
        }
    }

    @Test
    public void testChooseAllComputedNodes() {
        { // empty compute nodes
            WorkerProvider workerProvider =
                    new DefaultSharedDataWorkerProvider(ImmutableMap.of(), ImmutableMap.of());
            Assert.assertTrue(workerProvider.selectAllComputeNodes().isEmpty());
        }

        { // both compute nodes and backend are treated as compute nodes
            WorkerProvider workerProvider =
                    new DefaultSharedDataWorkerProvider(ImmutableMap.copyOf(id2AllNodes),
                            ImmutableMap.copyOf(id2AllNodes));

            List<Long> computeNodeIds = workerProvider.selectAllComputeNodes();
            Assert.assertEquals(id2AllNodes.size(), computeNodeIds.size());
            Set<Long> computeNodeIdSet = new HashSet<>(computeNodeIds);
            for (ComputeNode computeNode : id2AllNodes.values()) {
                Assert.assertTrue(computeNodeIdSet.contains(computeNode.getId()));
                testUsingWorkerHelper(workerProvider, computeNode.getId());
            }
        }
    }

    @Test
    public void testIsDataNodeAvailable() {
        HostBlacklist blockList = SimpleScheduler.getHostBlacklist();
        blockList.clear();
        SystemInfoService sysInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

        List<Long> availList = prepareNodeAliveAndBlock(sysInfo, blockList);
        WorkerProvider provider = newWorkerProvider();

        for (long id = -1; id < 16; id++) {
            boolean isAvail = provider.isDataNodeAvailable(id);
            ComputeNode worker = provider.getWorkerById(id);
            if (!availList.contains(id)) {
                Assert.assertFalse(isAvail);
            } else {
                Assert.assertEquals(id2AllNodes.get(id), worker);
                Assert.assertTrue(isAvail);
            }
        }
    }

    @Test
    public void testReportBackendNotFoundException() {
        WorkerProvider workerProvider = newWorkerProvider();
        Assert.assertThrows(NonRecoverableException.class, workerProvider::reportDataNodeNotFoundException);
    }

    @Test
    public void testSelectBackupWorkersEvenlySelected() {
        Set<Long> counters = Sets.newHashSet();
        WorkerProvider workerProvider = newWorkerProvider();
        Assert.assertTrue(workerProvider.allowUsingBackupNode());
        for (long id : id2AllNodes.keySet()) {
            long backupId = -1;
            for (int j = 0; j < 100; ++j) {
                long selectedId = workerProvider.selectBackupWorker(id);
                Assert.assertTrue(selectedId > 0);
                // cannot choose itself
                Assert.assertNotEquals(id, selectedId);
                if (backupId == -1) {
                    backupId = selectedId;
                } else {
                    // always get the same node
                    Assert.assertEquals(backupId, selectedId);
                }
            }
            Assert.assertTrue(backupId != -1);
            Assert.assertFalse(counters.contains(id));
            counters.add(id);
        }
        // every node is chosen as a backup node once
        Assert.assertEquals(id2AllNodes.size(), counters.size());
    }

    @Test
    public void testIsPreferComputeNode() {
        WorkerProvider provider = newWorkerProvider();
        Assert.assertTrue(provider.isPreferComputeNode());
    }

    @Test
    public void testSelectBackupWorkerStable() {
        HostBlacklist blockList = SimpleScheduler.getHostBlacklist();
        SystemInfoService sysInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<Long> availList = prepareNodeAliveAndBlock(sysInfo, blockList);

        Optional<Long> unavail = id2AllNodes.keySet().stream().filter(x -> !availList.contains(x)).findAny();
        Assert.assertTrue(unavail.isPresent());

        long unavailWorkerId = unavail.get();
        WorkerProvider provider = newWorkerProvider();

        int selectCount = 0;
        List<Long> selectedNodeId = Lists.newArrayList();

        while (selectCount < availList.size() * 2 + 1) { // make sure the while loop will stop
            Assert.assertFalse(provider.isDataNodeAvailable(unavailWorkerId));
            long alterNodeId = provider.selectBackupWorker(unavailWorkerId);
            if (alterNodeId == -1) {
                break;
            }
            // the backup node is not itself
            Assert.assertTrue(alterNodeId != unavailWorkerId);
            // the backup node is not any of the node before
            Assert.assertFalse(selectedNodeId.contains(alterNodeId));

            for (int j = 0; j < 10; ++j) {
                long selectAgainId = provider.selectBackupWorker(unavailWorkerId);
                Assert.assertEquals(alterNodeId, selectAgainId);
            }
            ++selectCount;
            // make it in blockList, so next time it will choose a different node
            blockList.add(alterNodeId);
            selectedNodeId.add(alterNodeId);
        }
        // all nodes are in block list, no nodes can be selected anymore
        Assert.assertEquals(-1, provider.selectBackupWorker(unavailWorkerId));
        // all the nodes are selected ever
        Assert.assertEquals(selectedNodeId.size(), availList.size());

        // a random workerId that doesn't exist in workerProvider
        Assert.assertEquals(-1, provider.selectBackupWorker(15678));
    }

    private OlapScanNode newOlapScanNode(int id, int numBuckets) {
        // copy from fe/fe-core/src/test/java/com/starrocks/qe/ColocatedBackendSelectorTest.java
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        OlapTable table = new OlapTable();
        table.setDefaultDistributionInfo(new HashDistributionInfo(numBuckets, Collections.emptyList()));
        desc.setTable(table);
        return new OlapScanNode(new PlanNodeId(id), desc, "OlapScanNode");
    }

    private ArrayListMultimap<Integer, TScanRangeLocations> genBucketSeq2Locations(
            Map<Integer, List<Long>> bucketSeqToBackends,
            int numTabletsPerBucket) {
        // copy from fe/fe-core/src/test/java/com/starrocks/qe/ColocatedBackendSelectorTest.java
        ArrayListMultimap<Integer, TScanRangeLocations> bucketSeq2locations = ArrayListMultimap.create();
        bucketSeqToBackends.forEach((bucketSeq, backends) -> {
            for (int i = 0; i < numTabletsPerBucket; i++) {
                TScanRangeLocations bucketLocations = new TScanRangeLocations();

                bucketLocations.setScan_range(new TScanRange().setInternal_scan_range(new TInternalScanRange()));

                List<TScanRangeLocation> locations = backends.stream()
                        .map(backendId -> new TScanRangeLocation().setBackend_id(backendId))
                        .collect(Collectors.toList());
                bucketLocations.setLocations(locations);

                bucketSeq2locations.put(bucketSeq, bucketLocations);
            }
        });

        return bucketSeq2locations;
    }

    /**
     * Generate a list of ScanRangeLocations, contains n element for bucketNum
     *
     * @param n         number of ScanRangeLocations
     * @param bucketNum number of buckets
     * @return lists of ScanRangeLocations
     */
    private List<TScanRangeLocations> generateScanRangeLocations(Map<Long, ComputeNode> nodes, int n, int bucketNum) {
        List<TScanRangeLocations> locations = Lists.newArrayList();
        int currentBucketIndex = 0;
        Iterator<Map.Entry<Long, ComputeNode>> iterator = nodes.entrySet().iterator();
        for (int i = 0; i < n; ++i) {
            if (!iterator.hasNext()) {
                iterator = nodes.entrySet().iterator();
            }
            TInternalScanRange internalRange = new TInternalScanRange();
            internalRange.setBucket_sequence(currentBucketIndex);
            internalRange.setRow_count(1);

            TScanRange range = new TScanRange();
            range.setInternal_scan_range(internalRange);

            TScanRangeLocations loc = new TScanRangeLocations();
            loc.setScan_range(range);

            TScanRangeLocation location = new TScanRangeLocation();
            ComputeNode node = iterator.next().getValue();
            location.setBackend_id(node.getId());
            location.setServer(node.getAddress());
            loc.addToLocations(location);

            locations.add(loc);
            currentBucketIndex = (currentBucketIndex + 1) % bucketNum;
        }
        return locations;
    }

    @Test
    public void testNormalBackendSelectorWithSharedDataWorkerProvider() {
        HostBlacklist blockList = SimpleScheduler.getHostBlacklist();
        blockList.clear();
        SystemInfoService sysInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<Long> availList = prepareNodeAliveAndBlock(sysInfo, blockList);

        int bucketNum = 3;
        OlapScanNode scanNode = newOlapScanNode(1, bucketNum);
        List<TScanRangeLocations> scanLocations = generateScanRangeLocations(id2AllNodes, 10, bucketNum);

        WorkerProvider provider = newWorkerProvider();

        int nonAvailNum = 0;
        for (TScanRangeLocations locations : scanLocations) {
            for (TScanRangeLocation location : locations.getLocations()) {
                if (!provider.isDataNodeAvailable(location.getBackend_id())) {
                    ++nonAvailNum;
                }
            }
        }
        // the scanRangeLocations contains non-avail locations
        Assert.assertTrue(nonAvailNum > 0);

        { // normal case
            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            NormalBackendSelector selector =
                    new NormalBackendSelector(scanNode, scanLocations, assignment, provider, false);
            // the computation will not fail even though there are non-available locations
            ExceptionChecker.expectThrowsNoException(selector::computeScanRangeAssignment);

            // check the assignment, should be all in the availList
            for (long id : assignment.keySet()) {
                Assert.assertTrue(availList.contains(id));
            }
        }

        { // make only one node available, the final assignment will be all on the single available node
            ComputeNode availNode = id2AllNodes.get(availList.get(0));
            WorkerProvider provider1 = new DefaultSharedDataWorkerProvider(ImmutableMap.copyOf(id2AllNodes),
                    ImmutableMap.of(availNode.getId(), availNode));

            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            NormalBackendSelector selector =
                    new NormalBackendSelector(scanNode, scanLocations, assignment, provider1, false);
            // the computation will not fail even though there are non-available locations
            ExceptionChecker.expectThrowsNoException(selector::computeScanRangeAssignment);

            Assert.assertEquals(1, assignment.size());
            // check the assignment, should be all in the availList
            for (long id : assignment.keySet()) {
                Assert.assertEquals(availNode.getId(), id);
            }
        }

        { // make no node available. Exception throws
            WorkerProvider providerNoAvailNode = new DefaultSharedDataWorkerProvider(ImmutableMap.copyOf(id2AllNodes),
                    ImmutableMap.of());
            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            NormalBackendSelector selector =
                    new NormalBackendSelector(scanNode, scanLocations, assignment, providerNoAvailNode, false);
            Assert.assertThrows(NonRecoverableException.class, selector::computeScanRangeAssignment);
        }
    }

    @Test
    public void testCollocationBackendSelectorWithSharedDataWorkerProvider() {
        HostBlacklist blockList = SimpleScheduler.getHostBlacklist();
        blockList.clear();
        SystemInfoService sysInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<Long> availList = prepareNodeAliveAndBlock(sysInfo, blockList);

        int bucketNum = 6;
        OlapScanNode scanNode = newOlapScanNode(10, bucketNum);
        final Map<Integer, List<Long>> bucketSeqToBackends = ImmutableMap.of(
                0, ImmutableList.of(1L),
                1, ImmutableList.of(2L),
                2, ImmutableList.of(3L),
                3, ImmutableList.of(4L),
                4, ImmutableList.of(5L),
                5, ImmutableList.of(6L)
        );
        scanNode.bucketSeq2locations = genBucketSeq2Locations(bucketSeqToBackends, 3);
        List<TScanRangeLocations> scanLocations = generateScanRangeLocations(id2AllNodes, 10, bucketNum);
        WorkerProvider provider = newWorkerProvider();

        int nonAvailNum = 0;
        for (TScanRangeLocations locations : scanLocations) {
            for (TScanRangeLocation location : locations.getLocations()) {
                if (!provider.isDataNodeAvailable(location.getBackend_id())) {
                    ++nonAvailNum;
                }
            }
        }
        // the scanRangeLocations contains non-avail locations
        Assert.assertTrue(nonAvailNum > 0);

        { // normal case
            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            ColocatedBackendSelector.Assignment colAssignment = new ColocatedBackendSelector.Assignment(scanNode, 1);
            ColocatedBackendSelector selector =
                    new ColocatedBackendSelector(scanNode, assignment, colAssignment, false, provider, 1);
            // the computation will not fail even though there are non-available locations
            ExceptionChecker.expectThrowsNoException(selector::computeScanRangeAssignment);

            // check the assignment, should be all in the availList
            for (long id : assignment.keySet()) {
                Assert.assertTrue(availList.contains(id));
            }
        }

        { // make only one node available, the final assignment will be all on the single available node
            ComputeNode availNode = id2AllNodes.get(availList.get(0));
            WorkerProvider provider1 = new DefaultSharedDataWorkerProvider(ImmutableMap.copyOf(id2AllNodes),
                    ImmutableMap.of(availNode.getId(), availNode));

            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            ColocatedBackendSelector.Assignment colAssignment = new ColocatedBackendSelector.Assignment(scanNode, 1);
            ColocatedBackendSelector selector =
                    new ColocatedBackendSelector(scanNode, assignment, colAssignment, false, provider1, 1);
            // the computation will not fail even though there are non-available locations
            ExceptionChecker.expectThrowsNoException(selector::computeScanRangeAssignment);

            Assert.assertEquals(1, assignment.size());
            // check the assignment, should be all in the availList
            for (long id : assignment.keySet()) {
                Assert.assertEquals(availNode.getId(), id);
            }
        }

        { // make no node available. Exception throws
            WorkerProvider providerNoAvailNode = new DefaultSharedDataWorkerProvider(ImmutableMap.copyOf(id2AllNodes),
                    ImmutableMap.of());
            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            ColocatedBackendSelector.Assignment colAssignment = new ColocatedBackendSelector.Assignment(scanNode, 1);
            ColocatedBackendSelector selector =
                    new ColocatedBackendSelector(scanNode, assignment, colAssignment, false, providerNoAvailNode, 1);
            Assert.assertThrows(NonRecoverableException.class, selector::computeScanRangeAssignment);
        }
    }

    @Test
    public void testNextWorkerOverflow() throws NonRecoverableException {
        WorkerProvider provider =
                new DefaultSharedDataWorkerProvider(ImmutableMap.copyOf(id2AllNodes), ImmutableMap.copyOf(id2AllNodes));
        for (int i = 0; i < 100; i++) {
            Long workerId = provider.selectNextWorker();
            assertThat(workerId).isNotNegative();
        }
        DefaultSharedDataWorkerProvider.getNextComputeNodeIndexer().set(Integer.MAX_VALUE);
        for (int i = 0; i < 100; i++) {
            Long workerId = provider.selectNextWorker();
            assertThat(workerId).isNotNegative();
        }
    }
}
