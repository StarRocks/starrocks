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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.BucketProperty;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.DefaultWorkerProvider;
import com.starrocks.qe.scheduler.LazyWorkerProvider;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.assignment.BackendSelectorFactory;
import com.starrocks.qe.scheduler.assignment.FragmentAssignmentStrategy;
import com.starrocks.qe.scheduler.assignment.FragmentAssignmentStrategyFactory;
import com.starrocks.qe.scheduler.assignment.LocalFragmentAssignmentStrategy;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TBucketFunction;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TUniqueId;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BucketAwareBackendSelectorTest {

    @Mocked
    private ScanNode scanNode;

    private WorkerProvider genWorkerProvider(Set<Long> backendIds) {
        ImmutableMap<Long, ComputeNode> id2Backend = ImmutableMap.copyOf(backendIds.stream().collect(Collectors.toMap(
                Function.identity(), backendId -> new ComputeNode(backendId, "host" + backendId, 9030)
        )));
        ImmutableMap<Long, ComputeNode> id2ComputeNode = ImmutableMap.of();

        return new DefaultWorkerProvider(id2Backend, id2ComputeNode, id2Backend, id2ComputeNode, false,
                WarehouseManager.DEFAULT_RESOURCE);
    }

    private List<TScanRangeLocations> genScanRangeLocations(int bucketNum, int scanRangesPerBucket) {
        List<TScanRangeLocations> locations = new ArrayList<>();

        for (int i = 0; i < bucketNum; i++) {
            for (int j = 0; j < scanRangesPerBucket; j++) {
                TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
                // Create HDFS scan range with bucket ID
                THdfsScanRange hdfsScanRange = new THdfsScanRange();
                hdfsScanRange.setBucket_id(i);
                hdfsScanRange.setFull_path(String.format("full_path_%d", j));
                TScanRange scanRange = new TScanRange();
                scanRange.setHdfs_scan_range(hdfsScanRange);
                scanRangeLocations.setScan_range(scanRange);
                TScanRangeLocation location = new TScanRangeLocation();
                location.setServer(new TNetworkAddress("localhost", -1));
                scanRangeLocations.setLocations(new ArrayList<>(List.of(location)));
                locations.add(scanRangeLocations);
            }
        }

        return locations;
    }

    private ColocatedBackendSelector.Assignment genColocatedAssignment(int bucketNum, int numScanNodes) {
        BucketProperty bucketProperty = new BucketProperty(TBucketFunction.MURMUR3_X86_32, 10, new Column("c1", Type.INT));
        return new ColocatedBackendSelector.Assignment(bucketNum, numScanNodes, Optional.of(List.of(bucketProperty)));
    }

    @Test
    public void testComputeScanRangeAssignmentBasic() throws StarRocksException {
        // Setup test data
        final int bucketNum = 4;
        final int scanRangesPerBucket = 2;
        final Set<Long> backendIds = Set.of(1L, 2L, 3L, 4L);

        // Create test objects
        List<TScanRangeLocations> locations = genScanRangeLocations(bucketNum, scanRangesPerBucket);
        ColocatedBackendSelector.Assignment colocatedAssignment = genColocatedAssignment(bucketNum, 1);
        WorkerProvider workerProvider = genWorkerProvider(backendIds);

        new Expectations() {
            {
                scanNode.getId();
                result = new PlanNodeId(1);
            }
        };

        // Create and test BucketAwareBackendSelector
        BucketAwareBackendSelector selector = new BucketAwareBackendSelector(
                scanNode, locations, colocatedAssignment, workerProvider, false, false, SessionVariableConstants.BALANCE);

        // Execute the method under test
        selector.computeScanRangeAssignment();

        // Verify results
        Map<Integer, Long> seqToWorkerId = colocatedAssignment.getSeqToWorkerId();
        ColocatedBackendSelector.BucketSeqToScanRange seqToScanRange = colocatedAssignment.getSeqToScanRange();

        // Check that all buckets are assigned to workers
        Assertions.assertEquals(bucketNum, seqToWorkerId.size());

        // Check that scan ranges are properly distributed
        for (int bucketSeq = 0; bucketSeq < bucketNum; bucketSeq++) {
            Assertions.assertTrue(seqToWorkerId.containsKey(bucketSeq));
            Assertions.assertTrue(backendIds.contains(seqToWorkerId.get(bucketSeq)));
            Assertions.assertTrue(seqToScanRange.containsKey(bucketSeq));
        }
        Assertions.assertTrue(seqToScanRange.entrySet().stream().allMatch(entry -> {
            return entry.getValue().size() == 1 && entry.getValue().containsKey(1)
                    && entry.getValue().get(1).size() == scanRangesPerBucket;
        }));
    }

    @Test
    public void testComputeScanRangeAssignmentWithIncrementalScanRanges() throws StarRocksException {
        // Setup test data
        final int bucketNum = 3;
        final int scanRangesPerBucket = 3;
        final Set<Long> backendIds = Set.of(1L, 2L, 3L);

        // Create test objects
        List<TScanRangeLocations> locations = genScanRangeLocations(bucketNum, scanRangesPerBucket);
        ColocatedBackendSelector.Assignment colocatedAssignment = genColocatedAssignment(bucketNum, 1);
        WorkerProvider workerProvider = genWorkerProvider(backendIds);

        new Expectations() {
            {
                scanNode.getId();
                result = new PlanNodeId(2);

                scanNode.hasMoreScanRanges();
                result = true;
            }
        };

        // Create and test BucketAwareBackendSelector with incremental scan ranges
        BucketAwareBackendSelector selector = new BucketAwareBackendSelector(
                scanNode, locations, colocatedAssignment, workerProvider, false, true, SessionVariableConstants.BALANCE);

        // Execute the method under test
        selector.computeScanRangeAssignment();

        // Verify results
        Map<Integer, Long> seqToWorkerId = colocatedAssignment.getSeqToWorkerId();
        ColocatedBackendSelector.BucketSeqToScanRange seqToScanRange = colocatedAssignment.getSeqToScanRange();

        // Check that all buckets are assigned to workers (including empty ones)
        Assertions.assertEquals(bucketNum, seqToWorkerId.size());

        // Check that all buckets have scan ranges (including empty marker)
        for (int bucketSeq = 0; bucketSeq < bucketNum; bucketSeq++) {
            Assertions.assertTrue(seqToWorkerId.containsKey(bucketSeq));
            Assertions.assertTrue(seqToScanRange.containsKey(bucketSeq));
            Assertions.assertTrue(seqToScanRange.get(bucketSeq).containsKey(2)); // scanNode.getId() = 2

            List<TScanRangeParams> scanRangeParams = seqToScanRange.get(bucketSeq).get(2);
            Assertions.assertFalse(scanRangeParams.isEmpty());
            Assertions.assertEquals(scanRangesPerBucket + 1, scanRangeParams.size());

            // The last scan range should be the empty marker with has_more = true
            TScanRangeParams lastParam = scanRangeParams.get(scanRangeParams.size() - 1);
            Assertions.assertTrue(lastParam.isEmpty());
            Assertions.assertTrue(lastParam.isHas_more());
        }
    }

    @Test
    public void testComputeScanRangeEmptyBucket() throws StarRocksException {
        // Setup test data
        final int bucketNum = 6;
        final int scanRangesPerBucket = 2;
        final Set<Long> backendIds = Set.of(1L, 2L, 3L, 4L);

        // Create test objects
        List<TScanRangeLocations> locations = genScanRangeLocations(bucketNum - 1, scanRangesPerBucket);
        ColocatedBackendSelector.Assignment colocatedAssignment = genColocatedAssignment(bucketNum, 1);
        WorkerProvider workerProvider = genWorkerProvider(backendIds);

        new Expectations() {
            {
                scanNode.getId();
                result = new PlanNodeId(1);
            }
        };

        // Create and test BucketAwareBackendSelector
        BucketAwareBackendSelector selector = new BucketAwareBackendSelector(
                scanNode, locations, colocatedAssignment, workerProvider, true, false, SessionVariableConstants.ELASTIC);

        // Execute the method under test
        selector.computeScanRangeAssignment();

        // Verify results
        Map<Integer, Long> seqToWorkerId = colocatedAssignment.getSeqToWorkerId();
        ColocatedBackendSelector.BucketSeqToScanRange seqToScanRange = colocatedAssignment.getSeqToScanRange();

        // Check that all buckets are assigned to workers
        Assertions.assertEquals(bucketNum, seqToWorkerId.size());

        // Check that scan ranges are properly distributed
        Assertions.assertTrue(seqToScanRange.get(5).containsKey(1));
        Assertions.assertEquals(0, seqToScanRange.get(5).get(1).size());
    }

    @Test
    public void testComputeScanRangeAssignmentWithNoWorkers() {
        // Setup test data with no workers
        final int bucketNum = 2;
        final int scanRangesPerBucket = 1;
        final Set<Long> backendIds = Set.of(); // Empty set

        List<TScanRangeLocations> locations = genScanRangeLocations(bucketNum, scanRangesPerBucket);
        ColocatedBackendSelector.Assignment colocatedAssignment = genColocatedAssignment(bucketNum, 1);
        WorkerProvider workerProvider = genWorkerProvider(backendIds);

        // Create BucketAwareBackendSelector
        BucketAwareBackendSelector selector = new BucketAwareBackendSelector(
                scanNode, locations, colocatedAssignment, workerProvider, false, false, SessionVariableConstants.BALANCE);

        // Execute and expect exception
        Assertions.assertThrows(StarRocksException.class, selector::computeScanRangeAssignment);
    }

    @Test
    public void testIncrementalBatchAccumulatesStaleEntries() throws StarRocksException {
        final int bucketNum = 6;

        ColocatedBackendSelector.Assignment assignment = genColocatedAssignment(bucketNum, 1);
        WorkerProvider wp = genWorkerProvider(Set.of(1L));

        List<TScanRangeLocations> locations = genScanRangeLocations(bucketNum, 1);
        List<TScanRangeLocations> onlyOne = genScanRangeLocations(1, 1);

        ConnectContext connectContext = new ConnectContext();
        connectContext.setExecutionId(new TUniqueId());

        new Expectations() {
            {
                scanNode.hasMoreScanRanges();
                result = true;
            }
        };

        new MockUp<CoordinatorPreprocessor>() {
            @Mock
            public void $init(ConnectContext context, JobSpec jobSpec, boolean enablePhasedSchedule) {
            }
        };

        final FragmentScanRangeAssignment fragmentScanRangeAssignment = new FragmentScanRangeAssignment();

        new MockUp<PlanFragment>() {
            @Mock
            public void $init(PlanFragmentId id, PlanNode root, DataPartition partition) {
            }

            @Mock
            public void disablePhysicalPropertyOptimize() {
            }
        };

        PlanFragment planFragment = new PlanFragment(null, scanNode, null);

        long id = wp.selectNextWorker();
        ComputeNode worker = wp.getWorkerById(id);
        FragmentInstance instance = new FragmentInstance(worker, null);

        new MockUp<ExecutionFragment>() {
            @Mock
            public void $init(ExecutionDAG executionDAG, PlanFragment planFragment, int fragmentIndex) {
            }

            @Mock
            public List<FragmentInstance> getInstances() {
                return List.of(instance);
            }

            @Mock
            public FragmentScanRangeAssignment getScanRangeAssignment() {
                return fragmentScanRangeAssignment;
            }

            @Mock
            public ColocatedBackendSelector.Assignment getColocatedAssignment() {
                return assignment;
            }

            @Mock
            public Collection<ScanNode> getScanNodes() {
                return List.of(scanNode);
            }

            @Mock
            public boolean isColocated() {
                return true;
            }

            @Mock
            public boolean isLocalBucketShuffleJoin() {
                return false;
            }

            @Mock
            public PlanFragment getPlanFragment() {
                return planFragment;
            }

        };

        new MockUp<FragmentAssignmentStrategyFactory>() {
            @Mock
            public void $init(ConnectContext connectContext, JobSpec jobSpec, ExecutionDAG executionDAG) {
            }

            @Mock
            public FragmentAssignmentStrategy create(ExecutionFragment execFragment, WorkerProvider workerProvider) {
                return new LocalFragmentAssignmentStrategy(null, workerProvider, true, false, true);
            }
        };

        LazyWorkerProvider lazyWorkerProvider = new LazyWorkerProvider(() -> wp);

        final boolean[] flag = {false};

        new MockUp<BackendSelectorFactory>() {
            @Mock
            public BackendSelector create(ScanNode scanNode,
                                          boolean isLoadType,
                                          ExecutionFragment execFragment,
                                          WorkerProvider workerProvider,
                                          ConnectContext connectContext,
                                          Set<Integer> destReplicatedScanIds,
                                          boolean useIncrementalScanRanges) {

                List<TScanRangeLocations> scanRanges;
                if (!flag[0]) {
                    flag[0] = true;
                    scanRanges = locations;
                } else {
                    scanRanges = onlyOne;
                }
                return new BucketAwareBackendSelector(
                        scanNode, scanRanges, assignment, lazyWorkerProvider.get(),
                        false, true, SessionVariableConstants.BALANCE);
            }
        };

        CoordinatorPreprocessor preprocessor = new CoordinatorPreprocessor(null, null, false);

        FragmentAssignmentStrategyFactory factory = new FragmentAssignmentStrategyFactory(null, null, null);


        Deencapsulation.setField(preprocessor, "fragmentAssignmentStrategyFactory", factory);
        Deencapsulation.setField(preprocessor, "lazyWorkerProvider", lazyWorkerProvider);

        ExecutionFragment executionFragment = new ExecutionFragment(null, null, 0);
        preprocessor.assignIncrementalScanRangesToFragmentInstances(executionFragment);

        long scanRanges;
        scanRanges = assignment.getSeqToScanRange().values().stream().flatMap(i -> i.values().
                stream()).flatMap(Collection::stream).filter(r -> !r.empty).count();
        Assertions.assertEquals(6, scanRanges);

        preprocessor.assignIncrementalScanRangesToFragmentInstances(executionFragment);
        scanRanges = assignment.getSeqToScanRange().values().
                stream().flatMap(i -> i.values().stream()).flatMap(Collection::stream).filter(r -> !r.empty).count();
        Assertions.assertEquals(1, scanRanges);


    }

}
