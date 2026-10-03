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
import com.google.common.hash.Hashing;
import com.starrocks.catalog.LanceTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.qe.scheduler.DefaultSharedDataWorkerProvider;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.LanceScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.scheduler.DefaultWorkerProvider;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.assignment.BackendSelectorFactory;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TScanRangeParams;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LanceBackendSelectorTest {
    private static Stream<Arguments> schedulingCases() {
        return Stream.of("backend", "compute", "mixed", "shared-data").flatMap(mode ->
                Stream.of(false, true).flatMap(local -> Stream.of(false, true)
                        .map(incremental -> Arguments.of(mode, local, incremental))));
    }

    private static ImmutableMap<Long, ComputeNode> workers(long firstId) {
        ComputeNode first = new ComputeNode(firstId, "worker-" + firstId, 9050);
        ComputeNode second = new ComputeNode(firstId + 1, "worker-" + (firstId + 1), 9050);
        first.setBePort(9060);
        second.setBePort(9060);
        first.setAlive(true);
        second.setAlive(true);
        return ImmutableMap.of(firstId, first, firstId + 1, second);
    }

    private static LanceScanNode newScan(String uri) {
        TupleDescriptor tuple = new DescriptorTable().createTupleDescriptor();
        tuple.setTable(new LanceTable(1, "vectors", List.of(), uri));
        LanceScanNode scan = new LanceScanNode(new PlanNodeId(0), tuple, "LanceScanNode") {
            @Override
            public List<Long> getAllAvailableBackendOrComputeIds() {
                return List.of(11L, 12L);
            }
        };
        scan.setupScanRangeLocations();
        return scan;
    }

    private static ConnectContext context(boolean forceLocal) {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setEnableDataCacheSharing(false);
        context.getSessionVariable().setForceScheduleLocal(forceLocal);
        return context;
    }

    @ParameterizedTest
    @MethodSource("schedulingCases")
    public void testFactoryRotatesWholeDatasetAcrossQueries(String mode, boolean forceLocal, boolean incremental)
            throws Exception {
        ImmutableMap<Long, ComputeNode> backends = mode.equals("compute") ? ImmutableMap.of() : workers(11);
        ImmutableMap<Long, ComputeNode> compute = mode.equals("backend") ? ImmutableMap.of() : workers(21);
        Set<Long> eligible = mode.equals("backend") ? backends.keySet() : compute.keySet();
        Map<Long, Integer> assignedQueries = new HashMap<>();
        // Each query gets a fresh provider; rotation must not restart at the first worker.
        for (int query = 0; query < 4; query++) {
            WorkerProvider provider = mode.equals("shared-data")
                    ? new DefaultSharedDataWorkerProvider(compute, compute, WarehouseManager.DEFAULT_RESOURCE)
                    : new DefaultWorkerProvider(backends, compute, backends, compute, false,
                            WarehouseManager.DEFAULT_RESOURCE);
            LanceScanNode scan = newScan("s3://bucket/vectors.lance");
            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            ExecutionFragment fragment = mock(ExecutionFragment.class);
            when(fragment.getScanRangeAssignment()).thenReturn(assignment);
            BackendSelector selector = BackendSelectorFactory.create(scan, false, fragment, provider,
                    context(forceLocal), new HashSet<>(), incremental);
            Assertions.assertInstanceOf(HDFSBackendSelector.class, selector);
            selector.computeScanRangeAssignment();
            int datasetRanges = 0;
            for (Map.Entry<Long, Map<Integer, List<TScanRangeParams>>> entry : assignment.entrySet()) {
                Assertions.assertTrue(eligible.contains(entry.getKey()));
                for (TScanRangeParams params : entry.getValue().get(scan.getId().asInt())) {
                    if (params.isEmpty()) {
                        Assertions.assertFalse(params.isHas_more());
                    } else {
                        datasetRanges++;
                        Assertions.assertTrue(provider.isWorkerSelected(entry.getKey()));
                        Assertions.assertEquals("s3://bucket/vectors.lance",
                                params.getScan_range().getHdfs_scan_range().getFull_path());
                        assignedQueries.merge(entry.getKey(), 1, Integer::sum);
                    }
                }
            }
            Assertions.assertEquals(1, datasetRanges);
        }
        Assertions.assertEquals(eligible, assignedQueries.keySet());
        assignedQueries.values().forEach(count -> Assertions.assertEquals(2, count.intValue()));
    }

    @Test
    public void testNoEligibleWorkerFailsDuringScheduling() {
        WorkerProvider provider = new DefaultWorkerProvider(ImmutableMap.of(), ImmutableMap.of(),
                ImmutableMap.of(), ImmutableMap.of(), false, WarehouseManager.DEFAULT_RESOURCE);
        LanceScanNode scan = newScan("s3://bucket/vectors.lance");
        HDFSBackendSelector selector = new HDFSBackendSelector(scan, scan.getScanRangeLocations(0),
                new FragmentScanRangeAssignment(), provider, false, false, false, context(false));
        Assertions.assertThrows(StarRocksException.class, selector::computeScanRangeAssignment);
    }

    @Test
    public void testHasherUsesDatasetIdentity() {
        WorkerProvider provider = new DefaultWorkerProvider(workers(11), ImmutableMap.of(), workers(11),
                ImmutableMap.of(), false, WarehouseManager.DEFAULT_RESOURCE);
        LanceScanNode first = newScan("s3://bucket/first.lance");
        LanceScanNode second = newScan("s3://bucket/second.lance");
        HDFSBackendSelector selector = new HDFSBackendSelector(first, first.getScanRangeLocations(0),
                new FragmentScanRangeAssignment(), provider, false, false, false, context(false));
        HDFSBackendSelector.HdfsScanRangeHasher hasher = selector.new HdfsScanRangeHasher();
        var firstHash = Hashing.murmur3_128().newHasher();
        var secondHash = Hashing.murmur3_128().newHasher();
        hasher.acceptScanRangeLocations(first.getScanRangeLocations(0).get(0), firstHash);
        hasher.acceptScanRangeLocations(second.getScanRangeLocations(0).get(0), secondHash);
        Assertions.assertNotEquals(firstHash.hash(), secondHash.hash());
    }
}
