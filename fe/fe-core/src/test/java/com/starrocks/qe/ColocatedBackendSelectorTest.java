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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.exception.UserException;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.qe.scheduler.DefaultWorkerProvider;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class ColocatedBackendSelectorTest {
    @Test
    public void testSingleScanNodeWithEmptyReplication() {
        final int numBuckets = 4;
        final Map<Integer, List<Long>> bucketSeqToBackends = ImmutableMap.of(
                0, ImmutableList.of(),
                1, ImmutableList.of(2L),
                2, ImmutableList.of(3L),
                3, ImmutableList.of(4L)
        );
        final Set<Long> backendIds =
                bucketSeqToBackends.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());

        OlapScanNode scanNode = genOlapScanNode(0, numBuckets);
        scanNode.bucketSeq2locations = genBucketSeq2Locations(bucketSeqToBackends, 3);
        WorkerProvider workerProvider = genWorkerProvider(backendIds);

        {
            int maxBucketsPerBeToUseBalancerAssignment = 5;
            Assert.assertThrows("Backend node not found", NonRecoverableException.class,
                    () -> checkColocatedAssignment(scanNode, workerProvider, maxBucketsPerBeToUseBalancerAssignment, null));
        }

        {
            int maxBucketsPerBeToUseBalancerAssignment = 0;
            Assert.assertThrows("Backend node not found", NonRecoverableException.class,
                    () -> checkColocatedAssignment(scanNode, workerProvider, maxBucketsPerBeToUseBalancerAssignment, null));
        }
    }

    @Test
    public void testSingleScanNodeWithSingleReplication() throws UserException {
        final int numBuckets = 4;
        final Map<Integer, List<Long>> bucketSeqToBackends = ImmutableMap.of(
                0, ImmutableList.of(1L),
                1, ImmutableList.of(2L),
                2, ImmutableList.of(3L),
                3, ImmutableList.of(4L)
        );
        final Set<Long> backendIds =
                bucketSeqToBackends.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());

        OlapScanNode scanNode = genOlapScanNode(0, numBuckets);
        scanNode.bucketSeq2locations = genBucketSeq2Locations(bucketSeqToBackends, 3);
        WorkerProvider workerProvider = genWorkerProvider(backendIds);

        {
            int maxBucketsPerBeToUseBalancerAssignment = 5;
            Map<Integer, Long> expectedSeqToBackendId = ImmutableMap.of(
                    0, 1L,
                    1, 2L,
                    2, 3L,
                    3, 4L
            );
            checkColocatedAssignment(scanNode, workerProvider, maxBucketsPerBeToUseBalancerAssignment, expectedSeqToBackendId);
        }

        {
            int maxBucketsPerBeToUseBalancerAssignment = 1;
            Map<Integer, Long> expectedSeqToBackendId = ImmutableMap.of(
                    0, 1L,
                    1, 2L,
                    2, 3L,
                    3, 4L
            );
            checkColocatedAssignment(scanNode, workerProvider, maxBucketsPerBeToUseBalancerAssignment, expectedSeqToBackendId);
        }

        {
            int maxBucketsPerBeToUseBalancerAssignment = -1;
            Map<Integer, Long> expectedSeqToBackendId = ImmutableMap.of(
                    0, 1L,
                    1, 2L,
                    2, 3L,
                    3, 4L
            );
            checkColocatedAssignment(scanNode, workerProvider, maxBucketsPerBeToUseBalancerAssignment, expectedSeqToBackendId);
        }
    }

    @Test
    public void testSingleScanNode() throws UserException {
        final int numBuckets = 4;
        final Map<Integer, List<Long>> bucketSeqToBackends = ImmutableMap.of(
                0, ImmutableList.of(1L),
                1, ImmutableList.of(4L, 2L),
                2, ImmutableList.of(3L, 2L),
                3, ImmutableList.of(4L, 1L)
        );
        final Set<Long> backendIds =
                bucketSeqToBackends.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());

        OlapScanNode scanNode = genOlapScanNode(0, numBuckets);
        scanNode.bucketSeq2locations = genBucketSeq2Locations(bucketSeqToBackends, 3);
        WorkerProvider workerProvider = genWorkerProvider(backendIds);

        {
            int maxBucketsPerBeToUseBalancerAssignment = 5;
            Map<Integer, Long> expectedSeqToBackendId = ImmutableMap.of(
                    0, 1L,
                    1, 2L,
                    2, 3L,
                    3, 4L
            );
            checkColocatedAssignment(scanNode, workerProvider, maxBucketsPerBeToUseBalancerAssignment, expectedSeqToBackendId);
        }

        {
            int maxBucketsPerBeToUseBalancerAssignment = 2;
            Map<Integer, Long> expectedSeqToBackendId = ImmutableMap.of(
                    0, 1L,
                    1, 2L,
                    2, 3L,
                    3, 4L
            );
            checkColocatedAssignment(scanNode, workerProvider, maxBucketsPerBeToUseBalancerAssignment, expectedSeqToBackendId);
        }

        {
            int maxBucketsPerBeToUseBalancerAssignment = 1;
            Map<Integer, Long> expectedSeqToBackendId = ImmutableMap.of(
                    0, 1L,
                    1, 4L,
                    2, 3L,
                    3, 4L
            );
            checkColocatedAssignment(scanNode, workerProvider, maxBucketsPerBeToUseBalancerAssignment, expectedSeqToBackendId);
        }

        {
            int maxBucketsPerBeToUseBalancerAssignment = -1;
            Map<Integer, Long> expectedSeqToBackendId = ImmutableMap.of(
                    0, 1L,
                    1, 4L,
                    2, 3L,
                    3, 4L
            );
            checkColocatedAssignment(scanNode, workerProvider, maxBucketsPerBeToUseBalancerAssignment, expectedSeqToBackendId);
        }
    }

    @Test
    public void testMultipleScanNodes() throws UserException {
        final int numBuckets = 4;
        final Map<Integer, List<Long>> bucketSeqToBackends0 = ImmutableMap.of(
                0, ImmutableList.of(1L, 3L),
                1, ImmutableList.of(4L, 2L),
                3, ImmutableList.of(4L, 1L)
        );
        final Map<Integer, List<Long>> bucketSeqToBackends1 = ImmutableMap.of(
                0, ImmutableList.of(1L, 3L),
                1, ImmutableList.of(4L, 2L),
                2, ImmutableList.of(3L, 2L),
                3, ImmutableList.of(4L, 1L)
        );
        final Set<Long> backendIds = Stream.concat(bucketSeqToBackends0.values().stream(), bucketSeqToBackends1.values().stream())
                .flatMap(Collection::stream).collect(Collectors.toSet());

        OlapScanNode scanNode0 = genOlapScanNode(0, numBuckets);
        scanNode0.bucketSeq2locations = genBucketSeq2Locations(bucketSeqToBackends0, 3);
        OlapScanNode scanNode1 = genOlapScanNode(1, numBuckets);
        scanNode1.bucketSeq2locations = genBucketSeq2Locations(bucketSeqToBackends1, 4);
        List<OlapScanNode> scanNodes = ImmutableList.of(scanNode0, scanNode1);

        WorkerProvider workerProvider = genWorkerProvider(backendIds);

        {
            int maxBucketsPerBeToUseBalancerAssignment = 5;
            Map<Integer, Long> expectedSeqToBackendId = ImmutableMap.of(
                    0, 1L,
                    1, 2L,
                    2, 3L,
                    3, 4L
            );
            checkColocatedAssignment(scanNodes, workerProvider, maxBucketsPerBeToUseBalancerAssignment, expectedSeqToBackendId);
        }

        {
            int maxBucketsPerBeToUseBalancerAssignment = 2;
            Map<Integer, Long> expectedSeqToBackendId = ImmutableMap.of(
                    0, 1L,
                    1, 2L,
                    2, 3L,
                    3, 4L
            );
            checkColocatedAssignment(scanNodes, workerProvider, maxBucketsPerBeToUseBalancerAssignment, expectedSeqToBackendId);
        }

        {
            int maxBucketsPerBeToUseBalancerAssignment = 1;
            Map<Integer, Long> expectedSeqToBackendId = ImmutableMap.of(
                    0, 1L,
                    1, 4L,
                    2, 3L,
                    3, 4L
            );
            checkColocatedAssignment(scanNodes, workerProvider, maxBucketsPerBeToUseBalancerAssignment, expectedSeqToBackendId);
        }

        {
            int maxBucketsPerBeToUseBalancerAssignment = -1;
            Map<Integer, Long> expectedSeqToBackendId = ImmutableMap.of(
                    0, 1L,
                    1, 4L,
                    2, 3L,
                    3, 4L
            );
            checkColocatedAssignment(scanNodes, workerProvider, maxBucketsPerBeToUseBalancerAssignment, expectedSeqToBackendId);
        }
    }

    private void checkColocatedAssignment(OlapScanNode scanNode, WorkerProvider workerProvider,
                                          int maxBucketsPerBeToUseBalancerAssignment,
                                          Map<Integer, Long> expectedSeqToBackendId) throws UserException {
        checkColocatedAssignment(Collections.singletonList(scanNode), workerProvider, maxBucketsPerBeToUseBalancerAssignment,
                expectedSeqToBackendId);
    }

    private void checkColocatedAssignment(List<OlapScanNode> scanNodes, WorkerProvider workerProvider,
                                          int maxBucketsPerBeToUseBalancerAssignment,
                                          Map<Integer, Long> expectedSeqToBackendId)
            throws UserException {
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        ColocatedBackendSelector.Assignment colocatedAssignemnt = new ColocatedBackendSelector.Assignment(scanNodes.get(0));

        for (OlapScanNode scanNode : scanNodes) {
            ColocatedBackendSelector backendSelector =
                    new ColocatedBackendSelector(scanNode, assignment, colocatedAssignemnt, false,
                            workerProvider, maxBucketsPerBeToUseBalancerAssignment);
            backendSelector.computeScanRangeAssignment();
        }

        assertThat(colocatedAssignemnt.getSeqToWorkerId()).containsExactlyInAnyOrderEntriesOf(expectedSeqToBackendId);
    }

    private OlapScanNode genOlapScanNode(int id, int numBuckets) {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        OlapTable table = new OlapTable();
        table.setDefaultDistributionInfo(new HashDistributionInfo(numBuckets, Collections.emptyList()));
        desc.setTable(table);

        return new OlapScanNode(new PlanNodeId(id), desc, "OlapScanNode");
    }

    private ArrayListMultimap<Integer, TScanRangeLocations> genBucketSeq2Locations(Map<Integer, List<Long>> bucketSeqToBackends,
                                                                                   int numTabletsPerBucket) {
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

    private WorkerProvider genWorkerProvider(Set<Long> backendIds) {
        ImmutableMap<Long, ComputeNode> id2Backend = ImmutableMap.copyOf(backendIds.stream().collect(Collectors.toMap(
                Function.identity(), backendId -> new ComputeNode(backendId, "host", 9030)
        )));
        ImmutableMap<Long, ComputeNode> id2ComputeNode = ImmutableMap.of();

        return new DefaultWorkerProvider(id2Backend, id2ComputeNode, id2Backend, id2ComputeNode, false);
    }

}
