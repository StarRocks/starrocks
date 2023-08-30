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

import com.google.api.client.util.Sets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.UserException;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

public class ColocatedBackendSelector implements BackendSelector {

    private final OlapScanNode scanNode;
    private final FragmentScanRangeAssignment assignment;
    private final ColocatedBackendSelector.Assignment colocatedAssignment;
    private final boolean isRightOrFullBucketShuffleFragment;
    private final WorkerProvider workerProvider;
    private final BucketIterator bucketIterator;

    public ColocatedBackendSelector(OlapScanNode scanNode, FragmentScanRangeAssignment assignment,
                                    ColocatedBackendSelector.Assignment colocatedAssignment,
                                    boolean isRightOrFullBucketShuffleFragment, WorkerProvider workerProvider,
                                    int maxBucketsPerBeToUseBalancerAssignment) {
        this.scanNode = scanNode;
        this.assignment = assignment;
        this.colocatedAssignment = colocatedAssignment;
        this.isRightOrFullBucketShuffleFragment = isRightOrFullBucketShuffleFragment;
        this.workerProvider = workerProvider;
        this.bucketIterator = createBucketIterator(scanNode, maxBucketsPerBeToUseBalancerAssignment);
    }

    @Override
    public void computeScanRangeAssignment() throws UserException {
        Map<Integer, Long> bucketSeqToWorkerId = colocatedAssignment.seqToWorkerId;
        ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange = colocatedAssignment.seqToScanRange;

        Iterable<Integer> bucketSeqs = () -> bucketIterator;
        for (Integer bucketSeq : bucketSeqs) {
            List<TScanRangeLocations> locations = scanNode.bucketSeq2locations.get(bucketSeq);
            if (!bucketSeqToWorkerId.containsKey(bucketSeq)) {
                computeExecAddressForBucketSeq(locations.get(0), bucketSeq);
            }

            List<TScanRangeParams> scanRangeParamsList =
                    bucketSeqToScanRange
                            .computeIfAbsent(bucketSeq, k -> Maps.newHashMap())
                            .computeIfAbsent(scanNode.getId().asInt(), k -> Lists.newArrayList());
            locations.stream()
                    .map(location -> new TScanRangeParams(location.scan_range))
                    .forEach(scanRangeParamsList::add);
        }
        // Because of the right table will not send data to the bucket which has been pruned, the right join or full join will get wrong result.
        // So if this bucket shuffle is right join or full join, we need to add empty bucket scan range which is pruned by predicate.
        if (isRightOrFullBucketShuffleFragment) {
            int bucketNum = colocatedAssignment.bucketNum;

            for (int bucketSeq = 0; bucketSeq < bucketNum; ++bucketSeq) {
                if (!bucketSeqToWorkerId.containsKey(bucketSeq)) {
                    long workerId = workerProvider.selectNextWorker();
                    bucketSeqToWorkerId.put(bucketSeq, workerId);
                }
                if (!bucketSeqToScanRange.containsKey(bucketSeq)) {
                    bucketSeqToScanRange.put(bucketSeq, Maps.newHashMap());
                    bucketSeqToScanRange.get(bucketSeq).put(scanNode.getId().asInt(), Lists.newArrayList());
                }
            }
        }

        // use bucketSeqToScanRange to fill FragmentScanRangeAssignment
        bucketSeqToScanRange.forEach((seq, nodeId2ScanRanges) -> {
            // fill FragmentScanRangeAssignment only when there are scan id in the bucket
            int scanNodeId = scanNode.getId().asInt();
            if (nodeId2ScanRanges.containsKey(scanNodeId)) {
                assignment.putAll(bucketSeqToWorkerId.get(seq), scanNodeId, nodeId2ScanRanges.get(scanNodeId));
            }
        });
    }

    // Make sure each host have average bucket to scan
    private void computeExecAddressForBucketSeq(TScanRangeLocations seqLocation, Integer bucketSeq)
            throws UserException {
        Map<Long, Integer> buckendIdToBucketCountMap = colocatedAssignment.backendIdToBucketCount;
        int minBucketNum = Integer.MAX_VALUE;
        long minBackendId = Long.MAX_VALUE;
        for (TScanRangeLocation location : seqLocation.locations) {
            if (!workerProvider.isDataNodeAvailable(location.getBackend_id())) {
                continue;
            }

            Integer bucketNum = buckendIdToBucketCountMap.getOrDefault(location.backend_id, 0);
            if (bucketNum < minBucketNum) {
                minBucketNum = bucketNum;
                minBackendId = location.backend_id;
            }
        }

        if (minBackendId == Long.MAX_VALUE) {
            workerProvider.reportDataNodeNotFoundException();
        }

        buckendIdToBucketCountMap.put(minBackendId, minBucketNum + 1);
        workerProvider.selectWorker(minBackendId);
        colocatedAssignment.seqToWorkerId.put(bucketSeq, minBackendId);
        bucketIterator.useBackend(minBackendId);
    }

    public static class BucketSeqToScanRange
            extends HashMap<Integer, Map<Integer, List<TScanRangeParams>>> {
    }

    public static class Assignment {
        private final Map<Integer, Long> seqToWorkerId = Maps.newHashMap();
        // < bucket_seq -> < scannode_id -> scan_range_params >>
        private final ColocatedBackendSelector.BucketSeqToScanRange seqToScanRange =
                new ColocatedBackendSelector.BucketSeqToScanRange();
        private final Map<Long, Integer> backendIdToBucketCount = Maps.newHashMap();
        private final int bucketNum;

        public Assignment(OlapScanNode scanNode) {
            int curBucketNum = scanNode.getOlapTable().getDefaultDistributionInfo().getBucketNum();
            if (scanNode.getSelectedPartitionIds().size() <= 1) {
                for (Long pid : scanNode.getSelectedPartitionIds()) {
                    curBucketNum = scanNode.getOlapTable().getPartition(pid).getDistributionInfo().getBucketNum();
                }
            }

            this.bucketNum = curBucketNum;
        }

        public Map<Integer, Long> getSeqToWorkerId() {
            return seqToWorkerId;
        }

        public ColocatedBackendSelector.BucketSeqToScanRange getSeqToScanRange() {
            return seqToScanRange;
        }

        public int getBucketNum() {
            return bucketNum;
        }
    }

    private static BucketIterator createBucketIterator(OlapScanNode scanNode, int maxBucketsPerBeToUseBalancerAssignment) {
        if (maxBucketsPerBeToUseBalancerAssignment <= 0) {
            return new NormalBucketIterator(scanNode);
        }

        int numTotalBuckets = 0;
        Set<Long> backends = Sets.newHashSet();
        for (Integer bucket : scanNode.bucketSeq2locations.keySet()) {
            List<TScanRangeLocations> bucketLocations = scanNode.bucketSeq2locations.get(bucket);
            if (bucketLocations.isEmpty()) {
                continue;
            }

            for (TScanRangeLocations locations : bucketLocations) {
                for (TScanRangeLocation location : locations.getLocations()) {
                    backends.add(location.getBackend_id());
                }
            }

            numTotalBuckets += bucketLocations.get(0).getLocationsSize();
        }

        if (numTotalBuckets / backends.size() <= maxBucketsPerBeToUseBalancerAssignment) {
            return new BalancerBucketIterator(scanNode);
        } else {
            return new NormalBucketIterator(scanNode);
        }
    }

    private interface BucketIterator extends Iterator<Integer> {
        void useBackend(Long backend);
    }

    private static class NormalBucketIterator implements BucketIterator {
        private final Iterator<Integer> iterator;

        public NormalBucketIterator(OlapScanNode scanNode) {
            this.iterator = scanNode.bucketSeq2locations.keySet().iterator();
        }

        @Override
        public void useBackend(Long backend) {
            // Do nothing.
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Integer next() {
            return iterator.next();
        }
    }

    private static class BalancerBucketIterator implements BucketIterator {
        private final Map<Long, Set<Integer>> backendToBuckets;
        private final Map<Integer, Integer> bucketToBackendUsedTimes;
        private final NavigableSet<Integer> buckets;

        public BalancerBucketIterator(OlapScanNode scanNode) {
            ArrayListMultimap<Integer, TScanRangeLocations> bucketToLocations = scanNode.bucketSeq2locations;

            this.backendToBuckets = Maps.newHashMap();
            this.bucketToBackendUsedTimes = Maps.newHashMap();
            this.buckets = new TreeSet<>(Comparator
                    .comparing((Function<Integer, Integer>) bucketToBackendUsedTimes::get)
                    .thenComparing(Function.identity()));

            for (Integer bucket : bucketToLocations.keySet()) {
                this.bucketToBackendUsedTimes.put(bucket, 0);
                this.buckets.add(bucket);

                List<TScanRangeLocations> bucketLocations = bucketToLocations.get(bucket);
                if (bucketLocations.isEmpty()) {
                    continue;
                }
                for (TScanRangeLocation location : bucketLocations.get(0).getLocations()) {
                    backendToBuckets.computeIfAbsent(location.getBackend_id(), k -> new HashSet<>()).add(bucket);
                }
            }
        }

        @Override
        public void useBackend(Long backend) {
            Set<Integer> bucketsOfBackend = backendToBuckets.get(backend);
            if (bucketsOfBackend == null) {
                return;
            }

            for (Integer bucket : bucketsOfBackend) {
                if (!buckets.remove(bucket)) {
                    continue;
                }

                bucketToBackendUsedTimes.compute(bucket, (k, prevTimes) -> {
                    if (prevTimes == null) {
                        return 1;
                    }
                    return prevTimes + 1;
                });
                buckets.add(bucket);
            }
        }

        @Override
        public boolean hasNext() {
            return !buckets.isEmpty();
        }

        @Override
        public Integer next() {
            return buckets.pollLast();
        }
    }

}
