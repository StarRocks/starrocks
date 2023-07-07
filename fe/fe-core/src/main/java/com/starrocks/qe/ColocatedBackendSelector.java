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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Reference;
import com.starrocks.common.UserException;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColocatedBackendSelector implements BackendSelector {

    private final OlapScanNode scanNode;
    private final FragmentScanRangeAssignment assignment;
    private final Assignment colocatedAssignment;
    private final boolean isRightOrFullBucketShuffleFragment;
    private final WorkerProvider workerProvider;

    public ColocatedBackendSelector(OlapScanNode scanNode, FragmentScanRangeAssignment assignment,
                                    Assignment colocatedAssignment,
                                    boolean isRightOrFullBucketShuffleFragment, WorkerProvider workerProvider) {
        this.scanNode = scanNode;
        this.assignment = assignment;
        this.colocatedAssignment = colocatedAssignment;
        this.isRightOrFullBucketShuffleFragment = isRightOrFullBucketShuffleFragment;
        this.workerProvider = workerProvider;
    }

    @Override
    public void computeScanRangeAssignment() throws UserException {
        Map<Integer, TNetworkAddress> bucketSeqToAddress = colocatedAssignment.seqToAddress;
        BucketSeqToScanRange bucketSeqToScanRange = colocatedAssignment.seqToScanRange;

        for (Integer bucketSeq : scanNode.bucketSeq2locations.keySet()) {
            List<TScanRangeLocations> locations = scanNode.bucketSeq2locations.get(bucketSeq);
            if (!bucketSeqToAddress.containsKey(bucketSeq)) {
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
                if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                    Reference<Long> backendIdRef = new Reference<>();
                    TNetworkAddress addr = workerProvider.chooseNextWorker(backendIdRef);
                    bucketSeqToAddress.put(bucketSeq, addr);
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
                assignment.putAll(bucketSeqToAddress.get(seq), scanNodeId, nodeId2ScanRanges.get(scanNodeId));
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
            if (!workerProvider.containsBackend(location.getBackend_id())) {
                continue;
            }

            Integer bucketNum = buckendIdToBucketCountMap.getOrDefault(location.backend_id, 0);
            if (bucketNum < minBucketNum) {
                minBucketNum = bucketNum;
                minBackendId = location.backend_id;
            }
        }

        if (minBackendId == Long.MAX_VALUE) {
            workerProvider.reportBackendNotFoundException();
        }

        buckendIdToBucketCountMap.put(minBackendId, minBucketNum + 1);
        TNetworkAddress execHostPort = workerProvider.chooseBackend(minBackendId);
        colocatedAssignment.seqToAddress.put(bucketSeq, execHostPort);
    }

    public static class BucketSeqToScanRange
            extends HashMap<Integer, Map<Integer, List<TScanRangeParams>>> {
    }

    public static class Assignment {
        private final Map<Integer, TNetworkAddress> seqToAddress = Maps.newHashMap();
        // < bucket_seq -> < scannode_id -> scan_range_params >>
        private final BucketSeqToScanRange seqToScanRange = new BucketSeqToScanRange();
        private final Map<Long, Integer> backendIdToBucketCount = Maps.newHashMap();
        private final int bucketNum;

        public Assignment(OlapScanNode scanNode) {
            int bucketNum = scanNode.getOlapTable().getDefaultDistributionInfo().getBucketNum();
            if (scanNode.getSelectedPartitionIds().size() <= 1) {
                for (Long pid : scanNode.getSelectedPartitionIds()) {
                    bucketNum = scanNode.getOlapTable().getPartition(pid).getDistributionInfo().getBucketNum();
                }
            }

            this.bucketNum = bucketNum;
        }

        public Map<Integer, TNetworkAddress> getSeqToAddress() {
            return seqToAddress;
        }

        public BucketSeqToScanRange getSeqToScanRange() {
            return seqToScanRange;
        }

        public int getBucketNum() {
            return bucketNum;
        }
    }
}
