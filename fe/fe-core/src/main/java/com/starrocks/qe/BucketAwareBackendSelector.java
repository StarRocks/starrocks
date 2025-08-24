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

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.HashRing;
import com.starrocks.common.util.RendezvousHashRing;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BucketAwareBackendSelector implements BackendSelector {
    public static final Logger LOG = LogManager.getLogger(BucketAwareBackendSelector.class);

    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final ColocatedBackendSelector.Assignment colocatedAssignment;
    private final WorkerProvider workerProvider;
    private final boolean isRightOrFullBucketShuffleFragment;
    private final boolean useIncrementalScanRanges;
    private final String assignMode;

    public BucketAwareBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                                      ColocatedBackendSelector.Assignment colocatedAssignment,
                                      WorkerProvider workerProvider, boolean isRightOrFullBucketShuffleFragment,
                                      boolean useIncrementalScanRanges, String assignMode) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.colocatedAssignment = colocatedAssignment;
        this.workerProvider = workerProvider;
        this.isRightOrFullBucketShuffleFragment = isRightOrFullBucketShuffleFragment;
        this.useIncrementalScanRanges = useIncrementalScanRanges;
        this.assignMode = assignMode;
    }

    static class BucketIdFunnel implements Funnel<Integer> {
        @Override
        public void funnel(Integer bucketId, PrimitiveSink primitiveSink) {
            primitiveSink.putInt(bucketId);
        }
    }

    private Map<Integer, Long> assignBucketToWorker() throws StarRocksException {
        Map<Integer, Long> bucketSeqToWorkerId = new HashMap<>();

        if (assignMode.equals(SessionVariableConstants.ELASTIC)) {
            // use consistent hashing to schedule remote scan ranges
            HashRing<Integer, ComputeNode> hashRing = new RendezvousHashRing<>(Hashing.murmur3_128(), new BucketIdFunnel(),
                    new HDFSBackendSelector.ComputeNodeFunnel(), workerProvider.getAllWorkers());
            // mapping bucket seq to worker id
            for (int i = 0; i < colocatedAssignment.getBucketNum(); i++) {
                List<ComputeNode> backends = hashRing.get(i, 1);
                if (backends.isEmpty()) {
                    throw new StarRocksException("Failed to find backend to execute");
                }
                bucketSeqToWorkerId.put(i, backends.get(0).getId());
            }
        } else {
            List<ComputeNode> workers = workerProvider.getAllWorkers().stream()
                    .sorted(Comparator.comparingLong(ComputeNode::getId)).toList();
            if (workers.isEmpty()) {
                throw new StarRocksException("Failed to find backend to execute");
            }
            for (int i = 0; i < colocatedAssignment.getBucketNum(); i++) {
                bucketSeqToWorkerId.put(i, workers.get(i % workers.size()).getId());
            }
        }

        return bucketSeqToWorkerId;
    }

    @Override
    public void computeScanRangeAssignment() throws StarRocksException {
        colocatedAssignment.recordAssignedScanNode(scanNode);
        Map<Integer, Long> bucketSeqToWorkerId = assignBucketToWorker();

        Map<Integer, List<TScanRangeParams>> bucketToScanRange = new HashMap<>();
        // split scan ranges.
        for (TScanRangeLocations scanRangeLocations : locations) {
            // add scan range params
            TScanRangeParams scanRangeParams = new TScanRangeParams();
            scanRangeParams.scan_range = scanRangeLocations.scan_range;
            int bucketId = scanRangeLocations.scan_range.hdfs_scan_range.bucket_id;
            if (!bucketToScanRange.containsKey(bucketId)) {
                bucketToScanRange.put(bucketId, new ArrayList<>());
            }
            bucketToScanRange.get(bucketId).add(scanRangeParams);
        }

        ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange = colocatedAssignment.getSeqToScanRange();
        Map<Integer, Long> seqToWorkerId = colocatedAssignment.getSeqToWorkerId();
        for (Map.Entry<Integer, List<TScanRangeParams>> entry : bucketToScanRange.entrySet()) {
            Map<Integer, List<TScanRangeParams>> scanRanges = bucketSeqToScanRange.computeIfAbsent(
                    entry.getKey(), k -> new HashMap<>());
            scanRanges.put(scanNode.getId().asInt(), entry.getValue());
            seqToWorkerId.put(entry.getKey(), bucketSeqToWorkerId.get(entry.getKey()));
        }

        if (useIncrementalScanRanges) {
            boolean hasMore = scanNode.hasMoreScanRanges();
            TScanRangeParams end = new TScanRangeParams();
            end.setScan_range(new TScanRange());
            end.setEmpty(true);
            end.setHas_more(hasMore);
            for (int bucketSeq = 0; bucketSeq < colocatedAssignment.getBucketNum(); ++bucketSeq) {
                if (!seqToWorkerId.containsKey(bucketSeq)) {
                    seqToWorkerId.put(bucketSeq, bucketSeqToWorkerId.get(bucketSeq));
                }
                if (!bucketSeqToScanRange.containsKey(bucketSeq)) {
                    bucketSeqToScanRange.put(bucketSeq, Maps.newHashMap());
                }
                if (!bucketSeqToScanRange.get(bucketSeq).containsKey(scanNode.getId().asInt())) {
                    bucketSeqToScanRange.get(bucketSeq).put(scanNode.getId().asInt(), Lists.newArrayList());
                }
                bucketSeqToScanRange.get(bucketSeq).get(scanNode.getId().asInt()).add(end);
            }
        } else if (isRightOrFullBucketShuffleFragment && colocatedAssignment.isAllScanNodesAssigned()) {
            // Because the right table will not send data to the bucket which has been pruned, the right join or full join will get wrong result.
            // Therefore, if this bucket shuffle is right join or full join, we need to add empty bucket scan range which is pruned by predicate,
            // after the last scan node of this fragment is assigned.
            for (int bucketSeq = 0; bucketSeq < colocatedAssignment.getBucketNum(); ++bucketSeq) {
                if (!seqToWorkerId.containsKey(bucketSeq)) {
                    seqToWorkerId.put(bucketSeq, bucketSeqToWorkerId.get(bucketSeq));
                }
                if (!bucketSeqToScanRange.containsKey(bucketSeq)) {
                    bucketSeqToScanRange.put(bucketSeq, Maps.newHashMap());
                    bucketSeqToScanRange.get(bucketSeq).put(scanNode.getId().asInt(), Lists.newArrayList());
                }
            }
        }
        recordScanRangeStatistic();
    }

    private void recordScanRangeStatistic() {
        // record bucket counts for each backend
        Map<Integer, Long> seqToWorkerId = colocatedAssignment.getSeqToWorkerId();
        Map<Long, Integer> workerIdSeqCount = new HashMap<>();
        for (Map.Entry<Integer, Long> entry : seqToWorkerId.entrySet()) {
            if (workerIdSeqCount.containsKey(entry.getValue())) {
                workerIdSeqCount.put(entry.getValue(), workerIdSeqCount.get(entry.getValue()) + 1);
            } else {
                workerIdSeqCount.put(entry.getValue(), 1);
            }
        }
        for (Map.Entry<Long, Integer> entry : workerIdSeqCount.entrySet()) {
            String host = workerProvider.getWorkerById(entry.getKey()).getAddress().hostname.replace('.', '_');
            long value = entry.getValue();
            String key = String.format("Bucket Placement.%s.assign.%s", scanNode.getTableName(), host);
            Tracers.record(Tracers.Module.EXTERNAL, key, String.valueOf(value));
        }
    }
}
