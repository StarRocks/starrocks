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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.UserException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;

import java.util.List;
import java.util.Map;

public class IcebergBucketBackendSelector implements BackendSelector {
    private final IcebergScanNode scanNode;

    private final Map<String, Long> assignedScansPerComputeNode = Maps.newHashMap();
    private final Map<Integer, Long> assignedScansPerBucket = Maps.newHashMap();
    private final FragmentScanRangeAssignment assignment;
    private final ColocatedBackendSelector.Assignment colocatedAssignment;
    private final boolean isRightOrFullBucketShuffleFragment;
    private final WorkerProvider workerProvider;

    public IcebergBucketBackendSelector(IcebergScanNode scanNode, FragmentScanRangeAssignment assignment,
                                        ColocatedBackendSelector.Assignment colocatedAssignment,
                                    boolean isRightOrFullBucketShuffleFragment, WorkerProvider workerProvider) {
        this.scanNode = scanNode;
        this.assignment = assignment;
        this.colocatedAssignment = colocatedAssignment;
        this.isRightOrFullBucketShuffleFragment = isRightOrFullBucketShuffleFragment;
        this.workerProvider = workerProvider;
    }

    @Override
    public void computeScanRangeAssignment() throws UserException {
        Map<Integer, Long> bucketSeqToWorkerId = colocatedAssignment.getSeqToWorkerId();
        ColocatedBackendSelector.BucketSeqToScanRange bucketSeqToScanRange = colocatedAssignment.getSeqToScanRange();
        ArrayListMultimap<Integer, TScanRangeLocations> bucketSeqToScanRangeLocations = scanNode.getBucketSeqToLocations();
        for (Integer bucketId : bucketSeqToScanRangeLocations.keySet()) {
            assignedScansPerBucket.put(bucketId, 0L);
        }

        List<ComputeNode> allWorkers = Lists.newArrayList(workerProvider.getAllWorkers());
        if (allWorkers.isEmpty()) {
            throw new StarRocksConnectorException("Failed to find backend to execute");
        }

        for (ComputeNode computeNode : allWorkers) {
            assignedScansPerComputeNode.put(computeNode.getAddress().getHostname(), 0L);
        }

        int pos = 0;
        for (Integer bucketSeq : bucketSeqToScanRangeLocations.keySet()) {
            List<TScanRangeLocations> scanRangeLocations = bucketSeqToScanRangeLocations.get(bucketSeq);

            ComputeNode node;
            if (!bucketSeqToWorkerId.containsKey(bucketSeq)) {
                node = allWorkers.get((pos++) % allWorkers.size());
            } else {
                Long workerId = bucketSeqToWorkerId.get(bucketSeq);
                node = workerProvider.getWorkerById(workerId);
            }

            for (TScanRangeLocations tScanRangeLocations : scanRangeLocations) {
                recordScanRangeAssignment(node, tScanRangeLocations, bucketSeq);
                recordScanRangeStatistic();
            }
        }
        // Because of the right table will not send data to the bucket which has been pruned, the right join or full join will get wrong result.
        // So if this bucket shuffle is right join or full join, we need to add empty bucket scan range which is pruned by predicate.
        if (isRightOrFullBucketShuffleFragment) {
            int bucketNum = colocatedAssignment.getBucketNum();

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

    private void recordScanRangeAssignment(ComputeNode node, TScanRangeLocations scanRangeLocations, int bucketId)
            throws NonRecoverableException {
        TNetworkAddress address = node.getAddress();

        workerProvider.selectWorker(node.getId());

        Map<Integer, List<TScanRangeParams>> scanRanges =
                colocatedAssignment.getSeqToScanRange().computeIfAbsent(bucketId, k -> Maps.newHashMap());
        // add in assignment
        List<TScanRangeParams> scanRangeParamsList =
                scanRanges.computeIfAbsent(scanNode.getId().asInt(), k -> Lists.newArrayList());

        // add scan range params
        TScanRangeParams scanRangeParams = new TScanRangeParams();
        scanRangeParams.scan_range = scanRangeLocations.scan_range;
        scanRangeParamsList.add(scanRangeParams);

        // fill bucket id to worker
        colocatedAssignment.getSeqToWorkerId().put(bucketId, node.getId());

        // update statistic
        long addedScans = scanRangeLocations.scan_range.hdfs_scan_range.length;
        assignedScansPerComputeNode.put(address.hostname, assignedScansPerComputeNode.get(address.hostname) + addedScans);
        assignedScansPerBucket.put(bucketId, assignedScansPerBucket.get(bucketId) + addedScans);
    }

    private void recordScanRangeStatistic() {
        // record scan range size for each backend
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> entry : assignedScansPerComputeNode.entrySet()) {
            sb.append(entry.getKey()).append(" : ").append(entry.getValue()).append(",");
        }
        Tracers.record(Tracers.Module.EXTERNAL, scanNode.getTableName() + " node_scan_range_bytes", sb.toString());

        sb = new StringBuilder();
        for (Map.Entry<Integer, Long> entry : assignedScansPerBucket.entrySet()) {
            sb.append(entry.getKey()).append(" : ").append(entry.getValue()).append(",");
        }
        Tracers.record(Tracers.Module.EXTERNAL, scanNode.getTableName() + " bucket_scan_range_bytes", sb.toString());

        sb = new StringBuilder();
        for (Map.Entry<Integer, Long> entry : colocatedAssignment.getSeqToWorkerId().entrySet()) {
            sb.append(entry.getKey()).append(" : ").append(workerProvider.getWorkerById(entry.getValue()).getAddress())
                    .append(",");
        }
        Tracers.record(Tracers.Module.EXTERNAL, scanNode.getTableName() + " bucket_id_to_be_addr", sb.toString());
    }
}
