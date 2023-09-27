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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.ScanNode;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class IcebergBucketBackendSelector implements BackendSelector {
    public static final Logger LOG = LogManager.getLogger(IcebergBucketBackendSelector.class);
    Map<ComputeNode, Long> assignedScansPerComputeNode = Maps.newHashMap();
    Map<Integer, Long> assignedScansPerBucket = Maps.newHashMap();

    private final ScanNode scanNode;
    private final FragmentScanRangeAssignment assignment;
    private final ImmutableMap<Long, ComputeNode> idToBackend;

    private final Set<Long> usedBackendIDs;
    private final Map<TNetworkAddress, Long> addressToBackendId;

    private final Map<PlanFragmentId, CoordinatorPreprocessor.BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap;
    private final Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap;
    private final Map<PlanFragmentId, Integer> fragmentIdToBucketNumMap;


    public IcebergBucketBackendSelector(ScanNode scanNode,
                                        FragmentScanRangeAssignment assignment,
                                        ImmutableMap<Long, ComputeNode> idToBackend,
                                        Map<TNetworkAddress, Long> addressToBackendId,
                                        Set<Long> usedBackendIDs,
                                        Map<PlanFragmentId, CoordinatorPreprocessor.BucketSeqToScanRange> fragmentIdBucketS,
                                        Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap,
                                        Map<PlanFragmentId, Integer> fragmentIdToBucketNumMap) {
        this.scanNode = scanNode;
        this.assignment = assignment;
        this.idToBackend = idToBackend;
        this.usedBackendIDs = usedBackendIDs;
        this.addressToBackendId = addressToBackendId;
        this.fragmentIdBucketSeqToScanRangeMap = fragmentIdBucketS;
        this.fragmentIdToSeqToAddressMap = fragmentIdToSeqToAddressMap;
        this.fragmentIdToBucketNumMap = fragmentIdToBucketNumMap;
    }

    @Override
    public void computeScanRangeAssignment() {
        PlanFragmentId fragmentId = scanNode.getFragmentId();

        if (!fragmentIdToSeqToAddressMap.containsKey(fragmentId)) {
            int res = 1;
            for (Integer num : ((IcebergScanNode) scanNode).getBucketNums()) {
                res = res * num;
            }

            fragmentIdToSeqToAddressMap.put(fragmentId, Maps.newHashMap());
            fragmentIdBucketSeqToScanRangeMap.put(fragmentId, new CoordinatorPreprocessor.BucketSeqToScanRange());
            fragmentIdToBucketNumMap.put(fragmentId, res);
        }

        Map<Integer, TNetworkAddress> bucketSeqToAddress = fragmentIdToSeqToAddressMap.get(fragmentId);
        CoordinatorPreprocessor.BucketSeqToScanRange bucketSeqToScanRange =
                fragmentIdBucketSeqToScanRangeMap.get(scanNode.getFragmentId());

        for (ComputeNode computeNode : idToBackend.values()) {
            if (!computeNode.isAlive() || SimpleScheduler.isInBlacklist(computeNode.getId())) {
                continue;
            }
            assignedScansPerComputeNode.put(computeNode, 0L);
        }

        for (Integer bucketId : ((IcebergScanNode) scanNode).getBucketSeq2locations().keySet()) {
            assignedScansPerBucket.put(bucketId, 0L);
        }

        int i = 0;
        for (Integer bucketSeq : ((IcebergScanNode) scanNode).getBucketSeq2locations().keySet()) {
            //fill scanRangeParamsList
            List<TScanRangeLocations> tScanRangeLocations = ((IcebergScanNode) scanNode).getBucketSeq2locations().get(bucketSeq);

            ComputeNode node;
            if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                node = idToBackend.values().asList().get(i++ % idToBackend.size());
                bucketSeqToAddress.put(bucketSeq, node.getAddress());
            } else {
                node = idToBackend.get(addressToBackendId.get(bucketSeqToAddress.get(bucketSeq)));
            }

            for (TScanRangeLocations scanRangeLocations : tScanRangeLocations) {
                recordScanRangeAssignment(node, scanRangeLocations, bucketSeq, bucketSeqToScanRange, bucketSeqToAddress);
                recordScanRangeStatistic();
            }
        }

        for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> entry : bucketSeqToScanRange.entrySet()) {
            Integer bucketSeq = entry.getKey();
            // fill FragmentScanRangeAssignment only when there are scan id in the bucket
            if (entry.getValue().containsKey(scanNode.getId().asInt())) {
                Map<Integer, List<TScanRangeParams>> scanRanges =
                        assignment.computeIfAbsent(bucketSeqToAddress.get(bucketSeq), k -> Maps.newHashMap());
                List<TScanRangeParams> scanRangeParamsList =
                        scanRanges.computeIfAbsent(scanNode.getId().asInt(), k -> Lists.newArrayList());
                scanRangeParamsList.addAll(entry.getValue().get(scanNode.getId().asInt()));
            }
        }
    }

    private void recordScanRangeAssignment(ComputeNode node, TScanRangeLocations scanRangeLocations, int bucketId,
                                           CoordinatorPreprocessor.BucketSeqToScanRange bucketSeqToScanRange,
                                           Map<Integer, TNetworkAddress> bucketSeqToAddress) {
        TNetworkAddress address = new TNetworkAddress(node.getHost(), node.getBePort());

        usedBackendIDs.add(node.getId());
        addressToBackendId.put(address, node.getId());
        // update statistic
        long addedScans = scanRangeLocations.scan_range.hdfs_scan_range.length;
        assignedScansPerComputeNode.put(node, assignedScansPerComputeNode.get(node) + addedScans);
        assignedScansPerBucket.put(bucketId, assignedScansPerBucket.get(bucketId) + addedScans);
        // add in assignment
        Map<Integer, List<TScanRangeParams>> scanRanges =
                bucketSeqToScanRange.computeIfAbsent(bucketId, k -> Maps.newHashMap());

        List<TScanRangeParams> scanRangeParamsList =
                scanRanges.computeIfAbsent(scanNode.getId().asInt(), k -> Lists.newArrayList());
        // add scan range params
        TScanRangeParams scanRangeParams = new TScanRangeParams();
        scanRangeParams.scan_range = scanRangeLocations.scan_range;
        scanRangeParamsList.add(scanRangeParams);

        bucketSeqToAddress.put(bucketId, address);
    }

    private void recordScanRangeStatistic() {
        // record scan range size for each backend
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<ComputeNode, Long> entry : assignedScansPerComputeNode.entrySet()) {
            sb.append(entry.getKey().getAddress().hostname).append(":").append(entry.getValue()).append(",");
        }
        PlannerProfile.addCustomProperties(scanNode.getTableName() + " node_scan_range_bytes", sb.toString());

        sb = new StringBuilder();
        for (Map.Entry<Integer, Long> entry : assignedScansPerBucket.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
        }

        PlannerProfile.addCustomProperties(scanNode.getTableName() + " bucket_scan_range_bytes", sb.toString());

        PlannerProfile.addCustomProperties(scanNode.getTableName() + " bucket_id_to_be_addr", sb.toString());
    }
}
