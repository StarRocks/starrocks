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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Reference;
import com.starrocks.common.UserException;
import com.starrocks.connector.exception.StarRocksConnectorException;
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

import static com.starrocks.qe.CoordinatorPreprocessor.BucketSeqToScanRange;

public class IcebergBucketBackendSelector implements BackendSelector {
    public static final Logger LOG = LogManager.getLogger(IcebergBucketBackendSelector.class);
    Map<String, Long> assignedScansPerComputeNode = Maps.newHashMap();
    Map<Integer, Long> assignedScansPerBucket = Maps.newHashMap();

    private final ScanNode scanNode;
    private final Set<Long> usedBackendIDs;
    private final FragmentScanRangeAssignment assignment;
    private final ImmutableMap<Long, ComputeNode> idToBackend;

    private final Map<TNetworkAddress, Long> addressToBackendId;

    private final Map<PlanFragmentId, CoordinatorPreprocessor.BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap;
    private final Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap;
    private final Map<PlanFragmentId, Integer> fragmentIdToBucketNumMap;

    private final Set<Integer> rightOrFullBucketShuffleFragmentIds;

    public IcebergBucketBackendSelector(ScanNode scanNode,
                                        FragmentScanRangeAssignment assignment,
                                        ImmutableMap<Long, ComputeNode> idToBackend,
                                        Map<TNetworkAddress, Long> addressToBackendId,
                                        Set<Long> usedBackendIDs,
                                        Map<PlanFragmentId, BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap,
                                        Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap,
                                        Map<PlanFragmentId, Integer> fragmentIdToBucketNumMap,
                                        Set<Integer> rightOrFullBucketShuffleFragmentIds) {
        this.scanNode = scanNode;
        this.assignment = assignment;
        this.idToBackend = idToBackend;
        this.usedBackendIDs = usedBackendIDs;
        this.addressToBackendId = addressToBackendId;
        this.fragmentIdBucketSeqToScanRangeMap = fragmentIdBucketSeqToScanRangeMap;
        this.fragmentIdToSeqToAddressMap = fragmentIdToSeqToAddressMap;
        this.fragmentIdToBucketNumMap = fragmentIdToBucketNumMap;
        this.rightOrFullBucketShuffleFragmentIds = rightOrFullBucketShuffleFragmentIds;
    }

    @Override
    public void computeScanRangeAssignment() throws UserException {
        PlanFragmentId fragmentId = scanNode.getFragmentId();
        IcebergScanNode icebergScanNode = (IcebergScanNode) scanNode;

        if (!fragmentIdToSeqToAddressMap.containsKey(fragmentId)) {
            fragmentIdToSeqToAddressMap.put(fragmentId, Maps.newHashMap());
            fragmentIdBucketSeqToScanRangeMap.put(fragmentId, new CoordinatorPreprocessor.BucketSeqToScanRange());
            fragmentIdToBucketNumMap.put(fragmentId, icebergScanNode.getTransformedBucketSize());
        }

        Map<Integer, TNetworkAddress> bucketSeqToAddress = fragmentIdToSeqToAddressMap.get(fragmentId);
        CoordinatorPreprocessor.BucketSeqToScanRange bucketSeqToScanRange =
                fragmentIdBucketSeqToScanRangeMap.get(scanNode.getFragmentId());

        for (ComputeNode computeNode : idToBackend.values()) {
            if (!computeNode.isAlive() || SimpleScheduler.isInBlacklist(computeNode.getId())) {
                continue;
            }
            assignedScansPerComputeNode.put(computeNode.getAddress().getHostname(), 0L);
        }

        ArrayListMultimap<Integer, TScanRangeLocations> bucketSeqToScanRangeLocations = icebergScanNode.getBucketSeqToLocations();

        for (Integer bucketId : bucketSeqToScanRangeLocations.keySet()) {
            assignedScansPerBucket.put(bucketId, 0L);
        }

        int pos = 0;
        for (Integer bucketSeq : bucketSeqToScanRangeLocations.keySet()) {
            List<TScanRangeLocations> scanRangeLocations = bucketSeqToScanRangeLocations.get(bucketSeq);

            ComputeNode node;
            if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                node = idToBackend.values().asList().get((pos++) % idToBackend.size());
            } else {
                TNetworkAddress address = bucketSeqToAddress.get(bucketSeq);
                Long beId = addressToBackendId.get(address);
                if (beId == null) {
                    throw new StarRocksConnectorException("Can't find be id with address : %s", address.hostname);
                }
                node = idToBackend.get(beId);
                if (node == null) {
                    throw new StarRocksConnectorException("Can't find be with id :%d", beId);
                }
            }

            for (TScanRangeLocations tScanRangeLocations : scanRangeLocations) {
                recordScanRangeAssignment(node, tScanRangeLocations, bucketSeq, bucketSeqToScanRange, bucketSeqToAddress);
                recordScanRangeStatistic(bucketSeqToAddress);
            }
        }

        if (rightOrFullBucketShuffleFragmentIds.contains(fragmentId.asInt())) {
            int bucketNum = fragmentIdToBucketNumMap.get(fragmentId);

            for (int bucketSeq = 0; bucketSeq < bucketNum; ++bucketSeq) {
                if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                    Reference<Long> backendIdRef = new Reference<>();
                    TNetworkAddress execHostport = SimpleScheduler.getBackendHost(idToBackend, backendIdRef);
                    if (execHostport == null) {
                        throw new UserException(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR);
                    }
                    usedBackendIDs.add(backendIdRef.getRef());
                    addressToBackendId.put(execHostport, backendIdRef.getRef());
                    bucketSeqToAddress.put(bucketSeq, execHostport);
                }
                if (!bucketSeqToScanRange.containsKey(bucketSeq)) {
                    bucketSeqToScanRange.put(bucketSeq, Maps.newHashMap());
                    bucketSeqToScanRange.get(bucketSeq).put(scanNode.getId().asInt(), Lists.newArrayList());
                }
            }
        }

        for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> entry : bucketSeqToScanRange.entrySet()) {
            Integer bucketSeq = entry.getKey();

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
        TNetworkAddress address = node.getAddress();

        usedBackendIDs.add(node.getId());
        addressToBackendId.put(address, node.getId());

        // add in assignment
        Map<Integer, List<TScanRangeParams>> scanRanges =
                bucketSeqToScanRange.computeIfAbsent(bucketId, k -> Maps.newHashMap());
        List<TScanRangeParams> scanRangeParamsList =
                scanRanges.computeIfAbsent(scanNode.getId().asInt(), k -> Lists.newArrayList());

        // add scan range params
        TScanRangeParams scanRangeParams = new TScanRangeParams();
        scanRangeParams.scan_range = scanRangeLocations.scan_range;
        scanRangeParamsList.add(scanRangeParams);

        // fill bucket id to address
        bucketSeqToAddress.put(bucketId, address);

        // update statistic
        long addedScans = scanRangeLocations.scan_range.hdfs_scan_range.length;
        assignedScansPerComputeNode.put(address.hostname, assignedScansPerComputeNode.get(address.hostname) + addedScans);
        assignedScansPerBucket.put(bucketId, assignedScansPerBucket.get(bucketId) + addedScans);
    }

    private void recordScanRangeStatistic(Map<Integer, TNetworkAddress> bucketSeqToAddr) {
        // record scan range size for each backend
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> entry : assignedScansPerComputeNode.entrySet()) {
            sb.append(entry.getKey()).append(" : ").append(entry.getValue()).append(",");
        }
        PlannerProfile.addCustomProperties(scanNode.getTableName() + " node_scan_range_bytes", sb.toString());

        sb = new StringBuilder();
        for (Map.Entry<Integer, Long> entry : assignedScansPerBucket.entrySet()) {
            sb.append(entry.getKey()).append(" : ").append(entry.getValue()).append(",");
        }
        PlannerProfile.addCustomProperties(scanNode.getTableName() + " bucket_scan_range_bytes", sb.toString());

        sb = new StringBuilder();
        for (Map.Entry<Integer, TNetworkAddress> entry : bucketSeqToAddr.entrySet()) {
            sb.append(entry.getKey()).append(" : ").append(entry.getValue().hostname).append(",");
        }
        PlannerProfile.addCustomProperties(scanNode.getTableName() + " bucket_id_to_be_addr", sb.toString());
    }
}
