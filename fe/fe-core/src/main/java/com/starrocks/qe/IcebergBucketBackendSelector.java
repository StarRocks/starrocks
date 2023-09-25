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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.starrocks.common.FeConstants;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IcebergBucketBackendSelector implements BackendSelector {
    public static final Logger LOG = LogManager.getLogger(IcebergBucketBackendSelector.class);
    Map<ComputeNode, Long> assignedScansPerComputeNode = Maps.newHashMap();
    Map<Integer, Long> assignedScansPerBucket = Maps.newHashMap();

    Multimap<String, ComputeNode> hostToBackends = HashMultimap.create();

    private final Map<Integer, Long> icebergBucketIdToBeId;
    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final FragmentScanRangeAssignment assignment;
    private final ImmutableMap<Long, ComputeNode> idToBackend;

    private final boolean chooseComputeNode;
    private final Map<String, Integer> filePathToBucketId;
    private final Set<Long> usedBackendIDs;
    private final Map<TNetworkAddress, Long> addressToBackendId;

    private final Map<PlanFragmentId, CoordinatorPreprocessor.BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap;
    private final Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap;
    private final Map<PlanFragmentId, Integer> fragmentIdToBucketNumMap;


    public IcebergBucketBackendSelector(ScanNode scanNode,
                                        List<TScanRangeLocations> locations,
                                        FragmentScanRangeAssignment assignment,
                                        ImmutableMap<Long, ComputeNode> idToBackend,
                                        Map<Integer, Long> bucketIdToBeId,
                                        Map<TNetworkAddress, Long> addressToBackendId,
                                        Set<Long> usedBackendIDs,
                                        boolean chooseComputeNode,
                                        Map<PlanFragmentId, CoordinatorPreprocessor.BucketSeqToScanRange> fragmentIdBucketS,
                                        Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap,
                                        Map<PlanFragmentId, Integer> fragmentIdToBucketNumMap) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.idToBackend = idToBackend;
        this.chooseComputeNode = chooseComputeNode;
        this.icebergBucketIdToBeId = bucketIdToBeId;
        this.filePathToBucketId = ((IcebergScanNode) scanNode).getFileToBucketId();
        this.usedBackendIDs = usedBackendIDs;
        this.addressToBackendId = addressToBackendId;
        this.fragmentIdBucketSeqToScanRangeMap = fragmentIdBucketS;
        this.fragmentIdToSeqToAddressMap = fragmentIdToSeqToAddressMap;
        this.fragmentIdToBucketNumMap = fragmentIdToBucketNumMap;
    }

    @Override
    public void computeScanRangeAssignment() throws Exception {
        if (locations.size() == 0) {
            return;
        }
        PlanFragmentId fragmentId = scanNode.getFragmentId();

        if (!fragmentIdToSeqToAddressMap.containsKey(fragmentId)) {
            fragmentIdToSeqToAddressMap.put(fragmentId, Maps.newHashMap());
            fragmentIdBucketSeqToScanRangeMap.put(fragmentId, new CoordinatorPreprocessor.BucketSeqToScanRange());
            fragmentIdToBucketNumMap.put(fragmentId, ((IcebergScanNode) scanNode).getBucketNum());
        }

        Map<Integer, TNetworkAddress> bucketSeqToAddress = fragmentIdToSeqToAddressMap.get(fragmentId);
        CoordinatorPreprocessor.BucketSeqToScanRange bucketSeqToScanRange =
                fragmentIdBucketSeqToScanRangeMap.get(scanNode.getFragmentId());

        for (ComputeNode computeNode : idToBackend.values()) {
            if (!computeNode.isAlive() || SimpleScheduler.isInBlacklist(computeNode.getId())) {
                continue;
            }
            assignedScansPerComputeNode.put(computeNode, 0L);
            hostToBackends.put(computeNode.getHost(), computeNode);
        }

        if (hostToBackends.isEmpty()) {
            throw new UserException(FeConstants.getNodeNotFoundError(chooseComputeNode));
        }

        for (Integer bucketId : icebergBucketIdToBeId.keySet()) {
            assignedScansPerBucket.put(bucketId, 0L);
        }

        if (assignedScansPerBucket.isEmpty()) {
            throw new StarRocksConnectorException("iceberg buckets is null");
        }

        for (TScanRangeLocations scanRangeLocations : locations) {
            String filePath = scanRangeLocations.scan_range.hdfs_scan_range.full_path;
            Integer bucketId = filePathToBucketId.get(filePath);
            if (bucketId == null) {
                throw new StarRocksConnectorException("Failed to find bucketId on file %s", filePath);
            }
            Long beId = icebergBucketIdToBeId.get(bucketId);
            if (beId == null) {
                throw new StarRocksConnectorException("Failed to find be id on bucket", bucketId);
            }
            ComputeNode node = idToBackend.get(beId);
            if (node == null) {
                throw new RuntimeException("Failed to find backend to execute");
            }

            recordScanRangeAssignment(node, scanRangeLocations, bucketId, bucketSeqToScanRange, bucketSeqToAddress);
            recordScanRangeStatistic();
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
                BackendSelector.findOrInsert(assignment, address, new HashMap<>());

        List<TScanRangeParams> scanRangeParamsList =
                BackendSelector.findOrInsert(scanRanges, scanNode.getId().asInt(), new ArrayList<>());
        // add scan range params
        TScanRangeParams scanRangeParams = new TScanRangeParams();
        scanRangeParams.scan_range = scanRangeLocations.scan_range;
        scanRangeParamsList.add(scanRangeParams);

        scanRanges = bucketSeqToScanRange.computeIfAbsent(bucketId, k -> Maps.newHashMap());
        scanRangeParamsList = scanRanges.computeIfAbsent(scanNode.getId().asInt(), k -> Lists.newArrayList());
        scanRangeParamsList.add(scanRangeParams);

        bucketSeqToAddress.put(bucketId, address);
    }

    private void recordScanRangeStatistic() {
        // record scan range size for each backend
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<ComputeNode, Long> entry : assignedScansPerComputeNode.entrySet()) {
            sb.append(entry.getKey().getAddress().hostname).append(":").append(entry.getValue()).append(",");
        }
        PlannerProfile.addCustomProperties(scanNode.getTableName() + " scan_range_bytes", sb.toString());

        sb = new StringBuilder();
        for (Map.Entry<Integer, Long> entry : assignedScansPerBucket.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
        }

        PlannerProfile.addCustomProperties(scanNode.getTableName() + " scan_range_bytes", sb.toString());
    }
}
