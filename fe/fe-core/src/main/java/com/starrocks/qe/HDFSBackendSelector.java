// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.starrocks.common.UserException;
import com.starrocks.planner.ScanNode;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hybrid backend selector for hive table.
 * Support hybrid and independent deployment with datanode.
 * <p>
 * Assign scan ranges to backend:
 * 1. local backend first,
 * 2. and smallest assigned scan ranges num or scan bytes.
 * <p>
 * If force_schedule_local variable is set, HybridBackendSelector will force to
 * assign scan ranges to local backend if there has one.
 */

public class HDFSBackendSelector implements BackendSelector {
    // be -> assigned scans
    Map<ComputeNode, Long> assignedScansPerComputeNode = Maps.newHashMap();
    // be host -> bes
    Multimap<String, ComputeNode> hostToBackends = HashMultimap.create();
    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final Coordinator.FragmentScanRangeAssignment assignment;
    private final Map<TNetworkAddress, Long> addressToBackendId;
    private final List<Long> scanRangesBytes;
    private final List<Long> remoteScanRangesBytes = Lists.newArrayList();
    private final ImmutableCollection<ComputeNode> computeNodes;
    private boolean forceScheduleLocal;

    public HDFSBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                               Coordinator.FragmentScanRangeAssignment assignment,
                               Map<TNetworkAddress, Long> addressToBackendId,
                               ImmutableCollection<ComputeNode> computeNodes,
                               boolean forceScheduleLocal) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.computeNodes = computeNodes;
        this.forceScheduleLocal = forceScheduleLocal;
        this.addressToBackendId = addressToBackendId;
        this.scanRangesBytes =
                locations.stream().map(x -> x.scan_range.hdfs_scan_range.length).collect(Collectors.toList());
    }

    @Override
    public void computeScanRangeAssignment() throws Exception {
        Preconditions.checkArgument(locations.size() == scanRangesBytes.size());
        for (ComputeNode computeNode : computeNodes) {
            if (!computeNode.isAlive() || SimpleScheduler.isInBlacklist(computeNode.getId())) {
                continue;
            }

            assignedScansPerComputeNode.put(computeNode, 0L);
            hostToBackends.put(computeNode.getHost(), computeNode);
        }
        if (hostToBackends.isEmpty()) {
            throw new UserException("Backend not found. Check if any backend is down or not");
        }

        // total scans / alive bes
        long avgScanBytes = -1;
        if (!forceScheduleLocal) {
            int numBes = assignedScansPerComputeNode.size();
            long totalBytes = 0L;
            for (long scanRangeBytes : scanRangesBytes) {
                totalBytes += scanRangeBytes;
            }
            avgScanBytes = totalBytes / numBes + (totalBytes % numBes == 0 ? 0 : 1);
        }

        List<TScanRangeLocations> remoteScanRangeLocations = Lists.newArrayList();
        for (int i = 0; i < locations.size(); ++i) {
            TScanRangeLocations scanRangeLocations = locations.get(i);
            long minAssignedScanRanges = Long.MAX_VALUE;
            ComputeNode node = null;
            for (final TScanRangeLocation location : scanRangeLocations.getLocations()) {
                Collection<ComputeNode> backends = hostToBackends.get(location.getServer().getHostname());
                if (backends == null || backends.isEmpty()) {
                    continue;
                }
                for (ComputeNode backend : backends) {
                    long assignedScanRanges = assignedScansPerComputeNode.get(backend);
                    if (!forceScheduleLocal && assignedScanRanges >= avgScanBytes) {
                        continue;
                    }
                    if (assignedScanRanges < minAssignedScanRanges) {
                        minAssignedScanRanges = assignedScanRanges;
                        node = backend;
                    }
                }
            }
            if (node == null) {
                remoteScanRangeLocations.add(scanRangeLocations);
                remoteScanRangesBytes.add(scanRangesBytes.get(i));
                continue;
            }
            long scansToAdd = scanRangesBytes.get(i);
            recordScanRangeAssignment(node, scanRangeLocations, scansToAdd);
        }

        if (remoteScanRangeLocations.isEmpty()) {
            return;
        }

        Preconditions.checkArgument(remoteScanRangeLocations.size() == remoteScanRangesBytes.size());
        for (int i = 0; i < remoteScanRangeLocations.size(); ++i) {
            TScanRangeLocations scanRangeLocations = remoteScanRangeLocations.get(i);
            long minAssignedScanRanges = Long.MAX_VALUE;
            ComputeNode node = null;
            for (Map.Entry<ComputeNode, Long> entry : assignedScansPerComputeNode.entrySet()) {
                ComputeNode backend = entry.getKey();
                long assignedScanRanges = entry.getValue();
                if (assignedScanRanges < minAssignedScanRanges) {
                    minAssignedScanRanges = assignedScanRanges;
                    node = backend;
                }
            }
            if (node == null) {
                throw new RuntimeException("Failed to find backend to execute");
            }
            long scansToAdd = remoteScanRangesBytes.get(i);
            recordScanRangeAssignment(node, scanRangeLocations, scansToAdd);
        }
    }

    private void recordScanRangeAssignment(ComputeNode node, TScanRangeLocations scanRangeLocations,
                                           long addedScans) {
        TNetworkAddress address = new TNetworkAddress(node.getHost(), node.getBePort());
        addressToBackendId.put(address, node.getId());

        // update statistic
        assignedScansPerComputeNode.put(node, assignedScansPerComputeNode.get(node) + addedScans);

        // add in assignment
        Map<Integer, List<TScanRangeParams>> scanRanges = BackendSelector.findOrInsert(
                assignment, address, new HashMap<>());
        List<TScanRangeParams> scanRangeParamsList = BackendSelector.findOrInsert(
                scanRanges, scanNode.getId().asInt(), new ArrayList<TScanRangeParams>());
        // add scan range params
        TScanRangeParams scanRangeParams = new TScanRangeParams();
        scanRangeParams.scan_range = scanRangeLocations.scan_range;
        scanRangeParamsList.add(scanRangeParams);
    }
}