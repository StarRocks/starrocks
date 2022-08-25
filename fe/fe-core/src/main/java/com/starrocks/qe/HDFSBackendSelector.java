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
    Multimap<String, ComputeNode> hostToBes = HashMultimap.create();
    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final Coordinator.FragmentScanRangeAssignment assignment;
    private final List<Long> scanRangesBytes;
    private final List<Long> remoteScanRangesBytes = Lists.newArrayList();
    private final ImmutableCollection<ComputeNode> computeNodes;
    private final List<ComputeNode> usedNodes = Lists.newArrayList();
    private boolean forceScheduleLocal;

    public HDFSBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                               Coordinator.FragmentScanRangeAssignment assignment,
                               ImmutableCollection<ComputeNode> computeNodes,
                               boolean forceScheduleLocal) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.computeNodes = computeNodes;
        this.forceScheduleLocal = forceScheduleLocal;
        this.scanRangesBytes =
                locations.stream().map(x -> x.scan_range.hdfs_scan_range.length).collect(Collectors.toList());
    }

    public List<ComputeNode> getUsedNodes() {
        return usedNodes;
    }

    @Override
    public void computeScanRangeAssignment() throws Exception {
        Preconditions.checkArgument(locations.size() == scanRangesBytes.size());
        for (ComputeNode computeNode : computeNodes) {
            if (!computeNode.isAlive() || SimpleScheduler.isInBlacklist(computeNode.getId())) {
                continue;
            }

            assignedScansPerComputeNode.put(computeNode, 0L);
            hostToBes.put(computeNode.getHost(), computeNode);
        }
        if (hostToBes.isEmpty()) {
            throw new UserException("Backend not found. Check if any backend is down or not");
        }

        // total scans / alive bes
        long avgScansPerBe = -1;
        if (!forceScheduleLocal) {
            int numBes = assignedScansPerComputeNode.size();
            long totalBytes = 0L;
            for (long scanRangeBytes : scanRangesBytes) {
                totalBytes += scanRangeBytes;
            }
            avgScansPerBe = totalBytes / numBes + (totalBytes % numBes == 0 ? 0 : 1);
        }

        List<TScanRangeLocations> remoteScanRangeLocations = Lists.newArrayList();
        for (int i = 0; i < locations.size(); ++i) {
            TScanRangeLocations scanRangeLocations = locations.get(i);
            long minAssignedScanRanges = Long.MAX_VALUE;
            ComputeNode minBe = null;
            for (final TScanRangeLocation location : scanRangeLocations.getLocations()) {
                Collection<ComputeNode> bes = hostToBes.get(location.getServer().getHostname());
                if (bes == null || bes.isEmpty()) {
                    continue;
                }
                for (ComputeNode backend : bes) {
                    long assignedScanRanges = assignedScansPerComputeNode.get(backend);
                    if (!forceScheduleLocal && assignedScanRanges >= avgScansPerBe) {
                        continue;
                    }
                    if (assignedScanRanges < minAssignedScanRanges) {
                        minAssignedScanRanges = assignedScanRanges;
                        minBe = backend;
                    }
                }
            }
            if (minBe == null) {
                remoteScanRangeLocations.add(scanRangeLocations);
                remoteScanRangesBytes.add(scanRangesBytes.get(i));
                continue;
            }
            long scansToAdd = scanRangesBytes.get(i);
            recordScanRangeAssignment(minBe, scanRangeLocations, scansToAdd);
        }

        if (remoteScanRangeLocations.isEmpty()) {
            return;
        }

        Preconditions.checkArgument(remoteScanRangeLocations.size() == remoteScanRangesBytes.size());
        for (int i = 0; i < remoteScanRangeLocations.size(); ++i) {
            TScanRangeLocations scanRangeLocations = remoteScanRangeLocations.get(i);
            long minAssignedScanRanges = Long.MAX_VALUE;
            ComputeNode minBe = null;
            for (Map.Entry<ComputeNode, Long> entry : assignedScansPerComputeNode.entrySet()) {
                ComputeNode backend = entry.getKey();
                long assignedScanRanges = entry.getValue();
                if (assignedScanRanges < minAssignedScanRanges) {
                    minAssignedScanRanges = assignedScanRanges;
                    minBe = backend;
                }
            }
            long scansToAdd = remoteScanRangesBytes.get(i);
            recordScanRangeAssignment(minBe, scanRangeLocations, scansToAdd);
        }
    }

    private void recordScanRangeAssignment(ComputeNode minBe, TScanRangeLocations scanRangeLocations,
                                           long addedScans) {
        usedNodes.add(minBe);

        // update statistic
        assignedScansPerComputeNode.put(minBe, assignedScansPerComputeNode.get(minBe) + addedScans);

        // add in assignment
        TNetworkAddress minBeAddress = new TNetworkAddress(minBe.getHost(), minBe.getBePort());
        Map<Integer, List<TScanRangeParams>> scanRanges = BackendSelector.findOrInsert(
                assignment, minBeAddress, new HashMap<>());
        List<TScanRangeParams> scanRangeParamsList = BackendSelector.findOrInsert(
                scanRanges, scanNode.getId().asInt(), new ArrayList<TScanRangeParams>());
        // add scan range params
        TScanRangeParams scanRangeParams = new TScanRangeParams();
        scanRangeParams.scan_range = scanRangeLocations.scan_range;
        scanRangeParamsList.add(scanRangeParams);
    }
}