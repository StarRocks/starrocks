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

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.common.util.ConsistentHashRing;
import com.starrocks.common.util.HashRing;
import com.starrocks.common.util.RendezvousHashRing;
import com.starrocks.planner.DeltaLakeScanNode;
import com.starrocks.planner.FileTableScanNode;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.HudiScanNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final CoordinatorPreprocessor.FragmentScanRangeAssignment assignment;
    private final Set<Long> usedBackendIDs;
    private final Map<TNetworkAddress, Long> addressToBackendId;
    private final ImmutableCollection<ComputeNode> computeNodes;
    private boolean forceScheduleLocal;
    private boolean chooseComputeNode;
    private final int kCandidateNumber = 3;
    private final int kMaxImbalanceRatio = 3;
    private final int kMaxNodeSizeUseRendezvousHashRing = 64;
    private final int kConsistenHashRingVirtualNumber = 32;

    class HdfsScanRangeHasher {
        String basePath;
        HDFSScanNodePredicates predicates;

        public HdfsScanRangeHasher() {
            if (scanNode instanceof HdfsScanNode) {
                HdfsScanNode node = (HdfsScanNode) scanNode;
                predicates = node.getScanNodePredicates();
                basePath = node.getHiveTable().getTableLocation();
            } else if (scanNode instanceof IcebergScanNode) {
                IcebergScanNode node = (IcebergScanNode) scanNode;
                predicates = node.getScanNodePredicates();
            } else if (scanNode instanceof HudiScanNode) {
                HudiScanNode node = (HudiScanNode) scanNode;
                predicates = node.getScanNodePredicates();
                basePath = node.getHudiTable().getTableLocation();
            } else if (scanNode instanceof DeltaLakeScanNode) {
                DeltaLakeScanNode node = (DeltaLakeScanNode) scanNode;
                predicates = node.getScanNodePredicates();
                basePath = node.getDeltaLakeTable().getTableLocation();
            } else if (scanNode instanceof FileTableScanNode) {
                FileTableScanNode node = (FileTableScanNode) scanNode;
                predicates = node.getScanNodePredicates();
                basePath = node.getFileTable().getTableLocation();
            } else {
                Preconditions.checkState(false);
            }
        }

        public void acceptScanRangeLocations(TScanRangeLocations tScanRangeLocations, PrimitiveSink primitiveSink) {
            THdfsScanRange hdfsScanRange = tScanRangeLocations.scan_range.hdfs_scan_range;
            if (hdfsScanRange.isSetFull_path()) {
                primitiveSink.putBytes(hdfsScanRange.full_path.getBytes(StandardCharsets.UTF_8));
            } else {
                if (basePath != null) {
                    primitiveSink.putBytes(basePath.getBytes(StandardCharsets.UTF_8));
                }
                if (hdfsScanRange.isSetPartition_id() &&
                        predicates.getIdToPartitionKey().containsKey(hdfsScanRange.getPartition_id())) {
                    PartitionKey partitionKey = predicates.getIdToPartitionKey().get(hdfsScanRange.getPartition_id());
                    primitiveSink.putInt(partitionKey.hashCode());
                }
                if (hdfsScanRange.isSetRelative_path()) {
                    primitiveSink.putBytes(hdfsScanRange.relative_path.getBytes(StandardCharsets.UTF_8));
                }
            }
            if (hdfsScanRange.isSetOffset()) {
                primitiveSink.putLong(hdfsScanRange.getOffset());
            }
        }
    }

    private HdfsScanRangeHasher hdfsScanRangeHasher;

    public HDFSBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                               CoordinatorPreprocessor.FragmentScanRangeAssignment assignment,
                               Map<TNetworkAddress, Long> addressToBackendId,
                               Set<Long> usedBackendIDs,
                               ImmutableCollection<ComputeNode> computeNodes,
                               boolean chooseComputeNode,
                               boolean forceScheduleLocal) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.computeNodes = computeNodes;
        this.chooseComputeNode = chooseComputeNode;
        this.forceScheduleLocal = forceScheduleLocal;
        this.addressToBackendId = addressToBackendId;
        this.usedBackendIDs = usedBackendIDs;
        this.hdfsScanRangeHasher = new HdfsScanRangeHasher();
    }

    private ComputeNode selectLeastScanBytesComputeNode(Collection<ComputeNode> backends, long maxImbalanceBytes) {
        if (backends == null || backends.isEmpty()) {
            return null;
        }

        ComputeNode node = null;
        long minAssignedScanRanges = Long.MAX_VALUE;
        for (ComputeNode backend : backends) {
            long assignedScanRanges = assignedScansPerComputeNode.get(backend);
            if (assignedScanRanges < minAssignedScanRanges) {
                minAssignedScanRanges = assignedScanRanges;
                node = backend;
            }
        }
        if (maxImbalanceBytes == 0) {
            return node;
        }

        for (ComputeNode backend : backends) {
            long assignedScanRanges = assignedScansPerComputeNode.get(backend);
            if (assignedScanRanges < (minAssignedScanRanges + maxImbalanceBytes)) {
                node = backend;
                break;
            }
        }
        return node;
    }

    class ComputeNodeFunnel implements Funnel<ComputeNode> {
        @Override
        public void funnel(ComputeNode computeNode, PrimitiveSink primitiveSink) {
            primitiveSink.putBytes(computeNode.getHost().getBytes(StandardCharsets.UTF_8));
            primitiveSink.putInt(computeNode.getBePort());
        }
    }

    class TScanRangeLocationsFunnel implements Funnel<TScanRangeLocations> {
        @Override
        public void funnel(TScanRangeLocations tScanRangeLocations, PrimitiveSink primitiveSink) {
            hdfsScanRangeHasher.acceptScanRangeLocations(tScanRangeLocations, primitiveSink);
        }
    }

    private HashRing makeHashRing() {
        Set<ComputeNode> nodes = assignedScansPerComputeNode.keySet();
        HashRing hashRing = null;
        if (nodes.size() > kMaxNodeSizeUseRendezvousHashRing) {
            hashRing = new ConsistentHashRing(Hashing.murmur3_128(), new TScanRangeLocationsFunnel(),
                    new ComputeNodeFunnel(), nodes, kConsistenHashRingVirtualNumber);
        } else {
            hashRing = new RendezvousHashRing(Hashing.murmur3_128(), new TScanRangeLocationsFunnel(),
                    new ComputeNodeFunnel(),
                    nodes);
        }
        return hashRing;
    }

    // Sort scan ranges, to shuffle them between hosts.
    // Let's say sc1-h1, sc2-h1, sc3-h2, sc4-h2, then we will sort them as
    // sc1-h1, sc3-h2, sc2-h1, sc4-h2, so h1, h2 will be interleaved assigned.
    class ScanRagesSorter {
        Map<ComputeNode, List<TScanRangeLocations>> map = new HashMap<>();

        public void addScanRange(TScanRangeLocations scanRange, ComputeNode backend) {
            List<TScanRangeLocations> scanRanges;
            if (map.containsKey(backend)) {
                scanRanges = map.get(backend);
            } else {
                scanRanges = new ArrayList<>();
                map.put(backend, scanRanges);
            }
            scanRanges.add(scanRange);
        }

        public List<TScanRangeLocations> sort() {
            List<TScanRangeLocations> ans = new ArrayList<>();
            List<List<TScanRangeLocations>> ways = new ArrayList<>(map.values());
            Collections.sort(ways, (o1, o2) -> o2.size() - o1.size());
            while (ways.size() > 0) {
                // shuffle them between hosts.
                for (int i = 0; i < ways.size(); i++) {
                    List<TScanRangeLocations> way = ways.get(i);
                    ans.add(way.remove(way.size() - 1));
                }
                // remove empty list.
                while (ways.size() > 0) {
                    int last = ways.size() - 1;
                    if (ways.get(last).size() == 0) {
                        ways.remove(last);
                    } else {
                        break;
                    }
                }
            }
            return ans;
        }
    }

    private long computeAverageScanRangeBytes() {
        long size = 0;
        for (TScanRangeLocations scanRangeLocations : locations) {
            size += scanRangeLocations.scan_range.hdfs_scan_range.getLength();
        }
        return size / (locations.size() + 1);
    }

    @Override
    public void computeScanRangeAssignment() throws Exception {
        if (locations.size() == 0) {
            return;
        }

        long avgScanRangeBytes = computeAverageScanRangeBytes();
        long maxImbalanceBytes = avgScanRangeBytes * kMaxImbalanceRatio;

        // exclude non-alive or in-blacklist compute nodes.
        for (ComputeNode computeNode : computeNodes) {
            if (!computeNode.isAlive() || SimpleScheduler.isInBlacklist(computeNode.getId())) {
                continue;
            }
            assignedScansPerComputeNode.put(computeNode, 0L);
            hostToBackends.put(computeNode.getHost(), computeNode);
        }
        if (hostToBackends.isEmpty()) {
            throw new UserException(FeConstants.getNodeNotFoundError(chooseComputeNode));
        }

        // schedule scan ranges to co-located backends.
        // and put rest scan ranges into remote scan ranges.
        List<TScanRangeLocations> remoteScanRangeLocations = Lists.newArrayList();
        if (forceScheduleLocal) {
            for (int i = 0; i < locations.size(); ++i) {
                TScanRangeLocations scanRangeLocations = locations.get(i);
                Collection<ComputeNode> backends = new ArrayList<>();
                // select all backends that are co-located with this scan range.
                for (final TScanRangeLocation location : scanRangeLocations.getLocations()) {
                    Collection<ComputeNode> servers = hostToBackends.get(location.getServer().getHostname());
                    if (servers == null || servers.isEmpty()) {
                        continue;
                    }
                    backends.addAll(servers);
                }
                ComputeNode node = selectLeastScanBytesComputeNode(backends, 0);
                if (node == null) {
                    remoteScanRangeLocations.add(scanRangeLocations);
                } else {
                    recordScanRangeAssignment(node, scanRangeLocations);
                }
            }
        } else {
            remoteScanRangeLocations = locations;
        }
        if (remoteScanRangeLocations.isEmpty()) {
            return;
        }

        // use consistent hashing to schedule remote scan ranges
        HashRing hashRing = makeHashRing();

        // sort scan ranges
        ScanRagesSorter sorter = new ScanRagesSorter();
        for (TScanRangeLocations scanRange : remoteScanRangeLocations) {
            List<ComputeNode> backends = hashRing.get(scanRange, 1);
            sorter.addScanRange(scanRange, backends.get(0));
        }
        remoteScanRangeLocations = sorter.sort();

        // assign scan ranges.
        for (int i = 0; i < remoteScanRangeLocations.size(); ++i) {
            TScanRangeLocations scanRangeLocations = remoteScanRangeLocations.get(i);
            List<ComputeNode> backends = hashRing.get(scanRangeLocations, kCandidateNumber);
            ComputeNode node = selectLeastScanBytesComputeNode(backends, maxImbalanceBytes);
            if (node == null) {
                throw new RuntimeException("Failed to find backend to execute");
            }
            recordScanRangeAssignment(node, scanRangeLocations);
        }
    }

    private void recordScanRangeAssignment(ComputeNode node, TScanRangeLocations scanRangeLocations) {
        TNetworkAddress address = new TNetworkAddress(node.getHost(), node.getBePort());
        usedBackendIDs.add(node.getId());
        addressToBackendId.put(address, node.getId());

        // update statistic
        long addedScans = scanRangeLocations.scan_range.hdfs_scan_range.length;
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