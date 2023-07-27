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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.UserException;
import com.starrocks.common.util.ConsistentHashRing;
import com.starrocks.common.util.HashRing;
import com.starrocks.common.util.RendezvousHashRing;
import com.starrocks.planner.DeltaLakeScanNode;
import com.starrocks.planner.FileTableScanNode;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.HudiScanNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PaimonScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    public static final Logger LOG = LogManager.getLogger(HDFSBackendSelector.class);
    // be -> assigned scans
    Map<ComputeNode, Long> assignedScansPerComputeNode = Maps.newHashMap();
    // be host -> bes
    Multimap<String, ComputeNode> hostToBackends = HashMultimap.create();
    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final FragmentScanRangeAssignment assignment;
    private final WorkerProvider workerProvider;
    private final boolean forceScheduleLocal;
    private final boolean shuffleScanRange;
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
            } else if (scanNode instanceof PaimonScanNode) {
                PaimonScanNode node = (PaimonScanNode) scanNode;
                predicates = node.getScanNodePredicates();
                basePath = node.getPaimonTable().getTableLocation();
            } else {
                Preconditions.checkState(false);
            }
        }

        public void acceptScanRangeLocations(TScanRangeLocations tScanRangeLocations, PrimitiveSink primitiveSink) {
            THdfsScanRange hdfsScanRange = tScanRangeLocations.scan_range.hdfs_scan_range;
            if (hdfsScanRange.isSetFull_path()) {
                primitiveSink.putString(hdfsScanRange.full_path, StandardCharsets.UTF_8);
            } else {
                if (hdfsScanRange.isSetPartition_id() &&
                        predicates.getIdToPartitionKey().containsKey(hdfsScanRange.getPartition_id())) {
                    PartitionKey partitionKey = predicates.getIdToPartitionKey().get(hdfsScanRange.getPartition_id());
                    primitiveSink.putInt(partitionKey.hashCode());
                }
                if (hdfsScanRange.isSetRelative_path()) {
                    primitiveSink.putString(hdfsScanRange.relative_path, StandardCharsets.UTF_8);
                }
            }
            if (hdfsScanRange.isSetOffset()) {
                primitiveSink.putLong(hdfsScanRange.getOffset());
            }
        }
    }

    private final HdfsScanRangeHasher hdfsScanRangeHasher;

    public HDFSBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                               FragmentScanRangeAssignment assignment, WorkerProvider workerProvider,
                               boolean forceScheduleLocal,
                               boolean shuffleScanRange) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.workerProvider = workerProvider;
        this.forceScheduleLocal = forceScheduleLocal;
        this.hdfsScanRangeHasher = new HdfsScanRangeHasher();
        this.shuffleScanRange = shuffleScanRange;
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
            primitiveSink.putString(computeNode.getHost(), StandardCharsets.UTF_8);
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
                    new ComputeNodeFunnel(), nodes);
        }
        return hashRing;
    }

    private long computeAverageScanRangeBytes() {
        long size = 0;
        for (TScanRangeLocations scanRangeLocations : locations) {
            size += scanRangeLocations.scan_range.hdfs_scan_range.getLength();
        }
        return size / (locations.size() + 1);
    }

    @Override
    public void computeScanRangeAssignment() throws UserException {
        if (locations.size() == 0) {
            return;
        }

        long avgScanRangeBytes = computeAverageScanRangeBytes();
        long maxImbalanceBytes = avgScanRangeBytes * kMaxImbalanceRatio;

        for (ComputeNode computeNode : workerProvider.getAllWorkers()) {
            assignedScansPerComputeNode.put(computeNode, 0L);
            hostToBackends.put(computeNode.getHost(), computeNode);
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
        if (shuffleScanRange) {
            Collections.shuffle(remoteScanRangeLocations);
        }
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

    private void recordScanRangeAssignment(ComputeNode worker, TScanRangeLocations scanRangeLocations)
            throws NonRecoverableException {
        workerProvider.selectWorker(worker.getId());

        // update statistic
        long addedScans = scanRangeLocations.scan_range.hdfs_scan_range.length;
        assignedScansPerComputeNode.put(worker, assignedScansPerComputeNode.get(worker) + addedScans);

        // add scan range params
        TScanRangeParams scanRangeParams = new TScanRangeParams();
        scanRangeParams.scan_range = scanRangeLocations.scan_range;
        assignment.put(worker.getId(), scanNode.getId().asInt(), scanRangeParams);
    }
}