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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.lake.qe.scheduler.DefaultSharedDataWorkerProvider;
import com.starrocks.lake.qe.scheduler.SkipBlacklistSharedDataWorkerProvider;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Assigns real native tablet tasks to stable homes within the captured shared-data resource. */
public class TabletParallelBackendSelector implements BackendSelector {
    private static final HashFunction HASH = Hashing.murmur3_128(0);

    private final OlapScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final FragmentScanRangeAssignment assignment;
    private final WorkerProvider workerProvider;

    public TabletParallelBackendSelector(OlapScanNode scanNode, List<TScanRangeLocations> locations,
                                         FragmentScanRangeAssignment assignment, WorkerProvider workerProvider) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.workerProvider = workerProvider;
    }

    @Override
    public void computeScanRangeAssignment() throws StarRocksException {
        if (!(workerProvider instanceof DefaultSharedDataWorkerProvider)) {
            throw unsupported("requires a shared-data worker provider");
        }
        ComputeResource resource = workerProvider.getComputeResource();
        ComputeResource scanResource = scanNode.getComputeResource();
        if (resource == null || scanResource == null) {
            throw unsupported("requires a captured compute resource");
        }
        long warehouseId = resource.getWarehouseId();
        long workerGroupId = resource.getWorkerGroupId();
        if (warehouseId != scanResource.getWarehouseId() || workerGroupId != scanResource.getWorkerGroupId()) {
            throw unsupported("scan and worker provider compute resources differ");
        }
        validateTasks();
        if (locations.isEmpty()) {
            recordPlacement(0);
            return;
        }

        // getAllWorkers is the selected resource's available snapshot, never the global CN inventory.
        List<ComputeNode> candidates = workerProvider.getAllWorkers().stream().filter(this::isEligible).toList();
        requireCapacity(candidates);
        FragmentScanRangeAssignment pending = new FragmentScanRangeAssignment();
        for (TScanRangeLocations task : locations) {
            long tabletId = task.getScan_range().getInternal_scan_range().getTablet_id();
            ComputeNode home = null;
            long bestScore = 0;
            for (ComputeNode candidate : candidates) {
                long score = HASH.newHasher().putInt(1).putLong(warehouseId).putLong(workerGroupId)
                        .putLong(tabletId).putLong(candidate.getId()).hash().asLong();
                if (home == null || Long.compareUnsigned(score, bestScore) > 0
                        || (score == bestScore && candidate.getId() < home.getId())) {
                    home = candidate;
                    bestScore = score;
                }
            }
            pending.put(home.getId(), scanNode.getId().asInt(), new TScanRangeParams(task.getScan_range()));
        }

        // Recheck just before normal worker registration. A later race follows ordinary deploy-failure handling.
        requireCapacity(candidates.stream().filter(this::isEligible).toList());
        for (long workerId : pending.keySet()) {
            if (!isEligible(workerProvider.getWorkerById(workerId))) {
                throw unsupported("selected tablet home became unavailable before registration");
            }
            workerProvider.selectWorker(workerId);
        }
        pending.forEach((worker, scans) -> scans.forEach((scan, ranges) -> assignment.putAll(worker, scan, ranges)));
        recordPlacement(pending.size());
    }

    private void validateTasks() throws NonRecoverableException {
        if (locations == null) {
            throw unsupported("missing native scan tasks");
        }
        Set<Long> tablets = new HashSet<>();
        for (TScanRangeLocations task : locations) {
            if (task == null || task.getScan_range() == null || !task.getScan_range().isSetInternal_scan_range()) {
                throw unsupported("malformed native tablet task");
            }
            TInternalScanRange range = task.getScan_range().getInternal_scan_range();
            if (!range.isSetTablet_id() || range.getTablet_id() <= 0 || !range.isSetVersion()
                    || range.getVersion().isEmpty() || !range.isSetSchema_hash() || range.getSchema_hash().isEmpty()) {
                throw unsupported("malformed native tablet identity, version or schema");
            }
            if (!tablets.add(range.getTablet_id())) {
                throw unsupported("duplicate native tablet task within one scan");
            }
        }
    }

    private boolean isEligible(ComputeNode worker) {
        return worker != null && worker.isAlive()
                && (workerProvider instanceof SkipBlacklistSharedDataWorkerProvider
                || !SimpleScheduler.isInBlocklist(worker.getId()));
    }

    private void requireCapacity(List<ComputeNode> candidates) throws NonRecoverableException {
        if (candidates.size() < 2) {
            throw unsupported("requires at least two eligible CNs in the selected compute resource");
        }
    }

    private void recordPlacement(int owners) {
        Tracers.record(Tracers.Module.SCHEDULER, "ParallelRangeScan.PlacementPolicy", "HRW_V1");
        Tracers.record(Tracers.Module.SCHEDULER, "ParallelRangeScan.TaskCount", String.valueOf(locations.size()));
        Tracers.record(Tracers.Module.SCHEDULER, "ParallelRangeScan.ActualOwnerCount", String.valueOf(owners));
    }

    private static NonRecoverableException unsupported(String reason) {
        return new NonRecoverableException("range_scan_execution_mode=parallel: " + reason);
    }
}
