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
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.lake.qe.scheduler.DefaultSharedDataWorkerProvider;
import com.starrocks.lake.qe.scheduler.SkipBlacklistSharedDataWorkerProvider;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.QueryGroupManifest;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TScanRangeParams;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Places complete captured C groups by descending approximate weight within a frozen resource snapshot. */
public class QueryGroupBackendSelector implements BackendSelector {
    private static final HashFunction HASH = Hashing.murmur3_128(0);
    private final OlapScanNode scanNode;
    private final ExecutionFragment fragment;
    private final WorkerProvider workerProvider;
    private final SessionVariable variables;

    public QueryGroupBackendSelector(OlapScanNode scanNode, ExecutionFragment fragment,
                                     WorkerProvider workerProvider, SessionVariable variables) {
        this.scanNode = scanNode;
        this.fragment = fragment;
        this.workerProvider = workerProvider;
        this.variables = variables;
    }

    @Override
    public void computeScanRangeAssignment() throws StarRocksException {
        if (!(workerProvider instanceof DefaultSharedDataWorkerProvider)) {
            throw unsupported("requires a shared-data worker provider");
        }
        var resource = workerProvider.getComputeResource();
        var scanResource = scanNode.getComputeResource();
        if (resource == null || scanResource == null || resource.getWarehouseId() != scanResource.getWarehouseId()
                || resource.getWorkerGroupId() != scanResource.getWorkerGroupId()) {
            throw unsupported("scan and worker provider compute resources differ");
        }
        QueryGroupManifest manifest = scanNode.getQueryGroupManifest();
        if (manifest == null || fragment.getQueryGroupAssignment() != null || !fragment.getScanRangeAssignment().isEmpty()) {
            throw unsupported("requires one captured manifest and an unassigned one-shot fragment");
        }
        int capacity;
        try {
            capacity = QueryGroupAssignment.ownerCapacity(variables, fragment.getPlanFragment().getPipelineDop());
        } catch (IllegalStateException invalid) {
            throw unsupported("group capacity and requested DOP must be positive");
        }
        // Capture exactly once. Later joins/recovery cannot expand this query's resource-scoped candidate set.
        List<ComputeNode> candidates = List.copyOf(workerProvider.getAllWorkers().stream()
                .filter(this::isEligible).sorted(Comparator.comparingLong(ComputeNode::getId)).toList());
        if (candidates.isEmpty() || (long) candidates.size() * capacity < manifest.getGroupCount()) {
            throw unsupported("insufficient eligible owner capacity including padded logical drivers");
        }
        Map<Long, BigInteger> loads = new HashMap<>();
        Map<Long, List<Integer>> owners = new LinkedHashMap<>();
        candidates.forEach(c -> loads.put(c.getId(), BigInteger.ZERO));
        Map<Integer, Long> groupToWorker = new HashMap<>();
        Map<Integer, Set<Long>> sourceLocations = new HashMap<>();
        manifest.getSources().forEach(source -> {
            var original = source.original();
            sourceLocations.put(source.sourceIndex(), original.isSetLocations() ? original.getLocations().stream()
                    .map(location -> location.getBackend_id()).collect(Collectors.toSet()) : Set.of());
        });
        List<QueryGroupManifest.Group> groups = new ArrayList<>(manifest.getGroups());
        groups.sort(Comparator.comparingLong(QueryGroupManifest.Group::estimatedWeight).reversed()
                .thenComparing(g -> g.interval().getLowerBound(), Comparator.nullsFirst(Comparator.naturalOrder()))
                .thenComparing(g -> g.interval().getUpperBound(), Comparator.nullsLast(Comparator.naturalOrder()))
                .thenComparingInt(QueryGroupManifest.Group::groupId));
        for (var group : groups) {
            Range<Tuple> interval = group.interval();
            Map<Long, Integer> localityByOwner = new HashMap<>();
            group.tasks().forEach(task -> sourceLocations.get(task.sourceIndex()).forEach(
                    owner -> localityByOwner.merge(owner, 1, Integer::sum)));
            ComputeNode best = null;
            int bestLocality = -1;
            long bestScore = 0;
            for (ComputeNode candidate : candidates) {
                long id = candidate.getId();
                if (owners.getOrDefault(id, List.of()).size() >= capacity) {
                    continue;
                }
                int locality = localityByOwner.getOrDefault(id, 0);
                long score = preference(manifest, interval, id);
                int loadOrder = best == null ? -1 : loads.get(id).compareTo(loads.get(best.getId()));
                if (best == null || loadOrder < 0 || (loadOrder == 0 && (locality > bestLocality
                        || (locality == bestLocality && (Long.compareUnsigned(score, bestScore) > 0
                        || (score == bestScore && id < best.getId())))))) {
                    best = candidate;
                    bestLocality = locality;
                    bestScore = score;
                }
            }
            if (best == null) {
                throw unsupported("insufficient eligible owner capacity");
            }
            long id = best.getId();
            owners.computeIfAbsent(id, ignored -> new ArrayList<>()).add(group.groupId());
            groupToWorker.put(group.groupId(), id);
            loads.put(id, loads.get(id).add(BigInteger.valueOf(group.estimatedWeight())));
        }
        QueryGroupAssignment pending = new QueryGroupAssignment(scanNode.getId().asInt(), manifest, groupToWorker, owners);
        pending.validatePlan(fragment.getPlanFragment().toThrift());
        FragmentScanRangeAssignment ranges = new FragmentScanRangeAssignment();
        for (var task : manifest.getTasks()) {
            ranges.put(groupToWorker.get(task.groupId()), scanNode.getId().asInt(),
                    new TScanRangeParams(manifest.toScanRange(task)).setHas_more(false));
        }
        // Check every owner before registering any; publishing assignment/ranges is the final step.
        for (long owner : pending.getWorkerToGroups().keySet()) {
            if (!isEligible(workerProvider.getWorkerById(owner))) {
                throw unsupported("selected group owner became unavailable before registration");
            }
        }
        for (long owner : pending.getWorkerToGroups().keySet()) {
            workerProvider.selectWorker(owner);
        }
        ranges.forEach((worker, scans) -> scans.forEach((scan, tasks) ->
                fragment.getScanRangeAssignment().putAll(worker, scan, tasks)));
        fragment.setQueryGroupAssignment(pending);
        Tracers.record(Tracers.Module.SCHEDULER, "GroupedRangeScan.PlacementPolicy", "WEIGHT_DESC_MIN_LOAD_V1");
        Tracers.record(Tracers.Module.SCHEDULER, "GroupedRangeScan.ActualOwnerCount", String.valueOf(owners.size()));
        Tracers.record(Tracers.Module.SCHEDULER, "GroupedRangeScan.GroupCount", String.valueOf(manifest.getGroupCount()));
        Tracers.record(Tracers.Module.SCHEDULER, "GroupedRangeScan.TaskCount", String.valueOf(manifest.getTaskCount()));
        long bytes = manifest.getSources().stream().filter(s -> s.estimatedBytes() > 0).count();
        long rows = manifest.getSources().stream().filter(s -> s.estimatedBytes() <= 0 && s.estimatedRows() > 0).count();
        Tracers.record(Tracers.Module.SCHEDULER, "GroupedRangeScan.WeightQuality",
                "approximate:bytes=" + bytes + ",rows=" + rows + ",floor=" + (manifest.getSourceCount() - bytes - rows));
        Tracers.record(Tracers.Module.SCHEDULER, "GroupedRangeScan.EstimatedTotalWeight",
                loads.values().stream().reduce(BigInteger.ZERO, BigInteger::add).toString());
    }

    private long preference(QueryGroupManifest manifest, Range<Tuple> interval, long owner) {
        var resource = workerProvider.getComputeResource();
        Hasher hash = HASH.newHasher().putInt(1).putLong(resource.getWarehouseId()).putLong(resource.getWorkerGroupId())
                .putLong(manifest.getKeys().tableId()).putLong(manifest.getKeys().indexMetaId());
        manifest.getKeys().orderedCUniqueIds().forEach(hash::putInt);
        putBound(hash, interval.getLowerBound());
        putBound(hash, interval.getUpperBound());
        return hash.putLong(owner).hash().asLong();
    }

    private static void putBound(Hasher hash, Tuple bound) {
        hash.putBoolean(bound != null);
        if (bound != null) {
            // Manifest endpoints have exactly three validated BIGINTs; no string/locale-dependent encoding.
            bound.getValues().forEach(value -> hash.putLong(value.getLongValue()));
        }
    }

    private boolean isEligible(ComputeNode worker) {
        return worker != null && worker.isAlive() && (workerProvider instanceof SkipBlacklistSharedDataWorkerProvider
                || !SimpleScheduler.isInBlocklist(worker.getId()));
    }

    private static NonRecoverableException unsupported(String reason) {
        return new NonRecoverableException("range_scan_execution_mode=grouped: " + reason);
    }
}
