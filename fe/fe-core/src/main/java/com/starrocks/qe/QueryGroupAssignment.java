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

import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.QueryGroupManifest;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.thrift.TPlanFragment;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Immutable ownership of every complete query group, independent of canonical tablet buckets. */
public final class QueryGroupAssignment {
    private final int scanNodeId;
    private final QueryGroupManifest manifest;
    private final Map<Integer, Long> groupToWorker;
    private final Map<Long, List<Integer>> workerToGroups;

    public QueryGroupAssignment(int scanNodeId, QueryGroupManifest manifest, Map<Integer, Long> groupToWorker,
                                Map<Long, List<Integer>> workerToGroups) {
        if (manifest == null || manifest.getScanNodeId() != scanNodeId || groupToWorker == null || workerToGroups == null) {
            throw new IllegalArgumentException("range_scan_execution_mode=grouped: invalid scan ownership identity");
        }
        this.scanNodeId = scanNodeId;
        this.manifest = manifest;
        this.groupToWorker = Map.copyOf(groupToWorker);
        this.workerToGroups = workerToGroups.entrySet().stream().collect(Collectors.toUnmodifiableMap(
                Map.Entry::getKey, e -> e.getValue().stream().sorted().toList()));
        Set<Integer> seen = new HashSet<>();
        for (var owner : this.workerToGroups.entrySet()) {
            if (owner.getKey() <= 0 || owner.getValue().isEmpty()) {
                throw new IllegalArgumentException("range_scan_execution_mode=grouped: invalid or empty owner");
            }
            for (int group : owner.getValue()) {
                if (!seen.add(group) || !owner.getKey().equals(this.groupToWorker.get(group))) {
                    throw new IllegalArgumentException("range_scan_execution_mode=grouped: duplicate or inconsistent ownership");
                }
            }
        }
        Set<Integer> expected = manifest.getGroups().stream().map(QueryGroupManifest.Group::groupId).collect(Collectors.toSet());
        if (!seen.equals(expected) || !this.groupToWorker.keySet().equals(expected)) {
            throw new IllegalArgumentException("range_scan_execution_mode=grouped: incomplete group ownership");
        }
    }

    public int getScanNodeId() {
        return scanNodeId;
    }

    public QueryGroupManifest getManifest() {
        return manifest;
    }

    public Map<Integer, Long> getGroupToWorker() {
        return groupToWorker;
    }

    public Map<Long, List<Integer>> getWorkerToGroups() {
        return workerToGroups;
    }

    /** Capacity includes padding and applies independently to each owner's one fragment instance. */
    public static int ownerCapacity(SessionVariable variables, int requestedDop) {
        require(requestedDop > 0 && variables.getGroupExecutionMaxGroups() > 0
                && variables.getGroupExecutionGroupScale() > 0, "group capacity and requested DOP must be positive");
        long limit = Math.min((long) variables.getGroupExecutionMaxGroups(),
                (long) variables.getGroupExecutionGroupScale() * requestedDop);
        return (int) (limit < requestedDop ? limit : (limit / requestedDop) * requestedDop);
    }

    public static int physicalDop(int groups, int requestedDop) {
        require(groups > 0 && requestedDop > 0, "an owner requires groups and positive DOP");
        return Math.min(groups, requestedDop);
    }

    public static int logicalDop(int groups, int requestedDop) {
        int physical = physicalDop(groups, requestedDop);
        return ((groups + physical - 1) / physical) * physical;
    }

    /** Recheck the emitted final consumer and actual execution-group protocol at scheduling/deployment handoff. */
    public void validatePlan(TPlanFragment fragment) {
        require(fragment != null && fragment.isSetPlan(), "missing grouped fragment plan");
        var nodes = fragment.getPlan().getNodes();
        var scans = nodes.stream().filter(n -> n.getNode_id() == scanNodeId).toList();
        require(scans.size() == 1 && scans.get(0).getNode_type() == TPlanNodeType.LAKE_GROUPED_SCAN_NODE
                && scans.get(0).isSetLake_scan_node(), "grouped scan must retain node46");
        var scan = scans.get(0).getLake_scan_node();
        require(!scan.isSorted_by_keys_per_tablet() && !scan.isOutput_chunk_by_bucket() && !scan.isSetOutput_asc_hint(),
                "grouped scan requires unordered complete-driver input");
        var aggregates = nodes.stream().filter(n -> n.isSetAgg_node()).toList();
        require(aggregates.size() == 1, "exactly one HASH consumer is required");
        var aggregate = aggregates.get(0);
        var agg = aggregate.getAgg_node();
        require(aggregate.getNode_type() == TPlanNodeType.AGGREGATION_NODE && agg.isNeed_finalize()
                && !agg.isUse_streaming_preaggregation() && !agg.isUse_sort_agg() && !agg.isUse_per_bucket_optimize(),
                "blocking finalized HASH execution is required");
        require(fragment.isSetGroup_execution_param() && fragment.getGroup_execution_param().isEnable_group_execution()
                && fragment.getGroup_execution_param().isSetExec_groups()
                && fragment.getGroup_execution_param().getExec_groups().stream().anyMatch(g -> g.isSetPlan_node_ids()
                && g.getPlan_node_ids().contains(scanNodeId) && g.getPlan_node_ids().contains(aggregate.getNode_id())),
                "scan and HASH consumer must share one execution group");
    }

    /** Validate exact fixed queues, including source versions and empty padding, before sending any instance. */
    public void validateLayout(ExecutionFragment fragment, SessionVariable variables) {
        require(fragment.getQueryGroupAssignment() == this && fragment.getScanNodes().size() == 1
                && fragment.getScanNode(new PlanNodeId(scanNodeId)) instanceof OlapScanNode scan
                && scan.isGroupedRangeScan() && scan.getQueryGroupManifest() == manifest,
                "fragment lost its captured grouped scan assignment");
        var plan = fragment.getPlanFragment();
        require(plan.getParallelExecNum() == 1 && !plan.isWithLocalShuffle() && !plan.isUseRuntimeAdaptiveDop(),
                "one fixed-DOP instance per owner without local shuffle is required");
        int requested = plan.getPipelineDop();
        int capacity = ownerCapacity(variables, requested);
        require(fragment.getInstances().size() == workerToGroups.size(), "one instance per owner is required");
        Set<Long> seenOwners = new HashSet<>();
        Map<Integer, List<TScanRange>> expected = manifest.getGroups().stream().collect(Collectors.toMap(
                QueryGroupManifest.Group::groupId,
                g -> g.tasks().stream().map(manifest::toScanRange).toList()));
        for (var instance : fragment.getInstances()) {
            long owner = instance.getWorkerId();
            List<Integer> groups = workerToGroups.get(owner);
            require(groups != null && seenOwners.add(owner), "duplicate or unexpected grouped instance owner");
            int physical = physicalDop(groups.size(), requested);
            int logical = logicalDop(groups.size(), requested);
            require(logical <= capacity && instance.getPipelineDop() == physical
                    && instance.getGroupExecutionScanDop() == logical, "invalid grouped physical/logical driver capacity");
            require(instance.getNode2ScanRanges().isEmpty()
                    && instance.getNode2DriverSeqToScanRanges().keySet().equals(Set.of(scanNodeId)),
                    "grouped input must use individual driver queues");
            var layout = instance.getDeployedScanRangeLayout(scanNodeId).orElse(null);
            require(layout != null && layout.isPerDriverSeq() && layout.getDriverSeqCount() == logical,
                    "missing fixed grouped driver layout");
            var queues = instance.getNode2DriverSeqToScanRanges().get(scanNodeId);
            require(queues.size() == logical && instance.getBucketSeqToDriverSeq().size() == groups.size(),
                    "incomplete grouped queues or bucket ownership");
            for (int driver = 0; driver < logical; driver++) {
                var queue = queues.get(driver);
                require(queue != null && queue.stream().noneMatch(p -> p.isHas_more() || p.isEmpty()),
                        "grouped queues must be complete and one-shot");
                if (driver >= groups.size()) {
                    require(queue.isEmpty(), "padding must be a truly empty driver queue");
                    continue;
                }
                int group = groups.get(driver);
                require(Integer.valueOf(driver).equals(instance.getDriverSeqOfBucketSeq(group)),
                        "group owner and logical driver disagree");
                var actual = queue.stream().map(p -> p.getScan_range()).toList();
                require(actual.equals(expected.get(group)), "incomplete, duplicated or altered captured group tasks");
            }
        }
    }

    private static void require(boolean condition, String reason) {
        if (!condition) {
            throw new IllegalStateException("range_scan_execution_mode=grouped: " + reason);
        }
    }
}
