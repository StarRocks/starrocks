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
import com.starrocks.catalog.IntVariant;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Range;
import com.starrocks.common.profile.Tracers;
import com.starrocks.lake.qe.scheduler.DefaultSharedDataWorkerProvider;
import com.starrocks.lake.qe.scheduler.SkipBlacklistSharedDataWorkerProvider;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.ExecGroup;
import com.starrocks.planner.ExecGroupId;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.QueryGroupManifest;
import com.starrocks.planner.QueryGroupManifestBuilder;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.assignment.BackendSelectorFactory;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TAggregationNode;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TLakeScanNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class QueryGroupBackendSelectorTest {
    public static final ComputeResource RESOURCE = new ComputeResource() {
        public long getWarehouseId() {
            return 7;
        }
        public long getWorkerGroupId() {
            return 9;
        }
    };

    public record Fixture(ConnectContext context, OlapScanNode scan, ExecutionFragment fragment,
                          Map<Long, ComputeNode> nodes, WorkerProvider provider) { }

    public static final class ConsumerNode extends PlanNode {
        public boolean needsFinalize = true;
        public boolean streaming;

        ConsumerNode(OlapScanNode scan) {
            super(new PlanNodeId(1), new ArrayList<>(List.of(new TupleId(0))), "AGGREGATE");
            addChild(scan);
        }

        @Override
        protected void toThrift(TPlanNode node) {
            node.setNode_type(TPlanNodeType.AGGREGATION_NODE);
            node.setAgg_node(new TAggregationNode(List.of(), 0, 0, needsFinalize)
                    .setUse_streaming_preaggregation(streaming).setUse_sort_agg(false)
                    .setUse_per_bucket_optimize(false));
        }

        @Override
        protected String getNodeExplainString(String prefix, com.starrocks.thrift.TExplainLevel level) {
            return "";
        }
    }

    private static Tuple tuple(long value) {
        return new Tuple(List.of(new IntVariant(IntegerType.BIGINT, value), new IntVariant(IntegerType.BIGINT, 0),
                new IntVariant(IntegerType.BIGINT, 0)));
    }

    public static Fixture fixture(int groups, int workers, int dop, long... weights) throws Exception {
        List<QueryGroupManifest.Source> sources = new ArrayList<>();
        for (int i = 0; i <= groups; i++) {
            int sourceIndex = 10 + i * 3; // Caller identity intentionally differs from list position.
            long weight = i == groups ? 0 : (weights.length == 0 ? 1 : weights[i]);
            var original = new TScanRangeLocations().setScan_range(new TScanRange().setInternal_scan_range(
                    new TInternalScanRange().setTablet_id(100 + i).setPartition_id(i == groups ? 2 : 1)
                            .setVersion("23").setSchema_hash("41").setRow_count(77)
                            .setVersion_hash("0").setDb_name("captured").setHosts(List.of())))
                    .setLocations(List.of(new TScanRangeLocation(new TNetworkAddress("host", 9000)).setBackend_id(2)));
            sources.add(new QueryGroupManifest.Source(sourceIndex, i == groups ? 2 : 1, 100 + i, 23, 41,
                    Range.gelt(tuple(i == groups ? 0 : i), tuple(i == groups ? groups : i + 1)),
                    original, weight, weight));
        }
        QueryGroupManifest manifest = QueryGroupManifestBuilder.build(0,
                new QueryGroupManifest.KeyDefinition(11, 12, List.of(3, 7, 9)), sources);
        OlapTable table = new OlapTable();
        table.setBaseIndexMetaId(0);
        table.setIndexMeta(0, "base", List.of(new com.starrocks.catalog.Column("k", IntegerType.BIGINT)),
                0, 0, (short) 1, com.starrocks.thrift.TStorageType.COLUMN, com.starrocks.sql.ast.KeysType.DUP_KEYS);
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        tuple.setTable(table);
        OlapScanNode scan = new OlapScanNode(new PlanNodeId(0), tuple, "GROUPED_SCAN", 0, RESOURCE) {
            @Override
            public QueryGroupManifest getQueryGroupManifest() {
                return manifest;
            }
            @Override
            public List<TScanRangeLocations> getScanRangeLocations(long maxLength) {
                return manifest.getSources().stream().map(QueryGroupManifest.Source::original).toList();
            }
            @Override
            public int getBucketNums() {
                throw new AssertionError("Grouped must bypass canonical bucket lookup");
            }
            @Override
            protected void toThrift(TPlanNode node) {
                node.setNode_type(TPlanNodeType.LAKE_GROUPED_SCAN_NODE);
                node.setLake_scan_node(new TLakeScanNode(0, List.of(), List.of(), false).setSorted_by_keys_per_tablet(false)
                        .setOutput_chunk_by_bucket(false));
            }
        };
        scan.setGroupedRangeScan(true);
        PlanFragment plan = new PlanFragment(new PlanFragmentId(0), new ConsumerNode(scan), DataPartition.RANDOM);
        plan.setPipelineDop(dop);
        plan.setParallelExecNum(1);
        ExecGroup group = new ExecGroup(new ExecGroupId(0));
        group.setColocateGroup();
        group.add(scan);
        group.add(plan.getPlanRoot());
        plan.assignColocateExecGroups(plan.getPlanRoot(), new ArrayList<>(List.of(group)));
        Map<Long, ComputeNode> nodes = new java.util.LinkedHashMap<>();
        for (long id = 1; id <= workers; id++) {
            ComputeNode node = new ComputeNode(id, "127.0.0." + id, 9000);
            node.setAlive(true);
            nodes.put(id, node);
        }
        ConnectContext context = new ConnectContext();
        context.setCurrentComputeResource(RESOURCE);
        context.getSessionVariable().setEnableGroupExecution(true);
        return new Fixture(context, scan, new ExecutionFragment(null, plan, 0), nodes,
                provider(nodes));
    }

    public static WorkerProvider provider(Map<Long, ComputeNode> nodes) {
        return new DefaultSharedDataWorkerProvider(ImmutableMap.copyOf(nodes), ImmutableMap.copyOf(nodes), RESOURCE);
    }

    public static void limit(Fixture f, int max, int scale) throws Exception {
        for (var setting : Map.of("groupExecutionMaxGroups", max, "groupExecutionGroupScale", scale).entrySet()) {
            var field = SessionVariable.class.getDeclaredField(setting.getKey());
            field.setAccessible(true);
            field.setInt(f.context().getSessionVariable(), setting.getValue());
        }
    }

    public static QueryGroupAssignment assign(Fixture f, WorkerProvider provider) throws Exception {
        var selector = BackendSelectorFactory.create(f.scan(), false, f.fragment(), provider, f.context(),
                new HashSet<>(), false);
        Assertions.assertInstanceOf(QueryGroupBackendSelector.class, selector);
        selector.computeScanRangeAssignment();
        Assertions.assertNull(f.fragment().getColocatedAssignment());
        return f.fragment().getQueryGroupAssignment();
    }

    @Test
    public void descendingWeightsBalanceWholeGroupsBeforeCacheTies() throws Exception {
        Fixture f = fixture(4, 2, 2, 7, 1, 5, 3);
        var assignment = assign(f, f.provider());
        Assertions.assertEquals(2L, assignment.getGroupToWorker().get(0));
        Assertions.assertEquals(1L, assignment.getGroupToWorker().get(2));
        Assertions.assertEquals(1L, assignment.getGroupToWorker().get(3));
        Assertions.assertEquals(2L, assignment.getGroupToWorker().get(1));
        Assertions.assertEquals(8, f.fragment().getScanRangeAssignment().values().stream()
                .flatMap(m -> m.values().stream()).mapToInt(List::size).sum());
        for (var ranges : f.fragment().getScanRangeAssignment().values()) {
            for (var task : ranges.get(0)) {
                Assertions.assertEquals("23", task.getScan_range().getInternal_scan_range().getVersion());
                Assertions.assertEquals("41", task.getScan_range().getInternal_scan_range().getSchema_hash());
                Assertions.assertEquals(77, task.getScan_range().getInternal_scan_range().getRow_count());
            }
        }
        f.scan().getQueryGroupManifest().getSources().forEach(s ->
                Assertions.assertFalse(s.original().getScan_range().isSetGrouped_range_task()));
    }

    @Test
    public void tiesUnknownEstimatesAndOverflowRemainDeterministic() throws Exception {
        for (long[] weights : List.of(new long[] {0, -1, 0, -1},
                new long[] {Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE})) {
            Fixture a = fixture(4, 2, 2, weights);
            var first = assign(a, a.provider());
            Fixture b = fixture(4, 2, 2, weights);
            var reverse = new java.util.LinkedHashMap<Long, ComputeNode>();
            reverse.put(2L, b.nodes().get(2L));
            reverse.put(1L, b.nodes().get(1L));
            Assertions.assertEquals(first.getGroupToWorker(), assign(b, provider(reverse)).getGroupToWorker());
            Assertions.assertEquals(List.of(2, 2), first.getWorkerToGroups().values().stream().map(List::size).sorted().toList());
        }
    }

    @Test
    public void capacityIsPerOwnerAndIncludesPaddedSlots() throws Exception {
        Fixture distributed = fixture(16, 2, 8);
        limit(distributed, 8, 1);
        Assertions.assertEquals(2, assign(distributed, distributed.provider()).getWorkerToGroups().size());
        Fixture insufficient = fixture(9, 1, 8);
        limit(insufficient, 8, 1);
        Assertions.assertThrows(NonRecoverableException.class, () -> assign(insufficient, insufficient.provider()));
        Assertions.assertNull(insufficient.fragment().getQueryGroupAssignment());
        Assertions.assertTrue(insufficient.fragment().getScanRangeAssignment().isEmpty());
        Fixture rounded = fixture(9, 1, 8);
        limit(rounded, 15, 10);
        Assertions.assertThrows(NonRecoverableException.class, () -> assign(rounded, rounded.provider()));
        Fixture defaultCap = fixture(127, 1, 9);
        limit(defaultCap, 128, 100);
        Assertions.assertThrows(NonRecoverableException.class, () -> assign(defaultCap, defaultCap.provider()));
    }

    @Test
    public void workerSnapshotBlacklistAndSkipBehaviorAreRespected() throws Exception {
        Fixture f = fixture(4, 3, 2);
        SimpleScheduler.addToBlocklist(2L);
        try {
            Assertions.assertFalse(assign(f, f.provider()).getWorkerToGroups().containsKey(2L));
            Fixture skipFixture = fixture(4, 3, 2);
            var all = ImmutableMap.copyOf(skipFixture.nodes());
            WorkerProvider skip = new SkipBlacklistSharedDataWorkerProvider(all, all, RESOURCE,
                    SessionVariableConstants.BlacklistBackupRoutingPolicy.getDefault()) { };
            Assertions.assertTrue(assign(skipFixture, skip).getWorkerToGroups().containsKey(2L));
        } finally {
            SimpleScheduler.removeFromBlocklist(2L);
        }
        Fixture scoped = fixture(4, 3, 2);
        var all = ImmutableMap.copyOf(scoped.nodes());
        var provider = new DefaultSharedDataWorkerProvider(all, ImmutableMap.of(1L, all.get(1L)), RESOURCE);
        Assertions.assertEquals(java.util.Set.of(1L), assign(scoped, provider).getWorkerToGroups().keySet());
    }

    @Test
    public void resourceMismatchAndUnavailableOwnerPublishNothing() throws Exception {
        for (boolean mismatch : List.of(true, false)) {
            Fixture f = fixture(4, 2, 2);
            var all = ImmutableMap.copyOf(f.nodes());
            ComputeResource resource = mismatch ? new ComputeResource() {
                public long getWarehouseId() {
                    return 8;
                }
                public long getWorkerGroupId() {
                    return 9;
                }
            } : RESOURCE;
            WorkerProvider provider = new DefaultSharedDataWorkerProvider(all, all, resource) {
                @Override
                public ComputeNode getWorkerById(long id) {
                    ComputeNode node = super.getWorkerById(id);
                    node.setAlive(false);
                    return node;
                }
            };
            Assertions.assertThrows(NonRecoverableException.class, () -> assign(f, provider));
            Assertions.assertNull(f.fragment().getQueryGroupAssignment());
            Assertions.assertTrue(f.fragment().getScanRangeAssignment().isEmpty());
            Assertions.assertTrue(provider.getSelectedWorkerIds().isEmpty());
        }
    }

    @Test
    public void immutablePlacementRejectsIncompleteOrDuplicateOwnership() throws Exception {
        Fixture f = fixture(3, 1, 8);
        var valid = assign(f, f.provider());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> valid.getGroupToWorker().clear());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> valid.getWorkerToGroups().get(1L).clear());
        Assertions.assertThrows(IllegalArgumentException.class, () -> new QueryGroupAssignment(0,
                f.scan().getQueryGroupManifest(), Map.of(0, 1L), Map.of(1L, List.of(0))));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new QueryGroupAssignment(0,
                f.scan().getQueryGroupManifest(), valid.getGroupToWorker(), Map.of(1L, List.of(0, 1, 1, 2))));
    }

    @Test
    public void eligibleMembershipIsFrozenAndNewlyBlacklistedOwnerFailsBeforeRegistration() throws Exception {
        for (boolean blacklist : List.of(false, true)) {
            Fixture f = fixture(4, 3, 2);
            var all = ImmutableMap.copyOf(f.nodes());
            int[] captures = {0};
            WorkerProvider provider = new DefaultSharedDataWorkerProvider(all, all, RESOURCE) {
                @Override
                public java.util.Collection<ComputeNode> getAllWorkers() {
                    captures[0]++;
                    return List.of(all.get(1L), all.get(2L));
                }
                @Override
                public ComputeNode getWorkerById(long id) {
                    if (blacklist) {
                        SimpleScheduler.addToBlocklist(id);
                    }
                    return super.getWorkerById(id);
                }
            };
            try {
                if (blacklist) {
                    Assertions.assertThrows(NonRecoverableException.class, () -> assign(f, provider));
                    Assertions.assertTrue(provider.getSelectedWorkerIds().isEmpty());
                    Assertions.assertTrue(f.fragment().getScanRangeAssignment().isEmpty());
                } else {
                    var assigned = assign(f, provider);
                    Assertions.assertEquals(java.util.Set.of(1L, 2L), assigned.getWorkerToGroups().keySet());
                }
                Assertions.assertEquals(1, captures[0]);
            } finally {
                f.nodes().keySet().forEach(SimpleScheduler::removeFromBlocklist);
            }
        }
    }

    @Test
    public void invalidCapacityAndNonSharedProviderRejectBeforePublishing() throws Exception {
        for (int max : List.of(0, -1)) {
            Fixture f = fixture(3, 1, 8);
            limit(f, max, 1);
            Assertions.assertThrows(NonRecoverableException.class, () -> assign(f, f.provider()));
            Assertions.assertTrue(f.provider().getSelectedWorkerIds().isEmpty());
        }
        Fixture empty = fixture(3, 0, 8);
        Assertions.assertThrows(NonRecoverableException.class, () -> assign(empty, empty.provider()));
        Fixture f = fixture(3, 1, 8);
        var all = ImmutableMap.copyOf(f.nodes());
        var provider = new com.starrocks.qe.scheduler.DefaultWorkerProvider(all, ImmutableMap.of(), all,
                ImmutableMap.of(), false, RESOURCE);
        Assertions.assertThrows(NonRecoverableException.class, () -> assign(f, provider));
    }

    @Test
    public void boundedPlacementDiagnosticsDescribeActualWork() throws Exception {
        Tracers.register();
        Tracers.init(Tracers.Mode.VARS, Tracers.Module.SCHEDULER, false, false);
        try {
            Fixture f = fixture(3, 2, 8);
            assign(f, f.provider());
            var values = new HashMap<String, String>();
            Tracers.getAllVars().forEach(v -> values.put(v.getName(), String.valueOf(v.getValue())));
            Assertions.assertEquals("WEIGHT_DESC_MIN_LOAD_V1", values.get("GroupedRangeScan.PlacementPolicy"));
            Assertions.assertEquals("2", values.get("GroupedRangeScan.ActualOwnerCount"));
            Assertions.assertEquals("3", values.get("GroupedRangeScan.GroupCount"));
            Assertions.assertEquals("6", values.get("GroupedRangeScan.TaskCount"));
            Assertions.assertNotNull(values.get("GroupedRangeScan.WeightQuality"));
        } finally {
            Tracers.close();
        }
    }
}
