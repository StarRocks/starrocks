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
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.lake.qe.scheduler.DefaultSharedDataWorkerProvider;
import com.starrocks.lake.qe.scheduler.SkipBlacklistSharedDataWorkerProvider;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.assignment.BackendSelectorFactory;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class TabletParallelBackendSelectorTest {
    private static final ComputeResource RESOURCE = new ComputeResource() {
        public long getWarehouseId() {
            return 7;
        }
        public long getWorkerGroupId() {
            return 9;
        }
    };

    private static Map<Long, ComputeNode> nodes() {
        Map<Long, ComputeNode> nodes = new java.util.LinkedHashMap<>();
        for (long id = 1; id <= 4; id++) {
            ComputeNode node = new ComputeNode(id, "127.0.0." + id, 9000);
            node.setAlive(true);
            node.setWarehouseId(7);
            node.setWorkerGroupId(9);
            nodes.put(id, node);
        }
        return nodes;
    }

    private static WorkerProvider provider(Map<Long, ComputeNode> nodes) {
        return new DefaultSharedDataWorkerProvider(ImmutableMap.copyOf(nodes), ImmutableMap.copyOf(nodes), RESOURCE);
    }

    private static List<TScanRangeLocations> tasks() {
        List<TScanRangeLocations> tasks = new ArrayList<>();
        for (long tablet = 100; tablet < 228; tablet++) {
            TInternalScanRange range = new TInternalScanRange().setTablet_id(tablet).setVersion("13")
                    .setSchema_hash("17").setPartition_id(50).setRow_count(tablet * 3);
            tasks.add(new TScanRangeLocations().setScan_range(new TScanRange().setInternal_scan_range(range))
                    .setLocations(List.of(new TScanRangeLocation(new TNetworkAddress("127.0.0.1", 9000))
                            .setBackend_id(1))));
        }
        return tasks;
    }

    private static OlapScanNode scan(List<TScanRangeLocations> tasks) {
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        OlapTable table = new OlapTable();
        table.setBaseIndexMetaId(0);
        table.setIndexMeta(0, "base", List.of(new com.starrocks.catalog.Column("k", com.starrocks.type.IntegerType.INT)),
                0, 0, (short) 1, com.starrocks.thrift.TStorageType.COLUMN, com.starrocks.sql.ast.KeysType.DUP_KEYS);
        tuple.setTable(table);
        OlapScanNode scan = new OlapScanNode(new PlanNodeId(0), tuple, "OlapScanNode", 0, RESOURCE) {
            @Override
            public List<TScanRangeLocations> getScanRangeLocations(long maxLength) {
                return tasks;
            }
        };
        scan.setParallelRangeScan(true);
        return scan;
    }

    private static FragmentScanRangeAssignment assign(List<TScanRangeLocations> tasks, WorkerProvider provider)
            throws StarRocksException {
        OlapScanNode scan = scan(tasks);
        ExecutionFragment fragment = new ExecutionFragment(null,
                new PlanFragment(new PlanFragmentId(0), scan, DataPartition.RANDOM), 0);
        ConnectContext context = new ConnectContext();
        context.setCurrentComputeResource(RESOURCE);
        Assertions.assertEquals("colocate", context.getSessionVariable().getRangeScanExecutionMode());
        BackendSelector selector = BackendSelectorFactory.create(scan, false, fragment, provider, context,
                new HashSet<>(), false);
        Assertions.assertEquals("TabletParallelBackendSelector", selector.getClass().getSimpleName());
        selector.computeScanRangeAssignment();
        Assertions.assertNull(fragment.getColocatedAssignment());
        Assertions.assertEquals(fragment.getScanRangeAssignment().keySet(), new HashSet<>(provider.getSelectedWorkerIds()));
        return fragment.getScanRangeAssignment();
    }

    private static Map<Long, Long> owners(FragmentScanRangeAssignment assignment) {
        Map<Long, Long> owners = new HashMap<>();
        assignment.forEach((worker, scans) -> scans.values().forEach(ranges -> ranges.forEach(range ->
                Assertions.assertNull(owners.put(range.getScan_range().getInternal_scan_range().getTablet_id(), worker)))));
        return owners;
    }

    @Test
    public void capturedModeSpreadsPackTasksOnceAndPreservesPayload() throws Exception {
        List<TScanRangeLocations> tasks = tasks();
        FragmentScanRangeAssignment assignment = assign(tasks, provider(nodes()));
        Assertions.assertTrue(assignment.size() > 1);
        Assertions.assertEquals(128, owners(assignment).size());
        var actual = assignment.values().stream().flatMap(scans -> scans.values().stream())
                .flatMap(List::stream).map(param -> param.getScan_range()).sorted().toList();
        Assertions.assertEquals(tasks.stream().map(TScanRangeLocations::getScan_range).sorted().toList(), actual);
        // Captures format/seed, resource domain, unsigned comparison and persistent CN identity.
        Assertions.assertEquals(List.of(1L, 3L, 2L, 4L),
                List.of(owners(assignment).get(100L), owners(assignment).get(101L),
                        owners(assignment).get(102L), owners(assignment).get(103L)));
    }

    @Test
    public void orderAndVersionDoNotChangeHomes() throws Exception {
        Map<Long, Long> expected = owners(assign(tasks(), provider(nodes())));
        List<TScanRangeLocations> reordered = tasks();
        Collections.reverse(reordered);
        reordered.forEach(task -> task.getScan_range().getInternal_scan_range().setVersion("99").setPartition_id(51));
        List<ComputeNode> reverse = new ArrayList<>(nodes().values());
        Collections.reverse(reverse);
        Map<Long, ComputeNode> nodes = new java.util.LinkedHashMap<>();
        reverse.forEach(node -> nodes.put(node.getId(), node));
        Assertions.assertEquals(expected, owners(assign(reordered, provider(nodes))));
    }

    @Test
    public void failedHomeMovesOnlyItsTasksAndRecoveryRestoresHomes() throws Exception {
        Map<Long, ComputeNode> nodes = nodes();
        Map<Long, Long> healthy = owners(assign(tasks(), provider(nodes)));
        nodes.get(2L).setAlive(false);
        Map<Long, Long> failed = owners(assign(tasks(), provider(nodes)));
        healthy.forEach((tablet, home) -> {
            if (home != 2) {
                Assertions.assertEquals(home, failed.get(tablet));
            } else {
                Assertions.assertNotEquals(home, failed.get(tablet));
            }
        });
        nodes.get(2L).setAlive(true);
        SimpleScheduler.addToBlocklist(2L);
        try {
            Assertions.assertEquals(failed, owners(assign(tasks(), provider(nodes))));
        } finally {
            SimpleScheduler.removeFromBlocklist(2L);
        }
        Assertions.assertEquals(healthy, owners(assign(tasks(), provider(nodes))));
        nodes.remove(2L);
        Assertions.assertEquals(failed, owners(assign(tasks(), provider(nodes))));
        ComputeNode extra = new ComputeNode(5, "127.0.0.5", 9000);
        extra.setAlive(true);
        extra.setWarehouseId(7);
        extra.setWorkerGroupId(9);
        nodes.put(5L, extra);
        Map<Long, Long> added = owners(assign(tasks(), provider(nodes)));
        failed.forEach((tablet, home) -> Assertions.assertTrue(added.get(tablet).equals(home) || added.get(tablet) == 5));
    }

    @Test
    public void skipBlacklistProviderKeepsAliveHomesButExcludesDeadWorkers() throws Exception {
        var nodes = ImmutableMap.copyOf(nodes());
        Map<Long, Long> healthy = owners(assign(tasks(), provider(nodes)));
        for (long id : List.of(2L, 3L, 4L)) {
            SimpleScheduler.addToBlocklist(id);
        }
        try {
            Assertions.assertThrows(NonRecoverableException.class, () -> assign(tasks(), provider(nodes)));
            WorkerProvider skip = new SkipBlacklistSharedDataWorkerProvider(nodes, nodes, RESOURCE,
                    SessionVariableConstants.BlacklistBackupRoutingPolicy.getDefault()) { };
            Assertions.assertEquals(healthy, owners(assign(tasks(), skip)));
            nodes.get(2L).setAlive(false);
            WorkerProvider withoutDead = new SkipBlacklistSharedDataWorkerProvider(nodes, nodes, RESOURCE,
                    SessionVariableConstants.BlacklistBackupRoutingPolicy.getDefault()) { };
            Map<Long, Long> actual = owners(assign(tasks(), withoutDead));
            Assertions.assertEquals(128, actual.size());
            Assertions.assertFalse(actual.containsValue(2L));
            healthy.forEach((tablet, home) -> {
                if (home != 2) {
                    Assertions.assertEquals(home, actual.get(tablet));
                }
            });
        } finally {
            nodes.keySet().forEach(SimpleScheduler::removeFromBlocklist);
        }
    }

    @Test
    public void skipBlacklistProviderKeepsNewlyBlocklistedHomes() throws Exception {
        var nodes = ImmutableMap.copyOf(nodes());
        Map<Long, Long> healthy = owners(assign(tasks(), provider(nodes)));
        WorkerProvider skip = new SkipBlacklistSharedDataWorkerProvider(nodes, nodes, RESOURCE,
                SessionVariableConstants.BlacklistBackupRoutingPolicy.getDefault()) {
            @Override
            public ComputeNode getWorkerById(long workerId) {
                SimpleScheduler.addToBlocklist(workerId);
                return super.getWorkerById(workerId);
            }
        };
        try {
            Assertions.assertEquals(healthy, owners(assign(tasks(), skip)));
        } finally {
            nodes.keySet().forEach(SimpleScheduler::removeFromBlocklist);
        }
    }

    @Test
    public void mismatchedResourceAndUnsupportedProviderFailBeforeAssignment() throws Exception {
        var nodes = ImmutableMap.copyOf(nodes());
        ComputeResource other = new ComputeResource() {
            public long getWarehouseId() {
                return 8;
            }
            public long getWorkerGroupId() {
                return 9;
            }
        };
        var mismatched = new DefaultSharedDataWorkerProvider(nodes, nodes, other);
        Assertions.assertThrows(StarRocksException.class, () -> assign(tasks(), mismatched));
        Assertions.assertTrue(mismatched.getSelectedWorkerIds().isEmpty());
        var unsupported = new com.starrocks.qe.scheduler.DefaultWorkerProvider(nodes, ImmutableMap.of(),
                nodes, ImmutableMap.of(), false, RESOURCE);
        Assertions.assertThrows(StarRocksException.class, () -> assign(tasks(), unsupported));
        Assertions.assertTrue(unsupported.getSelectedWorkerIds().isEmpty());
        // An out-of-resource CN in the complete inventory is absent from the authorized snapshot.
        var scoped = new DefaultSharedDataWorkerProvider(nodes, ImmutableMap.of(1L, nodes.get(1L), 2L, nodes.get(2L)), RESOURCE);
        Assertions.assertEquals(java.util.Set.of(1L, 2L), assign(tasks(), scoped).keySet());
    }

    @Test
    public void placementDiagnosticsReportActualOwnersWithBoundedFields() throws Exception {
        Tracers.register();
        Tracers.init(Tracers.Mode.VARS, Tracers.Module.SCHEDULER, false, false);
        try {
            var assignment = assign(tasks(), provider(nodes()));
            var values = Tracers.getAllVars().stream().collect(java.util.stream.Collectors.toMap(
                    variable -> variable.getName(), variable -> variable.getValue()));
            Assertions.assertEquals(Map.of("ParallelRangeScan.PlacementPolicy", "HRW_V1",
                    "ParallelRangeScan.TaskCount", "128",
                    "ParallelRangeScan.ActualOwnerCount", String.valueOf(assignment.size())), values);
        } finally {
            Tracers.close();
        }
    }

    @Test
    public void homeBecomingIneligibleAfterCapturePublishesNoAssignment() {
        for (boolean blocklisted : List.of(false, true)) {
            var nodes = ImmutableMap.copyOf(nodes());
            WorkerProvider provider = new DefaultSharedDataWorkerProvider(nodes, nodes, RESOURCE) {
                @Override
                public ComputeNode getWorkerById(long workerId) {
                    // The selector first captures candidates and builds pending assignments, then looks up each home.
                    ComputeNode selected = super.getWorkerById(workerId);
                    if (blocklisted) {
                        SimpleScheduler.addToBlocklist(workerId);
                    } else {
                        selected.setAlive(false);
                    }
                    return selected;
                }
            };
            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            List<TScanRangeLocations> tasks = tasks();
            TabletParallelBackendSelector selector = new TabletParallelBackendSelector(scan(tasks), tasks,
                    assignment, provider);
            try {
                NonRecoverableException failure = Assertions.assertThrows(NonRecoverableException.class,
                        selector::computeScanRangeAssignment);
                Assertions.assertTrue(failure.getMessage().contains("became unavailable before registration"));
                Assertions.assertTrue(assignment.isEmpty());
                Assertions.assertTrue(provider.getSelectedWorkerIds().isEmpty());
            } finally {
                nodes.keySet().forEach(SimpleScheduler::removeFromBlocklist);
            }
        }
    }

    @Test
    public void invalidTasksAndCapacityFailWithoutAssignment() throws Exception {
        Assertions.assertTrue(assign(List.of(), provider(nodes())).isEmpty());
        List<TScanRangeLocations> duplicate = tasks();
        duplicate.add(duplicate.get(0).deepCopy());
        Assertions.assertThrows(StarRocksException.class, () -> assign(duplicate, provider(nodes())));
        Assertions.assertThrows(StarRocksException.class,
                () -> assign(List.of(new TScanRangeLocations()), provider(nodes())));
        Map<Long, ComputeNode> nodes = nodes();
        nodes.values().forEach(node -> node.setAlive(node.getId() == 1));
        Assertions.assertThrows(StarRocksException.class, () -> assign(tasks(), provider(nodes)));
        Assertions.assertThrows(StarRocksException.class, () -> assign(tasks(),
                new DefaultSharedDataWorkerProvider(ImmutableMap.of(), ImmutableMap.of(), null)));
    }
}
