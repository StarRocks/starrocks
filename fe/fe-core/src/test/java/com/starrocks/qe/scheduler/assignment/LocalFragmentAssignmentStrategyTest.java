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

package com.starrocks.qe.scheduler.assignment;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.OlapTable;
import com.starrocks.lake.qe.scheduler.DefaultSharedDataWorkerProvider;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.FragmentInstance.DeployedScanRangeLayout;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class LocalFragmentAssignmentStrategyTest {
    @Test
    public void testFragmentInstanceRecordsDeployedScanRangeLayoutPerScanNode() {
        FragmentInstance instance = new FragmentInstance(null, null);

        instance.recordDeployedScanRangesWithoutDriverSeq(1);
        instance.recordDeployedScanRangesPerDriverSeq(2, 4);

        DeployedScanRangeLayout normalLayout = instance.getDeployedScanRangeLayout(1).orElseThrow();
        Assertions.assertFalse(normalLayout.isPerDriverSeq());

        DeployedScanRangeLayout perDriverLayout = instance.getDeployedScanRangeLayout(2).orElseThrow();
        Assertions.assertTrue(perDriverLayout.isPerDriverSeq());
        Assertions.assertEquals(4, perDriverLayout.getDriverSeqCount());
    }

    @Test
    public void testPaddingScanRangesUsesPerScanNodeDeployedLayout() {
        FragmentInstance instance = new FragmentInstance(null, null);
        instance.recordDeployedScanRangesPerDriverSeq(1, 2);
        instance.recordDeployedScanRangesPerDriverSeq(2, 4);

        instance.addScanRanges(1, 0, List.of());
        instance.addScanRanges(2, 0, List.of());
        instance.paddingScanRanges();

        Map<Integer, List<TScanRangeParams>> scanNode1DriverSeqToScanRanges =
                instance.getNode2DriverSeqToScanRanges().get(1);
        Assertions.assertEquals(2, scanNode1DriverSeqToScanRanges.size());
        Assertions.assertTrue(scanNode1DriverSeqToScanRanges.containsKey(0));
        Assertions.assertTrue(scanNode1DriverSeqToScanRanges.containsKey(1));

        Map<Integer, List<TScanRangeParams>> scanNode2DriverSeqToScanRanges =
                instance.getNode2DriverSeqToScanRanges().get(2);
        Assertions.assertEquals(4, scanNode2DriverSeqToScanRanges.size());
        Assertions.assertTrue(scanNode2DriverSeqToScanRanges.containsKey(0));
        Assertions.assertTrue(scanNode2DriverSeqToScanRanges.containsKey(1));
        Assertions.assertTrue(scanNode2DriverSeqToScanRanges.containsKey(2));
        Assertions.assertTrue(scanNode2DriverSeqToScanRanges.containsKey(3));
    }

    @Test
    public void testResetScanRangesKeepsDeployedScanRangeLayout() {
        FragmentInstance instance = new FragmentInstance(null, null);
        instance.recordDeployedScanRangesPerDriverSeq(1, 3);
        instance.addScanRanges(1, 0, List.of());

        instance.resetAllScanRanges();

        Assertions.assertTrue(instance.getNode2DriverSeqToScanRanges().isEmpty());
        DeployedScanRangeLayout layout = instance.getDeployedScanRangeLayout(1).orElseThrow();
        Assertions.assertTrue(layout.isPerDriverSeq());
        Assertions.assertEquals(3, layout.getDriverSeqCount());
    }

    @Test
    public void parallelSourceIgnoresForcedPerDriverPropertyAssignment() throws Exception {
        ComputeResource resource = new ComputeResource() {
            public long getWarehouseId() {
                return 0;
            }
            public long getWorkerGroupId() {
                return 0;
            }
        };
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        OlapTable table = new OlapTable();
        table.setBaseIndexMetaId(0);
        table.setIndexMeta(0, "base", List.of(new com.starrocks.catalog.Column("k", com.starrocks.type.IntegerType.INT)),
                0, 0, (short) 1, com.starrocks.thrift.TStorageType.COLUMN, com.starrocks.sql.ast.KeysType.DUP_KEYS);
        tuple.setTable(table);
        List<TScanRangeLocations> tasks = java.util.stream.LongStream.range(100, 132)
                .mapToObj(id -> new TScanRangeLocations().setScan_range(new TScanRange().setInternal_scan_range(
                        new TInternalScanRange().setTablet_id(id).setVersion("7").setSchema_hash("0"))))
                .toList();
        OlapScanNode scan = new OlapScanNode(new PlanNodeId(0), tuple, "OlapScanNode", 0, resource) {
            @Override
            public List<TScanRangeLocations> getScanRangeLocations(long maxLength) {
                return tasks;
            }
        };
        scan.setParallelRangeScan(true);
        for (int dop : List.of(1, 4)) {
            PlanFragment plan = new PlanFragment(new PlanFragmentId(0), scan, DataPartition.RANDOM);
            plan.setParallelExecNum(1);
            plan.setPipelineDop(dop);
            plan.setForceAssignScanRangesPerDriverSeq();
            ComputeNode first = new ComputeNode(1, "127.0.0.1", 9000);
            ComputeNode second = new ComputeNode(2, "127.0.0.2", 9000);
            first.setAlive(true);
            second.setAlive(true);
            var nodes = ImmutableMap.of(1L, first, 2L, second);
            var provider = new DefaultSharedDataWorkerProvider(nodes, nodes, resource);
            ConnectContext context = new ConnectContext();
            context.setCurrentComputeResource(resource);
            ExecutionFragment fragment = new ExecutionFragment(null, plan, 0);
            new LocalFragmentAssignmentStrategy(context, provider, true, false, false)
                    .assignFragmentToWorker(fragment);
            Assertions.assertNull(fragment.getColocatedAssignment());
            Assertions.assertEquals(2, fragment.getInstances().size());
            Assertions.assertEquals(32, fragment.getInstances().stream().mapToInt(instance -> {
                Assertions.assertTrue(instance.getBucketSeqs().isEmpty());
                Assertions.assertTrue(instance.getNode2DriverSeqToScanRanges().isEmpty());
                Assertions.assertEquals(dop, instance.getPipelineDop());
                return instance.getNode2ScanRanges().values().stream().mapToInt(List::size).sum();
            }).sum());
        }
    }
}
