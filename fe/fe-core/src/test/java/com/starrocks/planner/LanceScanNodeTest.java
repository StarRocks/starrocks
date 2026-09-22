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

package com.starrocks.planner;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.LanceTable;
import com.starrocks.planner.expression.ExprToThrift;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TPlanNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LanceScanNodeTest {
    private LanceScanNode newScan(List<Long> nodes) {
        TupleDescriptor tuple = new DescriptorTable().createTupleDescriptor();
        tuple.setTable(new LanceTable(1, "vectors", List.of(), "file:///tmp/vectors.lance"));
        return new LanceScanNode(new PlanNodeId(0), tuple, "LanceScanNode") {
            @Override
            public List<Long> getAllAvailableBackendOrComputeIds() {
                return nodes;
            }
        };
    }

    @Test
    public void testWholeDatasetRangeIsIdempotent() {
        LanceScanNode scan = newScan(List.of(1L));
        scan.setupScanRangeLocations();
        scan.setupScanRangeLocations();
        Assertions.assertEquals(1, scan.getScanRangeLocations(0).size());
        THdfsScanRange range = scan.getScanRangeLocations(0).get(0).scan_range.hdfs_scan_range;
        Assertions.assertTrue(range.isUse_lance_jni_reader());
        Assertions.assertFalse(range.isSetLance_split_info());
        Assertions.assertEquals("file:///tmp/vectors.lance", range.getFull_path());
        Assertions.assertTrue(scan.isConnectorScanNode());
        Assertions.assertFalse(scan.getScanRangeLocations(0).get(0).getLocations().get(0).isSetBackend_id());
        Assertions.assertEquals("-1", scan.getScanRangeLocations(0).get(0).getLocations().get(0).getServer().getHostname());
    }

    @Test
    public void testNoBackendFailsInsteadOfReturningEmptyResults() {
        LanceScanNode scan = newScan(List.of());
        Assertions.assertThrows(IllegalStateException.class, () -> scan.setupScanRangeLocations());
        Assertions.assertTrue(scan.getScanRangeLocations(0).isEmpty());
    }

    @Test
    public void testSerializationPreservesScanPredicates() {
        LanceScanNode scan = newScan(List.of(1L));
        BoolLiteral predicate = new BoolLiteral(false);
        scan.getConjuncts().add(predicate);
        scan.getScanNodePredicates().getNonPartitionConjuncts().add(predicate);
        scan.getScanNodePredicates().getNoEvalPartitionConjuncts().add(predicate);
        scan.getScanNodePredicates().getMinMaxConjuncts().add(predicate);
        scan.setScanOptimizeOption(new com.starrocks.sql.optimizer.ScanOptimizeOption());
        TPlanNode node = scan.treeToThrift().getNodes().get(0);
        Assertions.assertEquals(List.of(ExprToThrift.treeToThrift(predicate)), node.getConjuncts());
        Assertions.assertFalse(node.getHdfs_scan_node().isSetPartition_conjuncts());
        Assertions.assertFalse(node.getHdfs_scan_node().isSetMin_max_conjuncts());
    }
    private LanceScanNode newUnmockedScan() {
        TupleDescriptor tuple = new DescriptorTable().createTupleDescriptor();
        tuple.setTable(new LanceTable(1, "vectors", List.of(), "file:///tmp/vectors.lance"));
        return new LanceScanNode(new PlanNodeId(0), tuple, "LanceScanNode");
    }

    private GlobalStateMgr mockCluster(boolean sharedData) {
        GlobalStateMgr state = mock(GlobalStateMgr.class, RETURNS_DEEP_STUBS);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return state;
            }
        };
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return sharedData;
            }
        };
        return state;
    }

    @Test
    public void testSharedNothingUsesAvailableBackendsAndComputeNodes() {
        GlobalStateMgr state = mockCluster(false);
        SystemInfoService service = state.getNodeMgr().getClusterInfo();
        // Immutable lists ensure enumeration does not modify the cluster's lists.
        when(service.getAvailableBackendIds()).thenReturn(List.of(11L));
        when(service.getAvailableComputeNodeIds()).thenReturn(List.of(22L));
        LanceScanNode scan = newUnmockedScan();
        Assertions.assertEquals(List.of(11L, 22L), scan.getAllAvailableBackendOrComputeIds());
        scan.setupScanRangeLocations();
        Assertions.assertFalse(scan.getScanRangeLocations(0).get(0).getLocations().get(0).isSetBackend_id());
        Assertions.assertNotNull(scan.getScanRangeLocations(0).get(0).getLocations().get(0).getServer());

        when(service.getAvailableBackendIds()).thenReturn(null);
        Assertions.assertEquals(List.of(22L), scan.getAllAvailableBackendOrComputeIds());
        when(service.getAvailableComputeNodeIds()).thenReturn(null);
        Assertions.assertTrue(scan.getAllAvailableBackendOrComputeIds().isEmpty());
        Assertions.assertThrows(IllegalStateException.class, () -> scan.setupScanRangeLocations());
        Assertions.assertTrue(scan.getScanRangeLocations(0).isEmpty());
    }

    @Test
    public void testSharedDataUsesCurrentComputeResource() {
        GlobalStateMgr state = mockCluster(true);
        ConnectContext context = mock(ConnectContext.class);
        ComputeResource resource = mock(ComputeResource.class);
        when(context.getCurrentComputeResource()).thenReturn(resource);
        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return context;
            }
        };
        when(state.getWarehouseMgr().getAliveComputeNodes(resource))
                .thenReturn(List.of(new ComputeNode(22L, "compute", 9050)));
        Assertions.assertEquals(List.of(22L), newUnmockedScan().getAllAvailableBackendOrComputeIds());
        verify(state.getWarehouseMgr()).getAliveComputeNodes(resource);
    }

    @Test
    public void testSharedDataWithoutContextUsesDefaultResource() {
        GlobalStateMgr state = mockCluster(true);
        new MockUp<ConnectContext>() {
            @Mock
            public ConnectContext get() {
                return null;
            }
        };
        when(state.getWarehouseMgr().getAliveComputeNodes(WarehouseManager.DEFAULT_RESOURCE))
                .thenReturn(List.of(new ComputeNode(33L, "compute", 9050)));
        Assertions.assertEquals(List.of(33L), newUnmockedScan().getAllAvailableBackendOrComputeIds());
        verify(state.getWarehouseMgr()).getAliveComputeNodes(WarehouseManager.DEFAULT_RESOURCE);
    }

    @Test
    public void testExplainIncludesPredicatesAndPrunedComplexType() {
        LanceScanNode scan = newScan(List.of(1L));
        SlotDescriptor slot = new SlotDescriptor(new SlotId(1), scan.getDesc());
        slot.setColumn(new Column("embedding", new ArrayType(IntegerType.INT)));
        slot.setType(new ArrayType(IntegerType.INT));
        scan.getDesc().addSlot(slot);
        scan.sortColumn = "embedding";
        scan.getConjuncts().add(new BoolLiteral(false));
        scan.getScanNodePredicates().getPartitionConjuncts().add(new BoolLiteral(true));
        scan.getScanNodePredicates().getNonPartitionConjuncts().add(new BoolLiteral(false));
        scan.getScanNodePredicates().getNoEvalPartitionConjuncts().add(new BoolLiteral(true));
        scan.getScanNodePredicates().getMinMaxConjuncts().add(new BoolLiteral(false));
        String normal = scan.getNodeExplainString("  ", TExplainLevel.NORMAL);
        Assertions.assertTrue(normal.contains("  TABLE: vectors"));
        Assertions.assertTrue(normal.contains("SORT COLUMN: embedding"));
        Assertions.assertTrue(normal.contains("PREDICATES: FALSE"));
        Assertions.assertTrue(normal.contains("PARTITION PREDICATES: TRUE"));
        Assertions.assertTrue(normal.contains("NON-PARTITION PREDICATES: FALSE"));
        Assertions.assertTrue(normal.contains("NO EVAL-PARTITION PREDICATES: TRUE"));
        Assertions.assertTrue(normal.contains("MIN/MAX PREDICATES: FALSE"));
        Assertions.assertTrue(normal.contains("cardinality="));
        Assertions.assertTrue(normal.contains("avgRowSize="));
        String verbose = scan.getNodeExplainString("", TExplainLevel.VERBOSE);
        Assertions.assertTrue(verbose.contains("Pruned type: 1 <-> [ARRAY<INT>]"));
        Assertions.assertFalse(verbose.contains("cardinality="));
        Assertions.assertTrue(scan.debugString().contains("lanceTable=vectors"));
        Assertions.assertTrue(scan.canUseRuntimeAdaptiveDop());
    }

    @Test
    public void testDatasetUriIsSerializedInTableDescriptor() {
        LanceTable table = new LanceTable(42, "vectors", List.of(new Column("id", IntegerType.INT)),
                "s3://bucket/vectors.lance", "lance_catalog", "vectors_db");
        TTableDescriptor thrift = table.toThrift(List.of());
        Assertions.assertEquals(42, thrift.getId());
        Assertions.assertEquals(TTableType.LANCE_TABLE, thrift.getTableType());
        Assertions.assertEquals(1, thrift.getNumCols());
        Assertions.assertEquals("vectors", thrift.getTableName());
        Assertions.assertEquals("vectors_db", thrift.getDbName());
        Assertions.assertEquals(table.getTableLocation(), thrift.getLanceTable().getLance_dataset_uri());
    }

}
