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
import com.starrocks.catalog.StarRocksExternalTable;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TStarRocksRemoteScanRequiredOutput;
import com.starrocks.thrift.TStarRocksRemoteScanWireShape;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class StarRocksScanNodeTest {
    @Test
    public void testUsesRemoteOutputSchemaOrderForThrift() {
        StarRocksExternalTable table = new StarRocksExternalTable(1, "remote", "db1", "tbl1",
                Arrays.asList(new Column("k2", VarcharType.VARCHAR), new Column("v1", IntegerType.BIGINT)),
                7);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        addSlot(desc, 2, new Column("k2", VarcharType.VARCHAR));
        addSlot(desc, 3, new Column("v1", IntegerType.BIGINT));

        StarRocksScanNode scanNode = new StarRocksScanNode(new PlanNodeId(0), desc, null);
        scanNode.setOutputColumnNames(Collections.singletonList("v1"));

        TPlanNode msg = new TPlanNode();
        scanNode.toThrift(msg);

        Assertions.assertEquals(Collections.singletonList("v1"),
                msg.starrocks_scan_node.getOutput_column_names());
    }

    @Test
    public void testMaterializedNonOutputSlotIsIncludedForRemoteProjection() {
        StarRocksExternalTable table = new StarRocksExternalTable(1, "remote", "db1", "tbl1",
                Arrays.asList(new Column("k2", VarcharType.VARCHAR), new Column("v1", IntegerType.BIGINT)),
                7);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        addSlot(desc, 2, new Column("k2", VarcharType.VARCHAR), false);
        addSlot(desc, 3, new Column("v1", IntegerType.BIGINT), false);

        StarRocksScanNode scanNode = new StarRocksScanNode(new PlanNodeId(0), desc, null);

        TPlanNode msg = new TPlanNode();
        scanNode.toThrift(msg);

        Assertions.assertEquals(Arrays.asList("k2", "v1"),
                msg.starrocks_scan_node.getOutput_column_names());
    }

    @Test
    public void testRemoteScanDisablesRuntimeAdaptiveDop() {
        StarRocksExternalTable table = new StarRocksExternalTable(1, "remote", "db1", "tbl1",
                Collections.singletonList(new Column("v1", IntegerType.BIGINT)),
                7);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        addSlot(desc, 3, new Column("v1", IntegerType.BIGINT));

        StarRocksScanNode scanNode = new StarRocksScanNode(new PlanNodeId(0), desc, null);

        Assertions.assertFalse(scanNode.canUseRuntimeAdaptiveDop());
    }

    @Test
    public void testExplainShowsPushdownPredicateAtNormalLevel() {
        StarRocksScanNode scanNode = newScanNode();
        scanNode.setRemoteScanExplainInfo("`k1` > 1", Collections.emptyList(),
                Collections.emptyList(), -1L, -1L, Collections.emptyMap());

        String normal = scanNode.getNodeExplainString("  ", TExplainLevel.NORMAL);

        Assertions.assertTrue(normal.contains("PUSHDOWN PREDICATE: `k1` > 1"), normal);
        // VERBOSE-only remote execution parameters must not appear at NORMAL level.
        Assertions.assertFalse(normal.contains("FORWARDED SESSION VARS"), normal);
        Assertions.assertFalse(normal.contains("REQUIRED OUTPUTS"), normal);
    }

    @Test
    public void testVerboseExplainShowsRemoteExecutionParams() {
        StarRocksScanNode scanNode = newScanNode();

        TStarRocksRemoteScanRequiredOutput prunedStruct = new TStarRocksRemoteScanRequiredOutput();
        prunedStruct.setLocal_slot_id(4);
        prunedStruct.setRoot_column("struct_col");
        prunedStruct.setWire_shape(TStarRocksRemoteScanWireShape.PRUNED_ROOT_STRUCT);
        TStarRocksRemoteScanRequiredOutput fullRoot = new TStarRocksRemoteScanRequiredOutput();
        fullRoot.setLocal_slot_id(5);
        fullRoot.setRoot_column("k1");
        fullRoot.setWire_shape(TStarRocksRemoteScanWireShape.FULL_ROOT);

        Map<String, String> sessionVars = new LinkedHashMap<>();
        sessionVars.put("query_timeout", "300");
        sessionVars.put("time_zone", "Asia/Shanghai");

        scanNode.setRemoteScanExplainInfo("`k1` > 1",
                Collections.singletonList("subquery is not supported"),
                Arrays.asList(prunedStruct, fullRoot), 7L, 3L, sessionVars);

        String verbose = scanNode.getNodeExplainString("  ", TExplainLevel.VERBOSE);

        Assertions.assertTrue(verbose.contains("PUSHDOWN PREDICATE: `k1` > 1"), verbose);
        Assertions.assertTrue(verbose.contains("NON-PUSHDOWN PREDICATE: subquery is not supported"), verbose);
        Assertions.assertTrue(verbose.contains("SOFT LIMIT: 7"), verbose);
        Assertions.assertTrue(verbose.contains("SCHEMA VERSION: 3"), verbose);
        Assertions.assertTrue(verbose.contains("slot=4 col=struct_col shape=PRUNED_ROOT_STRUCT"), verbose);
        Assertions.assertTrue(verbose.contains("slot=5 col=k1 shape=FULL_ROOT"), verbose);
        Assertions.assertTrue(
                verbose.contains("FORWARDED SESSION VARS: query_timeout=300, time_zone=Asia/Shanghai"), verbose);
    }

    private static StarRocksScanNode newScanNode() {
        StarRocksExternalTable table = new StarRocksExternalTable(1, "remote", "db1", "tbl1",
                Collections.singletonList(new Column("k1", IntegerType.BIGINT)),
                7);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        addSlot(desc, 3, new Column("k1", IntegerType.BIGINT));
        return new StarRocksScanNode(new PlanNodeId(0), desc, null);
    }

    private static void addSlot(TupleDescriptor desc, int slotId, Column column) {
        addSlot(desc, slotId, column, true);
    }

    private static void addSlot(TupleDescriptor desc, int slotId, Column column, boolean isOutputColumn) {
        SlotDescriptor slot = new SlotDescriptor(new SlotId(slotId), desc);
        slot.setColumn(column);
        slot.setType(column.getType());
        slot.setIsNullable(column.isAllowNull());
        slot.setIsMaterialized(true);
        slot.setIsOutputColumn(isOutputColumn);
        desc.addSlot(slot);
    }
}
