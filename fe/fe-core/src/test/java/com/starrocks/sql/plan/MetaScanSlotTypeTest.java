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

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.planner.MetaScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TSlotDescriptor;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNodeType;
import com.starrocks.type.JsonType;
import com.starrocks.type.TypeFactory;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A meta scan slot must reach the backend with the type the meta reader produces, not the type of the
 * table column it was derived from. With a one-phase aggregate the meta scan's chunks cross an exchange
 * unaggregated, and when that exchange serializes (different node, or pass-through disabled) the receiver
 * decodes each column with the slot type. dict_merge used to be sent as VARCHAR while the meta reader
 * builds ARRAY&lt;VARCHAR&gt;, which failed with "deserialize chunk data failed" or crashed in
 * BinaryColumnSerde::deserialize (issue #82831).
 */
public class MetaScanSlotTypeTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE meta_slot_t (" +
                " k BIGINT NOT NULL," +
                " c VARCHAR(64) NOT NULL," +
                " j JSON" +
                ") DUPLICATE KEY (k) " +
                "DISTRIBUTED BY HASH (k) BUCKETS 1 " +
                "properties(\"replication_num\"=\"1\");");
    }

    private Map<String, TSlotDescriptor> metaScanSlots(String sql) throws Exception {
        Pair<String, ExecPlan> plan = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        Map<String, TSlotDescriptor> slots = new HashMap<>();
        for (ScanNode scanNode : plan.second.getScanNodes()) {
            if (!(scanNode instanceof MetaScanNode)) {
                continue;
            }
            // Meta columns are named <agg>_<column> in the node itself; the slot carries the table column.
            Map<Integer, String> idToNames = scanNode.treeToThrift().getNodes().get(0).getMeta_scan_node()
                    .getId_to_names();
            for (SlotDescriptor slot : scanNode.getDesc().getSlots()) {
                slots.put(idToNames.get(slot.getId().asInt()), slot.toThrift());
            }
        }
        Assertions.assertFalse(slots.isEmpty(), "no meta scan in plan of " + sql);
        return slots;
    }

    private static TSlotDescriptor slotByPrefix(Map<String, TSlotDescriptor> slots, String prefix) {
        return slots.entrySet().stream()
                .filter(e -> e.getKey() != null && e.getKey().startsWith(prefix))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no slot named " + prefix + "* in " + slots.keySet()));
    }

    private static void assertArrayOfVarchar(TSlotDescriptor slot) {
        TTypeDesc type = slot.getSlotType();
        Assertions.assertEquals(2, type.getTypesSize(), type.toString());
        Assertions.assertEquals(TTypeNodeType.ARRAY, type.getTypes().get(0).getType(), type.toString());
        Assertions.assertEquals(TPrimitiveType.VARCHAR,
                type.getTypes().get(1).getScalar_type().getType(), type.toString());
    }

    private static void assertScalar(TSlotDescriptor slot, TPrimitiveType expected) {
        TTypeDesc type = slot.getSlotType();
        Assertions.assertEquals(1, type.getTypesSize(), type.toString());
        Assertions.assertEquals(expected, type.getTypes().get(0).getScalar_type().getType(), type.toString());
    }

    @Test
    public void testDictMergeSlotIsArrayOfVarchar() throws Exception {
        for (int stage : List.of(0, 1, 2)) {
            connectContext.getSessionVariable().setNewPlanerAggStage(stage);
            try {
                Map<String, TSlotDescriptor> slots =
                        metaScanSlots("select dict_merge(c, 255) from meta_slot_t [_META_]");
                assertArrayOfVarchar(slotByPrefix(slots, "dict_merge"));
            } finally {
                connectContext.getSessionVariable().setNewPlanerAggStage(0);
            }
        }
    }

    @Test
    public void testFlatJsonMetaSlotIsArrayOfVarchar() throws Exception {
        Map<String, TSlotDescriptor> slots = metaScanSlots("select flat_json_meta(j) from meta_slot_t [_META_]");
        assertArrayOfVarchar(slotByPrefix(slots, "flat_json_meta"));
    }

    @Test
    public void testTableSchemaIsNotChanged() throws Exception {
        metaScanSlots("select dict_merge(c, 255) from meta_slot_t [_META_]");
        metaScanSlots("select flat_json_meta(j) from meta_slot_t [_META_]");
        OlapTable table = (OlapTable) starRocksAssert.getTable("test", "meta_slot_t");
        // The rules rewrite copies of the table's columns; the table itself keeps VARCHAR(64) and JSON.
        Assertions.assertEquals(TypeFactory.createVarcharType(64), table.getColumn("c").getType());
        Assertions.assertEquals(JsonType.JSON, table.getColumn("j").getType());
    }

    @Test
    public void testOtherMetaAggregatesKeepTheirTypes() throws Exception {
        Map<String, TSlotDescriptor> slots = metaScanSlots(
                "select min(c), max(k), count(c), count(*), column_size(c), dict_merge(c, 255) " +
                        "from meta_slot_t [_META_]");
        assertScalar(slotByPrefix(slots, "min_c"), TPrimitiveType.VARCHAR);
        assertScalar(slotByPrefix(slots, "max_k"), TPrimitiveType.BIGINT);
        assertScalar(slotByPrefix(slots, "count_c"), TPrimitiveType.BIGINT);
        assertScalar(slotByPrefix(slots, "column_size_c"), TPrimitiveType.BIGINT);
        assertArrayOfVarchar(slotByPrefix(slots, "dict_merge_c"));
    }
}
