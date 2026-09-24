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

import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * COUNT(col) on an Iceberg table is rewritten into ifnull(sum(placeholder), 0) + count(col), where the backend
 * fills the placeholder from each data file's manifest null count and reads only the files that lack one.
 */
public class IcebergCountColumnRewriteTest extends ConnectorPlanTestBase {

    @BeforeEach
    public void enableRewrite() {
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(true);
    }

    @AfterEach
    public void restoreRewrite() {
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(false);
    }

    @Test
    public void testCountColumnSumsPlaceholderAndCountsRowsRead() throws Exception {
        String plan = getFragmentPlan("select count(id) from iceberg0.unpartitioned_db.t0");
        assertContains(plan, "___count___id");
        assertContains(plan, "sum(");
        assertContains(plan, "count(");
        assertContains(plan, "ifnull(");
    }

    @Test
    public void testCountColumnSendsPlaceholderToCountedSlot() throws Exception {
        ExecPlan execPlan = getExecPlan("select count(data) from iceberg0.unpartitioned_db.t0");
        IcebergScanNode scanNode = onlyIcebergScanNode(execPlan);

        THdfsScanNode tHdfsScanNode = scanNode.treeToThrift().getNodes().get(0).getHdfs_scan_node();
        Assertions.assertTrue(tHdfsScanNode.isCan_use_count_opt());
        Map<Integer, Integer> nonNullCountSlots = tHdfsScanNode.getNon_null_count_slots();
        Assertions.assertEquals(1, nonNullCountSlots.size());
        Map.Entry<Integer, Integer> slots = nonNullCountSlots.entrySet().iterator().next();
        Assertions.assertEquals("___count___data", slotColumnName(execPlan, slots.getKey()));
        Assertions.assertEquals("data", slotColumnName(execPlan, slots.getValue()));
    }

    @Test
    public void testCountStarAndSeveralColumnsRewriteTogether() throws Exception {
        String sql = "select count(*), count(id), count(data), count(id) from iceberg0.unpartitioned_db.t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "___count___id");
        assertContains(plan, "___count___data");

        ExecPlan execPlan = getExecPlan(sql);
        IcebergScanNode scanNode = onlyIcebergScanNode(execPlan);
        Assertions.assertEquals(2, scanNode.getScanOptimizeOption().getNonNullCountColumns().size());
        Assertions.assertTrue(scanNode.getDesc().getSlots().stream()
                .anyMatch(slot -> slot.getColumn().getName().equals("___count___")));
    }

    @Test
    public void testCountColumnGroupedByPartitionColumn() throws Exception {
        String plan = getFragmentPlan("select date, count(*), count(id) from iceberg0.partitioned_db.t1 " +
                "where date = '2020-01-01' group by date");
        assertContains(plan, "___count___id");
    }

    @Test
    public void testPlaceholderSlotsAreBigint() throws Exception {
        ExecPlan execPlan = getExecPlan("select count(*), count(id) from iceberg0.unpartitioned_db.t0");
        IcebergScanNode scanNode = onlyIcebergScanNode(execPlan);
        List<SlotDescriptor> placeholders = scanNode.getDesc().getSlots().stream()
                .filter(slot -> slot.getColumn().getName().startsWith("___count___"))
                .toList();
        Assertions.assertEquals(2, placeholders.size());
        for (SlotDescriptor placeholder : placeholders) {
            Assertions.assertEquals(IntegerType.BIGINT, placeholder.getType());
        }
    }

    @Test
    public void testCountColumnNotRewritten() throws Exception {
        String[] sqls = {
                "select count(distinct id) from iceberg0.unpartitioned_db.t0",
                "select count(id + 1) from iceberg0.unpartitioned_db.t0",
                "select count(id), sum(id) from iceberg0.unpartitioned_db.t0",
                "select data, count(id) from iceberg0.unpartitioned_db.t0 group by data",
                "select count(id) from iceberg0.unpartitioned_db.t0 where id > 1",
                "select count(date) from iceberg0.partitioned_db.t1",
                "select count(l_orderkey) from hive0.partitioned_db.lineitem_par",
        };
        for (String sql : sqls) {
            assertNotContains(getFragmentPlan(sql), "___count___");
        }
    }

    @Test
    public void testCountColumnNotRewrittenWhenDisabled() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(false);
        assertNotContains(getFragmentPlan("select count(id) from iceberg0.unpartitioned_db.t0"), "___count___");
    }

    private static IcebergScanNode onlyIcebergScanNode(ExecPlan execPlan) {
        List<ScanNode> scanNodes = execPlan.getScanNodes();
        Assertions.assertEquals(1, scanNodes.size());
        return Assertions.assertInstanceOf(IcebergScanNode.class, scanNodes.get(0));
    }

    private static String slotColumnName(ExecPlan execPlan, int slotId) {
        return execPlan.getDescTbl().getSlotDesc(new SlotId(slotId)).getColumn().getName();
    }
}
