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

import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// Regression for #80033: OlapScanNode.toThrift forwarded partition_order_hint on the OLAP branch but
// omitted it on the lake branch, so a DESC top-N on a lake table's partition/sort key scanned every
// partition oldest-first and read the whole time range. assignOrderByHints already computes the hint
// for lake tables; toThrift must forward it to the TLakeScanNode just like it does for TOlapScanNode.
public class LakeScanPartitionOrderHintTest {
    private static ConnectContext connectContext;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();

        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("lake_part_order_test")
                .useDatabase("lake_part_order_test")
                // The partition column is also the sort key, mirroring the issue's logs-style table.
                .withTable("CREATE TABLE logs_repro (dt DATE NOT NULL, v1 INT) " +
                        "DUPLICATE KEY(dt) " +
                        "PARTITION BY RANGE(dt) (" +
                        "  PARTITION p1 VALUES [('2026-01-01'), ('2026-01-02'))," +
                        "  PARTITION p2 VALUES [('2026-01-02'), ('2026-01-03'))) " +
                        "DISTRIBUTED BY HASH(v1) BUCKETS 1");
    }

    private TPlanNode lakeScanThrift(String sql) throws Exception {
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
        Assertions.assertEquals(1, execPlan.getScanNodes().size());
        ScanNode scanNode = execPlan.getScanNodes().get(0);
        Assertions.assertInstanceOf(OlapScanNode.class, scanNode);

        TPlanNode thriftNode = scanNode.treeToThrift().getNodes().get(0);
        Assertions.assertEquals(TPlanNodeType.LAKE_SCAN_NODE, thriftNode.getNode_type());
        Assertions.assertTrue(thriftNode.isSetLake_scan_node());
        return thriftNode;
    }

    @Test
    public void testPartitionOrderHintForwardedToLakeScanNode() throws Exception {
        // DESC top-N on the partition/sort key: the lake scan must carry partition_order_hint=false so
        // the BE schedules partitions newest-first. Before the fix the field was never set.
        TPlanNode desc = lakeScanThrift("SELECT v1 FROM logs_repro ORDER BY dt DESC LIMIT 5");
        Assertions.assertTrue(desc.getLake_scan_node().isSetPartition_order_hint(),
                "lake scan must forward partition_order_hint for a top-N on the partition key");
        Assertions.assertFalse(desc.getLake_scan_node().isPartition_order_hint());

        // ASC top-N: the hint is forwarded as true (oldest-first), reflecting the sort direction.
        TPlanNode asc = lakeScanThrift("SELECT v1 FROM logs_repro ORDER BY dt ASC LIMIT 5");
        Assertions.assertTrue(asc.getLake_scan_node().isSetPartition_order_hint());
        Assertions.assertTrue(asc.getLake_scan_node().isPartition_order_hint());
    }

    @Test
    public void testNoPartitionOrderHintWithoutTopN() throws Exception {
        // No top-N, so there is no order hint to forward: the field stays unset and an unchanged BE
        // keeps its default partition scheduling.
        TPlanNode plain = lakeScanThrift("SELECT v1 FROM logs_repro");
        Assertions.assertFalse(plain.getLake_scan_node().isSetPartition_order_hint());
    }
}
