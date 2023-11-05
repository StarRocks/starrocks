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

import com.starrocks.catalog.system.information.BeConfigsSystemTable;
import com.starrocks.catalog.system.information.BeMetricsSystemTable;
import com.starrocks.catalog.system.information.BeTabletsSystemTable;
import com.starrocks.catalog.system.information.BeTxnsSystemTable;
import com.starrocks.pseudocluster.PseudoBackend;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

public class InformationSchemaBeFeTableTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void setUp() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testQueryBeSchemaTables() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            Assert.assertTrue(stmt.execute("select * from information_schema.be_tablets"));
            Assert.assertEquals(BeTabletsSystemTable.create().getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
            System.out.printf("get %d rows\n", stmt.getUpdateCount());
            Assert.assertTrue(stmt.execute("select * from information_schema.be_txns"));
            Assert.assertEquals(BeTxnsSystemTable.create().getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
            Assert.assertTrue(stmt.execute("select * from information_schema.be_configs"));
            Assert.assertEquals(BeConfigsSystemTable.create().getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
            Assert.assertTrue(stmt.execute("select * from information_schema.be_metrics"));
            Assert.assertEquals(BeMetricsSystemTable.create().getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
        } finally {
            stmt.close();
            connection.close();
        }
    }

    @Test
    public void testOnlyScanTargetedBE() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            for (int i = 0; i < 3; i++) {
                long beId = 10001 + i;
                PseudoBackend be = PseudoCluster.getInstance().getBackend(beId);
                long oldCnt = be.getNumSchemaScan();
                Assert.assertTrue(stmt.execute("select * from information_schema.be_tablets where be_id = " + beId));
                long newCnt = be.getNumSchemaScan();
                Assert.assertEquals(1, newCnt - oldCnt);
            }
        } finally {
            stmt.close();
            connection.close();
        }
    }

    @Test
    public void testBeSchemaTableAgg() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            // https://github.com/StarRocks/starrocks/issues/23306
            // verify aggregation behavior is correct
            Assert.assertTrue(stmt.execute(
                    "explain select table_id, count(*) as cnt from information_schema.be_tablets group by table_id"));
            int numExchange = 0;
            while (stmt.getResultSet().next()) {
                String line = stmt.getResultSet().getString(1);
                if (line.contains(":EXCHANGE")) {
                    numExchange++;
                }
            }
            Assert.assertEquals(2, numExchange);
        } finally {
            stmt.close();
            connection.close();
        }
    }

    @Test
    public void testUpdateBeConfig() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        String sql = "update information_schema.be_configs set value=\"1000\" where name=\"txn_info_history_size\"";
        String expected = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: BE_ID | 2: NAME | 7: expr | 4: TYPE | 5: DEFAULT | 6: MUTABLE\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "   SCHEMA TABLE(be_configs) SINK\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  2:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:1: BE_ID | 2: NAME | 7: expr | 4: TYPE | 5: DEFAULT | 6: MUTABLE\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: BE_ID\n" +
                "  |  <slot 2> : 2: NAME\n" +
                "  |  <slot 4> : 4: TYPE\n" +
                "  |  <slot 5> : 5: DEFAULT\n" +
                "  |  <slot 6> : 6: MUTABLE\n" +
                "  |  <slot 7> : '1000'\n" +
                "  |  \n" +
                "  0:SCAN SCHEMA\n";
        try {
            Assert.assertFalse(
                    stmt.execute(
                            "update information_schema.be_configs set value=\"1000\" where name=\"txn_info_history_size\""));

            // check whether STREAM DATA SINK use broadcast
            String explain = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                    getExplainString(TExplainLevel.NORMAL);
            Assert.assertEquals(explain, expected);

            String plan = UtFrameUtils.getPlanAndStartScheduling(connectContext, sql.toString()).first;
            // check whether paln has two plan fragment and each fragment has three different instance for each BE
            Map<Integer, Integer> result = PlanTestNoneDBBase.extractFragmentAndInstanceFromSchedulerPlan(plan);
            Assert.assertEquals(result.keySet().size(), 2);
            Assert.assertEquals(result.get(0).intValue(), 3);
            Assert.assertEquals(result.get(1).intValue(), 3);
        } finally {
            stmt.close();
            connection.close();
        }
    }
}
