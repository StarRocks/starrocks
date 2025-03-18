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

import com.starrocks.planner.ScanNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

public class HiveScanTest extends ConnectorPlanTestBase {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @BeforeClass
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.doInit(temp.newFolder().toURI().toString());
        connectContext.changeCatalogDb("hive0.partitioned_db");
    }

    @Test
    public void testPruneColumnTest() throws Exception {
        connectContext.getSessionVariable().setEnableCountStarOptimization(true);
        String[] sqlString = {
                "select count(*) from lineitem_par",
                "select count(*) from lineitem_par where l_shipdate = '1998-01-01'"
        };
        boolean[] expexted = {true, true};
        Assert.assertEquals(sqlString.length, expexted.length);
        for (int i = 0; i < sqlString.length; i++) {
            String sql = sqlString[i];
            ExecPlan plan = getExecPlan(sql);
            List<ScanNode> scanNodeList = plan.getScanNodes();
            Assert.assertEquals(scanNodeList.get(0).getScanOptimzeOption().getCanUseAnyColumn(), expexted[i]);
        }

        connectContext.getSessionVariable().setEnableCountStarOptimization(false);
        for (int i = 0; i < sqlString.length; i++) {
            String sql = sqlString[i];
            ExecPlan plan = getExecPlan(sql);
            List<ScanNode> scanNodeList = plan.getScanNodes();
            Assert.assertEquals(scanNodeList.get(0).getScanOptimzeOption().getCanUseAnyColumn(), false);
        }
        connectContext.getSessionVariable().setEnableCountStarOptimization(true);
    }

    @Test
    public void testLabelMinMaxCountTest() throws Exception {
        String[] sqlString = {
                "select count(l_orderkey) from lineitem_par", "true",
                "select count(l_orderkey) from lineitem_par where l_shipdate = '1998-01-01'", "true",
                "select count(distinct l_orderkey) from lineitem_par", "false",
                "select count(l_orderkey), min(l_partkey) from lineitem_par", "true",
                "select count(l_orderkey) from lineitem_par group by l_partkey", "false",
                "select count(l_orderkey) from lineitem_par limit 10", "true",
                "select count(l_orderkey), max(l_partkey), avg(l_partkey) from lineitem_par", "false",
                "select count(l_orderkey), max(l_partkey), min(l_partkey) from lineitem_par", "true",
        };
        Assert.assertTrue(sqlString.length % 2 == 0);
        for (int i = 0; i < sqlString.length; i += 2) {
            String sql = sqlString[i];
            boolean expexted = Boolean.valueOf(sqlString[i + 1]);
            ExecPlan plan = getExecPlan(sql);
            List<ScanNode> scanNodeList = plan.getScanNodes();
            Assert.assertEquals(expexted, scanNodeList.get(0).getScanOptimzeOption().getCanUseMinMaxCountOpt());
        }
    }

    @Test
    public void testHiveRewriteSimpleAggToHdfsScan() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(true);
        // positive cases.
        {
            String[] sqlString = {
                    "select count(*) from lineitem_par",
                    "select count(*) from lineitem_par where l_shipdate = '1998-01-01'",
                    "select count(*), l_shipdate from lineitem_par where l_shipdate = '1998-01-01' group by l_shipdate"
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                String plan = getFragmentPlan(sql);
                assertContains(plan, "___count___");
            }
        }
        // negative cases.
        {
            String[] sqlString = {
                    "select count(l_orderkey) from lineitem_par",
                    "select count(*) from lineitem_par where l_shipdate = '1998-01-01' and l_orderkey = 202",
                    "select count(*), count(l_orderkey), l_shipdate from lineitem_par where l_shipdate = '1998-01-01' group by " +
                            "l_shipdate"
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                String plan = getFragmentPlan(sql);
                assertNotContains(plan, "___count___");
            }
        }
        // bad cases
        {
            String[] sqlString = {
                    "select distinct l_shipdate from lineitem_par",
                    "select count(distinct l_shipdate) from lineitem_par",
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                // just make sure it's not stuck.
                String plan = getFragmentPlan(sql);
            }
        }
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(false);
    }

    @Test
    public void testIcebergRewriteSimpleAggToHdfsScan() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(true);
        // positive cases.
        {
            String[] sqlString = {
                    "select count(*) from iceberg0.partitioned_db.t1",
                    "select count(*) from iceberg0.partitioned_db.t1 where date = '2020-01-01'",
                    "select count(*), date from iceberg0.partitioned_db.t1 where date = '2020-01-01' " +
                            "group by date"
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                String plan = getFragmentPlan(sql);
                assertContains(plan, "___count___");
            }
        }
        // negative cases.
        {
            String[] sqlString = {
                    "select count(id) from iceberg0.partitioned_db.t1",
                    "select count(*) from iceberg0.partitioned_db.t1 where date = '1998-01-01' and id =" +
                            " 202",
                    "select count(*), count(id), date from iceberg0.partitioned_db.t1 where date " +
                            "= '2020-01-01' group by " +
                            "date"
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                String plan = getFragmentPlan(sql);
                assertNotContains(plan, "___count___");
            }
        }
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(false);
    }
}