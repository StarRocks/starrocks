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
import com.starrocks.server.GlobalStateMgr;
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
        GlobalStateMgr.getCurrentState().changeCatalogDb(connectContext, "hive0.partitioned_db");
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
}