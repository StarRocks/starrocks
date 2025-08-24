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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class HiveScanTest extends ConnectorPlanTestBase {
    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.doInit(newFolder(temp, "junit").toURI().toString());
        connectContext.changeCatalogDb("hive0.partitioned_db");
    }

    @Test
    public void testPruneColumnTest() throws Exception {
        connectContext.getSessionVariable().setEnableCountStarOptimization(true);
        String[] sqlString = {
                "select count(*) from lineitem_par",
                "select count(*) from lineitem_par where l_shipdate = '1998-01-01'"
        };
        boolean[] expected = {true, true};
        Assertions.assertEquals(sqlString.length, expected.length);
        for (int i = 0; i < sqlString.length; i++) {
            String sql = sqlString[i];
            ExecPlan plan = getExecPlan(sql);
            List<ScanNode> scanNodeList = plan.getScanNodes();
            Assertions.assertEquals(scanNodeList.get(0).getScanOptimizeOption().getCanUseAnyColumn(), expected[i]);
        }

        connectContext.getSessionVariable().setEnableCountStarOptimization(false);
        for (int i = 0; i < sqlString.length; i++) {
            String sql = sqlString[i];
            ExecPlan plan = getExecPlan(sql);
            List<ScanNode> scanNodeList = plan.getScanNodes();
            Assertions.assertEquals(scanNodeList.get(0).getScanOptimizeOption().getCanUseAnyColumn(), false);
        }
        connectContext.getSessionVariable().setEnableCountStarOptimization(true);
    }

    @Test
    public void testUseMinMaxOptTest() throws Exception {
        String[] sqlString = {
                "select min(id) from iceberg0.partitioned_db.t1", "true",
                "select min(id) from iceberg0.partitioned_db.t1 where date = '2020-01-01'", "true",
                "select max(id) from iceberg0.partitioned_db.t1 where date = '2020-01-01'", "true",
                "select count(id) from iceberg0.partitioned_db.t1 where date = '2020-01-01'", "false",
                "select min(id), date from iceberg0.partitioned_db.t1 group by date", "true",
                "select max(id), date from iceberg0.partitioned_db.t1 group by date", "true",
                "select max(id) as x, date from iceberg0.partitioned_db.t1 group by date having x > 10", "false",
                "select max(id) as x, date from iceberg0.partitioned_db.t1 group by date having date > '2020-01-01'", "true",
        };
        Assertions.assertTrue(sqlString.length % 2 == 0);
        for (int i = 0; i < sqlString.length; i += 2) {
            String sql = sqlString[i];
            boolean expexted = Boolean.valueOf(sqlString[i + 1]);
            ExecPlan plan = getExecPlan(sql);
            List<ScanNode> scanNodeList = plan.getScanNodes();
            Assertions.assertEquals(expexted, scanNodeList.get(0).getScanOptimizeOption().getCanUseMinMaxOpt());
        }
    }

    @Test
    public void testHiveRewriteSimpleAggToHdfsScan() throws Exception {
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(true);
        // positive cases.
        {
            String[] sqlString = {
                    "select count(*) + 1 from lineitem_par",
                    "select count(*) + 1 from lineitem_par where l_shipdate = '1998-01-01'",
                    "select count(*) + 1, l_shipdate from lineitem_par where l_shipdate = '1998-01-01' group by l_shipdate",
                    "select count(*) from lineitem_par",
                    "select count(*) from lineitem_par where l_shipdate = '1998-01-01'",
                    "select count(*), l_shipdate from lineitem_par where l_shipdate = '1998-01-01' group by l_shipdate",
                    "select count(*), l_shipdate from lineitem_par group by l_shipdate having l_shipdate > '1998-01-01'",
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                String plan = getFragmentPlan(sql);
                assertContains(plan, "___count___");
                assertContains(plan, "ifnull");
            }
        }
        // negative cases.
        {
            String[] sqlString = {
                    "select count(l_orderkey) from lineitem_par",
                    "select count(*) from lineitem_par where l_shipdate = '1998-01-01' and l_orderkey = 202",
                    "select count(*), count(l_orderkey), l_shipdate from lineitem_par where l_shipdate = '1998-01-01' group by " +
                            "l_shipdate",
                    "select count(*) as x, l_shipdate from lineitem_par group by l_shipdate having x > 10",
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
                assertNotContains(plan, "___count___");
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
                    "select count(*) + 1 from iceberg0.partitioned_db.t1",
                    "select count(*) + 1 from iceberg0.partitioned_db.t1 where date = '2020-01-01'",
                    "select count(*) + 1, date from iceberg0.partitioned_db.t1 where date = '2020-01-01' " +
                            "group by date",
                    "select count(*) from iceberg0.partitioned_db.t1",
                    "select count(*) from iceberg0.partitioned_db.t1 where date = '2020-01-01'",
                    "select count(*), date from iceberg0.partitioned_db.t1 where date = '2020-01-01' " +
                            "group by date",
                    "select count(*), date from iceberg0.partitioned_db.t1 group by date having date > '2020-01-01'",
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                String plan = getFragmentPlan(sql);
                assertContains(plan, "___count___");
                assertContains(plan, "ifnull");
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
                            "date",
                    "select count(*) as x, date from iceberg0.partitioned_db.t1 group by date having x > 10",
            };
            for (int i = 0; i < sqlString.length; i++) {
                String sql = sqlString[i];
                String plan = getFragmentPlan(sql);
                assertNotContains(plan, "___count___");
            }
        }
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToHdfsScan(false);
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
        String subFolder = String.join("/", subDirs);
        File result = new File(root, subFolder);
        if (!result.mkdirs()) {
            throw new IOException("Couldn't create folders " + root);
        }
        return result;
    }
}