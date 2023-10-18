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

import com.starrocks.common.DdlException;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class HivePartitionPruneTest extends ConnectorPlanTestBase {
    @Before
    public void setUp() throws DdlException {
        GlobalStateMgr.getCurrentState().changeCatalogDb(connectContext, "hive0.partitioned_db");
    }

    @Test
    public void testHivePartitionPrune() throws Exception {
        String sql = "select * from t1 where par_col = 0;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = 0\n" +
                "     partitions=1/3");

        sql = "select * from t1 where par_col = 1 and c1 = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = 1\n" +
                "     NON-PARTITION PREDICATES: 1: c1 = 2\n" +
                "     MIN/MAX PREDICATES: 1: c1 <= 2, 1: c1 >= 2\n" +
                "     partitions=1/3");

        sql = "select * from t1 where par_col = abs(-1) and c1 = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = CAST(abs(-1) AS INT)\n" +
                "     NON-PARTITION PREDICATES: 1: c1 = 2\n" +
                "     NO EVAL-PARTITION PREDICATES: 4: par_col = CAST(abs(-1) AS INT)\n" +
                "     MIN/MAX PREDICATES: 1: c1 <= 2, 1: c1 >= 2\n" +
                "     partitions=3/3");

        sql = "select * from t1 where par_col = 1+1 and c1 = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = 2\n" +
                "     NON-PARTITION PREDICATES: 1: c1 = 2\n" +
                "     MIN/MAX PREDICATES: 1: c1 <= 2, 1: c1 >= 2\n" +
                "     partitions=1/3");
    }

    @Test
    public void testCompoundPartitionPrune() throws Exception {
        String sql = "select * from t1 where par_col = 0 and par_col = 5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:EMPTYSET");

        sql = "select * from t1 where par_col = 0 and abs(par_col) = 3 or par_col = 5";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PARTITION PREDICATES: ((4: par_col = 0) AND (abs(4: par_col) = 3)) " +
                "OR (4: par_col = 5), 4: par_col IN (0, 5)\n" +
                "     partitions=1/3");

        sql = "select * from t1 where abs(par_col) = 3 and par_col = 0 or par_col = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PARTITION PREDICATES: ((abs(4: par_col) = 3) AND (4: par_col = 0)) " +
                "OR (4: par_col = 2), 4: par_col IN (0, 2)\n" +
                "     partitions=2/3");

        sql = "select * from t1 where abs(par_col) = 3 and par_col = 10 or par_col = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PARTITION PREDICATES: ((abs(4: par_col) = 3) AND (4: par_col = 10)) " +
                "OR (4: par_col = 2), 4: par_col IN (10, 2)\n" +
                "     partitions=1/3");

        sql = "select * from t1 where abs(par_col) = 1 and abs(par_col) = 3 or par_col = 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PARTITION PREDICATES: ((abs(4: par_col) = 1) AND " +
                "(abs(4: par_col) = 3)) OR (4: par_col = 10)\n" +
                "     NO EVAL-PARTITION PREDICATES: ((abs(4: par_col) = 1) AND (abs(4: par_col) = 3)) " +
                "OR (4: par_col = 10)\n" +
                "     partitions=3/3");

        sql = "select * from t1 where par_col = 1 or par_col = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PARTITION PREDICATES: 4: par_col IN (1, 2)\n" +
                "     partitions=2/3");

        sql = "select * from t1 where par_col = 1 or abs(par_col) = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PARTITION PREDICATES: (4: par_col = 1) OR (abs(4: par_col) = 2)\n" +
                "     NO EVAL-PARTITION PREDICATES: (4: par_col = 1) OR (abs(4: par_col) = 2)\n" +
                "     partitions=3/3");

        sql = "select * from t1 where par_col = 10 or abs(par_col) = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PARTITION PREDICATES: (4: par_col = 10) OR (abs(4: par_col) = 2)\n" +
                "     NO EVAL-PARTITION PREDICATES: (4: par_col = 10) OR (abs(4: par_col) = 2)\n" +
                "     partitions=3/3");

        sql = "select * from t1 where abs(par_col) = 1 or abs(par_col) = 2;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PARTITION PREDICATES: (abs(4: par_col) = 1) OR (abs(4: par_col) = 2)\n" +
                "     NO EVAL-PARTITION PREDICATES: (abs(4: par_col) = 1) OR (abs(4: par_col) = 2)\n" +
                "     partitions=3/3");
    }

    @Test
    public void testHivePartitionPredicatesPrune() throws Exception {
        String sql = "select a.l_orderkey,\n" +
                "    b.l_partkey\n" +
                "from (\n" +
                "        select l_orderkey,\n" +
                "            l_partkey\n" +
                "        from lineitem_mul_par2\n" +
                "        where l_shipdate = '1998-01-01'\n" +
                "            and l_returnflag = 'R'\n" +
                "        limit 10\n" +
                "    ) a\n" +
                "    join (\n" +
                "        select l_orderkey,\n" +
                "            l_partkey\n" +
                "        from lineitem_mul_par2\n" +
                "        where l_shipdate = '1998-01-01'\n" +
                "            and l_returnflag = 'A'\n" +
                "        limit 10\n" +
                "    ) b on a.l_orderkey = b.l_orderkey";
        ExecPlan plan = getExecPlan(sql);
        List<ScanNode> scanNodes = plan.getScanNodes();
        Assert.assertEquals(scanNodes.size(), 2);
        HdfsScanNode node0 = (HdfsScanNode) scanNodes.get(0);
        HdfsScanNode node1 = (HdfsScanNode) scanNodes.get(1);
        Assert.assertEquals(node0.getScanNodePredicates().getSelectedPartitionIds().size(), 1);
        Assert.assertEquals(node1.getScanNodePredicates().getSelectedPartitionIds().size(), 1);
        Assert.assertFalse(node0.getScanNodePredicates().getSelectedPartitionIds().equals(
                node1.getScanNodePredicates().getSelectedPartitionIds()));
    }
}