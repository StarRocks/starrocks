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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class HivePartitionPruneTest extends ConnectorPlanTestBase {
    @BeforeEach
    public void setUp() {
        super.setUp();
        try {
            connectContext.changeCatalogDb("hive0.partitioned_db");
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
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
                "     NO EVAL-PARTITION PREDICATES: ((4: par_col = 0) AND (abs(4: par_col) = 3)) " +
                "OR (4: par_col = 5)\n" +
                "     partitions=1/3");

        sql = "select * from t1 where abs(par_col) = 3 and par_col = 0 or par_col = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PARTITION PREDICATES: ((abs(4: par_col) = 3) AND (4: par_col = 0)) " +
                "OR (4: par_col = 2), 4: par_col IN (0, 2)\n" +
                "     NO EVAL-PARTITION PREDICATES: ((abs(4: par_col) = 3) AND (4: par_col = 0)) " +
                "OR (4: par_col = 2)\n" +
                "     partitions=2/3");

        sql = "select * from t1 where abs(par_col) = 3 and par_col = 10 or par_col = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PARTITION PREDICATES: ((abs(4: par_col) = 3) AND (4: par_col = 10)) " +
                "OR (4: par_col = 2), 4: par_col IN (10, 2)\n" +
                "     NO EVAL-PARTITION PREDICATES: ((abs(4: par_col) = 3) AND (4: par_col = 10)) " +
                "OR (4: par_col = 2)\n" +
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

        sql = "select * from t1 where par_col = 0 or (par_col = 1 and abs(par_col) = 2);";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PARTITION PREDICATES: (4: par_col = 0) OR ((4: par_col = 1) " +
                "AND (abs(4: par_col) = 2)), 4: par_col IN (0, 1)\n" +
                "     NO EVAL-PARTITION PREDICATES: (4: par_col = 0) OR ((4: par_col = 1) " +
                "AND (abs(4: par_col) = 2))\n" +
                "     partitions=2/3");
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
        Assertions.assertEquals(scanNodes.size(), 2);
        HdfsScanNode node0 = (HdfsScanNode) scanNodes.get(0);
        HdfsScanNode node1 = (HdfsScanNode) scanNodes.get(1);
        Assertions.assertEquals(node0.getScanNodePredicates().getSelectedPartitionIds().size(), 1);
        Assertions.assertEquals(node1.getScanNodePredicates().getSelectedPartitionIds().size(), 1);
        Assertions.assertFalse(node0.getScanNodePredicates().getSelectedPartitionIds().equals(
                node1.getScanNodePredicates().getSelectedPartitionIds()));
    }

    @Test
    public void testLikeInPartitionColumn() throws Exception {
        String sql = "select * from hive0.datacache_db.single_partition_table where l_shipdate LIKE '1998-01-03'";
        assertPlanContains(sql, "partitions=1/1");
    }

    @Test
    public void testWithDuplicatePartition() throws Exception {
        assertPlanContains("select * from hive0.partitioned_db.duplicate_partition", "partitions=2/2");
        assertPlanContains("select * from hive0.partitioned_db.duplicate_partition where day='2012-01-01'",
                "partitions=2/2");
        assertPlanContains("select * from hive0.partitioned_db.duplicate_partition where day='2012-01-01' and hour=6",
                "partitions=2/2");
        assertPlanContains("select * from hive0.partitioned_db.duplicate_partition where day='2012-01-01' and hour>0",
                "partitions=2/2");
        assertPlanContains("select * from hive0.partitioned_db.duplicate_partition where day='2012-01-01' and hour=0",
                "partitions=0/2");
        assertPlanContains("select * from hive0.partitioned_db.duplicate_partition where day='2012-01-01' and hour > 10",
                "partitions=0/2");
        assertPlanContains("select * from hive0.partitioned_db.duplicate_partition where day='2012-01-01' and hour < 10",
                "partitions=2/2");
    }

    @Test
    public void testFunctionPredicatePrunesPartitions() throws Exception {
        // o_orderdate is a DATE partition column with one partition per day from 1991-01-01 to 1993-12-30 (1095 days)
        String sql = "select * from orders where year(o_orderdate) = 1992";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "9: o_orderdate >= '1992-01-01'", "9: o_orderdate < '1993-01-01'", "partitions=366/1095");

        sql = "select * from orders where date_trunc('month', o_orderdate) = '1992-02-01'";
        assertContains(getFragmentPlan(sql), "partitions=29/1095");

        connectContext.getSessionVariable().setEnableHiveFunctionPredicatePruning(false);
        try {
            plan = getFragmentPlan("select * from orders where year(o_orderdate) = 1992");
            assertNotContains(plan, "o_orderdate >= '1992-01-01'");
        } finally {
            connectContext.getSessionVariable().setEnableHiveFunctionPredicatePruning(true);
        }
    }

    @Test
    public void testFunctionPredicateOnDataColumn() throws Exception {
        // l_shipdate is not a partition column: the derived range goes to the BE and its min/max row group check
        String plan = getFragmentPlan("select * from hive0.tpch.lineitem where year(l_shipdate) = 1995");
        assertContains(plan, "NON-PARTITION PREDICATES: year(11: l_shipdate) = 1995, "
                + "11: l_shipdate >= '1995-01-01', 11: l_shipdate < '1996-01-01'");
        assertContains(plan, "MIN/MAX PREDICATES: 11: l_shipdate >= '1995-01-01', 11: l_shipdate < '1996-01-01'");
    }

    @Test
    public void testFunctionPredicateInList() throws Exception {
        // on a partition column the OR of the IN ranges prunes partitions (1992-01 and 1993-03: 31 + 31 days)
        String plan = getFragmentPlan("select * from orders "
                + "where date_trunc('month', o_orderdate) in ('1992-01-01', '1993-03-01')");
        assertContains(plan, "partitions=62/1095");
        // on a data column it would only be evaluated on every row next to the IN itself
        plan = getFragmentPlan("select * from hive0.tpch.lineitem where year(l_shipdate) in (1995, 1997)");
        assertNotContains(plan, "l_shipdate >= '1995-01-01'");
    }
}
