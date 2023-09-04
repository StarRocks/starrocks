// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Before;
import org.junit.Test;

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
                "     MIN/MAX PREDICATES: 5: c1 <= 2, 6: c1 >= 2\n" +
                "     partitions=1/3");

        sql = "select * from t1 where par_col = abs(-1) and c1 = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = CAST(abs(-1) AS INT)\n" +
                "     NON-PARTITION PREDICATES: 1: c1 = 2\n" +
                "     NO EVAL-PARTITION PREDICATES: 4: par_col = CAST(abs(-1) AS INT)\n" +
                "     MIN/MAX PREDICATES: 5: c1 <= 2, 6: c1 >= 2\n" +
                "     partitions=3/3");

        sql = "select * from t1 where par_col = 1+1 and c1 = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = 2\n" +
                "     NON-PARTITION PREDICATES: 1: c1 = 2\n" +
                "     MIN/MAX PREDICATES: 5: c1 <= 2, 6: c1 >= 2\n" +
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
}