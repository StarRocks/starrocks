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
                "     PARTITION PREDICATES: 4: par_col = '0'\n" +
                "     partitions=1/3");

        sql = "select * from t1 where par_col = 1 and c1 = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = '1'\n" +
                "     NON-PARTITION PREDICATES: 1: c1 = 2\n" +
                "     MIN/MAX PREDICATES: 5: c1 <= 2, 6: c1 >= 2\n" +
                "     partitions=1/3");

        sql = "select * from t1 where par_col = abs(-1) and c1 = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = CAST(abs(-1) AS VARCHAR(1048576))\n" +
                "     NON-PARTITION PREDICATES: 1: c1 = 2\n" +
                "     NO EVAL-PARTITION PREDICATES: 4: par_col = CAST(abs(-1) AS VARCHAR(1048576))\n" +
                "     MIN/MAX PREDICATES: 5: c1 <= 2, 6: c1 >= 2\n" +
                "     partitions=3/3");

        sql = "select * from t1 where par_col = 1+1 and c1 = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "0:HdfsScanNode\n" +
                "     TABLE: t1\n" +
                "     PARTITION PREDICATES: 4: par_col = '2'\n" +
                "     NON-PARTITION PREDICATES: 1: c1 = 2\n" +
                "     MIN/MAX PREDICATES: 5: c1 <= 2, 6: c1 >= 2\n" +
                "     partitions=1/3");
    }
}