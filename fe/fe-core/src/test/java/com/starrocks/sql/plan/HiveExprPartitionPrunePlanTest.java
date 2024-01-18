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

import org.junit.BeforeClass;
import org.junit.Test;

public class HiveExprPartitionPrunePlanTest extends PlanTestBase {

    //  Test HivePartitionNames:
    //  {par_col=0/par_date=2020-01-01}, {par_col=0/par_date=2020-01-02},
    //  {par_col=0/par_date=2020-01-03}, {par_col=1/par_date=2020-01-02},
    //  {par_col=1/par_date=2020-01-03}, {par_col=3/par_date=2020-01-04}

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Test
    public void testHiveExprPartitionPrune() throws Exception {
        String sql1 = "select * from hive0.partitioned_db.t1_par where int_divide(par_col, 1) < 1";
        String plan1 = getFragmentPlan(sql1);
        assertContains(plan1, "partitions=3/6");

        String sql2 = "select * from hive0.partitioned_db.t1_par where int_divide(par_col, 1) < 1 or par_col > 2";
        String plan2 = getFragmentPlan(sql2);
        assertContains(plan2, "partitions=4/6");

        String sql3 = "select * from hive0.partitioned_db.t1_par where int_divide(par_col, 1) < 1 and "
                + "date_trunc('day', par_date) < '2020-01-02'";
        String plan3 = getFragmentPlan(sql3);
        assertContains(plan3, "partitions=1/6");

        String sql4 = "select * from hive0.partitioned_db.t1_par where date_trunc('day', par_date) in "
                + "('2020-01-02', '2020-01-01')";
        String plan4 = getFragmentPlan(sql4);
        assertContains(plan4, "partitions=3/6");

        String sql5 = "select * from hive0.partitioned_db.t1_par where int_divide(par_col, 1) < 1 or "
                + "day(par_date) in (1, 4)";
        String plan5 = getFragmentPlan(sql5);
        assertContains(plan5, "partitions=4/6");

        String sql6 = "select * from hive0.partitioned_db.t1_par where divide(par_col, 0) is null";
        String plan6 = getFragmentPlan(sql6);
        assertContains(plan6, "partitions=6/6");

        String sql7 = "select * from hive0.partitioned_db.t1_par where divide(par_col, 0) is not null";
        String plan7 = getFragmentPlan(sql7);
        assertContains(plan7, "partitions=0/6");
    }
}
