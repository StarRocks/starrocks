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

import com.google.api.client.util.Lists;
import com.starrocks.common.FeConstants;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;

public class GroupExecutionPlanTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setEnableGroupExecution(true);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(3000000);
    }

    @AfterClass
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableGroupExecution(false);
    }

    @Test
    public void testColocateGroupExecutionJoin() throws Exception {
        FeConstants.runningUnitTest = true;
        boolean enableGroupExecution = connectContext.getSessionVariable().isEnableGroupExecution();
        connectContext.getSessionVariable().setEnableGroupExecution(true);
        try {
            // normal simple case
            List<String> querys = Lists.newArrayList();
            querys.add("select * from colocate1 l join colocate2 r on l.k1=r.k1 and l.k2=r.k2;");
            querys.add("select * from colocate1 l left join colocate2 r on l.k1=r.k1 and l.k2=r.k2;");
            querys.add("select * from colocate1 l right join colocate2 r on l.k1=r.k1 and l.k2=r.k2;");
            querys.add("select l.* from colocate1 l left semi join colocate2 r on l.k1=r.k1 and l.k2=r.k2;");
            querys.add("select l.* from colocate1 l left anti join colocate2 r on l.k1=r.k1 and l.k2=r.k2;");
            // with cross join
            querys.add("select l.* from colocate1 l, colocate2 r,colocate1 z where l.k1=r.k1 and l.k2=r.k2 ");
            // colocate join with broadcast join
            querys.add("select * from (select l.* from colocate1 l, colocate2 r where l.k1=r.k1 and l.k2=r.k2) tb" +
                    " join [broadcast] colocate1 z on z.k1 = tb.k1 ");
            querys.add("select * from (select distinct k1, k2 from colocate1)l join (select k1, k2 from colocate2)r " +
                    " on l.k1=r.k1 and l.k2=r.k2;");

            for (String sql : querys) {
                String plan = getFragmentPlan(sql);
                assertContains(plan, "colocate exec groups:");
                assertContains(plan, "COLOCATE");
            }

        } finally {
            FeConstants.runningUnitTest = false;
            connectContext.getSessionVariable().setEnableGroupExecution(enableGroupExecution);
        }
    }

    @Test
    public void testGroupExecutionAgg() throws Exception {
        FeConstants.runningUnitTest = true;
        boolean enableGroupExecution = connectContext.getSessionVariable().isEnableGroupExecution();
        connectContext.getSessionVariable().setEnableGroupExecution(true);
        try {
            List<String> querys = Lists.newArrayList();
            querys.add("select distinct k1,k2 from colocate1 l");
            querys.add("select distinct k1,k2 from colocate1 l limit 10");
            querys.add("select distinct k1,k2 from colocate1 l where k1 = 1 or k3 = 3");
            querys.add("select distinct k1,k2 from colocate1 l where k1 = 1");
            // for streaming agg
            querys.add("select distinct L_SHIPDATE, L_LINENUMBER from lineitem_partition");
            // grouping set
            querys.add("select count(*) from colocate1 group by rollup(k1,k2)");

            for (String sql : querys) {
                String plan = getFragmentPlan(sql);
                assertContains(plan, "colocate exec groups:");
            }
        } finally {
            FeConstants.runningUnitTest = false;
            connectContext.getSessionVariable().setEnableGroupExecution(enableGroupExecution);
        }
    }

    @Test
    public void unsupportedQuerys() throws Exception {
        FeConstants.runningUnitTest = true;
        boolean enableGroupExecution = connectContext.getSessionVariable().isEnableGroupExecution();
        connectContext.getSessionVariable().setEnableGroupExecution(true);
        try {
            List<String> querys = Lists.newArrayList();
            // bucket-shuffle join
            querys.add("select * from colocate1 l join [bucket] colocate2 r on l.k1=r.k1 and l.k2=r.k2;");
            // distinct after bucket shuffle join
            querys.add("select distinct l.k1,r.k2 from colocate1 l join [bucket] colocate2 r " +
                    "on l.k1=r.k1 and l.k2=r.k2;");
            // bucket shuffle join with broadcast join
            querys.add("select distinct tb.k1,z.k2 from (select l.* from colocate1 l " +
                    "join [bucket] colocate2 r on l.k1=r.k1 and l.k2=r.k2) tb " +
                    "join [broadcast] colocate1 z on z.k1 = tb.k1 ");
            querys.add("select distinct tb.k1,tb.k2,tb.k3,tb.k4 from (select l.k1 k1, l.k2 k2,r.k1 k3,r.k2 k4 " +
                    "from (select k1, k2 from colocate1 l) l join [bucket] colocate2 r on l.k1 = r.k1 and l.k2 = r.k2) tb " +
                    "join colocate1 z;");
            // intersect
            querys.add("select k1, k2 from colocate1 l intersect select k1, k2 from colocate2 r;");
            querys.add("select k1 from colocate1 l intersect select k1 from colocate2 r;");
            // union all
            querys.add("select k1 from colocate1 l union all select k1 from colocate2 r");
            querys.add("select distinct k1 from (select k1 from colocate1 l union all select k1 from colocate2 r) t;");
            // unoin
            querys.add("select k1 from colocate1 l union select k1 from colocate2 r");
            querys.add("select k1,k2 from colocate1 l union select k1,k2 from colocate2 r");
            // except
            querys.add("select distinct k1 from (select k1 from colocate1 l except select k1 from colocate2 r) t;");
            querys.add("select distinct k1,k2 from (select k1,k2 from colocate1 l except select k1,k2 from colocate2 r) t;");
            // physical limit
            querys.add("select distinct k1 from (select k1 from colocate1 l union all select k1 from colocate2 r limit 10) t;");
            querys.add("select k1,k2 in (select k1 from colocate2) from (select k1,k2 from colocate1 l) tb");
            // physical filter
            querys.add("select k1,k2 from (select k1,k2 from colocate1 l) tb where k2 = (select k2 from colocate2)");
            // table function
            querys.add("select k1,k2 from colocate1, UNNEST([])");
            querys.add("select distinct generate_series from TABLE(generate_series(65530, 65536))");


            for (String sql : querys) {
                String plan = getFragmentPlan(sql);
                assertNotContains(plan, "colocate exec groups:");
            }

        } finally {
            FeConstants.runningUnitTest = false;
            connectContext.getSessionVariable().setEnableGroupExecution(enableGroupExecution);
        }
    }

    @Test
    public void partialSupported() throws Exception {
        FeConstants.runningUnitTest = true;
        boolean enableGroupExecution = connectContext.getSessionVariable().isEnableGroupExecution();
        connectContext.getSessionVariable().setEnableGroupExecution(true);
        try {
            List<String> querys = Lists.newArrayList();
            // distinct before bucket shuffle join
            querys.add("select l.k1, l.k2 from (select distinct k1, k2 from colocate1 l) l " +
                    "join [bucket] colocate2 r on l.k1 = r.k1 and l.k2 = r.k2;");
            querys.add("select *, row_number() over() from colocate1 l " +
                    "join [colocate] colocate2 r on l.k1=r.k1 and l.k2=r.k2;");
            querys.add("select distinct k1 from colocate1 l union all select distinct k1 from colocate2 r;");
            querys.add("select k1 from colocate1 l union all select distinct k1 from colocate2 r;");
            querys.add("select k1 from colocate1 l union all select distinct k1 from colocate2 r;");
            // assert node
            querys.add("select k1,k2 in (select k1 from colocate2) from (select distinct k1,k2 from colocate1 l) tb");
            querys.add("select k1,k2 = (select k1 from colocate2) from (select distinct k1,k2 from colocate1 l) tb");
            // table function
            querys.add("select distinct k1,k2 from colocate1, UNNEST([])");

            for (String sql : querys) {
                String plan = getFragmentPlan(sql);
                assertContains(plan, "colocate exec groups:");
            }
        } finally {
            FeConstants.runningUnitTest = false;
            connectContext.getSessionVariable().setEnableGroupExecution(enableGroupExecution);
        }
    }

}
