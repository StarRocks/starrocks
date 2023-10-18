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

import com.starrocks.qe.ConnectContext;
import org.junit.Before;
import org.junit.Test;

public class PushDownTopnNTest extends PlanTestBase {
    @Before
    public void before() {
        connectContext.getSessionVariable().setCboPushDownTopNLimit(1000);
    }

    @Test
    public void testPushDownTopBelowUnionAll1() throws Exception {
        String sql = "select * from (" +
                "select v1 as a, v2 as b, v3 as c from t0 " +
                "union all " +
                "select v4 as a, v5 as b, v6 as c from t1) AS t \n" +
                "ORDER BY t.b desc limit 20";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:TOP-N\n" +
                "  |  order by: <slot 5> 5: v5 DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 20\n" +
                "  |  \n" +
                "  5:OlapScanNode\n" +
                "     TABLE: t");
        assertContains(plan, "2:TOP-N\n" +
                "  |  order by: <slot 2> 2: v2 DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 20\n" +
                "  |  \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testPushDownTopBelowUnionAll2() throws Exception {
        String sql = "select * from (" +
                " select * from (select v1 as a, v2 as b, v3 as c from t0 limit 1) t00 " +
                "union all " +
                "select v4 as a, v5 as b, v6 as c from t1) AS t \n" +
                "ORDER BY t.b desc limit 20";
        String plan = getFragmentPlan(sql);
        // t0 should be added `topn` for limit 1 is less than limit 20
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");
        assertContains(plan, "5:TOP-N\n" +
                "  |  order by: <slot 5> 5: v5 DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 20\n" +
                "  |  \n" +
                "  4:OlapScanNode\n" +
                "     TABLE: t1");
    }

    @Test
    public void testPushDownTopBelowUnionAll3() throws Exception {
        String sql = "select * from (" +
                " select * from (select v1 as a, v2 as b, v3 as c from t0) t00 " +
                "union all " +
                "select v4 as a, v5 as b, v6 as c from t1) AS t \n" +
                "ORDER BY t.b desc limit 1, 20";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    RANDOM\n" +
                "\n" +
                "  3:OlapScanNode\n" +
                "     TABLE: t1");
    }

    @Test
    public void testPushDownTopBelowUnionAll4() throws Exception {
        String sql = "select * from (" +
                "select v1 as a, v2 as b, v3 as c from t0 " +
                "union all " +
                "select v4, v5 , v6  from t1) AS t \n" +
                "ORDER BY t.b desc limit 20";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:TOP-N\n" +
                "  |  order by: <slot 5> 5: v5 DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 20\n" +
                "  |  \n" +
                "  5:OlapScanNode\n" +
                "     TABLE: t1");
        assertContains(plan, "2:TOP-N\n" +
                "  |  order by: <slot 2> 2: v2 DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 20\n" +
                "  |  \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");
    }

    @Test
    public void testPushDownTopBelowUnionAll5() throws Exception {
        ConnectContext context = ConnectContext.get();
        String sql = "select * from (" +
                "select v1 as a, v2 as b, v3 as c from t0 " +
                "union all " +
                "select v4, v5 , v6  from t1) AS t \n" +
                "ORDER BY t.b desc limit " + (context.getSessionVariable().getCboPushDownTopNLimit() + 1);
        String plan = getFragmentPlan(sql);
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    RANDOM\n" +
                "\n" +
                "  3:OlapScanNode\n" +
                "     TABLE: t1");
    }

    @Test
    public void testPushDownTopBelowUnionAll6() throws Exception {
        ConnectContext context = ConnectContext.get();
        String sql = "select * from (" +
                "select v1 as a, v2 as b, v3 as c from t0 " +
                "union " +
                "select v4, v5 , v6  from t1) AS t \n" +
                "ORDER BY t.b desc limit " + (context.getSessionVariable().getCboPushDownTopNLimit() + 1);
        String plan = getFragmentPlan(sql);
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t0");
        assertContains(plan, "STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    RANDOM\n" +
                "\n" +
                "  3:OlapScanNode\n" +
                "     TABLE: t1");
    }
}
