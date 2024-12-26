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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class MvTimeSeriesRewriteWithHiveTest extends MVTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Test
    public void testAggTimeSeriesWithBasic() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW test_mv1\n" +
                "PARTITION BY dt\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "AS SELECT l_orderkey, l_suppkey, date_trunc('month', l_shipdate) as dt, sum(l_orderkey)\n" +
                "FROM hive0.partitioned_db.lineitem_par as a \n " +
                "GROUP BY l_orderkey, l_suppkey, date_trunc('month', l_shipdate);");

        // should not be rollup
        {
            String query = "SELECT l_suppkey, l_orderkey, sum(l_orderkey)  FROM hive0.partitioned_db.lineitem_par " +
                    "WHERE l_shipdate >= '1998-01-02' GROUP BY l_orderkey, l_suppkey;";

            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "     TABLE: test_mv1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 26: dt >= '1998-01-02'\n" +
                    "     partitions=1/2");
            PlanTestBase.assertContains(plan, "     TABLE: lineitem_par\n" +
                    "     PARTITION PREDICATES: (NOT (date_trunc('month', 35: l_shipdate) >= '1998-01-02')) OR " +
                    "(NOT (date_trunc('month', 35: l_shipdate) >= '1998-01-02')), 35: l_shipdate >= '1998-01-02', " +
                    "35: l_shipdate >= '1998-01-02'\n" +
                    "     NO EVAL-PARTITION PREDICATES: (NOT (date_trunc('month', 35: l_shipdate) >= '1998-01-02')) OR " +
                    "(NOT (date_trunc('month', 35: l_shipdate) >= '1998-01-02'))\n" +
                    "     partitions=4/6");
        }

        starRocksAssert.dropMaterializedView("test_mv1");
    }
}