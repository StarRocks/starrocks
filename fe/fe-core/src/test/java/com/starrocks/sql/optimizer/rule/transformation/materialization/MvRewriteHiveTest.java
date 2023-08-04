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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class MvRewriteHiveTest extends MvRewriteTestBase {
    @Test
    public void testHiveJoinMvRewrite() throws Exception {
        createAndRefreshMv("test", "hive_join_mv_1", "create materialized view hive_join_mv_1" +
                " distributed by hash(s_suppkey)" +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\"\n" +
                ") " +
                " as " +
                " SELECT s_suppkey , s_name, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_suppkey < 100");

        String query1 = "SELECT (s_suppkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where s_suppkey < 100";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "hive_join_mv_1");

        String query2 = "SELECT (s_suppkey + 1) * 2, n_name, n_comment" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where s_suppkey < 100";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertNotContains(plan2, "hive_join_mv_1");

        String query3 = "SELECT (s_suppkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where s_suppkey = 99";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "hive_join_mv_1");

        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(false);
        String query4 = "SELECT (s_suppkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where s_suppkey < 101";
        String plan4 = getFragmentPlan(query4);
        PlanTestBase.assertNotContains(plan4, "hive_join_mv_1");

        String query5 = "SELECT (s_suppkey + 1) * 2, n_name from hive0.tpch.supplier join hive0.tpch.nation on " +
                "s_nationkey = n_nationkey where s_suppkey < 100 and s_suppkey > 10";
        String plan5 = getFragmentPlan(query5);
        PlanTestBase.assertContains(plan5, "hive_join_mv_1");

        String query6 = "SELECT (s_suppkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey";
        String plan6 = getFragmentPlan(query6);
        PlanTestBase.assertNotContains(plan6, "hive_join_mv_1");

        dropMv("test", "hive_join_mv_1");

        createAndRefreshMv("test", "hive_join_mv_2", "create materialized view hive_join_mv_2" +
                " distributed by hash(s_nationkey)" +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\"\n" +
                ") " +
                " as " +
                " SELECT s_nationkey , s_name, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey <= 100");

        // test on equivalence classes for output and predicates
        String query7 = "SELECT (n_nationkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where n_nationkey < 100";
        String plan7 = getFragmentPlan(query7);
        PlanTestBase.assertContains(plan7, "hive_join_mv_2");

        String query8 = "SELECT (n_nationkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where n_nationkey < 10";
        String plan8 = getFragmentPlan(query8);
        PlanTestBase.assertContains(plan8, "hive_join_mv_2");

        String query9 = "SELECT (n_nationkey + 1) * 2, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation on s_nationkey = n_nationkey where n_nationkey = 100";
        String plan9 = getFragmentPlan(query9);
        PlanTestBase.assertContains(plan9, "hive_join_mv_2");

        String query10 = "SELECT (n_nationkey + 1) * 2, n_name from hive0.tpch.supplier join hive0.tpch.nation on " +
                "s_nationkey = n_nationkey where n_nationkey between 10 and 20";
        String plan10 = getFragmentPlan(query10);
        PlanTestBase.assertContains(plan10, "hive_join_mv_2");

        dropMv("test", "hive_join_mv_2");
    }

    @Test
    public void testHiveAggregateMvRewrite() throws Exception {
        createAndRefreshMv("test", "hive_agg_join_mv_1", "create materialized view hive_agg_join_mv_1" +
                " distributed by hash(s_nationkey)" +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\"\n" +
                ") " +
                " as " +
                " SELECT s_nationkey , n_name, sum(s_acctbal) as total_sum" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey < 100 " +
                "group by s_nationkey , n_name");

        String query1 = " SELECT s_nationkey , n_name, sum(s_acctbal) as total_sum" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey = 1 " +
                "group by s_nationkey , n_name";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "hive_agg_join_mv_1");

        String query2 = " SELECT s_nationkey , n_name, sum(s_acctbal) as total_sum" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey < 100 " +
                "group by s_nationkey , n_name";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "hive_agg_join_mv_1");

        String query3 = " SELECT s_nationkey , sum(s_acctbal) as total_sum" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_nationkey < 99 " +
                "group by s_nationkey";
        String plan3 = getFragmentPlan(query3);
        PlanTestBase.assertContains(plan3, "hive_agg_join_mv_1");
    }

    @Test
    public void testHiveUnionRewrite() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        createAndRefreshMv("test", "hive_union_mv_1",
                "create materialized view hive_union_mv_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5");
        String query1 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 10";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "0:UNION");
        PlanTestBase.assertContains(plan1, "hive_union_mv_1");
        PlanTestBase.assertContains(plan1, "1:HdfsScanNode\n" +
                "     TABLE: supplier\n" +
                "     NON-PARTITION PREDICATES: 13: s_suppkey < 10, 13: s_suppkey > 4");
        dropMv("test", "hive_union_mv_1");
    }

    @Test
    public void testHiveQueryWithMvs() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        // enforce choose the hive scan operator, not mv plan
        connectContext.getSessionVariable().setUseNthExecPlan(1);
        createAndRefreshMv("test", "hive_join_mv_1", "create materialized view hive_join_mv_1" +
                " distributed by hash(s_suppkey)" +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\"\n" +
                ") " +
                " as " +
                " SELECT s_suppkey , s_name, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_suppkey < 100");

        String query1 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 10";
        String plan = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan, "TABLE: supplier", "NON-PARTITION PREDICATES: 1: s_suppkey < 10");

        connectContext.getSessionVariable().setUseNthExecPlan(0);
        dropMv("test", "hive_join_mv_1");
    }

    @Test
    public void testHiveStaleness() throws Exception {
        createAndRefreshMv("test", "hive_staleness_1",
                "create materialized view hive_staleness_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"mv_rewrite_staleness_second\" = \"60\"," +
                        "\"force_external_table_query_rewrite\" = \"true\"\n" +
                        ") " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey = 5");

        // no refresh partitions if mv_rewrite_staleness is set.
        {
            MaterializedView mv1 = getMv("test", "hive_staleness_1");
            Assert.assertTrue(mv1.maxBaseTableRefreshTimestamp().isPresent());

            long mvMaxBaseTableRefreshTimestamp = mv1.maxBaseTableRefreshTimestamp().get();

            long mvRefreshTimeStamp = mv1.getLastRefreshTime();
            Assert.assertTrue(mvRefreshTimeStamp == mvMaxBaseTableRefreshTimestamp);
            Assert.assertTrue((mvMaxBaseTableRefreshTimestamp - mvRefreshTimeStamp) / 1000 < 60);
            Assert.assertTrue(mv1.isStalenessSatisfied());

            Set<String> partitionsToRefresh = mv1.getPartitionNamesToRefreshForMv(true);
            Assert.assertTrue(partitionsToRefresh.isEmpty());
        }
        starRocksAssert.dropMaterializedView("hive_staleness_1");
    }
}
