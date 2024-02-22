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
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;

public class MvRewriteHiveTest extends MvRewriteTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
    }

    @Test
    public void testHiveJoinMvRewrite() throws Exception {
        createAndRefreshMv("create materialized view hive_join_mv_1" +
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

        createAndRefreshMv("create materialized view hive_join_mv_2" +
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
        createAndRefreshMv("create materialized view hive_agg_join_mv_1" +
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
        createAndRefreshMv("create materialized view hive_union_mv_1 distributed by hash(s_suppkey) " +
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
                "     NON-PARTITION PREDICATES: 13: s_suppkey < 10, 13: s_suppkey >= 5");

        dropMv("test", "hive_union_mv_1");
    }

    @Test
    public void testHiveQueryWithMvs() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        // enforce choose the hive scan operator, not mv plan
        connectContext.getSessionVariable().setUseNthExecPlan(1);
        createAndRefreshMv("create materialized view hive_join_mv_1" +
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
        createAndRefreshMv("create materialized view hive_staleness_1 distributed by hash(s_suppkey) " +
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

            Set<String> partitionsToRefresh = getPartitionNamesToRefreshForMv(mv1);
            Assert.assertTrue(partitionsToRefresh.isEmpty());
        }
        starRocksAssert.dropMaterializedView("hive_staleness_1");
    }

    @Test
    public void testHivePartitionPrune1() throws Exception {
        createAndRefreshMv("CREATE MATERIALIZED VIEW `hive_partition_prune_mv1`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"force_external_table_query_rewrite\" = \"true\"" +
                        ")\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                        "FROM `hive0`.`partitioned_db`.`lineitem_par` as a \n " +
                        "GROUP BY " +
                        "`l_orderkey`, `l_suppkey`, `l_shipdate`;");

        // should not be rollup
        {
            String query = "SELECT `l_suppkey`, `l_orderkey`, sum(l_orderkey)  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                    "WHERE l_shipdate = '1998-01-01' GROUP BY `l_orderkey`, `l_suppkey`;";

            String plan = getFragmentPlan(query);
            System.out.println(plan);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "partitions=1/6\n" +
                    "     rollup: hive_partition_prune_mv1");
        }
        // should not be rollup
        {
            String query = "SELECT l_suppkey, sum(l_orderkey)  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                    "WHERE l_shipdate = '1998-01-01' and l_orderkey=1 GROUP BY `l_suppkey`;";

            String plan = getFragmentPlan(query);
            System.out.println(plan);
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "PREDICATES: 18: l_orderkey = 1\n" +
                    "     partitions=1/6\n" +
                    "     rollup: hive_partition_prune_mv1");
        }
        // rollup
        {
            String query = "SELECT l_suppkey, sum(l_orderkey)  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                    "WHERE l_orderkey>1 GROUP BY `l_suppkey`;";
            String plan = getFragmentPlan(query);
            System.out.println(plan);
            PlanTestBase.assertContains(plan, "1:AGGREGATE (update serialize)\n" +
                            "  |  STREAMING\n" +
                            "  |  output: sum(21: sum(l_orderkey))\n" +
                            "  |  group by: 19: l_suppkey");
            PlanTestBase.assertContains(plan, "PREDICATES: 18: l_orderkey > 1\n" +
                    "     partitions=6/6\n" +
                    "     rollup: hive_partition_prune_mv1");
        }
        starRocksAssert.dropMaterializedView("hive_partition_prune_mv1");
    }
}
