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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static com.starrocks.utframe.UtFrameUtils.getQueryScanOperators;

public class MvRewriteHiveTest extends MVTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Test
    public void testHiveJoinMvRewrite() throws Exception {
        createAndRefreshMv("create materialized view hive_join_mv_1" +
                " distributed by hash(s_suppkey)" +
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
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5");
        String query1 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 10";
        String plan1 = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan1, "0:UNION");
        PlanTestBase.assertContains(plan1, "hive_union_mv_1");
        PlanTestBase.assertContains(plan1, "1:HdfsScanNode\n" +
                "     TABLE: supplier\n" +
                "     NON-PARTITION PREDICATES: 12: s_suppkey < 10, 12: s_suppkey >= 5");
        dropMv("test", "hive_union_mv_1");
    }

    @Test
    public void testHiveQueryWithMvs() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        // enforce choose the hive scan operator, not mv plan
        connectContext.getSessionVariable().setUseNthExecPlan(1);
        createAndRefreshMv("create materialized view hive_union_mv_1 distributed by hash(s_suppkey) " +
                        " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5");
        createAndRefreshMv("create materialized view hive_join_mv_1" +
                " distributed by hash(s_suppkey)" +
                " as " +
                " SELECT s_suppkey , s_name, n_name" +
                " from hive0.tpch.supplier join hive0.tpch.nation" +
                " on s_nationkey = n_nationkey" +
                " where s_suppkey < 100");

        String query1 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 10";
        String plan = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan, "TABLE: supplier", "NON-PARTITION PREDICATES: 18: s_suppkey < 10, 18: s_suppkey >= 5");
        connectContext.getSessionVariable().setUseNthExecPlan(0);
        dropMv("test", "hive_union_mv_1");
        dropMv("test", "hive_join_mv_1");
    }

    @Test
    public void testHiveStaleness() throws Exception {
        createAndRefreshMv("create materialized view hive_staleness_1 distributed by hash(s_suppkey) " +
                        "PROPERTIES (\n" +
                        "\"mv_rewrite_staleness_second\" = \"60\"" +
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
            PlanTestBase.assertNotContains(plan, "AGGREGATE");
            PlanTestBase.assertContains(plan, "partitions=1/6\n" +
                    "     rollup: hive_partition_prune_mv1");
        }
        // should not be rollup
        {
            String query = "SELECT l_suppkey, sum(l_orderkey)  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                    "WHERE l_shipdate = '1998-01-01' and l_orderkey=1 GROUP BY `l_suppkey`;";

            String plan = getFragmentPlan(query);
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

    @Test
    public void testPartitionedHiveMVWithLooseMode() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `hive_partitioned_mv`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`l_shipdate`)\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"force_external_table_query_rewrite\" = \"true\",\n" +
                "\"query_rewrite_consistency\" = \"loose\"" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_par` as a \n " +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;");

        refreshMaterializedViewWithPartition("test", "hive_partitioned_mv",
                "1998-01-02", "1998-01-04");

        MaterializedView mv1 = getMv("test", "hive_partitioned_mv");
        Set<String> toRefreshPartitions = getPartitionNamesToRefreshForMv(mv1);
        Assert.assertEquals(4, toRefreshPartitions.size());
        Assert.assertTrue(toRefreshPartitions.contains("p19980101"));
        Assert.assertTrue(toRefreshPartitions.contains("p19980104"));
        Assert.assertTrue(toRefreshPartitions.contains("p19980105"));

        String query1 = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_par` as a \n " +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;";
        String plan = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan, "hive_partitioned_mv");
        dropMv("test", "hive_partitioned_mv");
    }

    @Test
    public void testPartitionedHiveMVWithLooseMode_MultiColumn() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `hive_partitioned_mv`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (str2date(`l_shipdate`, '%Y-%m-%d'))\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"query_rewrite_consistency\" = \"loose\"" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_mul_par3` as a \n " +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;");

        String mvName = "hive_partitioned_mv";
        refreshMaterializedViewWithPartition("test", mvName, "1998-01-02", "1998-01-04");

        MaterializedView mv1 = getMv("test", mvName);
        Set<String> toRefreshPartitions = getPartitionNamesToRefreshForMv(mv1);
        Assert.assertEquals(3, toRefreshPartitions.size());
        Assert.assertEquals(
                ImmutableSet.of("p19980101_19980102", "p19980104_19980105", "p19980105_19980106"), toRefreshPartitions);

        String query1 = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_mul_par3` as a \n " +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;";
        String plan = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan, mvName, "UNION");

        starRocksAssert.query("SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_mul_par3` as a \n " +
                "WHERE l_shipdate='1998-01-02'\n" +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;").explainContains(mvName);
        starRocksAssert.query("SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_mul_par3` as a \n " +
                "WHERE l_shipdate='1998-01-03'\n" +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;").explainContains(mvName);
        starRocksAssert.query("SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_mul_par3` as a \n " +
                "WHERE l_shipdate='1998-01-01'\n" +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;").explainWithout(mvName);
        starRocksAssert.query("SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_mul_par3` as a \n " +
                "WHERE l_shipdate='1998-01-05'\n" +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;").explainWithout(mvName);

        dropMv("test", "hive_partitioned_mv");
    }

    @Test
    public void testUnPartitionedHiveMVWithLooseMode() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `hive_unpartitioned_mv`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"force_external_table_query_rewrite\" = \"true\",\n" +
                "\"query_rewrite_consistency\" = \"loose\"" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_par` as a \n " +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;");
        MaterializedView mv1 = getMv("test", "hive_unpartitioned_mv");
        MvUpdateInfo mvUpdateInfo = getMvUpdateInfo(mv1);
        Set<String> toRefreshPartitions = mvUpdateInfo.getMvToRefreshPartitionNames();
        Assert.assertTrue(mvUpdateInfo.getMvToRefreshType() == MvUpdateInfo.MvToRefreshType.FULL);
        Assert.assertTrue(!mvUpdateInfo.isValidRewrite());
        Assert.assertEquals(0, toRefreshPartitions.size());

        toRefreshPartitions.clear();
        refreshMaterializedView("test", "hive_unpartitioned_mv");
        toRefreshPartitions = getPartitionNamesToRefreshForMv(mv1);
        Assert.assertEquals(0, toRefreshPartitions.size());

        String query1 = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_par` as a \n " +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;";
        String plan = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan, "hive_unpartitioned_mv");
        dropMv("test", "hive_unpartitioned_mv");
    }

    @Test
    public void testHiveEmptyMV_UnPartitioned_NotRewritten() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view hive_empty_mv " +
                " distributed by hash(s_suppkey) " +
                " refresh deferred manual  " +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"false\"\n" +
                ") " +
                " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5");
        // mv is not refreshed, even mv has no partitions, it should not be rewritten.
        String query1 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 10";
        String plan = getFragmentPlan(query1);
        PlanTestBase.assertNotContains(plan, "hive_empty_mv");
        dropMv("test", "hive_empty_mv");
    }

    @Test
    public void testHiveEmptyMV_UnPartitioned_Rewritten() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view hive_empty_mv " +
                " distributed by hash(s_suppkey) " +
                " refresh deferred manual  " +
                "PROPERTIES (\n" +
                "\"force_external_table_query_rewrite\" = \"true\"\n" +
                ") " +
                " as select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 5");
        refreshMaterializedView("test", "hive_empty_mv");

        // mv is not refreshed, even mv has no partitions, it should not be rewritten.
        String query1 = "select s_suppkey, s_name, s_address, s_acctbal from hive0.tpch.supplier where s_suppkey < 10";
        String plan = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan, "hive_empty_mv");
        dropMv("test", "hive_empty_mv");
    }

    @Test
    public void testHiveEmptyMV_Partitioned_NotRewritten() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `hive_partitioned_mv`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`l_shipdate`)\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"force_external_table_query_rewrite\" = \"false\"" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_par` as a \n " +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;");
        // mv is not refreshed, even mv has no partitions, it should not be rewritten.
        String query1 = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_par` as a \n " +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;";
        String plan = getFragmentPlan(query1);
        PlanTestBase.assertNotContains(plan, "hive_partitioned_mv");
        dropMv("test", "hive_partitioned_mv");
    }

    @Test
    public void testHiveEmptyMV_Partitioned_Rewritten() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `hive_partitioned_mv`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`l_shipdate`)\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"force_external_table_query_rewrite\" = \"true\"" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_par` as a \n " +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;");
        refreshMaterializedView("test", "hive_partitioned_mv");
        String query1 = "SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_orderkey)  " +
                "FROM `hive0`.`partitioned_db`.`lineitem_par` as a \n " +
                "GROUP BY " +
                "`l_orderkey`, `l_suppkey`, `l_shipdate`;";
        String plan = getFragmentPlan(query1);
        PlanTestBase.assertContains(plan, "hive_partitioned_mv");
        dropMv("test", "hive_partitioned_mv");
    }

    private ScanOperatorPredicates getScanOperatorPredicates(LogicalScanOperator logicalScanOperator) {
        try {
            return logicalScanOperator.getScanOperatorPredicates();
        } catch (Exception e) {
            Assert.fail();
        }
        return null;
    }

    @Test
    public void testHivePartitionPruner0() {
        String query = "SELECT `l_suppkey`, `l_orderkey`, sum(l_orderkey)  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "GROUP BY `l_orderkey`, `l_suppkey`;";
        List<LogicalScanOperator> scanOperators = getQueryScanOperators(connectContext, query);
        Assert.assertTrue(scanOperators.size() == 1);
        ScanOperatorPredicates scanOperatorPredicates = getScanOperatorPredicates(scanOperators.get(0));
        Assert.assertTrue(scanOperatorPredicates != null);
        Assert.assertTrue(scanOperatorPredicates.getIdToPartitionKey().size() == 6);
        Assert.assertTrue(scanOperatorPredicates.getPartitionConjuncts().size() == 0);
        Assert.assertTrue(scanOperatorPredicates.getSelectedPartitionIds().size() == 6);
        Assert.assertTrue(scanOperatorPredicates.getPrunedPartitionConjuncts().size() == 0);
        Assert.assertTrue(scanOperatorPredicates.getNonPartitionConjuncts().size() == 0);
        Assert.assertTrue(scanOperators.get(0).getPredicate() == null);
        Assert.assertTrue(scanOperatorPredicates.toString().equals("selectedPartitionIds=[0, 1, 2, 3, 4, 5]"));
    }

    @Test
    public void testHivePartitionPruner1() {
        // queries that can be pruned by optimizer
        List<String> queries = ImmutableList.of(
                "SELECT `l_suppkey`, `l_orderkey`, sum(l_orderkey)  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                        "WHERE l_shipdate = '1998-01-01' GROUP BY `l_orderkey`, `l_suppkey`;",
                "SELECT `l_suppkey`, `l_orderkey`, sum(l_orderkey)  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                        "WHERE date_sub(l_shipdate, interval 1 day) = '1998-01-02' GROUP BY `l_orderkey`, `l_suppkey`;"
        );

        List<String> expects = ImmutableList.of(
                "selectedPartitionIds=[1], partitionConjuncts=[16: l_shipdate = 1998-01-01]",
                "selectedPartitionIds=[3], partitionConjuncts=[16: l_shipdate = 1998-01-03]"
        );
        for (int i = 0; i < queries.size(); i++) {
            String query = queries.get(i);
            List<LogicalScanOperator> scanOperators = getQueryScanOperators(connectContext, query);
            Assert.assertTrue(scanOperators.size() == 1);
            ScanOperatorPredicates scanOperatorPredicates = getScanOperatorPredicates(scanOperators.get(0));
            Assert.assertTrue(scanOperatorPredicates != null);
            Assert.assertTrue(scanOperatorPredicates.getIdToPartitionKey().size() == 6);
            Assert.assertTrue(scanOperatorPredicates.getPartitionConjuncts().size() == 1);
            Assert.assertTrue(scanOperatorPredicates.getSelectedPartitionIds().size() == 1);
            Assert.assertTrue(scanOperatorPredicates.getNonPartitionConjuncts().size() == 0);
            Assert.assertTrue(scanOperatorPredicates.getNoEvalPartitionConjuncts().size() == 0);
            Assert.assertTrue(scanOperatorPredicates.getPrunedPartitionConjuncts().size() == 1);
            // TODO: fixme
            Assert.assertTrue(scanOperators.get(0).getPredicate() != null);
            Assert.assertTrue(scanOperatorPredicates.toString().equals(expects.get(i)));
        }
    }

    @Test
    public void testHivePartitionPruner2() {
        String query = "SELECT `l_suppkey`, `l_orderkey`, sum(l_orderkey)  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "WHERE date_trunc('month', l_shipdate) = date_sub('1998-01-02', interval 1 day) " +
                "GROUP BY `l_orderkey`, `l_suppkey`;";
        List<LogicalScanOperator> scanOperators = getQueryScanOperators(connectContext, query);
        Assert.assertTrue(scanOperators.size() == 1);
        ScanOperatorPredicates scanOperatorPredicates = getScanOperatorPredicates(scanOperators.get(0));
        Assert.assertTrue(scanOperatorPredicates != null);
        Assert.assertTrue(scanOperatorPredicates.getIdToPartitionKey().size() == 6);
        Assert.assertTrue(scanOperatorPredicates.getPartitionConjuncts().size() == 1);
        Assert.assertTrue(scanOperatorPredicates.getNonPartitionConjuncts().size() == 0);
        Assert.assertTrue(scanOperatorPredicates.getSelectedPartitionIds().size() == 6);
        Assert.assertTrue(scanOperatorPredicates.getNoEvalPartitionConjuncts().size() == 1);
        Assert.assertTrue(scanOperatorPredicates.getPrunedPartitionConjuncts().size() == 0);
        Assert.assertTrue(scanOperators.get(0).getPredicate() != null);
        Assert.assertTrue(scanOperatorPredicates.toString().equals("selectedPartitionIds=[0, 1, 2, 3, 4, 5], " +
                "partitionConjuncts=[date_trunc(month, 16: l_shipdate) = 1998-01-01], " +
                "noEvalPartitionConjuncts=[date_trunc(month, 16: l_shipdate) = 1998-01-01]"));
    }

    @Test
    public void testHivePartitionPruner3() {
        String query = "SELECT `l_suppkey`, `l_orderkey`, sum(l_orderkey)  FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                " WHERE date_trunc('month', l_shipdate) = date_sub('1998-01-02', interval 1 day) " +
                " and l_shipdate >= '1998-01-01' and l_orderkey > 1000 " +
                " GROUP BY `l_orderkey`, `l_suppkey`;";
        List<LogicalScanOperator> scanOperators = getQueryScanOperators(connectContext, query);
        Assert.assertTrue(scanOperators.size() == 1);
        ScanOperatorPredicates scanOperatorPredicates = getScanOperatorPredicates(scanOperators.get(0));
        Assert.assertTrue(scanOperatorPredicates != null);
        Assert.assertTrue(scanOperatorPredicates.getIdToPartitionKey().size() == 6);
        Assert.assertTrue(scanOperatorPredicates.getNonPartitionConjuncts().size() == 1);
        Assert.assertTrue(scanOperatorPredicates.getSelectedPartitionIds().size() == 5);

        Assert.assertTrue(scanOperatorPredicates.getPartitionConjuncts().size() == 2);
        Assert.assertTrue(scanOperatorPredicates.getNoEvalPartitionConjuncts().size() == 1);
        Assert.assertTrue(scanOperatorPredicates.getPrunedPartitionConjuncts().size() == 1);

        Assert.assertTrue(scanOperators.get(0).getPredicate() != null);
        List<ScalarOperator> predicates = Utils.extractConjuncts(scanOperators.get(0).getPredicate());
        Assert.assertTrue(predicates.size() == 3);
        Assert.assertTrue(scanOperatorPredicates.toString().equals("selectedPartitionIds=[1, 2, 3, 4, 5], " +
                "partitionConjuncts=[date_trunc(month, 16: l_shipdate) = 1998-01-01, 16: l_shipdate >= 1998-01-01], " +
                "noEvalPartitionConjuncts=[date_trunc(month, 16: l_shipdate) = 1998-01-01], " +
                "nonPartitionConjuncts=[1: l_orderkey > 1000], minMaxConjuncts=[1: l_orderkey > 1000]"));
    }

    @Test
    public void testHivePartitionPruneWithTwoTables1() {
        String mv1 = "CREATE MATERIALIZED VIEW mv1\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "PARTITION BY (l_shipdate)\n" +
                "PROPERTIES (\"force_external_table_query_rewrite\" = \"true\") AS \n" +
                " SELECT o_orderkey, l_suppkey, l_shipdate, sum(l_orderkey)  " +
                " FROM hive0.partitioned_db.lineitem_par as a " +
                " LEFT JOIN hive0.partitioned_db.orders as b ON b.o_orderkey=a.l_orderkey \n " +
                " WHERE b.o_orderdate >= '1991-01-01' and a.l_shipdate >= '1991-01-01' \n " +
                " GROUP BY o_orderkey, l_suppkey, l_shipdate;";

        starRocksAssert.withMaterializedView(mv1, (obj) -> {
            String mvName = (String) obj;
            refreshMaterializedView(DB_NAME, mvName);
            {
                String query = "SELECT o_orderkey, l_suppkey, l_shipdate, sum(l_orderkey)  " +
                        " FROM hive0.partitioned_db.lineitem_par as a " +
                        " LEFT JOIN hive0.partitioned_db.orders as b ON b.o_orderkey=a.l_orderkey \n " +
                        " WHERE b.o_orderdate >= '1991-01-01' and a.l_shipdate >= '1991-01-01' \n " +
                        " GROUP BY o_orderkey, l_suppkey, l_shipdate;";

                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=5/6\n" +
                        "     rollup: mv1");
            }

            {
                String query = "SELECT o_orderkey, l_suppkey, l_shipdate, sum(l_orderkey)  " +
                        " FROM hive0.partitioned_db.lineitem_par as a " +
                        " LEFT JOIN hive0.partitioned_db.orders as b ON b.o_orderkey=a.l_orderkey \n " +
                        " WHERE b.o_orderdate >= '1991-01-01' and a.l_suppkey > 1 and a.l_shipdate >= '1991-01-01' \n " +
                        " GROUP BY o_orderkey, l_suppkey, l_shipdate;";

                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 28: l_suppkey > 1\n" +
                        "     partitions=5/6\n" +
                        "     rollup: mv1");
            }
        });
    }

    @Test
    public void testHivePartitionPruneWithTwoTables2() {
        String mv1 = "CREATE MATERIALIZED VIEW mv1\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "PARTITION BY (l_shipdate)\n" +
                "PROPERTIES (\"force_external_table_query_rewrite\" = \"true\") AS \n" +
                " SELECT o_orderkey, l_suppkey, l_shipdate, sum(l_orderkey)  " +
                " FROM hive0.partitioned_db.lineitem_par as a " +
                " LEFT JOIN hive0.partitioned_db.orders as b ON b.o_orderkey=a.l_orderkey \n " +
                " WHERE b.o_orderdate is not null and a.l_shipdate is not null \n " +
                " GROUP BY o_orderkey, l_suppkey, l_shipdate;";

        starRocksAssert.withMaterializedView(mv1, (obj) -> {
            String mvName = (String) obj;
            refreshMaterializedView(DB_NAME, mvName);
            {
                String query = "SELECT o_orderkey, l_suppkey, l_shipdate, sum(l_orderkey)  " +
                        " FROM hive0.partitioned_db.lineitem_par as a " +
                        " LEFT JOIN hive0.partitioned_db.orders as b ON b.o_orderkey=a.l_orderkey \n " +
                        " WHERE b.o_orderdate is not null and a.l_shipdate is not null \n " +
                        " GROUP BY o_orderkey, l_suppkey, l_shipdate;";

                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=6/6\n" +
                        "     rollup: mv1");
            }

            {
                String query = "SELECT o_orderkey, l_suppkey, l_shipdate, sum(l_orderkey)  " +
                        " FROM hive0.partitioned_db.lineitem_par as a " +
                        " LEFT JOIN hive0.partitioned_db.orders as b ON b.o_orderkey=a.l_orderkey \n " +
                        " WHERE b.o_orderdate is not null and a.l_suppkey > 1 and a.l_shipdate is not null \n " +
                        " GROUP BY o_orderkey, l_suppkey, l_shipdate;";

                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 28: l_suppkey > 1\n" +
                        "     partitions=6/6\n" +
                        "     rollup: mv1");
            }
        });
    }

    @Test
    public void testHivePartitionPruneWithTwoTablesInnerJoin1() {
        String mv1 = "CREATE MATERIALIZED VIEW mv1\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "PARTITION BY (l_shipdate)\n" +
                "PROPERTIES (\"force_external_table_query_rewrite\" = \"true\") AS \n" +
                " SELECT o_orderkey, l_suppkey, l_shipdate, sum(l_orderkey)  " +
                " FROM hive0.partitioned_db.lineitem_par as a " +
                " INNER JOIN hive0.partitioned_db.orders as b ON b.o_orderkey=a.l_orderkey \n " +
                " WHERE b.o_orderdate >= '1991-01-01' and a.l_shipdate >= '1991-01-01' \n " +
                " GROUP BY o_orderkey, l_suppkey, l_shipdate;";

        starRocksAssert.withMaterializedView(mv1, (obj) -> {
            String mvName = (String) obj;
            refreshMaterializedView(DB_NAME, mvName);
            {
                String query = "SELECT o_orderkey, l_suppkey, l_shipdate, sum(l_orderkey)  " +
                        " FROM hive0.partitioned_db.lineitem_par as a " +
                        " INNER JOIN hive0.partitioned_db.orders as b ON b.o_orderkey=a.l_orderkey \n " +
                        " WHERE b.o_orderdate >= '1991-01-01' and a.l_shipdate >= '1991-01-01' \n " +
                        " GROUP BY o_orderkey, l_suppkey, l_shipdate;";

                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=5/6\n" +
                        "     rollup: mv1");
            }

            {
                String query = "SELECT o_orderkey, l_suppkey, l_shipdate, sum(l_orderkey)  " +
                        " FROM hive0.partitioned_db.lineitem_par as a " +
                        " INNER JOIN hive0.partitioned_db.orders as b ON b.o_orderkey=a.l_orderkey \n " +
                        " WHERE b.o_orderdate >= '1991-01-01' and a.l_suppkey > 1 and a.l_shipdate >= '1991-01-01' \n " +
                        " GROUP BY o_orderkey, l_suppkey, l_shipdate;";

                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 28: l_suppkey > 1\n" +
                        "     partitions=5/6\n" +
                        "     rollup: mv1");
            }
        });
    }

    @Test
    public void testHivePartitionPruneWithTwoTablesInnerJoin2() {
        String mv1 = "CREATE MATERIALIZED VIEW mv1\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "PARTITION BY (l_shipdate)\n" +
                "PROPERTIES (\"force_external_table_query_rewrite\" = \"true\") AS \n" +
                " SELECT o_orderkey, l_suppkey, l_shipdate, sum(l_orderkey)  " +
                " FROM hive0.partitioned_db.lineitem_par as a " +
                " INNER JOIN hive0.partitioned_db.orders as b ON b.o_orderkey=a.l_orderkey \n " +
                " WHERE b.o_orderdate is not null and a.l_shipdate is not null \n " +
                " GROUP BY o_orderkey, l_suppkey, l_shipdate;";

        starRocksAssert.withMaterializedView(mv1, (obj) -> {
            String mvName = (String) obj;
            refreshMaterializedView(DB_NAME, mvName);
            {
                String query = "SELECT o_orderkey, l_suppkey, l_shipdate, sum(l_orderkey)  " +
                        " FROM hive0.partitioned_db.lineitem_par as a " +
                        " INNER JOIN hive0.partitioned_db.orders as b ON b.o_orderkey=a.l_orderkey \n " +
                        " WHERE b.o_orderdate is not null and a.l_shipdate is not null \n " +
                        " GROUP BY o_orderkey, l_suppkey, l_shipdate;";

                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=6/6\n" +
                        "     rollup: mv1");
            }

            {
                String query = "SELECT o_orderkey, l_suppkey, l_shipdate, sum(l_orderkey)  " +
                        " FROM hive0.partitioned_db.lineitem_par as a " +
                        " INNER JOIN hive0.partitioned_db.orders as b ON b.o_orderkey=a.l_orderkey \n " +
                        " WHERE b.o_orderdate is not null and a.l_suppkey > 1 and a.l_shipdate is not null \n " +
                        " GROUP BY o_orderkey, l_suppkey, l_shipdate;";

                String plan = getFragmentPlan(query);
                PlanTestBase.assertContains(plan, "0:OlapScanNode\n" +
                        "     TABLE: mv1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 28: l_suppkey > 1\n" +
                        "     partitions=6/6\n" +
                        "     rollup: mv1");
            }
        });
    }
}
