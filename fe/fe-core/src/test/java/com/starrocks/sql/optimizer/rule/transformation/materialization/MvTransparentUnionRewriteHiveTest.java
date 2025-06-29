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
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MvTransparentUnionRewriteHiveTest extends MVTestBase {
    private static MockedHiveMetadata mockedHiveMetadata;

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        connectContext.getSessionVariable().setEnableMaterializedViewTransparentUnionRewrite(true);
    }

    private void withPartialScanMv(StarRocksAssert.ExceptionRunnable runner) {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 6\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "AS SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par as a;",
                (obj) -> {
                    String mvName = (String) obj;
                    refreshMaterializedViewWithPartition(DB_NAME, mvName, "1998-01-01", "1998-01-05");
                    mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                            ImmutableList.of("l_shipdate=1998-01-02"));
                    runner.run();
                });
    }

    private void withPartialAggregateMv(StarRocksAssert.ExceptionRunnable runner) {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 6\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "AS SELECT l_shipdate, l_orderkey, sum(l_suppkey) FROM " +
                        " hive0.partitioned_db.lineitem_par as a GROUP BY l_orderkey, l_shipdate;",
                (obj) -> {
                    String mvName = (String) obj;
                    refreshMaterializedViewWithPartition(DB_NAME, mvName, "1998-01-01", "1998-01-05");
                    mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                            ImmutableList.of("l_shipdate=1998-01-02"));
                    runner.run();
                });
    }

    private void withPartialJoinMv(StarRocksAssert.ExceptionRunnable runner) {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 3\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"" +
                        ")\n" +
                        "AS SELECT a.l_orderkey, a.l_suppkey, a.l_shipdate, b.o_orderkey, b.o_custkey FROM " +
                        "   hive0.partitioned_db.lineitem_par as a JOIN hive0.partitioned_db.orders b " +
                        " ON a.l_orderkey = b.o_orderkey and a.l_shipdate=b.o_orderdate;",
                (obj) -> {
                    String mvName = (String) obj;
                    refreshMaterializedViewWithPartition(DB_NAME, mvName, "1998-01-01", "1998-01-05");
                    mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                            ImmutableList.of("l_shipdate=1998-01-02"));
                    runner.run();
                });
    }

    @Test
    public void testTransparentRewriteWithScanMv() {
        withPartialScanMv(() -> {
            // no compensate rewrite
            {
                String[] sqls = {
                        "SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par as a WHERE" +
                                " l_shipdate='1998-01-01';",
                };
                String[] expects = {
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     partitions=1/4\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=6/6",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                    PlanTestBase.assertContains(plan, expects[i]);
                }
            }

            // no rewrite
            {
                String[] sqls = {
                        "SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par as a WHERE" +
                                " l_shipdate = '1998-01-02';",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, ":UNION", ": mv0");
                }
            }

            // transparent union rewrite
            {
                String[] sqls = {
                        "SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par as a WHERE" +
                                " l_shipdate >= '1998-01-02';",
                        "SELECT l_orderkey, date_trunc('month', l_shipdate) FROM hive0.partitioned_db.lineitem_par as" +
                                " a WHERE l_shipdate != '1998-01-01';",
                        "SELECT l_orderkey * 2, l_shipdate FROM hive0.partitioned_db.lineitem_par as a WHERE" +
                                " l_shipdate >= '1998-01-02' and l_suppkey > 1;",
                };
                String[] expects = {
                        "     TABLE: lineitem_par\n" +
                                "     PARTITION PREDICATES: 25: l_shipdate IN ('1998-01-02', '1998-01-05')\n" +
                                "     partitions=2/6",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     partitions=2/4\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=12/12", // case 1
                        "     TABLE: lineitem_par\n" +
                                "     PARTITION PREDICATES: 26: l_shipdate != '1998-01-01', " +
                                "26: l_shipdate IN ('1998-01-02', '1998-01-05')\n" +
                                "     partitions=2/6",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 23: l_shipdate != '1998-01-01'\n" +
                                "     partitions=3/4", // case 2
                        "     TABLE: lineitem_par\n" +
                                "     PARTITION PREDICATES: 26: l_shipdate >= '1998-01-02', " +
                                "26: l_shipdate IN ('1998-01-02', '1998-01-05')\n" +
                                "     NON-PARTITION PREDICATES: 25: l_suppkey > 1\n" +
                                "     MIN/MAX PREDICATES: 25: l_suppkey > 1\n" +
                                "     partitions=2/6",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 22: l_suppkey > 1\n" +
                                "     partitions=2/4\n" +
                                "     rollup: mv0", // case 3
                };
                for (int i = 0; i < sqls.length; i++) {
                    System.out.println("start to test case " + i);
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": lineitem_par");
                    PlanTestBase.assertContains(plan, expects[i * 2]);
                    PlanTestBase.assertContains(plan, expects[i * 2 + 1]);
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithScanMvNoPrune() {
        withPartialScanMv(() -> {
            // no compensate rewrite
            {
                String[] sqls = {
                        "SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par as a WHERE" +
                                " date_trunc('month', l_shipdate) ='1998-01-01';",
                };
                String[] expects = {
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: date_trunc('month', 22: l_shipdate) = '1998-01-01'\n" +
                                "     partitions=3/4\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=18/18",
                        "     TABLE: lineitem_par\n" +
                                "     PARTITION PREDICATES: date_trunc('month', 25: l_shipdate) = '1998-01-01', " +
                                "25: l_shipdate IN (NULL, '1998-01-02', '1998-01-05')\n" +
                                "     NO EVAL-PARTITION PREDICATES: date_trunc('month', 25: l_shipdate) = '1998-01-01'\n" +
                                "     partitions=2/6"
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                    PlanTestBase.assertContains(plan, expects[2 * i]);
                    PlanTestBase.assertContains(plan, expects[2 * i + 1]);
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithScanMvAndAggQuery() {
        withPartialScanMv(() -> {
            {
                String[] sqls = {
                        "SELECT l_shipdate, l_orderkey, sum(l_suppkey) FROM hive0.partitioned_db" +
                                ".lineitem_par as a WHERE l_shipdate='1998-01-01' GROUP BY l_orderkey, l_shipdate;",
                        "SELECT l_shipdate, sum(l_suppkey) FROM hive0.partitioned_db" +
                                ".lineitem_par as a WHERE l_shipdate='1998-01-01' and l_orderkey != 1 " +
                                " GROUP BY l_orderkey, l_shipdate;",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                }
            }

            {
                String[] sqls = {
                        "SELECT l_shipdate, l_orderkey, sum(l_suppkey) FROM hive0.partitioned_db" +
                                ".lineitem_par as a WHERE l_shipdate >= '1998-01-01' GROUP BY l_orderkey, l_shipdate;",
                        "SELECT l_shipdate, sum(l_suppkey) FROM hive0.partitioned_db" +
                                ".lineitem_par as a WHERE l_shipdate !='1998-01-01' and l_orderkey != 1 " +
                                " GROUP BY l_orderkey, l_shipdate;",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": lineitem_par");
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithAggMv() {
        withPartialAggregateMv(() -> {
            {
                String[] sqls = {
                        "SELECT l_shipdate, l_orderkey, sum(l_suppkey) FROM hive0.partitioned_db" +
                                ".lineitem_par as a WHERE l_shipdate='1998-01-01' GROUP BY l_orderkey, l_shipdate;",
                        "SELECT l_shipdate, sum(l_suppkey) FROM hive0.partitioned_db" +
                                ".lineitem_par as a WHERE l_shipdate='1998-01-01' and l_orderkey != 1 " +
                                " GROUP BY l_orderkey, l_shipdate;",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                }
            }

            {
                String[] sqls = {
                        "SELECT l_shipdate, l_orderkey, sum(l_suppkey) FROM hive0.partitioned_db" +
                                ".lineitem_par as a WHERE l_shipdate >= '1998-01-01' GROUP BY l_orderkey, l_shipdate;",
                        "SELECT l_shipdate, sum(l_suppkey) FROM hive0.partitioned_db" +
                                ".lineitem_par as a WHERE l_shipdate !='1998-01-01' and l_orderkey != 1 " +
                                " GROUP BY l_orderkey, l_shipdate;",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": lineitem_par");
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithJoinMv1() {
        withPartialJoinMv(() -> {
            String[] sqls = {
                    "SELECT a.l_orderkey, a.l_suppkey, a.l_shipdate, b.o_orderkey, b.o_custkey FROM " +
                            " hive0.partitioned_db.lineitem_par as a JOIN hive0.partitioned_db.orders b " +
                            " ON a.l_orderkey = b.o_orderkey and a.l_shipdate=b.o_orderdate " +
                            "WHERE a.l_shipdate='1998-01-01';",
                    "SELECT a.l_orderkey, a.l_suppkey, a.l_shipdate, b.o_orderkey, b.o_custkey FROM " +
                            " hive0.partitioned_db.lineitem_par as a JOIN hive0.partitioned_db.orders b " +
                            " ON a.l_orderkey = b.o_orderkey and a.l_shipdate=b.o_orderdate " +
                            "WHERE a.l_shipdate='1998-01-01' and a.l_suppkey > 100;",
            };
            for (String query : sqls) {
                String plan = getFragmentPlan(query);
                PlanTestBase.assertNotContains(plan, ":UNION");
                PlanTestBase.assertContains(plan, "mv0");
            }
        });
    }

    @Test
    public void testTransparentRewriteWithJoinMv2() {
        withPartialJoinMv(() -> {
            {
                String[] sqls = {
                        "SELECT a.l_orderkey, a.l_suppkey, a.l_shipdate, b.o_orderkey, b.o_custkey FROM " +
                                " hive0.partitioned_db.lineitem_par as a JOIN hive0.partitioned_db.orders b " +
                                " ON a.l_orderkey = b.o_orderkey and a.l_shipdate=b.o_orderdate " +
                                "WHERE a.l_shipdate >= '1998-01-01';",
                };
                // lineitem and order have no intersected dates.
                for (String query : sqls) {
                    System.out.println(query);
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": lineitem_par");
                }
            }

            {
                String[] sqls = {
                        "SELECT a.l_orderkey, a.l_suppkey, a.l_shipdate, b.o_orderkey, b.o_custkey FROM " +
                                " hive0.partitioned_db.lineitem_par as a JOIN hive0.partitioned_db.orders b " +
                                " ON a.l_orderkey = b.o_orderkey and a.l_shipdate=b.o_orderdate " +
                                "WHERE a.l_shipdate != '1998-01-01' and a.l_suppkey > 100;",
                        "SELECT a.l_orderkey, a.l_suppkey, a.l_shipdate, b.o_orderkey, b.o_custkey FROM " +
                                " hive0.partitioned_db.lineitem_par as a JOIN hive0.partitioned_db.orders b " +
                                " ON a.l_orderkey = b.o_orderkey and a.l_shipdate=b.o_orderdate " +
                                "WHERE a.l_shipdate <= '1998-01-05' and a.l_suppkey > 100;",
                };
                // lineitem and order have no intersected dates.
                for (String query : sqls) {
                    System.out.println(query);
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ": lineitem_par");
                    PlanTestBase.assertNotContains(plan, ":UNION", ": mv0");
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithPartitionPrune() {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 6\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "AS SELECT l_orderkey, l_suppkey, l_shipdate FROM hive0.partitioned_db.lineitem_par as a" +
                        " where l_shipdate >= '1998-01-02';",
                (obj) -> {
                    String mvName = (String) obj;
                    refreshMaterializedViewWithPartition(DB_NAME, mvName, "1998-01-01", "1998-01-05");
                    mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                            ImmutableList.of("l_shipdate=1998-01-02"));

                    connectContext.getSessionVariable()
                            .setMaterializedViewUnionRewriteMode(MVUnionRewriteMode.PULL_PREDICATE_V2.getOrdinal());
                    // transparent union rewrite
                    {
                        // TODO: support more cases
                        // "SELECT l_orderkey, l_shipdate, 2 * l_suppkey FROM hive0.partitioned_db.lineitem_par as a " +
                        // "WHERE l_shipdate >= '1998-01-01' and l_suppkey > 1;",
                        String[] sqls = {
                                "SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par as a WHERE" +
                                        " l_shipdate >= '1998-01-02';",
                                "SELECT l_orderkey, l_shipdate, l_suppkey FROM hive0.partitioned_db.lineitem_par as a WHERE" +
                                        " l_shipdate >= '1998-01-01' and l_suppkey > 1;",
                        };
                        String[] expects = {
                                "     TABLE: lineitem_par\n" +
                                        "     PARTITION PREDICATES: 25: l_shipdate >= '1998-01-02', " +
                                        "25: l_shipdate IN ('1998-01-02', '1998-01-05')\n" +
                                        "     partitions=2/6",
                                "     TABLE: mv0\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     partitions=3/4", // case 1
                                "     TABLE: lineitem_par\n" +
                                        "     PARTITION PREDICATES: 41: l_shipdate >= '1998-01-01', " +
                                        "(41: l_shipdate < '1998-01-02') OR (41: l_shipdate IS NULL)\n" +
                                        "     NON-PARTITION PREDICATES: 40: l_suppkey > 1\n" +
                                        "     MIN/MAX PREDICATES: 40: l_suppkey > 1\n" +
                                        "     partitions=1/6",
                                "     TABLE: mv0\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     PREDICATES: 21: l_suppkey > 1\n" +
                                        "     partitions=3/4\n" +
                                        "     rollup: mv0", // case 3
                        };
                        for (int i = 0; i < sqls.length; i++) {
                            System.out.println("start to test case " + i);
                            String query = sqls[i];
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": lineitem_par");
                            PlanTestBase.assertContains(plan, expects[i * 2]);
                            PlanTestBase.assertContains(plan, expects[i * 2 + 1]);
                        }
                    }
                });
    }
}