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

public class MvTransparentRewriteWithHiveTableTest extends MVTestBase {
    private static MockedHiveMetadata mockedHiveMetadata;

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
    }

    private void withPartialScanMv(StarRocksAssert.ExceptionRunnable runner) {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        " PROPERTIES (\n" +
                        " 'transparent_mv_rewrite_mode' = 'true'" +
                        " ) " +
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
                        "DISTRIBUTED BY RANDOM\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        " PROPERTIES (\n" +
                        " 'transparent_mv_rewrite_mode' = 'true'" +
                        " ) " +
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
                        "DISTRIBUTED BY RANDOM\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        " PROPERTIES (\n" +
                        " 'transparent_mv_rewrite_mode' = 'true'" +
                        " ) " +
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
                        "SELECT * FROM mv0 WHERE l_shipdate='1998-01-01';",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                }
            }

            // transparent union rewrite
            {
                String[] sqls = {
                        "SELECT * FROM mv0 WHERE l_shipdate='1998-01-02';",
                        "SELECT * FROM mv0 WHERE l_shipdate>='1998-01-02';",
                        "SELECT * FROM mv0 WHERE l_shipdate!='1998-01-02';",
                        "SELECT l_orderkey, date_trunc('month', l_shipdate) FROM mv0 WHERE l_shipdate!='1998-01-02' " +
                                "and l_suppkey>1;",
                        "SELECT l_orderkey, date_trunc('month', l_shipdate) FROM mv0 WHERE l_shipdate!='1998-01-02' and " +
                                "l_suppkey + 2 >1000;",
                };
                for (String query : sqls) {
                    System.out.println(query);
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": lineitem_par");
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithScanMvAndAggQuery() {
        withPartialScanMv(() -> {
            {
                String[] sqls = {
                        "SELECT l_shipdate, l_orderkey, sum(l_suppkey) FROM mv0 as a WHERE l_shipdate='1998-01-01' GROUP BY " +
                                "l_orderkey, l_shipdate;",
                        "SELECT l_shipdate, sum(l_suppkey) FROM mv0 WHERE l_shipdate='1998-01-01' and l_orderkey != 1 " +
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
                        "SELECT l_shipdate, l_orderkey, sum(l_suppkey) FROM mv0 WHERE l_shipdate >= '1998-01-01' GROUP BY " +
                                "l_orderkey, l_shipdate;",
                        "SELECT l_shipdate, sum(l_suppkey) FROM mv0 WHERE l_shipdate !='1998-01-01' and l_orderkey != 1 " +
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
                        "SELECT l_shipdate, l_orderkey FROM mv0 WHERE l_shipdate='1998-01-01'",
                        "SELECT l_shipdate FROM mv0 WHERE l_shipdate='1998-01-01' and l_orderkey != 1",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                }
            }

            {
                String[] sqls = {
                        "SELECT l_shipdate, l_orderkey FROM mv0 WHERE l_shipdate >= '1998-01-01';",
                        "SELECT * FROM mv0 WHERE l_shipdate !='1998-01-01' and l_orderkey != 1",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": lineitem_par");
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithJoinMv() {
        withPartialJoinMv(() -> {
            {
                String[] sqls = {
                        "SELECT * FROM mv0 WHERE l_shipdate='1998-01-01';",
                        "SELECT * FROM mv0 WHERE l_shipdate='1998-01-01' and l_suppkey > 100;",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                }
            }

            {
                String[] sqls = {
                        "SELECT * FROM mv0 WHERE l_shipdate != '1998-01-01' and l_suppkey > 100;",
                        "SELECT * FROM mv0 WHERE l_shipdate >= '1998-01-01' and l_suppkey > 100;",
                        "SELECT * FROM mv0 WHERE l_shipdate <= '1998-01-05' and l_suppkey > 100;",
                };
                for (String query : sqls) {
                    System.out.println(query);
                    String plan = getFragmentPlan(query);
                    // transparent plan will contain union, but it can be pruned
                    PlanTestBase.assertNotContains(plan, "UNION");
                    PlanTestBase.assertContains(plan, ": mv0");
                }
            }
        });
    }
}
