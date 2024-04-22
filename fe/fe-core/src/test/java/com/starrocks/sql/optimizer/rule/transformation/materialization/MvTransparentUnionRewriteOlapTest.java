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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.schema.MTable;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.util.Set;

public class MvTransparentUnionRewriteOlapTest extends MvRewriteTestBase {
    private static MTable m1;
    private static MTable m2;

    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();

        m1 = new MTable("m1", "k1",
                ImmutableList.of(
                        "k1 INT",
                        "k2 string",
                        "v1 INT",
                        "v2 INT"
                ),
                "k1",
                ImmutableList.of(
                        "PARTITION `p1` VALUES LESS THAN ('3')",
                        "PARTITION `p2` VALUES LESS THAN ('6')",
                        "PARTITION `p3` VALUES LESS THAN ('9')"
                )
        );
        m2 = new MTable("m2", "k1",
                ImmutableList.of(
                        "k1 INT",
                        "k2 string",
                        "v1 INT",
                        "v2 INT"
                ),
                "k1",
                ImmutableList.of(
                        "PARTITION `p1` VALUES LESS THAN ('3')",
                        "PARTITION `p2` VALUES LESS THAN ('6')",
                        "PARTITION `p3` VALUES LESS THAN ('9')"
                )
        );
        connectContext.getSessionVariable().setMaterializedViewUnionRewriteMode(MVUnionRewriteMode.TRANSPARENT.getOrdinal());
    }

    @Before
    public void before() {
        startCaseTime = Instant.now().getEpochSecond();
    }

    @After
    public void after() throws Exception {
        PlanTestBase.cleanupEphemeralMVs(starRocksAssert, startCaseTime);
    }

    private void withPartialScanMv(StarRocksAssert.ExceptionRunnable runner) {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1), (4,2,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                    " PARTITION BY (k1) " +
                    " DISTRIBUTED BY HASH(k1) " +
                    " REFRESH DEFERRED MANUAL " +
                    " AS SELECT k1, k2, v1, v2 from m1;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        runner.run();
                    });
        });
    }

    private void withPartialAggregateMv(StarRocksAssert.ExceptionRunnable runner) {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1), (4,2,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                    " PARTITION BY (k1) " +
                    " DISTRIBUTED BY HASH(k1) " +
                    " REFRESH DEFERRED MANUAL " +
                    " AS SELECT k1, k2, sum(v1), count(v2) from m1 group by k1, k2;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());

                        runner.run();
                    });
        });
    }

    private void withPartialJoinMv(StarRocksAssert.ExceptionRunnable runner) {
        starRocksAssert.withTables(ImmutableList.of(m1, m2), () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1), (4,2,1,1);");
            cluster.runSql("test", "insert into m2 values (1,1,1,1), (4,2,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " AS SELECT m1.k1, m1.k2, sum(m1.v1), sum(m2.v2) " +
                            "from m1 join m2 on m1.k1=m2.k1 group by m1.k1, m1.k2;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());

                        runner.run();
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithScanMv() {
        withPartialScanMv(() -> {
            // compensate rewrite: no compensation
            {
                String[] sqls = {
                        "SELECT k1, k2, v1, v2 from m1 where k1=1",
                        "SELECT k1, k2, v1, v2 from m1 where k1<3",
                        "SELECT k1, k2, v1, v2 from m1 where k1<2",
                };
                String[] expectPlans = {
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 5: k1 = 1\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=1/3",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 5: k1 < 3\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=3/3",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 5: k1 < 2\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=3/3",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                    PlanTestBase.assertContains(plan, expectPlans[i]);
                }
            }

            // no rewrite
            {
                String[] sqls = {
                        "SELECT k1, k2, v1, v2 from m1 where k1=6 and k2 like 'a%'",
                        "SELECT k1, k2, v1, v2 from m1 where k1=5  and k2 like 'a%'",
                        "SELECT k1, k2, v1, v2 from m1 where k1 > 3 and k1 < 6 and k2 like 'a%'",
                        "SELECT k1, k2, v1, v2 from m1 where k1>6 and k2 like 'a%'",
                };
                String[] expectPlans = {
                        "     TABLE: m1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 1: k1 = 6, 2: k2 LIKE 'a%'\n" +
                                "     partitions=1/3\n" +
                                "     rollup: m1\n" +
                                "     tabletRatio=1/3",
                        "     TABLE: m1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 1: k1 = 5, 2: k2 LIKE 'a%'\n" +
                                "     partitions=1/3\n" +
                                "     rollup: m1\n" +
                                "     tabletRatio=1/3",
                        "     TABLE: m1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 1: k1 > 3, 2: k2 LIKE 'a%'\n" +
                                "     partitions=1/3\n" +
                                "     rollup: m1\n" +
                                "     tabletRatio=3/3",
                        "     TABLE: m1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 1: k1 > 6, 2: k2 LIKE 'a%'\n" +
                                "     partitions=1/3\n" +
                                "     rollup: m1\n" +
                                "     tabletRatio=3/3",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, "mv0");
                    PlanTestBase.assertContains(plan, expectPlans[i]);
                }
            }

            // transparent union rewrite: pruned compensation
            {
                String[] sqls = {
                        "SELECT k1, k2, v1, v2 from m1 where k1<6 and k2 like 'a%'",
                        "SELECT k1, k2, v1, v2 from m1 where k1 > 0 and k1 < 6 and k2 like 'a%'",
                        "SELECT k1, k2, v1, v2 from m1 where k1>1 and k1 < 6 and k2 like 'a%'",
                };
                String[] expectPlans = {
                        "     TABLE: m1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 13: k1 < 6, 14: k2 LIKE 'a%'\n" +
                                "     partitions=1/3\n" +
                                "     rollup: m1\n" +
                                "     tabletRatio=3/3",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 9: k1 < 6, 10: k2 LIKE 'a%'\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=3/3",
                        "     TABLE: m1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 13: k1 > 0, 13: k1 < 6, 14: k2 LIKE 'a%'\n" +
                                "     partitions=1/3\n" +
                                "     rollup: m1\n" +
                                "     tabletRatio=3/3",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 9: k1 > 0, 9: k1 < 6, 10: k2 LIKE 'a%'\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=3/3",
                        "     TABLE: m1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 13: k1 > 1, 13: k1 < 6, 14: k2 LIKE 'a%'\n" +
                                "     partitions=1/3\n" +
                                "     rollup: m1\n" +
                                "     tabletRatio=3/3",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 9: k1 > 1, 9: k1 < 6, 10: k2 LIKE 'a%'\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=3/3",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": m1");
                    String expect1 = expectPlans[i * 2];
                    String expect2 = expectPlans[i * 2 + 1];
                    PlanTestBase.assertContains(plan, expect1);
                    PlanTestBase.assertContains(plan, expect2);
                }
            }


            // transparent union rewrite: no pruned compensation
            {
                String[] sqls = {
                        "SELECT k1, k2, v1, v2 from m1 where k1 != 3 and k2 like 'a%'",
                        "SELECT k1, k2, v1, v2 from m1 where k1 > 0 and k2 like 'a%'",
                        "SELECT k1, k2, v1, v2 from m1 where k1>1 and k2 like 'a%'",
                        "SELECT k1, k2, v1, v2 from m1 where k1>0 and k2 like 'a%'",
                };
                String[] expectPlans = {
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 9: k1 != 3, 10: k2 LIKE 'a%'\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=3/3",
                        "     TABLE: m1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 13: k1 != 3, 14: k2 LIKE 'a%'\n" +
                                "     partitions=2/3\n" +
                                "     rollup: m1\n" +
                                "     tabletRatio=6/6",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 9: k1 > 0, 10: k2 LIKE 'a%'\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=3/3",
                        "     TABLE: m1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 13: k1 > 0, 14: k2 LIKE 'a%'\n" +
                                "     partitions=2/3\n" +
                                "     rollup: m1\n" +
                                "     tabletRatio=6/6",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 9: k1 > 1, 10: k2 LIKE 'a%'\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=3/3",
                        "     TABLE: m1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 13: k1 > 1, 14: k2 LIKE 'a%'\n" +
                                "     partitions=2/3\n" +
                                "     rollup: m1\n" +
                                "     tabletRatio=6/6",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 9: k1 > 0, 10: k2 LIKE 'a%'\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=3/3",
                        "     TABLE: m1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 13: k1 > 0, 14: k2 LIKE 'a%'\n" +
                                "     partitions=2/3\n" +
                                "     rollup: m1\n" +
                                "     tabletRatio=6/6",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": m1");
                    String expect1 = expectPlans[i * 2];
                    String expect2 = expectPlans[i * 2 + 1];
                    PlanTestBase.assertContains(plan, expect1);
                    PlanTestBase.assertContains(plan, expect2);
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithScanMvAndAggQuery() {
        withPartialScanMv(() -> {
            // no compensation
            {
                String[] sqls = {
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1=1 group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1<3 group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1<=2 group by k1, k2",
                };
                String[] expects = {
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 7: k1 = 1\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=1/3",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 7: k1 < 3\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=3/3",
                        "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 7: k1 <= 2\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv0\n" +
                                "     tabletRatio=3/3"
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                }
            }

            {
                String[] sqls = {
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1<6 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1!=3 and k1 < 6 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1>1 and k1< 6 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1<6 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1+1<6 and k2 like 'a%' group by k1, k2",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": m1");
                }
            }

            // no pruned compensation
            {
                String[] sqls = {
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1!=3 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1>0 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1>1 and k2 like 'a%' group by k1, k2",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ": mv0");
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithAggMv() {
        withPartialAggregateMv(() -> {
            // no compensation
            {
                String[] sqls = {
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1=1 group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1<3 group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1<=2 group by k1, k2",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                }
            }

            // pruned compensation
            {
                String[] sqls = {
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1<6 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1!=3 and k1 < 6 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1>0 and k1 < 6 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1>1 and k1 < 6 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1<6 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1+1<6 and k2 like 'a%' group by k1, k2",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": m1");
                }
            }

            // no pruned compensation
            {
                String[] sqls = {
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1!=3 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1>0 and k2 like 'a%' group by k1, k2",
                        "SELECT k1, k2, sum(v1), count(v2) from m1 where k1>1 and k2 like 'a%' group by k1, k2",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0");
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithJoinMv() {
        withPartialJoinMv(() -> {
            {
                String[] sqls = {
                        "SELECT m1.k1, m1.k2, sum(m1.v1), sum(m2.v2) from m1 join m2 on m1.k1=m2.k1 where m1.k1=1 " +
                                "group by m1.k1, m1.k2;",
                        "SELECT m1.k1, m1.k2, sum(m1.v1), sum(m2.v2) from m1 join m2 on m1.k1=m2.k1 where m1.k1<3 " +
                                "group by m1.k1, m1.k2;",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                }
            }

            // pruned compensation
            {
                String[] sqls = {
                        "SELECT m1.k1, m1.k2, sum(m1.v1), sum(m2.v2) from m1 join m2 on m1.k1=m2.k1 where m1.k1 > 1 and " +
                                "m1.k1 < 6 group by m1.k1, m1.k2;",
                        "SELECT m1.k1, m1.k2, sum(m1.v1), sum(m2.v2) from m1 join m2 on m1.k1=m2.k1 " +
                                "where m1.k1 != 1 and m1.k1 < 6 and m1.k2 like 'a%' group by m1.k1, m1.k2;",
                        "SELECT m1.k1, m1.k2, sum(m1.v1), sum(m2.v2) from m1 join m2 on m1.k1=m2.k1 " +
                                "where m1.k1 < 6 and m1.k2 like 'a%' group by m1.k1, m1.k2;",
                        "SELECT m1.k1, m1.k2, sum(m1.v1), sum(m2.v2) from m1 join m2 on m1.k1=m2.k1 " +
                                "where m1.k1 + 1 < 6 and m1.k2 like 'a%' group by m1.k1, m1.k2;",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": m1");
                }
            }

            // no pruned compensation
            {
                String[] sqls = {
                        "SELECT m1.k1, m1.k2, sum(m1.v1), sum(m2.v2) from m1 join m2 on m1.k1=m2.k1 where m1.k1 > 1 " +
                                "group by m1.k1, m1.k2;",
                        "SELECT m1.k1, m1.k2, sum(m1.v1), sum(m2.v2) from m1 join m2 on m1.k1=m2.k1 " +
                                "where m1.k1 != 1 and m1.k2 like 'a%' group by m1.k1, m1.k2;",
                };
                for (int i = 0; i < sqls.length; i++) {
                    String query = sqls[i];
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": m1");
                }
            }
        });
    }
}
