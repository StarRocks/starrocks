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

public class MvTransparentRewriteWithOlapTableTest extends MvRewriteTestBase {
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
                            " PROPERTIES (\n" +
                            " 'enable_transparent_rewrite' = 'true'" +
                            " ) " +
                            " AS SELECT k1, k2, v1, v2 from m1;",
                    (obj) -> {
                        String mvName = (String) obj;
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", mvName);
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
                            " PROPERTIES (\n" +
                            " 'enable_transparent_rewrite' = 'true'" +
                            " ) " +
                            " AS SELECT k1, k2, sum(v1) as agg1, count(v2) as agg2 from m1 group by k1, k2;",
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
                            " PROPERTIES (\n" +
                            " 'enable_transparent_rewrite' = 'true'" +
                            " ) " +
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
            // no compensate rewrite
            {
                String[] sqls = {
                        "SELECT * from mv0 where k1=1",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query, "MV");
                    // TODO: How to prune partitions further.
                    PlanTestBase.assertContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                }
            }

            // transparent union rewrite
            {
                String[] sqls = {
                        "SELECT * from mv0 where k1<6 and k2 like 'a%'",
                        "SELECT * from mv0 where k1 != 3 and k2 like 'a%'",
                        "SELECT * from mv0 where k1 > 0 and k2 like 'a%'",
                        "SELECT * from mv0 where k1>1 and k2 like 'a%'",
                        "SELECT * from mv0 where k1>0 and k2 like 'a%'",
                };
                for (String query : sqls) {
                    System.out.println(query);
                    String plan = getFragmentPlan(query, "MV");
                    System.out.println(plan);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": m1");
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithScanMvAndAggQuery() {
        withPartialScanMv(() -> {
            {
                String[] sqls = {
                        "SELECT k1, k2 from mv0 where k1=1 ",
                        "SELECT k1, k2 from mv0 where k1<3 ",
                        "SELECT k1, k2 from mv0 where k1<=2 ",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query, "MV");
                    // TODO: How to prune partitions further.
                    PlanTestBase.assertContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                }
            }

            {
                String[] sqls = {
                        "SELECT k1, k2 from mv0 where k1<6 and k2 like 'a%' ",
                        "SELECT k1, k2 from mv0 where k1!=3 and k2 like 'a%' ",
                        "SELECT k1, k2 from mv0 where k1>0 and k2 like 'a%' ",
                        "SELECT k1, k2 from mv0 where k1>1 and k2 like 'a%' ",
                        "SELECT k1, k2 from mv0 where k1<6 and k2 like 'a%' ",
                        "SELECT k1, k2 from mv0 where k1+1<6 and k2 like 'a%' ",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query, "MV");
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": m1");
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithAggMv() {
        withPartialAggregateMv(() -> {
            {
                String[] sqls = {
                        "SELECT * from mv0 where k1=1 ",
                        "SELECT * from mv0 where k1<3 ",
                        "SELECT * from mv0 where k1<=2",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query, "MV");
                    // TODO: How to prune partitions further.
                    PlanTestBase.assertContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                }
            }

            {
                String[] sqls = {
                        "SELECT * from mv0 where k1<6 and k2 like 'a%' ",
                        "SELECT k1, k2 from mv0 where k1!=3 and k2 like 'a%' ",
                        "SELECT k1, k2 from mv0 where k1>0 and k2 like 'a%' ",
                        "SELECT k1, k2 from mv0 where k1>1 and k2 like 'a%' ",
                        "SELECT k1, k2 from mv0 where k1<6 and k2 like 'a%' ",
                        "SELECT k1, k2 from mv0 where k1+1<6 and k2 like 'a%' ",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query, "MV");
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": m1");
                }
            }
        });
    }

    @Test
    public void testTransparentRewriteWithJoinMv() {
        withPartialJoinMv(() -> {
            {
                String[] sqls = {
                        "SELECT * from mv0 where k1=1",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query, "MV");
                    // TODO: How to prune partitions further.
                    PlanTestBase.assertContains(plan, ":UNION");
                    PlanTestBase.assertContains(plan, "mv0");
                }
            }

            {
                String[] sqls = {
                        "SELECT * from mv0 where k1 >= 1",
                        "SELECT * from mv0 where k1 >= 1 and k2 like 'a% ",
                        "SELECT * from mv0 where k1 <= 6 and k2 like 'a% ",
                        "SELECT * from mv0 where k1 + 1>= 1 and k2 like 'a% ",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query, "MV");
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": m1");
                }
            }
        });
    }
}
