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
import com.starrocks.schema.MTable;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;

public class MvTransparentRewriteWithOlapTableTest extends MVTestBase {
    private static MTable m1;
    private static MTable m2;
    private static MTable m3;
    private static String t1;
    private static String t2;
    private static String t3;

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();

        m1 = new MTable("m1", "k1",
                ImmutableList.of(
                        "k1 INT",
                        "k2 string",
                        "v1 INT",
                        "v2 INT",
                        "v3 string"
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
                        "v2 INT",
                        "v3 string"
                ),
                "k1",
                ImmutableList.of(
                        "PARTITION `p1` VALUES LESS THAN ('3')",
                        "PARTITION `p2` VALUES LESS THAN ('6')",
                        "PARTITION `p3` VALUES LESS THAN ('9')"
                )
        );
        m3 = new MTable("m3", "k1",
                ImmutableList.of(
                        "k1 INT",
                        "k2 string",
                        "v1 INT",
                        "v2 TINYINT",
                        "v3 char(20)",
                        "v4 varchar(20)"
                ),
                "k1",
                ImmutableList.of(
                        "PARTITION `p1` VALUES LESS THAN ('3')",
                        "PARTITION `p2` VALUES LESS THAN ('6')",
                        "PARTITION `p3` VALUES LESS THAN ('9')"
                )
        );

        // table whose partitions have multiple values
        t1 = "CREATE TABLE t1 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province) (\n" +
                "     PARTITION p1 VALUES IN (\"beijing\",\"chongqing\") ,\n" +
                "     PARTITION p2 VALUES IN (\"guangdong\") \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n";

        // table whose partitions have only single values
        t2 = "CREATE TABLE t2 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province) (\n" +
                "     PARTITION p1 VALUES IN (\"beijing\") ,\n" +
                "     PARTITION p2 VALUES IN (\"guangdong\") \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n";
        // table whose partitions have multi columns
        t3 = "CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province, dt) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\")),\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\")),\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-01-02\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n";
    }

    private void withPartialScanMv(StarRocksAssert.ExceptionRunnable runner) {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
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
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
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
        starRocksAssert.withMTables(ImmutableList.of(m1, m2), () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            cluster.runSql("test", "insert into m2 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
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
                    String plan = getFragmentPlan(query);
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
                    String plan = getFragmentPlan(query);
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
                    String plan = getFragmentPlan(query);
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
                    String plan = getFragmentPlan(query);
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
                    String plan = getFragmentPlan(query);
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
                    String plan = getFragmentPlan(query);
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
                    String plan = getFragmentPlan(query);
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
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": m1");
                }
            }
        });
    }

    @Test
    public void testJoin2() {
        starRocksAssert.withMTables(ImmutableList.of(m1, m2), () -> {
            // only update the non ref table, transparent plan should also works too
            cluster.runSql("test", "insert into m2 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT m1.k1, m1.k2, sum(m1.v1), sum(m2.v2) " +
                            "from m1 join m2 on m1.k1=m2.k1 group by m1.k1, m1.k2;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());

                        {
                            String[] sqls = {
                                    "SELECT * from mv0 where k1=1"
                            };
                            for (String query : sqls) {
                                String plan = getFragmentPlan(query);
                                PlanTestBase.assertContains(plan, ":UNION", "mv0");
                                // all data can be queried from mv0
                                PlanTestBase.assertContains(plan, "     TABLE: m1\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     PREDICATES: 13: k1 = 1, 13: k1 IS NOT NULL\n" +
                                        "     partitions=0/3");
                                PlanTestBase.assertContains(plan, "     TABLE: mv0\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     PREDICATES: 9: k1 = 1\n" +
                                        "     partitions=1/1\n" +
                                        "     rollup: mv0\n" +
                                        "     tabletRatio=1/3");
                            }
                        }
                        {
                            String[] sqls = {
                                    "SELECT * from mv0 where k1 !=1 or k2 < 10",
                                    "SELECT * from mv0 where k1 !=1 or k2 < 10 union select * from mv0 where k1 = 1",
                            };
                            for (String query : sqls) {
                                String plan = getFragmentPlan(query);
                                PlanTestBase.assertContains(plan, ":UNION", "mv0");
                            }
                        }
                    });
        });
    }

    private void withPartialSetOperator(StarRocksAssert.ExceptionRunnable runner,
                                        String setOperator) {
        starRocksAssert.withMTables(ImmutableList.of(m1, m2), () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1);");
            cluster.runSql("test", "insert into m2 values (4,2,1,1,1);");
            starRocksAssert.withMaterializedView((String.format("CREATE MATERIALIZED VIEW mv0 " +
                                    " PARTITION BY (k1) " +
                                    " DISTRIBUTED BY HASH(k1) " +
                                    " REFRESH DEFERRED MANUAL " +
                                    " PROPERTIES (\n" +
                                    " 'transparent_mv_rewrite_mode' = 'true'" +
                                    " ) AS" +
                                    " select * from (SELECT m1.k1, m1.k2, sum(m1.v1), sum(m2.v2) from m1 join m2 on m1.k1=m2.k1" +
                                    "   group by m1.k1, m1.k2" +
                                    " %s " +
                                    " SELECT m1.k1, m1.k2, sum(m1.v1), sum(m2.v2) from m1 join m2 on m1.k1=m2.k1    " +
                                    "   group by m1.k1, m1.k2 ) t order by k1, k2;",
                            setOperator)),
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
    public void testTransparentRewriteWithSetOperators1() {
        final Set<String> setOps = ImmutableSet.of("UNION", "INTERSECT", "EXCEPT");
        for (String setOp : setOps) {
            withPartialSetOperator(() -> {
                {
                    String[] sqls = {
                            "SELECT * from mv0 where k1=1",
                    };
                    for (String query : sqls) {
                        String plan = getFragmentPlan(query);
                        PlanTestBase.assertContains(plan, ":UNION", "mv0");
                    }
                }

                {
                    String[] sqls = {
                            "SELECT * from mv0 where k1 >= 1",
                            "SELECT * from mv0 where k1 >= 1 and k2 like 'a% ",
                            "SELECT * from mv0 where k1 <= 6 and k2 like 'a% ",
                            "SELECT * from mv0 where k1 + 1>= 1 and k2 like 'a% "
                    };
                    for (String query : sqls) {
                        String plan = getFragmentPlan(query);
                        PlanTestBase.assertContains(plan, ":UNION", ": mv0", ": m1");
                    }
                }
            }, setOp);
        }
    }

    @Test
    public void testTransparentRewriteWithSetOperators2() {
        starRocksAssert.withMTables(ImmutableList.of(m1, m2), () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1);");
            cluster.runSql("test", "insert into m2 values (4,2,1,1,1);");
            starRocksAssert.withMaterializedView((String.format("CREATE MATERIALIZED VIEW mv0 " +
                                    " PARTITION BY (k1) " +
                                    " DISTRIBUTED BY HASH(k1) " +
                                    " REFRESH DEFERRED MANUAL " +
                                    " PROPERTIES (\n" +
                                    " 'transparent_mv_rewrite_mode' = 'true'" +
                                    " ) AS" +
                                    " select * from (SELECT k1, sum(v1) as num from m1 group by k1" +
                                    " %s " +
                                    " SELECT k1, sum(v1) as num from m2 group by k1) t order by k1;",
                            "UNION ALL")),
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());

                        {
                            String[] sqls = {
                                    "SELECT * from mv0 where k1=1",
                            };
                            for (String query : sqls) {
                                String plan = getFragmentPlan(query);
                                PlanTestBase.assertContains(plan, ":UNION", "mv0");
                            }
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithWindowOperator() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT k1, k2, sum(v1) over (partition by k1 order by k2) as agg1 from m1 ;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        String[] sqls = {
                                "SELECT * from mv0 where k1=1",
                        };
                        for (String query : sqls) {
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithTopNLimit() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT k1, k2, sum(v1) as agg1 from m1 group by k1, k2 order by k2, k1 limit 10 offset 1;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        String[] sqls = {
                                "SELECT * from mv0 where k1=1",
                        };
                        for (String query : sqls) {
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithRepeat() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT k1, k2, sum(v1) from m1 group by rollup(k1, k2) " +
                            " order by k2, k1 limit 10 offset 1;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        String[] sqls = {
                                "SELECT * from mv0 where k1=1",
                        };
                        for (String query : sqls) {
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithCTE() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS with cte1 as (SELECT k1, k2, sum(v1) as agg1, count(v1) as agg2 from m1 " +
                            "group by rollup(k1, k2) order by k2, k1 limit 10 offset 1) " +
                            " select k1, count(distinct k2), count(distinct agg1), count(distinct agg2) from cte1 group by k1;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        String[] sqls = {
                                "SELECT * from mv0 where k1=1",
                        };
                        for (String query : sqls) {
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithValues() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS with cte1 as (SELECT k1, k2, sum(v1) as agg1 from m1 group by k1, k2 " +
                            "   union all select cast('2023-01-01 00:00:00' as date) as k1, 'bb' as k2, 2 as agg1) " +
                            " select k1, count(distinct k2), count(distinct agg1) from cte1 group by k1;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        String[] sqls = {
                                "SELECT * from mv0 where k1=1",
                        };
                        for (String query : sqls) {
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithTableFunctions() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS select m1.*, unnest.* from m1, unnest(split(k2, ',')) unnest ;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        String[] sqls = {
                                "SELECT * from mv0 where k1=1",
                        };
                        for (String query : sqls) {
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithCharType1() {
        starRocksAssert.withTable(m3, () -> {
            cluster.runSql("test", "insert into m3 values (1,1,1,1,1,2), (4,2,1,1,1,1), (10,10,10,10,10,10);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS select * from m3;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        String[] sqls = {
                                "SELECT * from mv0",
                                "SELECT * from mv0 where k1=1",
                        };
                        String[] expects = {
                                "  4:Project\n" +
                                        "  |  <slot 19> : 19: k1\n" +
                                        "  |  <slot 20> : 20: k2\n" +
                                        "  |  <slot 21> : 21: v1\n" +
                                        "  |  <slot 22> : 22: v2\n" +
                                        "  |  <slot 24> : 24: v4\n" +
                                        "  |  <slot 25> : CAST(23: v3 AS CHAR(20))\n" +
                                        "  |  \n" +
                                        "  3:OlapScanNode\n" +
                                        "     TABLE: m3\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     partitions=2/3",
                                "  4:Project\n" +
                                        "  |  <slot 19> : 19: k1\n" +
                                        "  |  <slot 20> : 20: k2\n" +
                                        "  |  <slot 21> : 21: v1\n" +
                                        "  |  <slot 22> : 22: v2\n" +
                                        "  |  <slot 24> : 24: v4\n" +
                                        "  |  <slot 25> : CAST(23: v3 AS CHAR(20))\n" +
                                        "  |  \n" +
                                        "  3:OlapScanNode\n" +
                                        "     TABLE: m3\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     PREDICATES: 19: k1 = 1"
                        };
                        int len = sqls.length;
                        for (int i = 0; i < len; i++) {
                            String query = sqls[i];
                            String plan = getFragmentPlan(query);
                            System.out.println(plan);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, expects[i]);
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithCharType2() {
        starRocksAssert.withTable(m3, () -> {
            cluster.runSql("test", "insert into m3 values (1,1,1,1,1,2), (4,2,1,1,1,1), (10,10,10,10,10,10);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS select * from m3 where v1 > 2;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        String[] sqls = {
                                "SELECT * from mv0",
                                "SELECT * from mv0 where v1 > 1",
                        };
                        String[] expects = {
                                "  4:Project\n" +
                                        "  |  <slot 19> : 19: k1\n" +
                                        "  |  <slot 20> : 20: k2\n" +
                                        "  |  <slot 21> : 21: v1\n" +
                                        "  |  <slot 22> : 22: v2\n" +
                                        "  |  <slot 24> : 24: v4\n" +
                                        "  |  <slot 25> : CAST(23: v3 AS CHAR(20))\n" +
                                        "  |  \n" +
                                        "  3:OlapScanNode\n" +
                                        "     TABLE: m3\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     PREDICATES: 21: v1 > 2\n" +
                                        "     partitions=2/3",
                                "  4:Project\n" +
                                        "  |  <slot 19> : 19: k1\n" +
                                        "  |  <slot 20> : 20: k2\n" +
                                        "  |  <slot 21> : 21: v1\n" +
                                        "  |  <slot 22> : 22: v2\n" +
                                        "  |  <slot 24> : 24: v4\n" +
                                        "  |  <slot 25> : CAST(23: v3 AS CHAR(20))\n" +
                                        "  |  \n" +
                                        "  3:OlapScanNode\n" +
                                        "     TABLE: m3\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     PREDICATES: 21: v1 > 2\n" +
                                        "     partitions=2/3"
                        };
                        int len = sqls.length;
                        for (int i = 0; i < len; i++) {
                            String query = sqls[i];
                            String plan = getFragmentPlan(query);
                            System.out.println(plan);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, expects[i]);
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithCharTypeWithUnionRewrite() {
        starRocksAssert.withTable(m3, () -> {
            cluster.runSql("test", "insert into m3 values (1,1,1,1,1,2), (4,2,1,1,1,1), (10,10,10,10,10,10);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'false'" +
                            " ) " +
                            " AS select * from m3 where v1 > 2;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        String[] sqls = {
                                "SELECT * from m3",
                                "SELECT * from m3 where v1 > 1",
                        };
                        String[] expects = {
                                "  4:Project\n" +
                                        "  |  <slot 19> : 19: k1\n" +
                                        "  |  <slot 20> : 20: k2\n" +
                                        "  |  <slot 21> : 21: v1\n" +
                                        "  |  <slot 22> : 22: v2\n" +
                                        "  |  <slot 24> : 24: v4\n" +
                                        "  |  <slot 25> : CAST(23: v3 AS CHAR(20))\n" +
                                        "  |  \n" +
                                        "  3:OlapScanNode\n" +
                                        "     TABLE: m3\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     PREDICATES: 21: v1 > 2\n" +
                                        "     partitions=2/3",
                                "  4:Project\n" +
                                        "  |  <slot 19> : 19: k1\n" +
                                        "  |  <slot 20> : 20: k2\n" +
                                        "  |  <slot 21> : 21: v1\n" +
                                        "  |  <slot 22> : 22: v2\n" +
                                        "  |  <slot 24> : 24: v4\n" +
                                        "  |  <slot 25> : CAST(23: v3 AS CHAR(20))\n" +
                                        "  |  \n" +
                                        "  3:OlapScanNode\n" +
                                        "     TABLE: m3\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     PREDICATES: 21: v1 > 2\n" +
                                        "     partitions=2/3"
                        };
                        int len = sqls.length;
                        for (int i = 0; i < len; i++) {
                            String query = sqls[i];
                            String plan = getFragmentPlan(query);
                            System.out.println(plan);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, expects[i]);
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithPartitionPrune1() {
        starRocksAssert.withTable(m3, () -> {
            cluster.runSql("test", "insert into m3 values (1,1,1,1,1,2), (4,2,1,1,1,1), (10,10,10,10,10,10);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS select * from m3;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        String[] sqls = {
                                "SELECT * from mv0 where k1=1",
                        };
                        String[] expects = {
                                "  4:Project\n" +
                                        "  |  <slot 19> : 19: k1\n" +
                                        "  |  <slot 20> : 20: k2\n" +
                                        "  |  <slot 21> : 21: v1\n" +
                                        "  |  <slot 22> : 22: v2\n" +
                                        "  |  <slot 24> : 24: v4\n" +
                                        "  |  <slot 25> : CAST(23: v3 AS CHAR(20))\n" +
                                        "  |  \n" +
                                        "  3:OlapScanNode\n" +
                                        "     TABLE: m3\n" +
                                        "     PREAGGREGATION: ON\n" +
                                        "     PREDICATES: 19: k1 = 1\n" +
                                        "     partitions=0/3\n" + // pruned
                                        "     rollup: m3"
                        };
                        int len = sqls.length;
                        for (int i = 0; i < len; i++) {
                            String query = sqls[i];
                            String plan = getFragmentPlan(query);
                            System.out.println(plan);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, expects[i]);
                        }
                    });
        });
    }

    @Test
    public void testTransparentMVWithListPartitionsPartialColumns1() {
        starRocksAssert.withTable(t3, () -> {
            String insertSql = "insert into t3 values(1, 1, '2024-01-01', 'beijing')," +
                    "(1, 1, '2022-01-01', 'hangzhou');";
            cluster.runSql("test", insertSql);
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (dt) " +
                            " DISTRIBUTED BY HASH(province) " +
                            " REFRESH DEFERRED MANUAL " +
                            " AS select * from t3;",
                    () -> {
                        cluster.runSql("test", String.format("REFRESH MATERIALIZED VIEW mv0 PARTITION (('%s')) " +
                                "with sync mode", "2024-01-01"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1, p3]", mvNames.toString());
                        // transparent mv
                        {
                            String plan = getFragmentPlan("select dt from t3", TExplainLevel.COSTS, "");
                            PlanTestBase.assertContains(plan, "UNION", "mv0", "t3");
                            PlanTestBase.assertContains(plan, "  0:UNION\n" +
                                    "  |  output exprs:\n" +
                                    "  |      [7, VARCHAR(10), false]\n" +
                                    "  |  child exprs:\n" +
                                    "  |      [11: dt, VARCHAR, false]\n" +
                                    "  |      [15: dt, VARCHAR, false]");
                        }
                    });
        });
    }

    @Test
    public void testTransparentMVWithListPartitionsPartialColumns2() {
        starRocksAssert.withTable(t3, () -> {
            String insertSql = "insert into t3 values(1, 1, '2024-01-01', 'beijing')," +
                    "(1, 1, '2022-01-01', 'hangzhou');";
            cluster.runSql("test", insertSql);
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (dt) " +
                            " REFRESH DEFERRED MANUAL " +
                            " AS select province, dt, min(age) from t3 group by province, dt;",
                    () -> {
                        cluster.runSql("test", String.format("REFRESH MATERIALIZED VIEW mv0 PARTITION (('%s')) " +
                                "with sync mode", "2024-01-01"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1, p3]", mvNames.toString());
                        // transparent mv
                        {
                            String plan = getFragmentPlan("select min(age) from t3 group by province;", TExplainLevel.COSTS, "");
                            PlanTestBase.assertContains(plan, "UNION", "mv0", "t3");
                            PlanTestBase.assertContains(plan, "  |  output exprs:\n" +
                                    "  |      [6, VARCHAR(64), false] | [8, SMALLINT, true]\n" +
                                    "  |  child exprs:\n" +
                                    "  |      [9: province, VARCHAR, false] | [11: min(age), SMALLINT, true]\n" +
                                    "  |      [14: province, VARCHAR, false] | [16: min, SMALLINT, true]");
                        }
                    });
        });
    }
}