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
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.schema.MTable;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

public class MvTransparentRewriteWithOlapTableTest extends MVTestBase {
    private static MTable m1;
    private static MTable m2;
    private static MTable m3;
    private static String t1;
    private static String t2;
    private static String t3;
    private static String R2;

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
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

        // partition table by partition expression
        R2 = "CREATE TABLE r2 \n" +
                "(\n" +
                "    dt datetime,\n" +
                "    k2 int,\n" +
                "    v1 int \n" +
                ")\n" +
                "PARTITION BY date_trunc('day', dt)\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');";
    }

    private void withTablePartitionsV2(String tableName) {
        addRangePartition(tableName, "p1", "2024-01-29", "2024-01-30");
        addRangePartition(tableName, "p2", "2024-01-30", "2024-01-31");
        addRangePartition(tableName, "p3", "2024-01-31", "2024-02-01");
        addRangePartition(tableName, "p4", "2024-02-01", "2024-02-02");
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
                    String plan = getFragmentPlan(query);
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
                                "SELECT k1, agg1 from mv0 where k1=1",
                        };
                        for (String query : sqls) {
                            String plan = getFragmentPlan(query, TExplainLevel.VERBOSE);
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
                                "SELECT k1 from mv0 where k1=1",
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
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, expects[i]);
                        }
                    });
        });
    }

    @Test
    public void testTransparentMVWithListPartitions1() {
        starRocksAssert.withTable(t1, () -> {
            String insertSql = "insert into t1 values(1, 1, '2021-12-01', 'beijing'), (1, 1, '2021-12-01', 'guangdong');";
            cluster.runSql("test", insertSql);
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (province) " +
                            " DISTRIBUTED BY HASH(province) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS select * from t1;",
                    () -> {
                        {
                            String plan = getFragmentPlan("select * from mv0");
                            PlanTestBase.assertContains(plan, "UNION", "mv0", "t1");
                        }
                        cluster.runSql("test", String.format("REFRESH MATERIALIZED VIEW mv0 PARTITION ('%s') with sync mode",
                                "beijing"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1, p2]", mvNames.toString());
                        // transparent mv
                        {
                            String plan = getFragmentPlan("select * from mv0");
                            PlanTestBase.assertContains(plan, "UNION", "mv0", "t1");
                        }
                    });
        });
    }

    @Test
    public void testTransparentMVWithListPartitions2() {
        starRocksAssert.withTable(t3, () -> {
            String insertSql = "insert into t3 values(1, 1, '2024-01-01', 'beijing')," +
                    "(1, 1, '2022-01-01', 'hangzhou');";
            cluster.runSql("test", insertSql);
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (province, dt) " +
                            " DISTRIBUTED BY HASH(province) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS select * from t3;",
                    () -> {
                        {
                            String plan = getFragmentPlan("select * from mv0");
                            PlanTestBase.assertContains(plan, "UNION", "mv0", "t3");
                        }
                        cluster.runSql("test", String.format("REFRESH MATERIALIZED VIEW mv0 PARTITION (('%s', '%s')) " +
                                        "with sync mode", "beijing", "2024-01-01"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1, p2, p3, p4]", mvNames.toString());
                        // transparent mv
                        {
                            String plan = getFragmentPlan("select * from mv0");
                            PlanTestBase.assertContains(plan, "UNION", "mv0", "t3");
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
                            " PARTITION BY (province, dt) " +
                            " DISTRIBUTED BY HASH(province) " +
                            " REFRESH DEFERRED MANUAL " +
                            " AS select * from t3;",
                    () -> {
                        cluster.runSql("test", String.format("REFRESH MATERIALIZED VIEW mv0 PARTITION (('%s', '%s')) " +
                                "with sync mode", "beijing", "2024-01-01"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1, p2, p3, p4]", mvNames.toString());
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
                            " PARTITION BY (province, dt) " +
                            " REFRESH DEFERRED MANUAL " +
                            " AS select province, dt, min(age) from t3 group by province, dt;",
                    () -> {
                        cluster.runSql("test", String.format("REFRESH MATERIALIZED VIEW mv0 PARTITION (('%s', '%s')) " +
                                "with sync mode", "beijing", "2024-01-01"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1, p2, p3, p4]", mvNames.toString());
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

    @Test
    public void testTransparentRewriteWithAggregateColumnsPrune() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT k1, k2, sum(v1) as agg1 from m1 group by k1, k2;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());

                        {
                            String query = "SELECT k1, agg1 from mv0 where k1=1";
                            String plan = getFragmentPlan(query, TExplainLevel.VERBOSE);
                            PlanTestBase.assertContains(plan, "  0:UNION\n" +
                                    "  |  output exprs:\n" +
                                    "  |      [1, INT, true] | [3, BIGINT, true]\n" +
                                    "  |  child exprs:\n" +
                                    "  |      [7: k1, INT, true] | [9: agg1, BIGINT, true]\n" +
                                    "  |      [10: k1, INT, true] | [15: sum, BIGINT, true]");
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, "  4:AGGREGATE (update finalize)\n" +
                                    "  |  aggregate: sum[([12: v1, INT, true]); args: INT; result: BIGINT; " +
                                    "args nullable: true; result nullable: true]\n" +
                                    "  |  group by: [10: k1, INT, true], [11: k2, VARCHAR, true]");
                        }

                        {
                            String query = "SELECT k1, sum(v1) as agg1 from m1 group by k1;";
                            String plan = getFragmentPlan(query, TExplainLevel.VERBOSE);
                            PlanTestBase.assertContains(plan, "  0:UNION\n" +
                                    "  |  output exprs:\n" +
                                    "  |      [7, INT, true] | [9, BIGINT, true]\n" +
                                    "  |  child exprs:\n" +
                                    "  |      [10: k1, INT, true] | [12: agg1, BIGINT, true]\n" +
                                    "  |      [13: k1, INT, true] | [18: sum, BIGINT, true]");
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, "4:AGGREGATE (update serialize)\n" +
                                    "  |  STREAMING\n" +
                                    "  |  aggregate: sum[([15: v1, INT, true]); args: INT; result: BIGINT; " +
                                    "args nullable: true; result nullable: true]\n" +
                                    "  |  group by: [13: k1, INT, true]");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithAggregateFilterColumnsPrune() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT k1, k2, sum(v1) as agg1 from m1 group by k1, k2 having sum(v1) > 1;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        {
                            String query = "SELECT k1, sum(v1) as agg1 from m1 group by k1 having sum(v1) > 2;";
                            String plan = getFragmentPlan(query, TExplainLevel.VERBOSE);
                            PlanTestBase.assertContains(plan, "  0:UNION\n" +
                                    "  |  output exprs:\n" +
                                    "  |      [7, INT, true] | [9, BIGINT, true]\n" +
                                    "  |  child exprs:\n" +
                                    "  |      [10: k1, INT, true] | [12: agg1, BIGINT, true]\n" +
                                    "  |      [13: k1, INT, true] | [18: sum, BIGINT, true]");
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, "4:AGGREGATE (update serialize)\n" +
                                    "  |  STREAMING\n" +
                                    "  |  aggregate: sum[([15: v1, INT, true]); args: INT; result: BIGINT; " +
                                    "args nullable: true; result nullable: true]\n" +
                                    "  |  group by: [13: k1, INT, true]");
                        }

                        {
                            String query = "SELECT k1, sum(v1) as agg1 from m1 group by k1 having sum(v1) > 0;";
                            String plan = getFragmentPlan(query, TExplainLevel.VERBOSE);
                            PlanTestBase.assertContains(plan, "  0:UNION\n" +
                                    "  |  output exprs:\n" +
                                    "  |      [7, INT, true] | [9, BIGINT, true]\n" +
                                    "  |  child exprs:\n" +
                                    "  |      [10: k1, INT, true] | [12: agg1, BIGINT, true]\n" +
                                    "  |      [13: k1, INT, true] | [18: sum, BIGINT, true]");
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, "4:AGGREGATE (update serialize)\n" +
                                    "  |  STREAMING\n" +
                                    "  |  aggregate: sum[([15: v1, INT, true]); args: INT; result: BIGINT; " +
                                    "args nullable: true; result nullable: true]\n" +
                                    "  |  group by: [13: k1, INT, true]");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithJoinColumnsPrune() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (ak1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT a.k1 as ak1, a.k2 as ak2, a.v1 as av1," +
                            "   b.k1 as bk1, b.k2 as bk2, b.v1 as bv1 " +
                            "   from m1 a join m1 b on a.k1=b.k1;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());

                        {
                            String query = "SELECT a.k1, b.v1 from m1 a join m1 b on a.k1=b.k1;";
                            String plan = getFragmentPlan(query, TExplainLevel.VERBOSE);
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, "  0:UNION\n" +
                                    "  |  output exprs:\n" +
                                    "  |      [11, INT, true] | [16, INT, true]\n" +
                                    "  |  child exprs:\n" +
                                    "  |      [17: ak1, INT, true] | [22: bv1, INT, true]\n" +
                                    "  |      [23: k1, INT, true] | [30: v1, INT, true]");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithColumnsPrune() {
        final String ddl = "CREATE TABLE mock_tbl1 (\n" +
                "data_hour varchar(65533) NOT NULL,\n" +
                "event_min varchar(65533) NOT NULL,\n" +
                "mc_id int(11) NULL,\n" +
                "c_id int(11) NULL,\n" +
                "target_id int(11) NULL,\n" +
                "c_slot_id varchar(65533) NULL,\n" +
                "adx_slot_id int(11) NULL,\n" +
                "mc_slot_type int(11) NULL,\n" +
                "mc_slot_id varchar(65533) NULL,\n" +
                "c_slot_type int(11) NULL,\n" +
                "adx_c_slot_id int(11) NULL DEFAULT \"-1\",\n" +
                "conv_type_id varchar(65533) NULL,\n" +
                "sdkv varchar(65533) NULL,\n" +
                "pkg varchar(65533) NULL,\n" +
                "c_pkg varchar(65533) NULL,\n" +
                "platform varchar(65533) NULL,\n" +
                "integrate_by int(11) NULL,\n" +
                "flow_price double NULL DEFAULT \"0.2894\",\n" +
                "mc_category int(11) NULL,\n" +
                "mc_tier int(11) NULL,\n" +
                "version varchar(65533) NULL,\n" +
                "c_task_id varchar(65533) NULL,\n" +
                "p_id int(11) NULL DEFAULT \"-1\",\n" +
                "experiment_id varchar(65533) NULL,\n" +
                "touch_amount varchar(65533) NULL,\n" +
                "s_req bigint(20) NULL,\n" +
                "c_req bigint(20) NULL,\n" +
                "s_win bigint(20) NULL,\n" +
                "c_win bigint(20) NULL,\n" +
                "imp bigint(20) NULL,\n" +
                "clk bigint(20) NULL,\n" +
                "c_floor double NULL,\n" +
                "c_fill_price double NULL,\n" +
                "c_fee_price double NULL DEFAULT \"0\",\n" +
                "imp_c_bid_price double NULL DEFAULT \"0\",\n" +
                "c_req_timeout bigint(20) NULL,\n" +
                "s_bid_price double NULL,\n" +
                "s_floor double NULL,\n" +
                "c_win_price double NULL,\n" +
                "c_fill_req bigint(20) NULL,\n" +
                "c_fill_valid bigint(20) NULL DEFAULT \"0\",\n" +
                "used_time bigint(20) NULL,\n" +
                "c_used_time bigint(20) NULL DEFAULT \"0\",\n" +
                "cr double NULL,\n" +
                "cr_rebate double NULL,\n" +
                "mc_cr double NULL,\n" +
                "body_size bigint(20) NULL,\n" +
                "launch bigint(20) NULL,\n" +
                "mc_origin_cr double NULL DEFAULT \"0\",\n" +
                "data_date datetime NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(data_hour, event_min, mc_id)\n" +
                "PARTITION BY date_trunc('hour', data_date)\n" +
                "DISTRIBUTED BY HASH(pkg, c_slot_id, mc_id) BUCKETS 6\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(ddl, () -> {
            cluster.runSql("test", "insert into mock_tbl1(data_hour, event_min, " +
                    "mc_id, data_date,pkg, c_slot_id ) values('2025-01-20 05:17:01', '1', '1', " +
                    "'2025-01-20 05:17:02', '1', '1');");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_mv1\n" +
                            "PARTITION BY (data_date)\n" +
                            "REFRESH DEFERRED ASYNC\n" +
                            "PROPERTIES (\n" +
                            "\"partition_refresh_number\" = \"1\",\n" +
                            "\"transparent_mv_rewrite_mode\" = \"true\"\n" +
                            ")\n" +
                            "AS SELECT date_trunc('day', data_date) AS data_date, to_date(data_date) AS dt, " +
                            "mc_id, c_id, target_id, mc_slot_type, mc_slot_id, c_slot_id, " +
                            "adx_slot_id, platform, sdkv, pkg, c_pkg, conv_type_id, c_slot_type, adx_c_slot_id, " +
                            "c_task_id, p_id, experiment_id, mc_tier, mc_category, " +
                            "integrate_by, version, sum(c_floor) AS c_floor, sum(imp) AS imp, " +
                            "sum(cr) AS cr, sum(c_fill_price) AS c_fill_price, " +
                            "sum(c_fee_price) AS c_fee_price, sum(imp_c_bid_price) AS imp_c_bid_price, " +
                            "sum(c_req_timeout) AS c_req_timeout, sum(s_bid_price) AS s_bid_price, " +
                            "sum(mc_cr) AS mc_cr, sum(clk) AS clk, sum(s_floor) AS s_floor, " +
                            "sum(c_win_price) AS c_win_price, sum(s_req) AS s_req, sum(launch) AS launch, " +
                            "sum(s_win) AS s_win, sum(c_fill_req) AS c_fill_req, " +
                            "sum(c_req) AS c_req, sum(used_time) AS used_time, " +
                            "sum(c_used_time) AS c_used_time, sum(c_win) AS c_win, " +
                            "sum(body_size) AS body_size, sum(cr_rebate) AS cr_rebate, " +
                            "sum(mc_origin_cr) AS mc_origin_cr\n" +
                            "FROM mock_tbl1\n" +
                            "WHERE data_date >= '2024-10-11 00:00:00'\n" +
                            "GROUP BY date_trunc('day', data_date), to_date(data_date), " +
                            "mc_id, c_id, target_id, mc_slot_type, mc_slot_id, " +
                            "c_slot_id, adx_slot_id, platform, sdkv, pkg, c_pkg, conv_type_id, " +
                            "c_slot_type, adx_c_slot_id, c_task_id, p_id, " +
                            "experiment_id, mc_tier, mc_category, integrate_by, version;",
                    () -> {
                        starRocksAssert.refreshMV(connectContext, "test_mv1");

                        MaterializedView mv1 = getMv("test", "test_mv1");
                        Assert.assertTrue(mv1 != null);

                        cluster.runSql("test", "insert into mock_tbl1(data_hour, event_min, mc_id, data_date,pkg, " +
                                "c_slot_id ) values('2025-01-21 05:17:01', '1', '1', '2025-01-21 05:17:02', '1', '1');\n");
                        {
                            String query = "select date_trunc('day', data_date) \n" +
                                    "from mock_tbl1\n" +
                                    "group by date_trunc('day', data_date)";
                            String plan = getFragmentPlan(query, TExplainLevel.VERBOSE);
                            System.out.println(plan);
                            PlanTestBase.assertContains(plan, "test_mv1");
                            PlanTestBase.assertContains(plan, " 0:UNION\n" +
                                    "  |  output exprs:\n" +
                                    "  |      [150, DATETIME, true]\n" +
                                    "  |  child exprs:\n" +
                                    "  |      [148: date_trunc, DATETIME, true]\n" +
                                    "  |      [149: date_trunc, DATETIME, true");
                            PlanTestBase.assertContains(plan, "2:Project\n" +
                                    "  |  output columns:\n" +
                                    "  |  148 <-> date_trunc[('day', [98: data_date, DATETIME, false]); " +
                                    "args: VARCHAR,DATETIME; result: DATETIME; args nullable: false; result nullable: true]");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithOrderBy() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " ORDER BY (v11, k2, k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT k1, k2, sum(v1) as v11 from m1 group by k1, k2;",
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", "mv0");
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assert.assertEquals("[p1]", mvNames.toString());
                        // test: query rewrite
                        {
                            final String query = "SELECT k1, k2, sum(v1) as v11 from m1 group by k1, k2;";
                            final String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                        }

                        // test: query mv directly
                        {
                            final String query = "SELECT k1, k2, v11 from mv0 where k1=1";
                            final String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, " OUTPUT EXPRS:3: k1 | 2: k2 | 1: v11\n" +
                                    "  PARTITION: RANDOM");
                        }

                        // test: query mv directly
                        {
                            final String query = "SELECT k2, v11, k2 from mv0 where k1=1";
                            final String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, "OUTPUT EXPRS:2: k2 | 1: v11 | 2: k2");
                        }

                        // test: query mv directly
                        {
                            final String query = "SELECT * from mv0 where k1=1";
                            final String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, ":UNION");
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, " OUTPUT EXPRS:1: v11 | 2: k2 | 3: k1");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithNonDeterministicFunctions() {
        starRocksAssert.withTable(R2, (obj) -> {
            String tableName = (String) obj;
            withTablePartitionsV2(tableName);
            OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
            Assert.assertEquals(4, olapTable.getVisiblePartitions().size());
            cluster.runSql("test", String.format("insert into %s values ('2024-02-01', 1, 1);", tableName));

            starRocksAssert.withMaterializedView(String.format("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (dt) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT dt, k2, sum(v1) as agg1 from %s where date_trunc('day', dt) < timestamp(curdate()) " +
                            " group by dt, k2;", tableName),
                    () -> {
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n"));
                        MaterializedView mv = getMv("test", "mv0");
                        Set<String> mvNames = mv.getPartitionNames();
                        Assert.assertEquals(4, mvNames.size());

                        // test mv get plan context
                        {
                            MvPlanContext mvPlanContext = getOptimizedPlan(mv, true, true);
                            Assert.assertTrue(mvPlanContext != null);
                            Assert.assertTrue(!mvPlanContext.isValidMvPlan());
                            Assert.assertTrue(mvPlanContext.getLogicalPlan() == null);
                            Assert.assertTrue(mvPlanContext.getInvalidReason().contains("non-deterministic function"));
                        }
                        {
                            MvPlanContext mvPlanContext = getOptimizedPlan(mv, true, false);
                            Assert.assertTrue(mvPlanContext != null);
                            Assert.assertFalse(mvPlanContext.isValidMvPlan());
                            Assert.assertTrue(mvPlanContext.getLogicalPlan() != null);
                            // For transparent mv we cannot const fold non-deterministic function because it should be changed
                            // for each time.
                            Assert.assertTrue(hasNonDeterministicFunction(mvPlanContext.getLogicalPlan()));
                        }

                        {
                            String query = "SELECT * from mv0;";
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertNotContains(plan, tableName);
                        }

                        {
                            // add new partitions
                            LocalDateTime now = LocalDateTime.now();
                            addRangePartition(tableName, "p5",
                                    now.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                    now.plusDays(2).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                    true);
                            String query = "SELECT * from mv0;";
                            FeConstants.enablePruneEmptyOutputScan = true;
                            String plan = getFragmentPlan(query);
                            FeConstants.enablePruneEmptyOutputScan = false;
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertNotContains(plan, "UNION", tableName);
                        }
                        {
                            // add new partitions
                            LocalDateTime now = LocalDateTime.now();
                            addRangePartition(tableName, "p6",
                                    now.minusDays(2).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                    now.minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                    true);
                            String query = "SELECT * from mv0;";
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, "mv0");
                            PlanTestBase.assertContains(plan, "UNION", tableName);
                        }
                    });
        });
    }
}
