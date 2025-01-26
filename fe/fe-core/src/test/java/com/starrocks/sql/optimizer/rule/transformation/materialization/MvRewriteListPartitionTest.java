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
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class MvRewriteListPartitionTest extends MVTestBase {
    private static String T1;
    private static String T2;
    private static String T3;
    private static String T4;
    private static String T5;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // table whose partitions have multiple values
        T1 = "CREATE TABLE t1 (\n" +
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
        T2 = "CREATE TABLE t2 (\n" +
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
        T3 = "CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province, dt) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\"))  ,\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\"))  ,\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-01-02\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n";
        // table with partition expression whose partitions have multiple values
        T4 = "CREATE TABLE t4 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY (province) \n" +
                "DISTRIBUTED BY RANDOM\n";
        // table with partition expression whose partitions have multi columns
        T5 = "CREATE TABLE t5 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY (province, dt) \n" +
                "DISTRIBUTED BY RANDOM\n";
        MVTestBase.beforeClass();
    }

    @Test
    public void testRewriteWithNonPartitionedMV() {
        starRocksAssert.withTable(T2, () -> {
            // update base table
            String insertSql = "insert into t2 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
            executeInsertSql(connectContext, insertSql);
            // refresh complete
            withRefreshedMV("create materialized view mv1\n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "as select dt, province, sum(age) from t2 group by dt, province;",
                    () -> {
                        String query = "select dt, province, sum(age) from t2 group by dt, province;";
                        String plan = getFragmentPlan(query);
                        // assert contains mv1
                        PlanTestBase.assertContains(plan, "     TABLE: mv1\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     partitions=1/1\n" +
                                "     rollup: mv1");
                    });
        });
    }

    @Test
    public void testRewriteWithSingleColumnPartitionedMV() {
        starRocksAssert.withTable(T2, () -> {
            // update base table
            String insertSql = "insert into t2 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
            executeInsertSql(connectContext, insertSql);
            // refresh complete
            withRefreshedMV("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "as select dt, province, sum(age) from t2 group by dt, province;",
                    () -> {
                        String query = "select dt, province, sum(age) from t2 group by dt, province;";
                        {
                            String plan = getFragmentPlan(query);
                            // assert contains mv1
                            PlanTestBase.assertContains(plan, "     TABLE: mv1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=2/2\n" +
                                    "     rollup: mv1");
                        }
                        {
                            addListPartition("t2", "p3", "hangzhou");
                            String sql = "INSERT INTO t2 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            executeInsertSql(connectContext, sql);
                            String plan = getFragmentPlan(query);
                            // assert contains union
                            PlanTestBase.assertContains(plan, "UNION");
                            // assert contains mv1
                            PlanTestBase.assertContains(plan, "     TABLE: mv1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=2/2\n" +
                                    "     rollup: mv1");
                            PlanTestBase.assertContains(plan, "     TABLE: t2\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/3");
                        }
                    });
        });
    }

    @Test
    public void testRewriteMultiBaseTablesWithSingleColumnPartitionedMV() {
        starRocksAssert.withTables(ImmutableList.of(T2, T4), () -> {
            // update base table
            String insertSql = "insert into t2 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
            executeInsertSql(connectContext, insertSql);
            // refresh complete
            withRefreshedMV("create materialized view mv1\n" +
                            "distributed by random \n" +
                            "partition by province \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "as select t2.dt, t2.province, sum(t4.age) from t2 join t4 on t2.province = t4.province\n" +
                            "group by t2.dt, t2.province;",
                    () -> {
                        String query = "select t2.dt, t2.province, sum(t4.age) from t2 join t4 on t2.province = t4.province\n" +
                                "group by t2.dt, t2.province;";
                        {
                            String plan = getFragmentPlan(query);
                            // assert contains mv1
                            PlanTestBase.assertContains(plan, "    TABLE: mv1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=2/2\n" +
                                    "     rollup: mv1");
                        }
                        {
                            addListPartition("t2", "p3", "hangzhou");
                            String sql = "INSERT INTO t2 partition(p3) values(1, 1, '2022-01-01', 'hangzhou')";
                            executeInsertSql(connectContext, sql);
                            String plan = getFragmentPlan(query);
                            // assert contains union
                            PlanTestBase.assertContains(plan, "UNION");
                            // assert contains mv1
                            PlanTestBase.assertContains(plan, "     TABLE: mv1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=2/2\n" +
                                    "     rollup: mv1");
                            PlanTestBase.assertContains(plan, "     TABLE: t2\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/3");
                        }
                    });
        });
    }

    @Test
    public void testRewriteSingleColumnPartitionedMVUnionRewrite() {
        starRocksAssert.withTable(T2, () -> {
            // update base table
            String insertSql = "insert into t2 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
            executeInsertSql(connectContext, insertSql);
            // refresh complete
            withRefreshedMV("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "as select dt, province, sum(age) from t2 group by dt, province;",
                    () -> {
                        // TODO: support list partition partial refresh
                        // refreshMaterializedViewWithPartition(DB_NAME, mvName, "beijing", "beijing");

                        String query = "select dt, province, sum(age) from t2 group by dt, province;";
                        {
                            String plan = getFragmentPlan(query);
                            // assert contains mv1
                            PlanTestBase.assertContains(plan, "     TABLE: mv1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=2/2\n" +
                                    "     rollup: mv1");
                        }
                        {
                            String sql = "insert into t2 partition(p1) values (2, 2, '2021-12-02', 'guangdong');";
                            executeInsertSql(connectContext, sql);
                            // mv is only partial refreshed, needs to union rewrite
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, "UNION");
                            PlanTestBase.assertContains(plan, "     TABLE: mv1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/2");
                            PlanTestBase.assertContains(plan, "     TABLE: t2\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/2\n" +
                                    "     rollup: t2");
                        }
                    });
        });
    }

    @Test
    public void testRewriteMultiColumnsPartitionedMVUnionRewrite1() {
        starRocksAssert.withTable(T3, () -> {
            // update base table
            String insertSql = "insert into t3 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
            executeInsertSql(connectContext, insertSql);
            // refresh complete
            withRefreshedMV("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_refresh_number' = '-1')" +
                            "as select dt, province, sum(age) from t3 group by dt, province;",
                    () -> {
                        // TODO: support list partition partial refresh
                        // refreshMaterializedViewWithPartition(DB_NAME, mvName, "beijing", "beijing");

                        String query = "select dt, province, sum(age) from t3 group by dt, province;";
                        {
                            String plan = getFragmentPlan(query);
                            // assert contains mv1
                            PlanTestBase.assertContains(plan, "     TABLE: mv1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=2/2\n" +
                                    "     rollup: mv1");
                        }
                        {
                            String sql = "insert into t3 partition(p2) values(1, 1, '2021-12-02', 'beijing');";
                            executeInsertSql(connectContext, sql);
                            // mv is only partial refreshed, needs to union rewrite
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, "UNION");
                            PlanTestBase.assertContains(plan, "     TABLE: mv1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/2");
                            PlanTestBase.assertContains(plan, "     TABLE: t3\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=2/4");
                        }
                    });
        });
    }

    @Test
    public void testRewriteMultiColumnsPartitionedMVWithTTLPartitionNumber() {
        starRocksAssert.withTable(T3, () -> {
            // update base table
            String insertSql = "insert into t3 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
            executeInsertSql(connectContext, insertSql);
            // refresh complete
            withRefreshedMV("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            "properties ('partition_ttl_number' = '1')" +
                            "as select dt, province, sum(age) from t3 group by dt, province;",
                    () -> {
                        String query = "select dt, province, sum(age) from t3 group by dt, province;";
                        {
                            String plan = getFragmentPlan(query);
                            // assert contains mv1
                            PlanTestBase.assertContains(plan, "     TABLE: mv1\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=1/1\n" +
                                    "     rollup: mv1");
                        }
                        {
                            String sql = "insert into t3 partition(p2) values(1, 1, '2024-01-01', 'guangdong');";
                            executeInsertSql(connectContext, sql);
                            // mv is ttl outdated, use base table instead
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                                    "     TABLE: t3\n" +
                                    "     PREAGGREGATION: ON\n" +
                                    "     partitions=4/4");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewrite() {
        starRocksAssert.withTable(T2, () -> {
            // update base table
            String insertSql = "insert into t2 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
            executeInsertSql(connectContext, insertSql);
            // refresh complete
            withRefreshedMV("create materialized view mv1\n" +
                            "partition by province \n" +
                            "distributed by random \n" +
                            "REFRESH DEFERRED MANUAL \n" +
                            " PROPERTIES ('transparent_mv_rewrite_mode' = 'true') " +
                            "as select dt, province, sum(age) from t2 group by dt, province;",
                    () -> {
                        // TODO: support list partition partial refresh
                        // refreshMaterializedViewWithPartition(DB_NAME, mvName, "beijing", "beijing");
                        String sql = "insert into t2 partition(p2) values (2, 2, '2021-12-02', 'guangdong');";
                        executeInsertSql(connectContext, sql);
                        // mv is only partially refreshed, needs it to union rewrite
                        String[] sqls = {
                                "SELECT * from mv1",
                                "SELECT * from mv1 where province = 'beijing'",
                                "SELECT * from mv1 where province = 'guangdong'",
                        };
                        for (String query : sqls) {
                            System.out.println("start to check: " + query);
                            String plan = getFragmentPlan(query);
                            PlanTestBase.assertContains(plan, "UNION", "mv1", "t2");
                        }
                    });
        });
    }

    @Test
    public void testTransparentRewriteWithNestedMVs() {
        starRocksAssert.withTables(ImmutableList.of(T2, T3), () -> {
            // update base table
            String insertSql = "insert into t2 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
            executeInsertSql(connectContext, insertSql);
            // refresh complete
            createAndRefreshMv("create materialized view mv1\n" +
                    "partition by province \n" +
                    "distributed by random \n" +
                    "REFRESH DEFERRED MANUAL \n" +
                    "PROPERTIES ('transparent_mv_rewrite_mode' = 'true')\n" +
                    "as select dt, province, sum(age) from t2 group by dt, province;");
            createAndRefreshMv("create materialized view mv2\n" +
                    "partition by province \n" +
                    "distributed by random \n" +
                    "REFRESH DEFERRED MANUAL \n" +
                    "PROPERTIES ('transparent_mv_rewrite_mode' = 'true')\n" +
                    "as select dt, province, sum(age) from t3 group by dt, province;");
            createAndRefreshMv("create materialized view mv3\n" +
                    "partition by province \n" +
                    "distributed by random \n" +
                    "REFRESH DEFERRED MANUAL \n" +
                    "PROPERTIES ('transparent_mv_rewrite_mode' = 'true')\n" +
                    "as select b.province, count(1) from mv1 a join mv2 b on a.province=b.province \n" +
                    "group by b.province;");
            // with full refresh
            {
                String[] sqls = {
                        "SELECT * from mv3",
                        "SELECT * from mv3 where province = 'beijing'",
                        "SELECT * from mv3 where province = 'guangdong'",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, "UNION");
                    PlanTestBase.assertContains(plan, "mv3");
                }
            }
            // with one table updated
            {
                String sql = "insert into t2 partition(p2) values (2, 2, '2021-12-02', 'guangdong');";
                executeInsertSql(connectContext, sql);
                {
                    String query = "SELECT * from mv3";
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, "UNION", "mv3", "mv1", "mv2");
                }
                {
                    String query = "SELECT * from mv3 where province = 'beijing'";
                    String plan = getFragmentPlan(query);
                    // TODO(fixme): prune empty nodes
                    PlanTestBase.assertContains(plan, "UNION", "mv3", "mv1", "mv2");
                }
                {
                    String query = "SELECT * from mv3 where province = 'guangdong'";
                    String plan = getFragmentPlan(query);
                    // TODO(fixme): prune empty nodes
                    PlanTestBase.assertContains(plan, "UNION", "mv3", "mv1", "mv2");
                }
            }
            // with two tables updated
            {
                String sql = "insert into t3 partition(p2) values (2, 2, '2024-01-01', 'guangdong');";
                executeInsertSql(connectContext, sql);
                {
                    String query = "SELECT * from mv3";
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, "UNION", "mv3", "mv1", "mv2");
                }
                {
                    String query = "SELECT * from mv3 where province = 'beijing'";
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, "UNION", "mv3", "mv1", "mv2");
                }
                {
                    String query = "SELECT * from mv3 where province = 'guangdong'";
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, "UNION", "mv3", "mv1", "mv2");
                }
            }
            starRocksAssert.dropMaterializedView("mv3");
            starRocksAssert.dropMaterializedView("mv2");
            starRocksAssert.dropMaterializedView("mv1");
        });
    }

    @Test
    public void testRewriteWithNestedMVs() {
        starRocksAssert.withTables(ImmutableList.of(T2, T3), () -> {
            // update base table
            String insertSql = "insert into t2 partition(p1) values(1, 1, '2021-12-01', 'beijing');";
            executeInsertSql(connectContext, insertSql);
            // refresh complete
            createAndRefreshMv("create materialized view mv1\n" +
                    "partition by province \n" +
                    "distributed by random \n" +
                    "REFRESH DEFERRED MANUAL \n" +
                    "PROPERTIES ('transparent_mv_rewrite_mode' = 'false')\n" +
                    "as select dt, province, sum(age) from t2 group by dt, province;");
            createAndRefreshMv("create materialized view mv2\n" +
                    "partition by province \n" +
                    "distributed by random \n" +
                    "REFRESH DEFERRED MANUAL \n" +
                    "PROPERTIES ('transparent_mv_rewrite_mode' = 'false')\n" +
                    "as select dt, province, sum(age) from t3 group by dt, province;");
            createAndRefreshMv("create materialized view mv3\n" +
                    "partition by province \n" +
                    "distributed by random \n" +
                    "REFRESH DEFERRED MANUAL \n" +
                    "PROPERTIES ('transparent_mv_rewrite_mode' = 'false')\n" +
                    "as select b.province, count(1) from mv1 a join mv2 b on a.province=b.province \n" +
                    "group by b.province;");
            // with full refresh
            {
                String[] sqls = {
                        "select b.province, count(1) from mv1 a join mv2 b on a.province=b.province group by b.province;",
                        "select b.province, count(1) from mv1 a join mv2 b on a.province=b.province where b" +
                                ".province='guangdong' group by b.province",
                        "select b.province, count(1) from mv1 a join mv2 b on a.province=b.province where b.province='beijing' " +
                                "group by b.province",
                };
                for (String query : sqls) {
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, "UNION");
                    PlanTestBase.assertContains(plan, "mv3");
                }
            }
            // with one table updated
            {
                String sql = "insert into t2 partition(p2) values (2, 2, '2021-12-02', 'guangdong');";
                executeInsertSql(connectContext, sql);
                {
                    String query = "select b.province, count(1) from mv1 a join mv2 b on a.province=b.province " +
                            "group by b.province;";
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, "UNION", "mv3", "mv1", "mv2");
                }
                {
                    String query = "select b.province, count(1) from mv1 a join mv2 b on a.province=b.province " +
                            "where b.province='beijing' group by b.province;";
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, "UNION");
                    PlanTestBase.assertContains(plan, "mv3");
                }
                {
                    String query = "select b.province, count(1) from mv1 a join mv2 b on a.province=b.province " +
                            "where b.province='guangdong' group by b.province;";
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, "UNION", "mv3");
                    PlanTestBase.assertContains(plan, "mv1", "mv2");
                }
            }
            // with two tables updated
            {
                String sql = "insert into t3 partition(p2) values (2, 2, '2024-01-01', 'guangdong');";
                executeInsertSql(connectContext, sql);
                {
                    String query = "select b.province, count(1) from mv1 a join mv2 b on a.province=b.province " +
                            "group by b.province;";
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertContains(plan, "UNION", "mv3", "mv1", "mv2");
                }
                {
                    String query = "select b.province, count(1) from mv1 a join mv2 b on a.province=b.province " +
                            "where b.province='beijing' group by b.province;";
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, "UNION");
                    PlanTestBase.assertContains(plan, "mv3");
                }
                {
                    String query = "select b.province, count(1) from mv1 a join mv2 b on a.province=b.province " +
                            "where b.province='guangdong' group by b.province;";
                    String plan = getFragmentPlan(query);
                    PlanTestBase.assertNotContains(plan, "UNION", "mv3");
                    PlanTestBase.assertContains(plan, "mv1", "mv2");
                }
            }
            starRocksAssert.dropMaterializedView("mv3");
            starRocksAssert.dropMaterializedView("mv2");
            starRocksAssert.dropMaterializedView("mv1");
        });
    }
}
