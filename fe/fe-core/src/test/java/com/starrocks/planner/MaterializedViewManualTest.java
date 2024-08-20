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

package com.starrocks.planner;

import org.junit.BeforeClass;
import org.junit.Test;

public class MaterializedViewManualTest extends MaterializedViewTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MaterializedViewTestBase.beforeClass();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        starRocksAssert.useTable("depts");
        starRocksAssert.useTable("depts_null");
        starRocksAssert.useTable("emps");
        starRocksAssert.useTable("emps_null");
    }

    @Test
    public void testDistinct1() throws Exception {
        String mv = "CREATE MATERIALIZED VIEW `test_distinct_mv1`\n" +
                "DISTRIBUTED BY HASH(`deptno`, `locationid`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ")\n" +
                "AS \n" +
                "SELECT \n" +
                "  `locationid`,\n" +
                "  `deptno`,\n" +
                "  count(DISTINCT `empid`) AS `order_num`\n" +
                "FROM `emps`\n" +
                "GROUP BY `locationid`, `deptno`;";
        starRocksAssert.withMaterializedView(mv);
        sql("select deptno, count(distinct empid) from emps group by deptno")
                .nonMatch("test_distinct_mv1");
        starRocksAssert.dropMaterializedView("test_distinct_mv1");
    }

    @Test
    public void testDistinct2() throws Exception {
        String tableSQL = "CREATE TABLE `test_distinct2` (\n" +
                "  `order_id` bigint(20) NOT NULL DEFAULT \"-1\" COMMENT \"\",\n" +
                "  `dt` datetime NOT NULL DEFAULT \"1996-01-01 00:00:00\" COMMENT \"\",\n" +
                "  `k1`bigint,\n" +
                "  `v1` varchar(256) NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`order_id`, `dt`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(\n" +
                "PARTITION p2023041017 VALUES [(\"2023-04-10 17:00:00\"), (\"2023-04-10 18:00:00\")),\n" +
                "PARTITION p2023041021 VALUES [(\"2023-04-10 21:00:00\"), (\"2023-04-10 22:00:00\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`order_id`) BUCKETS 9\n" +
                "PROPERTIES (\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"HOUR\",\n" +
                "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n" +
                "\"dynamic_partition.start\" = \"-240\",\n" +
                "\"dynamic_partition.end\" = \"2\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"9\"," +
                "\"replication_num\" = \"1\"" +
                ");";
        starRocksAssert.withTable(tableSQL);
        String mv = "CREATE MATERIALIZED VIEW `test_distinct2_mv`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (date_trunc('hour', `dt`))\n" +
                "DISTRIBUTED BY HASH(`dt`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ")\n" +
                "AS\n" +
                "SELECT \n" +
                "k1, " +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "`dt`\n" +
                "FROM `test_distinct2`\n" +
                "group by dt, k1;";
        starRocksAssert.withMaterializedView(mv);
        sql("select dt, count(distinct order_id) " +
                "from test_partition_expr_tbl1 group by dt")
                .nonMatch("test_partition_expr_mv1");
        starRocksAssert.dropMaterializedView("test_distinct2_mv");
    }

    @Test
    public void testLimit1() throws Exception {
        String sql = "CREATE TABLE `test_limit_1` (\n" +
                "  `id` bigint(20) NULL COMMENT \"\",\n" +
                "  `dt` date NULL COMMENT \"\",\n" +
                "  `hour` varchar(65533) NULL COMMENT \"\"," +
                "  `v1` float NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`, `dt`, `hour`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p202207 VALUES [(\"2022-07-01\"), (\"2022-08-01\")),\n" +
                "PARTITION p202210 VALUES [(\"2022-10-01\"), (\"2022-11-01\")))\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 144\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ");";
        starRocksAssert.withTable(sql);
        String mv = "SELECT dt, count(DISTINCT id) from " +
                "test_limit_1 where dt >= '2022-07-01' and dt <= '2022-10-01' group by dt";
        testRewriteOK(mv, "SELECT dt, count(DISTINCT id) from test_limit_1 where " +
                "dt >= '2022-07-01' and dt <= '2022-10-01' group by dt limit 10");
    }

    @Test
    public void testPartitionColumnExpr1() throws Exception {
        String tableSQL = "CREATE TABLE `test_partition_expr_tbl1` (\n" +
                "  `order_id` bigint(20) NOT NULL DEFAULT \"-1\" COMMENT \"\",\n" +
                "  `dt` datetime NOT NULL DEFAULT \"1996-01-01 00:00:00\" COMMENT \"\",\n" +
                "  `value` varchar(256) NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`order_id`, `dt`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(\n" +
                "PARTITION p2023041017 VALUES [(\"2023-04-10 17:00:00\"), (\"2023-04-10 18:00:00\")),\n" +
                "PARTITION p2023041021 VALUES [(\"2023-04-10 21:00:00\"), (\"2023-04-10 22:00:00\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`order_id`) BUCKETS 9\n" +
                "PROPERTIES (\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"HOUR\",\n" +
                "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n" +
                "\"dynamic_partition.start\" = \"-240\",\n" +
                "\"dynamic_partition.end\" = \"2\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"9\"," +
                "\"replication_num\" = \"1\"" +
                ");";
        starRocksAssert.withTable(tableSQL);
        String mv = "CREATE MATERIALIZED VIEW `test_partition_expr_mv1`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (date_trunc('hour', ds))\n" +
                "DISTRIBUTED BY HASH(`order_num`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ")\n" +
                "AS\n" +
                "SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "time_slice(`dt`, INTERVAL 5 minute) AS ds\n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "group by ds;";
        starRocksAssert.withMaterializedView(mv);

        sql("SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "time_slice(`dt`, INTERVAL 5 minute) AS ds \n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "WHERE time_slice(`dt`, INTERVAL 5 minute) BETWEEN '2023-04-11' AND '2023-04-12'\n" +
                "group by ds")
                .match("test_partition_expr_mv1");

        sql("SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "time_slice(`dt`, INTERVAL 5 minute) AS ds \n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "WHERE time_slice(`dt`, INTERVAL 5 minute) BETWEEN '2023-04-11' AND '2023-04-12' or " +
                "time_slice(`dt`, INTERVAL 5 minute) BETWEEN '2023-04-12' AND '2023-04-13'\n" +
                "group by ds")
                .match("test_partition_expr_mv1");
        sql("SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "time_slice(`dt`, INTERVAL 5 minute) AS `ts`\n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "WHERE time_slice(dt, interval 5 minute) >= '2023-04-10 17:00:00' and time_slice(dt, interval 5 minute) < '2023-04-10 18:00:00'\n" +
                "group by ts")
                .match("test_partition_expr_mv1");

        sql("SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "time_slice(`dt`, INTERVAL 5 minute) AS `ts`\n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "WHERE `dt` >= '2023-04-10 17:00:00' and `dt` < '2023-04-10 18:00:00'\n" +
                "group by ts")
                .match("test_partition_expr_mv1");
        starRocksAssert.dropTable("test_partition_expr_tbl1");
        starRocksAssert.dropMaterializedView("test_partition_expr_mv1");
    }

    @Test
    public void testNullableTestCase1() throws Exception {
        String mv = "create materialized view join_null_mv_2\n" +
                "distributed by hash(empid)\n" +
                "as\n" +
                "select empid, depts.deptno, depts.name from emps_null join depts using (deptno);";
        starRocksAssert.withMaterializedView(mv);
        sql("select empid, emps_null.deptno \n" +
                "from emps_null join depts using (deptno) \n" +
                "where empid < 10")
                .match("join_null_mv_2");
    }

    @Test
    public void testNullableTestCase2() throws Exception {
        String mv = "create materialized view join_null_mv\n" +
                "distributed by hash(empid)\n" +
                "as\n" +
                "select empid, depts_null.deptno, depts_null.name from emps_null join depts_null using (deptno)\n";
        starRocksAssert.withMaterializedView(mv);
        sql("select empid, depts_null.deptno, depts_null.name from emps_null " +
                "join depts_null using (deptno) where depts_null.deptno < 10;")
                .match("join_null_mv");
    }

    @Test
    public void testDateTruncPartitionColumnExpr1() throws Exception {
        String tableSQL = "CREATE TABLE `test_partition_expr_tbl1` (\n" +
                "  `order_id` bigint(20) NOT NULL DEFAULT \"-1\" COMMENT \"\",\n" +
                "  `dt` datetime NOT NULL DEFAULT \"1996-01-01 00:00:00\" COMMENT \"\",\n" +
                "  `value` varchar(256) NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`order_id`, `dt`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(\n" +
                "PARTITION p2023041017 VALUES [(\"2023-04-10 17:00:00\"), (\"2023-04-10 18:00:00\")),\n" +
                "PARTITION p2023041021 VALUES [(\"2023-04-10 21:00:00\"), (\"2023-04-10 22:00:00\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`order_id`)";
        starRocksAssert.withTable(tableSQL);
        String mv = "CREATE MATERIALIZED VIEW `test_partition_expr_mv1`\n" +
                "PARTITION BY ds \n" +
                "DISTRIBUTED BY RANDOM \n" +
                "AS SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "date_trunc('minute', `dt`) AS ds\n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "group by ds;";
        starRocksAssert.withMaterializedView(mv);

        {
            sql("SELECT \n" +
                    "count(DISTINCT `order_id`) AS `order_num`, \n" +
                    "date_trunc('minute', `dt`) AS ds \n" +
                    "FROM `test_partition_expr_tbl1`\n" +
                    "WHERE date_trunc('minute', `dt`) BETWEEN '2023-04-11' AND '2023-04-12'\n" +
                    "group by ds")
                    .match("test_partition_expr_mv1");
        }

        {
            sql("SELECT \n" +
                    "count(DISTINCT `order_id`) AS `order_num`, \n" +
                    "date_trunc('minute', `dt`) AS ds \n" +
                    "FROM `test_partition_expr_tbl1`\n" +
                    "WHERE `dt` BETWEEN '2023-04-11' AND '2023-04-12'\n" +
                    "group by ds")
                    .match("test_partition_expr_mv1");
        }

        starRocksAssert.dropMaterializedView("test_partition_expr_mv1");
        starRocksAssert.dropTable("test_partition_expr_tbl1");
    }

    @Test
    public void testDateTruncPartitionColumnExpr2() throws Exception {
        String tableSQL = "CREATE TABLE `test_partition_expr_tbl1` (\n" +
                "  `order_id` bigint(20) NOT NULL DEFAULT \"-1\" COMMENT \"\",\n" +
                "  `dt` datetime NOT NULL DEFAULT \"1996-01-01 00:00:00\" COMMENT \"\",\n" +
                "  `value` varchar(256) NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`order_id`, `dt`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(\n" +
                "PARTITION p2023041017 VALUES [(\"2023-04-10 17:00:00\"), (\"2023-04-10 18:00:00\")),\n" +
                "PARTITION p2023041021 VALUES [(\"2023-04-10 21:00:00\"), (\"2023-04-10 22:00:00\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`order_id`)";
        starRocksAssert.withTable(tableSQL);
        String mv = "CREATE MATERIALIZED VIEW `test_partition_expr_mv1`\n" +
                "PARTITION BY ds \n" +
                "DISTRIBUTED BY RANDOM \n" +
                "AS SELECT \n" +
                "count(DISTINCT `order_id`) AS `order_num`, \n" +
                "date_trunc('day', `dt`) AS ds\n" +
                "FROM `test_partition_expr_tbl1`\n" +
                "group by ds;";
        starRocksAssert.withMaterializedView(mv);

        {
            sql("SELECT \n" +
                    "count(DISTINCT `order_id`) AS `order_num`, \n" +
                    "date_trunc('day', `dt`) AS ds \n" +
                    "FROM `test_partition_expr_tbl1`\n" +
                    "WHERE date_trunc('month', `dt`) = '2023-04-01'\n" +
                    "group by ds")
                    .nonMatch("test_partition_expr_mv1");
        }

        {
            sql("SELECT \n" +
                    "count(DISTINCT `order_id`) AS `order_num`, \n" +
                    "date_trunc('minute', `dt`) AS ds \n" +
                    "FROM `test_partition_expr_tbl1`\n" +
                    "WHERE date_trunc('month', `dt`) BETWEEN '2023-04-01' AND '2023-05-01'\n" +
                    "group by ds")
                    .nonMatch("test_partition_expr_mv1");
        }

        {
            sql("SELECT \n" +
                    "count(DISTINCT `order_id`) AS `order_num`, \n" +
                    "date_trunc('minute', `dt`) AS ds \n" +
                    "FROM `test_partition_expr_tbl1`\n" +
                    "WHERE `dt` BETWEEN '2023-04-11' AND '2023-04-12'\n" +
                    "group by ds")
                    .nonMatch("test_partition_expr_mv1");
        }

        starRocksAssert.dropMaterializedView("test_partition_expr_mv1");
        starRocksAssert.dropTable("test_partition_expr_tbl1");
    }

    @Test
    public void testMvRewriteForColumnReorder() throws Exception {
        {
            starRocksAssert.withMaterializedView("create materialized view mv0" +
                    " distributed by hash(t1a, t1b)" +
                    " as" +
                    " select sum(t1f) as total, t1a, t1b from test.test_all_type group by t1a, t1b;");
            String query = "select sum(t1f) as total, t1a, t1b from test.test_all_type group by t1a, t1b;";
            {
                sql(query, true).match("mv0")
                        .contains("1:t1a := 12:t1a\n" +
                                "            2:t1b := 14:t1b\n" +
                                "            11:sum := 13:total");
            }
            starRocksAssert.dropMaterializedView("mv0");
        }
    }

    @Test
    public void testRewriteWithCaseWhen() {
        starRocksAssert.withMaterializedView("create materialized view mv0" +
                " distributed by random" +
                " as select t1a, t1b, sum(t1f) as total from test.test_all_type group by t1a, t1b;", () -> {
            {
                String query = "select t1a, sum(if(t1b=0, t1f, 0)) as total from test.test_all_type group by t1a;";
                sql(query).notContain("mv0");
            }
            {
                String query = "select t1a, sum(if(t1b=0, t1b, 0)) as total from test.test_all_type group by t1a;";
                sql(query).notContain("mv0");
            }
            {
                String query = "select t1a, sum(if(murmur_hash3_32(t1b %3) = 0, t1b, 0)) as total from test.test_all_type group" +
                        " by t1a;";
                sql(query).notContain("mv0");
            }
            {
                String query = "select t1a, sum(case when(t1b=0) then t1b else 0 end) as total from test.test_all_type " +
                        "group by t1a;";
                sql(query).notContain("mv0");
            }
            {
                String query = "select t1a, sum(case when(t1b=0) then t1b else 0 end) as total from test.test_all_type " +
                        "group by t1a;";
                sql(query).notContain("mv0");
            }
        });
    }

    @Test
    public void testRewriteWithOnlyGroupByKeys() {
        starRocksAssert.withMaterializedView("create materialized view mv0" +
                " distributed by random" +
                " as select sum(t1f) as total, t1a, t1b from test.test_all_type group by t1a, t1b;", () -> {
            {
                String query = "select t1a, sum(t1b) as total from test.test_all_type group by t1a;";
                sql(query).nonMatch("mv0");
            }
            {
                String query = "select t1a, sum(t1b + 1) as total from test.test_all_type group by t1a;";
                sql(query).nonMatch("mv0");
            }
            {
                String query = "select t1a, max(t1b), sum(t1f) as total from test.test_all_type group by t1a;";
                sql(query).match("mv0");
            }
            {
                String query = "select t1a, max(concat(t1b,'.')), sum(t1f) as total from test.test_all_type group by t1a;";
                sql(query).match("mv0");
            }
            {
                String query = "select t1a, min(substr(t1b,1)), sum(t1f) as total from test.test_all_type group by t1a;";
                sql(query).match("mv0");
            }
            {
                String query = "select t1a, count(distinct t1b), sum(t1f) as total from test.test_all_type group by t1a;";
                sql(query).match("mv0");
            }
        });
    }

    @Test
    public void testMVRewriteWithNonDeterministicFunctions() {
        starRocksAssert.withMaterializedView("create materialized view mv0" +
                " distributed by random" +
                " as select current_date(), t1a, t1b from test.test_all_type ;", () -> {
            {
                String query = " select current_date(), t1a, t1b from test.test_all_type";
                sql(query).nonMatch("mv0");
            }
        });
    }

    @Test
    public void testRewriteWithEliminateJoinsBasic1() {
        starRocksAssert.withMaterializedView("create materialized view mv0" +
                " distributed by random" +
                " as select sum(t1f) as total, t1a, t1b from test.test_all_type group by t1a, t1b;", () -> {
            {
                String query = "select t1.t1b, sum(t1b) as total from test.test_all_type t1 " +
                        "join (select 'k1' as k1) t2 on t1.t1a=t2.k1 group by t1.t1b;";
                sql(query).match("mv0")
                        .contains("  1:Project\n" +
                                "  |  <slot 2> : 16: t1b\n" +
                                "  |  <slot 13> : sum(16: t1b)\n" +
                                "  |  \n" +
                                "  0:OlapScanNode\n" +
                                "     TABLE: mv0\n" +
                                "     PREAGGREGATION: ON\n" +
                                "     PREDICATES: 14: t1a = 'k1'");
            }
            {
                String query = "select t2.k1, t1.t1b, sum(t1f) as total from test.test_all_type t1 " +
                        "join (select 'k1' as k1) t2 on true group by t2.k1, t1.t1b;";
                sql(query).match("mv0")
                        .contains("  1:AGGREGATE (update serialize)\n" +
                                "  |  STREAMING\n" +
                                "  |  output: sum(15: total)\n" +
                                "  |  group by: 16: t1b\n" +
                                "  |  \n" +
                                "  0:OlapScanNode\n" +
                                "     TABLE: mv0");
            }
        });
    }

    @Test
    public void testRewriteWithEliminateJoinsBasic2() throws Exception {
       starRocksAssert.withTable("CREATE TABLE `tbl1` (\n" +
               "  `k1` date,\n" +
               "  `k2` decimal64(18, 2),\n" +
               "  `k3` varchar(255),\n" +
               "  `v1` varchar(255)\n" +
               ") ENGINE=OLAP \n" +
               "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
               "DISTRIBUTED BY RANDOM\n" +
               "PROPERTIES (\n" +
               "\"replication_num\" = \"1\"\n" +
               ");");
       starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `mv1` \n" +
               "DISTRIBUTED BY RANDOM\n" +
               "REFRESH ASYNC\n" +
               "PROPERTIES (\n" +
               "\"replication_num\" = \"1\"\n" +
               ")\n" +
               "AS SELECT k1, k2, k3, sum(v1) from tbl1 group by k1, k2, k3");
        {
            String sql = "with cte as(" +
                    "    select " +
                    "    '2024-07-20' as date1," +
                    "    date_add('2024-07-20', interval -1 month) AS start_date," +
                    "    date_add('2024-07-20', interval 1 month) AS end_date," +
                    "    'k3' as k3" +
                    ") select " +
                    "    cte.date1, cte.start_date, cte.end_date, t1.k1, t1.k2, t1.k3, sum(t1.v1) " +
                    "    from cte join tbl1 t1 " +
                    "    on cte.k3 = t1.k3 " +
                    "    group by cte.date1, cte.start_date, cte.end_date, t1.k1, t1.k2, t1.k3";
            sql(sql).contains("mv1")
                    .contains("  1:Project\n" +
                            "  |  <slot 2> : '2024-07-20'\n" +
                            "  |  <slot 3> : '2024-06-20 00:00:00'\n" +
                            "  |  <slot 4> : '2024-08-20 00:00:00'\n" +
                            "  |  <slot 6> : 11: k1\n" +
                            "  |  <slot 7> : 12: k2\n" +
                            "  |  <slot 8> : 13: k3\n" +
                            "  |  <slot 10> : 14: sum(v1)\n" +
                            "  |  \n" +
                            "  0:OlapScanNode\n" +
                            "     TABLE: mv1");
        }

        {
            String sql = "with cte as(" +
                    "    select " +
                    "    '2024-07-20' as date1," +
                    "    date_add('2024-07-20', interval -1 month) AS start_date," +
                    "    date_add('2024-07-20', interval 1 month) AS end_date," +
                    "    'k3' as k3" +
                    ") select " +
                    "    cte.date1, cte.start_date, cte.end_date, t1.k1, sum(t1.v1) " +
                    "    from cte join tbl1 t1 " +
                    "    on cte.k3 = t1.k3 " +
                    "    group by cte.date1, cte.start_date, cte.end_date, t1.k1";
            sql(sql).contains("mv1")
                    .contains("1:Project\n" +
                            "|  <slot 11> : col$: k1\n" +
                            "|  <slot 14> : col$: sum(v1)\n" +
                            "|\n" +
                            "0:OlapScanNode\n" +
                            "TABLE: mv1");
        }
        starRocksAssert.dropMaterializedView("mv1");
        starRocksAssert.dropTable("tbl1");
    }
}
