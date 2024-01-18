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
    public static void setUp() throws Exception {
        MaterializedViewTestBase.setUp();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);
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
        String tableSQL = "CREATE TABLE `test_partition_expr_tbl1` (\n" +
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
        String mv = "CREATE MATERIALIZED VIEW `test_partition_expr_mv1`\n" +
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
                "FROM `test_partition_expr_tbl1`\n" +
                "group by dt, k1;";
        starRocksAssert.withMaterializedView(mv);
        sql("select dt, count(distinct order_id) " +
                "from test_partition_expr_tbl1 group by dt")
                .nonMatch("test_partition_expr_mv1");
        starRocksAssert.dropMaterializedView("test_partition_expr_mv1");
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
<<<<<<< HEAD

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
    public void testDateTruncPartitionColumnExpr() throws Exception {
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
                "PARTITION BY ds \n" +
                "DISTRIBUTED BY HASH(`order_num`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ")\n" +
                "AS\n" +
                "SELECT \n" +
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
=======
>>>>>>> branch-2.5-mrs
}
