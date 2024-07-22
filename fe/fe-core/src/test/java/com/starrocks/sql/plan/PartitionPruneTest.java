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


package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class PartitionPruneTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("CREATE TABLE `ptest` (\n"
                + "  `k1` int(11) NOT NULL COMMENT \"\",\n"
                + "  `d2` date    NULL COMMENT \"\",\n"
                + "  `v1` int(11) NULL COMMENT \"\",\n"
                + "  `v2` int(11) NULL COMMENT \"\",\n"
                + "  `v3` int(11) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `d2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`d2`)\n"
                + "(PARTITION p202001 VALUES [('0000-01-01'), ('2020-01-01')),\n"
                + "PARTITION p202004 VALUES [('2020-01-01'), ('2020-04-01')),\n"
                + "PARTITION p202007 VALUES [('2020-04-01'), ('2020-07-01')),\n"
                + "PARTITION p202012 VALUES [('2020-07-01'), ('2020-12-01')))\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\"\n"
                + ");");

        // date_trunc('month', c1)
        starRocksAssert.withTable("CREATE TABLE t_gen_col (" +
                " c1 datetime NOT NULL," +
                " c2 bigint," +
                " c3 DATETIME NULL AS date_trunc('month', c1) " +
                " ) " +
                " DUPLICATE KEY(c1) " +
                " PARTITION BY (c2, c3) " +
                " PROPERTIES('replication_num'='1')");
        starRocksAssert.ddl("ALTER TABLE t_gen_col ADD PARTITION p1_202401 VALUES IN (('1', '2024-01-01'))");
        starRocksAssert.ddl("ALTER TABLE t_gen_col ADD PARTITION p1_202402 VALUES IN (('1', '2024-02-01'))");
        starRocksAssert.ddl("ALTER TABLE t_gen_col ADD PARTITION p1_202403 VALUES IN (('1', '2024-03-01'))");
        starRocksAssert.ddl("ALTER TABLE t_gen_col ADD PARTITION p2_202401 VALUES IN (('2', '2024-01-01'))");
        starRocksAssert.ddl("ALTER TABLE t_gen_col ADD PARTITION p2_202402 VALUES IN (('2', '2024-02-01'))");
        starRocksAssert.ddl("ALTER TABLE t_gen_col ADD PARTITION p2_202403 VALUES IN (('2', '2024-03-01'))");

        // year(c1)
        starRocksAssert.withTable("CREATE TABLE t_gen_col_1 (" +
                " c1 datetime NOT NULL," +
                " c2 bigint," +
                " c3 tinyint NULL AS month(c1) " +
                " ) " +
                " DUPLICATE KEY(c1) " +
                " PARTITION BY (c2, c3) " +
                " PROPERTIES('replication_num'='1')");
        starRocksAssert.ddl("ALTER TABLE t_gen_col_1 ADD PARTITION p1_01 VALUES IN (('1', '1'))");
    }

    @Test
    public void testPredicatePrune1() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 >= '2020-01-01';");
        assertTrue(sql.contains("     TABLE: ptest\n"
                + "     PREAGGREGATION: ON\n"
                + "     partitions=3/4\n"
                + "     rollup: ptest"));
    }

    @Test
    public void testPredicatePrune2() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 > '2020-01-01';");
        assertTrue(sql.contains("TABLE: ptest\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: d2 > '2020-01-01'\n" +
                "     partitions=3/4\n" +
                "     rollup: ptest"));
    }

    @Test
    public void testPredicatePrune3() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 >= '2020-01-01' and d2 <= '2020-07-01';");
        assertTrue(sql.contains("TABLE: ptest\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: 2: d2 <= '2020-07-01'\n"
                + "     partitions=3/4\n"
                + "     rollup: ptest"));
    }

    @Test
    public void testPredicatePrune4() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 >= '2020-01-01' and d2 < '2020-07-01';");
        assertTrue(sql.contains("     TABLE: ptest\n"
                + "     PREAGGREGATION: ON\n"
                + "     partitions=2/4\n"
                + "     rollup: ptest"));
    }

    @Test
    public void testPredicatePrune5() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 = '2020-08-01' and d2 < '2020-07-01';");
        assertTrue(sql.contains("  0:EMPTYSET\n"));
    }

    @Test
    public void testPredicatePrune6() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 = '2020-08-01' and d2 = '2020-09-01';");
        assertTrue(sql.contains("  0:EMPTYSET\n"));
    }

    @Test
    public void testPredicateEqPrune() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 = '2020-07-01'");
        assertTrue(sql.contains("  0:OlapScanNode\n" +
                "     TABLE: ptest\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: d2 = '2020-07-01'\n" +
                "     partitions=1/4\n" +
                "     rollup: ptest"));
    }

    @Test
    public void testPruneNullPredicate() throws Exception {
        String sql = "select * from ptest where (cast(d2 as int) / null) is null";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "partitions=4/4");

        sql = "select * from ptest where (cast(d2 as int) * null) <=> null";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "partitions=4/4");

        sql = "select * from ptest where d2 is null;";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "partitions=1/4");
    }

    @Test
    public void testInClauseCombineOr_1() throws Exception {
        String plan = getFragmentPlan("select * from ptest where (d2 > '1000-01-01') or (d2 in (null, '2020-01-01'));");
        assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: ptest\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (2: d2 > '1000-01-01') OR (2: d2 IN (NULL, '2020-01-01')), 2: d2 > '1000-01-01'\n" +
                "     partitions=4/4\n" +
                "     rollup: ptest"));
    }

    @Test
    public void testInClauseCombineOr_2() throws Exception {
        String plan = getFragmentPlan("select * from ptest where (d2 > '1000-01-01') or (d2 in (null, null));");
        assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: ptest\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (2: d2 > '1000-01-01') OR (2: d2 IN (NULL, NULL)), 2: d2 > '1000-01-01'\n" +
                "     partitions=4/4\n" +
                "     rollup: ptest"));
    }

    @Test
    public void testRightCastDatePrune() throws Exception {
        String sql = "select * from ptest where d2 <= '2020-05-01T13:45:57'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "partitions=3/4");
    }

    @Test
    public void testCastStringWithWhitSpace() throws Exception {
        String sql = "select * from ptest where cast('  111  ' as bigint) = k1";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "tabletRatio=4/40", "PREDICATES: 1: k1 = 111");

        sql = "select * from ptest where cast('  -111.12  ' as double) = k1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: CAST(1: k1 AS DOUBLE) = -111.12");

        sql = "select * from ptest where cast('  -111 2  ' as int) = k1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 1: k1 = CAST('  -111 2  ' AS INT)");
    }

    @Test
    public void testNullException() throws Exception {
        String sql = "select * from ptest partition(p202007) where d2 is null";
        String plan = getFragmentPlan(sql);
        assertCContains(plan, "partitions=0/4");
    }

    @Test
    public void testGeneratedColumnPrune() throws Exception {
        // c2
        starRocksAssert.query("select count(*) from t_gen_col where c2 = 1 ")
                .explainContains("partitions=3/7");

        // c1
        starRocksAssert.query("select count(*) from t_gen_col where c1 = '2024-01-01' ")
                .explainContains("partitions=2/7");
        starRocksAssert.query("select count(*) from t_gen_col where c1 = '2024-02-01' ")
                .explainContains("partitions=2/7");
        starRocksAssert.query("select count(*) from t_gen_col where c1 < '2024-02-01' ")
                .explainContains("partitions=4/7");
        starRocksAssert.query("select count(*) from t_gen_col where c1 <= '2024-02-01' ")
                .explainContains("partitions=4/7");
        starRocksAssert.query("select count(*) from t_gen_col where c1 > '2024-02-01' ")
                .explainContains("partitions=4/7");
        starRocksAssert.query("select count(*) from t_gen_col where c1 >= '2024-02-01' ")
                .explainContains("partitions=4/7");
        starRocksAssert.query("select count(*) from t_gen_col where c1 in ('2024-02-01') ")
                .explainContains("partitions=2/7");
        starRocksAssert.query("select count(*) from t_gen_col where c1 in ('2024-02-01', '2024-01-01') ")
                .explainContains("partitions=4/7");
        starRocksAssert.query("select count(*) from t_gen_col where c1 in ('2027-01-01') ")
                .explainContains("partitions=0/7");

        // c1 not supported
        starRocksAssert.query("select count(*) from t_gen_col where c1 != '2024-02-01' ")
                .explainContains("partitions=7/7");
        starRocksAssert.query("select count(*) from t_gen_col where c1 = c2 ")
                .explainContains("partitions=7/7");
        starRocksAssert.query("select count(*) from t_gen_col where date_trunc('year', c1) = '2024-02-01' ")
                .explainContains("partitions=7/7");
        starRocksAssert.query("select count(*) from t_gen_col where date_trunc('year', c1) = '2024-02-01' ")
                .explainContains("partitions=7/7");

        // compound
        starRocksAssert.query("select count(*) from t_gen_col where c1 >= '2024-02-01' and c1 <= '2024-03-01' ")
                .explainContains("partitions=4/7");
        starRocksAssert.query("select count(*) from t_gen_col where c1 >= '2024-02-01' and c1 = '2027-03-01' ")
                .explainContains("partitions=0/7");
        starRocksAssert.query("select count(*) from t_gen_col where c1 = '2024-02-01' or c1 = '2024-03-01' ")
                .explainContains("partitions=4/7");
        starRocksAssert.query("select count(*) from t_gen_col where c1 = '2024-02-01' or c1 = '2027-03-01' ")
                .explainContains("partitions=2/7");

        // c1 && c2
        starRocksAssert.query("select * from t_gen_col where c1 = '2024-01-01' and c2 = 1 ")
                .explainContains("partitions=1/7");

        // non-monotonic function
        starRocksAssert.query("select count(*) from t_gen_col_1 where c1 = '2024-01-01' ")
                .explainContains("partitions=2/2");
    }
}
