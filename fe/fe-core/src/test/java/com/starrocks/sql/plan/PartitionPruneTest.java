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

import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionPruneTest extends PlanTestBase {
    @BeforeAll
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

        starRocksAssert.withTable("CREATE TABLE `ptest_case` (\n"
                + "  `k1` int(11) NOT NULL COMMENT \"\",\n"
                + "  `d2` date    NULL COMMENT \"\",\n"
                + "  `v1` int(11) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `d2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`d2`)\n"
                + "(PARTITION P202001 VALUES [('0000-01-01'), ('2020-01-01')),\n"
                + "PARTITION P202004 VALUES [('2020-01-01'), ('2020-04-01')),\n"
                + "PARTITION P202007 VALUES [('2020-04-01'), ('2020-07-01')),\n"
                + "PARTITION P202012 VALUES [('2020-07-01'), ('2020-12-01')))\n"
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

        // date_trunc('month', hours_add(date_trunc('day', hours_sub(c1, 8)), 8))
        starRocksAssert.withTable("CREATE TABLE t_gen_col2 (" +
                " c1 datetime NOT NULL," +
                " c2 bigint," +
                " c3 DATETIME NULL AS date_trunc('month', hours_add(date_trunc('day', hours_sub(c1, 8)), 8)) " +
                " ) " +
                " DUPLICATE KEY(c1) " +
                " PARTITION BY (c2, c3) " +
                " PROPERTIES('replication_num'='1')");
        starRocksAssert.ddl("ALTER TABLE t_gen_col2 ADD PARTITION p1_202401 VALUES IN (('1', '2024-01-01'))");
        starRocksAssert.ddl("ALTER TABLE t_gen_col2 ADD PARTITION p1_202402 VALUES IN (('1', '2024-02-01'))");
        starRocksAssert.ddl("ALTER TABLE t_gen_col2 ADD PARTITION p1_202403 VALUES IN (('1', '2024-03-01'))");
        starRocksAssert.ddl("ALTER TABLE t_gen_col2 ADD PARTITION p2_202401 VALUES IN (('2', '2024-01-01'))");
        starRocksAssert.ddl("ALTER TABLE t_gen_col2 ADD PARTITION p2_202402 VALUES IN (('2', '2024-02-01'))");
        starRocksAssert.ddl("ALTER TABLE t_gen_col2 ADD PARTITION p2_202403 VALUES IN (('2', '2024-03-01'))");

        starRocksAssert.withTable("CREATE TABLE t_bool_partition (" +
                " c1 datetime NOT NULL, " +
                " c2 boolean" +
                " ) " +
                " PARTITION BY (c1, c2) " +
                " PROPERTIES('replication_num'='1')");

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
        starRocksAssert.getCtx().getSessionVariable().setEnableRewriteSimpleAggToMetaScan(false);
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
    public void testPartitionClauseCaseInsensitive() throws Exception {
        String sql = "select * from ptest_case partition(p202007)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "partitions=1/4");
    }

    private static Pair<ScalarOperator, LogicalScanOperator> buildConjunctAndScan(String sql) throws Exception {
        Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        ExecPlan execPlan = pair.second;
        System.out.println(execPlan.getExplainString(TExplainLevel.NORMAL));
        LogicalScanOperator scanOperator =
                (LogicalScanOperator) execPlan.getLogicalPlan().getRoot().inputAt(0).inputAt(0).inputAt(0).getOp();
        ScalarOperator predicate = execPlan.getPhysicalPlan().getOp().getPredicate();
        return Pair.create(predicate, scanOperator);
    }

    private void testRemovePredicate(String sql, String expected) throws Exception {
        Pair<ScalarOperator, LogicalScanOperator> pair = buildConjunctAndScan(sql);
        StatisticsCalculator calculator = new StatisticsCalculator();
        OptimizerContext context = OptimizerFactory.mockContext(new ColumnRefFactory());
        ScalarOperator newPredicate = calculator.removePartitionPredicate(pair.first, pair.second, context);
        Assertions.assertEquals(expected, newPredicate.toString());
    }

    private void testAssertContains(String sql, String expected) throws Exception {
        Pair<String, ExecPlan> pair = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        ExecPlan execPlan = pair.second;
        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
        PlanTestBase.assertContains(plan, expected);
    }

    @Test
    public void testGeneratedColumnPrune_RemovePredicate() throws Exception {
        testRemovePredicate("select * from t_gen_col where c1 = '2024-01-01' and c2 > 100", "true");
        testRemovePredicate("select * from t_gen_col where c1 >= '2024-01-01'  and c1 <= '2024-01-03' " +
                "and c2 > 100", "true");
        testRemovePredicate("select * from t_gen_col where c2 in (1, 2,3)", "true");
        testRemovePredicate("select * from t_gen_col where c2 = cast('123' as int)", "true");

        // bool partition column
        testRemovePredicate("select * from t_bool_partition where c2=true", "2: c2");
        testRemovePredicate("select * from t_bool_partition where c2=false", "true");

        // can not be removed
        testRemovePredicate("select * from t_gen_col where c1 = random() and c2 > 100",
                "cast(1: c1 as double) = random(1)");
        testRemovePredicate("select * from t_gen_col where c2 + 100 > c1 + 1",
                "cast(add(2: c2, 100) as double) > add(cast(1: c1 as double), 1)");
    }

    @Test
    public void testGeneratedColumnPrune_RemovePredicate2() throws Exception {
        testAssertContains("select * from t_gen_col2 where c1 >= '2024-02-02' ", "partitions=4/6");
        testAssertContains("select * from t_gen_col2 where c1 = '2024-02-02' ", "partitions=2/6");
        testAssertContains("select * from t_gen_col2 where c1 = '2024-02-02' and c2 > 100", "partitions=0/6");
        testAssertContains("select * from t_gen_col2 where c1 >= '2024-02-02'  and c1 <= '2024-02-03' " +
                "and c2 > 100", "partitions=0/6");
        testAssertContains("select * from t_gen_col2 where c2 in (1, 2,3)", "partitions=6/6");
        testAssertContains("select * from t_gen_col2 where c2 = cast('123' as int)", "partitions=0/6");

        // can not be removed
        testAssertContains("select * from t_gen_col2 where c1 = random() and c2 > 100",
                "partitions=0/6");
        testAssertContains("select * from t_gen_col2 where c2 + 100 > c1 + 1",
                "partitions=6/6");
    }

    @Test
    public void testGeneratedColumnPrune() throws Exception {
        // c2
        starRocksAssert.query("select count(*) from t_gen_col where c2 = 1 ")
                .explainContains("partitions=3/6");

        // c1
        starRocksAssert.query("select count(*) from t_gen_col where c1 = '2024-01-01' ")
                .explainContains("partitions=2/6");
        starRocksAssert.query("select count(*) from t_gen_col where c1 = '2024-02-01' ")
                .explainContains("partitions=2/6");
        starRocksAssert.query("select count(*) from t_gen_col where c1 < '2024-02-01' ")
                .explainContains("partitions=4/6");
        starRocksAssert.query("select count(*) from t_gen_col where c1 <= '2024-02-01' ")
                .explainContains("partitions=4/6");
        starRocksAssert.query("select count(*) from t_gen_col where c1 > '2024-02-01' ")
                .explainContains("partitions=4/6");
        starRocksAssert.query("select count(*) from t_gen_col where c1 >= '2024-02-01' ")
                .explainContains("partitions=4/6");
        starRocksAssert.query("select count(*) from t_gen_col where c1 in ('2024-02-01') ")
                .explainContains("partitions=2/6");
        starRocksAssert.query("select count(*) from t_gen_col where c1 in ('2024-02-01', '2024-01-01') ")
                .explainContains("partitions=4/6");
        starRocksAssert.query("select count(*) from t_gen_col where c1 in ('2027-01-01') ")
                .explainContains("partitions=0/6");

        // c1 not supported
        starRocksAssert.query("select count(*) from t_gen_col where c1 != '2024-02-01' ")
                .explainContains("partitions=6/6");
        starRocksAssert.query("select count(*) from t_gen_col where c1 = c2 ")
                .explainContains("partitions=6/6");
        starRocksAssert.query("select count(*) from t_gen_col where date_trunc('year', c1) = '2024-02-01' ")
                .explainContains("partitions=6/6");
        starRocksAssert.query("select count(*) from t_gen_col where date_trunc('year', c1) = '2024-02-01' ")
                .explainContains("partitions=6/6");

        // compound
        starRocksAssert.query("select count(*) from t_gen_col where c1 >= '2024-02-01' and c1 <= '2024-03-01' ")
                .explainContains("partitions=4/6");
        starRocksAssert.query("select count(*) from t_gen_col where c1 >= '2024-02-01' and c1 = '2027-03-01' ")
                .explainContains("partitions=0/6");
        starRocksAssert.query("select count(*) from t_gen_col where c1 = '2024-02-01' or c1 = '2024-03-01' ")
                .explainContains("partitions=4/6");
        starRocksAssert.query("select count(*) from t_gen_col where c1 = '2024-02-01' or c1 = '2027-03-01' ")
                .explainContains("partitions=2/6");

        // c1 && c2
        starRocksAssert.query("select * from t_gen_col where c1 = '2024-01-01' and c2 = 1 ")
                .explainContains("partitions=1/6");

        // non-monotonic function
        starRocksAssert.query("select count(*) from t_gen_col_1 where c1 = '2024-01-01' ")
                .explainContains("partitions=1/1");
    }

    @Test
    public void testGeneratedColumnPruneKeepsNullPartition() throws Exception {
        // from_unixtime is not defined for every bigint: from_unixtime(-1) is NULL, so a row with c1 = -1
        // lands in the NULL partition while still satisfying a predicate such as c1 < 1700000000. The
        // predicate deduced on the generated column says nothing about that partition, so it must not
        // prune it away.
        starRocksAssert.withTable("CREATE TABLE t_gen_col_null (" +
                " c1 bigint NOT NULL," +
                " c2 bigint," +
                " c3 varchar(64) NULL AS from_unixtime(c1) " +
                " ) " +
                " DUPLICATE KEY(c1) " +
                " PARTITION BY (c3) " +
                " PROPERTIES('replication_num'='1')");
        starRocksAssert.ddl("ALTER TABLE t_gen_col_null ADD PARTITION p202401 VALUES IN ('2024-01-01 00:00:00')");
        starRocksAssert.ddl("ALTER TABLE t_gen_col_null ADD PARTITION p202402 VALUES IN ('2024-02-01 00:00:00')");
        starRocksAssert.ddl("ALTER TABLE t_gen_col_null ADD PARTITION pnull VALUES IN (NULL)");

        // the deduced predicate matches no partition value, the NULL partition is still kept
        starRocksAssert.query("select count(*) from t_gen_col_null where c1 > 1707000000 ")
                .explainContains("partitions=1/3");
        starRocksAssert.query("select count(*) from t_gen_col_null where c1 < 1700000000 ")
                .explainContains("partitions=1/3");
        starRocksAssert.query("select count(*) from t_gen_col_null where c1 in (1706000000) ")
                .explainContains("partitions=1/3");
        // the deduced predicate still prunes the partitions it does describe
        starRocksAssert.query("select count(*) from t_gen_col_null where c1 > 1706000000 ")
                .explainContains("partitions=2/3");

        // a predicate written on the partition column itself is not a deduction: NULL is not greater
        // than any value, so the NULL partition is pruned exactly as before
        starRocksAssert.query("select count(*) from t_gen_col_null where c3 > '2024-01-23 00:00:00' ")
                .explainContains("partitions=1/3");
        starRocksAssert.query("select count(*) from t_gen_col_null where c3 is null ")
                .explainContains("partitions=1/3");
    }

    @Test
    public void testGeneratedColumnPruneSkipsNonOrderPreservingCast() throws Exception {
        // A varchar-to-bigint cast does not preserve the order: '99845' sorts after '998425506019'
        // as a string while 99845 is far below 998425506019 as a number. Mapping a range predicate
        // on c1 through that cast would prune the partition holding '99845' and lose the row.
        // getCallOperator() unwraps the enclosing cast, so the check has to look at the whole
        // expression, not at the call it digs out.
        starRocksAssert.withTable("CREATE TABLE t_gen_cast (" +
                " c1 varchar(64) NOT NULL," +
                " c2 bigint NULL AS cast(c1 as bigint) " +
                " ) " +
                " DUPLICATE KEY(c1) " +
                " PARTITION BY (c2) " +
                " PROPERTIES('replication_num'='1')");
        starRocksAssert.ddl("ALTER TABLE t_gen_cast ADD PARTITION p1 VALUES IN ('99845')");
        starRocksAssert.ddl("ALTER TABLE t_gen_cast ADD PARTITION p2 VALUES IN ('998425506019')");
        starRocksAssert.ddl("ALTER TABLE t_gen_cast ADD PARTITION p3 VALUES IN ('1234567')");

        starRocksAssert.query("select count(*) from t_gen_cast where c1 > '998425506019' ")
                .explainContains("partitions=3/3");
        starRocksAssert.query("select count(*) from t_gen_cast where c1 <= '998425506019' ")
                .explainContains("partitions=3/3");
    }

    @Test
    public void testRangeExprPruneSkipsNonMonotonicExpr() throws Exception {
        // The partition expression maps a varchar onto a bigint through substr() and a cast, and neither
        // step preserves the string order: '99845' sorts after '998425506019' while its partition value
        // (845) is far below the constant's (8425506019). Mapping a range predicate onto the partition
        // expression would prune p1 away and the rows in it would go silently missing, so a range
        // predicate must not prune at all here.
        starRocksAssert.withTable("CREATE TABLE `t_bill_detail` (\n" +
                "    `bill_code` varchar(200) NOT NULL DEFAULT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`bill_code`)\n" +
                "PARTITION BY RANGE(cast(substr(bill_code, 3, 11) as bigint))\n" +
                "(\n" +
                "    PARTITION p1 VALUES [(\"0\"), (\"5000000\")),\n" +
                "    PARTITION p2 VALUES [(\"20000000\"), (\"3021712368984\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`bill_code`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.query("select count(*) from t_bill_detail where bill_code > '998425506019' ")
                .explainContains("partitions=2/2");
        starRocksAssert.query("select count(*) from t_bill_detail where bill_code <= '998425506019' ")
                .explainContains("partitions=2/2");
        // equality maps soundly through any function -- a = c implies f(a) = f(c) -- and still prunes
        starRocksAssert.query("select count(*) from t_bill_detail where bill_code = '9984517' ")
                .explainContains("partitions=1/2");

        // a monotonic partition expression keeps pruning range predicates
        starRocksAssert.withTable("CREATE TABLE `t_daily_range` (\n" +
                "    `dt` datetime NOT NULL COMMENT \"\",\n" +
                "    `id` int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`)\n" +
                "PARTITION BY date_trunc('day', `dt`)(\n" +
                " START (\"2025-04-28\") END (\"2025-04-30\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
        starRocksAssert.query("select count(*) from t_daily_range where dt >= '2025-04-29 00:00:00' ")
                .explainContains("partitions=1/2");
    }

    @Test
    public void testMinMaxPrune_Check() throws Exception {
        starRocksAssert.withTable("create table t5_dup " +
                "(c1 datetime NOT NULL, c2 int) " +
                "duplicate key (c1) " +
                "partition by range(c1) ()" +
                "properties('replication_num'='1')");
        starRocksAssert.ddl("alter table t5_dup add partition p20240101 values less than('2024-01-01') ");
        starRocksAssert.ddl("alter table t5_dup add partition p20240102 values less than('2024-01-02') ");
        starRocksAssert.ddl("alter table t5_dup add partition p20240103 values less than('2024-01-03') ");
        starRocksAssert.ddl("alter table t5_dup add partition p20240104 values less than('2024-01-04') ");
        starRocksAssert.ddl("alter table t5_dup add partition p20240105 values less than('2024-01-05') ");

        // GROUP-BY
        starRocksAssert.query("select min(c1) from t5_dup group by c1 ")
                .explainContains("partitions=5/5");
        // HAVING
        starRocksAssert.query("select c1, min(c1) as m_c1 from t5_dup group by c1 having m_c1 > 1")
                .explainContains("partitions=5/5");
        // COUNT
        starRocksAssert.query("select count(c1) as m_c1 from t5_dup")
                .explainContains("partitions=5/5");
        // WHERE
        starRocksAssert.query("select min(c1) as m_c1 from t5_dup where c2 > 1")
                .explainContains("partitions=5/5");
        // SIMPLE AGG
        starRocksAssert.query("select min(c1-1)+1 from t5_dup")
                .explainContains("partitions=5/5");
        starRocksAssert.query("select min(c1 + c1) from t5_dup")
                .explainContains("partitions=5/5");
        starRocksAssert.query("select min(c1 + c2) from t5_dup")
                .explainContains("partitions=5/5");
        starRocksAssert.query("select min(c2) from t5_dup")
                .explainContains("partitions=5/5");
        starRocksAssert.query("select min(c1), min(c2) from t5_dup")
                .explainContains("partitions=5/5");
    }

    @Test
    public void testMinMaxPrune_NullValuePartition() throws Exception {
        // list partition
        starRocksAssert.withTable("create table t1_list " +
                "(c1 int, c2 int) " +
                "partition by (c1)" +
                "properties('replication_num'='1')");
        starRocksAssert.ddl("alter table t1_list add partition p4 values in ('4')");
        starRocksAssert.ddl("alter table t1_list add partition p3 values in ('3')");
        starRocksAssert.ddl("alter table t1_list add partition p2 values in ('2')");
        starRocksAssert.ddl("alter table t1_list add partition p1 values in ('1')");
        starRocksAssert.ddl("alter table t1_list add partition p0 values in (NULL)");
        {
            OlapTable t1 = (OlapTable) starRocksAssert.getTable("test", "t1_list");
            PartitionInfo partitionInfo = t1.getPartitionInfo();
            Set<Long> nullValuePartitions = partitionInfo.getNullValuePartitions();
            Assertions.assertEquals(1, nullValuePartitions.size());
        }

        // composite partition
        starRocksAssert.withTable("create table t3_composite " +
                "(c1 int, c2 int) " +
                "partition by (c1, c2)" +
                "properties('replication_num'='1')");
        starRocksAssert.ddl("alter table t3_composite add partition p1_1 values in (('1', '1'))");
        starRocksAssert.ddl("alter table t3_composite add partition p1_2 values in (('1', '2'))");
        starRocksAssert.ddl("alter table t3_composite add partition p2_1 values in (('5', '1'))");
        starRocksAssert.ddl("alter table t3_composite add partition p2_2 values in (('5', '2'))");
        {
            OlapTable t3 = (OlapTable) starRocksAssert.getTable("test", "t3_composite");
            PartitionInfo partitionInfo = t3.getPartitionInfo();

            Set<Long> nullValuePartitions = partitionInfo.getNullValuePartitions();
            Assertions.assertEquals(0, nullValuePartitions.size());

            starRocksAssert.ddl("alter table t3_composite add partition pnull values in ((NULL, NULL))");
            Assertions.assertEquals(1, partitionInfo.getNullValuePartitions().size());
        }

        // range
        starRocksAssert.withTable("create table t2_range " +
                "(c1 datetime, c2 int) " +
                "partition by range(c1) ()" +
                "properties('replication_num'='1')");
        starRocksAssert.ddl("alter table t2_range add partition p4 values less than ('2024-01-01')");
        starRocksAssert.ddl("alter table t2_range add partition p3 values less than ('2024-01-02')");
        starRocksAssert.ddl("alter table t2_range add partition p2 values less than ('2024-01-03')");
        starRocksAssert.ddl("alter table t2_range add partition p1 values less than ('2024-01-04')");
        {
            OlapTable t2 = (OlapTable) starRocksAssert.getTable("test", "t2_range");
            PartitionInfo partitionInfo = t2.getPartitionInfo();
            Set<Long> nullValuePartitions = partitionInfo.getNullValuePartitions();
            Assertions.assertEquals(1, nullValuePartitions.size());
        }

        starRocksAssert.dropTable("t1_list");
        starRocksAssert.dropTable("t2_range");
        starRocksAssert.dropTable("t3_composite");
    }

    @Test
    public void testMinMaxPrune_ListValues() throws Exception {
        UtFrameUtils.mockDML();
        // single-item list partition
        starRocksAssert.withTable("create table t1_list " +
                "(c1 int, c2 int) " +
                "partition by (c1)" +
                "properties('replication_num'='1')");
        starRocksAssert.ddl("alter table t1_list add partition p4 values in ('4')");
        starRocksAssert.ddl("alter table t1_list add partition p3 values in ('3')");
        starRocksAssert.ddl("alter table t1_list add partition p2 values in ('2')");
        starRocksAssert.ddl("alter table t1_list add partition p1 values in ('1')");
        starRocksAssert.getCtx().executeSql("insert into t1_list values(1, 1), (2, 2), (3, 3), (4, 4)");

        // LIST-PARTITION: MIN(partition_column)
        starRocksAssert.query("select max(c1) from t1_list")
                .explainContains("     constant exprs: \n         4\n");
        starRocksAssert.query("select min(c1) from t1_list")
                .explainContains("     constant exprs: \n         1\n");
        starRocksAssert.query("select min(c1), max(c1) from t1_list")
                .explainContains("     constant exprs: \n         1 | 4\n");
        starRocksAssert.query("select min(c1)+1, max(c1)-1 from t1_list")
                .explainContains("     constant exprs: \n         1 | 4\n");
        starRocksAssert.query("select min(c1-1)+1, max(c1-1)-1 from t1_list")
                .explainContains("OlapScanNode");

        // multi-values in a list
        starRocksAssert.withTable("create table t1_list_multi_values " +
                "(c1 int, c2 int) " +
                "partition by list(c1) (" +
                " partition p1 values in ('1', '10'), " +
                " partition p2 values in ('2', '9'), " +
                " partition p3 values in ('3', '8'), " +
                " partition p4 values in ('4', '5')" +
                ")" +
                "properties('replication_num'='1')");
        starRocksAssert.query("select min(c1) from t1_list_multi_values")
                .explainContains("     constant exprs: \n         1\n");
        starRocksAssert.query("select max(c1) from t1_list_multi_values")
                .explainContains("     constant exprs: \n         10\n");
        starRocksAssert.query("select min(c1), max(c1) from t1_list_multi_values")
                .explainContains("     constant exprs: \n         1 | 10\n");
        starRocksAssert.query("select min(c1)+1, max(c1)-1 from t1_list_multi_values")
                .explainContains("     constant exprs: \n         1 | 10\n");
        starRocksAssert.query("select min(c1-1)+1, max(c1-1)-1 from t1_list_multi_values")
                .explainContains("OlapScanNode");

        // multi-value partition doesn't support partition prune
        starRocksAssert.query("select * from t1_list_multi_values where c1 not in (10, 2, 8, 5)")
                .explainContains("partitions=4/4");

        starRocksAssert.query("select * from t1_list_multi_values where c1 != 10")
                .explainContains("partitions=4/4");

        // TODO: not supported
        // multi-item list partition
        starRocksAssert.withTable("create table t2_list " +
                "(c1 int, c2 int) " +
                "partition by (c1, c2)" +
                "properties('replication_num'='1')");
        starRocksAssert.ddl("alter table t2_list add partition p4 values in (('4', '4'))");
        starRocksAssert.ddl("alter table t2_list add partition p3 values in (('3', '3'))");
        starRocksAssert.ddl("alter table t2_list add partition p2 values in (('2', '2'))");
        starRocksAssert.ddl("alter table t2_list add partition p1 values in (('1', '2'))");
        starRocksAssert.query("select min(c1)+1, max(c1)-1 from t2_list")
                .explainContains("OlapScanNode");
    }

    @Test
    public void testMinMaxPrune_PartitionPrune() throws Exception {
        UtFrameUtils.mockDML();
        // single-item list partition
        starRocksAssert.withTable("create table t2_dup " +
                "(c1 datetime NOT NULL, c2 int) " +
                "duplicate key (c1) " +
                "partition by range(c1) ()" +
                "properties('replication_num'='1')");
        starRocksAssert.ddl("alter table t2_dup add partition p20240101 values less than('2024-01-01') ");
        starRocksAssert.ddl("alter table t2_dup add partition p20240102 values less than('2024-01-02') ");
        starRocksAssert.ddl("alter table t2_dup add partition p20240103 values less than('2024-01-03') ");
        starRocksAssert.ddl("alter table t2_dup add partition p20240104 values less than('2024-01-04') ");
        starRocksAssert.ddl("alter table t2_dup add partition p20240105 values less than('2024-01-05') ");

        starRocksAssert.query("select min(c1) from t2_dup").explainContains("partitions=2/5");
        starRocksAssert.query("select max(c1) from t2_dup").explainContains("partitions=1/5");
        starRocksAssert.query("select min(c1), max(c1) from t2_dup").explainContains("partitions=3/5");
        starRocksAssert.query("select min(c1)+1, max(c1)-1 from t2_dup").explainContains("partitions=3/5");
        starRocksAssert.query("select min(c1) from t2_dup limit 10").explainContains("partitions=2/5");

        // manually specify partition
        starRocksAssert.query("select min(c1) from t2_dup partition p20240101").explainContains("partitions=1/5");
        starRocksAssert.query("select max(c1) from t2_dup partition p20240101").explainContains("partitions=1/5");
        starRocksAssert.query("select min(c1) from t2_dup partition p20240105").explainContains("partitions=1/5");
        starRocksAssert.query("select max(c1) from t2_dup partition p20240105").explainContains("partitions=1/5");

        // NOT SUPPORTED for complicated MIN/MAX
        starRocksAssert.query("select min(c1-1)+1, max(c1+1)-1 from t2_dup").explainContains("partitions=5/5");

        // NOT SUPPORTED for filter
        starRocksAssert.query("select min(c1) from t2_dup where c2 > 1").explainContains("partitions=5/5");

        // NOT SUPPORTED for deletion
        starRocksAssert.getCtx().executeSql("delete from t2_dup where c1 = '2024-01-02' ");
        starRocksAssert.query("select min(c1) from t2_dup").explainContains("partitions=5/5");
    }

    @Test
    public void testMinMaxPrune_PrimaryKey() throws Exception {
        UtFrameUtils.mockDML();

        // single-item list partition
        starRocksAssert.withTable("create table t3_pri " +
                "(c1 datetime NOT NULL, c2 int) " +
                "primary key (c1) " +
                "partition by range(c1) ()" +
                "properties('replication_num'='1')");
        starRocksAssert.ddl("alter table t3_pri add partition p20240101 values less than('2024-01-01') ");
        starRocksAssert.ddl("alter table t3_pri add partition p20240102 values less than('2024-01-02') ");
        starRocksAssert.ddl("alter table t3_pri add partition p20240103 values less than('2024-01-03') ");
        starRocksAssert.ddl("alter table t3_pri add partition p20240104 values less than('2024-01-04') ");
        starRocksAssert.ddl("alter table t3_pri add partition p20240105 values less than('2024-01-05') ");

        starRocksAssert.query("select min(c1) from t3_pri")
                .explainContains("TOP-N", "order by: <slot 1> 1: c1", "AGGREGATE");
        starRocksAssert.query("select max(c1) from t3_pri")
                .explainContains("TOP-N", "order by: <slot 1> 1: c1 DESC", "AGGREGATE");
        starRocksAssert.query("select min(c1)+1 from t3_pri")
                .explainContains("TOP-N", "order by: <slot 1> 1: c1", "AGGREGATE");
        starRocksAssert.query("select max(c1)+1 from t3_pri")
                .explainContains("TOP-N", "order by: <slot 1> 1: c1 DESC", "AGGREGATE");

        // NOT SUPPORTED
        starRocksAssert.query("select max(c1-1)+1 from t3_pri").explainContains("OlapScanNode");
        starRocksAssert.query("select max(c1), min(c1) from t3_pri").explainContains("OlapScanNode");
    }

    @Test
    public void testMinMaxRangePartitionPruneWithEmptyPartition() throws Exception {
        UtFrameUtils.mockDML();
        starRocksAssert.withTable("CREATE TABLE `t4_range_minmax` (\n" +
                "    `dt` datetime NULL COMMENT \"\",\n" +
                "    `id` int(11) NULL COMMENT \"\",\n" +
                "    `name` varchar(65533) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`, `name`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(\n" +
                "    PARTITION p20250428 VALUES [(\"2025-04-28\"), (\"2025-04-29\")),\n" +
                "    PARTITION p20250429 VALUES [(\"2025-04-29\"), (\"2025-04-30\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`id`, `name`)\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.getCtx().executeSql("insert into t4_range_minmax values('2025-04-29', 1, 'bar')");
        try {
            FeConstants.runningUnitTest = false;
            starRocksAssert.getTable("test", "t4_range_minmax")
                    .getPartition("p20250428").getDefaultPhysicalPartition().updateVisibleVersion(1);
            starRocksAssert.getTable("test", "t4_range_minmax")
                    .getPartition("p20250428").getDefaultPhysicalPartition().setDataVersion(1);
            starRocksAssert.query("select min(dt), max(dt) from t4_range_minmax").explainContains("partitions=1/2");
        } finally {
            FeConstants.runningUnitTest = true;
        }
    }

    @Test
    public void testMinMaxRangePartitionPruneWithDateTruncPartition() throws Exception {
        UtFrameUtils.mockDML();
        starRocksAssert.withTable("CREATE TABLE `t4_expr_range_minmax` (\n" +
                "    `dt` datetime NULL COMMENT \"\",\n" +
                "    `id` int(11) NULL COMMENT \"\",\n" +
                "    `name` varchar(65533) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`, `name`)\n" +
                "PARTITION BY date_trunc('day', `dt`)(\n" +
                " START (\"2025-04-28\") END (\"2025-04-30\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`id`, `name`)\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.getCtx().executeSql("insert into t4_expr_range_minmax values('2025-04-29', 1, 'bar')");
        FeConstants.runningUnitTest = false;
        starRocksAssert.getTable("test", "t4_expr_range_minmax")
                .getPartition(ExpressionRangePartitionInfo.AUTOMATIC_SHADOW_PARTITION_NAME)
                .getDefaultPhysicalPartition()
                // after `alter table t4_expr_range_minmax modify column `id` varchar(11)`,
                // visible version of shadow partition will increase
                .updateVisibleVersion(2);
        // min(dt) should not be affected by shadow partition
        starRocksAssert.query("select min(dt) from t4_expr_range_minmax").explainContains("partitions=1/2");
        FeConstants.runningUnitTest = true;

    }

    @Test
    public void testMinMaxConstantWithRangePartition() throws Exception {
        UtFrameUtils.mockDML();
        starRocksAssert.withTable("CREATE TABLE `t4_range_minmax2` (\n" +
                "    `dt` date NULL COMMENT \"\",\n" +
                "    `id` int(11) NULL COMMENT \"\",\n" +
                "    `name` varchar(65533) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`, `name`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(\n" +
                "    PARTITION p20250428 VALUES [(\"2025-04-28\"), (\"2025-04-29\")),\n" +
                "    PARTITION p20250429 VALUES [(\"2025-04-29\"), (\"2025-04-30\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`id`, `name`)\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.getCtx().executeSql("insert into t4_range_minmax2 partition(p20250429) values('2025-04-29', 1, 'bar')");
        FeConstants.runningUnitTest = false;
        starRocksAssert.query("select min(dt) from t4_range_minmax2")
                .explainContains("     constant exprs: \n         '2025-04-29'\n");
        FeConstants.runningUnitTest = true;

    }

    @Test
    public void testMinMaxConstantWithDateTruncPartition() throws Exception {
        UtFrameUtils.mockDML();
        starRocksAssert.withTable("CREATE TABLE `t4_expr_range_minmax` (\n" +
                "    `dt` date NULL COMMENT \"\",\n" +
                "    `id` int(11) NULL COMMENT \"\",\n" +
                "    `name` varchar(65533) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`, `name`)\n" +
                "PARTITION BY date_trunc('day', `dt`)(\n" +
                " START (\"2025-04-28\") END (\"2025-04-30\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`id`, `name`)\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.getCtx()
                .executeSql("insert into t4_expr_range_minmax partition(p20250429) values('2025-04-29', 1, 'bar')");
        FeConstants.runningUnitTest = false;
        // there are 2 partitions: p20250428 and p20250429, p20250428 is empty
        starRocksAssert.query("select min(dt) from t4_expr_range_minmax")
                .explainContains("     constant exprs: \n         '2025-04-29'\n");
        starRocksAssert.dropTable("t4_expr_range_minmax");
        FeConstants.runningUnitTest = true;

    }

    @Test
    public void testMinMaxConstantWithDateTruncPartition2() throws Exception {
        UtFrameUtils.mockDML();
        starRocksAssert.withTable("CREATE TABLE `t4_expr_range_minmax` (\n" +
                "    `dt` date NULL COMMENT \"\",\n" +
                "    `id` int(11) NULL COMMENT \"\",\n" +
                "    `name` varchar(65533) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`, `name`)\n" +
                "PARTITION BY date_trunc('month', `dt`)(\n" +
                " START (\"2025-04-01\") END (\"2025-06-01\") EVERY (INTERVAL 1 MONTH)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`id`, `name`)\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.getCtx().executeSql("insert into t4_expr_range_minmax values('2025-04-29', 1, 'bar')");
        FeConstants.runningUnitTest = false;
        starRocksAssert.getTable("test", "t4_expr_range_minmax")
                .getPartition("p202505").getDefaultPhysicalPartition().updateVisibleVersion(1);
        // month partition should not produce constant result
        starRocksAssert.query("select min(dt), max(dt) from t4_expr_range_minmax").explainContains("partitions=1/2");
        FeConstants.runningUnitTest = true;
        starRocksAssert.dropTable("t4_expr_range_minmax");

    }

    @Test
    public void testMinMaxConstantWithDateTruncPartition3() throws Exception {
        UtFrameUtils.mockDML();
        starRocksAssert.withTable("CREATE TABLE `t4_expr_range_minmax` (\n" +
                "    `dt` datetime NULL COMMENT \"\",\n" +
                "    `id` int(11) NULL COMMENT \"\",\n" +
                "    `name` varchar(65533) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`, `name`)\n" +
                "PARTITION BY date_trunc('day', `dt`)(\n" +
                " START (\"2025-04-28\") END (\"2025-04-30\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`id`, `name`)\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.getCtx()
                .executeSql("insert into t4_expr_range_minmax partition(p20250429) values('2025-04-29', 1, 'bar')");
        FeConstants.runningUnitTest = false;
        // datetime partition column should not produce constant result
        starRocksAssert.query("select min(dt), max(dt) from t4_expr_range_minmax").explainContains("partitions=1/2");
        starRocksAssert.dropTable("t4_expr_range_minmax");
        FeConstants.runningUnitTest = true;

    }
}
