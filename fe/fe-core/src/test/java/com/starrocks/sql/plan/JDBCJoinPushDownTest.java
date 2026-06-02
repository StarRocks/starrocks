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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.connector.jdbc.MockedJDBCMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.CanPushDownPredicateVisitor;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorToJDBCSQLVisitor;
import com.starrocks.sql.optimizer.rule.transformation.JDBCJoinPushDownSQLBuilder;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JDBCJoinPushDownTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
    }

    @BeforeEach
    public void setUp() {
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(true);
    }

    @AfterEach
    public void tearDown() {
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(false);
    }

    private static int countOccurrences(String text, String pattern) {
        int count = 0;
        int fromIndex = 0;
        while (true) {
            int matchIndex = text.indexOf(pattern, fromIndex);
            if (matchIndex < 0) {
                return count;
            }
            count++;
            fromIndex = matchIndex + pattern.length();
        }
    }

    private static void assertContainsInOrder(String text, String... fragments) {
        int fromIndex = 0;
        for (String fragment : fragments) {
            int matchIndex = text.indexOf(fragment, fromIndex);
            Assertions.assertTrue(matchIndex >= 0,
                    "Expected fragment in order: " + fragment + "\nplan=\n" + text);
            fromIndex = matchIndex + fragment.length();
        }
    }

    private JDBCTable getMockedJDBCTable(String tableName) {
        return (JDBCTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(connectContext,
                MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME,
                MockedJDBCMetadata.MOCKED_PARTITIONED_DB_NAME,
                tableName);
    }

    private LogicalJDBCScanOperator newJDBCScan(JDBCTable table, long limit, ScalarOperator predicate,
                                                Map<ColumnRefOperator, Column> columnRefMap) {
        Map<Column, ColumnRefOperator> columnToRefMap = new HashMap<>();
        columnRefMap.forEach((colRef, column) -> columnToRefMap.put(column, colRef));
        return new LogicalJDBCScanOperator(table, columnRefMap, columnToRefMap, limit, predicate, null);
    }

    @Test
    public void testBasicTwoTableJoin() throws Exception {
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, countOccurrences(plan, "TABLE: (SELECT "));
        assertContains(plan,
                "TABLE: (SELECT ",
                "FROM (SELECT `a` FROM `tbl0` WHERE (`a` IS NOT NULL)) t0 "
                        + "INNER JOIN (SELECT `a`, `b` FROM `tbl1` WHERE (`a` IS NOT NULL)) t1 "
                        + "ON (t0.`a` = t1.`a`)) sr_merged",
                "QUERY: SELECT `c");
    }

    @Test
    public void testTwoTableJoinWithOuterLimit() throws Exception {
        // After PushDownJoinToJDBCRule merges the join into a derived JDBC scan, MERGE_LIMIT_RULES
        // folds the outer LIMIT 100 into the scan's limit field. JDBCScanNode.getJDBCQueryStr()
        // then emits "LIMIT 100" at the tail of the BE-side SQL — the LIMIT lands in the wrapped
        // subquery's outer SELECT (not inside the merged inner SQL).
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "limit 100";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, countOccurrences(plan, "TABLE: (SELECT "));
        assertContains(plan,
                "TABLE: (SELECT ",
                "FROM (SELECT `a` FROM `tbl0` WHERE (`a` IS NOT NULL)) t0 "
                        + "INNER JOIN (SELECT `a`, `b` FROM `tbl1` WHERE (`a` IS NOT NULL)) t1 "
                        + "ON (t0.`a` = t1.`a`)) sr_merged",
                ") sr_merged LIMIT 100");
    }

    @Test
    public void testTwoTableJoinWithCountStar() throws Exception {
        // count(*) doesn't reference any specific column. PruneScanColumnRule's "smallest column"
        // fallback after PushDownJoinToJDBCRule leaves the merged scan with a single column in
        // its external output, but the inner merged SQL still SELECTs both join keys (needed
        // for the ON clause). The count(*) aggregate stays as a local AGGREGATE above the JDBC
        // scan — there is no aggregation pushdown to JDBC.
        String sql = "select count(*) from jdbc0.partitioned_db0.tbl0 t1 "
                + "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, countOccurrences(plan, "TABLE: (SELECT "));
        // Merged scan: both atoms' join key visible in inner SQL.
        assertContains(plan,
                "FROM (SELECT `a` FROM `tbl0` WHERE (`a` IS NOT NULL)) t0 "
                        + "INNER JOIN (SELECT `a` FROM `tbl1` WHERE (`a` IS NOT NULL)) t1 "
                        + "ON (t0.`a` = t1.`a`)) sr_merged",
                "AGGREGATE",
                "count(*)");
        // count(*) is NOT pushed to JDBC — the BE-side SQL must not contain count.
        assertNotContains(plan, "QUERY: SELECT count");
    }

    @Test
    public void testThreeTableJoin() throws Exception {
        String sql = "select t1.a, t2.b, t3.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "join jdbc0.partitioned_db0.tbl2 t3 on t2.a = t3.a";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, countOccurrences(plan, "TABLE: (SELECT "));
        assertContainsInOrder(plan,
                "FROM (SELECT `a` FROM `tbl0` WHERE (`a` IS NOT NULL)) t0",
                "INNER JOIN (SELECT `a`, `b` FROM `tbl1` WHERE (`a` IS NOT NULL)) t1 ON (t0.`a` = t1.`a`)",
                "INNER JOIN (SELECT `a`, `c` FROM `tbl2` WHERE (`a` IS NOT NULL)) t2 ON (t1.`a` = t2.`a`)");
    }

    @Test
    public void testThreeTablePredicate() throws Exception {
        // PushDownJoinOnExpressionToChildProject rewrites t1.c + t2.c = t3.c into a helper
        // projection on the t1/t2 side. The t1/t2 subtree can still be partially pushed down,
        // but the helper expression must stay computable above the merged scan and the final
        // t3 join remains local.
        String sql = "select t1.a, t2.b, t3.a from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "join jdbc0.partitioned_db0.tbl2 t3 on t1.c + t2.c = t3.c";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, countOccurrences(plan, "TABLE: (SELECT "));
        assertContains(plan,
                "HASH JOIN",
                "CAST(c AS BIGINT) + CAST(c AS BIGINT)",
                "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`a` IS NOT NULL)) t0 "
                        + "INNER JOIN (SELECT `a`, `b`, `c` FROM `tbl1` WHERE (`a` IS NOT NULL)) t1 "
                        + "ON (t0.`a` = t1.`a`) AND ((t0.`c` + t1.`c`) IS NOT NULL)) sr_merged",
                "QUERY: SELECT `c1`, `c3`, `c6`, `c7`",
                "TABLE: `tbl2`");
    }

    @Test
    public void testMixedJoinJDBCAndNative() throws Exception {
        // Join between JDBC tables and a native StarRocks table - only JDBC part merges
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "join test_all_type t3 on t1.a = t3.t1a";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, countOccurrences(plan, "TABLE: (SELECT "));
        assertContains(plan, "OlapScanNode");
    }

    @Test
    public void testLeftJoinNotPushed() throws Exception {
        // LEFT JOIN should not be pushed down (MultiJoinNode only handles INNER/CROSS)
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "left join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "HASH JOIN");
        assertNotContains(plan, "sr_merged");
    }

    @Test
    public void testFeatureDisabledSessionVariable() throws Exception {
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(false);
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "sr_merged");
    }

    @Test
    public void testWithWherePredicates() throws Exception {
        String sql = "select t1.a, t1.c, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.c > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "sr_merged",
                "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` > 10)) t0");
    }

    @Test
    public void testPostgreSQLJoin() throws Exception {
        String sql = "select t1.a, t2.b from jdbc_postgres.partitioned_db0.tbl0 t1 " +
                "join jdbc_postgres.partitioned_db0.tbl1 t2 on t1.a = t2.a";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "TABLE: (SELECT ",
                "FROM (SELECT \"a\" FROM \"tbl0\" WHERE (\"a\" IS NOT NULL)) t0 "
                        + "INNER JOIN (SELECT \"a\", \"b\" FROM \"tbl1\" WHERE (\"a\" IS NOT NULL)) t1 "
                        + "ON (t0.\"a\" = t1.\"a\")) sr_merged",
                "QUERY: SELECT \"c");
    }

    @Test
    public void testMultipleGroupsBothMerged() throws Exception {
        // Two independent JDBC catalogs each having a 2-table join. Both groups should
        // produce their own merged scan (single transform, multiple pushdowns).
        String sql = "select m1.a, m1.c, p1.a, p1.c from " +
                "jdbc0.partitioned_db0.tbl0 m1 join jdbc0.partitioned_db0.tbl1 m2 on m1.a = m2.a " +
                "join jdbc_postgres.partitioned_db0.tbl0 p1 on m1.c = p1.c " +
                "join jdbc_postgres.partitioned_db0.tbl1 p2 on p1.a = p2.a";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(2, countOccurrences(plan, "TABLE: (SELECT "));
        assertContains(plan,
                "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` IS NOT NULL)) t0 "
                        + "INNER JOIN (SELECT `a` FROM `tbl1` WHERE (`a` IS NOT NULL)) t1 "
                        + "ON (t0.`a` = t1.`a`)) sr_merged",
                "FROM (SELECT \"a\", \"c\" FROM \"tbl0\" WHERE (\"a\" IS NOT NULL)) t0 "
                        + "INNER JOIN (SELECT \"a\" FROM \"tbl1\" WHERE (\"a\" IS NOT NULL)) t1 "
                        + "ON (t0.\"a\" = t1.\"a\")) sr_merged");
    }

    @Test
    public void testJoinWithPerTableLimit() throws Exception {
        // SQL subqueries with LIMIT still leave a global limit above the scan when
        // PushDownJoinToJDBCRule runs, so the planner path cannot reach this builder branch.
        JDBCTable table0 = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME0);
        JDBCTable table1 = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME1);
        ColumnRefOperator t0a = new ColumnRefOperator(10, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t1a = new ColumnRefOperator(11, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t1b = new ColumnRefOperator(12, VarcharType.VARCHAR, "b", true);

        Map<ColumnRefOperator, Column> t0Columns = new LinkedHashMap<>();
        t0Columns.put(t0a, table0.getColumn("a"));
        Map<ColumnRefOperator, Column> t1Columns = new LinkedHashMap<>();
        t1Columns.put(t1a, table1.getColumn("a"));
        t1Columns.put(t1b, table1.getColumn("b"));

        LogicalJDBCScanOperator t0Scan = newJDBCScan(table0, 10, null, t0Columns);
        LogicalJDBCScanOperator t1Scan = newJDBCScan(table1, -1, null, t1Columns);
        Map<ColumnRefOperator, String> qualifiedNames = new HashMap<>();
        qualifiedNames.put(t0a, "t0.`a`");
        qualifiedNames.put(t1a, "t1.`a`");
        qualifiedNames.put(t1b, "t1.`b`");
        JDBCJoinPushDownSQLBuilder sqlBuilder = new JDBCJoinPushDownSQLBuilder("`",
                List.of(new JDBCJoinPushDownSQLBuilder.TableEntry(table0, "t0", t0Scan),
                        new JDBCJoinPushDownSQLBuilder.TableEntry(table1, "t1", t1Scan)),
                qualifiedNames);

        String sql = sqlBuilder.build(List.of(t0a, t1b),
                List.of(new BinaryPredicateOperator(BinaryType.GT, t0a, t1a)),
                List.of());
        Assertions.assertEquals("SELECT t0.`a` AS c10, t1.`b` AS c12 "
                + "FROM (SELECT `a` FROM `tbl0` LIMIT 10) t0 "
                + "INNER JOIN `tbl1` t1 ON (t0.`a` > t1.`a`)", sql);
    }

    @Test
    public void testJoinWithLimitAndPredicate() throws Exception {
        JDBCTable table0 = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME0);
        JDBCTable table1 = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME1);
        ColumnRefOperator t0a = new ColumnRefOperator(10, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t0c = new ColumnRefOperator(11, IntegerType.INT, "c", true);
        ColumnRefOperator t1a = new ColumnRefOperator(12, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t1b = new ColumnRefOperator(13, VarcharType.VARCHAR, "b", true);

        Map<ColumnRefOperator, Column> t0Columns = new LinkedHashMap<>();
        t0Columns.put(t0a, table0.getColumn("a"));
        t0Columns.put(t0c, table0.getColumn("c"));
        Map<ColumnRefOperator, Column> t1Columns = new LinkedHashMap<>();
        t1Columns.put(t1a, table1.getColumn("a"));
        t1Columns.put(t1b, table1.getColumn("b"));

        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GT, t0c, ConstantOperator.createInt(5));
        LogicalJDBCScanOperator t0Scan = newJDBCScan(table0, 10, predicate, t0Columns);
        LogicalJDBCScanOperator t1Scan = newJDBCScan(table1, -1, null, t1Columns);
        Map<ColumnRefOperator, String> qualifiedNames = new HashMap<>();
        qualifiedNames.put(t0a, "t0.`a`");
        qualifiedNames.put(t0c, "t0.`c`");
        qualifiedNames.put(t1a, "t1.`a`");
        qualifiedNames.put(t1b, "t1.`b`");
        JDBCJoinPushDownSQLBuilder sqlBuilder = new JDBCJoinPushDownSQLBuilder("`",
                List.of(new JDBCJoinPushDownSQLBuilder.TableEntry(table0, "t0", t0Scan),
                        new JDBCJoinPushDownSQLBuilder.TableEntry(table1, "t1", t1Scan)),
                qualifiedNames);

        String sql = sqlBuilder.build(List.of(t0a, t1b),
                List.of(new BinaryPredicateOperator(BinaryType.GT, t0a, t1a)),
                List.of());
        Assertions.assertEquals("SELECT t0.`a` AS c10, t1.`b` AS c13 "
                + "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` > 5) LIMIT 10) t0 "
                + "INNER JOIN `tbl1` t1 ON (t0.`a` > t1.`a`)", sql);
    }

    @Test
    public void testBuilderBetweenPredicateWithoutNormalize() throws Exception {
        JDBCTable table0 = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME0);
        ColumnRefOperator t0c = new ColumnRefOperator(10, IntegerType.INT, "c", true);

        Map<ColumnRefOperator, Column> t0Columns = new LinkedHashMap<>();
        t0Columns.put(t0c, table0.getColumn("c"));
        Map<ColumnRefOperator, String> qualifiedNames = new HashMap<>();
        qualifiedNames.put(t0c, "t0.`c`");

        ScalarOperator betweenPredicate = new BetweenPredicateOperator(false, t0c,
                ConstantOperator.createInt(1), ConstantOperator.createInt(10));
        LogicalJDBCScanOperator betweenScan = newJDBCScan(table0, -1, betweenPredicate, t0Columns);
        JDBCJoinPushDownSQLBuilder betweenSqlBuilder = new JDBCJoinPushDownSQLBuilder("`",
                List.of(new JDBCJoinPushDownSQLBuilder.TableEntry(table0, "t0", betweenScan)),
                qualifiedNames);
        Assertions.assertEquals("SELECT t0.`c` AS c10 "
                        + "FROM (SELECT `c` FROM `tbl0` WHERE (`c` BETWEEN 1 AND 10)) t0",
                betweenSqlBuilder.build(List.of(t0c), List.of(), List.of()));

        ScalarOperator notBetweenPredicate = new BetweenPredicateOperator(true, t0c,
                ConstantOperator.createInt(1), ConstantOperator.createInt(10));
        LogicalJDBCScanOperator notBetweenScan = newJDBCScan(table0, -1, notBetweenPredicate, t0Columns);
        JDBCJoinPushDownSQLBuilder notBetweenSqlBuilder = new JDBCJoinPushDownSQLBuilder("`",
                List.of(new JDBCJoinPushDownSQLBuilder.TableEntry(table0, "t0", notBetweenScan)),
                qualifiedNames);
        Assertions.assertEquals("SELECT t0.`c` AS c10 "
                        + "FROM (SELECT `c` FROM `tbl0` WHERE (`c` NOT BETWEEN 1 AND 10)) t0",
                notBetweenSqlBuilder.build(List.of(t0c), List.of(), List.of()));
    }

    // -----------------------------------------------------------------------
    // PushDownJoinToJDBCRule: partitionPredicates — disqualification paths
    // -----------------------------------------------------------------------

    @Test
    public void testNonPushableOnPredicateDisqualifiesGroup() throws Exception {
        // abs() is not in JDBCJoinPushDownSQLBuilder's pushable function set, so the owning JDBC
        // group is disqualified and the optimizer keeps the local HASH JOIN.
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on abs(t1.c) = t2.c";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "HASH JOIN");
        assertNotContains(plan, "sr_merged");
    }

    @Test
    public void testNonPushableWherePredicateDisqualifiesGroup() throws Exception {
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where abs(t1.c) > 5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "HASH JOIN");
        assertNotContains(plan, "sr_merged");
    }

    @Test
    public void testNoCrossTableJoinPredicateNotMerged() throws Exception {
        // CROSS JOIN with only single-table WHERE filters. After MultiJoinNode flattening,
        // all predicates belong to a single table → joinPredicates bucket stays empty →
        // group is disqualified (line 368 of PushDownJoinToJDBCRule) → no push-down.
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "cross join jdbc0.partitioned_db0.tbl1 t2 " +
                "where t1.c > 5 and t2.c > 10";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "sr_merged");
    }

    @Test
    public void testSingleJdbcTableWithNativeTableNotMerged() throws Exception {
        // Only one JDBC atom is in the plan; the other table is a native OlapTable.
        // No JDBC group has >= 2 atoms, so mergeableGroups is empty → rule returns empty.
        String sql = "select t1.a from jdbc0.partitioned_db0.tbl0 t1 join t0 on t1.c = t0.v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN JDBC");
        assertNotContains(plan, "sr_merged");
    }

    @Test
    public void testDifferentCatalogsEachSingleTableNotMerged() throws Exception {
        // One JDBC table from each of two catalogs. Each catalog-group has only one atom
        // (< 2), so neither group is eligible → rule returns empty → local HASH JOIN.
        String sql = "select t1.a, t2.a from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc_postgres.partitioned_db0.tbl0 t2 on t1.a = t2.a";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "HASH JOIN");
        assertNotContains(plan, "sr_merged");
    }

    // -----------------------------------------------------------------------
    // JDBCJoinPushDownSQLBuilder: SQL generation — CROSS JOIN in pushed-down SQL
    // -----------------------------------------------------------------------

    @Test
    public void testCrossJoinInPushdownSQL() throws Exception {
        // Three JDBC tables: t1 CROSS JOIN t2 (no direct predicate between them), then t2
        // JOIN t3 ON t2.a = t3.a. After flattening, the only join predicate is t2.a=t3.a.
        // The SQL builder cannot attach a predicate to the t2 ON clause (t3 is not yet in
        // scope) → it emits "CROSS JOIN t2" and defers the predicate to t3's ON clause.
        String sql = "select t1.a, t2.b, t3.a from jdbc0.partitioned_db0.tbl0 t1 " +
                "cross join jdbc0.partitioned_db0.tbl1 t2 " +
                "join jdbc0.partitioned_db0.tbl2 t3 on t2.a = t3.a";
        String plan = getFragmentPlan(sql);
        assertContainsInOrder(plan,
                "FROM `tbl0` t0",
                "CROSS JOIN (SELECT `a`, `b` FROM `tbl1` WHERE (`a` IS NOT NULL)) t1",
                "INNER JOIN (SELECT `a` FROM `tbl2` WHERE (`a` IS NOT NULL)) t2 ON (t1.`a` = t2.`a`)");
    }

    // -----------------------------------------------------------------------
    // ScalarOperatorToJDBCSQLVisitor — predicate types in WHERE
    // -----------------------------------------------------------------------

    @Test
    public void testIsNullPredicatePushedDown() throws Exception {
        // visitIsNullPredicate: col + " IS NULL" → appears in the pushed-down WHERE clause
        String sql = "select t1.a, t1.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.b is null";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `b` FROM `tbl0` WHERE (`b` IS NULL)) t0");
    }

    @Test
    public void testIsNotNullPredicatePushedDown() throws Exception {
        // visitIsNullPredicate (isNotNull branch): col + " IS NOT NULL"
        String sql = "select t1.a, t1.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.b is not null";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `b` FROM `tbl0` WHERE (`b` IS NOT NULL)) t0");
    }

    @Test
    public void testInPredicatePushedDown() throws Exception {
        // visitInPredicate: col + " IN (...)" → appears in the pushed-down WHERE clause
        String sql = "select t1.a, t1.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.c in (1, 2, 3)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` IN (1, 2, 3))) t0");
    }

    @Test
    public void testNotInPredicatePushedDown() throws Exception {
        // visitInPredicate (isNotIn branch): col + " NOT IN (...)"
        String sql = "select t1.a, t1.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.c not in (1, 2, 3)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` NOT IN (1, 2, 3))) t0");
    }

    @Test
    public void testBetweenPredicatePushedDown() throws Exception {
        // SQL BETWEEN is normalized before JDBC SQL generation.
        String sql = "select t1.a, t1.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.c between 1 and 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` >= 1) AND (`c` <= 10)) t0");
    }

    @Test
    public void testNotBetweenPredicatePushedDown() throws Exception {
        // SQL NOT BETWEEN is normalized before JDBC SQL generation.
        String sql = "select t1.a, t1.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.c not between 1 and 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `c` FROM `tbl0` WHERE ((`c` < 1) OR (`c` > 10))) t0");
    }

    @Test
    public void testNotPredicatePushedDown() throws Exception {
        String sql = "select t1.a, t1.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where not (t1.c = 1)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` != 1)) t0");
    }

    @Test
    public void testNotNullSafeEqualPredicatePushedDown() throws Exception {
        String sql = "select t1.a, t1.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where not (t1.c <=> 1)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (NOT (`c` <=> 1))) t0");
    }

    @Test
    public void testMysqlConcatPredicatePushedDown() throws Exception {
        String sql = "select t1.a from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on concat(t1.a, t2.a, t1.b) = t2.b";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "sr_merged",
                "ON (CONCAT(t0.`a`, t1.`a`, t0.`b`) = t1.`b`)");
    }

    @Test
    public void testPostgresNullSafeEqualRendered() throws Exception {
        // <=> has no native operator on Postgres; the renderer must emit ANSI form.
        String sql = "select t1.a from jdbc_postgres.partitioned_db0.tbl0 t1 " +
                "join jdbc_postgres.partitioned_db0.tbl1 t2 on t1.a <=> t2.a";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "IS NOT DISTINCT FROM");
    }

    @Test
    public void testPostgresConcatPredicateNotPushedDown() throws Exception {
        // concat push-down is restricted to MySQL-compatible dialects (see
        // CanPushDownPredicateVisitor.visitCall): rendering concat correctly on Postgres
        // would require the single-table Expr→SQL path to also be dialect-aware, which it
        // is not. So the join here cannot be merged into a single JDBC query.
        String sql = "select t1.a from jdbc_postgres.partitioned_db0.tbl0 t1 " +
                "join jdbc_postgres.partitioned_db0.tbl1 t2 on concat(t1.a, t2.a, t1.b) = t2.b";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "sr_merged");
    }

    @Test
    public void testCanPushExpressionChecksFunctionArityAndCastDialect() {
        ColumnRefOperator varcharCol = new ColumnRefOperator(1, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator intCol = new ColumnRefOperator(2, IntegerType.INT, "c", true);

        // Arithmetic functions: must be binary, dialect-independent.
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(
                new CallOperator(FunctionSet.ADD, IntegerType.INT, List.of(intCol, intCol)),
                JDBCTable.ProtocolType.MYSQL));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(
                new CallOperator(FunctionSet.ADD, IntegerType.INT, List.of(intCol)),
                JDBCTable.ProtocolType.MYSQL));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(
                new CallOperator(FunctionSet.ADD, IntegerType.INT, List.of(intCol, intCol, intCol)),
                JDBCTable.ProtocolType.MYSQL));

        // concat: arity >= 2 only on MySQL-compatible dialects; everything else rejected.
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(
                new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR, List.of(varcharCol, varcharCol)),
                JDBCTable.ProtocolType.MYSQL));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(
                new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR, List.of(varcharCol, varcharCol)),
                JDBCTable.ProtocolType.MARIADB));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(
                new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR, List.of(varcharCol)),
                JDBCTable.ProtocolType.MYSQL));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(
                new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR,
                        List.of(varcharCol, varcharCol, varcharCol)),
                JDBCTable.ProtocolType.ORACLE));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(
                new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR, List.of(varcharCol, varcharCol)),
                JDBCTable.ProtocolType.POSTGRES));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(
                new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR, List.of(varcharCol, varcharCol)),
                JDBCTable.ProtocolType.UNKNOWN));

        // Cast: every dialect enforces the same 7-type whitelist via JDBCCastTypeMapper.
        // BIGINT is outside the whitelist, so non-implicit casts are rejected on all dialects.
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(
                new CastOperator(IntegerType.BIGINT, intCol, false), JDBCTable.ProtocolType.MYSQL));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(
                new CastOperator(IntegerType.BIGINT, intCol, false), JDBCTable.ProtocolType.POSTGRES));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(
                new CastOperator(IntegerType.BIGINT, intCol, true), JDBCTable.ProtocolType.MYSQL));
    }

    @Test
    public void testCanPushExpressionEqForNullDialectGate() {
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        BinaryPredicateOperator nullSafe = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, a, b);

        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(nullSafe, JDBCTable.ProtocolType.MYSQL));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(nullSafe, JDBCTable.ProtocolType.MARIADB));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(nullSafe, JDBCTable.ProtocolType.POSTGRES));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(nullSafe, JDBCTable.ProtocolType.CLICKHOUSE));
        // Oracle has no native null-safe equality and we don't synthesize the OR expansion.
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(nullSafe, JDBCTable.ProtocolType.ORACLE));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(nullSafe, JDBCTable.ProtocolType.UNKNOWN));
    }

    @Test
    public void testCanPushExpressionDivideRejectsPostgres() {
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        CallOperator divide = new CallOperator(FunctionSet.DIVIDE, IntegerType.INT, List.of(a, b));

        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(divide, JDBCTable.ProtocolType.MYSQL));
        // PG truncates int/int; the renderer strips implicit casts so the original int columns
        // would reach PG and silently produce wrong results — reject up front.
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(divide, JDBCTable.ProtocolType.POSTGRES));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(divide, JDBCTable.ProtocolType.ORACLE));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(divide, JDBCTable.ProtocolType.CLICKHOUSE));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(divide, JDBCTable.ProtocolType.UNKNOWN));
    }

    @Test
    public void testCanPushExpressionModRejectsOracle() {
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        CallOperator mod = new CallOperator(FunctionSet.MOD, IntegerType.INT, List.of(a, b));

        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(mod, JDBCTable.ProtocolType.MYSQL));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(mod, JDBCTable.ProtocolType.MARIADB));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(mod, JDBCTable.ProtocolType.POSTGRES));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(mod, JDBCTable.ProtocolType.CLICKHOUSE));
        // Single-table scan path renders mod as `%` via AstToStringBuilder, which Oracle rejects.
        // Until that path becomes dialect-aware, gate Oracle/UNKNOWN out.
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(mod, JDBCTable.ProtocolType.ORACLE));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(mod, JDBCTable.ProtocolType.UNKNOWN));
    }

    @Test
    public void testCanPushExpressionOracleConstantsAndInLimit() {
        ConstantOperator trueLit = ConstantOperator.createBoolean(true);
        // Oracle SQL has no BOOLEAN type at all — gate must reject boolean constants.
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(trueLit, JDBCTable.ProtocolType.ORACLE));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(trueLit, JDBCTable.ProtocolType.UNKNOWN));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(trueLit, JDBCTable.ProtocolType.MYSQL));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(trueLit, JDBCTable.ProtocolType.POSTGRES));

        // IN list of 1000 items is fine on Oracle; 1001 is not (ORA-01795).
        ColumnRefOperator intCol = new ColumnRefOperator(2, IntegerType.INT, "c", true);
        List<ScalarOperator> children1000 = new java.util.ArrayList<>();
        children1000.add(intCol);
        for (int i = 0; i < 1000; i++) {
            children1000.add(ConstantOperator.createInt(i));
        }
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(
                new InPredicateOperator(false, children1000), JDBCTable.ProtocolType.ORACLE));

        List<ScalarOperator> children1001 = new java.util.ArrayList<>(children1000);
        children1001.add(ConstantOperator.createInt(1001));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(
                new InPredicateOperator(false, children1001), JDBCTable.ProtocolType.ORACLE));
        // Same predicate is fine on the other dialects.
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(
                new InPredicateOperator(false, children1001), JDBCTable.ProtocolType.MYSQL));
    }

    @Test
    public void testCanPushExpressionCastWhitelistPerDialect() {
        ColumnRefOperator intCol = new ColumnRefOperator(1, IntegerType.INT, "c", true);
        // DATE is in the 7-type whitelist for every dialect → allowed.
        CastOperator toDate = new CastOperator(DateType.DATE, intCol, false);
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(toDate, JDBCTable.ProtocolType.MYSQL));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(toDate, JDBCTable.ProtocolType.POSTGRES));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(toDate, JDBCTable.ProtocolType.ORACLE));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(toDate, JDBCTable.ProtocolType.CLICKHOUSE));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(toDate, JDBCTable.ProtocolType.UNKNOWN));

        // JSON is excluded specifically on Oracle.
        CastOperator toJson = new CastOperator(JsonType.JSON, intCol, false);
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(toJson, JDBCTable.ProtocolType.MYSQL));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(toJson, JDBCTable.ProtocolType.POSTGRES));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(toJson, JDBCTable.ProtocolType.ORACLE));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(toJson, JDBCTable.ProtocolType.CLICKHOUSE));
    }

    @Test
    public void testRendererEqForNullDispatch() {
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        BinaryPredicateOperator nullSafe = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, a, b);
        Map<ColumnRefOperator, String> names = new HashMap<>();
        names.put(a, "`a`");
        names.put(b, "`b`");

        Assertions.assertEquals("(`a` <=> `b`)",
                nullSafe.accept(ScalarOperatorToJDBCSQLVisitor.forDialect(names, JDBCTable.ProtocolType.MYSQL), null));
        Assertions.assertEquals("(`a` IS NOT DISTINCT FROM `b`)",
                nullSafe.accept(ScalarOperatorToJDBCSQLVisitor.forDialect(names, JDBCTable.ProtocolType.POSTGRES), null));
        Assertions.assertEquals("(`a` <=> `b`)",
                nullSafe.accept(ScalarOperatorToJDBCSQLVisitor.forDialect(names, JDBCTable.ProtocolType.CLICKHOUSE), null));
    }

    @Test
    public void testRendererModRendersAsPercent() {
        // Gate restricts mod push-down to dialects where `%` is valid (no Oracle/UNKNOWN), so the
        // renderer emits the infix form uniformly.
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        CallOperator mod = new CallOperator(FunctionSet.MOD, IntegerType.INT, List.of(a, b));
        Map<ColumnRefOperator, String> names = new HashMap<>();
        names.put(a, "`a`");
        names.put(b, "`b`");

        Assertions.assertEquals("(`a` % `b`)",
                mod.accept(ScalarOperatorToJDBCSQLVisitor.forDialect(names, JDBCTable.ProtocolType.MYSQL), null));
        Assertions.assertEquals("(`a` % `b`)",
                mod.accept(ScalarOperatorToJDBCSQLVisitor.forDialect(names, JDBCTable.ProtocolType.POSTGRES), null));
        Assertions.assertEquals("(`a` % `b`)",
                mod.accept(ScalarOperatorToJDBCSQLVisitor.forDialect(names, JDBCTable.ProtocolType.CLICKHOUSE), null));
    }

    @Test
    public void testRendererCastTypePerDialect() {
        ColumnRefOperator c = new ColumnRefOperator(1, IntegerType.INT, "c", true);
        CastOperator toDate = new CastOperator(DateType.DATE, c, false);
        Map<ColumnRefOperator, String> names = new HashMap<>();
        names.put(c, "`c`");

        Assertions.assertEquals("CAST(`c` AS date)",
                toDate.accept(ScalarOperatorToJDBCSQLVisitor.forDialect(names, JDBCTable.ProtocolType.MYSQL), null));
        Assertions.assertEquals("CAST(`c` AS date)",
                toDate.accept(ScalarOperatorToJDBCSQLVisitor.forDialect(names, JDBCTable.ProtocolType.POSTGRES), null));
        Assertions.assertEquals("CAST(`c` AS DATE)",
                toDate.accept(ScalarOperatorToJDBCSQLVisitor.forDialect(names, JDBCTable.ProtocolType.ORACLE), null));
        Assertions.assertEquals("CAST(`c` AS Date)",
                toDate.accept(ScalarOperatorToJDBCSQLVisitor.forDialect(names, JDBCTable.ProtocolType.CLICKHOUSE), null));
    }

    @Test
    public void testRendererOracleDateLiteralWrapping() throws Exception {
        ConstantOperator date = ConstantOperator.createDate(
                java.time.LocalDateTime.of(2024, 1, 15, 0, 0, 0));
        ScalarOperatorToJDBCSQLVisitor oracle =
                ScalarOperatorToJDBCSQLVisitor.forDialect(new HashMap<>(), JDBCTable.ProtocolType.ORACLE);
        ScalarOperatorToJDBCSQLVisitor mysql =
                ScalarOperatorToJDBCSQLVisitor.forDialect(new HashMap<>(), JDBCTable.ProtocolType.MYSQL);

        // Oracle wraps in ANSI form so the literal is self-describing (NLS_DATE_FORMAT-safe).
        Assertions.assertTrue(date.accept(oracle, null).startsWith("DATE '"));
        // Other dialects keep the bare string-literal form.
        Assertions.assertTrue(date.accept(mysql, null).startsWith("'"));
    }

    @Test
    public void testRendererOracleImplicitCastDateWrapping() {
        // `dt = '2024-01-15'` on a DATE column reaches the renderer as an implicit cast
        // around a VARCHAR ConstantOperator; without special handling the cast is stripped
        // and Oracle would parse the bare string via NLS_DATE_FORMAT.
        ConstantOperator stringLiteral = ConstantOperator.createVarchar("2024-01-15");
        CastOperator implicitToDate = new CastOperator(DateType.DATE, stringLiteral, true);

        ScalarOperatorToJDBCSQLVisitor oracle =
                ScalarOperatorToJDBCSQLVisitor.forDialect(new HashMap<>(), JDBCTable.ProtocolType.ORACLE);
        ScalarOperatorToJDBCSQLVisitor mysql =
                ScalarOperatorToJDBCSQLVisitor.forDialect(new HashMap<>(), JDBCTable.ProtocolType.MYSQL);

        Assertions.assertEquals("DATE '2024-01-15'", implicitToDate.accept(oracle, null));
        Assertions.assertEquals("'2024-01-15'", implicitToDate.accept(mysql, null));
    }

    @Test
    public void testOrJoinPredicatePushedDown() throws Exception {
        // An OR compound predicate that is cross-table is classified as a join predicate.
        // JDBCJoinPushDownSQLBuilder pushes OR when all children are pushable, and
        // visitCompoundPredicate(OR) emits "(child1 OR child2)".
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on (t1.a = t2.a or t1.b = t2.b)";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "INNER JOIN `tbl1` t1 ON ((t0.`a` = t1.`a`) OR (t0.`b` = t1.`b`))");
    }

    // -----------------------------------------------------------------------
    // JDBCJoinPushDownSQLBuilder: static utility methods
    // -----------------------------------------------------------------------

    @Test
    public void testOutputColumnAlias() {
        Assertions.assertEquals("c0", JDBCJoinPushDownSQLBuilder.outputColumnAlias(0));
        Assertions.assertEquals("c1", JDBCJoinPushDownSQLBuilder.outputColumnAlias(1));
        Assertions.assertEquals("c42", JDBCJoinPushDownSQLBuilder.outputColumnAlias(42));
        Assertions.assertEquals("c100", JDBCJoinPushDownSQLBuilder.outputColumnAlias(100));
    }

    // -----------------------------------------------------------------------
    // PushDownJoinToJDBCRule: derived-table atoms (multi-stage pushdown)
    // -----------------------------------------------------------------------

    @Test
    public void testBuilderDerivedAtomNoLimit() throws Exception {
        // A derived-table atom — built by an earlier pushdown round — joined with a base atom.
        // No limit, no predicate on the derived scan → SQL builder takes the early-return path
        // and inlines as "(<inner>) t<alias>", peeling off the inner sr_merged via getPushDownQuery().
        JDBCTable primary = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME0);
        JDBCTable derived = new JDBCTable(primary);
        String innerSql = "SELECT t0.`a` AS c5 FROM `tbl0` t0 INNER JOIN `tbl1` t1 ON (t0.`a` = t1.`a`)";
        derived.setPushDownQuery(innerSql);

        // The derived scan exports c5 (matching the inner "AS c5") as its single output col.
        ColumnRefOperator c5Ref = new ColumnRefOperator(5, VarcharType.VARCHAR, "c5", true);
        Map<ColumnRefOperator, Column> derivedColumns = new LinkedHashMap<>();
        derivedColumns.put(c5Ref, new Column("c5", VarcharType.VARCHAR));
        LogicalJDBCScanOperator derivedScan = newJDBCScan(derived, -1, null, derivedColumns);

        JDBCTable base = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME2);
        ColumnRefOperator t1a = new ColumnRefOperator(20, VarcharType.VARCHAR, "a", true);
        Map<ColumnRefOperator, Column> baseColumns = new LinkedHashMap<>();
        baseColumns.put(t1a, base.getColumn("a"));
        LogicalJDBCScanOperator baseScan = newJDBCScan(base, -1, null, baseColumns);

        Map<ColumnRefOperator, String> qualifiedNames = new HashMap<>();
        qualifiedNames.put(c5Ref, "t0.`c5`");
        qualifiedNames.put(t1a, "t1.`a`");
        JDBCJoinPushDownSQLBuilder sqlBuilder = new JDBCJoinPushDownSQLBuilder("`",
                List.of(new JDBCJoinPushDownSQLBuilder.TableEntry(derived, "t0", derivedScan),
                        new JDBCJoinPushDownSQLBuilder.TableEntry(base, "t1", baseScan)),
                qualifiedNames);

        String sql = sqlBuilder.build(List.of(c5Ref, t1a),
                List.of(new BinaryPredicateOperator(BinaryType.EQ, c5Ref, t1a)),
                List.of());

        Assertions.assertEquals("SELECT t0.`c5` AS c5, t1.`a` AS c20 "
                + "FROM (" + innerSql + ") t0 "
                + "INNER JOIN `tbl2` t1 ON (t0.`c5` = t1.`a`)", sql);
    }

    @Test
    public void testBuilderDerivedAtomWithLimit() throws Exception {
        // Same derived atom, but the scan has a LIMIT (the MERGE_LIMIT_RULES pass after JDBC
        // pushdown can push an upstream Limit onto a derived scan). The early-return path no
        // longer applies; SQL builder falls into the shared wrapping path, which produces
        // "(SELECT cols FROM (<inner>) sr_merged LIMIT n) t<alias>". Note that the original
        // sr_merged alias on the derived survives here because the wrapping path uses
        // getCatalogTableName() as quotedTable — and the outer SELECT picks a different alias,
        // so the two don't collide.
        JDBCTable primary = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME0);
        JDBCTable derived = new JDBCTable(primary);
        String innerSql = "SELECT t0.`a` AS c5 FROM `tbl0` t0 INNER JOIN `tbl1` t1 ON (t0.`a` = t1.`a`)";
        derived.setPushDownQuery(innerSql);

        ColumnRefOperator c5Ref = new ColumnRefOperator(5, VarcharType.VARCHAR, "c5", true);
        Map<ColumnRefOperator, Column> derivedColumns = new LinkedHashMap<>();
        derivedColumns.put(c5Ref, new Column("c5", VarcharType.VARCHAR));
        LogicalJDBCScanOperator derivedScan = newJDBCScan(derived, 10, null, derivedColumns);

        JDBCTable base = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME2);
        ColumnRefOperator t1a = new ColumnRefOperator(20, VarcharType.VARCHAR, "a", true);
        Map<ColumnRefOperator, Column> baseColumns = new LinkedHashMap<>();
        baseColumns.put(t1a, base.getColumn("a"));
        LogicalJDBCScanOperator baseScan = newJDBCScan(base, -1, null, baseColumns);

        Map<ColumnRefOperator, String> qualifiedNames = new HashMap<>();
        qualifiedNames.put(c5Ref, "t0.`c5`");
        qualifiedNames.put(t1a, "t1.`a`");
        JDBCJoinPushDownSQLBuilder sqlBuilder = new JDBCJoinPushDownSQLBuilder("`",
                List.of(new JDBCJoinPushDownSQLBuilder.TableEntry(derived, "t0", derivedScan),
                        new JDBCJoinPushDownSQLBuilder.TableEntry(base, "t1", baseScan)),
                qualifiedNames);

        String sql = sqlBuilder.build(List.of(c5Ref, t1a),
                List.of(new BinaryPredicateOperator(BinaryType.EQ, c5Ref, t1a)),
                List.of());

        Assertions.assertEquals("SELECT t0.`c5` AS c5, t1.`a` AS c20 "
                + "FROM (SELECT `c5` FROM (" + innerSql + ") sr_merged LIMIT 10) t0 "
                + "INNER JOIN `tbl2` t1 ON (t0.`c5` = t1.`a`)", sql);
    }

    @Test
    public void testQueryTableAtomBypassesMerging() throws Exception {
        // One side of the JOIN is a JDBC native_query (queryTable). Step 3 filter excludes any
        // group containing a queryTable atom, so the rule must not merge — plan stays as local
        // HASH JOIN with two separate JDBC scans, neither wrapped in sr_merged.
        String sql = "select t1.a, q.a from jdbc0.partitioned_db0.tbl0 t1 "
                + "join table(jdbc0.native_query('select a from remote_table')) q on t1.a = q.a";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "sr_merged");
        assertContains(plan, "HASH JOIN");
    }
}
