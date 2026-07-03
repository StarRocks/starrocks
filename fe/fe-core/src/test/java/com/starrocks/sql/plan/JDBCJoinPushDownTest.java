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
import com.starrocks.connector.jdbc.JDBCPushDownSQLBuilder;
import com.starrocks.connector.jdbc.MockedJDBCMetadata;
import com.starrocks.connector.jdbc.ScalarOperatorToJDBCSQLVisitor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.JoinOperator;
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
                "FROM (SELECT `a` FROM `tbl0` WHERE (`a` IS NOT NULL)) sr_t0"
                        + " INNER JOIN (SELECT `a`, `b` FROM `tbl1` WHERE (`a` IS NOT NULL)) sr_t1 "
                        + "ON (sr_t0.`a` = sr_t1.`a`)) sr_inline",
                "QUERY: SELECT `sr_c");
    }

    @Test
    public void testPredicateOnAbsorbedProjectionMergesAllTables() throws Exception {
        // The middle projections (t1.c + 1 AS s, t3.c + 1 AS w — single-child expressions are
        // the ones MultiJoinNode absorbs into expressionMap during flattening; a two-child
        // expression stops flattening instead) leave the top ON referencing refs no atom
        // outputs. The rule must re-expand such predicates down to atom columns before
        // grouping/routing — otherwise the predicate can't connect the two subquery groups
        // and the join degrades to two partial merges plus a local HASH JOIN.
        String sql = "select v.s from (select t1.c + 1 as s from jdbc0.partitioned_db0.tbl0 t1 "
                + "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a) v "
                + "join (select t3.c + 1 as w from jdbc0.partitioned_db0.tbl2 t3 "
                + "join jdbc0.partitioned_db0.tbl3 t4 on t3.a = t4.a) u on v.s = u.w";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, countOccurrences(plan, "TABLE: (SELECT "));
        assertNotContains(plan, "HASH JOIN");
        assertContains(plan, "sr_t0.`c` + 1");
        assertContains(plan, "sr_t2.`c` + 1");
    }

    @Test
    public void testPerTableLimitScanNotMerged() throws Exception {
        // A JDBC scan carrying a row limit is not a bare merge atom (the limit sits above it until
        // after join pushdown), so it is not merged — it stays a standalone scan whose limit reaches
        // the BE as the scan's limit field (rendered dialect-aware there), not baked into merged SQL.
        String sql = "select t1.a, t2.b from (select a from jdbc0.partitioned_db0.tbl0 limit 5) t1 "
                + "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "sr_inline");
        assertContains(plan, "QUERY: SELECT `a` FROM `tbl0` LIMIT 5");
    }

    @Test
    public void testProbeLimitedScanRightSide() throws Exception {
        // Symmetric to testPerTableLimitScanNotMerged: a row-limited scan on the probe (right)
        // side is not a bare merge atom, so the group is not merged; its LIMIT reaches the BE as
        // the scan's own limit.
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 "
                + "join (select a, b from jdbc0.partitioned_db0.tbl1 limit 5) t2 on t1.a = t2.a";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "sr_inline");
        assertContains(plan, "QUERY: SELECT `a`, `b` FROM `tbl1` LIMIT 5");
    }

    @Test
    public void testTwoTableJoinWithOuterLimit() throws Exception {
        // After the join merges into a derived JDBC scan, the join's row limit is carried onto the
        // merged scan (PushDownJoinToJDBCRule.transform, Step 7) and rendered onto the BE-side SQL.
        // The global LIMIT stays local above the scan as the authoritative trim.
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "limit 100";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, countOccurrences(plan, "TABLE: (SELECT "));
        assertContains(plan,
                "FROM (SELECT `a` FROM `tbl0` WHERE (`a` IS NOT NULL)) sr_t0"
                        + " INNER JOIN (SELECT `a`, `b` FROM `tbl1` WHERE (`a` IS NOT NULL)) sr_t1 "
                        + "ON (sr_t0.`a` = sr_t1.`a`)) sr_inline");
        assertContains(plan, ") sr_inline LIMIT 100");
    }

    @Test
    public void testTwoTableJoinWithCountStar() throws Exception {
        // count(*) doesn't reference any specific column. PruneScanColumnRule's "smallest column"
        // fallback after PushDownJoinToJDBCRule leaves the merged scan with a single column in
        // its external output, but the inner merged SQL still SELECTs both join keys (needed
        // for the remote WHERE clause). The count(*) aggregate stays as a local AGGREGATE above the JDBC
        // scan — there is no aggregation pushdown to JDBC.
        String sql = "select count(*) from jdbc0.partitioned_db0.tbl0 t1 "
                + "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, countOccurrences(plan, "TABLE: (SELECT "));
        // Merged scan: both atoms' join key visible in inner SQL.
        assertContains(plan,
                "FROM (SELECT `a` FROM `tbl0` WHERE (`a` IS NOT NULL)) sr_t0"
                        + " INNER JOIN (SELECT `a` FROM `tbl1` WHERE (`a` IS NOT NULL)) sr_t1 "
                        + "ON (sr_t0.`a` = sr_t1.`a`)) sr_inline",
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
                "FROM (SELECT `a` FROM `tbl0` WHERE (`a` IS NOT NULL)) sr_t0",
                "INNER JOIN (SELECT `a`, `b` FROM `tbl1` WHERE (`a` IS NOT NULL)) sr_t1 ON (sr_t0.`a` = sr_t1.`a`)",
                "INNER JOIN (SELECT `a`, `c` FROM `tbl2` WHERE (`a` IS NOT NULL)) sr_t2 ON (sr_t1.`a` = sr_t2.`a`)");
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
                "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`a` IS NOT NULL)) sr_t0"
                        + " INNER JOIN (SELECT `a`, `b`, `c` FROM `tbl1` WHERE (`a` IS NOT NULL)) sr_t1 "
                        + "ON (sr_t0.`a` = sr_t1.`a`) AND ((sr_t0.`c` + sr_t1.`c`) IS NOT NULL)) sr_inline",
                "QUERY: SELECT `sr_c1`, `sr_c3`, `sr_c6`, `sr_c7`",
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
        assertNotContains(plan, "sr_inline");
    }

    @Test
    public void testFeatureDisabledSessionVariable() throws Exception {
        connectContext.getSessionVariable().setEnableJdbcJoinPushDown(false);
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "sr_inline");
    }

    @Test
    public void testWithWherePredicates() throws Exception {
        String sql = "select t1.a, t1.c, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.c > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "sr_inline",
                "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` > 10)) sr_t0");
    }

    @Test
    public void testFilterOnUnselectedColumnPushedDown() throws Exception {
        // The WHERE column is filtered remotely but not selected, which leaves a pure
        // column-pruning projection on the scan; that must not block the merge. The filter
        // renders inside the per-table subquery, and the merged SELECT must not fetch the
        // filter column back.
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.c > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "sr_inline",
                "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` > 10)) sr_t0");
        assertNotContains(plan, "HASH JOIN");
        assertNotContains(plan, "sr_t0.`c` AS");
    }

    @Test
    public void testHiddenFilterColumnNotFetchedWhenRootHasNoProjection() throws Exception {
        // Selecting every visible column makes the final identity Project collapse, so the
        // matched join root carries no projection and Step 4 takes the
        // input.getOutputColumns() path. t1.c is consumed only by the remote WHERE and is
        // hidden behind the scan's pruning projection — it must not be fetched back even
        // though it is still present in the scan's colRefToColumnMetaMap.
        String sql = "select t1.a, t1.b, t1.d, t2.a, t2.b, t2.c, t2.d " +
                "from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.c > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "sr_inline",
                "FROM `tbl0` WHERE (`c` > 10)");
        assertNotContains(plan, "HASH JOIN");
        // tbl0's filter-only column c stays remote; tbl1 (alias sr_t1) legitimately ships its c.
        assertNotContains(plan, "sr_t0.`c` AS");
        assertContains(plan, "sr_t1.`c` AS");
    }

    @Test
    public void testPostgreSQLJoin() throws Exception {
        String sql = "select t1.a, t2.b from jdbc_postgres.partitioned_db0.tbl0 t1 " +
                "join jdbc_postgres.partitioned_db0.tbl1 t2 on t1.a = t2.a";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "TABLE: (SELECT ",
                "FROM (SELECT \"a\" FROM \"tbl0\" WHERE (\"a\" IS NOT NULL)) sr_t0"
                        + " INNER JOIN (SELECT \"a\", \"b\" FROM \"tbl1\" WHERE (\"a\" IS NOT NULL)) sr_t1 "
                        + "ON (sr_t0.\"a\" = sr_t1.\"a\")) sr_inline",
                "QUERY: SELECT \"sr_c");
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
                "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` IS NOT NULL)) sr_t0"
                        + " INNER JOIN (SELECT `a` FROM `tbl1` WHERE (`a` IS NOT NULL)) sr_t1 "
                        + "ON (sr_t0.`a` = sr_t1.`a`)) sr_inline",
                "FROM (SELECT \"a\", \"c\" FROM \"tbl0\" WHERE (\"a\" IS NOT NULL)) sr_t0"
                        + " INNER JOIN (SELECT \"a\" FROM \"tbl1\" WHERE (\"a\" IS NOT NULL)) sr_t1 "
                        + "ON (sr_t0.\"a\" = sr_t1.\"a\")) sr_inline");
    }

    @Test
    public void testJoinWithPerTablePredicate() throws Exception {
        // A per-table predicate makes the scan render as a derived subquery.
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
        LogicalJDBCScanOperator t0Scan = newJDBCScan(table0, -1, predicate, t0Columns);
        LogicalJDBCScanOperator t1Scan = newJDBCScan(table1, -1, null, t1Columns);
        String sql = JDBCPushDownSQLBuilder.buildJoinQuery(List.of(t0Scan, t1Scan),
                List.of(t0a, t1b),
                List.of(new BinaryPredicateOperator(BinaryType.GT, t0a, t1a)));
        Assertions.assertEquals("SELECT sr_t0.`a` AS sr_c10, sr_t1.`b` AS sr_c13 "
                + "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` > 5)) sr_t0"
                + ", `tbl1` sr_t1 WHERE (sr_t0.`a` > sr_t1.`a`)", sql);
    }

    @Test
    public void testBuilderInnerJoinExplicitOn() throws Exception {
        // The typed overload renders an explicit "INNER JOIN ... ON ..." chain instead of a comma
        // join. The cross-table predicate becomes the ON clause; per-table predicates stay inside
        // each scan's derived subquery.
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

        LogicalJDBCScanOperator t0Scan = newJDBCScan(table0, -1, null, t0Columns);
        LogicalJDBCScanOperator t1Scan = newJDBCScan(table1, -1, null, t1Columns);
        String sql = JDBCPushDownSQLBuilder.buildJoinQuery(List.of(t0Scan, t1Scan),
                List.of(t0a, t1b), JoinOperator.INNER_JOIN,
                List.of(new BinaryPredicateOperator(BinaryType.EQ, t0a, t1a)),
                List.of());
        Assertions.assertEquals("SELECT sr_t0.`a` AS sr_c10, sr_t1.`b` AS sr_c12 "
                + "FROM `tbl0` sr_t0 INNER JOIN `tbl1` sr_t1 ON (sr_t0.`a` = sr_t1.`a`)", sql);
    }

    @Test
    public void testBuilderLeftOuterJoinPreservesOrder() throws Exception {
        // For an order-sensitive LEFT OUTER JOIN the scans are emitted in the given order with an
        // explicit ON clause; the join condition must not be demoted to WHERE (that would turn the
        // outer join into an inner one). A post-join filter goes to the trailing WHERE.
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

        LogicalJDBCScanOperator t0Scan = newJDBCScan(table0, -1, null, t0Columns);
        LogicalJDBCScanOperator t1Scan = newJDBCScan(table1, -1, null, t1Columns);
        String sql = JDBCPushDownSQLBuilder.buildJoinQuery(List.of(t0Scan, t1Scan),
                List.of(t0a, t1b), JoinOperator.LEFT_OUTER_JOIN,
                List.of(new BinaryPredicateOperator(BinaryType.EQ, t0a, t1a)),
                List.of(new BinaryPredicateOperator(BinaryType.GT, t1b, ConstantOperator.createVarchar("x"))));
        Assertions.assertEquals("SELECT sr_t0.`a` AS sr_c10, sr_t1.`b` AS sr_c12 "
                + "FROM `tbl0` sr_t0 LEFT OUTER JOIN `tbl1` sr_t1 ON (sr_t0.`a` = sr_t1.`a`) "
                + "WHERE (sr_t1.`b` > 'x')", sql);
    }

    @Test
    public void testBuilderOuterJoinStepWithoutOnPredicateRejected() throws Exception {
        // An outer-join chain step with no ON predicate is invalid SQL and changes semantics, so
        // the builder refuses to render it.
        JDBCTable table0 = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME0);
        JDBCTable table1 = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME1);
        ColumnRefOperator t0a = new ColumnRefOperator(10, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t1a = new ColumnRefOperator(11, VarcharType.VARCHAR, "a", true);

        Map<ColumnRefOperator, Column> t0Columns = new LinkedHashMap<>();
        t0Columns.put(t0a, table0.getColumn("a"));
        Map<ColumnRefOperator, Column> t1Columns = new LinkedHashMap<>();
        t1Columns.put(t1a, table1.getColumn("a"));

        LogicalJDBCScanOperator t0Scan = newJDBCScan(table0, -1, null, t0Columns);
        LogicalJDBCScanOperator t1Scan = newJDBCScan(table1, -1, null, t1Columns);
        Assertions.assertThrows(IllegalStateException.class, () ->
                JDBCPushDownSQLBuilder.buildJoinQuery(List.of(t0Scan, t1Scan),
                        List.of(t0a, t1a), JoinOperator.LEFT_OUTER_JOIN, List.of(), List.of()));
    }

    @Test
    public void testBuilderJoinWithInlineOperand() throws Exception {
        // A join operand may be an inline table (a native_query pass-through or a prior pushdown):
        // with no per-table predicate it is emitted as its own parenthesized derived subquery under
        // the join-local alias sr_t{i} — never double-aliased with the sr_inline getInlineTableExpr() adds.
        JDBCTable base = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME0);
        JDBCTable inline = new JDBCTable(getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME1));
        inline.setPassThroughQuery("select a, b from remote_table");
        ColumnRefOperator t0a = new ColumnRefOperator(10, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t1a = new ColumnRefOperator(11, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t1b = new ColumnRefOperator(12, VarcharType.VARCHAR, "b", true);

        Map<ColumnRefOperator, Column> t0Columns = new LinkedHashMap<>();
        t0Columns.put(t0a, base.getColumn("a"));
        Map<ColumnRefOperator, Column> t1Columns = new LinkedHashMap<>();
        t1Columns.put(t1a, inline.getColumn("a"));
        t1Columns.put(t1b, inline.getColumn("b"));

        LogicalJDBCScanOperator t0Scan = newJDBCScan(base, -1, null, t0Columns);
        LogicalJDBCScanOperator t1Scan = newJDBCScan(inline, -1, null, t1Columns);
        String sql = JDBCPushDownSQLBuilder.buildJoinQuery(List.of(t0Scan, t1Scan),
                List.of(t0a, t1b), JoinOperator.INNER_JOIN,
                List.of(new BinaryPredicateOperator(BinaryType.EQ, t0a, t1a)),
                List.of());
        Assertions.assertEquals("SELECT sr_t0.`a` AS sr_c10, sr_t1.`b` AS sr_c12 "
                + "FROM `tbl0` sr_t0 INNER JOIN (select a, b from remote_table) sr_t1 "
                + "ON (sr_t0.`a` = sr_t1.`a`)", sql);
    }

    @Test
    public void testBuilderJoinWithInlineOperandPerTablePredicate() throws Exception {
        // An inline operand carrying its own predicate pre-filters inside a derived subquery: the
        // raw body keeps the sr_inline alias and the wrapping SELECT takes the join-local sr_t{i}.
        JDBCTable base = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME0);
        JDBCTable inline = new JDBCTable(getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME1));
        inline.setPassThroughQuery("select a, b from remote_table");
        ColumnRefOperator t0a = new ColumnRefOperator(10, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t1a = new ColumnRefOperator(11, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t1b = new ColumnRefOperator(12, VarcharType.VARCHAR, "b", true);

        Map<ColumnRefOperator, Column> t0Columns = new LinkedHashMap<>();
        t0Columns.put(t0a, base.getColumn("a"));
        Map<ColumnRefOperator, Column> t1Columns = new LinkedHashMap<>();
        t1Columns.put(t1a, inline.getColumn("a"));
        t1Columns.put(t1b, inline.getColumn("b"));

        LogicalJDBCScanOperator t0Scan = newJDBCScan(base, -1, null, t0Columns);
        LogicalJDBCScanOperator t1Scan = newJDBCScan(inline, -1,
                new BinaryPredicateOperator(BinaryType.GT, t1b, ConstantOperator.createVarchar("x")), t1Columns);
        String sql = JDBCPushDownSQLBuilder.buildJoinQuery(List.of(t0Scan, t1Scan),
                List.of(t0a, t1b), JoinOperator.INNER_JOIN,
                List.of(new BinaryPredicateOperator(BinaryType.EQ, t0a, t1a)),
                List.of());
        Assertions.assertEquals("SELECT sr_t0.`a` AS sr_c10, sr_t1.`b` AS sr_c12 "
                + "FROM `tbl0` sr_t0 INNER JOIN (SELECT `a`, `b` FROM (select a, b from remote_table) sr_inline "
                + "WHERE (`b` > 'x')) sr_t1 ON (sr_t0.`a` = sr_t1.`a`)", sql);
    }

    @Test
    public void testJoinWithPerTableLimit() throws Exception {
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

        LogicalJDBCScanOperator t0Scan = newJDBCScan(table0, 7, null, t0Columns);
        LogicalJDBCScanOperator t1Scan = newJDBCScan(table1, -1, null, t1Columns);
        String sql = JDBCPushDownSQLBuilder.buildJoinQuery(List.of(t0Scan, t1Scan),
                List.of(t0a, t1b),
                List.of(new BinaryPredicateOperator(BinaryType.EQ, t0a, t1a)));
        Assertions.assertEquals("SELECT sr_t0.`a` AS sr_c10, sr_t1.`b` AS sr_c13 "
                + "FROM (SELECT `a`, `c` FROM `tbl0` LIMIT 7) sr_t0"
                + ", `tbl1` sr_t1 WHERE (sr_t0.`a` = sr_t1.`a`)", sql);
    }

    @Test
    public void testBuilderBetweenPredicateWithoutNormalize() throws Exception {
        JDBCTable table0 = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME0);
        ColumnRefOperator t0c = new ColumnRefOperator(10, IntegerType.INT, "c", true);

        Map<ColumnRefOperator, Column> t0Columns = new LinkedHashMap<>();
        t0Columns.put(t0c, table0.getColumn("c"));
        ScalarOperator betweenPredicate = new BetweenPredicateOperator(false, t0c,
                ConstantOperator.createInt(1), ConstantOperator.createInt(10));
        LogicalJDBCScanOperator betweenScan = newJDBCScan(table0, -1, betweenPredicate, t0Columns);
        Assertions.assertEquals("SELECT sr_t0.`c` AS sr_c10 "
                        + "FROM (SELECT `c` FROM `tbl0` WHERE (`c` BETWEEN 1 AND 10)) sr_t0",
                JDBCPushDownSQLBuilder.buildJoinQuery(List.of(betweenScan), List.of(t0c), List.of()));

        ScalarOperator notBetweenPredicate = new BetweenPredicateOperator(true, t0c,
                ConstantOperator.createInt(1), ConstantOperator.createInt(10));
        LogicalJDBCScanOperator notBetweenScan = newJDBCScan(table0, -1, notBetweenPredicate, t0Columns);
        Assertions.assertEquals("SELECT sr_t0.`c` AS sr_c10 "
                        + "FROM (SELECT `c` FROM `tbl0` WHERE (`c` NOT BETWEEN 1 AND 10)) sr_t0",
                JDBCPushDownSQLBuilder.buildJoinQuery(List.of(notBetweenScan), List.of(t0c), List.of()));
    }

    @Test
    public void testBuilderProjectQuery() throws Exception {
        JDBCTable table0 = getMockedJDBCTable(MockedJDBCMetadata.MOCKED_PARTITIONED_TABLE_NAME0);
        ColumnRefOperator t0c = new ColumnRefOperator(10, IntegerType.INT, "c", true);
        Map<ColumnRefOperator, Column> t0Columns = new LinkedHashMap<>();
        t0Columns.put(t0c, table0.getColumn("c"));
        LogicalJDBCScanOperator scan = newJDBCScan(table0, -1, null, t0Columns);

        String sql = JDBCPushDownSQLBuilder.buildScalarSelectQuery(scan,
                List.of(new CallOperator(FunctionSet.ADD, IntegerType.INT, List.of(t0c, t0c))),
                List.of("s"), List.of(), List.of());
        Assertions.assertEquals("SELECT (`c` + `c`) AS `s` FROM `tbl0`", sql);
    }

    @Test
    public void testProjectPushDownFoldsExpressionIntoScan() throws Exception {
        connectContext.getSessionVariable().setEnableJdbcProjectPushDown(true);
        try {
            String sql = "select c + c as s from jdbc0.partitioned_db0.tbl0";
            String plan = getFragmentPlan(sql);
            // The arithmetic projection is folded into the pushed JDBC SQL (dialect-rendered).
            assertContains(plan, "(`c` + `c`)");
        } finally {
            connectContext.getSessionVariable().setEnableJdbcProjectPushDown(false);
        }
    }

    @Test
    public void testProjectPushDownDisabledKeepsExpressionLocal() throws Exception {
        // Flag off (default): the projection is not folded; the scan SELECTs the base column and
        // the arithmetic is computed locally, so the dialect-rendered form never reaches the JDBC SQL.
        String sql = "select c + c as s from jdbc0.partitioned_db0.tbl0";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "(`c` + `c`)");
    }

    // -----------------------------------------------------------------------
    // PushDownJoinToJDBCRule: routePredicates — groups that must not merge
    // -----------------------------------------------------------------------

    @Test
    public void testNonPushableOnPredicateDisqualifiesGroup() throws Exception {
        // abs() is not in JDBCPushDownSQLBuilder's pushable function set, so the owning JDBC
        // group is disqualified and the optimizer keeps the local HASH JOIN.
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on abs(t1.c) = t2.c";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "HASH JOIN");
        assertNotContains(plan, "sr_inline");
    }

    @Test
    public void testNonPushableWherePredicateDisqualifiesGroup() throws Exception {
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where abs(t1.c) > 5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "HASH JOIN");
        assertNotContains(plan, "sr_inline");
    }

    @Test
    public void testNoCrossTableJoinPredicateNotMerged() throws Exception {
        // CROSS JOIN with only single-table WHERE filters. After MultiJoinNode flattening,
        // all predicates belong to a single table → no cross-table join predicate → the
        // group does not merge (see PushDownJoinToJDBCRule.routePredicates) → no push-down.
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "cross join jdbc0.partitioned_db0.tbl1 t2 " +
                "where t1.c > 5 and t2.c > 10";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "sr_inline");
    }

    @Test
    public void testSingleJdbcTableWithNativeTableNotMerged() throws Exception {
        // Only one JDBC atom is in the plan; the other table is a native OlapTable.
        // No JDBC group has >= 2 atoms, so there is no merge group → rule returns empty.
        String sql = "select t1.a from jdbc0.partitioned_db0.tbl0 t1 join t0 on t1.c = t0.v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN JDBC");
        assertNotContains(plan, "sr_inline");
    }

    @Test
    public void testDifferentCatalogsEachSingleTableNotMerged() throws Exception {
        // One JDBC table from each of two catalogs. Each catalog-group has only one atom
        // (< 2), so neither group is eligible → rule returns empty → local HASH JOIN.
        String sql = "select t1.a, t2.a from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc_postgres.partitioned_db0.tbl0 t2 on t1.a = t2.a";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "HASH JOIN");
        assertNotContains(plan, "sr_inline");
    }

    // -----------------------------------------------------------------------
    // JDBCPushDownSQLBuilder: SQL generation — comma join in pushed-down SQL
    // -----------------------------------------------------------------------

    @Test
    public void testCrossJoinInPushdownSQL() throws Exception {
        // Three JDBC tables: t1 CROSS JOIN t2 (no direct predicate between them), then t2
        // JOIN t3 ON t2.a = t3.a. t1 has no join predicate linking it to the rest, so it is
        // left out of the merge (it would otherwise ride along as a remote Cartesian product)
        // and stays a standalone scan joined locally; only t2 and t3, connected by t2.a = t3.a,
        // are merged into one pushdown SQL.
        String sql = "select t1.a, t2.b, t3.a from jdbc0.partitioned_db0.tbl0 t1 " +
                "cross join jdbc0.partitioned_db0.tbl1 t2 " +
                "join jdbc0.partitioned_db0.tbl2 t3 on t2.a = t3.a";
        String plan = getFragmentPlan(sql);
        assertContainsInOrder(plan,
                "CROSS JOIN",
                "FROM (SELECT `a`, `b` FROM `tbl1` WHERE (`a` IS NOT NULL)) sr_t0",
                "INNER JOIN (SELECT `a` FROM `tbl2` WHERE (`a` IS NOT NULL)) sr_t1 ON (sr_t0.`a` = sr_t1.`a`)",
                "TABLE: `tbl0`",
                "QUERY: SELECT `a` FROM `tbl0`");
    }

    @Test
    public void testCrossJoinedInlineAtomNotMerged() throws Exception {
        // The relaxation lets inline (native_query / prior-pushdown) atoms join-merge, but ONLY when
        // a join predicate connects them — splitConnectedComponents leaves a cross-joined atom in its
        // own single-atom component, which is below the >= 2 merge threshold.
        // A native_query CROSS JOIN'd (no connecting predicate) alongside a mergeable base-table pair
        // must stay a standalone local scan: a single cross-joined table is never pushed into a remote
        // Cartesian product, inline or not. Only the connected t2+t3 pair merges.
        String sql = "select q.a, t2.b, t3.a from "
                + "table(jdbc0.native_query('select a from remote_table')) q "
                + "cross join jdbc0.partitioned_db0.tbl1 t2 "
                + "join jdbc0.partitioned_db0.tbl2 t3 on t2.a = t3.a";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "CROSS JOIN");
        // Only the connected base-table pair is merged (one uppercase-SELECT pushdown scan).
        Assertions.assertEquals(1, countOccurrences(plan, "TABLE: (SELECT "));
        assertContains(plan,
                "FROM (SELECT `a`, `b` FROM `tbl1` WHERE (`a` IS NOT NULL)) sr_t0"
                        + " INNER JOIN (SELECT `a` FROM `tbl2` WHERE (`a` IS NOT NULL)) sr_t1 "
                        + "ON (sr_t0.`a` = sr_t1.`a`)) sr_inline");
        // The native_query stays a standalone scan — not absorbed into the merged Cartesian product.
        assertContains(plan, "TABLE: (select a from remote_table) sr_inline");
    }

    @Test
    public void testCrossJoinedComponentsNotMergedAcross() throws Exception {
        // Two independently-connected pairs in the SAME catalog, cross-joined to each other with no
        // predicate between them: (tbl0 JOIN tbl1) CROSS JOIN (tbl2 JOIN tbl3). Each connected
        // component must merge into its OWN pushdown; the cross join between the two components must
        // stay local. Pushing all four into one SQL would render a remote CROSS JOIN — a remote
        // Cartesian product across the two pairs — which we never want.
        String sql = "select t1.a, t2.b, t3.a, t4.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "cross join jdbc0.partitioned_db0.tbl2 t3 " +
                "join jdbc0.partitioned_db0.tbl3 t4 on t3.a = t4.a";
        String plan = getFragmentPlan(sql);
        // Two separate merged pushdowns, not one four-table Cartesian product.
        Assertions.assertEquals(2, countOccurrences(plan, "TABLE: (SELECT "));
        // The two components are cross-joined locally, never inside the pushed SQL.
        assertContains(plan, "CROSS JOIN");
        assertContains(plan,
                "FROM (SELECT `a` FROM `tbl0` WHERE (`a` IS NOT NULL)) sr_t0"
                        + " INNER JOIN (SELECT `a`, `b` FROM `tbl1` WHERE (`a` IS NOT NULL)) sr_t1 "
                        + "ON (sr_t0.`a` = sr_t1.`a`)) sr_inline");
        assertContains(plan,
                "FROM (SELECT `a` FROM `tbl2` WHERE (`a` IS NOT NULL)) sr_t0"
                        + " INNER JOIN (SELECT `a`, `b` FROM `tbl3` WHERE (`a` IS NOT NULL)) sr_t1 "
                        + "ON (sr_t0.`a` = sr_t1.`a`)) sr_inline");
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
        assertContains(plan, "FROM (SELECT `a`, `b` FROM `tbl0` WHERE (`b` IS NULL)) sr_t0");
    }

    @Test
    public void testIsNotNullPredicatePushedDown() throws Exception {
        // visitIsNullPredicate (isNotNull branch): col + " IS NOT NULL"
        String sql = "select t1.a, t1.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.b is not null";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `b` FROM `tbl0` WHERE (`b` IS NOT NULL)) sr_t0");
    }

    @Test
    public void testInPredicatePushedDown() throws Exception {
        // visitInPredicate: col + " IN (...)" → appears in the pushed-down WHERE clause
        String sql = "select t1.a, t1.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.c in (1, 2, 3)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` IN (1, 2, 3))) sr_t0");
    }

    @Test
    public void testNotInPredicatePushedDown() throws Exception {
        // visitInPredicate (isNotIn branch): col + " NOT IN (...)"
        String sql = "select t1.a, t1.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.c not in (1, 2, 3)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` NOT IN (1, 2, 3))) sr_t0");
    }

    @Test
    public void testBetweenPredicatePushedDown() throws Exception {
        // SQL BETWEEN is normalized before JDBC SQL generation.
        String sql = "select t1.a, t1.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.c between 1 and 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` >= 1) AND (`c` <= 10)) sr_t0");
    }

    @Test
    public void testNotBetweenPredicatePushedDown() throws Exception {
        // SQL NOT BETWEEN is normalized before JDBC SQL generation.
        String sql = "select t1.a, t1.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where t1.c not between 1 and 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `c` FROM `tbl0` WHERE ((`c` < 1) OR (`c` > 10))) sr_t0");
    }

    @Test
    public void testNotPredicatePushedDown() throws Exception {
        String sql = "select t1.a, t1.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where not (t1.c = 1)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` != 1)) sr_t0");
    }

    @Test
    public void testNotNullSafeEqualPredicatePushedDown() throws Exception {
        String sql = "select t1.a, t1.c from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on t1.a = t2.a " +
                "where not (t1.c <=> 1)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (NOT (`c` <=> 1))) sr_t0");
    }

    @Test
    public void testMysqlConcatPredicatePushedDown() throws Exception {
        String sql = "select t1.a from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on concat(t1.a, t2.a, t1.b) = t2.b";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "sr_inline",
                "ON (CONCAT(sr_t0.`a`, sr_t1.`a`, sr_t0.`b`) = sr_t1.`b`)");
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
        assertNotContains(plan, "sr_inline");
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

        // MySQL/MariaDB evaluate `/` as DECIMAL bounded by div_precision_increment (default 4),
        // and PG truncates int/int -- both diverge from StarRocks DOUBLE division, so reject up
        // front. Oracle/ClickHouse use float division and stay pushable.
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(divide, JDBCTable.ProtocolType.MYSQL));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(divide, JDBCTable.ProtocolType.MARIADB));
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
    public void testCanPushExpressionOracleConstants() {
        ConstantOperator trueLit = ConstantOperator.createBoolean(true);
        // Oracle SQL has no BOOLEAN type at all — gate must reject boolean constants.
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(trueLit, JDBCTable.ProtocolType.ORACLE));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(trueLit, JDBCTable.ProtocolType.UNKNOWN));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(trueLit, JDBCTable.ProtocolType.MYSQL));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(trueLit, JDBCTable.ProtocolType.POSTGRES));
    }

    @Test
    public void testInListPushdownSizeLimitConfigurable() {
        ColumnRefOperator intCol = new ColumnRefOperator(2, IntegerType.INT, "c", true);
        InPredicateOperator in600 = inPredicateOf(intCol, 600);
        InPredicateOperator in1001 = inPredicateOf(intCol, 1001);

        // -1 = no limit: non-Oracle dialects push any size.
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(in600, JDBCTable.ProtocolType.MYSQL, -1));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(in1001, JDBCTable.ProtocolType.MYSQL, -1));

        // 0 = never push an IN list down, on any dialect.
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(in600, JDBCTable.ProtocolType.MYSQL, 0));

        // N > 0 = cap: a larger list stays local, a list within the cap pushes.
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(in600, JDBCTable.ProtocolType.MYSQL, 500));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(in600, JDBCTable.ProtocolType.POSTGRES, 500));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(in600, JDBCTable.ProtocolType.MYSQL, 600));

        // Oracle uses the same configurable cap as every dialect — no special hard floor.
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(in1001, JDBCTable.ProtocolType.ORACLE, -1));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(in1001, JDBCTable.ProtocolType.ORACLE, 5000));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(in600, JDBCTable.ProtocolType.ORACLE, 500));
        // A cap can still be set to Oracle's per-version limit (e.g. 1000) when desired.
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(in1001, JDBCTable.ProtocolType.ORACLE, 1000));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(in600, JDBCTable.ProtocolType.ORACLE, -1));
    }

    private static InPredicateOperator inPredicateOf(ColumnRefOperator col, int items) {
        List<ScalarOperator> children = new java.util.ArrayList<>();
        children.add(col);
        for (int i = 0; i < items; i++) {
            children.add(ConstantOperator.createInt(i));
        }
        return new InPredicateOperator(false, children);
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
                nullSafe.accept(new ScalarOperatorToJDBCSQLVisitor.MySQLLikeSQLRenderer(names), null));
        Assertions.assertEquals("(`a` IS NOT DISTINCT FROM `b`)",
                nullSafe.accept(new ScalarOperatorToJDBCSQLVisitor.PostgresSQLRenderer(names), null));
        Assertions.assertEquals("(`a` <=> `b`)",
                nullSafe.accept(new ScalarOperatorToJDBCSQLVisitor.ClickHouseSQLRenderer(names), null));
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
                mod.accept(new ScalarOperatorToJDBCSQLVisitor.MySQLLikeSQLRenderer(names), null));
        Assertions.assertEquals("(`a` % `b`)",
                mod.accept(new ScalarOperatorToJDBCSQLVisitor.PostgresSQLRenderer(names), null));
        Assertions.assertEquals("(`a` % `b`)",
                mod.accept(new ScalarOperatorToJDBCSQLVisitor.ClickHouseSQLRenderer(names), null));
    }

    @Test
    public void testRendererCastTypePerDialect() {
        ColumnRefOperator c = new ColumnRefOperator(1, IntegerType.INT, "c", true);
        CastOperator toDate = new CastOperator(DateType.DATE, c, false);
        Map<ColumnRefOperator, String> names = new HashMap<>();
        names.put(c, "`c`");

        Assertions.assertEquals("CAST(`c` AS date)",
                toDate.accept(new ScalarOperatorToJDBCSQLVisitor.MySQLLikeSQLRenderer(names), null));
        Assertions.assertEquals("CAST(`c` AS date)",
                toDate.accept(new ScalarOperatorToJDBCSQLVisitor.PostgresSQLRenderer(names), null));
        Assertions.assertEquals("CAST(`c` AS DATE)",
                toDate.accept(new ScalarOperatorToJDBCSQLVisitor.OracleSQLRenderer(names), null));
        Assertions.assertEquals("CAST(`c` AS Date)",
                toDate.accept(new ScalarOperatorToJDBCSQLVisitor.ClickHouseSQLRenderer(names), null));
    }

    @Test
    public void testRendererOracleDateLiteralWrapping() throws Exception {
        ConstantOperator date = ConstantOperator.createDate(
                java.time.LocalDateTime.of(2024, 1, 15, 0, 0, 0));
        ScalarOperatorToJDBCSQLVisitor oracle =
                new ScalarOperatorToJDBCSQLVisitor.OracleSQLRenderer(new HashMap<>());
        ScalarOperatorToJDBCSQLVisitor mysql =
                new ScalarOperatorToJDBCSQLVisitor.MySQLLikeSQLRenderer(new HashMap<>());

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
                new ScalarOperatorToJDBCSQLVisitor.OracleSQLRenderer(new HashMap<>());
        ScalarOperatorToJDBCSQLVisitor mysql =
                new ScalarOperatorToJDBCSQLVisitor.MySQLLikeSQLRenderer(new HashMap<>());

        Assertions.assertEquals("DATE '2024-01-15'", implicitToDate.accept(oracle, null));
        Assertions.assertEquals("'2024-01-15'", implicitToDate.accept(mysql, null));
    }

    @Test
    public void testOrJoinPredicatePushedDown() throws Exception {
        // An OR compound predicate that is cross-table is classified as a join predicate.
        // JDBCPushDownSQLBuilder pushes OR when all children are pushable, and
        // visitCompoundPredicate(OR) emits "(child1 OR child2)".
        String sql = "select t1.a, t2.b from jdbc0.partitioned_db0.tbl0 t1 " +
                "join jdbc0.partitioned_db0.tbl1 t2 on (t1.a = t2.a or t1.b = t2.b)";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                " INNER JOIN `tbl1` sr_t1 ON ((sr_t0.`a` = sr_t1.`a`) OR (sr_t0.`b` = sr_t1.`b`))");
    }

    // -----------------------------------------------------------------------
    // JDBCPushDownSQLBuilder: static utility methods
    // -----------------------------------------------------------------------

    @Test
    public void testOutputColumnAlias() {
        Assertions.assertEquals("sr_c0", JDBCPushDownSQLBuilder.outputColumnAlias(0));
        Assertions.assertEquals("sr_c1", JDBCPushDownSQLBuilder.outputColumnAlias(1));
        Assertions.assertEquals("sr_c42", JDBCPushDownSQLBuilder.outputColumnAlias(42));
        Assertions.assertEquals("sr_c100", JDBCPushDownSQLBuilder.outputColumnAlias(100));
    }

    @Test
    public void testQueryTableAtomIsMerged() throws Exception {
        // One side of the JOIN is a JDBC native_query (an inline table). collectMergeGroups now
        // includes inline atoms, so the join is pushed down: the native_query is emitted as its own
        // derived subquery — its raw body wrapped as (select ...) sr_inline and pre-filtered by the
        // inner join's IS NOT NULL — and joined remotely, leaving a single JDBC scan, no HASH JOIN.
        String sql = "select t1.a, q.a from jdbc0.partitioned_db0.tbl0 t1 "
                + "join table(jdbc0.native_query('select a from remote_table')) q on t1.a = q.a";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(1, countOccurrences(plan, "TABLE: (SELECT "));
        assertContains(plan,
                "FROM (SELECT `a` FROM `tbl0` WHERE (`a` IS NOT NULL)) sr_t0"
                        + " INNER JOIN (SELECT `a` FROM (select a from remote_table) sr_inline "
                        + "WHERE (`a` IS NOT NULL)) sr_t1 "
                        + "ON (sr_t0.`a` = sr_t1.`a`)) sr_inline");
        assertNotContains(plan, "HASH JOIN");
    }

    @Test
    public void testDerivedTableAtomIsMerged() throws Exception {
        // The left atom is first turned into a derived (inline) scan by projection pushdown (concat
        // is folded into its remote SQL), then joined with a base table. The relaxed rule merges the
        // derived scan too, so the whole thing collapses to one pushed JDBC scan with no local HASH
        // JOIN — proving prior-pushdown derived tables (synthesized column names included) flow
        // through the join merge, not just native_query pass-throughs.
        connectContext.getSessionVariable().setEnableJdbcProjectPushDown(true);
        try {
            String sql = "select x.e, t2.b from "
                    + "(select a, concat(a, 'x') as e from jdbc0.partitioned_db0.tbl0) x "
                    + "join jdbc0.partitioned_db0.tbl1 t2 on x.a = t2.a";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "sr_inline");
            assertContains(plan, "CONCAT(");
            assertNotContains(plan, "HASH JOIN");
            Assertions.assertEquals(1, countOccurrences(plan, "TABLE: (SELECT "));
        } finally {
            connectContext.getSessionVariable().setEnableJdbcProjectPushDown(false);
        }
    }
}
