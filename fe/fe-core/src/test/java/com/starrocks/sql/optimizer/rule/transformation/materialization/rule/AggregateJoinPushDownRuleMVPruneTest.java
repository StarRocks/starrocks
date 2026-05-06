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

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AggregateJoinPushDownRuleMVPruneTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static Table mockTable(String name) {
        Table t = Mockito.mock(Table.class);
        Mockito.when(t.toString()).thenReturn(name);
        return t;
    }

    /**
     * Builds an {@link OptExpression} wrapping a mocked {@link LogicalScanOperator} whose
     * {@code getPredicate()} returns {@code predicate} and {@code getColRefToColumnMetaMap()}
     * returns {@code refToCol}.
     */
    private static OptExpression scanNode(Table table,
                                          Map<ColumnRefOperator, Column> refToCol,
                                          ScalarOperator predicate) {
        LogicalScanOperator scan = Mockito.mock(LogicalScanOperator.class);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getColRefToColumnMetaMap()).thenReturn(refToCol);
        Mockito.when(scan.getPredicate()).thenReturn(predicate);
        // make instanceof LogicalScanOperator work
        OptExpression node = Mockito.mock(OptExpression.class);
        Mockito.when(node.getOp()).thenReturn(scan);
        Mockito.when(node.getInputs()).thenReturn(Collections.emptyList());
        return node;
    }

    /**
     * Builds an {@link OptExpression} wrapping a mocked {@link LogicalJoinOperator} whose
     * {@code getOnPredicate()} returns {@code onPredicate}, with the given child scan nodes.
     */
    private static OptExpression joinNode(ScalarOperator onPredicate, List<OptExpression> children) {
        LogicalJoinOperator join = Mockito.mock(LogicalJoinOperator.class);
        Mockito.when(join.getOnPredicate()).thenReturn(onPredicate);
        OptExpression node = Mockito.mock(OptExpression.class);
        Mockito.when(node.getOp()).thenReturn(join);
        Mockito.when(node.getInputs()).thenReturn(children);
        return node;
    }

    // -----------------------------------------------------------------------
    // Group 1: Scan predicate
    // -----------------------------------------------------------------------

    /**
     * Scan with predicate {@code col = 1} → col added.
     */
    @Test
    public void testScanPredicateAddsColumn() {
        Table tableA = mockTable("a");
        ColumnRefOperator colRef = new ColumnRefOperator(1, IntegerType.INT, "col", true);
        Column col = new Column("col", IntegerType.INT);
        ScalarOperator pred = new BinaryPredicateOperator(BinaryType.EQ, colRef, ConstantOperator.createInt(1));

        OptExpression node = scanNode(tableA, ImmutableMap.of(colRef, col), pred);
        Map<Table, Set<String>> result = new HashMap<>();
        AggregateJoinPushDownRule.collectPredicateColumnsByTable(node, result);

        assertEquals(Set.of("col"), result.get(tableA));
    }

    /**
     * Scan with no predicate → nothing collected.
     */
    @Test
    public void testScanNullPredicateCollectsNothing() {
        Table tableA = mockTable("a");
        ColumnRefOperator colRef = new ColumnRefOperator(1, IntegerType.INT, "col", true);
        Column col = new Column("col", IntegerType.INT);

        OptExpression node = scanNode(tableA, ImmutableMap.of(colRef, col), null);
        Map<Table, Set<String>> result = new HashMap<>();
        AggregateJoinPushDownRule.collectPredicateColumnsByTable(node, result);

        assertTrue(result.isEmpty());
    }

    /**
     * Scan with compound predicate {@code col1 = 1 AND col2 > 5} → both cols added.
     */
    @Test
    public void testScanCompoundPredicateAddsMultipleColumns() {
        Table tableA = mockTable("a");
        ColumnRefOperator ref1 = new ColumnRefOperator(1, IntegerType.INT, "col1", true);
        ColumnRefOperator ref2 = new ColumnRefOperator(2, IntegerType.INT, "col2", true);
        Column col1 = new Column("col1", IntegerType.INT);
        Column col2 = new Column("col2", IntegerType.INT);

        ScalarOperator pred = CompoundPredicateOperator.and(
                new BinaryPredicateOperator(BinaryType.EQ, ref1, ConstantOperator.createInt(1)),
                new BinaryPredicateOperator(BinaryType.GT, ref2, ConstantOperator.createInt(5)));

        OptExpression node = scanNode(tableA, ImmutableMap.of(ref1, col1, ref2, col2), pred);
        Map<Table, Set<String>> result = new HashMap<>();
        AggregateJoinPushDownRule.collectPredicateColumnsByTable(node, result);

        assertEquals(Set.of("col1", "col2"), result.get(tableA));
    }

    // -----------------------------------------------------------------------
    // Group 2: JOIN ON filter predicate
    // -----------------------------------------------------------------------

    /**
     * JOIN ON {@code a.id = b.id} (pure equi-join) → nothing collected.
     */
    @Test
    public void testJoinOnEquiJoinSkipped() {
        Table tableA = mockTable("a");
        Table tableB = mockTable("b");
        ColumnRefOperator aId = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        ColumnRefOperator bId = new ColumnRefOperator(2, IntegerType.INT, "id", true);
        Column colA = new Column("id", IntegerType.INT);
        Column colB = new Column("id", IntegerType.INT);

        ScalarOperator onPred = new BinaryPredicateOperator(BinaryType.EQ, aId, bId);

        OptExpression leftScan = scanNode(tableA, ImmutableMap.of(aId, colA), null);
        OptExpression rightScan = scanNode(tableB, ImmutableMap.of(bId, colB), null);
        OptExpression join = joinNode(onPred, ImmutableList.of(leftScan, rightScan));

        Map<Table, Set<String>> result = new HashMap<>();
        AggregateJoinPushDownRule.collectPredicateColumnsByTable(join, result);

        assertTrue(result.isEmpty(), "Pure equi-join should not contribute predicate columns");
    }

    /**
     * JOIN ON {@code a.id = b.id AND a.col = 1} → only a.col collected, equi-join skipped.
     */
    @Test
    public void testJoinOnCompoundPartialFilter() {
        Table tableA = mockTable("a");
        Table tableB = mockTable("b");
        ColumnRefOperator aId  = new ColumnRefOperator(1, IntegerType.INT, "id",  true);
        ColumnRefOperator bId  = new ColumnRefOperator(2, IntegerType.INT, "id",  true);
        ColumnRefOperator aCol = new ColumnRefOperator(3, IntegerType.INT, "col", true);
        Column colAId  = new Column("id",  IntegerType.INT);
        Column colBId  = new Column("id",  IntegerType.INT);
        Column colACol = new Column("col", IntegerType.INT);

        ScalarOperator onPred = CompoundPredicateOperator.and(
                new BinaryPredicateOperator(BinaryType.EQ, aId, bId),
                new BinaryPredicateOperator(BinaryType.EQ, aCol, ConstantOperator.createInt(1)));

        OptExpression leftScan  = scanNode(tableA, ImmutableMap.of(aId, colAId, aCol, colACol), null);
        OptExpression rightScan = scanNode(tableB, ImmutableMap.of(bId, colBId),                null);
        OptExpression join = joinNode(onPred, ImmutableList.of(leftScan, rightScan));

        Map<Table, Set<String>> result = new HashMap<>();
        AggregateJoinPushDownRule.collectPredicateColumnsByTable(join, result);

        assertEquals(1, result.size());
        assertEquals(Set.of("col"), result.get(tableA));
        assertFalse(result.containsKey(tableB));
    }

    /**
     * JOIN ON {@code concat(a.lo_linenumber,'c') = concat(1,'2',3)} → lo_linenumber collected.
     */
    @Test
    public void testJoinOnFunctionWrappedFilterAddsColumn() {
        Table tableA = mockTable("a");
        Table tableB = mockTable("b");
        ColumnRefOperator lineNum = new ColumnRefOperator(1, IntegerType.INT, "lo_linenumber", true);
        ColumnRefOperator bId    = new ColumnRefOperator(2, IntegerType.INT, "id",            true);
        Column colLineNum = new Column("lo_linenumber", IntegerType.INT);
        Column colBId     = new Column("id",            IntegerType.INT);

        ScalarOperator lhs = new CallOperator("concat", VarcharType.VARCHAR,
                ImmutableList.of(lineNum, ConstantOperator.createVarchar("c")));
        ScalarOperator rhs = new CallOperator("concat", VarcharType.VARCHAR,
                ImmutableList.of(ConstantOperator.createInt(1),
                        ConstantOperator.createVarchar("2"),
                        ConstantOperator.createInt(3)));
        ScalarOperator onPred = new BinaryPredicateOperator(BinaryType.EQ, lhs, rhs);

        OptExpression leftScan  = scanNode(tableA, ImmutableMap.of(lineNum, colLineNum), null);
        OptExpression rightScan = scanNode(tableB, ImmutableMap.of(bId,     colBId),     null);
        OptExpression join = joinNode(onPred, ImmutableList.of(leftScan, rightScan));

        Map<Table, Set<String>> result = new HashMap<>();
        AggregateJoinPushDownRule.collectPredicateColumnsByTable(join, result);

        assertEquals(1, result.size());
        assertEquals(Set.of("lo_linenumber"), result.get(tableA));
    }

    /**
     * JOIN ON scan predicate and JOIN ON filter both contribute: scan has {@code col1 = 1},
     * ON has {@code a.col2 = b.id AND a.col3 = 99} → col1, col3 collected for tableA (equi skipped).
     */
    @Test
    public void testScanPredicateAndJoinOnFilterCombined() {
        Table tableA = mockTable("a");
        Table tableB = mockTable("b");
        ColumnRefOperator aCol1 = new ColumnRefOperator(1, IntegerType.INT, "col1", true);
        ColumnRefOperator aCol2 = new ColumnRefOperator(2, IntegerType.INT, "col2", true);
        ColumnRefOperator aCol3 = new ColumnRefOperator(3, IntegerType.INT, "col3", true);
        ColumnRefOperator bId   = new ColumnRefOperator(4, IntegerType.INT, "id",   true);
        Column colA1 = new Column("col1", IntegerType.INT);
        Column colA2 = new Column("col2", IntegerType.INT);
        Column colA3 = new Column("col3", IntegerType.INT);
        Column colBI = new Column("id",   IntegerType.INT);

        // scan predicate: col1 = 1
        ScalarOperator scanPred = new BinaryPredicateOperator(BinaryType.EQ, aCol1, ConstantOperator.createInt(1));
        // ON: a.col2 = b.id (equi, skipped) AND a.col3 = 99 (filter)
        ScalarOperator onPred = CompoundPredicateOperator.and(
                new BinaryPredicateOperator(BinaryType.EQ, aCol2, bId),
                new BinaryPredicateOperator(BinaryType.EQ, aCol3, ConstantOperator.createInt(99)));

        OptExpression leftScan  = scanNode(tableA,
                ImmutableMap.of(aCol1, colA1, aCol2, colA2, aCol3, colA3), scanPred);
        OptExpression rightScan = scanNode(tableB, ImmutableMap.of(bId, colBI), null);
        OptExpression join = joinNode(onPred, ImmutableList.of(leftScan, rightScan));

        Map<Table, Set<String>> result = new HashMap<>();
        AggregateJoinPushDownRule.collectPredicateColumnsByTable(join, result);

        assertEquals(1, result.size());
        assertEquals(Set.of("col1", "col3"), result.get(tableA));
    }

    // -----------------------------------------------------------------------
    // Group 3: validMvGroupByAndPredicateColumns
    // -----------------------------------------------------------------------

    private static final AggregateJoinPushDownRule RULE = new AggregateJoinPushDownRule();

    /**
     * Builds a mock {@link MaterializationContext} with pre-cached predicate/grouping columns
     * and an {@link OptExpression} that reports whether the MV contains aggregation.
     *
     * @param mvPredicateCols  pre-cached MV predicate columns (null → not cached, will be computed)
     * @param mvGroupingCols   pre-cached MV grouping columns (null → not cached, will be computed)
     * @param hasAggregation   whether the MV expression tree contains an aggregation operator
     */
    private static MaterializationContext mockMvContext(Set<String> mvGroupingCols,
                                                        Set<String> mvPredicateCols,
                                                        boolean hasAggregation) {
        // Build a minimal MV OptExpression that answers containsAggregation correctly.
        OptExpression mvExpr = Mockito.mock(OptExpression.class);
        if (hasAggregation) {
            LogicalAggregationOperator agg = Mockito.mock(LogicalAggregationOperator.class);
            Mockito.when(mvExpr.getOp()).thenReturn(agg);
            Mockito.when(mvExpr.getInputs()).thenReturn(Collections.emptyList());
        } else {
            LogicalScanOperator scan = Mockito.mock(LogicalScanOperator.class);
            Mockito.when(mvExpr.getOp()).thenReturn(scan);
            Mockito.when(mvExpr.getInputs()).thenReturn(Collections.emptyList());
        }

        MaterializationContext ctx = Mockito.mock(MaterializationContext.class);
        Mockito.when(ctx.getMvExpression()).thenReturn(mvExpr);
        Mockito.when(ctx.getPredicateColumns()).thenReturn(mvPredicateCols);
        Mockito.when(ctx.getGroupingColumns()).thenReturn(mvGroupingCols);
        return ctx;
    }

    /**
     * MV predicate = {}, query predicate = null, MV has aggregation, MV groupBy = {a, b}.
     * Query groupBy+pred = {a} → MV groupBy covers it → valid.
     */
    @Test
    public void testValidMv_NoPredicate_GroupByCovered() {
        MaterializationContext ctx = mockMvContext(
                Set.of("a", "b"),  // mvGroupingCols  (cached)
                Set.of(),          // mvPredicateCols (cached, empty)
                true);             // has aggregation

        boolean result = RULE.validMvGroupByAndPredicateColumns(
                ctx,
                Set.of("a"),   // queryGroupByAndPred
                null);         // queryPredicateColumns (no predicate in query)

        assertTrue(result);
    }

    /**
     * MV predicate = {lo_linenumber}, query predicate = null → MV is over-filtered → invalid.
     */
    @Test
    public void testInvalidMv_MvHasPredicate_QueryHasNone() {
        MaterializationContext ctx = mockMvContext(
                Set.of("lo_orderdate", "lo_linenumber"),
                Set.of("lo_linenumber"), // mvPredicateCols
                true);

        boolean result = RULE.validMvGroupByAndPredicateColumns(
                ctx,
                Set.of("lo_orderdate"),
                null);  // query has no predicate

        assertFalse(result);
    }

    /**
     * MV predicate = {lo_linenumber}, query predicate = {lo_linenumber} (covered),
     * MV groupBy = {lo_orderdate, lo_linenumber, lo_custkey},
     * query groupBy+pred = {lo_orderdate, lo_linenumber} → covered → valid.
     * (This is the scenario from testAggPushDown_ValidMv_QueryJoinOnPredicateCovered.)
     */
    @Test
    public void testValidMv_PredicateCovered_GroupByCovered() {
        MaterializationContext ctx = mockMvContext(
                Set.of("lo_orderdate", "lo_linenumber", "lo_custkey"),
                Set.of("lo_linenumber"),
                true);

        boolean result = RULE.validMvGroupByAndPredicateColumns(
                ctx,
                Set.of("lo_orderdate", "lo_linenumber"),  // queryGroupByAndPred
                Set.of("lo_linenumber"));                  // queryPredicateColumns

        assertTrue(result);
    }

    /**
     * MV predicate = {lo_linenumber, lo_custkey}, query predicate = {lo_linenumber} only
     * → query doesn't cover all MV predicate columns → invalid.
     */
    @Test
    public void testInvalidMv_MvPredicateNotCoveredByQuery() {
        MaterializationContext ctx = mockMvContext(
                Set.of("lo_orderdate", "lo_linenumber", "lo_custkey"),
                Set.of("lo_linenumber", "lo_custkey"),
                true);

        boolean result = RULE.validMvGroupByAndPredicateColumns(
                ctx,
                Set.of("lo_orderdate", "lo_linenumber"),
                Set.of("lo_linenumber"));  // missing lo_custkey

        assertFalse(result);
    }

    /**
     * MV predicate = {}, MV has aggregation, MV groupBy = {a},
     * query groupBy+pred = {a, b} → MV groupBy does NOT cover b → invalid.
     */
    @Test
    public void testInvalidMv_GroupByNotCovered() {
        MaterializationContext ctx = mockMvContext(
                Set.of("a"),
                Set.of(),
                true);

        boolean result = RULE.validMvGroupByAndPredicateColumns(
                ctx,
                Set.of("a", "b"),  // query needs both a and b
                Set.of());         // empty predicate

        assertFalse(result);
    }
}
