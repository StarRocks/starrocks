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

package com.starrocks.sql.optimizer.rule.ivm;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrVersion;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link IvmDeltaAggregateRule} check conditions and
 * {@link IvmVersionAggregateRule} transform logic.
 *
 * Note: IvmDeltaAggregateRule.transform() requires a real MaterializedView with
 * __ROW_ID__ and __AGG_STATE__ columns, plus MvRewritePreprocessor. This is covered
 * by the integration test IVMBasedMvRefreshProcessorIcebergTest.testPartitionedIVMWithAggregate1.
 */
public class IvmAggregateRuleTest {

    // ==================== IvmDeltaAggregateRule check conditions ====================

    @Test
    public void testCheckRejectsNonRootDelta(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        OptExpression aggExpr = buildDeltaAggregate(factory, table, false);  // isRootDelta=false
        deriveLogicalProperty(aggExpr);

        Assertions.assertFalse(new IvmDeltaAggregateRule().check(aggExpr, context));
    }

    @Test
    public void testCheckRejectsEmptyGroupBy(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);
        ColumnRefOperator sumRef = factory.create("sum_combine", IntegerType.BIGINT, true);

        // Aggregate with empty grouping keys
        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(sumRef, new CallOperator("sum_combine", IntegerType.BIGINT, List.of()));
        LogicalAggregationOperator agg = new LogicalAggregationOperator(
                AggType.GLOBAL, List.of(), aggMap);  // empty group by

        OptExpression scanExpr = newIcebergScan(factory, table);
        OptExpression aggExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(agg, scanExpr));
        deriveLogicalProperty(aggExpr);

        Assertions.assertFalse(new IvmDeltaAggregateRule().check(aggExpr, context));
    }

    @Test
    public void testCheckRejectsDistinct(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);
        ColumnRefOperator dateRef = factory.create("date", IntegerType.INT, false);
        ColumnRefOperator countRef = factory.create("count_distinct", IntegerType.BIGINT, true);

        // Aggregate with DISTINCT
        CallOperator distinctCall = new CallOperator("count", IntegerType.BIGINT, List.of(dateRef), null, true);
        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(countRef, distinctCall);
        LogicalAggregationOperator agg = new LogicalAggregationOperator(
                AggType.GLOBAL, List.of(dateRef), aggMap);

        OptExpression scanExpr = newIcebergScan(factory, table);
        OptExpression aggExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(agg, scanExpr));
        deriveLogicalProperty(aggExpr);

        Assertions.assertFalse(new IvmDeltaAggregateRule().check(aggExpr, context));
    }

    @Test
    public void testCheckRejectsHaving(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);
        ColumnRefOperator dateRef = factory.create("date", IntegerType.INT, false);
        ColumnRefOperator sumRef = factory.create("sum_combine", IntegerType.BIGINT, true);

        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(sumRef, new CallOperator("sum_combine", IntegerType.BIGINT, List.of(dateRef)));
        LogicalAggregationOperator agg = new LogicalAggregationOperator(
                AggType.GLOBAL, List.of(dateRef), aggMap);
        // Set HAVING predicate
        agg.setPredicate(new com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator(
                com.starrocks.sql.ast.expression.BinaryType.GT, sumRef,
                com.starrocks.sql.optimizer.operator.scalar.ConstantOperator.createInt(100)));

        OptExpression scanExpr = newIcebergScan(factory, table);
        OptExpression aggExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(agg, scanExpr));
        deriveLogicalProperty(aggExpr);

        Assertions.assertFalse(new IvmDeltaAggregateRule().check(aggExpr, context));
    }

    @Test
    public void testCheckRejectsWhenNoTargetMv(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        // No statement set → loadTargetMv returns null

        OptExpression aggExpr = buildDeltaAggregate(factory, table, true);
        deriveLogicalProperty(aggExpr);

        Assertions.assertFalse(new IvmDeltaAggregateRule().check(aggExpr, context));
    }

    // ==================== IvmVersionAggregateRule ====================

    @Test
    public void testVersionAggregatePushDown(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator dateRef = factory.create("date", IntegerType.INT, false);
        ColumnRefOperator sumRef = factory.create("sum_combine", IntegerType.BIGINT, true);

        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(sumRef, new CallOperator("sum_combine", IntegerType.BIGINT, List.of(dateRef)));
        LogicalAggregationOperator agg = new LogicalAggregationOperator(
                AggType.GLOBAL, List.of(dateRef), aggMap);

        OptExpression scanExpr = newIcebergScan(factory, table);
        // Version → Aggregate → Scan
        OptExpression versionAgg = OptExpression.create(LogicalVersionOperator.fromVersion(),
                OptExpression.create(agg, scanExpr));
        deriveLogicalProperty(versionAgg);

        List<OptExpression> result = new IvmVersionAggregateRule().transform(versionAgg, context);

        // Should produce: Aggregate → Version → Scan
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalAggregationOperator);
        Assertions.assertEquals(OperatorType.LOGICAL_VERSION, result.get(0).inputAt(0).getOp().getOpType());
        LogicalVersionOperator newVersion = (LogicalVersionOperator) result.get(0).inputAt(0).getOp();
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.FROM_VERSION, newVersion.getVersionRefType());
        Assertions.assertTrue(result.get(0).inputAt(0).inputAt(0).getOp() instanceof LogicalIcebergScanOperator);
    }

    @Test
    public void testVersionAggregatePreservesToVersion(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator dateRef = factory.create("date", IntegerType.INT, false);
        ColumnRefOperator sumRef = factory.create("sum_combine", IntegerType.BIGINT, true);

        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(sumRef, new CallOperator("sum_combine", IntegerType.BIGINT, List.of(dateRef)));
        LogicalAggregationOperator agg = new LogicalAggregationOperator(
                AggType.GLOBAL, List.of(dateRef), aggMap);

        OptExpression scanExpr = newIcebergScan(factory, table);
        // Version(TO) → Aggregate → Scan
        OptExpression versionAgg = OptExpression.create(LogicalVersionOperator.toVersion(),
                OptExpression.create(agg, scanExpr));
        deriveLogicalProperty(versionAgg);

        List<OptExpression> result = new IvmVersionAggregateRule().transform(versionAgg, context);

        // Should preserve TO_VERSION
        LogicalVersionOperator newVersion = (LogicalVersionOperator) result.get(0).inputAt(0).getOp();
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.TO_VERSION, newVersion.getVersionRefType());
    }

    // ==================== Helpers ====================

    private void mockIcebergTable(IcebergTable table) {
        new Expectations() {
            {
                table.getType();
                result = com.starrocks.catalog.Table.TableType.ICEBERG;
                minTimes = 0;
            }
        };
    }

    private OptExpression buildDeltaAggregate(ColumnRefFactory factory, IcebergTable table, boolean isRootDelta) {
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);
        ColumnRefOperator dateRef = factory.create("date", IntegerType.INT, false);
        ColumnRefOperator sumRef = factory.create("sum_combine", IntegerType.BIGINT, true);

        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(sumRef, new CallOperator("sum_combine", IntegerType.BIGINT, List.of(dateRef)));
        LogicalAggregationOperator agg = new LogicalAggregationOperator(
                AggType.GLOBAL, List.of(dateRef), aggMap);

        OptExpression scanExpr = newIcebergScan(factory, table);
        return OptExpression.create(new LogicalDeltaOperator(isRootDelta, actionRef),
                OptExpression.create(agg, scanExpr));
    }

    private OptExpression newIcebergScan(ColumnRefFactory factory, IcebergTable table) {
        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        Column idCol = new Column("id", IntegerType.INT, false);
        Column dataCol = new Column("data", StringType.STRING, true);
        Map<ColumnRefOperator, Column> colRefMap = Maps.newHashMap();
        colRefMap.put(idRef, idCol);
        colRefMap.put(dataRef, dataCol);
        LogicalIcebergScanOperator scan = new LogicalIcebergScanOperator.Builder()
                .setTable(table)
                .setColRefToColumnMetaMap(colRefMap)
                .setTableVersionRange(TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)))
                .build();
        return OptExpression.create(scan);
    }

    private static void deriveLogicalProperty(OptExpression expression) {
        for (OptExpression child : expression.getInputs()) {
            deriveLogicalProperty(child);
        }
        ExpressionContext ctx = new ExpressionContext(expression);
        ctx.deriveLogicalProperty();
        expression.setLogicalProperty(ctx.getRootProperty());
    }
}
