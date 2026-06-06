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
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
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

public class IvmJoinRuleTest {

    // ==================== IvmDeltaJoinRule check ====================

    @Test
    public void testCheckAcceptsInnerJoin(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        OptExpression input = buildDeltaJoin(factory, table, JoinOperator.INNER_JOIN);
        deriveLogicalProperty(input);

        Assertions.assertTrue(new IvmDeltaJoinRule().check(input, context));
    }

    @Test
    public void testCheckAcceptsCrossJoin(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        OptExpression input = buildDeltaJoin(factory, table, JoinOperator.CROSS_JOIN);
        deriveLogicalProperty(input);

        Assertions.assertTrue(new IvmDeltaJoinRule().check(input, context));
    }

    @Test
    public void testCheckRejectsLeftOuterJoin(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        OptExpression input = buildDeltaJoin(factory, table, JoinOperator.LEFT_OUTER_JOIN);
        deriveLogicalProperty(input);

        Assertions.assertFalse(new IvmDeltaJoinRule().check(input, context));
    }

    @Test
    public void testCheckRejectsLeftSemiJoin(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        OptExpression input = buildDeltaJoin(factory, table, JoinOperator.LEFT_SEMI_JOIN);
        deriveLogicalProperty(input);

        Assertions.assertFalse(new IvmDeltaJoinRule().check(input, context));
    }

    // ==================== IvmDeltaJoinRule transform ====================

    @Test
    public void testTransformInnerJoinProducesUnionWithTwoBranches(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator leftId = factory.create("a_id", IntegerType.INT, false);
        ColumnRefOperator leftData = factory.create("a_data", StringType.STRING, true);
        ColumnRefOperator rightId = factory.create("b_id", IntegerType.INT, false);
        ColumnRefOperator rightData = factory.create("b_data", StringType.STRING, true);
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);

        OptExpression leftScan = newIcebergScan(table, leftId, leftData,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression rightScan = newIcebergScan(table, rightId, rightData,
                TvrTableDelta.of(TvrVersion.of(300L), TvrVersion.of(400L)));

        LogicalJoinOperator join = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, leftId, rightId));
        OptExpression joinExpr = OptExpression.create(join, leftScan, rightScan);
        OptExpression input = OptExpression.create(new LogicalDeltaOperator(true, actionRef), joinExpr);
        deriveLogicalProperty(input);

        List<OptExpression> result = new IvmDeltaJoinRule().transform(input, context);

        // Should produce UnionAll with 2 children
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalUnionOperator);
        LogicalUnionOperator union = (LogicalUnionOperator) result.get(0).getOp();
        Assertions.assertTrue(union.isUnionAll());
        Assertions.assertEquals(2, result.get(0).getInputs().size());

        // Branch 1: Join(Delta → left, Version(FROM) → right)
        OptExpression branch1 = result.get(0).inputAt(0);
        Assertions.assertTrue(branch1.getOp() instanceof LogicalJoinOperator);
        Assertions.assertEquals(OperatorType.LOGICAL_DELTA, branch1.inputAt(0).getOp().getOpType());
        LogicalDeltaOperator branch1Delta = (LogicalDeltaOperator) branch1.inputAt(0).getOp();
        Assertions.assertFalse(branch1Delta.isRootDelta());
        Assertions.assertEquals(OperatorType.LOGICAL_VERSION, branch1.inputAt(1).getOp().getOpType());
        LogicalVersionOperator branch1Version = (LogicalVersionOperator) branch1.inputAt(1).getOp();
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.FROM_VERSION,
                branch1Version.getVersionRefType());

        // Branch 2: Join(Version(TO) → left, Delta → right)
        OptExpression branch2 = result.get(0).inputAt(1);
        Assertions.assertTrue(branch2.getOp() instanceof LogicalJoinOperator);
        Assertions.assertEquals(OperatorType.LOGICAL_VERSION, branch2.inputAt(0).getOp().getOpType());
        LogicalVersionOperator branch2Version = (LogicalVersionOperator) branch2.inputAt(0).getOp();
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.TO_VERSION,
                branch2Version.getVersionRefType());
        Assertions.assertEquals(OperatorType.LOGICAL_DELTA, branch2.inputAt(1).getOp().getOpType());
        LogicalDeltaOperator branch2Delta = (LogicalDeltaOperator) branch2.inputAt(1).getOp();
        Assertions.assertFalse(branch2Delta.isRootDelta());
    }

    @Test
    public void testTransformUnionOutputsIncludeActionColumn(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator leftId = factory.create("a_id", IntegerType.INT, false);
        ColumnRefOperator rightId = factory.create("b_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);

        OptExpression leftScan = newIcebergScan(table, leftId, null,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression rightScan = newIcebergScan(table, rightId, null,
                TvrTableDelta.of(TvrVersion.of(300L), TvrVersion.of(400L)));

        LogicalJoinOperator join = new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null);
        OptExpression joinExpr = OptExpression.create(join, leftScan, rightScan);
        OptExpression input = OptExpression.create(new LogicalDeltaOperator(true, actionRef), joinExpr);
        deriveLogicalProperty(input);

        List<OptExpression> result = new IvmDeltaJoinRule().transform(input, context);

        // Union output columns should include __ACTION__
        LogicalUnionOperator union = (LogicalUnionOperator) result.get(0).getOp();
        boolean hasAction = union.getOutputColumnRefOp().stream()
                .anyMatch(col -> col.getId() == actionRef.getId());
        Assertions.assertTrue(hasAction, "Union outputs should include __ACTION__ column");
    }

    // ==================== IvmVersionJoinRule ====================

    @Test
    public void testVersionJoinPushDown(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator leftId = factory.create("a_id", IntegerType.INT, false);
        ColumnRefOperator rightId = factory.create("b_id", IntegerType.INT, false);

        OptExpression leftScan = newIcebergScan(table, leftId, null,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression rightScan = newIcebergScan(table, rightId, null,
                TvrTableDelta.of(TvrVersion.of(300L), TvrVersion.of(400L)));

        LogicalJoinOperator join = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, leftId, rightId));
        OptExpression joinExpr = OptExpression.create(join, leftScan, rightScan);
        // Version(FROM) → Join(left, right)
        OptExpression input = OptExpression.create(LogicalVersionOperator.fromVersion(), joinExpr);
        deriveLogicalProperty(input);

        List<OptExpression> result = new IvmVersionJoinRule().transform(input, context);

        // Should produce: Join(Version(FROM) → left, Version(FROM) → right)
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalJoinOperator);

        // Left child: Version(FROM)
        Assertions.assertEquals(OperatorType.LOGICAL_VERSION, result.get(0).inputAt(0).getOp().getOpType());
        LogicalVersionOperator leftVersion = (LogicalVersionOperator) result.get(0).inputAt(0).getOp();
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.FROM_VERSION, leftVersion.getVersionRefType());
        Assertions.assertTrue(result.get(0).inputAt(0).inputAt(0).getOp() instanceof LogicalIcebergScanOperator);

        // Right child: Version(FROM)
        Assertions.assertEquals(OperatorType.LOGICAL_VERSION, result.get(0).inputAt(1).getOp().getOpType());
        LogicalVersionOperator rightVersion = (LogicalVersionOperator) result.get(0).inputAt(1).getOp();
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.FROM_VERSION, rightVersion.getVersionRefType());
        Assertions.assertTrue(result.get(0).inputAt(1).inputAt(0).getOp() instanceof LogicalIcebergScanOperator);
    }

    @Test
    public void testVersionJoinPreservesToVersion(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator leftId = factory.create("a_id", IntegerType.INT, false);
        ColumnRefOperator rightId = factory.create("b_id", IntegerType.INT, false);

        OptExpression leftScan = newIcebergScan(table, leftId, null,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression rightScan = newIcebergScan(table, rightId, null,
                TvrTableDelta.of(TvrVersion.of(300L), TvrVersion.of(400L)));

        LogicalJoinOperator join = new LogicalJoinOperator(JoinOperator.INNER_JOIN, null);
        OptExpression joinExpr = OptExpression.create(join, leftScan, rightScan);
        // Version(TO) → Join
        OptExpression input = OptExpression.create(LogicalVersionOperator.toVersion(), joinExpr);
        deriveLogicalProperty(input);

        List<OptExpression> result = new IvmVersionJoinRule().transform(input, context);

        // Both children should have Version(TO)
        LogicalVersionOperator leftVersion = (LogicalVersionOperator) result.get(0).inputAt(0).getOp();
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.TO_VERSION, leftVersion.getVersionRefType());
        LogicalVersionOperator rightVersion = (LogicalVersionOperator) result.get(0).inputAt(1).getOp();
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.TO_VERSION, rightVersion.getVersionRefType());
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

    private OptExpression buildDeltaJoin(ColumnRefFactory factory, IcebergTable table, JoinOperator joinType) {
        ColumnRefOperator leftId = factory.create("a_id", IntegerType.INT, false);
        ColumnRefOperator rightId = factory.create("b_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);

        OptExpression leftScan = newIcebergScan(table, leftId, null,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression rightScan = newIcebergScan(table, rightId, null,
                TvrTableDelta.of(TvrVersion.of(300L), TvrVersion.of(400L)));

        LogicalJoinOperator join = new LogicalJoinOperator(joinType,
                joinType.isCrossJoin() ? null :
                        new BinaryPredicateOperator(BinaryType.EQ, leftId, rightId));
        OptExpression joinExpr = OptExpression.create(join, leftScan, rightScan);
        return OptExpression.create(new LogicalDeltaOperator(true, actionRef), joinExpr);
    }

    private OptExpression newIcebergScan(IcebergTable table, ColumnRefOperator idRef,
                                          ColumnRefOperator dataRef,
                                          com.starrocks.common.tvr.TvrVersionRange versionRange) {
        Column idCol = new Column("id", IntegerType.INT, false);
        Map<ColumnRefOperator, Column> colRefMap = Maps.newHashMap();
        colRefMap.put(idRef, idCol);
        if (dataRef != null) {
            Column dataCol = new Column("data", StringType.STRING, true);
            colRefMap.put(dataRef, dataCol);
        }
        LogicalIcebergScanOperator scan = new LogicalIcebergScanOperator.Builder()
                .setTable(table)
                .setColRefToColumnMetaMap(colRefMap)
                .setTableVersionRange(versionRange)
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
