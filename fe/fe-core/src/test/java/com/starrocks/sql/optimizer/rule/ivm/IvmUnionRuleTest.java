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

import com.google.common.collect.Lists;
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
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
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

public class IvmUnionRuleTest {

    // ==================== IvmDeltaUnionRule ====================

    @Test
    public void testCheckAcceptsUnionAll(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        OptExpression input = buildDeltaUnion(factory, table, true);
        deriveLogicalProperty(input);

        Assertions.assertTrue(new IvmDeltaUnionRule().check(input, context));
    }

    @Test
    public void testCheckRejectsUnionDistinct(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        OptExpression input = buildDeltaUnion(factory, table, false);
        deriveLogicalProperty(input);

        Assertions.assertFalse(new IvmDeltaUnionRule().check(input, context));
    }

    @Test
    public void testTransformPushesDeltaToEachChild(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator leftId = factory.create("a_id", IntegerType.INT, false);
        ColumnRefOperator leftData = factory.create("a_data", StringType.STRING, true);
        ColumnRefOperator rightId = factory.create("b_id", IntegerType.INT, false);
        ColumnRefOperator rightData = factory.create("b_data", StringType.STRING, true);
        ColumnRefOperator unionId = factory.create("u_id", IntegerType.INT, false);
        ColumnRefOperator unionData = factory.create("u_data", StringType.STRING, true);
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);

        OptExpression leftScan = newIcebergScan(table, leftId, leftData,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression rightScan = newIcebergScan(table, rightId, rightData,
                TvrTableDelta.of(TvrVersion.of(300L), TvrVersion.of(400L)));

        LogicalUnionOperator union = new LogicalUnionOperator(
                Lists.newArrayList(unionId, unionData),
                Lists.newArrayList(Lists.newArrayList(leftId, leftData), Lists.newArrayList(rightId, rightData)),
                true);
        OptExpression unionExpr = OptExpression.create(union, leftScan, rightScan);
        OptExpression input = OptExpression.create(new LogicalDeltaOperator(true, actionRef), unionExpr);
        deriveLogicalProperty(input);

        List<OptExpression> result = new IvmDeltaUnionRule().transform(input, context);

        // Root should be Union
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalUnionOperator);
        LogicalUnionOperator newUnion = (LogicalUnionOperator) result.get(0).getOp();
        Assertions.assertTrue(newUnion.isUnionAll());

        // Both children should be Delta (isRootDelta=false)
        Assertions.assertEquals(2, result.get(0).getInputs().size());
        for (OptExpression child : result.get(0).getInputs()) {
            Assertions.assertEquals(OperatorType.LOGICAL_DELTA, child.getOp().getOpType());
            LogicalDeltaOperator childDelta = (LogicalDeltaOperator) child.getOp();
            Assertions.assertFalse(childDelta.isRootDelta(), "Child delta should NOT be root delta");
            Assertions.assertNotNull(childDelta.getActionColumn());
        }

        // Union output should include the __ACTION__ column
        boolean hasAction = newUnion.getOutputColumnRefOp().stream()
                .anyMatch(col -> col.getId() == actionRef.getId());
        Assertions.assertTrue(hasAction, "Union output should include __ACTION__ column");

        // Each child's output list should have one extra column (the branch action)
        for (int i = 0; i < 2; i++) {
            List<ColumnRefOperator> childOutputs = newUnion.getChildOutputColumns().get(i);
            // Original had 2 columns, now 3 with branch action
            Assertions.assertEquals(3, childOutputs.size());
        }
    }

    @Test
    public void testTransformWithThreeChildren(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator id1 = factory.create("id1", IntegerType.INT, false);
        ColumnRefOperator id2 = factory.create("id2", IntegerType.INT, false);
        ColumnRefOperator id3 = factory.create("id3", IntegerType.INT, false);
        ColumnRefOperator unionId = factory.create("u_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);

        OptExpression scan1 = newIcebergScan(table, id1, null,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression scan2 = newIcebergScan(table, id2, null,
                TvrTableDelta.of(TvrVersion.of(300L), TvrVersion.of(400L)));
        OptExpression scan3 = newIcebergScan(table, id3, null,
                TvrTableDelta.of(TvrVersion.of(500L), TvrVersion.of(600L)));

        LogicalUnionOperator union = new LogicalUnionOperator(
                Lists.newArrayList(unionId),
                Lists.newArrayList(Lists.newArrayList(id1), Lists.newArrayList(id2), Lists.newArrayList(id3)),
                true);
        OptExpression unionExpr = OptExpression.create(union, scan1, scan2, scan3);
        OptExpression input = OptExpression.create(new LogicalDeltaOperator(true, actionRef), unionExpr);
        deriveLogicalProperty(input);

        List<OptExpression> result = new IvmDeltaUnionRule().transform(input, context);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(3, result.get(0).getInputs().size());
        for (OptExpression child : result.get(0).getInputs()) {
            Assertions.assertEquals(OperatorType.LOGICAL_DELTA, child.getOp().getOpType());
        }
    }

    // ==================== IvmVersionUnionRule ====================

    @Test
    public void testVersionUnionPushDown(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator id1 = factory.create("id1", IntegerType.INT, false);
        ColumnRefOperator id2 = factory.create("id2", IntegerType.INT, false);
        ColumnRefOperator unionId = factory.create("u_id", IntegerType.INT, false);

        OptExpression scan1 = newIcebergScan(table, id1, null,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression scan2 = newIcebergScan(table, id2, null,
                TvrTableDelta.of(TvrVersion.of(300L), TvrVersion.of(400L)));

        LogicalUnionOperator union = new LogicalUnionOperator(
                Lists.newArrayList(unionId),
                Lists.newArrayList(Lists.newArrayList(id1), Lists.newArrayList(id2)),
                true);
        OptExpression unionExpr = OptExpression.create(union, scan1, scan2);
        OptExpression input = OptExpression.create(LogicalVersionOperator.fromVersion(), unionExpr);
        deriveLogicalProperty(input);

        List<OptExpression> result = new IvmVersionUnionRule().transform(input, context);

        // Root should be Union
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalUnionOperator);

        // Each child should be Version(FROM) → Scan
        Assertions.assertEquals(2, result.get(0).getInputs().size());
        for (OptExpression child : result.get(0).getInputs()) {
            Assertions.assertEquals(OperatorType.LOGICAL_VERSION, child.getOp().getOpType());
            LogicalVersionOperator childVersion = (LogicalVersionOperator) child.getOp();
            Assertions.assertEquals(LogicalVersionOperator.VersionRefType.FROM_VERSION,
                    childVersion.getVersionRefType());
            Assertions.assertTrue(child.inputAt(0).getOp() instanceof LogicalIcebergScanOperator);
        }
    }

    @Test
    public void testVersionUnionPreservesToVersion(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator id1 = factory.create("id1", IntegerType.INT, false);
        ColumnRefOperator id2 = factory.create("id2", IntegerType.INT, false);
        ColumnRefOperator unionId = factory.create("u_id", IntegerType.INT, false);

        OptExpression scan1 = newIcebergScan(table, id1, null,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression scan2 = newIcebergScan(table, id2, null,
                TvrTableDelta.of(TvrVersion.of(300L), TvrVersion.of(400L)));

        LogicalUnionOperator union = new LogicalUnionOperator(
                Lists.newArrayList(unionId),
                Lists.newArrayList(Lists.newArrayList(id1), Lists.newArrayList(id2)),
                true);
        OptExpression unionExpr = OptExpression.create(union, scan1, scan2);
        OptExpression input = OptExpression.create(LogicalVersionOperator.toVersion(), unionExpr);
        deriveLogicalProperty(input);

        List<OptExpression> result = new IvmVersionUnionRule().transform(input, context);

        // All children should preserve TO_VERSION
        for (OptExpression child : result.get(0).getInputs()) {
            LogicalVersionOperator childVersion = (LogicalVersionOperator) child.getOp();
            Assertions.assertEquals(LogicalVersionOperator.VersionRefType.TO_VERSION,
                    childVersion.getVersionRefType());
        }
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

    private OptExpression buildDeltaUnion(ColumnRefFactory factory, IcebergTable table, boolean isUnionAll) {
        ColumnRefOperator id1 = factory.create("id1", IntegerType.INT, false);
        ColumnRefOperator id2 = factory.create("id2", IntegerType.INT, false);
        ColumnRefOperator unionId = factory.create("u_id", IntegerType.INT, false);
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);

        OptExpression scan1 = newIcebergScan(table, id1, null,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression scan2 = newIcebergScan(table, id2, null,
                TvrTableDelta.of(TvrVersion.of(300L), TvrVersion.of(400L)));

        LogicalUnionOperator union = new LogicalUnionOperator(
                Lists.newArrayList(unionId),
                Lists.newArrayList(Lists.newArrayList(id1), Lists.newArrayList(id2)),
                isUnionAll);
        OptExpression unionExpr = OptExpression.create(union, scan1, scan2);
        return OptExpression.create(new LogicalDeltaOperator(true, actionRef), unionExpr);
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
