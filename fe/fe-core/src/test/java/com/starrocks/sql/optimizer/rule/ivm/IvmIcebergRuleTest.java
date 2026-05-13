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
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersion;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
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
 * Tests for IVM delta/version rules on Iceberg scan operators.
 * Covers: IvmDeltaIcebergScanRule, IvmVersionIcebergScanRule,
 *         IvmDeltaFilterRule, IvmDeltaProjectRule,
 *         IvmVersionFilterRule, IvmVersionProjectRule.
 */
public class IvmIcebergRuleTest {

    // ==================== IvmDeltaIcebergScanRule ====================

    @Test
    public void testDeltaIcebergScan(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);

        OptExpression scanExpr = newIcebergScan(table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression deltaExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef), scanExpr);
        deriveLogicalProperty(deltaExpr);

        List<OptExpression> result = new IvmDeltaIcebergScanRule().transform(deltaExpr, context);

        // Should produce: Project(id, data, __ACTION__=0) → IcebergScan
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalProjectOperator);
        LogicalProjectOperator project = (LogicalProjectOperator) result.get(0).getOp();
        // __ACTION__ = 0 (constant)
        Assertions.assertTrue(project.getColumnRefMap().containsKey(actionRef));
        ScalarOperator actionExpr = project.getColumnRefMap().get(actionRef);
        Assertions.assertTrue(actionExpr instanceof ConstantOperator);
        Assertions.assertEquals((byte) 0, ((ConstantOperator) actionExpr).getTinyInt());
        // Project should pass through original columns
        Assertions.assertTrue(project.getColumnRefMap().containsKey(idRef));
        Assertions.assertTrue(project.getColumnRefMap().containsKey(dataRef));
        // Child should be IcebergScan
        Assertions.assertTrue(result.get(0).inputAt(0).getOp() instanceof LogicalIcebergScanOperator);
    }

    @Test
    public void testDeltaIcebergScanEmptyDelta(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);

        // from == to → empty delta
        OptExpression scanExpr = newIcebergScan(table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(100L)));
        OptExpression deltaExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef), scanExpr);
        deriveLogicalProperty(deltaExpr);

        List<OptExpression> result = new IvmDeltaIcebergScanRule().transform(deltaExpr, context);

        // Should produce empty LogicalValuesOperator with __ACTION__ column preserved
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalValuesOperator);
        LogicalValuesOperator values = (LogicalValuesOperator) result.get(0).getOp();
        // Must include __ACTION__ column so parent projections don't break
        Assertions.assertTrue(values.getColumnRefSet().contains(actionRef),
                "Empty values must include __ACTION__ column to preserve column dependencies");
        // Also includes original scan columns
        Assertions.assertTrue(values.getColumnRefSet().contains(idRef));
        Assertions.assertTrue(values.getColumnRefSet().contains(dataRef));
    }

    @Test
    public void testDeltaIcebergScanCheckRejectsNonDelta(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);

        // Use TvrTableSnapshot instead of TvrTableDelta → check() should return false
        OptExpression scanExpr = newIcebergScan(table, idRef, dataRef,
                TvrTableSnapshot.of(java.util.Optional.of(100L)));
        OptExpression deltaExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef), scanExpr);
        deriveLogicalProperty(deltaExpr);

        boolean checkResult = new IvmDeltaIcebergScanRule().check(deltaExpr, context);
        Assertions.assertFalse(checkResult);
    }

    // ==================== IvmVersionIcebergScanRule ====================

    @Test
    public void testVersionIcebergScanFromVersion(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);

        OptExpression scanExpr = newIcebergScan(table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression versionExpr = OptExpression.create(LogicalVersionOperator.fromVersion(), scanExpr);
        deriveLogicalProperty(versionExpr);

        List<OptExpression> result = new IvmVersionIcebergScanRule().transform(versionExpr, context);

        // Should produce IcebergScan with snapshot = 100 (from version)
        Assertions.assertEquals(1, result.size());
        LogicalIcebergScanOperator newScan = (LogicalIcebergScanOperator) result.get(0).getOp();
        Assertions.assertTrue(newScan.getTvrVersionRange() instanceof TvrTableSnapshot);
        TvrTableSnapshot snapshot = (TvrTableSnapshot) newScan.getTvrVersionRange();
        Assertions.assertEquals(100L, snapshot.getSnapshotId());
    }

    @Test
    public void testVersionIcebergScanToVersion(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);

        OptExpression scanExpr = newIcebergScan(table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        OptExpression versionExpr = OptExpression.create(LogicalVersionOperator.toVersion(), scanExpr);
        deriveLogicalProperty(versionExpr);

        List<OptExpression> result = new IvmVersionIcebergScanRule().transform(versionExpr, context);

        // Should produce IcebergScan with snapshot = 200 (to version)
        Assertions.assertEquals(1, result.size());
        LogicalIcebergScanOperator newScan = (LogicalIcebergScanOperator) result.get(0).getOp();
        Assertions.assertTrue(newScan.getTvrVersionRange() instanceof TvrTableSnapshot);
        TvrTableSnapshot snapshot = (TvrTableSnapshot) newScan.getTvrVersionRange();
        Assertions.assertEquals(200L, snapshot.getSnapshotId());
    }

    // ==================== IvmDeltaFilterRule ====================

    @Test
    public void testDeltaFilterPushDown(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);

        OptExpression scanExpr = newIcebergScan(table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        LogicalFilterOperator filter = new LogicalFilterOperator(
                new BinaryPredicateOperator(BinaryType.GT, idRef, ConstantOperator.createInt(10)));
        // Delta → Filter → Scan
        OptExpression deltaExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(filter, scanExpr));
        deriveLogicalProperty(deltaExpr);

        List<OptExpression> result = new IvmDeltaFilterRule().transform(deltaExpr, context);

        // Should produce: Filter(with __ACTION__ in projection) → Delta → Scan
        Assertions.assertEquals(1, result.size());
        // Root should be Filter
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalFilterOperator);
        LogicalFilterOperator newFilter = (LogicalFilterOperator) result.get(0).getOp();
        // Filter should have projection containing actionRef
        Assertions.assertNotNull(newFilter.getProjection());
        Assertions.assertTrue(newFilter.getProjection().getColumnRefMap().containsKey(actionRef));
        // Child should be Delta
        Assertions.assertEquals(OperatorType.LOGICAL_DELTA, result.get(0).inputAt(0).getOp().getOpType());
        // Grandchild should be IcebergScan
        Assertions.assertTrue(result.get(0).inputAt(0).inputAt(0).getOp() instanceof LogicalIcebergScanOperator);
    }

    // ==================== IvmDeltaProjectRule ====================

    @Test
    public void testDeltaProjectPushDown(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        ColumnRefOperator exprRef = factory.create("expr", IntegerType.INT, false);
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);

        OptExpression scanExpr = newIcebergScan(table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        // Project: expr = id * 2, data = data
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        projectMap.put(exprRef, idRef);
        projectMap.put(dataRef, dataRef);
        LogicalProjectOperator project = new LogicalProjectOperator(projectMap);
        // Delta → Project → Scan
        OptExpression deltaExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(project, scanExpr));
        deriveLogicalProperty(deltaExpr);

        List<OptExpression> result = new IvmDeltaProjectRule().transform(deltaExpr, context);

        // Should produce: Project(expr, data, __ACTION__) → Delta → Scan
        Assertions.assertEquals(1, result.size());
        // Root should be Project
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalProjectOperator);
        LogicalProjectOperator newProject = (LogicalProjectOperator) result.get(0).getOp();
        // Project should contain __ACTION__ pass-through
        Assertions.assertTrue(newProject.getColumnRefMap().containsKey(actionRef));
        Assertions.assertEquals(actionRef, newProject.getColumnRefMap().get(actionRef));
        // Child should be Delta with isRootDelta preserved
        LogicalDeltaOperator newDelta = (LogicalDeltaOperator) result.get(0).inputAt(0).getOp();
        Assertions.assertTrue(newDelta.isRootDelta());
        Assertions.assertEquals(actionRef, newDelta.getActionColumn());
    }

    // ==================== IvmVersionFilterRule ====================

    @Test
    public void testVersionFilterPushDown(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);

        OptExpression scanExpr = newIcebergScan(table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        LogicalFilterOperator filter = new LogicalFilterOperator(
                new BinaryPredicateOperator(BinaryType.GT, idRef, ConstantOperator.createInt(10)));
        // Version → Filter → Scan
        OptExpression versionExpr = OptExpression.create(LogicalVersionOperator.fromVersion(),
                OptExpression.create(filter, scanExpr));
        deriveLogicalProperty(versionExpr);

        List<OptExpression> result = new IvmVersionFilterRule().transform(versionExpr, context);

        // Should produce: Filter → Version → Scan
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalFilterOperator);
        Assertions.assertEquals(OperatorType.LOGICAL_VERSION, result.get(0).inputAt(0).getOp().getOpType());
        LogicalVersionOperator newVersion = (LogicalVersionOperator) result.get(0).inputAt(0).getOp();
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.FROM_VERSION, newVersion.getVersionRefType());
        Assertions.assertTrue(result.get(0).inputAt(0).inputAt(0).getOp() instanceof LogicalIcebergScanOperator);
    }

    // ==================== IvmVersionProjectRule ====================

    @Test
    public void testVersionProjectPushDown(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);

        OptExpression scanExpr = newIcebergScan(table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        projectMap.put(idRef, idRef);
        LogicalProjectOperator project = new LogicalProjectOperator(projectMap);
        // Version → Project → Scan
        OptExpression versionExpr = OptExpression.create(LogicalVersionOperator.toVersion(),
                OptExpression.create(project, scanExpr));
        deriveLogicalProperty(versionExpr);

        List<OptExpression> result = new IvmVersionProjectRule().transform(versionExpr, context);

        // Should produce: Project → Version → Scan
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof LogicalProjectOperator);
        Assertions.assertEquals(OperatorType.LOGICAL_VERSION, result.get(0).inputAt(0).getOp().getOpType());
        LogicalVersionOperator newVersion = (LogicalVersionOperator) result.get(0).inputAt(0).getOp();
        Assertions.assertEquals(LogicalVersionOperator.VersionRefType.TO_VERSION, newVersion.getVersionRefType());
        Assertions.assertTrue(result.get(0).inputAt(0).inputAt(0).getOp() instanceof LogicalIcebergScanOperator);
    }

    // ==================== Full pipeline: Delta through Filter+Project to Scan ====================

    @Test
    public void testFullPipelineDeltaThroughFilterProjectToScan(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        ColumnRefOperator exprRef = factory.create("expr", IntegerType.INT, false);
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);

        OptExpression scanExpr = newIcebergScan(table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));

        // Build: Delta → Project(expr=id+1, data) → Filter(id>10) → Scan
        LogicalFilterOperator filter = new LogicalFilterOperator(
                new BinaryPredicateOperator(BinaryType.GT, idRef, ConstantOperator.createInt(10)));
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        projectMap.put(exprRef, idRef);
        projectMap.put(dataRef, dataRef);
        LogicalProjectOperator project = new LogicalProjectOperator(projectMap);

        OptExpression tree = OptExpression.create(new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(project,
                        OptExpression.create(filter, scanExpr)));
        deriveLogicalProperty(tree);

        // Step 1: Delta pushes through Project
        List<OptExpression> afterProject = new IvmDeltaProjectRule().transform(tree, context);
        Assertions.assertEquals(1, afterProject.size());
        // Now: Project(expr, data, __ACTION__) → Delta → Filter → Scan
        deriveLogicalProperty(afterProject.get(0));

        // Step 2: Delta pushes through Filter
        OptExpression deltaFilter = afterProject.get(0).inputAt(0); // Delta → Filter → Scan
        List<OptExpression> afterFilter = new IvmDeltaFilterRule().transform(deltaFilter, context);
        Assertions.assertEquals(1, afterFilter.size());
        // Now: Filter(with __ACTION__) → Delta → Scan
        deriveLogicalProperty(afterFilter.get(0));

        // Step 3: Delta resolves at Iceberg Scan
        OptExpression deltaScan = afterFilter.get(0).inputAt(0); // Delta → Scan
        Assertions.assertTrue(new IvmDeltaIcebergScanRule().check(deltaScan, context));
        List<OptExpression> afterScan = new IvmDeltaIcebergScanRule().transform(deltaScan, context);
        Assertions.assertEquals(1, afterScan.size());
        // Now: Project(id, data, __ACTION__=1) → Scan — Delta fully eliminated

        // Verify no Delta or Version markers remain in the final sub-tree
        Assertions.assertFalse(IvmRuleUtils.containsLogicalDelta(afterScan.get(0)));
        Assertions.assertFalse(IvmRuleUtils.containsLogicalVersion(afterScan.get(0)));

        // __ACTION__ = 0 (constant)
        LogicalProjectOperator scanProject = (LogicalProjectOperator) afterScan.get(0).getOp();
        ScalarOperator actionValue = scanProject.getColumnRefMap().get(actionRef);
        Assertions.assertTrue(actionValue instanceof ConstantOperator);
        Assertions.assertEquals((byte) 0, ((ConstantOperator) actionValue).getTinyInt());
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

    private OptExpression newIcebergScan(IcebergTable table, ColumnRefOperator idRef,
                                         ColumnRefOperator dataRef,
                                         com.starrocks.common.tvr.TvrVersionRange versionRange) {
        Column idCol = new Column("id", IntegerType.INT, false);
        Column dataCol = new Column("data", StringType.STRING, true);
        Map<ColumnRefOperator, Column> colRefMap = Maps.newHashMap();
        colRefMap.put(idRef, idCol);
        colRefMap.put(dataRef, dataCol);
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
        ExpressionContext context = new ExpressionContext(expression);
        context.deriveLogicalProperty();
        expression.setLogicalProperty(context.getRootProperty());
    }
}
