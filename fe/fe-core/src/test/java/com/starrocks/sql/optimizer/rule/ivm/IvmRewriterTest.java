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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrVersion;
import com.starrocks.load.Load;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TaskScheduler;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Tests for {@link IvmRewriter} covering:
 * - Gate: skip when IVM refresh not enabled
 * - Convergence success: Delta markers eliminated for supported patterns
 * - Convergence failure: original plan restored for unsupported patterns
 * - appendPkLoadOpColumn: __op column + TopN for PK MVs
 * - isPrimaryKeyTargetMv: various statement types
 */
public class IvmRewriterTest {

    // ==================== Gate: isEnableIVMRefresh ====================

    @Test
    public void testRewriteSkipsWhenIvmDisabled(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        // IVM refresh is disabled by default
        Assertions.assertFalse(context.getSessionVariable().isEnableIVMRefresh());

        OptExpression scan = newIcebergScan(factory, table);
        // Wrap in LogicalTreeAnchorOperator (required by RewriteTreeTask)
        OptExpression root = OptExpression.create(new LogicalTreeAnchorOperator(), scan);
        deriveLogicalProperty(root);
        String originalPlanDigest = IvmRuleUtils.structureDigest(scan);

        TaskContext taskContext = newTaskContext(context);
        TaskScheduler scheduler = new TaskScheduler();
        ColumnRefSet requiredColumns = new ColumnRefSet();

        IvmRewriter.rewrite(root, taskContext, scheduler, requiredColumns);

        // Plan should be unchanged — no Delta operator injected
        Assertions.assertFalse(IvmRuleUtils.containsLogicalDelta(root));
        Assertions.assertSame(scan, root.inputAt(0));
        Assertions.assertEquals(originalPlanDigest, IvmRuleUtils.structureDigest(scan));
    }

    // ==================== Convergence: success ====================

    @Test
    public void testRewriteSucceedsForIcebergScan(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        context.getSessionVariable().setEnableIVMRefresh(true);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        OptExpression scan = newIcebergScan(factory, table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));

        // Wrap: root → scan (simulating INSERT target → query)
        OptExpression root = OptExpression.create(new LogicalTreeAnchorOperator(), scan);
        deriveLogicalProperty(root);
        String originalPlanDigest = IvmRuleUtils.structureDigest(scan);

        TaskContext taskContext = newTaskContext(context);
        TaskScheduler scheduler = new TaskScheduler();
        ColumnRefSet requiredColumns = new ColumnRefSet();

        IvmRewriter.rewrite(root, taskContext, scheduler, requiredColumns);

        // Delta/Version markers should be fully eliminated
        Assertions.assertFalse(IvmRuleUtils.containsLogicalDelta(root));
        Assertions.assertFalse(IvmRuleUtils.containsLogicalVersion(root));
        // Root child should no longer be the original scan (plan was rewritten)
        Assertions.assertNotSame(scan, root.inputAt(0));
        Assertions.assertEquals(originalPlanDigest, IvmRuleUtils.structureDigest(scan));
    }

    // ==================== Convergence: failure → fallback ====================

    @Test
    public void testRewriteFallsBackForUnsupportedOperator(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        context.getSessionVariable().setEnableIVMRefresh(true);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);

        // Use a scan without tvrVersionRange (no TvrTableDelta) → DeltaIcebergScanRule won't match
        OptExpression scan = newIcebergScan(factory, table, idRef, dataRef, null);

        OptExpression root = OptExpression.create(new LogicalTreeAnchorOperator(), scan);
        deriveLogicalProperty(root);
        OptExpression originalChild = root.inputAt(0);
        String originalPlanDigest = IvmRuleUtils.structureDigest(originalChild);

        TaskContext taskContext = newTaskContext(context);
        TaskScheduler scheduler = new TaskScheduler();
        ColumnRefSet requiredColumns = new ColumnRefSet();
        ColumnRefSet requiredColumnsBefore = requiredColumns.clone();
        ColumnRefSet taskRequiredColumnsBefore = taskContext.getRequiredColumns().clone();

        IvmRewriter.rewrite(root, taskContext, scheduler, requiredColumns);

        // Convergence failed → original plan restored
        Assertions.assertFalse(IvmRuleUtils.containsLogicalDelta(root));
        Assertions.assertSame(originalChild, root.inputAt(0));
        Assertions.assertEquals(originalPlanDigest, IvmRuleUtils.structureDigest(originalChild));
        Assertions.assertEquals(requiredColumnsBefore, requiredColumns);
        Assertions.assertEquals(taskRequiredColumnsBefore, taskContext.getRequiredColumns());
    }

    @Test
    public void testRewriteRestoresOriginalPlanWhenRewriteThrows(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        context.getSessionVariable().setEnableIVMRefresh(true);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        OptExpression scan = newIcebergScan(factory, table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));

        OptExpression root = OptExpression.create(new LogicalTreeAnchorOperator(), scan);
        deriveLogicalProperty(root);
        OptExpression originalChild = root.inputAt(0);
        String originalPlanDigest = IvmRuleUtils.structureDigest(originalChild);

        TaskContext taskContext = newTaskContext(context);
        ColumnRefSet requiredColumns = new ColumnRefSet();
        requiredColumns.union(idRef);
        taskContext.getRequiredColumns().union(dataRef);
        ColumnRefSet requiredColumnsBefore = requiredColumns.clone();
        ColumnRefSet taskRequiredColumnsBefore = taskContext.getRequiredColumns().clone();
        TaskScheduler throwingScheduler = new TaskScheduler() {
            @Override
            public void rewriteIterative(OptExpression rewriteTree, TaskContext rewriteTaskContext,
                                         com.starrocks.sql.optimizer.rule.Rule rule) {
                throw new RuntimeException("injected IVM rewrite failure");
            }
        };

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> IvmRewriter.rewrite(root, taskContext, throwingScheduler, requiredColumns));
        Assertions.assertEquals("injected IVM rewrite failure", exception.getMessage());

        Assertions.assertSame(originalChild, root.inputAt(0));
        Assertions.assertEquals(originalPlanDigest, IvmRuleUtils.structureDigest(originalChild));
        Assertions.assertEquals(requiredColumnsBefore, requiredColumns);
        Assertions.assertEquals(taskRequiredColumnsBefore, taskContext.getRequiredColumns());
    }

    // ==================== appendPkLoadOpColumn ====================

    @Test
    public void testAppendPkLoadOpColumnForPkMv(@Mocked IcebergTable table,
                                                 @Mocked MaterializedView targetMv,
                                                 @Mocked InsertStmt insertStmt) {
        mockIcebergTable(table);
        new Expectations() {
            {
                insertStmt.getTargetTable();
                result = targetMv;
                minTimes = 0;

                targetMv.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
            }
        };

        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        context.getSessionVariable().setEnableIVMRefresh(true);
        context.setStatement(insertStmt);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        OptExpression scan = newIcebergScan(factory, table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));

        OptExpression root = OptExpression.create(new LogicalTreeAnchorOperator(), scan);
        deriveLogicalProperty(root);

        TaskContext taskContext = newTaskContext(context);
        TaskScheduler scheduler = new TaskScheduler();
        ColumnRefSet requiredColumns = new ColumnRefSet();

        IvmRewriter.rewrite(root, taskContext, scheduler, requiredColumns);

        // Append-only Iceberg → constant __ACTION__ → TopN is skipped. Top is Project(__op).
        OptExpression rewrittenChild = root.inputAt(0);
        Assertions.assertFalse(rewrittenChild.getOp() instanceof LogicalTopNOperator);
        Assertions.assertTrue(rewrittenChild.getOp() instanceof LogicalProjectOperator);
        LogicalProjectOperator opProjectOp = (LogicalProjectOperator) rewrittenChild.getOp();
        // Should contain __op column
        boolean hasOpColumn = opProjectOp.getColumnRefMap().keySet().stream()
                .anyMatch(col -> Load.LOAD_OP_COLUMN.equalsIgnoreCase(col.getName()));
        Assertions.assertTrue(hasOpColumn, "PK MV should have __op column");
        // Should NOT contain __ACTION__ column (replaced by __op)
        boolean hasActionColumn = opProjectOp.getColumnRefMap().keySet().stream()
                .anyMatch(col -> IvmRuleUtils.ACTION_COLUMN_NAME.equalsIgnoreCase(col.getName()));
        Assertions.assertFalse(hasActionColumn, "__ACTION__ should be replaced by __op");
    }

    /**
     * Non-aggregate incremental MVs are PK tables, so
     * {@code IvmRewriter.appendPkLoadOpColumn} runs for them too.
     * Verify via the same mock-based plumbing: when the target MV has
     * {@link KeysType#PRIMARY_KEYS}, the rewritten plan contains {@code __op}.
     */
    @Test
    public void testNonAggregateIncrementalMvGetsOpColumn(@Mocked IcebergTable table,
                                                           @Mocked MaterializedView targetMv,
                                                           @Mocked InsertStmt insertStmt) {
        mockIcebergTable(table);
        new Expectations() {
            {
                insertStmt.getTargetTable();
                result = targetMv;
                minTimes = 0;

                // Non-aggregate incremental MVs are also PRIMARY_KEYS.
                targetMv.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
            }
        };

        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        context.getSessionVariable().setEnableIVMRefresh(true);
        context.setStatement(insertStmt);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        OptExpression scan = newIcebergScan(factory, table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));

        OptExpression root = OptExpression.create(new LogicalTreeAnchorOperator(), scan);
        deriveLogicalProperty(root);

        TaskContext taskContext = newTaskContext(context);
        TaskScheduler scheduler = new TaskScheduler();
        ColumnRefSet requiredColumns = new ColumnRefSet();

        IvmRewriter.rewrite(root, taskContext, scheduler, requiredColumns);

        // Append-only Iceberg → constant __ACTION__ → TopN is skipped. Top is Project(__op).
        OptExpression rewrittenChild = root.inputAt(0);
        Assertions.assertFalse(rewrittenChild.getOp() instanceof LogicalTopNOperator,
                "No TopN expected for append-only Iceberg (constant __ACTION__)");
        Assertions.assertTrue(rewrittenChild.getOp() instanceof LogicalProjectOperator);
        LogicalProjectOperator opProjectOp = (LogicalProjectOperator) rewrittenChild.getOp();
        boolean hasOpColumn = opProjectOp.getColumnRefMap().keySet().stream()
                .anyMatch(col -> Load.LOAD_OP_COLUMN.equalsIgnoreCase(col.getName()));
        Assertions.assertTrue(hasOpColumn,
                "Non-aggregate PK MV should have __op column (IvmRewriter.appendPkLoadOpColumn)");
    }

    /** {@code __op} must be a direct alias of {@code __ACTION__} (not a CASE WHEN). */
    @Test
    public void testLoadOpColumnIsDirectAliasOfActionColumn(@Mocked IcebergTable table,
                                                             @Mocked MaterializedView targetMv,
                                                             @Mocked InsertStmt insertStmt) {
        mockIcebergTable(table);
        new Expectations() {
            {
                insertStmt.getTargetTable();
                result = targetMv;
                minTimes = 0;

                targetMv.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
            }
        };

        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        context.getSessionVariable().setEnableIVMRefresh(true);
        context.setStatement(insertStmt);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        OptExpression scan = newIcebergScan(factory, table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));

        OptExpression root = OptExpression.create(new LogicalTreeAnchorOperator(), scan);
        deriveLogicalProperty(root);
        IvmRewriter.rewrite(root, newTaskContext(context), new TaskScheduler(), new ColumnRefSet());

        OptExpression opProject = root.inputAt(0);
        Assertions.assertTrue(opProject.getOp() instanceof LogicalProjectOperator);
        LogicalProjectOperator opProjectOp = (LogicalProjectOperator) opProject.getOp();
        ColumnRefOperator opCol = opProjectOp.getColumnRefMap().keySet().stream()
                .filter(col -> Load.LOAD_OP_COLUMN.equalsIgnoreCase(col.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("__op column must be produced"));
        ScalarOperator opExpr = opProjectOp.getColumnRefMap().get(opCol);
        Assertions.assertTrue(opExpr instanceof ColumnRefOperator,
                "__op must be a direct column reference (not CASE WHEN), but was: " + opExpr);
        Assertions.assertEquals(IvmRuleUtils.ACTION_COLUMN_NAME,
                ((ColumnRefOperator) opExpr).getName(),
                "__op must reference __ACTION__ directly");
    }

    /** TopN is skipped when {@code __ACTION__} is provably constant (no DELETEs to order). */
    @Test
    public void testTopNSkippedWhenActionConstant(@Mocked IcebergTable table,
                                                   @Mocked MaterializedView targetMv,
                                                   @Mocked InsertStmt insertStmt) {
        mockIcebergTable(table);
        new Expectations() {
            {
                insertStmt.getTargetTable();
                result = targetMv;
                minTimes = 0;

                targetMv.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
            }
        };

        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        context.getSessionVariable().setEnableIVMRefresh(true);
        context.setStatement(insertStmt);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        OptExpression scan = newIcebergScan(factory, table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));

        OptExpression root = OptExpression.create(new LogicalTreeAnchorOperator(), scan);
        deriveLogicalProperty(root);
        IvmRewriter.rewrite(root, newTaskContext(context), new TaskScheduler(), new ColumnRefSet());

        OptExpression top = root.inputAt(0);
        Assertions.assertTrue(top.getOp() instanceof LogicalProjectOperator,
                "top operator must be Project when __ACTION__ is constant; no TopN expected");
        Assertions.assertFalse(top.getOp() instanceof LogicalTopNOperator);
    }

    /**
     * Regression: when {@code __ACTION__} is forwarded through an aliasing projection
     * (e.g., {@code IvmDeltaFilterRule} attaches {@code action → action}), the constant
     * produced at the scan must still be detected — TopN is skipped.
     */
    @Test
    public void testTopNSkippedAcrossAliasForwardingProjection(@Mocked IcebergTable table,
                                                                @Mocked MaterializedView targetMv,
                                                                @Mocked InsertStmt insertStmt) {
        mockIcebergTable(table);
        new Expectations() {
            {
                insertStmt.getTargetTable();
                result = targetMv;
                minTimes = 0;

                targetMv.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
            }
        };

        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        context.getSessionVariable().setEnableIVMRefresh(true);
        context.setStatement(insertStmt);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        OptExpression scan = newIcebergScan(factory, table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));
        // Filter above scan → IvmDeltaFilterRule attaches a passthrough Projection carrying
        // `actionColumn → actionColumn` on the Filter operator. Without alias-chain walking,
        // isActionColumnConstant would miss the constant and keep the TopN.
        OptExpression filter = OptExpression.create(
                new LogicalFilterOperator(ConstantOperator.createBoolean(true)), scan);
        OptExpression root = OptExpression.create(new LogicalTreeAnchorOperator(), filter);
        deriveLogicalProperty(root);

        IvmRewriter.rewrite(root, newTaskContext(context), new TaskScheduler(), new ColumnRefSet());

        Assertions.assertFalse(containsTopN(root.inputAt(0)),
                "TopN must be skipped when __ACTION__ is constant, even through alias forwarding");
    }

    private static boolean containsTopN(OptExpression expr) {
        if (expr.getOp() instanceof LogicalTopNOperator) {
            return true;
        }
        for (OptExpression child : expr.getInputs()) {
            if (containsTopN(child)) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void testNoPkLoadOpColumnForDupKeysMv(@Mocked IcebergTable table,
                                                  @Mocked MaterializedView targetMv,
                                                  @Mocked InsertStmt insertStmt) {
        mockIcebergTable(table);
        new Expectations() {
            {
                insertStmt.getTargetTable();
                result = targetMv;
                minTimes = 0;

                targetMv.getKeysType();
                result = KeysType.DUP_KEYS;
                minTimes = 0;
            }
        };

        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        context.getSessionVariable().setEnableIVMRefresh(true);
        context.setStatement(insertStmt);

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        OptExpression scan = newIcebergScan(factory, table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));

        OptExpression root = OptExpression.create(new LogicalTreeAnchorOperator(), scan);
        deriveLogicalProperty(root);

        TaskContext taskContext = newTaskContext(context);
        TaskScheduler scheduler = new TaskScheduler();
        ColumnRefSet requiredColumns = new ColumnRefSet();

        IvmRewriter.rewrite(root, taskContext, scheduler, requiredColumns);

        // For DUP_KEYS MV, no TopN or __op should be added
        OptExpression rewrittenChild = root.inputAt(0);
        Assertions.assertFalse(rewrittenChild.getOp() instanceof LogicalTopNOperator,
                "DUP_KEYS MV should NOT have TopN");
    }

    @Test
    public void testNoStatementIsNotPkMv(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        context.getSessionVariable().setEnableIVMRefresh(true);
        // No statement set → isPrimaryKeyTargetMv returns false

        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        OptExpression scan = newIcebergScan(factory, table, idRef, dataRef,
                TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)));

        OptExpression root = OptExpression.create(new LogicalTreeAnchorOperator(), scan);
        deriveLogicalProperty(root);

        TaskContext taskContext = newTaskContext(context);
        TaskScheduler scheduler = new TaskScheduler();
        ColumnRefSet requiredColumns = new ColumnRefSet();

        IvmRewriter.rewrite(root, taskContext, scheduler, requiredColumns);

        // No statement → no __op, no TopN
        OptExpression rewrittenChild = root.inputAt(0);
        Assertions.assertFalse(rewrittenChild.getOp() instanceof LogicalTopNOperator);
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

    private OptExpression newIcebergScan(ColumnRefFactory factory, IcebergTable table) {
        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        return newIcebergScan(factory, table, idRef, dataRef, null);
    }

    private OptExpression newIcebergScan(ColumnRefFactory factory, IcebergTable table,
                                         ColumnRefOperator idRef, ColumnRefOperator dataRef,
                                         com.starrocks.common.tvr.TvrVersionRange versionRange) {
        Column idCol = new Column("id", IntegerType.INT, false);
        Column dataCol = new Column("data", StringType.STRING, true);
        Map<ColumnRefOperator, Column> colRefMap = Maps.newHashMap();
        colRefMap.put(idRef, idCol);
        colRefMap.put(dataRef, dataCol);
        LogicalIcebergScanOperator.Builder builder = new LogicalIcebergScanOperator.Builder()
                .setTable(table)
                .setColRefToColumnMetaMap(colRefMap);
        if (versionRange != null) {
            builder.setTableVersionRange(versionRange);
        }
        return OptExpression.create(builder.build());
    }

    private TaskContext newTaskContext(OptimizerContext context) {
        TaskScheduler scheduler = new TaskScheduler();
        context.setTaskScheduler(scheduler);
        ColumnRefSet requiredColumns = new ColumnRefSet();
        return new TaskContext(context, new PhysicalPropertySet(), requiredColumns, Double.MAX_VALUE);
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
