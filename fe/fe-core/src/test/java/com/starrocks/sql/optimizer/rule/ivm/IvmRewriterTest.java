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
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
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

        TaskContext taskContext = newTaskContext(context);
        TaskScheduler scheduler = new TaskScheduler();
        ColumnRefSet requiredColumns = new ColumnRefSet();

        IvmRewriter.rewrite(root, taskContext, scheduler, requiredColumns);

        // Plan should be unchanged — no Delta operator injected
        Assertions.assertFalse(IvmRuleUtils.containsLogicalDelta(root));
        Assertions.assertSame(scan, root.inputAt(0));
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

        TaskContext taskContext = newTaskContext(context);
        TaskScheduler scheduler = new TaskScheduler();
        ColumnRefSet requiredColumns = new ColumnRefSet();

        IvmRewriter.rewrite(root, taskContext, scheduler, requiredColumns);

        // Delta/Version markers should be fully eliminated
        Assertions.assertFalse(IvmRuleUtils.containsLogicalDelta(root));
        Assertions.assertFalse(IvmRuleUtils.containsLogicalVersion(root));
        // Root child should no longer be the original scan (plan was rewritten)
        Assertions.assertNotSame(scan, root.inputAt(0));
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

        TaskContext taskContext = newTaskContext(context);
        TaskScheduler scheduler = new TaskScheduler();
        ColumnRefSet requiredColumns = new ColumnRefSet();

        IvmRewriter.rewrite(root, taskContext, scheduler, requiredColumns);

        // Convergence failed → original plan restored
        Assertions.assertFalse(IvmRuleUtils.containsLogicalDelta(root));
        Assertions.assertSame(originalChild, root.inputAt(0));
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

        // For PK MV, plan should have TopN → Project(__op) at the top
        OptExpression rewrittenChild = root.inputAt(0);
        Assertions.assertTrue(rewrittenChild.getOp() instanceof LogicalTopNOperator,
                "PK MV should have TopN for DELETE-before-INSERT ordering");
        OptExpression opProject = rewrittenChild.inputAt(0);
        Assertions.assertTrue(opProject.getOp() instanceof LogicalProjectOperator);
        LogicalProjectOperator opProjectOp = (LogicalProjectOperator) opProject.getOp();
        // Should contain __op column
        boolean hasOpColumn = opProjectOp.getColumnRefMap().keySet().stream()
                .anyMatch(col -> Load.LOAD_OP_COLUMN.equalsIgnoreCase(col.getName()));
        Assertions.assertTrue(hasOpColumn, "PK MV should have __op column");
        // Should NOT contain __ACTION__ column (replaced by __op)
        boolean hasActionColumn = opProjectOp.getColumnRefMap().keySet().stream()
                .anyMatch(col -> IvmRuleUtils.ACTION_COLUMN_NAME.equalsIgnoreCase(col.getName()));
        Assertions.assertFalse(hasActionColumn, "__ACTION__ should be replaced by __op");
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
