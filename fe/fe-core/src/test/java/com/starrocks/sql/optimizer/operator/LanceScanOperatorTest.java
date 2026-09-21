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

package com.starrocks.sql.optimizer.operator;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.LanceTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.LogicalPlanPrinter;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalLanceScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLanceScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rule.implementation.LanceScanImplementationRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneScanColumnRule;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LanceScanOperatorTest {

    private static LanceTable newLanceTable() {
        List<Column> schema = List.of(new Column("c1", IntegerType.INT));
        return new LanceTable(1L, "lance", schema, "file:///tmp/lance");
    }

    @Test
    public void testLogicalLanceScanOperator() {
        LanceTable table = newLanceTable();
        Map<ColumnRefOperator, Column> colRefToColumn = new HashMap<>();
        Map<Column, ColumnRefOperator> columnToColRef = new HashMap<>();
        Column column = new Column("c1", IntegerType.INT);
        ColumnRefOperator columnRef = new ColumnRefOperator(1, IntegerType.INT, "c1", true);
        colRefToColumn.put(columnRef, column);
        columnToColRef.put(column, columnRef);

        LogicalLanceScanOperator scanOperator =
                new LogicalLanceScanOperator(table, colRefToColumn, columnToColRef, -1, null);

        Assertions.assertEquals(table, scanOperator.getTable());
        Assertions.assertEquals(columnRef, scanOperator.getColumnReference(column));
        Assertions.assertNotNull(scanOperator.getScanOperatorPredicates());
        Assertions.assertFalse(scanOperator.isEmptyOutputRows());

        ScanOperatorPredicates predicates = new ScanOperatorPredicates();
        scanOperator.setScanOperatorPredicates(predicates);
        Assertions.assertSame(predicates, scanOperator.getScanOperatorPredicates());

        // Default hook: visitLogicalLanceScan falls back to visitLogicalTableScan.
        OperatorVisitor<String, Void> fallbackVisitor = new OperatorVisitor<>() {
            @Override
            public String visitOperator(Operator node, Void context) {
                return "operator";
            }

            @Override
            public String visitLogicalTableScan(LogicalScanOperator node, Void context) {
                return "logicalTableScan";
            }
        };
        Assertions.assertEquals("logicalTableScan", scanOperator.accept(fallbackVisitor, null));

        // Overridden hook: dispatches to visitLogicalLanceScan.
        OperatorVisitor<String, Void> lanceVisitor = new OperatorVisitor<>() {
            @Override
            public String visitOperator(Operator node, Void context) {
                return "operator";
            }

            @Override
            public String visitLogicalLanceScan(LogicalLanceScanOperator node, Void context) {
                return "logicalLance";
            }
        };
        Assertions.assertEquals("logicalLance", scanOperator.accept(lanceVisitor, null));
    }

    @Test
    public void testLogicalLanceScanOperatorRejectsNonLanceTable() {
        Table table = new Table(Table.TableType.OLAP);
        Assertions.assertThrows(IllegalStateException.class,
                () -> new LogicalLanceScanOperator(table, Map.of(), Map.of(), -1, null));
    }

    @Test
    public void testLogicalLanceScanOperatorBuilder() {
        LanceTable table = newLanceTable();
        LogicalLanceScanOperator original =
                new LogicalLanceScanOperator(table, Map.of(), Map.of(), -1, null);

        LogicalLanceScanOperator copy = new LogicalLanceScanOperator.Builder()
                .withOperator(original)
                .build();

        Assertions.assertEquals(table, copy.getTable());
        Assertions.assertNotNull(copy.getScanOperatorPredicates());
        Assertions.assertNotSame(original.getScanOperatorPredicates(), copy.getScanOperatorPredicates());
    }

    @Test
    public void testPhysicalLanceScanOperatorAccept() {
        LogicalLanceScanOperator logical =
                new LogicalLanceScanOperator(newLanceTable(), Map.of(), Map.of(), -1, null);
        PhysicalLanceScanOperator physical = new PhysicalLanceScanOperator(logical);

        Assertions.assertNotSame(logical.getScanOperatorPredicates(), physical.getScanOperatorPredicates());
        ScanOperatorPredicates predicates = new ScanOperatorPredicates();
        physical.setScanOperatorPredicates(predicates);
        Assertions.assertSame(predicates, physical.getScanOperatorPredicates());
        Assertions.assertNotNull(physical.getUsedColumns());

        // Default hook: visitPhysicalLanceScan falls back to visitOperator.
        OperatorVisitor<String, Void> fallbackVisitor = new OperatorVisitor<>() {
            @Override
            public String visitOperator(Operator node, Void context) {
                return "operator";
            }
        };
        Assertions.assertEquals("operator", physical.accept(fallbackVisitor, null));

        // Overridden hook: dispatches to visitPhysicalLanceScan.
        OperatorVisitor<String, Void> lanceVisitor = new OperatorVisitor<>() {
            @Override
            public String visitOperator(Operator node, Void context) {
                return "operator";
            }

            @Override
            public String visitPhysicalLanceScan(PhysicalLanceScanOperator node, Void context) {
                return "physicalLance";
            }
        };
        Assertions.assertEquals("physicalLance", physical.accept(lanceVisitor, null));

        // Default OptExpression hook: visitPhysicalLanceScan falls back to visitPhysicalScan.
        OptExpressionVisitor<String, Void> optFallbackVisitor = new OptExpressionVisitor<>() {
            @Override
            public String visitPhysicalScan(OptExpression optExpression, Void context) {
                return "physicalScan";
            }
        };
        Assertions.assertEquals("physicalScan",
                physical.accept(optFallbackVisitor, new OptExpression(physical), null));

        // Overridden OptExpression hook: dispatches to visitPhysicalLanceScan.
        OptExpressionVisitor<String, Void> optLanceVisitor = new OptExpressionVisitor<>() {
            @Override
            public String visitPhysicalLanceScan(OptExpression optExpression, Void context) {
                return "optLance";
            }
        };
        Assertions.assertEquals("optLance",
                physical.accept(optLanceVisitor, new OptExpression(physical), null));
    }

    @Test
    public void testLanceScanImplementationRule() {
        LogicalLanceScanOperator logical =
                new LogicalLanceScanOperator(newLanceTable(), Map.of(), Map.of(), -1, null);
        OptExpression input = new OptExpression(logical);

        LanceScanImplementationRule rule = new LanceScanImplementationRule();
        List<OptExpression> result = rule.transform(input, null);

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).getOp() instanceof PhysicalLanceScanOperator);
    }

    @Test
    public void testLogicalPlanPrinterLanceScan() {
        LogicalLanceScanOperator logical =
                new LogicalLanceScanOperator(newLanceTable(), Map.of(), Map.of(), -1, null);
        PhysicalLanceScanOperator physical = new PhysicalLanceScanOperator(logical);

        String plan = LogicalPlanPrinter.print(new OptExpression(physical));
        Assertions.assertTrue(plan.contains("LANCE SCAN"), plan);
    }
    @Test
    public void testPruneColumnsPreservesPredicateAndIndependentState() {
        ColumnRefOperator id = new ColumnRefOperator(1, IntegerType.INT, "id", false);
        ColumnRefOperator filter = new ColumnRefOperator(2, IntegerType.INT, "filter", true);
        ColumnRefOperator embedding = new ColumnRefOperator(3, new ArrayType(IntegerType.INT), "embedding", true);
        Column idColumn = new Column("id", id.getType());
        Column filterColumn = new Column("filter", filter.getType());
        Column embeddingColumn = new Column("embedding", embedding.getType());
        LanceTable table = new LanceTable(1, "lance", List.of(idColumn, filterColumn, embeddingColumn),
                "file:///tmp/lance");
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(BinaryType.GT, filter,
                ConstantOperator.createInt(10));
        LogicalLanceScanOperator scan = new LogicalLanceScanOperator(table,
                Map.of(id, idColumn, filter, filterColumn, embedding, embeddingColumn),
                Map.of(idColumn, id, filterColumn, filter, embeddingColumn, embedding), 7, predicate);
        scan.getScanOperatorPredicates().getNonPartitionConjuncts().add(predicate);
        OptimizerContext context = mock(OptimizerContext.class);
        TaskContext task = mock(TaskContext.class);
        when(context.getTaskContext()).thenReturn(task);
        when(task.getRequiredColumns()).thenReturn(new ColumnRefSet(id.getId()));
        when(context.getSessionVariable()).thenReturn(new SessionVariable());
        PruneScanColumnRule rule = new PruneScanColumnRule();
        OptExpression input = OptExpression.create(scan);
        Assertions.assertTrue(rule.getPattern().matchWithoutChild(input));
        LogicalLanceScanOperator pruned = (LogicalLanceScanOperator) rule.transform(input, context).get(0).getOp();
        Assertions.assertEquals(java.util.Set.of(id, filter), pruned.getColRefToColumnMetaMap().keySet());
        Assertions.assertEquals(predicate, pruned.getPredicate());
        Assertions.assertEquals(7, pruned.getLimit());
        Assertions.assertEquals(scan.getScanOperatorPredicates(), pruned.getScanOperatorPredicates());
        PhysicalLanceScanOperator physical = new PhysicalLanceScanOperator(pruned);
        physical.getScanOperatorPredicates().getNonPartitionConjuncts().clear();
        pruned.getScanOperatorPredicates().getNonPartitionConjuncts().clear();
        Assertions.assertEquals(List.of(predicate), scan.getScanOperatorPredicates().getNonPartitionConjuncts());
        Assertions.assertTrue(rule.transform(OptExpression.create(pruned), context).isEmpty());

        // COUNT(*) must retain a cheap scalar column, rather than reading the embedding.
        when(task.getRequiredColumns()).thenReturn(new ColumnRefSet());
        LogicalLanceScanOperator countScan = new LogicalLanceScanOperator.Builder().withOperator(scan)
                .setPredicate(null).build();
        LogicalLanceScanOperator countPruned = (LogicalLanceScanOperator)
                rule.transform(OptExpression.create(countScan), context).get(0).getOp();
        Assertions.assertEquals(1, countPruned.getColRefToColumnMetaMap().size());
        Assertions.assertFalse(countPruned.getColRefToColumnMetaMap().containsKey(embedding));
    }

}
