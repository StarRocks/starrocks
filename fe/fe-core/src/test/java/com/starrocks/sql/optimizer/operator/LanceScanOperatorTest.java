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
import com.starrocks.sql.optimizer.LogicalPlanPrinter;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalLanceScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLanceScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.implementation.LanceScanImplementationRule;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        Assertions.assertSame(logical.getScanOperatorPredicates(), physical.getScanOperatorPredicates());
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
}
