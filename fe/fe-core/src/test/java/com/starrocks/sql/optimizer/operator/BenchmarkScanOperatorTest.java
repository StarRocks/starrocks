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

import com.starrocks.catalog.BenchmarkTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.connector.benchmark.BenchmarkCatalogConfig;
import com.starrocks.connector.benchmark.BenchmarkConfig;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalBenchmarkScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalBenchmarkScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BenchmarkScanOperatorTest {

    @Test
    public void testLogicalBenchmarkScanOperator() {
        BenchmarkTable table = newBenchmarkTable(1.0);
        Map<ColumnRefOperator, Column> colRefToColumn = new HashMap<>();
        Map<Column, ColumnRefOperator> columnToColRef = new HashMap<>();
        Column column = new Column("c1", IntegerType.INT);
        ColumnRefOperator columnRef = new ColumnRefOperator(1, IntegerType.INT, "c1", true);
        colRefToColumn.put(columnRef, column);
        columnToColRef.put(column, columnRef);

        LogicalBenchmarkScanOperator scanOperator =
                new LogicalBenchmarkScanOperator(table, colRefToColumn, columnToColRef, -1, null, null);

        Assertions.assertEquals(table, scanOperator.getTable());
        Assertions.assertEquals(columnRef, scanOperator.getColumnReference(column));

        OperatorVisitor<String, Void> visitor = new OperatorVisitor<>() {
            @Override
            public String visitOperator(Operator node, Void context) {
                return "operator";
            }

            @Override
            public String visitLogicalBenchmarkScan(LogicalBenchmarkScanOperator node, Void context) {
                return "logical";
            }
        };
        Assertions.assertEquals("logical", scanOperator.accept(visitor, null));
    }

    @Test
    public void testLogicalBenchmarkScanOperatorRejectsNonBenchmarkTable() {
        Table table = new Table(Table.TableType.OLAP);
        Assertions.assertThrows(IllegalStateException.class,
                () -> new LogicalBenchmarkScanOperator(table, Map.of(), Map.of(), -1, null, null));
    }

    @Test
    public void testPhysicalBenchmarkScanOperatorAccept() {
        LogicalBenchmarkScanOperator logical =
                new LogicalBenchmarkScanOperator(newBenchmarkTable(1.0), Map.of(), Map.of(), -1, null, null);
        PhysicalBenchmarkScanOperator physical = new PhysicalBenchmarkScanOperator(logical);

        OperatorVisitor<String, Void> visitor = new OperatorVisitor<>() {
            @Override
            public String visitOperator(Operator node, Void context) {
                return "operator";
            }

            @Override
            public String visitPhysicalBenchmarkScan(PhysicalBenchmarkScanOperator node, Void context) {
                return "physical";
            }
        };
        Assertions.assertEquals("physical", physical.accept(visitor, null));

        OptExpressionVisitor<String, Void> optVisitor = new OptExpressionVisitor<>() {
            @Override
            public String visitPhysicalBenchmarkScan(OptExpression optExpression, Void context) {
                return "opt";
            }
        };
        Assertions.assertEquals("opt", physical.accept(optVisitor, new OptExpression(physical), null));
    }

    private static BenchmarkTable newBenchmarkTable(double scale) {
        List<Column> schema = List.of(new Column("c1", IntegerType.INT));
        BenchmarkConfig config = new BenchmarkConfig();
        Map<String, String> properties = new HashMap<>();
        properties.put(BenchmarkConfig.SCALE, Double.toString(scale));
        config.loadConfig(properties);
        BenchmarkCatalogConfig catalogConfig = BenchmarkCatalogConfig.from(config);
        return new BenchmarkTable(1L, "benchmark", "tpcds", "store_sales", schema, catalogConfig);
    }
}
