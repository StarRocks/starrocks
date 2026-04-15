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

import com.starrocks.catalog.ADBCTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalADBCScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalADBCScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ADBCScanOperatorTest {

    @Test
    public void testLogicalADBCScanOperatorType() {
        ADBCTable table = newADBCTable();
        Map<ColumnRefOperator, Column> colRefToColumn = new HashMap<>();
        Map<Column, ColumnRefOperator> columnToColRef = new HashMap<>();
        Column column = new Column("c1", IntegerType.INT);
        ColumnRefOperator columnRef = new ColumnRefOperator(1, IntegerType.INT, "c1", true);
        colRefToColumn.put(columnRef, column);
        columnToColRef.put(column, columnRef);

        LogicalADBCScanOperator scanOperator =
                new LogicalADBCScanOperator(table, colRefToColumn, columnToColRef, -1, null, null);

        Assertions.assertEquals(OperatorType.LOGICAL_ADBC_SCAN, scanOperator.getOpType());
        Assertions.assertEquals(table, scanOperator.getTable());
        Assertions.assertEquals(columnRef, scanOperator.getColumnReference(column));
    }

    @Test
    public void testLogicalADBCScanOperatorAccept() {
        LogicalADBCScanOperator scanOperator =
                new LogicalADBCScanOperator(newADBCTable(), Map.of(), Map.of(), -1, null, null);

        OperatorVisitor<String, Void> visitor = new OperatorVisitor<>() {
            @Override
            public String visitOperator(Operator node, Void context) {
                return "operator";
            }

            @Override
            public String visitLogicalADBCScan(LogicalADBCScanOperator node, Void context) {
                return "logical";
            }
        };
        Assertions.assertEquals("logical", scanOperator.accept(visitor, null));
    }

    @Test
    public void testLogicalADBCScanOperatorRejectsNonADBCTable() {
        Table table = new Table(Table.TableType.OLAP);
        Assertions.assertThrows(IllegalStateException.class,
                () -> new LogicalADBCScanOperator(table, Map.of(), Map.of(), -1, null, null));
    }

    @Test
    public void testPhysicalADBCScanOperatorType() {
        LogicalADBCScanOperator logical =
                new LogicalADBCScanOperator(newADBCTable(), Map.of(), Map.of(), -1, null, null);
        PhysicalADBCScanOperator physical = new PhysicalADBCScanOperator(logical);

        Assertions.assertEquals(OperatorType.PHYSICAL_ADBC_SCAN, physical.getOpType());
    }

    @Test
    public void testPhysicalADBCScanOperatorAccept() {
        LogicalADBCScanOperator logical =
                new LogicalADBCScanOperator(newADBCTable(), Map.of(), Map.of(), -1, null, null);
        PhysicalADBCScanOperator physical = new PhysicalADBCScanOperator(logical);

        OperatorVisitor<String, Void> visitor = new OperatorVisitor<>() {
            @Override
            public String visitOperator(Operator node, Void context) {
                return "operator";
            }

            @Override
            public String visitPhysicalADBCScan(PhysicalADBCScanOperator node, Void context) {
                return "physical";
            }
        };
        Assertions.assertEquals("physical", physical.accept(visitor, null));

        OptExpressionVisitor<String, Void> optVisitor = new OptExpressionVisitor<>() {
            @Override
            public String visitPhysicalADBCScan(OptExpression optExpression, Void context) {
                return "opt";
            }
        };
        Assertions.assertEquals("opt", physical.accept(optVisitor, new OptExpression(physical), null));
    }

    private static ADBCTable newADBCTable() {
        List<Column> schema = List.of(new Column("c1", IntegerType.INT));
        Map<String, String> properties = new HashMap<>();
        properties.put("adbc.driver", "flight_sql");
        properties.put("adbc.uri", "grpc://localhost:12345");
        return new ADBCTable(1L, "test_table", schema, "test_db", "test_catalog", properties);
    }
}
