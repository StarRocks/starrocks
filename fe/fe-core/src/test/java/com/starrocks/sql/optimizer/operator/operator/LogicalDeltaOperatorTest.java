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

package com.starrocks.sql.optimizer.operator.operator;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogicalDeltaOperatorTest {

    @Test
    void testDefaultConstructor() {
        LogicalDeltaOperator op = new LogicalDeltaOperator();
        assertEquals(OperatorType.LOGICAL_DELTA, op.getOpType());
        assertFalse(op.isRootDelta());
        assertNull(op.getActionColumn());
        assertNotNull(op.getMvColumnMapping());
        assertTrue(op.getMvColumnMapping().isEmpty());
    }

    @Test
    void testConstructorWithActionColumn() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator op = new LogicalDeltaOperator(true, actionCol);
        assertTrue(op.isRootDelta());
        assertEquals(actionCol, op.getActionColumn());
        assertTrue(op.getMvColumnMapping().isEmpty());
    }

    @Test
    void testConstructorWithMvColumnMapping() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        ColumnRefOperator col1 = new ColumnRefOperator(2, IntegerType.INT, "c1", false);
        Column mvCol = new Column("c1", IntegerType.INT);
        Map<ColumnRefOperator, Column> mapping = Maps.newHashMap();
        mapping.put(col1, mvCol);

        LogicalDeltaOperator op = new LogicalDeltaOperator(false, actionCol, mapping);
        assertFalse(op.isRootDelta());
        assertEquals(actionCol, op.getActionColumn());
        assertEquals(1, op.getMvColumnMapping().size());
    }

    @Test
    void testMvColumnMappingIsImmutable() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        Map<ColumnRefOperator, Column> mapping = Maps.newHashMap();
        LogicalDeltaOperator op = new LogicalDeltaOperator(true, actionCol, mapping);

        assertThrows(UnsupportedOperationException.class, () ->
                op.getMvColumnMapping().put(
                        new ColumnRefOperator(2, IntegerType.INT, "c2", false),
                        new Column("c2", IntegerType.INT)));
    }

    @Test
    void testNullableActionColumnRejected() {
        ColumnRefOperator nullableCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", true);
        assertThrows(IllegalArgumentException.class, () ->
                new LogicalDeltaOperator(true, nullableCol));
    }

    @Test
    void testBuilder() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator original = new LogicalDeltaOperator(true, actionCol);

        LogicalDeltaOperator.Builder builder = new LogicalDeltaOperator.Builder();
        LogicalDeltaOperator copy = builder.withOperator(original).build();

        assertEquals(original.isRootDelta(), copy.isRootDelta());
        assertEquals(original.getActionColumn(), copy.getActionColumn());
    }

    @Test
    void testBuilderSetters() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator op = new LogicalDeltaOperator.Builder()
                .withOperator(new LogicalDeltaOperator())
                .setRootDelta(true)
                .setActionColumn(actionCol)
                .build();

        assertTrue(op.isRootDelta());
        assertEquals(actionCol, op.getActionColumn());
    }

    @Test
    void testRootAndNonRootDeltaNotEqual() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator root = new LogicalDeltaOperator(true, actionCol);
        LogicalDeltaOperator nonRoot = new LogicalDeltaOperator(false, actionCol);
        assertNotEquals(root, nonRoot);
    }

    @Test
    void testSameFieldsEqual() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator op1 = new LogicalDeltaOperator(true, actionCol);
        LogicalDeltaOperator op2 = new LogicalDeltaOperator(true, actionCol);
        assertEquals(op1, op2);
        assertEquals(op1.hashCode(), op2.hashCode());
    }

    @Test
    void testEqualsSameInstance() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator op = new LogicalDeltaOperator(true, actionCol);
        assertEquals(op, op);
    }

    @Test
    void testDifferentActionColumnNotEqual() {
        ColumnRefOperator actionCol1 = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        ColumnRefOperator actionCol2 = new ColumnRefOperator(2, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator op1 = new LogicalDeltaOperator(true, actionCol1);
        LogicalDeltaOperator op2 = new LogicalDeltaOperator(true, actionCol2);
        assertNotEquals(op1, op2);
    }

    @Test
    void testGetOutputColumns() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "c1", false);
        ColumnRefOperator actionCol = new ColumnRefOperator(2, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator deltaOp = new LogicalDeltaOperator(true, actionCol);

        // Build a child with LogicalProperty containing col1
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        projectMap.put(col1, col1);
        OptExpression child = OptExpression.create(new LogicalProjectOperator(projectMap));
        child.setLogicalProperty(new LogicalProperty(new ColumnRefSet(List.of(col1))));

        OptExpression deltaExpr = OptExpression.create(deltaOp, child);
        ExpressionContext context = new ExpressionContext(deltaExpr);

        ColumnRefSet output = deltaOp.getOutputColumns(context);
        assertTrue(output.contains(col1));
        assertTrue(output.contains(actionCol));
    }

    @Test
    void testGetOutputColumnsWithoutActionColumn() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "c1", false);
        LogicalDeltaOperator deltaOp = new LogicalDeltaOperator();  // actionColumn is null

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        projectMap.put(col1, col1);
        OptExpression child = OptExpression.create(new LogicalProjectOperator(projectMap));
        child.setLogicalProperty(new LogicalProperty(new ColumnRefSet(List.of(col1))));

        OptExpression deltaExpr = OptExpression.create(deltaOp, child);
        ExpressionContext context = new ExpressionContext(deltaExpr);

        ColumnRefSet output = deltaOp.getOutputColumns(context);
        assertTrue(output.contains(col1));
        assertEquals(1, output.size());
    }

    @Test
    void testDeriveRowOutputInfo() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "c1", false);
        ColumnRefOperator actionCol = new ColumnRefOperator(2, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator deltaOp = new LogicalDeltaOperator(true, actionCol);

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        projectMap.put(col1, col1);
        OptExpression child = OptExpression.create(new LogicalProjectOperator(projectMap));
        child.setLogicalProperty(new LogicalProperty(new ColumnRefSet(List.of(col1))));

        OptExpression deltaExpr = OptExpression.create(deltaOp, child);
        var rowOutputInfo = deltaOp.deriveRowOutputInfo(deltaExpr.getInputs());

        // Should contain both col1 and actionColumn
        assertNotNull(rowOutputInfo);
        assertTrue(rowOutputInfo.getOutputColumnRefSet().contains(col1));
        assertTrue(rowOutputInfo.getOutputColumnRefSet().contains(actionCol));
    }

    @Test
    void testDeriveRowOutputInfoWithoutActionColumn() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "c1", false);
        LogicalDeltaOperator deltaOp = new LogicalDeltaOperator();  // actionColumn is null

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        projectMap.put(col1, col1);
        OptExpression child = OptExpression.create(new LogicalProjectOperator(projectMap));
        child.setLogicalProperty(new LogicalProperty(new ColumnRefSet(List.of(col1))));

        OptExpression deltaExpr = OptExpression.create(deltaOp, child);
        var rowOutputInfo = deltaOp.deriveRowOutputInfo(deltaExpr.getInputs());

        assertNotNull(rowOutputInfo);
        assertTrue(rowOutputInfo.getOutputColumnRefSet().contains(col1));
        assertEquals(1, rowOutputInfo.getOutputColumnRefSet().size());
    }

    @Test
    void testDeriveDomainProperty() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "c1", false);
        LogicalDeltaOperator deltaOp = new LogicalDeltaOperator();

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        projectMap.put(col1, col1);
        OptExpression child = OptExpression.create(new LogicalProjectOperator(projectMap));
        child.setLogicalProperty(new LogicalProperty(new ColumnRefSet(List.of(col1))));

        OptExpression deltaExpr = OptExpression.create(deltaOp, child);
        var domain = deltaOp.deriveDomainProperty(deltaExpr.getInputs());
        // Should pass through child's domain property
        assertSame(child.getDomainProperty(), domain);
    }

    @Test
    void testOperatorVisitorAccept() {
        ColumnRefOperator actionCol = new ColumnRefOperator(1, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator op = new LogicalDeltaOperator(true, actionCol);

        boolean[] visited = {false};
        op.accept(new OperatorVisitor<Void, Void>() {
            @Override
            public Void visitOperator(com.starrocks.sql.optimizer.operator.Operator node, Void context) {
                return null;
            }

            @Override
            public Void visitLogicalDelta(LogicalDeltaOperator node, Void context) {
                visited[0] = true;
                assertSame(op, node);
                return null;
            }
        }, null);

        assertTrue(visited[0]);
    }

    @Test
    void testOptExpressionVisitorAccept() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "c1", false);
        ColumnRefOperator actionCol = new ColumnRefOperator(2, IntegerType.TINYINT, "__ACTION__", false);
        LogicalDeltaOperator deltaOp = new LogicalDeltaOperator(true, actionCol);

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        projectMap.put(col1, col1);
        OptExpression child = OptExpression.create(new LogicalProjectOperator(projectMap));
        child.setLogicalProperty(new LogicalProperty(new ColumnRefSet(List.of(col1))));
        OptExpression deltaExpr = OptExpression.create(deltaOp, child);

        boolean[] visited = {false};
        deltaOp.accept(new OptExpressionVisitor<Void, Void>() {
            @Override
            public Void visit(OptExpression optExpression, Void context) {
                return null;
            }

            @Override
            public Void visitLogicalDelta(OptExpression optExpression, Void context) {
                visited[0] = true;
                assertSame(deltaExpr, optExpression);
                return null;
            }
        }, deltaExpr, null);

        assertTrue(visited[0]);
    }
}
