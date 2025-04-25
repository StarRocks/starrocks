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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IcebergDateTruncToRangeRuleTest {

    @Mocked
    private IcebergTable icebergTable;
    private IcebergDateTruncToRangeRule rule;
    private ColumnRefOperator timestampColumn;
    private ColumnRefOperator dateColumn;
    private ColumnRefFactory columnRefFactory;
    private OptimizerContext context;

    @Before
    public void setUp() {
        rule = new IcebergDateTruncToRangeRule();

        // Setup column references
        columnRefFactory = new ColumnRefFactory();
        timestampColumn = columnRefFactory.create("ts_col", Type.DATETIME, true);
        dateColumn = columnRefFactory.create("date_col", Type.DATE, true);
        context = OptimizerFactory.mockContext(columnRefFactory);

        // Mock isIcebergTable
        new MockUp<IcebergTable>() {
            @Mock
            public boolean isIcebergTable() {
                return true;
            }
        };
    }

    @Test
    public void testDateTruncDayTransformation() {
        // Create date_trunc('day', ts_col) = '2023-02-26' predicate
        ConstantOperator dayUnit = ConstantOperator.createVarchar("day");
        ConstantOperator dateValue = ConstantOperator.createDatetime(
                LocalDateTime.of(2023, 2, 26, 0, 0, 0));

        CallOperator dateTruncCall = new CallOperator(
                FunctionSet.DATE_TRUNC,
                Type.DATETIME,
                Lists.newArrayList(dayUnit, timestampColumn));

        BinaryPredicateOperator dateTruncPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, dateTruncCall, dateValue);

        // Create filter and scan operators
        LogicalFilterOperator filterOperator = new LogicalFilterOperator(dateTruncPredicate);
        LogicalScanOperator scanOperator = new LogicalScanOperator(
                icebergTable,
                Maps.newHashMap(),
                Maps.newHashMap(),
                null, -1, null);

        // Create expression tree
        OptExpression filterExpression = new OptExpression(filterOperator);
        OptExpression scanExpression = new OptExpression(scanOperator);
        filterExpression.getInputs().add(scanExpression);

        // Apply the rule
        List<OptExpression> result = rule.transform(filterExpression, context);

        // Verify the result
        assertEquals(1, result.size());
        LogicalFilterOperator newFilterOp = (LogicalFilterOperator) result.get(0).getOp();
        ScalarOperator newPredicate = newFilterOp.getPredicate();

        // Should be transformed to: ts_col >= '2023-02-26 00:00:00' AND ts_col < '2023-02-27 00:00:00'
        assertTrue(newPredicate instanceof CompoundPredicateOperator);
        CompoundPredicateOperator compoundOp = (CompoundPredicateOperator) newPredicate;
        assertEquals(com.starrocks.analysis.CompoundPredicate.Operator.AND, compoundOp.getCompoundType());

        // Check first child: ts_col >= '2023-02-26 00:00:00'
        ScalarOperator firstChild = compoundOp.getChild(0);
        assertTrue(firstChild instanceof BinaryPredicateOperator);
        BinaryPredicateOperator firstBinaryOp = (BinaryPredicateOperator) firstChild;
        assertEquals(BinaryType.GE, firstBinaryOp.getBinaryType());
        assertEquals(OperatorType.VARIABLE, firstBinaryOp.getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, firstBinaryOp.getChild(1).getOpType());

        // Check second child: ts_col < '2023-02-27 00:00:00'
        ScalarOperator secondChild = compoundOp.getChild(1);
        assertTrue(secondChild instanceof BinaryPredicateOperator);
        BinaryPredicateOperator secondBinaryOp = (BinaryPredicateOperator) secondChild;
        assertEquals(BinaryType.LT, secondBinaryOp.getBinaryType());
        assertEquals(OperatorType.VARIABLE, secondBinaryOp.getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, secondBinaryOp.getChild(1).getOpType());

        // Check values in the range bounds
        LocalDateTime lowerBound = ((ConstantOperator) firstBinaryOp.getChild(1)).getDatetime();
        LocalDateTime upperBound = ((ConstantOperator) secondBinaryOp.getChild(1)).getDatetime();

        assertEquals(LocalDateTime.of(2023, 2, 26, 0, 0, 0), lowerBound);
        assertEquals(LocalDateTime.of(2023, 2, 27, 0, 0, 0), upperBound);
    }

    @Test
    public void testDateTruncMonthTransformation() {
        // Create date_trunc('month', date_col) = '2023-02-01' predicate for DATE column
        ConstantOperator monthUnit = ConstantOperator.createVarchar("month");
        ConstantOperator dateValue = ConstantOperator.createDate(LocalDate.of(2023, 2, 1));

        CallOperator dateTruncCall = new CallOperator(
                FunctionSet.DATE_TRUNC,
                Type.DATE,
                Lists.newArrayList(monthUnit, dateColumn));

        BinaryPredicateOperator dateTruncPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, dateTruncCall, dateValue);

        // Create filter and scan operators
        LogicalFilterOperator filterOperator = new LogicalFilterOperator(dateTruncPredicate);
        LogicalScanOperator scanOperator = new LogicalScanOperator(
                icebergTable,
                Maps.newHashMap(),
                Maps.newHashMap(),
                null, -1, null);

        // Create expression tree
        OptExpression filterExpression = new OptExpression(filterOperator);
        OptExpression scanExpression = new OptExpression(scanOperator);
        filterExpression.getInputs().add(scanExpression);

        // Apply the rule
        List<OptExpression> result = rule.transform(filterExpression, context);

        // Verify the result
        assertEquals(1, result.size());
        LogicalFilterOperator newFilterOp = (LogicalFilterOperator) result.get(0).getOp();
        ScalarOperator newPredicate = newFilterOp.getPredicate();

        // Should be transformed to: date_col >= '2023-02-01' AND date_col < '2023-03-01'
        assertTrue(newPredicate instanceof CompoundPredicateOperator);
        CompoundPredicateOperator compoundOp = (CompoundPredicateOperator) newPredicate;
        assertEquals(com.starrocks.analysis.CompoundPredicate.Operator.AND, compoundOp.getCompoundType());

        // Check first child: date_col >= '2023-02-01'
        ScalarOperator firstChild = compoundOp.getChild(0);
        assertTrue(firstChild instanceof BinaryPredicateOperator);
        BinaryPredicateOperator firstBinaryOp = (BinaryPredicateOperator) firstChild;
        assertEquals(BinaryType.GE, firstBinaryOp.getBinaryType());
        assertEquals(OperatorType.VARIABLE, firstBinaryOp.getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, firstBinaryOp.getChild(1).getOpType());

        // Check second child: date_col < '2023-03-01'
        ScalarOperator secondChild = compoundOp.getChild(1);
        assertTrue(secondChild instanceof BinaryPredicateOperator);
        BinaryPredicateOperator secondBinaryOp = (BinaryPredicateOperator) secondChild;
        assertEquals(BinaryType.LT, secondBinaryOp.getBinaryType());
        assertEquals(OperatorType.VARIABLE, secondBinaryOp.getChild(0).getOpType());
        assertEquals(OperatorType.CONSTANT, secondBinaryOp.getChild(1).getOpType());

        // Check values in the range bounds for DATE type
        LocalDate lowerBound = ((ConstantOperator) firstBinaryOp.getChild(1)).getDate();
        LocalDate upperBound = ((ConstantOperator) secondBinaryOp.getChild(1)).getDate();

        assertEquals(LocalDate.of(2023, 2, 1), lowerBound);
        assertEquals(LocalDate.of(2023, 3, 1), upperBound);
    }
}
