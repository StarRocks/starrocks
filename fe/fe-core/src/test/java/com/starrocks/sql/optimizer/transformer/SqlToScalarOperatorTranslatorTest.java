// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.transformer;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class SqlToScalarOperatorTranslatorTest {

    @Test
    public void testTranslateConstant() {
        DateLiteral literal = new DateLiteral(2000, 12, 1);
        ScalarOperator so =
                SqlToScalarOperatorTranslator.translate(literal, new ExpressionMapping(null, Collections.emptyList()),
                        new ColumnRefFactory());

        assertEquals(OperatorType.CONSTANT, so.getOpType());
        assertEquals(LocalDateTime.of(2000, 12, 1, 0, 0), ((ConstantOperator) so).getDate());
    }

    @Test
    public void testTranslateComplexFunction() {
        StringLiteral test = new StringLiteral("test");
        StringLiteral defaultStr = new StringLiteral("default");
        BinaryPredicate predicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, defaultStr, test);
        FunctionCallExpr baseFunc = new FunctionCallExpr("if", ImmutableList.of(predicate, test, defaultStr));
        FunctionCallExpr complexFunc = baseFunc;
        for (int i = 0; i < 100; i++) {
            complexFunc = new FunctionCallExpr("if",  ImmutableList.of(predicate, test, complexFunc));
        }
        CallOperator so = (CallOperator) SqlToScalarOperatorTranslator.translate(complexFunc,
                new ExpressionMapping(null, Collections.emptyList()), new ColumnRefFactory());
        assertEquals("if", so.getFnName());
    }
}
