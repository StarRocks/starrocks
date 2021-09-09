// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReduceCastRuleTest {

    @Test
    public void testDuplicateCastReduceFail() {
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator operator =
                new CastOperator(Type.BIGINT, new CastOperator(Type.INT, ConstantOperator.createChar("hello")));

        ScalarOperator result = rule.apply(operator, null);

        assertEquals(OperatorType.CALL, result.getOpType());
        assertTrue(result instanceof CastOperator);

        assertTrue(result.getType().isBigint());
        assertEquals(OperatorType.CALL, result.getChild(0).getOpType());
        assertTrue(result.getChild(0).getChild(0).getType().isChar());
    }

    @Test
    public void testDuplicateCastReduceSuccess() {
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator operator =
                new CastOperator(Type.BIGINT, new CastOperator(Type.INT, ConstantOperator.createSmallInt((short) 3)));

        ScalarOperator result = rule.apply(operator, null);

        assertEquals(OperatorType.CALL, result.getOpType());
        assertTrue(result instanceof CastOperator);

        assertTrue(result.getType().isBigint());
        assertEquals(OperatorType.CONSTANT, result.getChild(0).getOpType());
        assertTrue(result.getChild(0).getType().isNumericType());
    }

    @Test
    public void testSameTypeCast() {
        ScalarOperatorRewriteRule rule = new ReduceCastRule();

        ScalarOperator operator =
                new CastOperator(Type.CHAR, ConstantOperator.createChar("hello"));

        ScalarOperator result = rule.apply(operator, null);

        assertTrue(result.getType().isChar());
        assertEquals(OperatorType.CONSTANT, result.getOpType());
    }
}