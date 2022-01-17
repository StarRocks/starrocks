// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteRule;
import javassist.expr.Cast;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

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

    private ConstantOperator createConstOperatorFromType(Type type) {
        if (type.isTinyint()) {
            return ConstantOperator.createTinyInt(Byte.MAX_VALUE);
        }
        if (type.isSmallint()) {
            return ConstantOperator.createSmallInt(Short.MAX_VALUE);
        }
        if (type.isInt()) {
            return ConstantOperator.createInt(Integer.MAX_VALUE);
        }
        if (type.isBigint()) {
            return ConstantOperator.createBigint(Long.MAX_VALUE);
        }
        if (type.isLargeint()) {
            return ConstantOperator.createLargeInt(BigInteger.ONE);
        }
        if (type.isDecimalV3()) {
            return ConstantOperator.createDecimal(BigDecimal.ONE, type);
        }
        return ConstantOperator.createNull(type);
    }

    @Test
    public void testBinaryPredicateInvolvingDecimalSuccess() {
        Type[][] typeListList = new Type[][]{
                {Type.TINYINT, Type.SMALLINT, ScalarType.createDecimalV3NarrowestType(16,9)},
                {Type.TINYINT, Type.SMALLINT, ScalarType.createDecimalV3NarrowestType(9,0)},
                {ScalarType.createDecimalV3NarrowestType(4,0), Type.SMALLINT, Type.BIGINT},
                {ScalarType.createDecimalV3NarrowestType(18, 0), Type.BIGINT, Type.LARGEINT},
                {ScalarType.createDecimalV3NarrowestType(2, 0), Type.TINYINT, Type.INT},
        };

        ScalarOperatorRewriteRule reduceCastRule = new ReduceCastRule();
        ScalarOperatorRewriteRule foldConstantsRule = new FoldConstantsRule();
        for (Type[] types : typeListList) {
            ScalarOperator binPredRhs = createConstOperatorFromType(types[0]);
            ScalarOperator castChild = createConstOperatorFromType(types[1]);
            CastOperator binPredLhs = new CastOperator(types[2], castChild);
            BinaryPredicateOperator binPred = new BinaryPredicateOperator(
                    BinaryPredicateOperator.BinaryType.GE, binPredLhs, binPredRhs);
            ScalarOperator result = reduceCastRule.apply(binPred, null);
            result = foldConstantsRule.apply(result, null);
            Assert.assertTrue(result instanceof ConstantOperator);
        }
    }

    @Test
    public void testBinaryPredicateInvolvingDecimalFail() {
        Type[][] typeListList = new Type[][]{
                {Type.TINYINT, Type.SMALLINT, ScalarType.createDecimalV3NarrowestType(13,9)},
                {Type.INT, Type.SMALLINT, ScalarType.createDecimalV3NarrowestType(9,0)},
                {ScalarType.createDecimalV3NarrowestType(6,0), Type.SMALLINT, Type.BIGINT},
                {ScalarType.createDecimalV3NarrowestType(19, 0), Type.BIGINT, Type.LARGEINT},
                {ScalarType.createDecimalV3NarrowestType(10, 0), Type.TINYINT, Type.INT},
        };

        ScalarOperatorRewriteRule reduceCastRule = new ReduceCastRule();
        ScalarOperatorRewriteRule foldConstantsRule = new FoldConstantsRule();
        for (Type[] types : typeListList) {
            ScalarOperator binPredRhs = createConstOperatorFromType(types[0]);
            ScalarOperator castChild = createConstOperatorFromType(types[1]);
            CastOperator binPredLhs = new CastOperator(types[2], castChild);
            BinaryPredicateOperator binPred = new BinaryPredicateOperator(
                    BinaryPredicateOperator.BinaryType.GE, binPredLhs, binPredRhs);
            ScalarOperator result = reduceCastRule.apply(binPred, null);
            result = foldConstantsRule.apply(result, null);
            Assert.assertTrue(!(result instanceof ConstantOperator));
        }
    }
}