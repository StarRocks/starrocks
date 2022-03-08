// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg;

import com.starrocks.analysis.*;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionConverterTest {

    @Mocked
    private Expr expr;

    @Mocked
    private SlotRef ref;

    @Mocked Column col;

    @Test
    public void testToIcebergExpression() throws AnalysisException {
        new Expectations() {
            {
                col.getName();
                result = "col_name";
            }
        };

        UnboundPredicate unboundPredicate;

        expr = new BinaryPredicate(BinaryPredicate.Operator.EQ, ref, LiteralExpr.create("test", Type.VARCHAR));
        unboundPredicate = ExpressionConverter.toIcebergExpression(expr);
        Assert.assertTrue(unboundPredicate.literal().getClass().toString().contains("org.apache.iceberg.expressions.Literals$StringLiteral"));

        expr = new BinaryPredicate(BinaryPredicate.Operator.EQ, ref, LiteralExpr.create("true", Type.BOOLEAN));
        unboundPredicate = ExpressionConverter.toIcebergExpression(expr);
        Assert.assertTrue(unboundPredicate.literal().getClass().toString().contains("org.apache.iceberg.expressions.Literals$BooleanLiteral"));

        expr = new BinaryPredicate(BinaryPredicate.Operator.EQ, ref, LiteralExpr.create("111", Type.INT));
        unboundPredicate = ExpressionConverter.toIcebergExpression(expr);
        Assert.assertTrue(unboundPredicate.literal().getClass().toString().contains("org.apache.iceberg.expressions.Literals$IntegerLiteral"));

        expr = new BinaryPredicate(BinaryPredicate.Operator.EQ, ref, LiteralExpr.create("111.22", Type.FLOAT));
        unboundPredicate = ExpressionConverter.toIcebergExpression(expr);
        Assert.assertTrue(unboundPredicate.literal().getClass().toString().contains("org.apache.iceberg.expressions.Literals$DoubleLiteral"));

        expr = new BinaryPredicate(BinaryPredicate.Operator.EQ, ref, LiteralExpr.create("111.22", Type.DECIMALV2));
        unboundPredicate = ExpressionConverter.toIcebergExpression(expr);
        Assert.assertTrue(unboundPredicate.literal().getClass().toString().contains("org.apache.iceberg.expressions.Literals$DecimalLiteral"));

        expr = new IsNullPredicate(new NullLiteral(), false);
        unboundPredicate = ExpressionConverter.toIcebergExpression(expr);
        Assert.assertNull(unboundPredicate);
    }
}
