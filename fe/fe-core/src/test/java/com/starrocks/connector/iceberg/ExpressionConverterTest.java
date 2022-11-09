// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.iceberg.ExpressionConverter;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.Assert;
import org.junit.Test;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ExpressionConverterTest {

    @Mocked
    Column col;

    @Mocked
    SlotDescriptor desc;

    @Test
    public void testToIcebergExpression() throws AnalysisException {
        new Expectations() {
            {
                col.getName();
                result = "col_name";
                desc.getColumn();
            }
        };

        Expression convertedExpression = null;
        Expression expectedExpression = null;
        ExpressionConverter converter = new ExpressionConverter();
        SlotRef ref = new SlotRef(desc);

        // isNull
        convertedExpression = converter.convert(new IsNullPredicate(ref, false));
        expectedExpression = Expressions.isNull("col_name");
        Assert.assertEquals("Generated isNull expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());

        // notNull
        convertedExpression = converter.convert(new IsNullPredicate(ref, true));
        expectedExpression = Expressions.notNull("col_name");
        Assert.assertEquals("Generated notNull expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());

        // equal
        DateLiteral dateLiteral = (DateLiteral) LiteralExpr.create("2018-10-18", Type.DATE);
        long epochDay = dateLiteral.toLocalDateTime().toLocalDate().toEpochDay();

        convertedExpression = converter.convert(new BinaryPredicate(BinaryPredicate.Operator.EQ, ref, dateLiteral));
        expectedExpression = Expressions.equal("col_name", epochDay);
        Assert.assertEquals("Generated equal expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());

        // equal
        DateLiteral dateTimeLiteral = (DateLiteral) LiteralExpr.create("2018-10-18 00:00:00", Type.DATETIME);
        long epochSeconds = dateTimeLiteral.toLocalDateTime().toEpochSecond(OffsetDateTime.now().getOffset());

        convertedExpression = converter.convert(new BinaryPredicate(BinaryPredicate.Operator.EQ, ref, dateTimeLiteral));
        expectedExpression =
                Expressions.equal("col_name", TimeUnit.MICROSECONDS.convert(epochSeconds, TimeUnit.SECONDS));
        Assert.assertEquals("Generated equal expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());

        // notEqual
        BoolLiteral boolLiteral = (BoolLiteral) BoolLiteral.create("true", Type.BOOLEAN);

        convertedExpression = converter.convert(new BinaryPredicate(BinaryPredicate.Operator.NE, ref, boolLiteral));
        expectedExpression = Expressions.notEqual("col_name", true);
        Assert.assertEquals("Generated notEqual expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());

        // lessThan
        IntLiteral intLiteral = (IntLiteral) IntLiteral.create("1", Type.INT);
        int normalInt = (int) intLiteral.getValue();

        convertedExpression = converter.convert(new BinaryPredicate(BinaryPredicate.Operator.LT, ref, intLiteral));
        expectedExpression = Expressions.lessThan("col_name", normalInt);
        Assert.assertEquals("Generated lessThan expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());

        // lessThanOrEqual
        intLiteral = (IntLiteral) IntLiteral.create("2", Type.SMALLINT);
        int smallInt = (int) intLiteral.getValue();

        convertedExpression = converter.convert(new BinaryPredicate(BinaryPredicate.Operator.LE, ref, intLiteral));
        expectedExpression = Expressions.lessThanOrEqual("col_name", smallInt);
        Assert.assertEquals("Generated lessThanOrEqual expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());

        // greaterThan
        intLiteral = (IntLiteral) IntLiteral.create("3", Type.TINYINT);
        int tinyInt = (int) intLiteral.getValue();

        convertedExpression = converter.convert(new BinaryPredicate(BinaryPredicate.Operator.GT, ref, intLiteral));
        expectedExpression = Expressions.greaterThan("col_name", tinyInt);
        Assert.assertEquals("Generated greaterThan expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());

        // greaterThanOrEqual
        intLiteral = (IntLiteral) IntLiteral.create("1", Type.BIGINT);
        long bigInt = intLiteral.getValue();

        convertedExpression = converter.convert(new BinaryPredicate(BinaryPredicate.Operator.GE, ref, intLiteral));
        expectedExpression = Expressions.greaterThanOrEqual("col_name", bigInt);
        Assert.assertEquals("Generated greaterThanOrEqual expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());
        // in
        List<Expr> inListExpr = Lists.newArrayList();
        inListExpr.add(new StringLiteral("1234"));
        inListExpr.add(new StringLiteral("5678"));
        inListExpr.add(new StringLiteral("1314"));
        inListExpr.add(new StringLiteral("8972"));
        List<String> inList =
                inListExpr.stream().map(s -> ((StringLiteral) s).getUnescapedValue()).collect(Collectors.toList());

        convertedExpression = converter.convert(new InPredicate(ref, inListExpr, false));
        expectedExpression = Expressions.in("col_name", inList);
        Assert.assertEquals("Generated in expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());

        // notIn
        convertedExpression = converter.convert(new InPredicate(ref, inListExpr, true));
        expectedExpression = Expressions.notIn("col_name", inList);
        Assert.assertEquals("Generated notIn expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());

        // startsWith
        List<Expr> params = Lists.newArrayList();
        params.add(0, ref);
        params.add(new StringLiteral("a"));
        StringLiteral stringLiteral = (StringLiteral) StringLiteral.create("a%", Type.STRING);
        expectedExpression = Expressions.startsWith("col_name", "a");

        convertedExpression = converter.convert(new FunctionCallExpr("starts_with", params));
        Assert.assertEquals("Generated startsWith expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());

        convertedExpression = converter.convert(new LikePredicate(LikePredicate.Operator.LIKE, ref, stringLiteral));
        Assert.assertEquals("Generated startsWith expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());

        // or
        Expr expr1 = new BinaryPredicate(BinaryPredicate.Operator.GT, ref, IntLiteral.create("10", Type.BIGINT));
        Expr expr2 = new BinaryPredicate(BinaryPredicate.Operator.LT, ref, IntLiteral.create("5", Type.BIGINT));
        Expression expression1 = converter.convert(expr1);
        Expression expression2 = converter.convert(expr2);

        convertedExpression = converter.convert(new CompoundPredicate(CompoundPredicate.Operator.OR, expr1, expr2));
        expectedExpression = Expressions.or(expression1, expression2);
        Assert.assertEquals("Generated or expression should be correct",
                expectedExpression.toString(), convertedExpression.toString());
    }
}
