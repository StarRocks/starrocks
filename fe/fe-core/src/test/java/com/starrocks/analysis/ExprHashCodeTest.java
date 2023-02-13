// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectRelation;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ExprHashCodeTest {

    private Set<Expr> exprSet = Sets.newHashSet();

    @ParameterizedTest
    @MethodSource("generateExprStream")
    void testExprHashCode(Expr expr) {
        assertTrue(exprSet.add(expr));
    }

    private static Stream<Arguments> generateExprStream() throws Exception {
        FloatLiteral floatLiteral = new FloatLiteral(1.0d);
        IntLiteral intLiteral = new IntLiteral(1);
        LargeIntLiteral largeIntLiteral = new LargeIntLiteral("123");
        StringLiteral stringLiteral = new StringLiteral("test");
        DateLiteral dateLiteral = new DateLiteral(2000L, 10L, 10L);
        DecimalLiteral decimalLiteral = new DecimalLiteral(new BigDecimal(100));
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("abs", ImmutableList.of(intLiteral));
        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, stringLiteral, stringLiteral);
        SelectRelation selectRelation = new SelectRelation(new SelectList(),
                null, null, null, null);
        ExistsPredicate existsPredicate = new ExistsPredicate(new Subquery(new QueryStatement(selectRelation)), false);
        BinaryPredicate predicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, stringLiteral, stringLiteral);
        CompoundPredicate compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.OR,
                predicate, predicate);
        ArithmeticExpr arithmeticExpr = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, intLiteral, largeIntLiteral);
        AnalyticExpr analyticExpr = new AnalyticExpr(functionCallExpr, ImmutableList.of(stringLiteral),
                null, AnalyticWindow.DEFAULT_WINDOW, null);
        List<Expr> exprList = Lists.newArrayList(floatLiteral, intLiteral, largeIntLiteral, stringLiteral, dateLiteral,
                decimalLiteral, functionCallExpr, likePredicate, existsPredicate, predicate, compoundPredicate,
                arithmeticExpr, analyticExpr);
        return exprList.stream().map(e -> Arguments.of(e));
    }
}
