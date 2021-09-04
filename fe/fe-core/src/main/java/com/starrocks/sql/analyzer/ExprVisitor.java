// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.ArrayElementExpr;
import com.starrocks.analysis.ArrayExpr;
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DefaultValueExpr;
import com.starrocks.analysis.ExistsPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FieldReference;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.InformationFunction;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.SysVariableDesc;
import com.starrocks.analysis.TimestampArithmeticExpr;

public abstract class ExprVisitor<R, C> {
    public R visit(Expr node) {
        return visit(node, null);
    }

    public R visit(Expr node, C context) {
        return node.accept(this, context);
    }

    public R visitExpression(Expr node, C context) {
        return null;
    }

    public R visitSlot(SlotRef node, C context) {
        return visitExpression(node, context);
    }

    public R visitFieldReference(FieldReference node, C context) {
        return visitExpression(node, context);
    }

    public R visitArrayExpr(ArrayExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitArrayElementExpr(ArrayElementExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitBetweenPredicate(BetweenPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitBinaryPredicate(BinaryPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitCompoundPredicate(CompoundPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitExistsPredicate(ExistsPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitArithmeticExpr(ArithmeticExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitTimestampArithmeticExpr(TimestampArithmeticExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitInPredicate(InPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitIsNullPredicate(IsNullPredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitLikePredicate(LikePredicate node, C context) {
        return visitExpression(node, context);
    }

    public R visitLiteral(LiteralExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitFunctionCall(FunctionCallExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitGroupingFunctionCall(GroupingFunctionCallExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitCastExpr(CastExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitCaseWhenExpr(CaseExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitSubquery(Subquery node, C context) {
        return visitExpression(node, context);
    }

    public R visitAnalyticExpr(AnalyticExpr node, C context) {
        return visitExpression(node, context);
    }

    public R visitInformationFunction(InformationFunction node, C context) {
        return visitExpression(node, context);
    }

    public R visitSysVariableDesc(SysVariableDesc node, C context) {
        return visitExpression(node, context);
    }

    public R visitDefaultValueExpr(DefaultValueExpr node, C context) {
        return visitExpression(node, context);
    }
}