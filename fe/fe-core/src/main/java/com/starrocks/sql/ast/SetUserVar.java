// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.SetVar;
import com.starrocks.catalog.Type;

public class SetUserVar extends SetVar {

    public SetUserVar(String variable, Expr value) {
        super(SetType.USER, variable, value);
    }

    public void analyze() {
        Expr expression = getExpression();
        if (expression instanceof NullLiteral) {
            setResolvedExpression(NullLiteral.create(Type.STRING));
        } else {
            Expr foldedExpression = Expr.analyzeAndCastFold(expression);
            if (foldedExpression instanceof LiteralExpr) {
                setResolvedExpression((LiteralExpr) foldedExpression);
            } else {
                //TODO : compute value from query statement
            }
        }
    }
}
