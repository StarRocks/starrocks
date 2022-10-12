// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TExprNode;

public class IntervalLiteral extends LiteralExpr {
    private final Expr value;
    private final UnitIdentifier unitIdentifier;

    public IntervalLiteral(Expr value, UnitIdentifier unitIdentifier) {
        this.value = value;
        this.unitIdentifier = unitIdentifier;
    }

    public Expr getValue() {
        return value;
    }

    public UnitIdentifier getUnitIdentifier() {
        return unitIdentifier;
    }

    @Override
    protected String toSqlImpl() {
        return "interval " + value.toSql() + unitIdentifier;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new StarRocksPlannerException("IntervalLiteral not implement toThrift", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Expr clone() {
        return new IntervalLiteral(this.value, this.unitIdentifier);
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        return 0;
    }
}
