// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TExprNode;

public class IntervalLiteral extends LiteralExpr {
    private final Expr value;
    private final String timeUnitIdent;

    public IntervalLiteral(Expr value, String timeUnitIdent) {
        this.value = value;
        this.timeUnitIdent = timeUnitIdent;
    }

    public Expr getValue() {
        return value;
    }

    public String getTimeUnitIdent() {
        return timeUnitIdent;
    }

    @Override
    protected String toSqlImpl() {
        return "interval " + value.toSql() + timeUnitIdent;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new StarRocksPlannerException("IntervalLiteral not implement toThrift", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Expr clone() {
        return new IntervalLiteral(this.value, this.timeUnitIdent);
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
