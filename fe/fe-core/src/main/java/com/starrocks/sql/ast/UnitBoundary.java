// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TExprNode;

/*
 * UnitBoundary used to specify time boundary of time_slice:
 * FLOOR specify START as result time.
 * CEIL specify END as result time.
 */
public class UnitBoundary extends LiteralExpr {
    private final String description;

    public UnitBoundary(String description) {
        this.description = description.toUpperCase();
    }

    public String getDescription() {
        return description;
    }

    @Override
    protected String toSqlImpl() {
        return "unitBoundary " + description;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new StarRocksPlannerException("UnitBoundary not implement toThrift", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Expr clone() {
        return new UnitBoundary(this.description);
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
