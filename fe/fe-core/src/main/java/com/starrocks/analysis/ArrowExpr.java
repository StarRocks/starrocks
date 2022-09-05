// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TExprNode;

/**
 * ArrowExpr: col->'xxx'
 * Mostly used in JSON expression
 */
public class ArrowExpr extends Expr {

    public ArrowExpr(Expr left, Expr right) {
        this.children.add(left);
        this.children.add(right);
    }

    public ArrowExpr(ArrowExpr rhs) {
        super(rhs);
    }

    public Expr getItem() {
        Preconditions.checkState(getChildren().size() == 2);
        return getChild(0);
    }

    public Expr getKey() {
        Preconditions.checkState(getChildren().size() == 2);
        return getChild(1);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(false, "unreachable");
    }

    @Override
    protected String toSqlImpl() {
        return String.format("%s->%s", getItem().toSqlImpl(), getKey().toSqlImpl());
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new StarRocksPlannerException("not support", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Expr clone() {
        return new ArrowExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitArrowExpr(this, context);
    }
}
