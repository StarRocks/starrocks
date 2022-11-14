// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TExprNode;

public class LambdaArgument extends Expr {
    private String name;
    private boolean nullable;

    public LambdaArgument(String name) {
        this.name = name;
    }

    public LambdaArgument(LambdaArgument rhs) {
        super(rhs);
        name = rhs.getName();
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(false, "unreachable");
    }

    @Override
    protected String toSqlImpl() {
        return name;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new StarRocksPlannerException("not support", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public Expr clone() {
        return new LambdaArgument(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLambdaArguments(this, context);
    }
}
