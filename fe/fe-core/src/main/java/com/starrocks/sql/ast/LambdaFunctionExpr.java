// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.List;

public class LambdaFunctionExpr extends Expr {
    private LambdaFunctionOperator transformedOp = null;

    public LambdaFunctionExpr(List<Expr> arguments) {
        this.children.addAll(arguments);
    }

    public LambdaFunctionExpr(LambdaFunctionExpr rhs) {
        super(rhs);
    }

    public LambdaFunctionOperator getTransformed() {
        return transformedOp;
    }

    public void setTransformed(LambdaFunctionOperator op) {
        transformedOp = op;
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(false, "unreachable");
    }

    @Override
    protected String toSqlImpl() {
        String names = getChild(1).toSql();
        if (getChildren().size() > 2) {
            names = "(" + getChild(1).toSql();
            for (int i = 2; i < getChildren().size(); ++i) {
                names = names + ", " + getChild(i).toSql();
            }
            names = names + ")";
        }
        return String.format("%s -> %s", names, getChild(0).toSql());
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.LAMBDA_FUNCTION_EXPR);
    }

    @Override
    public Expr clone() {
        return new LambdaFunctionExpr(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLambdaFunctionExpr(this, context);
    }

}
