package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.List;

public class LambdaFunction extends Expr {
    public LambdaFunction(Expr left, Expr right) {
        this.children.add(left);
        this.children.add(right);
    }
    public LambdaFunction(Expr left) {
        this.children.add(left);
    }
    public LambdaFunction(LambdaFunction rhs) {
        super(rhs);
    }


    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Preconditions.checkState(false, "unreachable");
    }

    @Override
    protected String toSqlImpl() {
        return String.format("%s->%s", getChild(0).toSqlImpl(), getChild(1).toSqlImpl());
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.LAMBDA_FUNCTION);
    }

    @Override
    public Expr clone() {
        return new LambdaFunction(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLambdaFunction(this, context);
    }

}