// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

public class CloneExpr extends Expr {
    public CloneExpr(Expr child) {
        super();
        this.addChild(child);
        setType(child.getType());
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    @Override
    public Type getType() {
        return getChild(0).getType();
    }

    @Override
    protected String toSqlImpl() {
        return "clone(" + getChild(0).toSqlImpl() + ")";
    }

    @Override
    protected String explainImpl() {
        return "clone(" + getChild(0).explain() + ")";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.CLONE_EXPR);
    }

    @Override
    public Expr clone() {
        return new CloneExpr(getChild(0));
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCloneExpr(this, context);
    }
}
