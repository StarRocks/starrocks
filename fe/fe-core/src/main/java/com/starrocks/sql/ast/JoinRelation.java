// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class JoinRelation extends Relation {
    private final JoinOperator joinOp;
    private Relation left;
    private Relation right;
    private Expr onPredicate;
    private String joinHint = "";
    private boolean lateral;
    private boolean isImplicit;

    /**
     * usingColNames is created by parser
     * and will be converted to onPredicate in Analyzer
     */
    private List<String> usingColNames;

    public JoinRelation(JoinOperator joinOp, Relation left, Relation right, Expr onPredicate, boolean isLateral) {
        if (joinOp == null) {
            this.joinOp = JoinOperator.CROSS_JOIN;
            isImplicit = true;
        } else {
            this.joinOp = joinOp;
        }
        this.left = left;
        this.right = right;
        this.onPredicate = onPredicate;
        this.lateral = isLateral;
    }

    public JoinOperator getJoinOp() {
        return joinOp;
    }

    public Relation getLeft() {
        return left;
    }

    public Relation getRight() {
        return right;
    }

    public void setLeft(Relation left) {
        this.left = left;
    }

    public void setRight(Relation right) {
        this.right = right;
    }

    public Expr getOnPredicate() {
        return onPredicate;
    }

    public void setOnPredicate(Expr onPredicate) {
        this.onPredicate = onPredicate;
    }

    public void setJoinHint(String joinHint) {
        this.joinHint = StringUtils.upperCase(joinHint);
    }

    public String getJoinHint() {
        return joinHint;
    }

    public boolean isLateral() {
        return lateral;
    }

    public void setLateral(boolean lateral) {
        this.lateral = lateral;
    }

    public boolean isImplicit() {
        return isImplicit;
    }

    public List<String> getUsingColNames() {
        return usingColNames;
    }

    public void setUsingColNames(List<String> usingColNames) {
        this.usingColNames = usingColNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitJoin(this, context);
    }
}