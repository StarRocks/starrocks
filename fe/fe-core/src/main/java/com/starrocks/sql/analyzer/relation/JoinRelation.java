// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.analyzer.RelationFields;

public class JoinRelation extends Relation {
    private final JoinOperator type;
    private final Relation left;
    private final Relation right;
    private final Expr onPredicate;
    private String joinHint = "";
    private final boolean lateral;

    public JoinRelation(JoinOperator type, Relation left, Relation right, Expr onPredicate, boolean isLateral) {
        super(left.getRelationFields().joinWith(right.getRelationFields()));
        this.type = type;
        this.left = left;
        this.right = right;
        this.onPredicate = onPredicate;
        this.lateral = isLateral;
    }

    public JoinOperator getType() {
        return type;
    }

    public Relation getLeft() {
        return left;
    }

    public Relation getRight() {
        return right;
    }

    public Expr getOnPredicate() {
        return onPredicate;
    }

    public void setJoinHint(String joinHint) {
        this.joinHint = joinHint;
    }

    public String getJoinHint() {
        return joinHint;
    }

    public boolean isLateral() {
        return lateral;
    }

    @Override
    public RelationFields getRelationFields() {
        if (type.isLeftSemiAntiJoin()) {
            return left.getRelationFields();
        } else if (type.isRightSemiAntiJoin()) {
            return right.getRelationFields();
        } else {
            return super.getRelationFields();
        }
    }

    @Override
    public <R, C> R accept(RelationVisitor<R, C> visitor, C context) {
        return visitor.visitJoin(this, context);
    }
}