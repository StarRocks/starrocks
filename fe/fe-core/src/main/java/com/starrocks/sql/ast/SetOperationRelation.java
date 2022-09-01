// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;

import java.util.ArrayList;
import java.util.List;

public abstract class SetOperationRelation extends QueryRelation {
    private List<QueryRelation> relations;
    private final SetQualifier qualifier;

    public SetOperationRelation(List<QueryRelation> relations, SetQualifier qualifier) {
        this.relations = new ArrayList<>(relations);
        this.qualifier = qualifier;
    }

    public List<QueryRelation> getRelations() {
        return relations;
    }

    public void addRelation(QueryRelation relation) {
        relations.add(relation);
    }

    public SetQualifier getQualifier() {
        return qualifier;
    }

    public void setRelations(List<QueryRelation> relations) {
        this.relations = relations;
    }

    @Override
    public List<Expr> getOutputExpression() {
        return relations.get(0).getOutputExpression();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetOp(this, context);
    }
}
