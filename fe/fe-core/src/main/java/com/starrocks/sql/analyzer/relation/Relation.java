// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;

public abstract class Relation {
    private Scope scope;

    public Relation() {
    }

    public Scope getScope() {
        if (scope == null) {
            throw new StarRocksPlannerException("Scope is null", ErrorType.INTERNAL_ERROR);
        }
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public RelationFields getRelationFields() {
        return scope.getRelationFields();
    }

    public <R, C> R accept(RelationVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }
}