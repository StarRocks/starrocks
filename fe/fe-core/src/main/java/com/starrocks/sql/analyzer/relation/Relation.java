// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

import com.starrocks.sql.analyzer.RelationFields;

public abstract class Relation {
    /**
     * output fields of this relation
     */
    protected final RelationFields relationFields;

    public Relation(RelationFields relationFields) {
        this.relationFields = relationFields;
    }

    public RelationFields getRelationFields() {
        return relationFields;
    }

    public <R, C> R accept(RelationVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }
}