// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

import com.starrocks.sql.optimizer.base.SetQualifier;

import java.util.ArrayList;
import java.util.List;

public abstract class SetOperationRelation extends QueryRelation {
    private final List<QueryRelation> relations;
    private final SetQualifier qualifier;

    public SetOperationRelation(List<QueryRelation> relations, SetQualifier qualifier) {
        //Use the first column names as the column name of the set-operation
        super(relations.get(0).getColumnOutputNames());
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
}
