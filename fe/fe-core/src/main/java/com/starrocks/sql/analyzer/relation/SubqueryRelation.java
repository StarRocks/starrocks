// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;

import java.util.List;

public class SubqueryRelation extends Relation {
    private final String name;
    private final QueryRelation query;

    public SubqueryRelation(String name, QueryRelation query, List<Field> relationFields) {
        super(new RelationFields(relationFields));
        this.name = name;
        this.query = query;
        // The order by is meaningless in subquery
        if (this.query instanceof QuerySpecification && !((QuerySpecification) this.query).hasLimit()) {
            QuerySpecification qs = (QuerySpecification) this.query;
            qs.clearOrder();
        }
    }

    public String getName() {
        return name;
    }

    public QueryRelation getQuery() {
        return query;
    }

    @Override
    public String toString() {
        return name == null ? "anonymous" : name;
    }

    public <R, C> R accept(RelationVisitor<R, C> visitor, C context) {
        return visitor.visitSubquery(this, context);
    }
}
