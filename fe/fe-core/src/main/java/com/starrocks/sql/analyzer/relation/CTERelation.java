// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.analyzer.relation;

import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;

import java.util.List;

public class CTERelation extends Relation {
    private final String cteId;

    private final String name;

    private QueryRelation cteQuery;

    public CTERelation(String cteId, String name, QueryRelation cteQuery, List<Field> relationFields) {
        super(new RelationFields(relationFields));
        this.cteId = cteId;
        this.name = name;
        this.cteQuery = cteQuery;
    }

    public QueryRelation getCteQuery() {
        return cteQuery;
    }

    public String getCteId() {
        return cteId;
    }

    @Override
    public String toString() {
        return name == null ? cteId : name;
    }

    @Override
    public <R, C> R accept(RelationVisitor<R, C> visitor, C context) {
        return visitor.visitCTE(this, context);
    }
}
