// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.analyzer.relation;

public class CTERelation extends Relation {
    private final String cteId;
    private final String name;
    private final QueryRelation cteQuery;

    public CTERelation(String cteId, String name, QueryRelation cteQuery) {
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

    public String getName() {
        return name;
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
