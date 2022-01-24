// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer.relation;

import java.util.List;

public class CTERelation extends Relation {
    private final int cteId;
    private final String name;
    private final List<String> columnOutputNames;
    private final QueryRelation cteQuery;

    public CTERelation(int cteId, String name, List<String> columnOutputNames, QueryRelation cteQuery) {
        this.cteId = cteId;
        this.name = name;
        this.columnOutputNames = columnOutputNames;
        this.cteQuery = cteQuery;
    }

    public QueryRelation getCteQuery() {
        return cteQuery;
    }

    public int getCteId() {
        return cteId;
    }

    public String getName() {
        return name;
    }

    public List<String> getColumnOutputNames() {
        return columnOutputNames;
    }

    @Override
    public String toString() {
        return name == null ? String.valueOf(cteId) : name;
    }

    @Override
    public <R, C> R accept(RelationVisitor<R, C> visitor, C context) {
        return visitor.visitCTE(this, context);
    }
}
