// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.sql.analyzer.FieldId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class QueryRelation extends Relation {

    /**
     * columnOutputNames is the output column header on the terminal,
     * and outputExpr is the output expression.
     * Because outputExpr may be rewritten, we recorded the primitive SQL column name
     * The alias will also be recorded in columnOutputNames
     */
    private final List<String> columnOutputNames;

    private final List<CTERelation> cteRelations = new ArrayList<>();

    public QueryRelation(List<String> columnOutputNames) {
        this.columnOutputNames = columnOutputNames;
    }

    public List<String> getColumnOutputNames() {
        return columnOutputNames;
    }

    public Map<Expr, FieldId> getColumnReferences() {
        return Maps.newHashMap();
    }

    public void addCTERelation(CTERelation cteRelation) {
        this.cteRelations.add(cteRelation);
    }

    public List<CTERelation> getCteRelations() {
        return cteRelations;
    }

    public <R, C> R accept(RelationVisitor<R, C> visitor, C context) {
        return visitor.visitQuery(this, context);
    }
}
