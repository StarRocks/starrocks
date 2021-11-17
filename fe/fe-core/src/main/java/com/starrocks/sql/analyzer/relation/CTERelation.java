// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.analyzer.relation;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.analyzer.Scope;

import java.util.List;

public class CTERelation extends QueryRelation {
    private String cteId;
    private QueryRelation cteQueryRelation;

    public CTERelation(String cteId, QueryRelation cteQueryRelation) {
        super(cteQueryRelation.getOutputExpr(), cteQueryRelation.getOutputScope(),
                cteQueryRelation.getColumnOutputNames());
        this.cteId = cteId;
    }

    public CTERelation(List<Expr> outputExpr, Scope outputScope,
                       List<String> columnOutputNames,
                       QueryRelation cteQueryRelation, String cteId) {
        super(outputExpr, outputScope, columnOutputNames);
        this.cteQueryRelation = cteQueryRelation;
        this.cteId = cteId;
    }

    public QueryRelation getCteQueryRelation() {
        return cteQueryRelation;
    }

    public String getCteId() {
        return cteId;
    }
}
