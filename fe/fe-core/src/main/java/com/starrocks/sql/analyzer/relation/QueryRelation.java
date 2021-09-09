// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.sql.analyzer.FieldId;
import com.starrocks.sql.analyzer.Scope;

import java.util.List;
import java.util.Map;

public abstract class QueryRelation extends Relation {

    private List<Expr> outputExpr;
    /**
     * out fields is different with output expr
     * output fields is represent externally visible resolved names
     * such as "k+1 as alias_name", k+1 is ArithmericExpr, alias_name is the output field
     */
    private Scope outputScope;

    /**
     * columnOutputNames is the output column header on the terminal,
     * and outputExpr is the output expression.
     * Because outputExpr may be rewritten, we recorded the primitive SQL column name
     * The alias will also be recorded in columnOutputNames
     */
    private final List<String> columnOutputNames;

    public QueryRelation(List<Expr> outputExpr, Scope outputScope, List<String> columnOutputNames) {
        super(outputScope.getRelationFields());
        this.outputExpr = outputExpr;
        this.outputScope = outputScope;
        this.columnOutputNames = columnOutputNames;
    }

    public List<String> getColumnOutputNames() {
        return columnOutputNames;
    }

    public Scope getOutputScope() {
        Preconditions.checkState(outputScope != null);
        return outputScope;
    }

    public void setOutputScope(Scope outputScope) {
        this.outputScope = outputScope;
    }

    public List<Expr> getOutputExpr() {
        return outputExpr;
    }

    public void setOutputExpr(List<Expr> outputExpr) {
        this.outputExpr = outputExpr;
    }

    public Map<Expr, FieldId> getColumnReferences() {
        return Maps.newHashMap();
    }

    public <R, C> R accept(RelationVisitor<R, C> visitor, C context) {
        return visitor.visitQuery(this, context);
    }
}
