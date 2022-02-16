// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
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
    private List<String> columnOutputNames;
    protected List<OrderByElement> sortClause;
    protected LimitElement limit;
    private final List<CTERelation> cteRelations = new ArrayList<>();

    public QueryRelation(List<String> columnOutputNames) {
        this.columnOutputNames = columnOutputNames;
    }

    public List<String> getColumnOutputNames() {
        return columnOutputNames;
    }

    public void setColumnOutputNames(List<String> columnOutputNames) {
        this.columnOutputNames = columnOutputNames;
    }

    public Map<Expr, FieldId> getColumnReferences() {
        return Maps.newHashMap();
    }

    public void setOrderBy(List<OrderByElement> sortClause) {
        this.sortClause = sortClause;
    }

    public List<OrderByElement> getOrderBy() {
        return sortClause;
    }

    public boolean hasOrderByClause() {
        return sortClause != null;
    }

    public LimitElement getLimit() {
        return limit;
    }

    public void setLimit(LimitElement limit) {
        this.limit = limit;
    }

    public boolean hasLimit() {
        return limit != null;
    }

    public long getOffset() {
        return limit.getOffset();
    }

    public boolean hasWithClause() {
        return !cteRelations.isEmpty();
    }

    public void addCTERelation(CTERelation cteRelation) {
        this.cteRelations.add(cteRelation);
    }

    public List<CTERelation> getCteRelations() {
        return cteRelations;
    }

    @Override
    public String toSql() {
        StringBuilder sqlBuilder = new StringBuilder();

        if (hasOrderByClause()) {
            sqlBuilder.append(" ORDER BY ");
            for (int i = 0; i < sortClause.size(); ++i) {
                if (i != 0) {
                    sqlBuilder.append(", ");
                }
                sqlBuilder.append(sortClause.get(i).toSql());
            }
        }

        // Limit clause.
        if (limit != null) {
            sqlBuilder.append(limit.toSql());
        }
        return sqlBuilder.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQueryRelation(this, context);
    }
}
