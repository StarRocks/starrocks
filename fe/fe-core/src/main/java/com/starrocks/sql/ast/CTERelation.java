// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;

import java.util.List;

public class CTERelation extends Relation {
    private final int cteId;
    private final String name;
    private List<String> columnOutputNames;
    private final QueryStatement cteQueryStatement;
    private boolean resolvedInFromClause;

    public CTERelation(int cteId, String name, List<String> columnOutputNames, QueryStatement cteQueryStatement) {
        this.cteId = cteId;
        this.name = name;
        this.columnOutputNames = columnOutputNames;
        this.cteQueryStatement = cteQueryStatement;
    }

    public QueryStatement getCteQueryStatement() {
        return cteQueryStatement;
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

    public void setColumnOutputNames(List<String> columnOutputNames) {
        this.columnOutputNames = columnOutputNames;
    }

    public void setResolvedInFromClause(boolean resolvedInFromClause) {
        this.resolvedInFromClause = resolvedInFromClause;
    }

    public boolean isResolvedInFromClause() {
        return resolvedInFromClause;
    }

    public TableName getAliasWithoutNameRewrite() {
        return alias;
    }

    @Override
    public String toString() {
        return name == null ? String.valueOf(cteId) : name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCTE(this, context);
    }

    @Override
    public TableName getAlias() {
        if (alias != null) {
            return alias;
        } else {
            return new TableName(null, name);
        }
    }
}
