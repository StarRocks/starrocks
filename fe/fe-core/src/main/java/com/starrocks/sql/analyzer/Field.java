// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Type;

public class Field {
    private final String name;
    private Type type;
    //shadow column is not visible, eg. schema change column and materialized column
    private final boolean visible;

    /**
     * TableName of field
     * relationAlias is origin table which table name is explicit, such as t0.a
     * Field come from scope is resolved by scope relation alias,
     * such as subquery alias and table relation name
     */
    private final TableName relationAlias;
    private final Expr originExpression;

    public Field(String name, Type type, TableName relationAlias, Expr originExpression) {
        this(name, type, relationAlias, originExpression, true);
    }

    public Field(String name, Type type, TableName relationAlias, Expr originExpression, boolean visible) {
        this.name = name;
        this.type = type;
        this.relationAlias = relationAlias;
        this.originExpression = originExpression;
        this.visible = visible;
    }

    public String getName() {
        return name;
    }

    public TableName getRelationAlias() {
        return relationAlias;
    }

    public Expr getOriginExpression() {
        return originExpression;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public boolean isVisible() {
        return visible;
    }

    public boolean canResolve(SlotRef expr) {
        TableName tableName = expr.getTblNameWithoutAnalyzed();
        if (tableName != null) {
            if (relationAlias == null) {
                return false;
            }
            return matchesPrefix(expr.getTblNameWithoutAnalyzed()) && expr.getColumnName().equalsIgnoreCase(this.name);
        } else {
            return expr.getColumnName().equalsIgnoreCase(this.name);
        }
    }

    public boolean matchesPrefix(TableName tableName) {
        if (tableName.getCatalog() != null && !tableName.getCatalog().equals(relationAlias.getCatalog())) {
            return false;
        }

        if (tableName.getDb() != null && !tableName.getDb().equals(relationAlias.getDb())) {
            return false;
        }

        return tableName.getTbl().equals(relationAlias.getTbl());
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        if (name == null) {
            result.append("<anonymous>");
        } else {
            result.append(name);
        }
        result.append(":").append(type);
        return result.toString();
    }
}