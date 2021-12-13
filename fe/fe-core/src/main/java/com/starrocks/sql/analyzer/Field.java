// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer;

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
    private final TableName tableName;
    //private final Expr originExpression;
    private final RelationId originRelationId;

    public Field(String name, Type type, TableName tableName, RelationId originRelationId) {
        this(name, type, tableName, originRelationId, true);
    }

    public Field(String name, Type type, TableName tableName, RelationId originRelationId, boolean visible) {
        this.name = name;
        this.type = type;
        this.tableName = tableName;
        this.originRelationId = originRelationId;
        this.visible = visible;
    }

    public String getName() {
        return name;
    }

    public TableName getTableName() {
        return tableName;
    }

    public RelationId getOriginRelationId() {
        return originRelationId;
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
            if (this.tableName == null) {
                return false;
            }
            return this.tableName.getTbl().equals(expr.getTblNameWithoutAnalyzed().getTbl())
                    && expr.getColumnName().equalsIgnoreCase(this.name);
        } else {
            return expr.getColumnName().equalsIgnoreCase(this.name);
        }
    }

    public boolean matchesPrefix(TableName prefix) {
        if (tableName != null) {
            return tableName.getTbl().equals(prefix.getTbl());
        }
        return false;
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