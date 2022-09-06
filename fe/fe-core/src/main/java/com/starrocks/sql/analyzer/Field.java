// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.DereferenceExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.QualifiedName;

public class Field {
    // The name here is a column name, not qualified name.
    private final String name;
    private Type type;
    // shadow column is not visible, e.g. schema change column and materialized column
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
        if (expr instanceof DereferenceExpr && !((DereferenceExpr) expr).isParsed()) {
            return canResolve((DereferenceExpr) expr);
        }
        TableName tableName = expr.getTblNameWithoutAnalyzed();
        if (tableName != null) {
            if (relationAlias == null) {
                return false;
            }
            return relationAlias.getTbl().equals(expr.getTblNameWithoutAnalyzed().getTbl())
                    && expr.getColumnName().equalsIgnoreCase(this.name);
        } else {
            return expr.getColumnName().equalsIgnoreCase(this.name);
        }
    }

    private boolean canResolve(DereferenceExpr expr) {
        if (relationAlias == null) {
            return false;
        }

        String[] fieldFullQualifiedName = new String[] {
                relationAlias.getCatalog(),
                relationAlias.getDb(),
                relationAlias.getTbl(),
                name
        };

        for (int i = 0; i < 4; i++) {
            if (tryToMatch(fieldFullQualifiedName, i, expr.getQualifiedName())) {
                expr.setSlotRef(relationAlias, name, name);
                return true;
            }
        }
        return false;
    }

    private boolean tryToMatch(String[] fieldFullQualifiedName, int index, QualifiedName qualifiedName) {
        String[] partsArray = qualifiedName.getParts().toArray(new String[0]);
        int partsIndex = 0;
        for (int i = index; i < 4 && partsIndex < partsArray.length; i++) {
            if (fieldFullQualifiedName[i] == null) {
                return false;
            }

            String part = partsArray[partsIndex++];
            String comparedPart = fieldFullQualifiedName[i];
            // Only table name is case-sensitive.
            if (i != 2) {
                part = part.toLowerCase();
                comparedPart = comparedPart.toLowerCase();
            }
            if (!part.equals(comparedPart)) {
                return false;
            }
        }

        // partsIndex reach end of partsArray, means match all part.
        if (partsIndex == partsArray.length) {
            return true;
        }

        // partsIndex not reach end of partsArray, maybe it's a struct type.
        Type tmpType = type;
        for (; partsIndex < partsArray.length; partsIndex++) {
            if (!tmpType.isStructType()) {
                return false;
            }
            StructField structField = ((StructType) tmpType).getField(partsArray[partsIndex]);
            if (structField == null) {
                return false;
            }
            tmpType = structField.getType();
        }
        return true;
    }

    public boolean matchesPrefix(TableName prefix) {
        if (relationAlias != null) {
            return relationAlias.getTbl().equals(prefix.getTbl());
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