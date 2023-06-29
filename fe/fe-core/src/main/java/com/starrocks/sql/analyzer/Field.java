// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QualifiedName;

import java.util.LinkedList;
import java.util.List;

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
    private boolean isNullable;

    // Record tmp match record.
    private final List<Integer> tmpUsedStructFieldPos = new LinkedList<>();

    public Field(String name, Type type, TableName relationAlias, Expr originExpression) {
        this(name, type, relationAlias, originExpression, true);
    }

    public Field(String name, Type type, TableName relationAlias, Expr originExpression, boolean visible) {
        this.name = name;
        this.type = type;
        this.relationAlias = relationAlias;
        this.originExpression = originExpression;
        this.visible = visible;
        this.isNullable = true;
    }

    public Field(String name, Type type, TableName relationAlias, Expr originExpression, boolean visible, boolean isNullable) {
        this.name = name;
        this.type = type;
        this.relationAlias = relationAlias;
        this.originExpression = originExpression;
        this.visible = visible;
        this.isNullable = isNullable;
    }

    public Field(Field other) {
        this.name = other.name;
        this.type = other.type;
        this.relationAlias = other.relationAlias;
        this.originExpression = other.originExpression;
        this.visible = other.visible;
        this.isNullable = other.isNullable;
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

    public boolean isNullable() {
        return isNullable;
    }

    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }

    public boolean canResolve(SlotRef expr) {
        if (type.isStructType()) {
            return tryToParseAsStructType(expr);
        }

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

    private boolean tryToParseAsStructType(SlotRef slotRef) {
        QualifiedName qualifiedName = slotRef.getQualifiedName();
        tmpUsedStructFieldPos.clear();

        if (qualifiedName == null) {
            return slotRef.getColumnName().equalsIgnoreCase(this.name);
        }

        if (relationAlias == null) {
            return false;
        }

        // Generate current field's full qualified name.
        // fieldFullQualifiedName: [CatalogName, DatabaseName, TableName, ColumnName]
        String[] fieldFullQualifiedName = new String[] {
                relationAlias.getCatalog(),
                relationAlias.getDb(),
                relationAlias.getTbl(),
                name
        };

        // First start matching from CatalogName, if it fails, then start matching from DatabaseName, and so on.
        for (int i = 0; i < 4; i++) {
            if (tryToMatch(fieldFullQualifiedName, i, qualifiedName)) {
                return true;
            }
        }
        return false;
    }

    private boolean tryToMatch(String[] fieldFullQualifiedName, int index, QualifiedName qualifiedName) {
        String[] slotRefPartsArray = qualifiedName.getParts().toArray(new String[0]);
        int matchIndex = 0;
        // i = 0 means match from catalog name,
        // i = 1, match from database name,
        // i = 2, match from table name, only table name is case-sensitive,
        // i = 3, match from column name.
        for (; index < 4 && matchIndex < slotRefPartsArray.length; index++) {
            if (fieldFullQualifiedName[index] == null) {
                return false;
            }

            String part = slotRefPartsArray[matchIndex++];
            String comparedPart = fieldFullQualifiedName[index];
            // Only table name is case-sensitive, we will convert other parts to lower case.
            if (index != 2) {
                part = part.toLowerCase();
                comparedPart = comparedPart.toLowerCase();
            }
            if (!part.equals(comparedPart)) {
                return false;
            }
        }

        if (index < 4) {
            // Not match to col name, return false directly.
            return false;
        }

        // matchIndex reach the end of slotRefPartsArray, means this SlotRef matched all.
        if (matchIndex == slotRefPartsArray.length) {
            return true;
        }

        // matchIndex not reach end of slotRefPartsArray, it must be StructType.
        Type tmpType = type;
        for (; matchIndex < slotRefPartsArray.length; matchIndex++) {
            if (!tmpType.isStructType()) {
                return false;
            }
            StructField structField = ((StructType) tmpType).getField(slotRefPartsArray[matchIndex]);
            if (structField == null) {
                return false;
            }
            // Record the struct field position that matches successfully.
            tmpUsedStructFieldPos.add(structField.getPosition());
            tmpType = structField.getType();
        }
        return true;
    }

    public List<Integer> getTmpUsedStructFieldPos() {
        return tmpUsedStructFieldPos;
    }

    public boolean matchesPrefix(TableName tableName) {
        if (tableName.getCatalog() != null && relationAlias.getCatalog() != null &&
                !tableName.getCatalog().equals(relationAlias.getCatalog())) {
            return false;
        }

        if (tableName.getDb() != null && !tableName.getDb().equals(relationAlias.getDb())) {
            return false;
        }

        if (ConnectContext.get() != null && ConnectContext.get().isRelationAliasCaseInsensitive()) {
            return tableName.getTbl().equalsIgnoreCase(relationAlias.getTbl());
        } else {
            return tableName.getTbl().equals(relationAlias.getTbl());
        }
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