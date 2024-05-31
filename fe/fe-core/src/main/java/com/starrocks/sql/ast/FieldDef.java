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

package com.starrocks.sql.ast;

import com.starrocks.analysis.ColumnPosition;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class FieldDef implements ParseNode {

    private final String fieldName;
    private final List<String> parentFiledNames;
    private final TypeDef typeDef;
    private final ColumnPosition fieldPos;

    public FieldDef(String fieldName, List<String> parentFiledNames, TypeDef typeDef, ColumnPosition fieldPos) {
        this.fieldName = fieldName;
        this.parentFiledNames = parentFiledNames;
        this.typeDef = typeDef;
        this.fieldPos = fieldPos;
    }

    public String getFieldName() {
        return fieldName;
    }

    public List<String> getParentFieldNames() {
        return parentFiledNames;
    }

    public String getParentFieldName(int i) {
        return parentFiledNames.get(i);
    }

    public TypeDef getTypeDef() {
        return typeDef;
    }

    public Type getType() {
        return typeDef.getType();
    }

    public ColumnPosition getFieldPos() {
        return fieldPos;
    }

    @Override
    public NodePosition getPos() {
        return null;
    }

    private boolean checkType(Type type) {
        if (type.isStructType() || type.isArrayType()) {
            return true;
        }
        return false;
    }

    private Type getFieldType(Type type, String fieldName) {
        if (type.isStructType()) {
            StructField field = ((StructType) type).getField(fieldName);
            if (field == null) {
                return null;
            }
            return field.getType();
        }

        if (type.isArrayType()) {
            return ((ArrayType) type).getItemType();
        }

        return null;
    }

    public void analyze(Column baseCol, boolean dropField) throws AnalysisException {
        if (baseCol == null) {
            throw new AnalysisException(String.format("Analyze add/drop field failed, modifyc column is not exist"));
        }

        Type targetFieldType = baseCol.getType();
        if (!checkType(targetFieldType)) {
            throw new AnalysisException(
                    String.format("column %s type %s is not Struct", baseCol.getName(), targetFieldType.toString()));
        }

        if (parentFiledNames != null && !parentFiledNames.isEmpty()) {
            for (String name : parentFiledNames) {
                targetFieldType = getFieldType(targetFieldType, name);
                if (targetFieldType == null) {
                    throw new AnalysisException(
                        String.format("No field %s exist in column %s", name, baseCol.getName()));
                }
                if (!checkType(targetFieldType)) {
                    throw new AnalysisException(
                        String.format("Field %s type %s is not valid", name, targetFieldType.toString()));
                }
            }
        }

        if (!targetFieldType.isStructType()) {
            throw new AnalysisException("Target Field is not struct");
        }

        StructField childField = ((StructType) targetFieldType).getField(fieldName);
        if (dropField) {
            if (childField == null) {
                throw new AnalysisException(String.format("Drop field %s is not found", fieldName));
            }
        } else {
            if (childField != null) {
                throw new AnalysisException(String.format("Field %s is already exist", fieldName));
            }

            if (typeDef == null) {
                throw new AnalysisException("No filed type in field definition");
            }
            if (typeDef.getType().isScalarType()) {
                final ScalarType targetType = (ScalarType) typeDef.getType();
                if (targetType.getPrimitiveType().isStringType()) {
                    if (targetType.getLength() <= 0) {
                        targetType.setLength(1);
                    }
                }
            }
            typeDef.analyze();
            if (fieldPos != null) {
                fieldPos.analyze();
            }
        }
    }
}