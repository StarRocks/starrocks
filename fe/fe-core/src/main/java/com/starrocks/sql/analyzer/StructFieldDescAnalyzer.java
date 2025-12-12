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

import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.sql.ast.ColumnPosition;
import com.starrocks.sql.ast.StructFieldDesc;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.type.ArrayType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;

import java.util.List;

public class StructFieldDescAnalyzer {
    public static void analyze(StructFieldDesc structFieldDesc, Column baseCol, boolean dropField) {
        if (baseCol == null) {
            throw new SemanticException("Analyze add/drop field failed, modify column is not exist");
        }

        Type targetFieldType = baseCol.getType();
        if (checkType(targetFieldType)) {
            throw new SemanticException(
                    String.format("column %s type %s is not Struct", baseCol.getName(), targetFieldType.toString()));
        }

        List<String> nestedParentFieldNames = structFieldDesc.getNestedParentFieldNames();

        if (nestedParentFieldNames != null && !nestedParentFieldNames.isEmpty()) {
            for (String name : nestedParentFieldNames) {
                targetFieldType = getFieldType(targetFieldType, name);
                if (targetFieldType == null) {
                    throw new SemanticException(
                            String.format("No field %s exist in column %s", name, baseCol.getName()));
                }
                if (checkType(targetFieldType)) {
                    throw new SemanticException(
                            String.format("Field %s type %s is not valid", name, targetFieldType.toString()));
                }
            }
        }

        if (!targetFieldType.isStructType()) {
            throw new SemanticException("Target Field is not struct");
        }

        String fieldName = structFieldDesc.getFieldName();
        TypeDef typeDef = structFieldDesc.getTypeDef();
        ColumnPosition fieldPos = structFieldDesc.getFieldPos();

        StructField childField = ((StructType) targetFieldType).getField(fieldName);
        if (dropField) {
            if (childField == null) {
                throw new SemanticException(String.format("Drop field %s is not found", fieldName));
            }
        } else {
            if (childField != null) {
                throw new SemanticException(String.format("Field %s is already exist", fieldName));
            }

            if (typeDef == null) {
                throw new SemanticException("No filed type in field definition");
            }
            if (typeDef.getType().isScalarType()) {
                final ScalarType targetType = (ScalarType) typeDef.getType();
                if (targetType.getPrimitiveType().isStringType()) {
                    if (targetType.getLength() <= 0) {
                        targetType.setLength(1);
                    }
                }
            }
            TypeDefAnalyzer.analyze(typeDef);
            if (fieldPos != null) {
                if (fieldPos != ColumnPosition.FIRST && Strings.isNullOrEmpty(fieldPos.getLastCol())) {
                    throw new SemanticException("Column is empty.");
                }
            }
        }
    }

    private static boolean checkType(Type type) {
        return !type.isStructType() && !type.isArrayType();
    }

    private static Type getFieldType(Type type, String fieldName) {
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
}
