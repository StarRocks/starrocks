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

import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.util.List;

public class TypeDefAnalyzer {
    public static void analyze(TypeDef typeDef) {
        Type parsedType = typeDef.getType();

        // Check the max nesting depth before calling the recursive analyze() to avoid
        // a stack overflow.
        if (parsedType.exceedsMaxNestingDepth()) {
            throw new SemanticException(String.format(
                    "Type exceeds the maximum nesting depth of %s:\n%s",
                    Type.MAX_NESTING_DEPTH, parsedType.toSql()));
        }
        analyze(parsedType);
    }

    private static void analyze(Type type) {
        if (!type.isSupported()) {
            throw new SemanticException("Unsupported data type: " + type.toSql());
        }
        if (type.isScalarType()) {
            analyzeScalarType((ScalarType) type);
        } else if (type.isArrayType()) {
            analyzeArrayType((ArrayType) type);
        } else if (type.isStructType()) {
            analyzeStructType((StructType) type);
        } else if (type.isMapType()) {
            analyzeMapType((MapType) type);
        } else {
            throw new SemanticException("Unsupported data type: " + type.toSql());
        }
    }

    private static void analyzeScalarType(ScalarType scalarType) {
        PrimitiveType type = scalarType.getPrimitiveType();
        switch (type) {
            case CHAR:
            case VARCHAR: {
                String name;
                int maxLen;
                if (type == PrimitiveType.VARCHAR) {
                    name = "Varchar";
                    maxLen = TypeFactory.getOlapMaxVarcharLength();
                } else {
                    name = "Char";
                    maxLen = ScalarType.MAX_CHAR_LENGTH;
                }
                int len = scalarType.getLength();
                // len is decided by child, when it is -1.

                if (len <= 0) {
                    throw new SemanticException(name + " size must be > 0: " + len);
                }
                if (scalarType.getLength() > maxLen) {
                    throw new SemanticException(
                            name + " size must be <= " + maxLen + ": " + len);
                }
                break;
            }
            case VARBINARY: {
                String name = "VARBINARY";
                int maxLen = TypeFactory.getOlapMaxVarcharLength();
                int len = scalarType.getLength();
                // len is decided by child, when it is -1.
                if (scalarType.getLength() > maxLen) {
                    throw new SemanticException(
                            name + " size must be <= " + maxLen + ": " + len);
                }
                break;
            }
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256: {
                final String name = scalarType.getPrimitiveType().name();
                final int precision = scalarType.decimalPrecision();
                final int scale = scalarType.decimalScale();
                final int max_precision = PrimitiveType.getMaxPrecisionOfDecimal(scalarType.getPrimitiveType());
                final int max_scale = type.isDecimalV2Type() ? Math.min(9, precision) : precision;
                if (precision < 1 || precision > max_precision) {
                    throw new SemanticException(
                            String.format("Precision of %s must between 1 and %d, precision was set to: %d.",
                                    name, max_precision, precision));
                }
                if (scale < 0 || scale > max_scale) {
                    throw new SemanticException(
                            String.format("Scale of %s must between 0 and %d,  scale was set to: %d.",
                                    name, max_scale, scale));
                }
                break;
            }
            case INVALID_TYPE:
                throw new SemanticException("Invalid type.");
            default:
                break;
        }
    }

    private static void analyzeArrayType(ArrayType type) {
        Type baseType = getInnermostType(type);
        if (baseType == null) {
            throw new SemanticException("Cannot get innermost type of '" + type + "'");
        }
        analyze(baseType);
        if (baseType.isHllType() || baseType.isBitmapType() || baseType.isPseudoType() || baseType.isPercentile()) {
            throw new SemanticException("Invalid data type: " + type.toSql());
        }
    }

    // getInnermostType() is only used for array
    private static Type getInnermostType(Type type) {
        if (type.isScalarType() || type.isStructType() || type.isMapType()) {
            return type;
        }
        if (type.isArrayType()) {
            return getInnermostType(((ArrayType) type).getItemType());
        }

        return null;
    }

    private static void analyzeStructType(StructType type) {
        List<StructField> structFields = type.getFields();
        for (StructField structField : structFields) {
            analyze(structField.getType());
        }
    }

    private static void analyzeMapType(MapType type) {
        Type keyType = type.getKeyType();
        if (!keyType.isValidMapKeyType()) {
            throw new SemanticException("Invalid map.key's type: " + keyType.toSql() +
                    ", which should be base types");
        }
        analyze(keyType);
        Type valueType = type.getValueType();
        analyze(valueType);
    }
}
