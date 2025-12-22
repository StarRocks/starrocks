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

package com.starrocks.sql.parser;

import com.starrocks.common.Config;
import com.starrocks.sql.ast.Identifier;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BitmapType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.FunctionType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.MapType;
import com.starrocks.type.NullType;
import com.starrocks.type.PercentileType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StringType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.UnknownType;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import com.starrocks.type.VariantType;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.sql.parser.AstBuilderUtils.createPos;
import static com.starrocks.sql.parser.AstBuilderUtils.getIdentifier;
import static com.starrocks.sql.parser.ErrorMsgProxy.PARSER_ERROR_MSG;

public class TypeParser {
    public static Type getType(com.starrocks.sql.parser.StarRocksParser.TypeContext context) {
        if (context.baseType() != null) {
            return getBaseType(context.baseType());
        } else if (context.decimalType() != null) {
            return getDecimalType(context.decimalType());
        } else if (context.arrayType() != null) {
            return getArrayType(context.arrayType());
        } else if (context.structType() != null) {
            return getStructType(context.structType());
        } else {
            return getMapType(context.mapType());
        }
    }

    private static Type getBaseType(com.starrocks.sql.parser.StarRocksParser.BaseTypeContext context) {
        int length = -1;
        if (context.typeParameter() != null) {
            length = Integer.parseInt(context.typeParameter().INTEGER_VALUE().toString());
        }
        if (context.STRING() != null || context.TEXT() != null) {
            return TypeFactory.createVarcharType(StringType.DEFAULT_STRING_LENGTH);
        } else if (context.VARCHAR() != null) {
            return TypeFactory.createVarcharType(length);
        } else if (context.CHAR() != null) {
            return TypeFactory.createCharType(length);
        } else if (context.SIGNED() != null) {
            return IntegerType.INT;
        } else if (context.HLL() != null) {
            return HLLType.HLL;
        } else if (context.BINARY() != null || context.VARBINARY() != null) {
            return TypeFactory.createVarbinary(length);
        } else {
            String typeStr = context.getChild(0).getText();
            return getTypeByName(typeStr);
        }
    }

    public static ScalarType getDecimalType(com.starrocks.sql.parser.StarRocksParser.DecimalTypeContext context) {
        Integer precision = null;
        Integer scale = null;
        if (context.precision != null) {
            precision = Integer.parseInt(context.precision.getText());
            if (context.scale != null) {
                scale = Integer.parseInt(context.scale.getText());
            }
        }
        if (context.DECIMAL() != null || context.NUMBER() != null || context.NUMERIC() != null) {
            if (precision != null) {
                if (scale != null) {
                    return TypeFactory.createUnifiedDecimalType(precision, scale);
                }
                return TypeFactory.createUnifiedDecimalType(precision);
            }
            return TypeFactory.createUnifiedDecimalType(10, 0);
        } else if (context.DECIMAL32() != null || context.DECIMAL64() != null ||
                context.DECIMAL128() != null || context.DECIMAL256() != null) {
            if (!Config.enable_decimal_v3) {
                throw new ParsingException("Config field enable_decimal_v3 is false now, " +
                        "turn it on before decimal32/64/128/256 are used, " +
                        "execute cmd 'admin set frontend config (\"enable_decimal_v3\" = \"true\")' " +
                        "on every FE server");
            }
            final PrimitiveType primitiveType = PrimitiveType.valueOf(context.children.get(0).getText().toUpperCase());
            if (precision != null) {
                if (scale != null) {
                    return TypeFactory.createDecimalV3Type(primitiveType, precision, scale);
                }
                return TypeFactory.createDecimalV3Type(primitiveType, precision);
            }
            return TypeFactory.createDecimalV3Type(primitiveType);
        } else if (context.DECIMALV2() != null) {
            if (precision != null) {
                if (scale != null) {
                    return TypeFactory.createDecimalV2Type(precision, scale);
                }
                return TypeFactory.createDecimalV2Type(precision);
            }
            return DecimalType.DEFAULT_DECIMALV2;
        } else {
            throw new IllegalArgumentException("Unsupported type " + context.getText());
        }
    }

    public static ArrayType getArrayType(com.starrocks.sql.parser.StarRocksParser.ArrayTypeContext context) {
        return new ArrayType(getType(context.type()));
    }

    public static StructType getStructType(com.starrocks.sql.parser.StarRocksParser.StructTypeContext context) {
        ArrayList<StructField> fields = new ArrayList<>();
        List<com.starrocks.sql.parser.StarRocksParser.SubfieldDescContext> subfields =
                context.subfieldDescs().subfieldDesc();
        for (com.starrocks.sql.parser.StarRocksParser.SubfieldDescContext type : subfields) {
            Identifier fieldIdentifier = getIdentifier(type.identifier());
            String fieldName = fieldIdentifier.getValue();
            fields.add(new StructField(fieldName, getType(type.type()), null));
        }

        return new StructType(fields);
    }

    public static MapType getMapType(com.starrocks.sql.parser.StarRocksParser.MapTypeContext context) {
        Type keyType = getType(context.type(0));
        if (!keyType.isValidMapKeyType()) {
            throw new ParsingException(PARSER_ERROR_MSG.unsupportedType(keyType.toString(),
                    "for map's key, which should be base types"),
                    createPos(context.type(0)));
        }
        Type valueType = getType(context.type(1));
        return new MapType(keyType, valueType);
    }

    private static ScalarType getTypeByName(String typeName) {
        if (typeName == null) {
            return null;
        }

        String upperTypeName = typeName.toUpperCase();
        return switch (upperTypeName) {
            // Null type
            case "NULL_TYPE" -> NullType.NULL;

            // Boolean type
            case "BOOLEAN" -> BooleanType.BOOLEAN;

            // Integer types
            case "TINYINT" -> IntegerType.TINYINT;
            case "SMALLINT" -> IntegerType.SMALLINT;
            case "INTEGER", "UNSIGNED", "INT" -> IntegerType.INT;
            case "BIGINT" -> IntegerType.BIGINT;
            case "LARGEINT" -> IntegerType.LARGEINT;

            // Float types
            case "FLOAT" -> FloatType.FLOAT;
            case "DOUBLE" -> FloatType.DOUBLE;

            // Decimal types
            case "DECIMAL", "DECIMALV2" -> DecimalType.DEFAULT_DECIMALV2;
            case "DECIMAL32" -> DecimalType.DECIMAL32;
            case "DECIMAL64" -> DecimalType.DECIMAL64;
            case "DECIMAL128" -> DecimalType.DECIMAL128;
            case "DECIMAL256" -> DecimalType.DECIMAL256;

            // String types
            case "STRING" -> StringType.DEFAULT_STRING;
            case "CHAR" -> CharType.CHAR;
            case "VARCHAR" -> VarcharType.VARCHAR;

            // Date/Time types
            case "DATE" -> DateType.DATE;
            case "DATETIME" -> DateType.DATETIME;
            case "TIME" -> DateType.TIME;

            // Binary types
            case "VARBINARY" -> VarbinaryType.VARBINARY;

            // Special types
            case "HLL" -> HLLType.HLL;
            case "BITMAP" -> BitmapType.BITMAP;
            case "PERCENTILE" -> PercentileType.PERCENTILE;
            case "JSON" -> JsonType.JSON;
            case "VARIANT" -> VariantType.VARIANT;
            case "FUNCTION" -> FunctionType.FUNCTION;
            case "UNKNOWN_TYPE" -> UnknownType.UNKNOWN_TYPE;
            default -> null;
        };
    }
}
