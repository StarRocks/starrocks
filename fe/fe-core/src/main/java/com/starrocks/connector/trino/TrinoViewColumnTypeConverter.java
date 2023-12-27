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

package com.starrocks.connector.trino;

import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.starrocks.connector.ColumnTypeConverter.getCharLength;
import static com.starrocks.connector.ColumnTypeConverter.getPrecisionAndScale;
import static com.starrocks.connector.ColumnTypeConverter.getTypeKeyword;
import static com.starrocks.connector.ColumnTypeConverter.getVarcharLength;
import static com.starrocks.connector.ColumnTypeConverter.splitByFirstLevel;

public class TrinoViewColumnTypeConverter {

    public static final String COMPLEX_PATTERN = "([0-9a-z<>(),:_ \"]+)";
    public static final String ARRAY_PATTERN = "^array\\(" + COMPLEX_PATTERN + "\\)";

    public static final String MAP_PATTERN = "^map\\(" + COMPLEX_PATTERN + "\\)";

    public static final String STRUCT_PATTERN = "^row\\(" + COMPLEX_PATTERN + "\\)";

    private static final List<String> UNSUPPORTED_TYPES = Arrays.asList("UNIONTYPE");

    public static Type fromTrinoType(String trinoType) {
        String typeUpperCase = getTypeKeyword(trinoType).toUpperCase();
        PrimitiveType primitiveType;
        switch (typeUpperCase) {
            case "TINYINT":
                primitiveType = PrimitiveType.TINYINT;
                break;
            case "SMALLINT":
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case "INT":
            case "INTEGER":
                primitiveType = PrimitiveType.INT;
                break;
            case "BIGINT":
                primitiveType = PrimitiveType.BIGINT;
                break;
            case "REAL":
                primitiveType = PrimitiveType.FLOAT;
                break;
            case "DOUBLE":
            case "DOUBLE PRECISION":
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case "DECIMAL":
            case "NUMERIC":
                primitiveType = PrimitiveType.DECIMAL32;
                break;
            case "TIMESTAMP":
                primitiveType = PrimitiveType.DATETIME;
                break;
            case "DATE":
                primitiveType = PrimitiveType.DATE;
                break;
            case "STRING":
                return ScalarType.createDefaultCatalogString();
            case "VARCHAR":
                int length = 0;
                try {
                    length = getVarcharLength(trinoType);
                } catch (StarRocksConnectorException e) {
                    return ScalarType.createDefaultCatalogString();
                }
                return ScalarType.createVarcharType(length);
            case "CHAR":
                return ScalarType.createCharType(getCharLength(trinoType));
            case "BINARY":
            case "VARBINARY":
                return Type.VARBINARY;
            case "BOOLEAN":
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case "ARRAY":
                Type type = fromTrinoTypeToArrayType(trinoType);
                if (type.isArrayType()) {
                    return type;
                } else {
                    return Type.UNKNOWN_TYPE;
                }
            case "MAP":
                Type mapType = fromTrinoTypeToMapType(trinoType);
                if (mapType.isMapType()) {
                    return mapType;
                } else {
                    return Type.UNKNOWN_TYPE;
                }
            case "ROW":
                Type structType = fromTrinoTypeToStructType(trinoType);
                if (structType.isStructType()) {
                    return structType;
                } else {
                    return Type.UNKNOWN_TYPE;
                }
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }

        if (primitiveType != PrimitiveType.DECIMAL32) {
            return ScalarType.createType(primitiveType);
        } else {
            int[] parts = getPrecisionAndScale(trinoType);
            return ScalarType.createUnifiedDecimalType(parts[0], parts[1]);
        }
    }

    // Array string like "Array(Integer)"
    public static Type fromTrinoTypeToArrayType(String typeStr) {
        if (UNSUPPORTED_TYPES.stream().anyMatch(typeStr.toUpperCase()::contains)) {
            return Type.UNKNOWN_TYPE;
        }
        Matcher matcher = Pattern.compile(ARRAY_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        Type itemType;
        if (matcher.find()) {
            if (fromTrinoTypeToArrayType(matcher.group(1)).equals(Type.UNKNOWN_TYPE)) {
                itemType = Type.UNKNOWN_TYPE;
            } else {
                itemType = new ArrayType(fromTrinoTypeToArrayType(matcher.group(1)));
            }
        } else {
            itemType = fromTrinoType(typeStr);
        }
        return itemType;
    }

    public static Type fromTrinoTypeToStructType(String typeStr) {
        Matcher matcher = Pattern.compile(STRUCT_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        if (matcher.find()) {
            String str = matcher.group(1);
            String[] subfields = splitByFirstLevel(str, ',');
            ArrayList<StructField> structFields = new ArrayList<>(subfields.length);
            for (String subfield : subfields) {
                String[] structField = splitByFirstLevel(subfield, ' ');
                if (structField.length != 2) {
                    throw new StarRocksConnectorException("Error Struct Type" + typeStr);
                }
                structFields.add(new StructField(structField[0].replace("\"", ""),
                        fromTrinoType(structField[1])));
            }
            return new StructType(structFields);
        } else {
            throw new StarRocksConnectorException("Failed to get StructType at " + typeStr);
        }
    }

    public static Type fromTrinoTypeToMapType(String typeStr) {
        String[] kv = getKeyValueStr(typeStr);
        return new MapType(fromTrinoType(kv[0]), fromTrinoType(kv[1]));
    }

    public static String[] getKeyValueStr(String typeStr) {
        Matcher matcher = Pattern.compile(MAP_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        if (matcher.find()) {
            String kvStr = matcher.group(1);
            String[] kvs = splitByFirstLevel(kvStr, ',');
            if (kvs.length != 2) {
                throw new StarRocksConnectorException("Error Map Type" + typeStr);
            }
            return new String[] {kvs[0], kvs[1]};
        } else {
            throw new StarRocksConnectorException("Failed to get MapType at " + typeStr);
        }
    }
}
