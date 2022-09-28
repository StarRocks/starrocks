// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ColumnTypeConverter {
    public static final String DECIMAL_PATTERN = "^decimal\\((\\d+),(\\d+)\\)";
    public static final String ARRAY_PATTERN = "^array<([0-9a-z<>(),:]+)>";
    public static final String MAP_PATTERN = "^map<([0-9a-z<>(),:]+)>";
    public static final String CHAR_PATTERN = "^char\\(([0-9]+)\\)";
    public static final String VARCHAR_PATTERN = "^varchar\\(([0-9,-1]+)\\)";
    protected static final List<String> HIVE_UNSUPPORTED_TYPES = Arrays.asList("STRUCT", "BINARY", "MAP", "UNIONTYPE");

    public static Type fromHiveType(String hiveType) {
        String typeUpperCase = getTypeKeyword(hiveType).toUpperCase();
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
            case "FLOAT":
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
                return ScalarType.createDefaultString();
            case "VARCHAR":
                return ScalarType.createVarcharType(getVarcharLength(hiveType));
            case "CHAR":
                return ScalarType.createCharType(getCharLength(hiveType));
            case "BOOLEAN":
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case "ARRAY":
                Type type = fromHiveTypeToArrayType(hiveType);
                if (type.isArrayType()) {
                    return type;
                } else {
                    return Type.UNKNOWN_TYPE;
                }
            case "MAP":
                Type mapType = fromHiveTypeToMapType(hiveType);
                if (mapType.isMapType()) {
                    return mapType;
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
            int[] parts = getPrecisionAndScale(hiveType);
            return ScalarType.createUnifiedDecimalType(parts[0], parts[1]);
        }
    }

    // this func targets at convert hudi column type(avroSchema) to starrocks column type(primitiveType)
    public static Type fromHudiType(Schema avroSchema) {
        Schema.Type columnType = avroSchema.getType();
        LogicalType logicalType = avroSchema.getLogicalType();
        PrimitiveType primitiveType = null;
        boolean isConvertedFailed = false;

        switch (columnType) {
            case BOOLEAN:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case INT:
                if (logicalType instanceof LogicalTypes.Date) {
                    primitiveType = PrimitiveType.DATE;
                } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                    primitiveType = PrimitiveType.TIME;
                } else {
                    primitiveType = PrimitiveType.INT;
                }
                break;
            case LONG:
                if (logicalType instanceof LogicalTypes.TimeMicros) {
                    primitiveType = PrimitiveType.TIME;
                } else if (logicalType instanceof LogicalTypes.TimestampMillis
                        || logicalType instanceof LogicalTypes.TimestampMicros) {
                    primitiveType = PrimitiveType.DATETIME;
                } else {
                    primitiveType = PrimitiveType.BIGINT;
                }
                break;
            case FLOAT:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case STRING:
                return ScalarType.createDefaultString();
            case ARRAY:
                Type type = fromHudiTypeToArrayType(avroSchema);
                if (type.isArrayType()) {
                    return type;
                } else {
                    isConvertedFailed = false;
                    break;
                }
            case FIXED:
            case BYTES:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    int precision = 0;
                    int scale = 0;
                    if (avroSchema.getObjectProp("precision") instanceof Integer) {
                        precision = (int) avroSchema.getObjectProp("precision");
                    }
                    if (avroSchema.getObjectProp("scale") instanceof Integer) {
                        scale = (int) avroSchema.getObjectProp("scale");
                    }
                    return ScalarType.createUnifiedDecimalType(precision, scale);
                } else {
                    primitiveType = PrimitiveType.VARCHAR;
                    break;
                }
            case UNION:
                List<Schema> nonNullMembers = avroSchema.getTypes().stream()
                        .filter(schema -> !Schema.Type.NULL.equals(schema.getType()))
                        .collect(Collectors.toList());

                if (nonNullMembers.size() == 1) {
                    return fromHudiType(nonNullMembers.get(0));
                } else {
                    isConvertedFailed = true;
                    break;
                }
            case ENUM:
            case MAP:
            default:
                isConvertedFailed = true;
                break;
        }

        if (isConvertedFailed) {
            primitiveType = PrimitiveType.UNKNOWN_TYPE;
        }

        return ScalarType.createType(primitiveType);
    }

    public static String getTypeKeyword(String type) {
        String keyword = type;
        int parenthesesIndex;
        if ((parenthesesIndex = keyword.indexOf('<')) >= 0) {
            keyword = keyword.substring(0, parenthesesIndex).trim();
        } else if ((parenthesesIndex = keyword.indexOf('(')) >= 0) {
            keyword = keyword.substring(0, parenthesesIndex).trim();
        }
        return keyword;
    }

    // Decimal string like "Decimal(3,2)"
    public static int[] getPrecisionAndScale(String typeStr) {
        Matcher matcher = Pattern.compile(DECIMAL_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        if (matcher.find()) {
            return new int[] {Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2))};
        }
        throw new StarRocksConnectorException("Failed to get precision and scale at " + typeStr);
    }

    // Array string like "Array<Array<int>>"
    public static Type fromHiveTypeToArrayType(String typeStr) {
        if (!HIVE_UNSUPPORTED_TYPES.stream().filter(typeStr.toUpperCase()::contains).collect(Collectors.toList())
                .isEmpty()) {
            return Type.UNKNOWN_TYPE;
        }
        Matcher matcher = Pattern.compile(ARRAY_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        Type itemType;
        if (matcher.find()) {
            if (fromHiveTypeToArrayType(matcher.group(1)).equals(Type.UNKNOWN_TYPE)) {
                itemType = Type.UNKNOWN_TYPE;
            } else {
                itemType = new ArrayType(fromHiveTypeToArrayType(matcher.group(1)));
            }
        } else {
            itemType = fromHiveType(typeStr);
        }
        return itemType;
    }

    public static String[] getKeyValueStr(String typeStr) {
        Matcher matcher = Pattern.compile(MAP_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        if (matcher.find()) {
            String kvStr = matcher.group(1);
            int size = kvStr.length();
            int stack = 0;
            int index = 0;
            for (int i = 0; i < size; i++) {
                char c = kvStr.charAt(i);
                if (c == '<' || c == '(') {
                    stack++;
                } else if (c == '>' || c == ')') {
                    stack--;
                } else if (c == ',' && stack == 0) {
                    index = i;
                    break;
                }
            }
            if (index == 0 || index == size - 1) {
                throw new StarRocksConnectorException("Error Map Type" + typeStr);
            }
            return new String[] {kvStr.substring(0, index).trim(), kvStr.substring(index + 1, size).trim()};
        } else {
            throw new StarRocksConnectorException("Failed to get MapType at " + typeStr);
        }
    }

    // Map string like map<keytype, valuetype>
    public static Type fromHiveTypeToMapType(String typeStr) {
        String[] kv = getKeyValueStr(typeStr);
        return new MapType(fromHiveType(kv[0]), fromHiveType(kv[1]));
    }

    // Char string like char(100)
    public static int getCharLength(String typeStr) {
        Matcher matcher = Pattern.compile(CHAR_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        throw new StarRocksConnectorException("Failed to get char length at " + typeStr);
    }

    // Varchar string like varchar(100)
    public static int getVarcharLength(String typeStr) {
        Matcher matcher = Pattern.compile(VARCHAR_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        throw new StarRocksConnectorException("Failed to get varchar length at " + typeStr);
    }

    private static ArrayType fromHudiTypeToArrayType(Schema typeSchema) {
        return new ArrayType(fromHudiType(typeSchema.getElementType()));
    }
}
