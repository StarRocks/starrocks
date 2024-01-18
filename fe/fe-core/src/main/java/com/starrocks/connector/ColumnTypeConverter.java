// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ColumnTypeConverter {
    public static final String DECIMAL_PATTERN = "^decimal\\((\\d+),(\\d+)\\)";
    public static final String COMPLEX_PATTERN = "([0-9a-z<>(),:_]+)";
    public static final String ARRAY_PATTERN = "^array<" + COMPLEX_PATTERN + ">";
    public static final String MAP_PATTERN = "^map<" + COMPLEX_PATTERN + ">";
    public static final String STRUCT_PATTERN = "^struct<" + COMPLEX_PATTERN + ">";
    public static final String CHAR_PATTERN = "^char\\(([0-9]+)\\)";
    public static final String VARCHAR_PATTERN = "^varchar\\(([0-9,-1]+)\\)";
    protected static final List<String> HIVE_UNSUPPORTED_TYPES = Arrays.asList("BINARY", "UNIONTYPE");

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
                return ScalarType.createDefaultExternalTableString();
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
            case  "STRUCT":
                Type structType = fromHiveTypeToStructType(hiveType);
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
                Type type = new ArrayType(fromHudiType(avroSchema.getElementType()), true);
                if (type.isArrayType()) {
                    return type;
                } else {
                    isConvertedFailed = true;
                    break;
                }
            case FIXED:
            case BYTES:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    int precision = ((LogicalTypes.Decimal) logicalType).getPrecision();
                    int scale = ((LogicalTypes.Decimal) logicalType).getScale();
                    return ScalarType.createUnifiedDecimalType(precision, scale);
                } else {
                    primitiveType = PrimitiveType.VARCHAR;
                    break;
                }
            case RECORD:
                // Struct type
                List<Schema.Field> fields = avroSchema.getFields();
                Preconditions.checkArgument(fields.size() > 0);
                ArrayList<StructField> structFields = new ArrayList<>(fields.size());
                for (Schema.Field field : fields) {
                    String fieldName = field.name();
                    Type fieldType = fromHudiType(field.schema());
                    if (fieldType.isUnknown()) {
                        isConvertedFailed = true;
                        break;
                    }
                    structFields.add(new StructField(fieldName, fieldType));
                }

                if (!isConvertedFailed) {
                    return new StructType(structFields);
                }
            case MAP:
                Schema value = avroSchema.getValueType();
                Type valueType = fromHudiType(value);
                if (valueType.isUnknown()) {
                    isConvertedFailed = true;
                    break;
                }

                if (!isConvertedFailed) {
                    // Hudi map's key must be string
                    return new MapType(ScalarType.createDefaultString(), valueType);
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
            default:
                isConvertedFailed = true;
                break;
        }

        if (isConvertedFailed) {
            primitiveType = PrimitiveType.UNKNOWN_TYPE;
        }

        return ScalarType.createType(primitiveType);
    }

    // used for HUDI MOR reader only
    // convert hudi column type(avroSchema) to hive type string with some customization
    public static String fromHudiTypeToHiveTypeString(Schema avroSchema) {
        Schema.Type columnType = avroSchema.getType();
        LogicalType logicalType = avroSchema.getLogicalType();

        switch (columnType) {
<<<<<<< HEAD
=======
            case BOOLEAN:
                return "boolean";
            case INT:
                if (logicalType instanceof LogicalTypes.Date) {
                    return "date";
                } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                    throw new StarRocksConnectorException("Unsupported hudi {} type of column {}",
                            logicalType.getName(), avroSchema.getName());
                } else {
                    return "int";
                }
            case LONG:
                if (logicalType instanceof LogicalTypes.TimeMicros) {
                    throw new StarRocksConnectorException("Unsupported hudi {} type of column {}",
                            logicalType.getName(), avroSchema.getName());
                } else if (logicalType instanceof LogicalTypes.TimestampMillis
                        || logicalType instanceof LogicalTypes.TimestampMicros) {
                    // customized value for int64 based timestamp
                    return logicalType.getName();
                } else {
                    return "bigint";
                }
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case STRING:
                return "string";
            case ARRAY:
                String elementType = fromHudiTypeToHiveTypeString(avroSchema.getElementType());
                return String.format("array<%s>", elementType);
            case FIXED:
            case BYTES:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    int precision = ((LogicalTypes.Decimal) logicalType).getPrecision();
                    int scale = ((LogicalTypes.Decimal) logicalType).getScale();
                    return String.format("decimal(%s,%s)", precision, scale);
                } else {
                    return "string";
                }
            case RECORD:
                // Struct type
                List<Schema.Field> fields = avroSchema.getFields();
                Preconditions.checkArgument(fields.size() > 0);
                String nameToType = fields.stream()
                        .map(f -> String.format("%s:%s", f.name(),
                                fromHudiTypeToHiveTypeString(f.schema())))
                        .collect(Collectors.joining(","));
                return String.format("struct<%s>", nameToType);
            case MAP:
                Schema value = avroSchema.getValueType();
                String valueType = fromHudiTypeToHiveTypeString(value);
                return String.format("map<%s,%s>", "string", valueType);
            case UNION:
                List<Schema> nonNullMembers = avroSchema.getTypes().stream()
                        .filter(schema -> !Schema.Type.NULL.equals(schema.getType()))
                        .collect(Collectors.toList());
                return fromHudiTypeToHiveTypeString(nonNullMembers.get(0));
            case ENUM:
            default:
                throw new StarRocksConnectorException("Unsupported hudi {} type of column {}",
                        avroSchema.getType().getName(), avroSchema.getName());
        }
    }

    public static Type fromDeltaLakeType(DataType dataType) {
        if (dataType == null) {
            return Type.NULL;
        }
        PrimitiveType primitiveType;
        DeltaDataType deltaDataType = DeltaDataType.instanceFrom(dataType.getClass());
        switch (deltaDataType) {
>>>>>>> 2.5.18
            case BOOLEAN:
                return "boolean";
            case INT:
                if (logicalType instanceof LogicalTypes.Date) {
                    return "date";
                } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                    throw new StarRocksConnectorException("Unsupported hudi {} type of column {}",
                            logicalType.getName(), avroSchema.getName());
                } else {
                    return "int";
                }
            case LONG:
                if (logicalType instanceof LogicalTypes.TimeMicros) {
                    throw new StarRocksConnectorException("Unsupported hudi {} type of column {}",
                            logicalType.getName(), avroSchema.getName());
                } else if (logicalType instanceof LogicalTypes.TimestampMillis
                        || logicalType instanceof LogicalTypes.TimestampMicros) {
                    // customized value for int64 based timestamp
                    return logicalType.getName();
                } else {
                    return "bigint";
                }
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case STRING:
                return "string";
            case ARRAY:
                String elementType = fromHudiTypeToHiveTypeString(avroSchema.getElementType());
                return String.format("array<%s>", elementType);
            case FIXED:
            case BYTES:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    int precision = ((LogicalTypes.Decimal) logicalType).getPrecision();
                    int scale = ((LogicalTypes.Decimal) logicalType).getScale();
                    return String.format("decimal(%s,%s)", precision, scale);
                } else {
                    return "string";
                }
            case RECORD:
                // Struct type
                List<Schema.Field> fields = avroSchema.getFields();
                Preconditions.checkArgument(fields.size() > 0);
                String nameToType = fields.stream()
                        .map(f -> String.format("%s:%s", f.name(),
                                fromHudiTypeToHiveTypeString(f.schema())))
                        .collect(Collectors.joining(","));
                return String.format("struct<%s>", nameToType);
            case MAP:
                Schema value = avroSchema.getValueType();
                String valueType = fromHudiTypeToHiveTypeString(value);
                return String.format("map<%s,%s>", "string", valueType);
            case UNION:
                List<Schema> nonNullMembers = avroSchema.getTypes().stream()
                        .filter(schema -> !Schema.Type.NULL.equals(schema.getType()))
                        .collect(Collectors.toList());
                return fromHudiTypeToHiveTypeString(nonNullMembers.get(0));
            case ENUM:
            default:
                throw new StarRocksConnectorException("Unsupported hudi {} type of column {}",
                        avroSchema.getType().getName(), avroSchema.getName());
        }
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
                itemType = new ArrayType(fromHiveTypeToArrayType(matcher.group(1)), true);
            }
        } else {
            itemType = fromHiveType(typeStr);
        }
        return itemType;
    }

    public static Type fromHiveTypeToStructType(String typeStr) {
        Matcher matcher = Pattern.compile(STRUCT_PATTERN).matcher(typeStr.toLowerCase(Locale.ROOT));
        if (matcher.find()) {
            String str = matcher.group(1);
            String[] subfields = splitByFirstLevel(str, ',');
            ArrayList<StructField> structFields = new ArrayList<>(subfields.length);
            for (String subfield : subfields) {
                String[] structField = splitByFirstLevel(subfield, ':');
                if (structField.length != 2) {
                    throw new StarRocksConnectorException("Error Struct Type" + typeStr);
                }
                structFields.add(new StructField(structField[0], fromHiveType(structField[1])));
            }
            return new StructType(structFields);
        } else {
            throw new StarRocksConnectorException("Failed to get StructType at " + typeStr);
        }
    }

    // Like "a: int, b: struct<a: int, b: double>" => ["a: int", "b: struct<a:int, b: double>"]
    // It will do trim operator automatically.
    public static String[] splitByFirstLevel(String str, char splitter) {
        int level = 0;
        int start = 0;
        List<String> list = new LinkedList<>();
        char[] cStr = str.toCharArray();
        for (int i = 0; i < cStr.length; i++) {
            char c = cStr[i];
            if (c == '<' || c == '(') {
                level++;
            } else if (c == '>' || c == ')') {
                level--;
            } else if (c == splitter && level == 0) {
                list.add(str.substring(start, i).trim());
                start = i + 1;
            }
        }

        if (start < cStr.length) {
            list.add(str.substring(start, cStr.length).trim());
        }
        return list.toArray(new String[] {});
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

    public static boolean validateHiveColumnType(Type type, Type otherType) {
        if (type == null || otherType == null) {
            return false;
        }

        if (type == Type.UNKNOWN_TYPE || otherType == Type.UNKNOWN_TYPE) {
            return false;
        }

        if (type.isArrayType()) {
            if (otherType.isArrayType()) {
                return validateHiveColumnType(((ArrayType) type).getItemType(), ((ArrayType) otherType).getItemType());
            } else {
                return false;
            }
        }

        if (type.isMapType()) {
            if (otherType.isMapType()) {
                return validateHiveColumnType(((MapType) type).getKeyType(), ((MapType) otherType).getKeyType()) &&
                        validateHiveColumnType(((MapType) type).getValueType(), ((MapType) otherType).getValueType());
            } else {
                return false;
            }
        }

        if (type.isStructType()) {
            if (otherType.isStructType()) {
                StructType structType = (StructType) type;
                StructType otherStructType = (StructType) otherType;
                for (int i = 0; i < structType.getFields().size(); i++) {
                    if (!validateHiveColumnType(
                            structType.getField(i).getType(),
                            otherStructType.getField(i).getType())) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        }

        PrimitiveType primitiveType = type.getPrimitiveType();
        PrimitiveType otherPrimitiveType = otherType.getPrimitiveType();
        switch (primitiveType) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATETIME:
            case DATE:
            case BOOLEAN:
            case CHAR:
                return primitiveType == otherPrimitiveType;
            case VARCHAR:
                return otherPrimitiveType == PrimitiveType.CHAR || otherPrimitiveType == PrimitiveType.VARCHAR;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return otherPrimitiveType.isDecimalOfAnyVersion();
            default:
                return false;
        }
    }

    public static boolean columnEquals(Column base, Column other) {
        if (base == other) {
            return true;
        }

        if (!base.getName().equalsIgnoreCase(other.getName())) {
            return false;
        }

        if (!base.getType().equals(other.getType())) {
            return false;
        }

        return true;
    }
}

