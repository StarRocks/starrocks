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

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.delta.DeltaDataType;
import com.starrocks.connector.exception.StarRocksConnectorException;
import io.delta.standalone.types.DataType;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.types.Types;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Type.BIGINT;
import static com.starrocks.catalog.Type.BOOLEAN;
import static com.starrocks.catalog.Type.DATE;
import static com.starrocks.catalog.Type.DATETIME;
import static com.starrocks.catalog.Type.DOUBLE;
import static com.starrocks.catalog.Type.FLOAT;
import static com.starrocks.catalog.Type.INT;
import static com.starrocks.catalog.Type.SMALLINT;
import static com.starrocks.catalog.Type.TINYINT;
import static com.starrocks.catalog.Type.VARBINARY;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.binaryTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.booleanTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.byteTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.dateTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.doubleTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.floatTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getListTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getMapTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getStructTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getVarcharTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.intTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.longTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.shortTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.stringTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.timestampTypeInfo;

public class ColumnTypeConverter {
    public static final String DECIMAL_PATTERN = "^decimal\\((\\d+), *(\\d+)\\)";
    public static final String COMPLEX_PATTERN = "([0-9a-z<>(),:_ ]+)";
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
                return ScalarType.createDefaultCatalogString();
            case "VARCHAR":
                return ScalarType.createVarcharType(getVarcharLength(hiveType));
            case "CHAR":
                return ScalarType.createCharType(getCharLength(hiveType));
            case "BINARY":
                return Type.VARBINARY;
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

    public static String toHiveType(Type type) {
        TypeInfo typeInfo = toTypeInfo(type);
        return typeInfo.getTypeName();
    }

    public static TypeInfo toTypeInfo(Type type) {
        requireNonNull(type, "type is null");
        if (type.equals(TINYINT)) {
            return byteTypeInfo;
        } else if (type.equals(SMALLINT)) {
            return shortTypeInfo;
        } else if (type.equals(INT)) {
            return intTypeInfo;
        } else if (type.equals(BIGINT)) {
            return longTypeInfo;
        } else if (type.equals(FLOAT)) {
            return floatTypeInfo;
        } else if (type.equals(DOUBLE)) {
            return doubleTypeInfo;
        } else if (type.isDecimalOfAnyVersion()) {
            return new DecimalTypeInfo(type.getPrecision(), type.getDecimalDigits());
        } else if (type.equals(BOOLEAN)) {
            return booleanTypeInfo;
        } else if (type.equals(VARBINARY)) {
            return binaryTypeInfo;
        } else if (type.equals(DATE)) {
            return dateTypeInfo;
        } else if (type.equals(DATETIME)) {
            return timestampTypeInfo;
        } else if (type.isChar()) {
            int charLength = type.getColumnSize();
            if (charLength <= HiveChar.MAX_CHAR_LENGTH) {
                return getCharTypeInfo(charLength);
            }
            throw new StarRocksConnectorException("Unsupported Hive type: %s. Supported CHAR types: CHAR(<=%d).",
                    type, HiveChar.MAX_CHAR_LENGTH);
        } else if (type.isVarchar()) {
            if (type.getColumnSize() == -1 || type.getColumnSize() > HiveVarchar.MAX_VARCHAR_LENGTH) {
                return stringTypeInfo;
            }
            return getVarcharTypeInfo(type.getColumnSize());
        } else if (type.isArrayType()) {
            TypeInfo itemType = toTypeInfo(((ArrayType) type).getItemType());
            return getListTypeInfo(itemType);
        } else if (type.isMapType()) {
            TypeInfo keyType = toTypeInfo(((MapType) type).getKeyType());
            TypeInfo valueType = toTypeInfo(((MapType) type).getValueType());
            return getMapTypeInfo(keyType, valueType);
        } else if (type.isStructType()) {
            StructType structType = (StructType) type;
            ImmutableList.Builder<String> fieldNames = ImmutableList.builder();
            for (StructField structField : structType.getFields()) {
                fieldNames.add(structField.getName());
            }
            return getStructTypeInfo(
                    fieldNames.build(),
                    structType.getFields().stream()
                            .map(subType -> toTypeInfo(subType.getType()))
                            .collect(Collectors.toList()));
        }

        throw new StarRocksConnectorException("Unsupported Hive type: %s", type);
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
                return ScalarType.createDefaultCatalogString();
            case ARRAY:
                Type type = new ArrayType(fromHudiType(avroSchema.getElementType()));
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
                break;
            case MAP:
                Schema value = avroSchema.getValueType();
                Type valueType = fromHudiType(value);
                if (valueType.isUnknown()) {
                    isConvertedFailed = true;
                    break;
                }

                if (!isConvertedFailed) {
                    // Hudi map's key must be string
                    return new MapType(ScalarType.createDefaultCatalogString(), valueType);
                }
                break;
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
            case BOOLEAN:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case BYTE:
            case TINYINT:
                primitiveType = PrimitiveType.TINYINT;
                break;
            case SMALLINT:
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case INTEGER:
                primitiveType = PrimitiveType.INT;
                break;
            case LONG:
                primitiveType = PrimitiveType.BIGINT;
                break;
            case FLOAT:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            case TIMESTAMP:
                primitiveType = PrimitiveType.DATETIME;
                break;
            case STRING:
                return ScalarType.createDefaultCatalogString();
            case DECIMAL:
                int precision = ((io.delta.standalone.types.DecimalType) dataType).getPrecision();
                int scale = ((io.delta.standalone.types.DecimalType) dataType).getScale();
                return ScalarType.createUnifiedDecimalType(precision, scale);
            case NULL:
                primitiveType = PrimitiveType.NULL_TYPE;
                break;
            case BINARY:
                primitiveType = PrimitiveType.VARBINARY;
                break;
            case ARRAY:
                return convertToArrayTypeForDeltaLake((io.delta.standalone.types.ArrayType) dataType);
            case MAP:
                return convertToMapTypeForDeltaLake((io.delta.standalone.types.MapType) dataType);
            case STRUCT:
                return convertToStructTypeForDeltaLake(((io.delta.standalone.types.StructType) dataType));
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
        }
        return ScalarType.createType(primitiveType);
    }

    public static Type fromPaimonType(org.apache.paimon.types.DataType type) {
        return type.accept(PaimonToHiveTypeVisitor.INSTANCE);
    }

    private static class PaimonToHiveTypeVisitor extends DataTypeDefaultVisitor<Type> {

        private static final PaimonToHiveTypeVisitor INSTANCE = new PaimonToHiveTypeVisitor();

        public Type visit(BinaryType binaryType) {
            return ScalarType.createType(PrimitiveType.VARBINARY);
        }

        public Type visit(CharType charType) {
            return ScalarType.createCharType(charType.getLength());
        }

        public Type visit(VarCharType varCharType) {
            return ScalarType.createDefaultCatalogString();
        }

        public Type visit(BooleanType booleanType) {
            return ScalarType.createType(PrimitiveType.BOOLEAN);
        }

        public Type visit(DecimalType decimalType) {
            return ScalarType.createUnifiedDecimalType(decimalType.getPrecision(), decimalType.getScale());
        }

        public Type visit(TinyIntType tinyIntType) {
            return ScalarType.createType(PrimitiveType.TINYINT);
        }

        public Type visit(SmallIntType smallIntType) {
            return ScalarType.createType(PrimitiveType.SMALLINT);
        }

        public Type visit(IntType intType) {
            return ScalarType.createType(PrimitiveType.INT);
        }

        public Type visit(BigIntType bigIntType) {
            return ScalarType.createType(PrimitiveType.BIGINT);
        }

        public Type visit(FloatType floatType) {
            return ScalarType.createType(PrimitiveType.FLOAT);
        }

        public Type visit(DoubleType doubleType) {
            return ScalarType.createType(PrimitiveType.DOUBLE);
        }

        public Type visit(DateType dateType) {
            return ScalarType.createType(PrimitiveType.DATE);
        }

        public Type visit(TimestampType timestampType) {
            return ScalarType.createType(PrimitiveType.DATETIME);
        }

        public Type visit(org.apache.paimon.types.ArrayType arrayType) {
            return new ArrayType(fromPaimonType(arrayType.getElementType()));
        }

        public Type visit(org.apache.paimon.types.MapType mapType) {
            return new MapType(fromPaimonType(mapType.getKeyType()), fromPaimonType(mapType.getValueType()));
        }

        public Type visit(RowType rowType) {
            List<DataField> fields = rowType.getFields();
            ArrayList<StructField> structFields = new ArrayList<>(fields.size());
            for (DataField field : fields) {
                String fieldName = field.name();
                Type fieldType = fromPaimonType(field.type());
                structFields.add(new StructField(fieldName, fieldType));
            }
            return new StructType(structFields);
        }

        @Override
        protected Type defaultMethod(org.apache.paimon.types.DataType dataType) {
            return ScalarType.createType(PrimitiveType.UNKNOWN_TYPE);
        }
    }

    public static Type fromIcebergType(org.apache.iceberg.types.Type icebergType) {
        if (icebergType == null) {
            return Type.NULL;
        }

        PrimitiveType primitiveType;

        switch (icebergType.typeId()) {
            case BOOLEAN:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case INTEGER:
                primitiveType = PrimitiveType.INT;
                break;
            case LONG:
                primitiveType = PrimitiveType.BIGINT;
                break;
            case FLOAT:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            case TIMESTAMP:
                primitiveType = PrimitiveType.DATETIME;
                break;
            case STRING:
            case UUID:
                return ScalarType.createDefaultCatalogString();
            case DECIMAL:
                int precision = ((Types.DecimalType) icebergType).precision();
                int scale = ((Types.DecimalType) icebergType).scale();
                return ScalarType.createUnifiedDecimalType(precision, scale);
            case LIST:
                Type type = convertToArrayTypeForIceberg(icebergType);
                if (type.isArrayType()) {
                    return type;
                } else {
                    return Type.UNKNOWN_TYPE;
                }
            case MAP:
                Type mapType = convertToMapTypeForIceberg(icebergType);
                if (mapType.isMapType()) {
                    return mapType;
                } else {
                    return Type.UNKNOWN_TYPE;
                }
            case STRUCT:
                List<Types.NestedField> fields = icebergType.asStructType().fields();
                Preconditions.checkArgument(fields.size() > 0);
                ArrayList<StructField> structFields = new ArrayList<>(fields.size());
                for (Types.NestedField field : fields) {
                    String fieldName = field.name();
                    Type fieldType = fromIcebergType(field.type());
                    if (fieldType.isUnknown()) {
                        return Type.UNKNOWN_TYPE;
                    }
                    structFields.add(new StructField(fieldName, fieldType));
                }
                return new StructType(structFields);
            case BINARY:
                return Type.VARBINARY;
            case TIME:
            case FIXED:
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
        }
        return ScalarType.createType(primitiveType);
    }

    private static ArrayType convertToArrayTypeForIceberg(org.apache.iceberg.types.Type icebergType) {
        return new ArrayType(fromIcebergType(icebergType.asNestedType().asListType().elementType()));
    }

    private static Type convertToMapTypeForIceberg(org.apache.iceberg.types.Type icebergType) {
        Type keyType = fromIcebergType(icebergType.asMapType().keyType());
        // iceberg support complex type as key type, but sr is not supported now
        if (keyType.isComplexType() || keyType.isUnknown()) {
            return Type.UNKNOWN_TYPE;
        }
        Type valueType = fromIcebergType(icebergType.asMapType().valueType());
        if (valueType.isUnknown()) {
            return Type.UNKNOWN_TYPE;
        }
        return new MapType(keyType, valueType);
    }

    private static Type convertToArrayTypeForDeltaLake(io.delta.standalone.types.ArrayType arrayType) {
        Type itemType = fromDeltaLakeType(arrayType.getElementType());
        if (itemType.isUnknown()) {
            return Type.UNKNOWN_TYPE;
        }
        return new ArrayType(itemType);
    }

    private static Type convertToMapTypeForDeltaLake(io.delta.standalone.types.MapType mapType) {
        Type keyType = fromDeltaLakeType(mapType.getKeyType());
        // do not support complex type as key in map type
        if (keyType.isComplexType() || keyType.isUnknown()) {
            return Type.UNKNOWN_TYPE;
        }
        Type valueType = fromDeltaLakeType(mapType.getValueType());
        if (valueType.isUnknown()) {
            return Type.UNKNOWN_TYPE;
        }
        return new MapType(keyType, valueType);
    }

    private static Type convertToStructTypeForDeltaLake(io.delta.standalone.types.StructType structType) {
        io.delta.standalone.types.StructField[] fields = structType.getFields();
        Preconditions.checkArgument(fields.length > 0);
        ArrayList<StructField> structFields = new ArrayList<>(fields.length);
        for (io.delta.standalone.types.StructField field : fields) {
            String fieldName = field.getName();
            Type fieldType = fromDeltaLakeType(field.getDataType());
            if (fieldType.isUnknown()) {
                return Type.UNKNOWN_TYPE;
            }
            structFields.add(new StructField(fieldName, fieldType));
        }
        return new StructType(structFields);
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
        if (HIVE_UNSUPPORTED_TYPES.stream().anyMatch(typeStr.toUpperCase()::contains)) {
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

    public static boolean validateColumnType(Type type, Type otherType) {
        if (type == null || otherType == null) {
            return false;
        }

        if (type == Type.UNKNOWN_TYPE || otherType == Type.UNKNOWN_TYPE) {
            return false;
        }

        if (type.isArrayType()) {
            if (otherType.isArrayType()) {
                return validateColumnType(((ArrayType) type).getItemType(), ((ArrayType) otherType).getItemType());
            } else {
                return false;
            }
        }

        if (type.isMapType()) {
            if (otherType.isMapType()) {
                return validateColumnType(((MapType) type).getKeyType(), ((MapType) otherType).getKeyType()) &&
                        validateColumnType(((MapType) type).getValueType(), ((MapType) otherType).getValueType());
            } else {
                return false;
            }
        }

        if (type.isStructType()) {
            if (otherType.isStructType()) {
                StructType structType = (StructType) type;
                StructType otherStructType = (StructType) otherType;
                for (int i = 0; i < structType.getFields().size(); i++) {
                    if (!validateColumnType(
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

