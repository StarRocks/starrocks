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

package com.starrocks.connector.lance;

import com.starrocks.catalog.Column;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

final class LanceSchemaConverter {
    static final String ARROW_EXTENSION_NAME_KEY = "ARROW:extension:name";
    static final String ARROW_JSON_EXTENSION_NAME = "arrow.json";
    static final String LANCE_JSON_EXTENSION_NAME = "lance.json";

    private LanceSchemaConverter() {
    }

    static List<Column> fromArrowSchema(Schema schema) {
        List<Column> columns = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            columns.add(new Column(field.getName(), fromArrowField(field), true));
        }
        return columns;
    }

    static Type fromArrowField(Field field) {
        if (isJsonField(field)) {
            return JsonType.JSON;
        }

        ArrowType arrowType = field.getType();
        switch (arrowType.getTypeID()) {
            case Int:
                int bitWidth = ((ArrowType.Int) arrowType).getBitWidth();
                if (bitWidth == 8) {
                    return IntegerType.TINYINT;
                } else if (bitWidth == 16) {
                    return IntegerType.SMALLINT;
                } else if (bitWidth == 32) {
                    return IntegerType.INT;
                } else if (bitWidth == 64) {
                    return IntegerType.BIGINT;
                }
                break;
            case FloatingPoint:
                return ((ArrowType.FloatingPoint) arrowType).getPrecision() == FloatingPointPrecision.SINGLE
                        ? FloatType.FLOAT
                        : FloatType.DOUBLE;
            case Bool:
                return BooleanType.BOOLEAN;
            case Utf8:
            case LargeUtf8:
                return VarcharType.VARCHAR;
            case Binary:
            case LargeBinary:
            case FixedSizeBinary:
                return VarbinaryType.VARBINARY;
            case Date:
                return DateType.DATE;
            case Timestamp:
                return DateType.DATETIME;
            case Decimal:
                ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
                return TypeFactory.createUnifiedDecimalType(decimal.getPrecision(), decimal.getScale());
            case List:
            case LargeList:
            case FixedSizeList:
                return new ArrayType(fromArrowField(field.getChildren().get(0)));
            case Map:
                return mapType(field);
            case Struct:
                return structType(field);
            default:
                break;
        }
        throw new StarRocksConnectorException("Unsupported lance/arrow type: %s for column %s",
                arrowType, field.getName());
    }

    private static boolean isJsonField(Field field) {
        String extensionName = field.getMetadata().get(ARROW_EXTENSION_NAME_KEY);
        ArrowType.ArrowTypeID typeId = field.getType().getTypeID();
        if (ARROW_JSON_EXTENSION_NAME.equals(extensionName)) {
            if (typeId == ArrowType.ArrowTypeID.Utf8 || typeId == ArrowType.ArrowTypeID.LargeUtf8) {
                return true;
            }
            throw invalidJsonExtension(field, extensionName);
        }
        if (LANCE_JSON_EXTENSION_NAME.equals(extensionName)) {
            if (typeId == ArrowType.ArrowTypeID.LargeBinary) {
                return true;
            }
            throw invalidJsonExtension(field, extensionName);
        }
        return false;
    }

    private static StarRocksConnectorException invalidJsonExtension(Field field, String extensionName) {
        return new StarRocksConnectorException(
                "Invalid Lance JSON field %s: extension %s is not compatible with Arrow type %s",
                field.getName(), extensionName, field.getType());
    }

    private static Type mapType(Field field) {
        if (field.getChildren().isEmpty() || field.getChildren().get(0).getChildren().size() < 2) {
            throw new StarRocksConnectorException("Invalid lance map field: %s", field.getName());
        }
        List<Field> keyValueFields = field.getChildren().get(0).getChildren();
        return new MapType(fromArrowField(keyValueFields.get(0)), fromArrowField(keyValueFields.get(1)));
    }

    private static Type structType(Field field) {
        ArrayList<StructField> structFields = new ArrayList<>(field.getChildren().size());
        for (Field child : field.getChildren()) {
            structFields.add(new StructField(child.getName(), fromArrowField(child)));
        }
        return new StructType(structFields);
    }
}
