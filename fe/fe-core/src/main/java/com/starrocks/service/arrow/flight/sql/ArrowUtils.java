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

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class ArrowUtils {
    private ArrowUtils() {
    }

    // Note, this needs to be consistent with BE's `convert_to_arrow_type`.
    public static Field convertToArrowType(Type type, String colName, boolean nullable) {
        if (type.isScalarType()) {
            ScalarType scalarType = (ScalarType) type;
            ArrowType arrowType = convertToScalarArrowType(scalarType);

            PrimitiveType primitiveType = type.getPrimitiveType();
            if (primitiveType == PrimitiveType.HLL || primitiveType == PrimitiveType.BITMAP ||
                    primitiveType == PrimitiveType.PERCENTILE) {
                nullable = true;
            }
            return new Field(colName, new FieldType(nullable, arrowType, null), Collections.emptyList());
        } else if (type.isArrayType()) {
            ArrayType arrayType = (ArrayType) type;
            Field childField = convertToArrowType(arrayType.getItemType(), "item", true);
            return new Field(colName, new FieldType(nullable, new ArrowType.List(), null), Collections.singletonList(childField));
        } else if (type.isMapType()) {
            MapType mapType = (MapType) type;

            // MapType
            //    └── Struct(name=entries, nullable=false)
            //         ├── Field("key",  <key_type>,  nullable=false)
            //         └── Field("value", <value_type>, nullable=true)
            Field keyField = convertToArrowType(mapType.getKeyType(), "key", false);
            Field valueField = convertToArrowType(mapType.getValueType(), "value", true);
            Field structField =
                    new Field("entries", FieldType.notNullable(new ArrowType.Struct()), Arrays.asList(keyField, valueField));

            return new Field(colName,
                    new FieldType(nullable, new ArrowType.Map(false), null),
                    Collections.singletonList(structField));
        } else if (type.isStructType()) {
            StructType structType = (StructType) type;

            List<Field> childFields = structType.getFields().stream()
                    .map(child -> convertToArrowType(child.getType(), child.getName(), true))
                    .toList();
            return new Field(colName, new FieldType(nullable, new ArrowType.Struct(), null), childFields);
        } else {
            throw new UnsupportedOperationException("Unknown type: " + type);
        }
    }

    private static ArrowType convertToScalarArrowType(ScalarType type) {
        switch (type.getPrimitiveType()) {
            case BOOLEAN:
                return new ArrowType.Bool();
            case TINYINT:
                return new ArrowType.Int(8, true);
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case INT:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE:
            case TIME:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case LARGEINT:
                return new ArrowType.Decimal(38, 0, 128);
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case DATETIME:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
            case VARCHAR:
            case CHAR:
            case JSON:
                return new ArrowType.Utf8();
            case VARBINARY:
            case HLL:
            case BITMAP:
            case PERCENTILE:
                // HLL,BITMAP,PERCENTILE are always converted to utf8 with null values, which is the same as MySQL output.
                return new ArrowType.Binary();
            case DECIMALV2:
                return new ArrowType.Decimal(27, 9, 128);
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return new ArrowType.Decimal(type.decimalPrecision(), type.decimalScale(), 128);
            case DECIMAL256:
                return new ArrowType.Decimal(type.decimalPrecision(), type.decimalScale(), 256);
            default:
                throw new UnsupportedOperationException("Unknown scalar type: " + type);
        }
    }
}
