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

package com.starrocks.lance.reader;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps Arrow types from a Lance dataset schema to Hive-style type strings
 * used by {@link com.starrocks.jni.connector.ColumnType}.
 */
public class LanceTypeUtils {
    private LanceTypeUtils() {
    }

    /**
     * Convert an Arrow ArrowType to a Hive-style type string.
     */
    public static String fromArrowType(ArrowType arrowType) {
        if (arrowType instanceof ArrowType.Bool) {
            return "boolean";
        } else if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            switch (intType.getBitWidth()) {
                case 8:
                    return "tinyint";
                case 16:
                    return "short";
                case 32:
                    return "int";
                case 64:
                    return "bigint";
                default:
                    return "string";
            }
        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
            if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
                return "float";
            } else {
                return "double";
            }
        } else if (arrowType instanceof ArrowType.Utf8) {
            return "string";
        } else if (arrowType instanceof ArrowType.Binary) {
            return "binary";
        } else if (arrowType instanceof ArrowType.Date) {
            return "date";
        } else if (arrowType instanceof ArrowType.Timestamp) {
            return "timestamp-micros";
        } else if (arrowType instanceof ArrowType.Decimal) {
            ArrowType.Decimal decType = (ArrowType.Decimal) arrowType;
            return "decimal(" + decType.getPrecision() + "," + decType.getScale() + ")";
        } else if (arrowType instanceof ArrowType.List) {
            return "array";
        } else if (arrowType instanceof ArrowType.Struct) {
            return "struct";
        } else {
            // Fallback for unsupported types
            return "string";
        }
    }

    /**
     * Build a mapping from field name to Hive-style type string for a given Arrow schema.
     */
    public static Map<String, String> buildTypeMapping(Schema schema) {
        Map<String, String> typeMap = new HashMap<>();
        for (Field field : schema.getFields()) {
            typeMap.put(field.getName(), fromArrowType(field.getType()));
        }
        return typeMap;
    }
}
