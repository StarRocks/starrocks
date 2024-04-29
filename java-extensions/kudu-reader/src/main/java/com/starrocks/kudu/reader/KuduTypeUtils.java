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

package com.starrocks.kudu.reader;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Type;

/** Convert kudu type to hive string representation. */
public class KuduTypeUtils {
    private KuduTypeUtils() {}

    public static String fromKuduType(ColumnSchema columnSchema) {
        Type type = columnSchema.getType();
        switch (type) {
            case BOOL:
                return "boolean";
            case INT8:
                return "tinyint";
            case INT16:
                return "short";
            case INT32:
                return "int";
            case INT64:
                return "bigint";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case BINARY:
                return "binary";
            case STRING:
            case VARCHAR:
                return "string";
            case DATE:
                return "date";
            case UNIXTIME_MICROS:
                return "timestamp-micros";
            case DECIMAL:
                ColumnTypeAttributes typeAttributes = columnSchema.getTypeAttributes();
                int precision = typeAttributes.getPrecision();
                int scale = typeAttributes.getScale();
                return "decimal(" + precision + "," + scale + ")";
            default:
                throw new IllegalArgumentException("Unsupported Kudu type: " + type.name());
        }
    }
}
