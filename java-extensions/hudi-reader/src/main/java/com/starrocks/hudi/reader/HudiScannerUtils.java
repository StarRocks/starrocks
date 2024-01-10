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

package com.starrocks.hudi.reader;

import com.starrocks.jni.connector.ColumnType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HudiScannerUtils {
    public static final Map<String, String> HIVE_TYPE_MAPPING = new HashMap<>();
    public static Map<ColumnType.TypeValue, TimeUnit> TIMESTAMP_UNIT_MAPPING = new HashMap<>();

    static {
        HIVE_TYPE_MAPPING.put("timestamp-micros", "timestamp");
        HIVE_TYPE_MAPPING.put("timestamp-millis", "timestamp");

        TIMESTAMP_UNIT_MAPPING.put(ColumnType.TypeValue.DATETIME_MICROS, TimeUnit.MICROSECONDS);
        TIMESTAMP_UNIT_MAPPING.put(ColumnType.TypeValue.DATETIME_MILLIS, TimeUnit.MILLISECONDS);
        // https://spark.apache.org/docs/3.1.3/api/java/org/apache/spark/sql/types/TimestampType.html
        TIMESTAMP_UNIT_MAPPING.put(ColumnType.TypeValue.DATETIME, TimeUnit.MICROSECONDS);
    }

    private static final long MILLI = 1000;
    private static final long MICRO = 1_000_000;
    private static final long NANO = 1_000_000_000;

    public static LocalDateTime getTimestamp(long value, TimeUnit timeUnit, String timeZone) {
        long seconds = 0L;
        long nanoseconds = 0L;

        switch (timeUnit) {
            case SECONDS:
                seconds = value;
                nanoseconds = 0;
                break;

            case MILLISECONDS:
                seconds = value / MILLI;
                nanoseconds = (value % MILLI) * MICRO;
                break;

            case MICROSECONDS:
                seconds = value / MICRO;
                nanoseconds = (value % MICRO) * MILLI;
                break;

            case NANOSECONDS:
                seconds = value / NANO;
                nanoseconds = (value % NANO);
                break;
            default:
                break;
        }
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanoseconds), ZoneId.of(timeZone));
    }

    public static boolean isMaybeInt64Timestamp(ColumnType.TypeValue type) {
        return (type == ColumnType.TypeValue.DATETIME_MICROS
                || type == ColumnType.TypeValue.DATETIME_MILLIS
                || type == ColumnType.TypeValue.DATETIME);
    }

    public static String mapColumnTypeToHiveType(ColumnType type) {
        ColumnType.TypeValue typeValue = type.getTypeValue();
        StringBuilder sb = new StringBuilder();
        if (type.isStruct()) {
            List<String> childNames = type.getChildNames();
            List<ColumnType> childTypes = type.getChildTypes();
            sb.append("struct<");
            for (int i = 0; i < childNames.size(); i++) {
                sb.append(childNames.get(i));
                sb.append(":");
                sb.append(mapColumnTypeToHiveType(childTypes.get(i)));
                sb.append(",");
            }
            if (childNames.size() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append(">");
            return sb.toString();
        } else if (type.isMap() || type.isArray()) {
            List<ColumnType> childTypes = type.getChildTypes();
            sb.append(type.getTypeValueString());
            sb.append("<");
            for (int i = 0; i < childTypes.size(); i++) {
                sb.append(mapColumnTypeToHiveType(childTypes.get(i)));
                sb.append(",");
            }
            if (childTypes.size() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append(">");
            return sb.toString();
        } else if (type.isDecimal()) {
            return type.getRawTypeValue();
        } else {
            String value = type.getTypeValueString();
            if (value == null) {
                throw new IllegalArgumentException("Invalid type: " + type.toString());
            }
            return HIVE_TYPE_MAPPING.getOrDefault(value, value);
        }
    }
}