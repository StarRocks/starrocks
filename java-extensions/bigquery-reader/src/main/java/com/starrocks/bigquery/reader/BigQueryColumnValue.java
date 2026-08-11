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

package com.starrocks.bigquery.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Wraps a value extracted from a BigQuery Arrow vector and adapts it to the
 * {@link ColumnValue} interface consumed by the off-heap table writer.
 */
public class BigQueryColumnValue implements ColumnValue {
    private final Object value;

    public BigQueryColumnValue(Object value) {
        this.value = value;
    }

    @Override
    public boolean getBoolean() {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }

    @Override
    public byte getByte() {
        return ((Number) value).byteValue();
    }

    @Override
    public short getShort() {
        return ((Number) value).shortValue();
    }

    @Override
    public int getInt() {
        return ((Number) value).intValue();
    }

    @Override
    public float getFloat() {
        return ((Number) value).floatValue();
    }

    @Override
    public long getLong() {
        return ((Number) value).longValue();
    }

    @Override
    public double getDouble() {
        return ((Number) value).doubleValue();
    }

    @Override
    public BigDecimal getDecimal() {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        return new BigDecimal(value.toString());
    }

    @Override
    public String getString(ColumnType.TypeValue type) {
        return value != null ? value.toString() : null;
    }

    @Override
    public byte[] getBytes() {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public LocalDate getDate() {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }
        if (value instanceof Long) {
            // Arrow DATE32 is days since epoch
            return LocalDate.ofEpochDay((Long) value);
        }
        return LocalDate.parse(value.toString());
    }

    @Override
    public LocalDateTime getDateTime(ColumnType.TypeValue type) {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof Long) {
            // Arrow TIMESTAMP_MICRO is microseconds since epoch (UTC)
            long micros = (Long) value;
            long seconds = micros / 1_000_000L;
            int nanos = (int) ((micros % 1_000_000L) * 1_000L);
            return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), ZoneOffset.UTC);
        }
        if (value instanceof Instant) {
            return LocalDateTime.ofInstant((Instant) value, ZoneOffset.UTC);
        }
        return LocalDateTime.parse(value.toString());
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        if (value instanceof List) {
            for (Object element : (List<?>) value) {
                values.add(element != null ? new BigQueryColumnValue(element) : null);
            }
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        // BigQuery does not natively expose MAP types via the Storage Read API Arrow schema;
        // this is a placeholder for completeness.
        throw new UnsupportedOperationException(
                "BigQuery MAP type unpacking is not supported in Phase 1");
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        if (value instanceof List) {
            List<?> fields = (List<?>) value;
            for (int idx : structFieldIndex) {
                Object fieldVal = (idx < fields.size()) ? fields.get(idx) : null;
                values.add(fieldVal != null ? new BigQueryColumnValue(fieldVal) : null);
            }
        }
    }
}
