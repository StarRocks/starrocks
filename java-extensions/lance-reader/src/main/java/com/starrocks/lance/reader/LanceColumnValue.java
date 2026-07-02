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

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.arrow.vector.util.Text;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LanceColumnValue implements ColumnValue {
    private final Object fieldData;

    public LanceColumnValue(Object fieldData) {
        this.fieldData = fieldData;
    }

    @Override
    public boolean getBoolean() {
        return (boolean) fieldData;
    }

    @Override
    public short getShort() {
        return ((Number) fieldData).shortValue();
    }

    @Override
    public int getInt() {
        return ((Number) fieldData).intValue();
    }

    @Override
    public float getFloat() {
        return ((Number) fieldData).floatValue();
    }

    @Override
    public long getLong() {
        return ((Number) fieldData).longValue();
    }

    @Override
    public double getDouble() {
        return ((Number) fieldData).doubleValue();
    }

    @Override
    public String getString(ColumnType.TypeValue type) {
        if (fieldData instanceof Text) {
            return fieldData.toString();
        }
        if (fieldData instanceof byte[]) {
            return new String((byte[]) fieldData, StandardCharsets.UTF_8);
        }
        return String.valueOf(fieldData);
    }

    @Override
    public byte[] getBytes() {
        if (fieldData instanceof byte[]) {
            return (byte[]) fieldData;
        }
        if (fieldData instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) fieldData).slice();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
        if (fieldData instanceof Text) {
            return fieldData.toString().getBytes(StandardCharsets.UTF_8);
        }
        return String.valueOf(fieldData).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        if (!(fieldData instanceof List<?>)) {
            return;
        }
        for (Object value : (List<?>) fieldData) {
            values.add(value == null ? null : new LanceColumnValue(value));
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        if (fieldData instanceof Map<?, ?>) {
            ((Map<?, ?>) fieldData).forEach((key, value) -> {
                keys.add(key == null ? null : new LanceColumnValue(key));
                values.add(value == null ? null : new LanceColumnValue(value));
            });
            return;
        }
        if (fieldData instanceof List<?>) {
            for (Object entry : (List<?>) fieldData) {
                if (entry instanceof Map<?, ?>) {
                    List<?> pair = new ArrayList<>(((Map<?, ?>) entry).values());
                    keys.add(pair.size() > 0 && pair.get(0) != null ? new LanceColumnValue(pair.get(0)) : null);
                    values.add(pair.size() > 1 && pair.get(1) != null ? new LanceColumnValue(pair.get(1)) : null);
                }
            }
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        List<?> fields;
        if (fieldData instanceof Map<?, ?>) {
            fields = new ArrayList<>(((Map<?, ?>) fieldData).values());
        } else if (fieldData instanceof List<?>) {
            fields = (List<?>) fieldData;
        } else {
            return;
        }

        for (Integer index : structFieldIndex) {
            if (index == null || index < 0 || index >= fields.size() || fields.get(index) == null) {
                values.add(null);
            } else {
                values.add(new LanceColumnValue(fields.get(index)));
            }
        }
    }

    @Override
    public byte getByte() {
        return ((Number) fieldData).byteValue();
    }

    @Override
    public BigDecimal getDecimal() {
        if (fieldData instanceof BigDecimal) {
            return (BigDecimal) fieldData;
        }
        return new BigDecimal(String.valueOf(fieldData));
    }

    @Override
    public LocalDate getDate() {
        if (fieldData instanceof LocalDate) {
            return (LocalDate) fieldData;
        }
        return LocalDate.ofEpochDay(((Number) fieldData).longValue());
    }

    @Override
    public LocalDateTime getDateTime(ColumnType.TypeValue type) {
        if (fieldData instanceof LocalDateTime) {
            return (LocalDateTime) fieldData;
        }
        if (fieldData instanceof Instant) {
            return LocalDateTime.ofInstant((Instant) fieldData, ZoneOffset.UTC);
        }
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) fieldData).longValue()), ZoneOffset.UTC);
    }
}
