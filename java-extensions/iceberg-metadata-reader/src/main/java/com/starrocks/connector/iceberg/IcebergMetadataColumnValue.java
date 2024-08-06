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

package com.starrocks.connector.iceberg;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.iceberg.data.GenericRecord;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class IcebergMetadataColumnValue implements ColumnValue {
    private static final String DEFAULT_TIME_ZONE = "Asia/Shanghai";

    private final Object fieldData;
    private final String timezone;

    public IcebergMetadataColumnValue(Object fieldData) {
        this(fieldData, DEFAULT_TIME_ZONE);
    }

    public IcebergMetadataColumnValue(Object fieldData, String timezone) {
        this.fieldData = fieldData;
        this.timezone = timezone;
    }

    @Override
    public boolean getBoolean() {
        return (boolean) fieldData;
    }

    @Override
    public short getShort() {
        return (short) fieldData;
    }

    @Override
    public int getInt() {
        return (int) fieldData;
    }

    @Override
    public float getFloat() {
        return (float) fieldData;
    }

    @Override
    public long getLong() {
        return (long) fieldData;
    }

    @Override
    public double getDouble() {
        return (double) fieldData;
    }

    @Override
    public String getString(ColumnType.TypeValue type) {
        return (String) fieldData;
    }


    @Override
    public byte[] getBytes() {
        return (byte[]) fieldData;
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        List<?> items = (List<?>) fieldData;
        for (Object item : items) {
            IcebergMetadataColumnValue cv = null;
            if (item != null) {
                cv = new IcebergMetadataColumnValue(item);
            }
            values.add(cv);
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        Map data = (Map) fieldData;
        data.forEach((key, value) -> {
            keys.add(new IcebergMetadataColumnValue(key, timezone));
            values.add(new IcebergMetadataColumnValue(value, timezone));
        });
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        GenericRecord record = (GenericRecord) fieldData;
        for (int i = 0; i < structFieldIndex.size(); i++) {
            Integer idx = structFieldIndex.get(i);
            IcebergMetadataColumnValue value = new IcebergMetadataColumnValue(record.get(idx));
            values.add(value);
        }
    }

    @Override
    public byte getByte() {
        return 0;
    }

    @Override
    public BigDecimal getDecimal() {
        return null;
    }

    @Override
    public LocalDate getDate() {
        return null;
    }

    @Override
    public LocalDateTime getDateTime(ColumnType.TypeValue type) {
        if (type == ColumnType.TypeValue.DATETIME) {
            Instant instant = Instant.ofEpochMilli((long) fieldData);
            return LocalDateTime.ofInstant(instant, ZoneId.of(timezone));
        } else {
            return null;
        }
    }
}
