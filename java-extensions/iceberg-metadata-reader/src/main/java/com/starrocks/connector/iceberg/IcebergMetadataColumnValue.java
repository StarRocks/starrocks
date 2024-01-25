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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class IcebergMetadataColumnValue implements ColumnValue {
    private final Object fieldData;

    public IcebergMetadataColumnValue(Object fieldData) {
        this.fieldData = fieldData;
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

    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {

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
        return null;
    }
}
