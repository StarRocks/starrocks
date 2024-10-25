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

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnType.TypeValue;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.kudu.client.RowResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static com.starrocks.kudu.reader.KuduScannerUtils.ZONE_UTC;

public class KuduColumnValue implements ColumnValue {
    private final RowResult row;
    private final int index;
    public KuduColumnValue(RowResult row, int index) {
        this.row = row;
        this.index = index;
    }
    @Override
    public boolean getBoolean() {
        return row.getBoolean(index);
    }

    @Override
    public short getShort() {
        return row.getShort(index);
    }

    @Override
    public int getInt() {
        return row.getInt(index);
    }

    @Override
    public float getFloat() {
        return row.getFloat(index);
    }

    @Override
    public long getLong() {
        return row.getLong(index);
    }

    @Override
    public double getDouble() {
        return row.getDouble(index);
    }

    @Override
    public String getString(ColumnType.TypeValue type) {
        if (type == ColumnType.TypeValue.DATE) {
            LocalDate date = row.getDate(index).toLocalDate();
            return KuduScannerUtils.formatDate(date);
        } else {
            return row.getString(index);
        }
    }

    @Override
    public LocalDate getDate() {
        return row.getDate(index).toLocalDate();
    }

    @Override
    public LocalDateTime getDateTime(TypeValue type) {
        long millis = row.getLong(index) / 1000;
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZONE_UTC);
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        throw new UnsupportedOperationException("Not supported array type.");
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        throw new UnsupportedOperationException("Not supported map type.");
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        throw new UnsupportedOperationException("Not supported struct type.");
    }

    @Override
    public byte getByte() {
        return row.getByte(index);
    }

    public BigDecimal getDecimal() {
        return row.getDecimal(index);
    }
}
