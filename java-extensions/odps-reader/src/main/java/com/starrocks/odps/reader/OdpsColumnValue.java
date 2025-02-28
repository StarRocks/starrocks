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

package com.starrocks.odps.reader;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class OdpsColumnValue implements ColumnValue {
    private final Object fieldData;
    private final TypeInfo dataType;

    private final String timezone;

    public OdpsColumnValue(Object fieldData, TypeInfo dataType, String timeZone) {
        this.fieldData = fieldData;
        this.dataType = dataType;
        this.timezone = timeZone;
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
        return fieldData.toString();
    }

    @Override
    public byte[] getBytes() {
        Binary b = (Binary) fieldData;
        return b.data();
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        List data = (List) fieldData;
        ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) dataType;
        for (int i = 0; i < data.size(); i++) {
            values.add(new OdpsColumnValue(data.get(i), arrayTypeInfo.getElementTypeInfo(), timezone));
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) dataType;
        Map data = (Map) fieldData;
        data.forEach((key, value) -> {
            keys.add(key == null ? null : new OdpsColumnValue(key, mapTypeInfo.getKeyTypeInfo(), timezone));
            values.add(value == null ? null : new OdpsColumnValue(value, mapTypeInfo.getValueTypeInfo(), timezone));
        });
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        StructTypeInfo structTypeInfo = (StructTypeInfo) dataType;
        List<TypeInfo> fieldTypeInfos = structTypeInfo.getFieldTypeInfos();
        Struct data = (Struct) fieldData;
        for (int i = 0; i < data.getFieldCount(); i++) {
            values.add(new OdpsColumnValue(data.getFieldValue(i), fieldTypeInfos.get(i), timezone));
        }
    }

    @Override
    public byte getByte() {
        return (byte) fieldData;
    }

    @Override
    public BigDecimal getDecimal() {
        return (BigDecimal) fieldData;
    }

    @Override
    public LocalDate getDate() {
        return (LocalDate) fieldData;
    }

    @Override
    public LocalDateTime getDateTime(ColumnType.TypeValue type) {
        Instant ts = (Instant) fieldData;
        return LocalDateTime.ofInstant(ts, ZoneId.systemDefault());
    }
}
