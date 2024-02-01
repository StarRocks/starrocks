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

package com.starrocks.paimon.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

public class PaimonColumnValue implements ColumnValue {
    private final Object fieldData;
    private final DataType dataType;
    private final String timeZone;
    public PaimonColumnValue(Object fieldData, DataType dataType, String timeZone) {
        this.fieldData = fieldData;
        this.dataType = dataType;
        this.timeZone = timeZone;
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
        return (byte[]) fieldData;
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        InternalArray array = (InternalArray) fieldData;
        toPaimonColumnValue(values, array, ((ArrayType) dataType).getElementType());
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        InternalMap map = (InternalMap) fieldData;
        DataType keyType;
        DataType valueType;
        if (dataType instanceof MapType) {
            keyType = ((MapType) dataType).getKeyType();
            valueType = ((MapType) dataType).getValueType();
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }

        InternalArray keyArray = map.keyArray();
        toPaimonColumnValue(keys, keyArray, keyType);

        InternalArray valueArray = map.valueArray();
        toPaimonColumnValue(values, valueArray, valueType);
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        ColumnarRow array = (ColumnarRow) fieldData;
        List<DataField> fields = ((RowType) dataType).getFields();
        for (int i = 0; i < structFieldIndex.size(); i++) {
            Integer idx = structFieldIndex.get(i);
            PaimonColumnValue cv = null;
            if (idx != null) {
                DataField dataField = fields.get(idx);
                Object o = InternalRowUtils.get(array, idx, dataField.type());
                if (o != null) {
                    cv = new PaimonColumnValue(o, dataField.type(), timeZone);
                }
            }
            values.add(cv);
        }
    }

    @Override
    public byte getByte() {
        return (byte) fieldData;
    }

    public BigDecimal getDecimal() {
        return ((Decimal) fieldData).toBigDecimal();
    }

    private void toPaimonColumnValue(List<ColumnValue> values, InternalArray array, DataType dataType) {
        for (int i = 0; i < array.size(); i++) {
            PaimonColumnValue cv = null;
            Object o = InternalRowUtils.get(array, i, dataType);
            if (o != null) {
                cv = new PaimonColumnValue(o, dataType, timeZone);
            }
            values.add(cv);
        }
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay((int) fieldData);
    }

    @Override
    public LocalDateTime getDateTime(ColumnType.TypeValue type) {
        switch (dataType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((Timestamp) fieldData).toLocalDateTime();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalDateTime.ofInstant(((Timestamp) fieldData).toInstant(), ZoneId.of(timeZone));
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
