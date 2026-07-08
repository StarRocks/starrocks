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

package com.starrocks.fluss.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

public class FlussColumnValue implements ColumnValue {
    private final Object fieldData;
    private final DataType dataType;
    private final String timeZone;

    public FlussColumnValue(Object fieldData, DataType dataType, String timeZone) {
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
        DataType elementType = ((ArrayType) dataType).getElementType();
        InternalArray.ElementGetter elementGetter = InternalArray.createElementGetter(elementType);
        for (int i = 0; i < array.size(); i++) {
            Object element = elementGetter.getElementOrNull(array, i);
            if (element != null) {
                values.add(new FlussColumnValue(element, elementType, timeZone));
            } else {
                values.add(null);
            }
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        InternalMap map = (InternalMap) fieldData;
        DataType keyType = ((MapType) dataType).getKeyType();
        DataType valueType = ((MapType) dataType).getValueType();
        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();
        InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(keyType);
        InternalArray.ElementGetter valueGetter = InternalArray.createElementGetter(valueType);
        for (int i = 0; i < map.size(); i++) {
            Object key = keyGetter.getElementOrNull(keyArray, i);
            Object value = valueGetter.getElementOrNull(valueArray, i);
            if (key != null) {
                keys.add(new FlussColumnValue(key, keyType, timeZone));
            } else {
                keys.add(null);
            }
            if (value != null) {
                values.add(new FlussColumnValue(value, valueType, timeZone));
            } else {
                values.add(null);
            }
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        InternalRow row = (InternalRow) fieldData;
        List<DataField> fields = ((RowType) dataType).getFields();
        for (int i = 0; i < structFieldIndex.size(); i++) {
            Integer idx = structFieldIndex.get(i);
            FlussColumnValue cv = null;
            if (idx != null) {
                DataField dataField = fields.get(idx);
                InternalRow.FieldGetter fieldGetter =
                        InternalRow.createFieldGetter(dataField.getType(), idx);
                Object o = fieldGetter.getFieldOrNull(row);
                if (o != null) {
                    cv = new FlussColumnValue(o, dataField.getType(), timeZone);
                }
            }
            values.add(cv);
        }
    }

    @Override
    public byte getByte() {
        return (byte) fieldData;
    }

    @Override
    public BigDecimal getDecimal() {
        return ((Decimal) fieldData).toBigDecimal();
    }

    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay((int) fieldData);
    }

    @Override
    public LocalDateTime getDateTime(ColumnType.TypeValue type) {
        switch (dataType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampNtz) fieldData).toLocalDateTime();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalDateTime.ofInstant(((TimestampLtz) fieldData).toInstant(), ZoneId.of(timeZone));
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
