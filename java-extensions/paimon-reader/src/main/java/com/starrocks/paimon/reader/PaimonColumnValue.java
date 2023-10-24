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
import java.util.List;

public class PaimonColumnValue implements ColumnValue {
    private final Object fieldData;
    private final DataType dataType;
    public PaimonColumnValue(Object fieldData, DataType dataType) {
        this.fieldData = fieldData;
        this.dataType = dataType;
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
        if (type == ColumnType.TypeValue.DATE) {
            int epoch = (int) fieldData;
            LocalDate date = LocalDate.ofEpochDay(epoch);
            return PaimonScannerUtils.formatDate(date);
        } else {
            return fieldData.toString();
        }
    }

    @Override
    public String getTimestamp(ColumnType.TypeValue type) {
        if (type == ColumnType.TypeValue.DATETIME_MILLIS) {
            Timestamp ts = (Timestamp) fieldData;
            LocalDateTime dateTime = ts.toLocalDateTime();
            return PaimonScannerUtils.formatDateTime(dateTime);
        } else {
            return fieldData.toString();
        }
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
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
                    cv = new PaimonColumnValue(o, dataField.type());
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
            PaimonColumnValue cv = new PaimonColumnValue(InternalRowUtils.get(array, i, dataType), dataType);
            values.add(cv);
        }
    }
}
