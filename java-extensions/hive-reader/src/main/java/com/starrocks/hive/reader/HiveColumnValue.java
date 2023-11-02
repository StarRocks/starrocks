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

package com.starrocks.hive.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class HiveColumnValue implements ColumnValue {
    private final Object fieldData;
    private final ObjectInspector fieldInspector;

    HiveColumnValue(ObjectInspector fieldInspector, Object fieldData) {
        this.fieldInspector = fieldInspector;
        this.fieldData = fieldData;
    }

    private Object inspectObject() {
        return ((PrimitiveObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData);
    }

    @Override
    public boolean getBoolean() {
        return (boolean) inspectObject();
    }

    @Override
    public short getShort() {
        Object value = inspectObject();
        if (value instanceof Integer) {
            return ((Integer) value).shortValue();
        } else {
            return (short) inspectObject();
        }
    }

    @Override
    public int getInt() {
        Object value = inspectObject();
        if (value instanceof Integer) {
            return ((Integer) value).intValue();
        } else {
            return (int) inspectObject();
        }
    }

    @Override
    public float getFloat() {
        return (float) inspectObject();
    }

    @Override
    public long getLong() {
        return (long) inspectObject();
    }

    @Override
    public double getDouble() {
        return (double) inspectObject();
    }

    @Override
    public String getString(ColumnType.TypeValue type) {
        Object o = inspectObject();
        return o.toString();
    }

    @Override
    public String getTimestamp(ColumnType.TypeValue type) {
        return inspectObject().toString();
    }

    @Override
    public byte[] getBytes() {
        return (byte[]) inspectObject();
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        ListObjectInspector inspector = (ListObjectInspector) fieldInspector;
        List<?> items = inspector.getList(fieldData);
        ObjectInspector itemInspector = inspector.getListElementObjectInspector();
        for (Object item : items) {
            HiveColumnValue cv = null;
            if (item != null) {
                cv = new HiveColumnValue(itemInspector, item);
            }
            values.add(cv);
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        MapObjectInspector inspector = (MapObjectInspector) fieldInspector;
        ObjectInspector keyObjectInspector = inspector.getMapKeyObjectInspector();
        ObjectInspector valueObjectInspector = inspector.getMapValueObjectInspector();
        for (Map.Entry kv : inspector.getMap(fieldData).entrySet()) {
            HiveColumnValue cv0 = null;
            HiveColumnValue cv1 = null;
            if (kv.getKey() != null) {
                cv0 = new HiveColumnValue(keyObjectInspector, kv.getKey());
            }
            if (kv.getValue() != null) {
                cv1 = new HiveColumnValue(valueObjectInspector, kv.getValue());
            }
            keys.add(cv0);
            values.add(cv1);
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        StructObjectInspector inspector = (StructObjectInspector) fieldInspector;
        List<? extends StructField> fields = inspector.getAllStructFieldRefs();
        for (int i = 0; i < structFieldIndex.size(); i++) {
            Integer idx = structFieldIndex.get(i);
            HiveColumnValue cv = null;
            if (idx != null) {
                StructField sf = fields.get(idx);
                Object o = inspector.getStructFieldData(fieldData, sf);
                if (o != null) {
                    cv = new HiveColumnValue(sf.getFieldObjectInspector(), o);
                }
            }
            values.add(cv);
        }
    }

    @Override
    public byte getByte() {
        Object value = inspectObject();
        if (value instanceof Integer) {
            return ((Integer) value).byteValue();
        } else {
            return (Byte) inspectObject();
        }
    }

    @Override
    public BigDecimal getDecimal() {
        return ((HiveDecimal) inspectObject()).bigDecimalValue();
    }
}
