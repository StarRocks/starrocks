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

package com.starrocks.hudi.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.LongWritable;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.starrocks.hudi.reader.HudiScannerUtils.TIMESTAMP_UNIT_MAPPING;

public class HudiColumnValue implements ColumnValue {
    private final Object fieldData;
    private final ObjectInspector fieldInspector;
    private final String timezone;

    HudiColumnValue(ObjectInspector fieldInspector, Object fieldData, String timezone) {
        this.fieldInspector = fieldInspector;
        this.fieldData = fieldData;
        this.timezone = timezone;
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
        return (short) inspectObject();
    }

    @Override
    public int getInt() {
        return (int) inspectObject();
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
            HudiColumnValue cv = null;
            if (item != null) {
                cv = new HudiColumnValue(itemInspector, item, timezone);
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
            HudiColumnValue cv0 = null;
            HudiColumnValue cv1 = null;
            if (kv.getKey() != null) {
                cv0 = new HudiColumnValue(keyObjectInspector, kv.getKey(), timezone);
            }
            if (kv.getValue() != null) {
                cv1 = new HudiColumnValue(valueObjectInspector, kv.getValue(), timezone);
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
            HudiColumnValue cv = null;
            if (idx != null) {
                StructField sf = fields.get(idx);
                Object o = inspector.getStructFieldData(fieldData, sf);
                if (o != null) {
                    cv = new HudiColumnValue(sf.getFieldObjectInspector(), o, timezone);
                }
            }
            values.add(cv);
        }
    }

    @Override
    public byte getByte() {
        throw new UnsupportedOperationException("Hoodie type does not support tinyint");
    }

    @Override
    public BigDecimal getDecimal() {
        return ((HiveDecimal) inspectObject()).bigDecimalValue();
    }


    @Override
    public LocalDate getDate() {
        return LocalDate.ofEpochDay((((DateObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData))
                .toEpochDay());
    }

    @Override
    public LocalDateTime getDateTime(ColumnType.TypeValue type) {
        ZoneId zoneId = ZoneId.systemDefault();
        if (timezone != null) {
            zoneId = ZoneId.of(timezone);
        }
        if (fieldData instanceof Timestamp) {
            return ((Timestamp) fieldData).toLocalDateTime();
        } else if (fieldData instanceof TimestampWritableV2) {
            return LocalDateTime.ofInstant(Instant.ofEpochSecond((((TimestampObjectInspector) fieldInspector)
                    .getPrimitiveJavaObject(fieldData)).toEpochSecond()), zoneId);
        } else {
            Long dateTime = ((LongWritable) fieldData).get();
            TimeUnit timeUnit = TIMESTAMP_UNIT_MAPPING.get(type);
            return HudiScannerUtils.getTimestamp(dateTime, timeUnit, zoneId);
        }
    }
}