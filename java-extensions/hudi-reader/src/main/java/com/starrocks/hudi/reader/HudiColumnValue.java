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

import com.starrocks.jni.connector.ColumnValue;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class HudiColumnValue implements ColumnValue {
    private Object fieldData;
    private ObjectInspector fieldInspector;
    private boolean isPrimitiveType;

    HudiColumnValue(ObjectInspector fieldInspector, Object fieldData, boolean isPrimitiveType) {
        this.fieldInspector = fieldInspector;
        this.fieldData = fieldData;
        this.isPrimitiveType = isPrimitiveType;
    }

    private Object inspectObject() {
        if (isPrimitiveType) {
            return ((PrimitiveObjectInspector) fieldInspector).getPrimitiveJavaObject(fieldData);
        } else {
            // TODO(yanz):
            return null;
        }
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
    public String getString() {
        return inspectObject().toString();
    }
}
