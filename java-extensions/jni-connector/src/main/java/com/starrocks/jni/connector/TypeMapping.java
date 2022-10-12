// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.jni.connector;

import java.util.HashMap;
import java.util.Map;

public class TypeMapping {
    public static Map<String, OffHeapColumnVector.OffHeapColumnType> hiveTypeMappings = new HashMap<>();
    static {
        hiveTypeMappings.put("byte", OffHeapColumnVector.OffHeapColumnType.BYTE);
        hiveTypeMappings.put("bool", OffHeapColumnVector.OffHeapColumnType.BOOLEAN);
        hiveTypeMappings.put("short", OffHeapColumnVector.OffHeapColumnType.SHORT);
        hiveTypeMappings.put("int", OffHeapColumnVector.OffHeapColumnType.INT);
        hiveTypeMappings.put("float", OffHeapColumnVector.OffHeapColumnType.FLOAT);
        hiveTypeMappings.put("bigint", OffHeapColumnVector.OffHeapColumnType.LONG);
        hiveTypeMappings.put("double", OffHeapColumnVector.OffHeapColumnType.DOUBLE);
        hiveTypeMappings.put("string", OffHeapColumnVector.OffHeapColumnType.STRING);
        hiveTypeMappings.put("date", OffHeapColumnVector.OffHeapColumnType.DATE);
        hiveTypeMappings.put("decimal", OffHeapColumnVector.OffHeapColumnType.DECIMAL);
    }
}
