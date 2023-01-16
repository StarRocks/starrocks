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

package com.starrocks.jni.connector;

import java.util.HashMap;
import java.util.Map;

public class TypeMapping {
    public static Map<String, OffHeapColumnVector.OffHeapColumnType> hiveTypeMappings = new HashMap<>();
    static {
        hiveTypeMappings.put("byte", OffHeapColumnVector.OffHeapColumnType.BYTE);
        hiveTypeMappings.put("bool", OffHeapColumnVector.OffHeapColumnType.BOOLEAN);
        hiveTypeMappings.put("boolean", OffHeapColumnVector.OffHeapColumnType.BOOLEAN);
        hiveTypeMappings.put("short", OffHeapColumnVector.OffHeapColumnType.SHORT);
        hiveTypeMappings.put("int", OffHeapColumnVector.OffHeapColumnType.INT);
        hiveTypeMappings.put("float", OffHeapColumnVector.OffHeapColumnType.FLOAT);
        hiveTypeMappings.put("bigint", OffHeapColumnVector.OffHeapColumnType.LONG);
        hiveTypeMappings.put("double", OffHeapColumnVector.OffHeapColumnType.DOUBLE);
        hiveTypeMappings.put("string", OffHeapColumnVector.OffHeapColumnType.STRING);
        hiveTypeMappings.put("binary", OffHeapColumnVector.OffHeapColumnType.BINARY);
        hiveTypeMappings.put("date", OffHeapColumnVector.OffHeapColumnType.DATE);
        hiveTypeMappings.put("timestamp", OffHeapColumnVector.OffHeapColumnType.DATETIME);
        hiveTypeMappings.put("decimal", OffHeapColumnVector.OffHeapColumnType.DECIMAL);
    }
}
