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

public class ColumnType {

    public enum TypeValue {
        BYTE,
        BOOLEAN,
        SHORT,
        INT,
        FLOAT,
        LONG,
        DOUBLE,
        STRING,
        DATE,
        DECIMAL
    }

    TypeValue typeValue;

    private static Map<String, TypeValue> primitiveTypeMapping = new HashMap<>();

    static {
        primitiveTypeMapping.put("byte", TypeValue.BYTE);
        primitiveTypeMapping.put("bool", TypeValue.BOOLEAN);
        primitiveTypeMapping.put("short", TypeValue.SHORT);
        primitiveTypeMapping.put("int", TypeValue.INT);
        primitiveTypeMapping.put("float", TypeValue.FLOAT);
        primitiveTypeMapping.put("bigint", TypeValue.LONG);
        primitiveTypeMapping.put("double", TypeValue.DOUBLE);
        primitiveTypeMapping.put("string", TypeValue.STRING);
        primitiveTypeMapping.put("date", TypeValue.DATE);
        primitiveTypeMapping.put("decimal", TypeValue.DECIMAL);
    }

    public ColumnType(String type) {
        // TODO(yanz):
        //        // convert decimal(x,y) to decimal
        //        if (type.startsWith("decimal")) {
        //            type = "decimal";
        //        }
    }
}
