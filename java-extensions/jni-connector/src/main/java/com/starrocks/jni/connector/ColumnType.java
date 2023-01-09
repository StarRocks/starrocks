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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
        DECIMAL,
        ARRAY,
        MAP,
        STRUCT,
    }

    TypeValue typeValue;
    List<String> childNames = new ArrayList<>();
    List<ColumnType> childTypeValues = new ArrayList<>();

    private final static Map<String, TypeValue> primitiveTypeValueMapping = new HashMap<>();
    private final static Map<TypeValue, Integer> primitiveTypeValueSize = new HashMap<>();

    static {
        primitiveTypeValueMapping.put("byte", TypeValue.BYTE);
        primitiveTypeValueMapping.put("bool", TypeValue.BOOLEAN);
        primitiveTypeValueMapping.put("short", TypeValue.SHORT);
        primitiveTypeValueMapping.put("int", TypeValue.INT);
        primitiveTypeValueMapping.put("float", TypeValue.FLOAT);
        primitiveTypeValueMapping.put("bigint", TypeValue.LONG);
        primitiveTypeValueMapping.put("double", TypeValue.DOUBLE);
        primitiveTypeValueMapping.put("string", TypeValue.STRING);
        primitiveTypeValueMapping.put("date", TypeValue.DATE);
        primitiveTypeValueMapping.put("decimal", TypeValue.DECIMAL);

        primitiveTypeValueSize.put(TypeValue.BYTE, 1);
        primitiveTypeValueSize.put(TypeValue.BOOLEAN, 1);
        primitiveTypeValueSize.put(TypeValue.SHORT, 2);
        primitiveTypeValueSize.put(TypeValue.INT, 4);
        primitiveTypeValueSize.put(TypeValue.FLOAT, 4);
        primitiveTypeValueSize.put(TypeValue.LONG, 8);
        primitiveTypeValueSize.put(TypeValue.DOUBLE, 8);
    }

    static final class StringScanner {
        String s;
        int offset;

        StringScanner(String s) {
            this.s = s;
            offset = 0;
        }

        int indexOf(char ch) {
            return s.indexOf(ch, offset);
        }

        String substr(int end) {
            if (end == -1) {
                end = s.length();
            }
            return s.substring(offset, end);
        }

        void moveTo(int p) {
            offset = p;
        }

        void next() {
            offset += 1;
        }

        char peek() {
            return s.charAt(offset);
        }
    }

    private void parseArray(List<ColumnType> childTypeValues, StringScanner scanner) {
        while (scanner.peek() != '>') {
            scanner.next();
            ColumnType x = new ColumnType();
            x.parse(scanner, ',');
            childTypeValues.add(x);
        }
        scanner.next();
    }

    private void parseStruct(List<String> childNames, List<ColumnType> childTypeValues, StringScanner scanner) {
        while (scanner.peek() != '>') {
            scanner.next();
            int p = scanner.indexOf(':');
            String name = scanner.substr(p);
            scanner.moveTo(p + 1);
            childNames.add(name);
            ColumnType x = new ColumnType();
            x.parse(scanner, ',');
            childTypeValues.add(x);
        }
        scanner.next();
    }

    private void parse(StringScanner scanner, char sep) {
        int p = scanner.indexOf(sep);
        String t = scanner.substr(p);
        scanner.moveTo(p);

        // assume there is no blank char in `type`.
        if (t.equals("array")) {
            // array<TYPE>
            typeValue = TypeValue.ARRAY;
            parseArray(childTypeValues, scanner);
        } else if (t.equals("map")) {
            // map<TYPE1,TYPE2>
            parseArray(childTypeValues, scanner);
        } else if (t.equals("struct")) {
            // struct<F1:TYPE1,F2:TYPE2,F3:TYPE3..>
            parseStruct(childNames, childTypeValues, scanner);
        } else {
            // convert decimal(x,y) to decimal
            if (t.startsWith("decimal")) {
                t = "decimal";
            }
            typeValue = primitiveTypeValueMapping.getOrDefault(t, null);
        }
        if (typeValue == null) {
            throw new RuntimeException("Unknown type: " + t);
        }
    }

    public ColumnType() {
    }

    public ColumnType(ColumnType.TypeValue value) {
        typeValue = value;
    }

    public ColumnType(String type) {
        StringScanner scanner = new StringScanner(type);
        parse(scanner, '<');
    }

    public boolean isArray() {
        switch (typeValue) {
            case STRING:
            case DATE:
            case DECIMAL:
            case ARRAY:
                return true;
            default:
                return false;
        }
    }

    public int computeColumnSize() {
        switch (typeValue) {
            case ARRAY: {
                // [ null | offset | data ]
                return 2 + childTypeValues.get(0).computeColumnSize();
            }
            case MAP: {
                // [ null | offset | key | value ]
                return 2 + childTypeValues.get(0).computeColumnSize() + childTypeValues.get(1).computeColumnSize();
            }
            case STRUCT: {
                // [null | c0 | c1 .. ]
                int res = 1;
                for (ColumnType t : childTypeValues) {
                    res += t.computeColumnSize();
                }
                return res;
            }
            case STRING:
            case DECIMAL:
            case DATE:
                // [null | offset | data ]
                return 3;
            default:
                return 2;
        }
    }

    public ColumnType.TypeValue getTypeValue() {
        return typeValue;
    }

    public int getPrimitiveTypeValueSize() {
        return primitiveTypeValueSize.getOrDefault(typeValue, -1);
    }
}
