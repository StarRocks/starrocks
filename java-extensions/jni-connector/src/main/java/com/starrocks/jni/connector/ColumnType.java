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

    public static final String FIELD_PREFIX = "$";
    public static final String FIELD_0_NAME = FIELD_PREFIX + "0";
    public static final String FIELD_1_NAME = FIELD_PREFIX + "1";

    public enum TypeValue {
        UNKNOWN,
        BYTE,
        BOOLEAN,
        SHORT,
        INT,
        FLOAT,
        LONG,
        DOUBLE,
        STRING,
        BINARY,
        DATE,
        // INT96 timestamp type, hive compatible (hive version < 4.x)
        DATETIME,
        // INT64 timestamp type, TIMESTAMP(isAdjustedToUTC=true, unit=MICROS)
        DATETIME_MICROS,
        // INT64 timestamp type, TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS)
        DATETIME_MILLIS,
        DECIMALV2,
        DECIMAL32,
        DECIMAL64,
        DECIMAL128,
        ARRAY,
        MAP,
        STRUCT,
        TINYINT,
    }

    TypeValue typeValue;
    String name;
    List<String> childNames;
    List<ColumnType> childTypes;
    List<Integer> fieldIndex;
    String rawTypeValue;

    int precision = -1;
    int scale = -1;
    private static final Map<String, TypeValue> PRIMITIVE_TYPE_VALUE_MAPPING = new HashMap<>();
    private static final Map<TypeValue, Integer> PRIMITIVE_TYPE_VALUE_SIZE = new HashMap<>();
    private static final Map<TypeValue, String> PRIMITIVE_TYPE_VALUE_STRING_MAPPING = new HashMap<>();

    private static final int MAX_DECIMAL32_PRECISION = 9;
    private static final int MAX_DECIMAL64_PRECISION = 18;

    static {
        PRIMITIVE_TYPE_VALUE_MAPPING.put("byte", TypeValue.BYTE);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("boolean", TypeValue.BOOLEAN);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("short", TypeValue.SHORT);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("smallint", TypeValue.SHORT);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("int", TypeValue.INT);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("float", TypeValue.FLOAT);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("bigint", TypeValue.LONG);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("double", TypeValue.DOUBLE);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("string", TypeValue.STRING);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("binary", TypeValue.BINARY);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("date", TypeValue.DATE);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("timestamp", TypeValue.DATETIME);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("timestamp-micros", TypeValue.DATETIME_MICROS);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("timestamp-millis", TypeValue.DATETIME_MILLIS);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("decimalv2", TypeValue.DECIMALV2);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("decimal32", TypeValue.DECIMAL32);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("decimal64", TypeValue.DECIMAL64);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("decimal128", TypeValue.DECIMAL128);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("tinyint", TypeValue.TINYINT);

        for (String k : PRIMITIVE_TYPE_VALUE_MAPPING.keySet()) {
            PRIMITIVE_TYPE_VALUE_STRING_MAPPING.put(PRIMITIVE_TYPE_VALUE_MAPPING.get(k), k);
        }
        PRIMITIVE_TYPE_VALUE_STRING_MAPPING.put(TypeValue.STRUCT, "struct");
        PRIMITIVE_TYPE_VALUE_STRING_MAPPING.put(TypeValue.MAP, "map");
        PRIMITIVE_TYPE_VALUE_STRING_MAPPING.put(TypeValue.ARRAY, "array");

        // varchar and char for hive, must put after PRIMITIVE_TYPE_VALUE_STRING_MAPPING is generated
        // so it won't trouble hudi reader to map hudi type to hive type
        PRIMITIVE_TYPE_VALUE_MAPPING.put("varchar", TypeValue.STRING);
        PRIMITIVE_TYPE_VALUE_MAPPING.put("char", TypeValue.STRING);

        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.BYTE, 1);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.BOOLEAN, 1);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.SHORT, 2);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.INT, 4);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.FLOAT, 4);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.LONG, 8);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.DOUBLE, 8);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.TINYINT, 1);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.DECIMALV2, 16);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.DECIMAL32, 4);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.DECIMAL64, 8);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.DECIMAL128, 16);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.DATE, 16);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.DATETIME, 16);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.DATETIME_MICROS, 16);
        PRIMITIVE_TYPE_VALUE_SIZE.put(TypeValue.DATETIME_MILLIS, 16);
    }

    @Override
    public String toString() {
        return typeValue.toString() + "(" + name + ")";
    }

    static final class StringScanner {
        String s;
        int offset;

        StringScanner(String s) {
            this.s = s;
            offset = 0;
        }

        int indexOf(char... args) {
            for (int i = offset; i < s.length(); i++) {
                char c = s.charAt(i);
                for (char ch : args) {
                    if (c == ch) {
                        return i;
                    }
                }
            }
            return s.length();
        }

        String substr(int end) {
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
        int idx = 0;
        while (scanner.peek() != '>') {
            scanner.next(); // '<', or ','
            String name = FIELD_PREFIX + idx;
            childNames.add(name);
            String fieldName = this.name + '.' + name;
            ColumnType x = new ColumnType(fieldName, TypeValue.BYTE);
            idx += 1;
            x.parse(scanner);
            childTypeValues.add(x);
        }
        scanner.next(); // '>'
    }

    private void parseStruct(List<String> childNames, List<ColumnType> childTypeValues, StringScanner scanner) {
        while (scanner.peek() != '>') {
            scanner.next(); // '<' or ','
            int p = scanner.indexOf(':');
            String name = scanner.substr(p);
            childNames.add(name);
            String fieldName = this.name + '.' + name;
            scanner.moveTo(p + 1);
            ColumnType x = new ColumnType(fieldName, TypeValue.BYTE);
            x.parse(scanner);
            childTypeValues.add(x);
        }
        scanner.next(); // '>'
    }

    private void parse(StringScanner scanner) {
        int p = scanner.indexOf('<', ',', '>', '(', ')');
        int end = scanner.indexOf(')') + 1;
        String t = scanner.substr(p);
        if (t.startsWith("decimal")) {
            t = scanner.substr(end);
            scanner.moveTo(end);
        } else if (t.startsWith("char") || t.startsWith("varchar")) {
            // right now this only used in hive scanner
            // for char(xx) and varchar(xx), we only need t to be char or varchar and skip (xx)
            // otherwise struct<c_char:char(30),c_varchar:varchar(200)> will get wrong result
            scanner.moveTo(end);
        } else {
            scanner.moveTo(p);
        }
        // assume there is no blank char in `type`.
        typeValue = null;
        switch (t) {
            case "array": {
                // array<TYPE>
                typeValue = TypeValue.ARRAY;
                childNames = new ArrayList<>();
                childTypes = new ArrayList<>();
                parseArray(childTypes, scanner);
            }
            break;
            case "map": {
                // map<TYPE1,TYPE2>
                typeValue = TypeValue.MAP;
                childNames = new ArrayList<>();
                childTypes = new ArrayList<>();
                parseArray(childTypes, scanner);
            }
            break;
            case "struct": {
                // struct<F1:TYPE1,F2:TYPE2,F3:TYPE3..>
                typeValue = TypeValue.STRUCT;
                childNames = new ArrayList<>();
                childTypes = new ArrayList<>();
                parseStruct(childNames, childTypes, scanner);
            }
            break;
            default: {
                if (t.startsWith("decimal")) {
                    typeValue = parseDecimal(t);
                } else {
                    typeValue = PRIMITIVE_TYPE_VALUE_MAPPING.getOrDefault(t, null);
                }
            }
        }

        if (typeValue == null) {
            throw new RuntimeException("Unsupported type: " + t);
        }
    }

    public ColumnType(String type) {
        this("null", type);
    }

    public ColumnType(String name, ColumnType.TypeValue value) {
        this.name = name;
        typeValue = value;
    }

    public ColumnType(String name, String type) {
        this.name = name;
        StringScanner scanner = new StringScanner(type);
        parse(scanner);
    }

    public boolean isByteStorageType() {
        return typeValue == TypeValue.STRING || typeValue == TypeValue.BINARY;
    }

    public boolean isArray() {
        return typeValue == TypeValue.ARRAY;
    }

    public boolean isUnknown() {
        return typeValue == TypeValue.UNKNOWN;
    }

    public boolean isMap() {
        return typeValue == TypeValue.MAP;
    }

    public boolean isStruct() {
        return typeValue == TypeValue.STRUCT;
    }

    public boolean isMapKeySelected() {
        return childNames.indexOf(FIELD_0_NAME) != -1;
    }

    public boolean isMapValueSelected() {
        return childNames.indexOf(FIELD_1_NAME) != -1;
    }

    public boolean isDecimal() {
        return typeValue == TypeValue.DECIMALV2 || typeValue == TypeValue.DECIMAL32 ||
                typeValue == TypeValue.DECIMAL64 ||
                typeValue == TypeValue.DECIMAL128;
    }

    public int computeColumnSize() {
        switch (typeValue) {
            case UNKNOWN:
                // [0] (indicate this unknown column)
                return 1;
            case ARRAY:
            case MAP:
            case STRUCT: {
                // array & map
                // [ null | offset | ... ]
                // struct
                // [ null | ... ]
                int res = 2;
                if (typeValue == TypeValue.STRUCT) {
                    res = 1;
                }
                for (ColumnType t : childTypes) {
                    res += t.computeColumnSize();
                }
                return res;
            }
            case STRING:
            case BINARY:
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DATE:
            case DATETIME:
            case DATETIME_MICROS:
            case DATETIME_MILLIS:
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
        return PRIMITIVE_TYPE_VALUE_SIZE.getOrDefault(typeValue, -1);
    }

    public String getTypeValueString() {
        return PRIMITIVE_TYPE_VALUE_STRING_MAPPING.get(typeValue);
    }

    public List<String> getChildNames() {
        return childNames;
    }

    public List<ColumnType> getChildTypes() {
        return childTypes;
    }

    public List<Integer> getFieldIndex() {
        return fieldIndex;
    }

    public void pruneOnField(SelectedFields ssf, String name) {
        if (isStruct() || isMap() || isArray()) {
            SelectedFields ssf2 = ssf != null ? ssf.findChildren(name) : null;
            pruneOnSelectedFields(ssf2);
        }
    }

    private void pruneChildren(SelectedFields ssf) {
        for (int i = 0; i < childTypes.size(); i++) {
            ColumnType type = childTypes.get(i);
            String name = childNames.get(i);
            type.pruneOnField(ssf, name);
        }
    }

    public void pruneOnSelectedFields(SelectedFields ssf) {
        // If no spec at all, then select all fields
        if (ssf == null) {
            fieldIndex = new ArrayList<>();
            for (int i = 0; i < childNames.size(); i++) {
                fieldIndex.add(i);
            }
            pruneChildren(null);
            return;
        }

        // make index and prune on this struct.
        Map<String, Integer> index = new HashMap<>();
        for (int i = 0; i < childNames.size(); i++) {
            index.put(childNames.get(i), i);
        }

        List<String> fields = ssf.getFields();
        List<String> names = new ArrayList<>();
        List<ColumnType> types = new ArrayList<>();
        fieldIndex = new ArrayList<>();
        for (String f : fields) {
            Integer i = index.get(f);
            if (i == null) {
                // field name mismatch.
                fieldIndex.add(null);
                types.add(new ColumnType(name + '.' + f, TypeValue.UNKNOWN));
            } else {
                fieldIndex.add(i);
                types.add(childTypes.get(i));
            }
            names.add(f);
        }
        childNames = names;
        childTypes = types;
        pruneChildren(ssf);
    }

    public void buildNestedFieldsSpec(String top, StringBuilder sb) {
        if (isStruct()) {
            for (int i = 0; i < childNames.size(); i++) {
                String name = childNames.get(i);
                ColumnType type = childTypes.get(i);
                String s = top + "." + name;
                type.buildNestedFieldsSpec(s, sb);
            }
        } else {
            sb.append(top);
            sb.append(',');
        }
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public int getScale() {
        return scale;
    }

    public String getRawTypeValue() {
        return rawTypeValue;
    }

    // convert decimal(x,y) to decimal
    private TypeValue parseDecimal(String rawType) {
        String type = rawType;
        int precision = -1;
        int scale = -1;
        int s = type.indexOf('(');
        int e = type.indexOf(')');
        if (s != -1 && e != -1) {
            String[] ps = type.substring(s + 1, e).split(",");
            precision = Integer.parseInt(ps[0].trim());
            scale = Integer.parseInt(ps[1].trim());
            // this logic is the same as FE's ScalarType.createUnifiedDecimalType
            if (precision <= MAX_DECIMAL64_PRECISION) {
                type = "decimal64";
            } else {
                type = "decimal128";
            }
        }
        TypeValue value = PRIMITIVE_TYPE_VALUE_MAPPING.get(type);
        rawTypeValue = rawType;
        setScale(scale);
        return value;
    }
}
