// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.format.util;


import java.util.Arrays;
import java.util.Optional;

public enum DataType {

    BOOLEAN(1, "BOOLEAN"),
    TINYINT(2, "TINYINT"),
    SMALLINT(3, "SMALLINT"),
    INT(4, "INT"),
    BIGINT(5, "BIGINT"),
    LARGEINT(6, "LARGEINT"),
    FLOAT(7, "FLOAT"),
    DOUBLE(8, "DOUBLE"),
    DECIMAL(9, "DECIMAL"),
    DECIMAL32(10, "DECIMAL32"),
    DECIMAL64(11, "DECIMAL64"),
    DECIMAL128(12, "DECIMAL128"),
    DATE(13, "DATE"),
    DATETIME(14, "DATETIME"),
    CHAR(15, "CHAR"),
    VARCHAR(16, "VARCHAR"),
    BINARY(17, "BINARY"),
    VARBINARY(18, "VARBINARY"),
    BITMAP(19, "BITMAP"),
    OBJECT(20, "OBJECT"),
    HLL(21, "HLL"),
    ARRAY(22, "ARRAY"),
    JSON(23, "JSON"),
    MAP(24, "MAP"),
    STRUCT(25, "STRUCT");

    private final int id;
    private final String literal;

    DataType(int id, String literal) {
        this.id = id;
        this.literal = literal;
    }

    public static DataType of(String nameOrLiteral) {
        return elegantOf(nameOrLiteral)
                .orElseThrow(() -> new IllegalArgumentException("Unsupported data type: " + nameOrLiteral));
    }

    public static Optional<DataType> elegantOf(String nameOrLiteral) {
        return Arrays.stream(values())
                .filter(dt -> dt.name().equalsIgnoreCase(nameOrLiteral)
                        || dt.getLiteral().equalsIgnoreCase(nameOrLiteral))
                .findFirst();
    }

    public static boolean isScalar(String nameOrLiteral) {
        return !isArray(nameOrLiteral) && !isStruct(nameOrLiteral) && !isMap(nameOrLiteral);
    }

    public static boolean isArray(String nameOrLiteral) {
        return DataType.ARRAY.equals(of(nameOrLiteral));
    }

    public static boolean isStruct(String nameOrLiteral) {
        return DataType.STRUCT.equals(of(nameOrLiteral));
    }

    public static boolean isMap(String nameOrLiteral) {
        return DataType.MAP.equals(of(nameOrLiteral));
    }

    public boolean is(String type) {
        return name().equalsIgnoreCase(type) || getLiteral().equalsIgnoreCase(type);
    }

    public boolean not(String type) {
        return !is(type);
    }

    public int getId() {
        return id;
    }

    public String getLiteral() {
        return literal;
    }

}
