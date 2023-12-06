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
package com.starrocks.catalog;

import com.starrocks.common.type.StarrocksType;

import java.util.HashMap;
import java.util.Map;

public class StarrocksTypeConverter {

    private static Map<Type, StarrocksType> typeMapper = new HashMap<>();
    private static Map<StarrocksType, Type> starrocksTypeMapper = new HashMap<>();

    static {
        typeMapper.put(Type.BOOLEAN, StarrocksType.BOOLEAN);
        typeMapper.put(Type.TINYINT, StarrocksType.TINYINT);
        typeMapper.put(Type.SMALLINT, StarrocksType.SMALLINT);
        typeMapper.put(Type.INT, StarrocksType.INT);
        typeMapper.put(Type.BIGINT, StarrocksType.BIGINT);
        typeMapper.put(Type.FLOAT, StarrocksType.FLOAT);
        typeMapper.put(Type.DOUBLE, StarrocksType.DOUBLE);
        typeMapper.put(Type.DEFAULT_STRING, StarrocksType.STRING);
        typeMapper.put(Type.VARCHAR, StarrocksType.VARCHAR);

        starrocksTypeMapper.put(StarrocksType.BOOLEAN, Type.BOOLEAN);
        starrocksTypeMapper.put(StarrocksType.TINYINT, Type.TINYINT);
        starrocksTypeMapper.put(StarrocksType.SMALLINT, Type.SMALLINT);
        starrocksTypeMapper.put(StarrocksType.INT, Type.INT);
        starrocksTypeMapper.put(StarrocksType.BIGINT, Type.BIGINT);
        starrocksTypeMapper.put(StarrocksType.FLOAT, Type.FLOAT);
        starrocksTypeMapper.put(StarrocksType.DOUBLE, Type.DOUBLE);
        starrocksTypeMapper.put(StarrocksType.STRING, Type.DEFAULT_STRING);
        starrocksTypeMapper.put(StarrocksType.VARCHAR, Type.VARCHAR);
    }

    public static StarrocksType getSRType(Type type) {
        if (typeMapper.containsKey(type)) {
            return typeMapper.get(type);
        }
        throw new RuntimeException("Unsupported type " + type + " for UDF");
    }

    public static StarrocksType[] getSRTypes(Type[] types) {
        StarrocksType[] res = new StarrocksType[types.length];
        for (int i = 0; i < types.length; i++) {
            res[i] = getSRType(types[i]);
        }
        return res;
    }

    public static Type getTypeFromSR(StarrocksType type) {
        if (starrocksTypeMapper.containsKey(type)) {
            return starrocksTypeMapper.get(type);
        }
        throw new RuntimeException("Unsupported type " + type + " for UDF");
    }

}
