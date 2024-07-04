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

package com.starrocks.sql.util;

import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;

import java.util.Objects;

public class TypePlus {
    private final Type type;
    private final int len;
    private final int precision;
    private final int scale;
    private transient Type rectifiedType = null;
    private transient Type decayedType = null;

    private TypePlus(Type type, int len, int precision, int scale) {
        this.type = Objects.requireNonNull(type);
        this.len = len;
        this.precision = precision;
        this.scale = scale;
    }

    public static TypePlus of(Type type, int len, int precision, int scale) {
        return new TypePlus(type, len, precision, scale);
    }

    public static TypePlus of(Type type) {
        return new TypePlus(type, -1, -1, -1);
    }

    public int getLen() {
        return len;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public Type getType() {
        if (rectifiedType == null) {
            if (type instanceof ScalarType) {
                ScalarType scalarType = (ScalarType) type;
                rectifiedType = ScalarType.createType(scalarType.getPrimitiveType(), len, precision, scale);
            } else {
                rectifiedType = type;
            }
        }
        return rectifiedType;
    }

    public Type getDecayedType() {
        if (decayedType == null) {
            if (type instanceof ScalarType) {
                ScalarType scalarType = (ScalarType) type;
                decayedType = ScalarType.createType(scalarType.getPrimitiveType());
            } else {
                decayedType = type;
            }
        }
        return decayedType;
    }
}