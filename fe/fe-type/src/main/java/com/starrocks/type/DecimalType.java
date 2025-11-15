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

package com.starrocks.type;

import com.google.common.collect.ImmutableList;

public class DecimalType extends ScalarType {
    public static final DecimalType DEFAULT_DECIMAL32 = new DecimalType(PrimitiveType.DECIMAL32, 9, 3);
    public static final DecimalType DEFAULT_DECIMAL64 = new DecimalType(PrimitiveType.DECIMAL64, 18, 6);
    public static final DecimalType DEFAULT_DECIMAL128 = new DecimalType(PrimitiveType.DECIMAL128, 38, 9);
    public static final DecimalType DEFAULT_DECIMAL256 = new DecimalType(PrimitiveType.DECIMAL256, 76, 12);

    // SQL standard
    public static final int DEFAULT_SCALE = 0;
    // SQL allows the engine to pick the default precision. We pick the largest
    // precision that is supported by the smallest decimal type in the BE (4 bytes).
    public static final int DEFAULT_PRECISION = 9;
    public static final ScalarType DEFAULT_DECIMALV2 = new DecimalType(PrimitiveType.DECIMALV2, DEFAULT_PRECISION, DEFAULT_SCALE);
    public static final ScalarType DECIMALV2 = DEFAULT_DECIMALV2;

    public static final DecimalType DECIMAL32 = new DecimalType(PrimitiveType.DECIMAL32, -1, -1);
    public static final DecimalType DECIMAL64 = new DecimalType(PrimitiveType.DECIMAL64, -1, -1);
    public static final DecimalType DECIMAL128 = new DecimalType(PrimitiveType.DECIMAL128, -1, -1);
    public static final DecimalType DECIMAL256 = new DecimalType(PrimitiveType.DECIMAL256, -1, -1);
    // NOTE: DECIMAL_TYPES not contain DECIMALV2
    public static final ImmutableList<ScalarType> DECIMAL_TYPES = ImmutableList.of(DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256);

    // DECIMAL64_INT and DECIMAL128_INT for integer casting to decimal
    public static final DecimalType DECIMAL_ZERO = new DecimalType(PrimitiveType.DECIMAL32, 0, 0);
    public static final DecimalType DECIMAL32_INT = new DecimalType(PrimitiveType.DECIMAL64, 9, 0);
    public static final DecimalType DECIMAL64_INT = new DecimalType(PrimitiveType.DECIMAL64, 18, 0);
    public static final DecimalType DECIMAL128_INT = new DecimalType(PrimitiveType.DECIMAL128, 38, 0);

    public DecimalType(PrimitiveType primitiveType, int precision, int scale) {
        super(primitiveType);
        setPrecision(precision);
        setScale(scale);
    }
}
