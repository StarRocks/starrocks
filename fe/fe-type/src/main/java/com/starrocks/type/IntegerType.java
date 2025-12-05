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

public class IntegerType extends ScalarType {
    public static final IntegerType TINYINT = new IntegerType(PrimitiveType.TINYINT);
    public static final IntegerType SMALLINT = new IntegerType(PrimitiveType.SMALLINT);
    public static final IntegerType INT = new IntegerType(PrimitiveType.INT);
    public static final IntegerType BIGINT = new IntegerType(PrimitiveType.BIGINT);
    public static final IntegerType LARGEINT = new IntegerType(PrimitiveType.LARGEINT);

    public static final ImmutableList<ScalarType> INTEGER_TYPES =
            ImmutableList.of(BooleanType.BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT);

    public IntegerType(PrimitiveType primitiveType) {
        super(primitiveType);
    }
}
