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


package com.starrocks.connector.delta;

import com.google.common.collect.ImmutableMap;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

import java.util.Comparator;

public class DeltaLakeComparators {
    private DeltaLakeComparators() {
    }

    private static final ImmutableMap<DataType, Comparator<?>> COMPARATORS =
            ImmutableMap.<DataType, Comparator<?>>builder()
                    .put(BooleanType.BOOLEAN, Comparator.naturalOrder())
                    .put(ShortType.SHORT, Comparator.naturalOrder())
                    .put(IntegerType.INTEGER, Comparator.naturalOrder())
                    .put(LongType.LONG, Comparator.naturalOrder())
                    .put(FloatType.FLOAT, Comparator.naturalOrder())
                    .put(DoubleType.DOUBLE, Comparator.naturalOrder())
                    .put(TimestampType.TIMESTAMP, Comparator.naturalOrder())
                    .put(TimestampNTZType.TIMESTAMP_NTZ, Comparator.naturalOrder())
                    .put(DateType.DATE, Comparator.naturalOrder())
                    .put(StringType.STRING, Comparator.naturalOrder())
                    .put(BinaryType.BINARY, Comparator.naturalOrder())
                    .put(ByteType.BYTE, Comparator.naturalOrder())
                    .buildOrThrow();

    @SuppressWarnings("unchecked")
    public static <T> Comparator<T> forType(DataType type) {
        Comparator<?> cmp = COMPARATORS.get(type);
        if (cmp != null) {
            return (Comparator<T>) cmp;
        } else {
            throw new UnsupportedOperationException("Cannot determine comparator for type: " + type);
        }
    }
}
