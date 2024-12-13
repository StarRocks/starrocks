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
import io.delta.kernel.types.DataType;

import java.util.Comparator;

public class DeltaLakeComparators {
    private DeltaLakeComparators() {
    }

    private static final ImmutableMap<DeltaDataType, Comparator<?>> COMPARATORS =
            ImmutableMap.<DeltaDataType, Comparator<?>>builder()
                    .put(DeltaDataType.BOOLEAN, Comparator.naturalOrder())
                    .put(DeltaDataType.SMALLINT, Comparator.naturalOrder())
                    .put(DeltaDataType.INTEGER, Comparator.naturalOrder())
                    .put(DeltaDataType.LONG, Comparator.naturalOrder())
                    .put(DeltaDataType.FLOAT, Comparator.naturalOrder())
                    .put(DeltaDataType.DOUBLE, Comparator.naturalOrder())
                    .put(DeltaDataType.TIMESTAMP, Comparator.naturalOrder())
                    .put(DeltaDataType.TIMESTAMP_NTZ, Comparator.naturalOrder())
                    .put(DeltaDataType.DATE, Comparator.naturalOrder())
                    .put(DeltaDataType.STRING, Comparator.naturalOrder())
                    .put(DeltaDataType.BINARY, Comparator.naturalOrder())
                    .put(DeltaDataType.BYTE, Comparator.naturalOrder())
                    .put(DeltaDataType.DECIMAL, Comparator.naturalOrder())
                    .buildOrThrow();

    @SuppressWarnings("unchecked")
    public static <T> Comparator<T> forType(DataType type) {
        Comparator<?> cmp = COMPARATORS.get(DeltaDataType.instanceFrom(type.getClass()));
        if (cmp != null) {
            return (Comparator<T>) cmp;
        } else {
            throw new UnsupportedOperationException("Cannot determine comparator for type: " + type);
        }
    }
}
