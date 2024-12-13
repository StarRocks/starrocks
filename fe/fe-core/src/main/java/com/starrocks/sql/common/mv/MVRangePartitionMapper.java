// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
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


package com.starrocks.sql.common.mv;

import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;

import java.util.Map;

import static com.starrocks.sql.common.TimeUnitUtils.DAY;
import static com.starrocks.sql.common.TimeUnitUtils.TIME_MAP;

/**
 * {@link MVRangePartitionMapper} is used to map the partition range of the base table to the partition range of mv,
 * It's for the mv with partition expr(e.g. {@code date_trunc(granularity, dt)}).
 * </p>
 * Why are there two different implementations of {@link MVRangePartitionMapper}?
 * * Eager mode, which will map/unroll the base table partition range by the granularity of mv partition expr.
 * * Lazy mode, which will use the base table partition range directly and make the mv's range mapping continuous.
 * </p>
 * But eager mode may generate too many partitions when the granularity is {@code minute or hour}, so distinguish them
 * by granularity.
 */
public abstract class MVRangePartitionMapper {
    /**
     * Generate the mapping ranges of mv partition by the base table partition range.
     * @param baseRangeMap base ref table's partition range map
     * @param granularity mv partition expr's granularity
     * @param partitionType mv partition expr's type
     * @return mv partition range map
     */
    public abstract Map<String, Range<PartitionKey>> toMappingRanges(Map<String, Range<PartitionKey>> baseRangeMap,
                                                                     String granularity,
                                                                     PrimitiveType partitionType);

    /**
     * Get the mv range partition mapper instance.
     * @param granularity mv partition expr's granularity
     * @return mv range partition mapper instance according to the granularity
     */
    public static MVRangePartitionMapper getInstance(String granularity) {
        if (granularity != null && TIME_MAP.containsKey(granularity) && TIME_MAP.get(granularity) < TIME_MAP.get(DAY)) {
            return MVLazyRangePartitionMapper.INSTANCE;
        } else {
            return MVEagerRangePartitionMapper.INSTANCE;
        }
    }
}
