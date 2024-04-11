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

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

/**
 * Store the update information of base table for MV
 */
public class MvBaseTableUpdateInfo {
    // The partition names of base table that have been updated
    private final Set<String> toRefreshPartitionNames = Sets.newHashSet();
    // The mapping of partition name to partition range
    private final Map<String, Range<PartitionKey>> partitionNameWithRanges = Maps.newHashMap();

    public MvBaseTableUpdateInfo() {
    }

    public Set<String> getToRefreshPartitionNames() {
        return toRefreshPartitionNames;
    }

    public Map<String, Range<PartitionKey>> getPartitionNameWithRanges() {
        return partitionNameWithRanges;
    }

    @Override
    public String toString() {
        return "BaseTableRefreshInfo{" +
                ", toRefreshPartitionNames=" + toRefreshPartitionNames +
                ", partitionNameWithRanges=" + partitionNameWithRanges +
                '}';
    }
}
