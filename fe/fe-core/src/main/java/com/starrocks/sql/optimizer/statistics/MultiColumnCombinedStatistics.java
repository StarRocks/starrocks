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

package com.starrocks.sql.optimizer.statistics;

import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MultiColumnCombinedStatistics {
    public static MultiColumnCombinedStatistics EMPTY = new MultiColumnCombinedStatistics();

    private final Map<Set<Integer>, Long> distinctCounts =  new HashMap<>();

    private MultiColumnCombinedStatistics() {
    }

    public MultiColumnCombinedStatistics(Set<Integer> columns, long distinctCount) {
        distinctCounts.put(columns, distinctCount);
    }

    public void update(Set<Integer> columnIds, long distinctCount) {
        distinctCounts.put(columnIds, distinctCount);
    }

    public Map<Set<Integer>, Long> getDistinctCounts() {
        return distinctCounts;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("distinctCounts", distinctCounts)
                .toString();
    }
}
