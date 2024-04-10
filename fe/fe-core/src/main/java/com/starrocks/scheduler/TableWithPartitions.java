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

package com.starrocks.scheduler;

import com.google.api.client.util.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.sql.common.PartitionRange;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TableWithPartitions {
    private final Table table;
    private final Set<String> partitionNames;
    public TableWithPartitions(Table table, Set<String> partitionNames) {
        this.table = table;
        this.partitionNames = partitionNames;
    }

    public Table getTable() {
        return table;
    }

    public Set<String> getPartitionNames() {
        return partitionNames;
    }

    public List<PartitionRange> getSortedPartitionRanges(Map<String, Range<PartitionKey>> partitinRangeMap) {
        return getSortedPartitionRanges(partitinRangeMap, this.partitionNames);
    }

    public static List<PartitionRange> getSortedPartitionRanges(Map<String, Range<PartitionKey>> partitinRangeMap,
                                                                Set<String> partitionNames) {
        if (partitionNames == null || partitionNames.isEmpty()) {
            return Lists.newArrayList();
        }
        return partitionNames.stream()
                .map(p -> new PartitionRange(p, partitinRangeMap.get(p)))
                .sorted(PartitionRange::compareTo)
                .collect(Collectors.toList());
    }
}