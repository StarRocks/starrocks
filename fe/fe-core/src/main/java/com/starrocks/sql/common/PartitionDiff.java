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


package com.starrocks.sql.common;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.starrocks.catalog.PartitionKey;

import java.util.List;
import java.util.Map;

/**
 * The base class for partition diff which is used to represent the difference between two partition sets.
 */
public class PartitionDiff {
    private final PCellSortedSet adds;
    private final PCellSortedSet deletes;

    public PartitionDiff(PCellSortedSet adds, PCellSortedSet deletes) {
        this.adds = adds;
        this.deletes = deletes;
    }

    public PCellSortedSet getAdds() {
        return adds;
    }

    public PCellSortedSet getDeletes() {
        return deletes;
    }

    /**
     * Merge multiple-diff into one diff for multi-basetable MV
     * T1: [p0, p1, p2]
     * T2: [p1, p2, p3]
     * Merged => [p0, p1, p2, p3]
     * NOTE: for intersected partitions, they must be identical
     */
    public static void checkRangePartitionAligned(List<PartitionDiff> diffList) {
        if (diffList.size() == 1) {
            return;
        }
        RangeMap<PartitionKey, String> addRanges = TreeRangeMap.create();
        for (PartitionDiff diff : diffList) {
            for (PCellWithName add : diff.getAdds().getPartitions()) {
                Range<PartitionKey> range = ((PRangeCell) add.cell()).getRange();
                Map<Range<PartitionKey>, String> intersectedRange = addRanges.subRangeMap(range).asMapOfRanges();
                // should either empty or exactly same
                if (!intersectedRange.isEmpty()) {
                    Range<PartitionKey> existingRange = intersectedRange.keySet().iterator().next();
                    if (intersectedRange.size() > 1 || !existingRange.equals(range)) {
                        throw new IllegalArgumentException(
                                "partitions are intersected: " + existingRange + " and " + add);
                    }
                }
            }
            diff.getAdds().forEach((pCellWithName ->
                    addRanges.put(((PRangeCell) pCellWithName.cell()).getRange(), pCellWithName.name())));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PartitionDiff{");
        sb.append("adds=").append(adds);
        sb.append(", deletes=").append(deletes);
        sb.append('}');
        return sb.toString();
    }
}
