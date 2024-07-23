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

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.starrocks.catalog.PartitionKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class RangePartitionDiff extends PartitionDiff {

    private static final Logger LOG = LogManager.getLogger(RangePartitionDiff.class);

    private Map<String, Range<PartitionKey>> adds = Maps.newHashMap();
    private Map<String, Range<PartitionKey>> deletes = Maps.newHashMap();

    public RangePartitionDiff() {
    }

    public Map<String, Range<PartitionKey>> getAdds() {
        return adds;
    }

    public void setAdds(Map<String, Range<PartitionKey>> adds) {
        this.adds = adds;
    }

    public Map<String, Range<PartitionKey>> getDeletes() {
        return deletes;
    }

    public void setDeletes(Map<String, Range<PartitionKey>> deletes) {
        this.deletes = deletes;
    }

    /**
     * Merge multiple-diff into one diff for multi-basetable MV
     * T1: [p0, p1, p2]
     * T2: [p1, p2, p3]
     * Merged => [p0, p1, p2, p3]
     * NOTE: for intersected partitions, they must be identical
     */
    public static void checkRangePartitionAligned(List<RangePartitionDiff> diffList) {
        if (diffList.size() == 1) {
            return;
        }
        RangeMap<PartitionKey, String> addRanges = TreeRangeMap.create();
        for (RangePartitionDiff diff : diffList) {
            for (Map.Entry<String, Range<PartitionKey>> add : diff.getAdds().entrySet()) {
                Map<Range<PartitionKey>, String> intersectedRange =
                        addRanges.subRangeMap(add.getValue()).asMapOfRanges();
                // should either empty or exactly same
                if (!intersectedRange.isEmpty()) {
                    Range<PartitionKey> existingRange = intersectedRange.keySet().iterator().next();
                    if (intersectedRange.size() > 1 ||
                            !existingRange.equals(add.getValue())) {
                        throw new IllegalArgumentException(
                                "partitions are intersected: " + existingRange + " and " + add);
                    }
                }
            }
            diff.getAdds().forEach((key, value) -> addRanges.put(value, key));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RangePartitionDiff{");
        sb.append("adds=").append(adds);
        sb.append(", deletes=").append(deletes);
        sb.append('}');
        return sb.toString();
    }
}
