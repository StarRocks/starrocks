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

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangePartitionDiff {

    private Map<String, Range<PartitionKey>> adds = Maps.newHashMap();
    private Map<String, Set<String>> rollupToBasePartitionMap = Maps.newHashMap();
    private Map<String, Range<PartitionKey>> deletes = Maps.newHashMap();

    public RangePartitionDiff() {
    }

    public RangePartitionDiff(Map<String, Range<PartitionKey>> adds, Map<String, Range<PartitionKey>> deletes) {
        this.adds = adds;
        this.deletes = deletes;
    }

    public Map<String, Set<String>> getRollupToBasePartitionMap() {
        return rollupToBasePartitionMap;
    }

    public void setRollupToBasePartitionMap(Map<String, Set<String>> rollupToBasePartitionMap) {
        this.rollupToBasePartitionMap = rollupToBasePartitionMap;
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

    public static RangePartitionDiff merge(List<RangePartitionDiff> diffList) {
        RangePartitionDiff result = new RangePartitionDiff();
        RangeMap<PartitionKey, String> addRanges = TreeRangeMap.create();
        for (RangePartitionDiff diff : diffList) {
            for (Map.Entry<String, Range<PartitionKey>> add : diff.getAdds().entrySet()) {
                Map<Range<PartitionKey>, String> intersectedRange =
                        addRanges.subRangeMap(add.getValue()).asMapOfRanges();
                // should either empty or exactly same
                if (!intersectedRange.isEmpty()) {
                    Range<PartitionKey> existingRange = intersectedRange.keySet().iterator().next();
                    if (intersectedRange.size() > 1 ||
                            !existingRange.equals(add.getValue()) ||
                            !addRanges.getEntry(existingRange.lowerEndpoint()).getKey().equals(add.getValue())) {
                        throw new IllegalArgumentException(
                                "Partition is intersected: " + existingRange + " and " + add);
                    }
                }

                addRanges.put(add.getValue(), add.getKey());
            }
            result.getAdds().putAll(diff.getAdds());
            result.getDeletes().putAll(diff.getDeletes());
            result.getRollupToBasePartitionMap().putAll(diff.getRollupToBasePartitionMap());
        }
        result.getDeletes().keySet().removeAll(result.getAdds().keySet());
        return result;
    }
}
