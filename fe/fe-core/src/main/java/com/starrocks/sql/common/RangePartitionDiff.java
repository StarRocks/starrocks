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
import com.starrocks.catalog.PartitionKey;

import java.util.Map;
import java.util.Set;

public class RangePartitionDiff {

    Map<String, Range<PartitionKey>> adds = Maps.newHashMap();

    Map<String, Set<String>> rollupToBasePartitionMap = Maps.newHashMap();

    Map<String, Range<PartitionKey>> deletes = Maps.newHashMap();

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
}
