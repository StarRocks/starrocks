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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.PartitionKey;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MVToRefreshPartitionInfo {
    private final Set<String> refTableUpdatedPartitionNames = Sets.newHashSet();
    private final Map<String, Range<PartitionKey>> refTablePartitionNameWithRanges = Maps.newHashMap();
    private final Map<String, List<List<String>>> refTablePartitionNameWithLists = Maps.newHashMap();

    public MVToRefreshPartitionInfo() {
    }

    public Set<String> getRefTableUpdatedPartitionNames() {
        return refTableUpdatedPartitionNames;
    }

    public Map<String, Range<PartitionKey>> getRefTablePartitionNameWithRanges() {
        return refTablePartitionNameWithRanges;
    }

    public Map<String, List<List<String>>> getRefTablePartitionNameWithLists() {
        return refTablePartitionNameWithLists;
    }
}
