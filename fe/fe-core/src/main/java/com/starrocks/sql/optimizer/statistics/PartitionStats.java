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

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Partition-level statistics
 */
public class PartitionStats {
    public final Map<Long, Double> distinctCount;
    public final Map<Long, Double> nullFraction;

    public PartitionStats() {
        this.distinctCount = Maps.newHashMap();
        this.nullFraction = Maps.newHashMap();
    }

    public Map<Long, Double> getDistinctCount() {
        return distinctCount;
    }

    public Map<Long, Double> getNullFraction() {
        return nullFraction;
    }
}
