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
// limitations under the License
package com.starrocks.planner;

import com.google.common.collect.Maps;
import com.starrocks.catalog.PartitionKey;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PartitionIdGenerator is used to ensure each PartitionKey get a unique and consistent partitionId in the lifetime of a query.
 */
public class PartitionIdGenerator {
    private final Map<PartitionKey, Long> tablePartitionKeyToId = Maps.newHashMap();
    private final AtomicLong partitionIdGen = new AtomicLong(0L);

    public PartitionIdGenerator() {
    }

    public static PartitionIdGenerator of() {
        return new PartitionIdGenerator();
    }

    public long getOrGenerate(PartitionKey partitionKey) {
        return tablePartitionKeyToId.computeIfAbsent(partitionKey, k -> partitionIdGen.getAndIncrement());
    }
}
