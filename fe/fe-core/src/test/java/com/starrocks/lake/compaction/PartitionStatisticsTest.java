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

package com.starrocks.lake.compaction;

import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PartitionStatisticsTest {

    // This JSON string represents an older version of the JSON without the "priority" field
    private static final String OLD_JSON_WITHOUT_PRIORITY = "{"
            + "\"partition\": { /* PartitionIdentifier fields */ },"
            + "\"compactionVersion\": { /* PartitionVersion fields */ },"
            + "\"currentVersion\": { /* PartitionVersion fields */ },"
            + "\"nextCompactionTime\": 123456789,"
            + "\"compactionScore\": { /* Quantiles fields */ }"
            + "}";

    @Test
    public void testDeserializationOfOldJsonShouldSetPriorityToDefault() {
        PartitionStatistics statistics = GsonUtils.GSON.fromJson(OLD_JSON_WITHOUT_PRIORITY, PartitionStatistics.class);

        // Assert that the priority field is set to the default value as defined in the PartitionStatistics class
        assertEquals(PartitionStatistics.CompactionPriority.DEFAULT, statistics.getPriority());
    }
}