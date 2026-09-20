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

    @Test
    public void testPunishFactor() {
        PartitionStatistics statistics = new PartitionStatistics(new PartitionIdentifier(100, 200, 300));
        // test compaction
        Quantiles q1 = new Quantiles(1.0, 2.0, 3.0);
        statistics.setCompactionScoreAndAdjustPunishFactor(q1, true /* isPartialSuccess */);
        assertEquals(1, statistics.getPunishFactor());

        Quantiles q2 = new Quantiles(1.0, 2.0, 3.0);
        statistics.setCompactionScoreAndAdjustPunishFactor(q2, true /* isPartialSuccess */);
        assertEquals(2, statistics.getPunishFactor());

        Quantiles q3 = new Quantiles(1.0, 2.0, 3.0);
        statistics.setCompactionScoreAndAdjustPunishFactor(q3, true /* isPartialSuccess */);
        assertEquals(4, statistics.getPunishFactor());

        Quantiles q4 = new Quantiles(1.0, 2.0, 3.0);
        statistics.setCompactionScoreAndAdjustPunishFactor(q4, false /* isPartialSuccess */);
        assertEquals(1, statistics.getPunishFactor());

        Quantiles q5 = new Quantiles(1.0, 1.0, 2.0);
        statistics.setCompactionScoreAndAdjustPunishFactor(q5, false /* isPartialSuccess */);
        assertEquals(1, statistics.getPunishFactor());
    }

    @Test
    public void testGetCompactionVersion() {
        PartitionStatistics statistics = new PartitionStatistics(new PartitionIdentifier(100, 200, 300));
        assertEquals(0, statistics.getCompactionVersion().getVersion());
    }

    @Test
    public void testLastPublishMarkersDefaults() {
        PartitionStatistics statistics = new PartitionStatistics(new PartitionIdentifier(100, 200, 300));
        // Defaults: 0 means "never published" — important for the schedule predicate
        // that skips the max-interval trigger until the first successful publish.
        assertEquals(0L, statistics.getLastPublishVisibleVersion());
        assertEquals(0L, statistics.getLastPublishTimeMs());
        assertEquals(0.0, statistics.getLastPublishScore(), 0.0);
    }

    @Test
    public void testLastPublishMarkersRoundTrip() {
        PartitionStatistics statistics = new PartitionStatistics(new PartitionIdentifier(100, 200, 300));
        statistics.setLastPublishVisibleVersion(42L);
        statistics.setLastPublishTimeMs(1234567890L);
        statistics.setLastPublishScore(15.5);
        assertEquals(42L, statistics.getLastPublishVisibleVersion());
        assertEquals(1234567890L, statistics.getLastPublishTimeMs());
        assertEquals(15.5, statistics.getLastPublishScore(), 0.0);
    }

    @Test
    public void testLastPublishMarkersSurviveJsonRoundTrip() {
        PartitionStatistics original = new PartitionStatistics(new PartitionIdentifier(1, 2, 3));
        original.setLastPublishVisibleVersion(99L);
        original.setLastPublishTimeMs(2024_04_28_00L);
        original.setLastPublishScore(7.25);

        String json = GsonUtils.GSON.toJson(original);
        PartitionStatistics restored = GsonUtils.GSON.fromJson(json, PartitionStatistics.class);
        assertEquals(99L, restored.getLastPublishVisibleVersion());
        assertEquals(2024_04_28_00L, restored.getLastPublishTimeMs());
        assertEquals(7.25, restored.getLastPublishScore(), 0.0);
    }

    @Test
    public void testOldJsonWithoutLastPublishFieldsDefaultsToZero() {
        // Pre-feature JSON without the last_publish_* fields should still deserialize.
        PartitionStatistics statistics = GsonUtils.GSON.fromJson(OLD_JSON_WITHOUT_PRIORITY, PartitionStatistics.class);
        assertEquals(0L, statistics.getLastPublishVisibleVersion());
        assertEquals(0L, statistics.getLastPublishTimeMs());
        assertEquals(0.0, statistics.getLastPublishScore(), 0.0);
    }
}