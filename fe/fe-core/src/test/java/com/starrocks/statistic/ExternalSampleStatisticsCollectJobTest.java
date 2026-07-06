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

package com.starrocks.statistic;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExternalSampleStatisticsCollectJobTest {

    @Test
    public void testLowCardinalityColumnConverges() {
        // A boolean-like column: NDV settles at 2 after the first round and never moves again.
        Map<String, Long> prev = Map.of("is_active", 2L);
        Map<String, Long> curr = Map.of("is_active", 2L);
        assertTrue(ExternalSampleStatisticsCollectJob.isNdvConverging(prev, curr, 0.05));
    }

    @Test
    public void testHighCardinalityColumnKeepsGrowingDoesNotConverge() {
        // A near-unique id column: doubling the sample roughly doubles the observed NDV, so the
        // round loop must keep going instead of stopping early.
        Map<String, Long> prev = Map.of("id", 2_000_000L);
        Map<String, Long> curr = Map.of("id", 3_900_000L);
        assertFalse(ExternalSampleStatisticsCollectJob.isNdvConverging(prev, curr, 0.05));
    }

    @Test
    public void testWithinThresholdConverges() {
        Map<String, Long> prev = Map.of("c1", 1000L);
        Map<String, Long> curr = Map.of("c1", 1020L);
        assertTrue(ExternalSampleStatisticsCollectJob.isNdvConverging(prev, curr, 0.05));
    }

    @Test
    public void testSingleNonConvergingColumnBlocksWholeRound() {
        // Even if most columns have settled, one still-changing column (typically the
        // highest-cardinality one) must keep the round loop from stopping early.
        Map<String, Long> prev = Map.of("stable", 5L, "growing", 1000L);
        Map<String, Long> curr = Map.of("stable", 5L, "growing", 2000L);
        assertFalse(ExternalSampleStatisticsCollectJob.isNdvConverging(prev, curr, 0.05));
    }

    @Test
    public void testMissingPreviousColumnTreatedAsNotConverged() {
        Map<String, Long> prev = Map.of();
        Map<String, Long> curr = Map.of("new_column", 10L);
        assertFalse(ExternalSampleStatisticsCollectJob.isNdvConverging(prev, curr, 0.05));
    }

    @Test
    public void testZeroCardinalityBothRoundsConverges() {
        // An all-null column: cardinality stays 0 across rounds, must not divide by zero.
        Map<String, Long> prev = Map.of("all_null", 0L);
        Map<String, Long> curr = Map.of("all_null", 0L);
        assertTrue(ExternalSampleStatisticsCollectJob.isNdvConverging(prev, curr, 0.05));
    }
}
