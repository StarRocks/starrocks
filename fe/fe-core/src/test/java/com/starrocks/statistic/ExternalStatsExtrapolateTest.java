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

import com.starrocks.statistic.ExternalFullStatisticsCollectJob.ScaledStats;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// Unit tests for bounded-cost sample -> full-partition extrapolation (design 2.5, up-front ratio approach).
public class ExternalStatsExtrapolateTest {

    @Test
    public void testTruncatedSampleScaledToFull() {
        // Scanned 100 of 1000 rows (10%): row_count becomes the exact total, null_count/data_size scale x10.
        ScaledStats s = ExternalFullStatisticsCollectJob.extrapolate(100, 5, 800, 1000L);
        Assertions.assertEquals(1000, s.rowCount);
        Assertions.assertEquals(50, s.nullCount);
        Assertions.assertEquals(8000, s.dataSize);
    }

    @Test
    public void testNotTruncatedIsNoOp() {
        // Sample already covers the whole partition (total == sample): stored as-is.
        ScaledStats s = ExternalFullStatisticsCollectJob.extrapolate(1000, 5, 800, 1000L);
        Assertions.assertEquals(1000, s.rowCount);
        Assertions.assertEquals(5, s.nullCount);
        Assertions.assertEquals(800, s.dataSize);
    }

    @Test
    public void testUnknownTotalIsNoOp() {
        // No metadata total available -> keep raw sample values.
        ScaledStats s = ExternalFullStatisticsCollectJob.extrapolate(100, 5, 800, null);
        Assertions.assertEquals(100, s.rowCount);
        Assertions.assertEquals(5, s.nullCount);
        Assertions.assertEquals(800, s.dataSize);
    }

    @Test
    public void testNullCountCappedAtTotal() {
        // Pathological: an all-null sample must not extrapolate to more nulls than rows.
        ScaledStats s = ExternalFullStatisticsCollectJob.extrapolate(100, 100, 800, 1000L);
        Assertions.assertEquals(1000, s.rowCount);
        Assertions.assertEquals(1000, s.nullCount);
    }

    @Test
    public void testZeroSampleRowsIsNoOp() {
        // Guard against divide-by-zero when nothing was scanned.
        ScaledStats s = ExternalFullStatisticsCollectJob.extrapolate(0, 0, 0, 1000L);
        Assertions.assertEquals(0, s.rowCount);
        Assertions.assertEquals(0, s.nullCount);
        Assertions.assertEquals(0, s.dataSize);
    }

    @Test
    public void testTotalSmallerThanSampleIsNoOp() {
        // Stale/drifted metadata total below the actual sample: never scale down, keep the sample.
        ScaledStats s = ExternalFullStatisticsCollectJob.extrapolate(100, 5, 800, 50L);
        Assertions.assertEquals(100, s.rowCount);
        Assertions.assertEquals(5, s.nullCount);
        Assertions.assertEquals(800, s.dataSize);
    }
}
