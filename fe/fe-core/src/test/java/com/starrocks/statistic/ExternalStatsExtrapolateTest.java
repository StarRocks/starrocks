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
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
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


    @Test
    public void testIntegerPartitionValueIsNotQuoted() {
        // Quoting it makes the comparison string-to-string, and where the implicit cast resolving that
        // lands decides whether the partition is pruned and whether the file min/max can be used - if it
        // lands on the column, the statistics query aggregates over rows it should never have read.
        Assertions.assertEquals("2452583",
                ExternalFullStatisticsCollectJob.partitionValueLiteral(IntegerType.INT, "2452583"));
        Assertions.assertEquals("-7",
                ExternalFullStatisticsCollectJob.partitionValueLiteral(IntegerType.BIGINT, "-7"));
    }

    @Test
    public void testNonIntegerPartitionValueStaysQuoted() {
        // Correct for strings, and a date literal folds cleanly into a DATE comparison.
        Assertions.assertEquals("'east'",
                ExternalFullStatisticsCollectJob.partitionValueLiteral(StringType.STRING, "east"));
        Assertions.assertEquals("'2024-01-01'",
                ExternalFullStatisticsCollectJob.partitionValueLiteral(DateType.DATE, "2024-01-01"));
        Assertions.assertEquals("'7'",
                ExternalFullStatisticsCollectJob.partitionValueLiteral(null, "7"));
    }

    @Test
    public void testAValueThatIsNotAnIntegerKeepsTheQuotedForm() {
        // A column declared integer whose partition value is not one - a null marker, or metadata that
        // does not match the schema - must not be pasted in raw, which would not parse.
        Assertions.assertEquals("'__HIVE_DEFAULT_PARTITION__'", ExternalFullStatisticsCollectJob
                .partitionValueLiteral(IntegerType.INT, "__HIVE_DEFAULT_PARTITION__"));
        Assertions.assertEquals("''", ExternalFullStatisticsCollectJob.partitionValueLiteral(IntegerType.INT, ""));
        Assertions.assertEquals("'1.5'", ExternalFullStatisticsCollectJob.partitionValueLiteral(IntegerType.INT, "1.5"));
    }

    @Test
    public void testQuotedPartitionValueIsEscaped() {
        // A partition value is data. Unescaped, a quote in one ends the literal early and the rest of it
        // is parsed as SQL.
        Assertions.assertEquals("'a''b'",
                ExternalFullStatisticsCollectJob.partitionValueLiteral(StringType.STRING, "a'b"));
    }
}
