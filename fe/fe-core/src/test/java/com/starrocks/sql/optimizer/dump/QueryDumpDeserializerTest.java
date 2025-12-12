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

package com.starrocks.sql.optimizer.dump;

import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for QueryDumpDeserializer's ability to parse both old and new label-formatted statistics.
 */
public class QueryDumpDeserializerTest {

    @Test
    public void testParseOldFormatStatistics() {
        // Test parsing old format: [minValue, maxValue, nullsFraction, averageRowSize, distinctValuesCount]
        String oldFormat = "[1.0, 100.0, 0.0, 8.0, 50.0] ESTIMATE";
        ColumnStatistic stats = ColumnStatistic.buildFrom(oldFormat).build();

        assertThat(stats.getMinValue()).isEqualTo(1.0);
        assertThat(stats.getMaxValue()).isEqualTo(100.0);
        assertThat(stats.getNullsFraction()).isEqualTo(0.0);
        assertThat(stats.getAverageRowSize()).isEqualTo(8.0);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(50.0);
        assertThat(stats.getType()).isEqualTo(ColumnStatistic.StatisticType.ESTIMATE);
    }

    @Test
    public void testParseNewLabeledFormatStatistics() {
        // Test parsing new labeled format: [MIN: minValue, MAX: maxValue, NULLS: nullsFraction, ROS: averageRowSize, NDV: distinctValuesCount]
        String newFormat = "[MIN: 1.0, MAX: 100.0, NULLS: 0.0, ROS: 8.0, NDV: 50.0] ESTIMATE";
        ColumnStatistic stats = ColumnStatistic.buildFrom(newFormat).build();

        assertThat(stats.getMinValue()).isEqualTo(1.0);
        assertThat(stats.getMaxValue()).isEqualTo(100.0);
        assertThat(stats.getNullsFraction()).isEqualTo(0.0);
        assertThat(stats.getAverageRowSize()).isEqualTo(8.0);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(50.0);
        assertThat(stats.getType()).isEqualTo(ColumnStatistic.StatisticType.ESTIMATE);
    }

    @Test
    public void testParseOldFormatWithUnknownType() {
        String oldFormat = "[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN";
        ColumnStatistic stats = ColumnStatistic.buildFrom(oldFormat).build();

        assertThat(stats.getMinValue()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(stats.getMaxValue()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(stats.getNullsFraction()).isEqualTo(0.0);
        assertThat(stats.getAverageRowSize()).isEqualTo(1.0);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(1.0);
        assertThat(stats.getType()).isEqualTo(ColumnStatistic.StatisticType.UNKNOWN);
    }

    @Test
    public void testParseNewLabeledFormatWithUnknownType() {
        String newFormat = "[MIN: -Infinity, MAX: Infinity, NULLS: 0.0, ROS: 1.0, NDV: 1.0] UNKNOWN";
        ColumnStatistic stats = ColumnStatistic.buildFrom(newFormat).build();

        assertThat(stats.getMinValue()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(stats.getMaxValue()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(stats.getNullsFraction()).isEqualTo(0.0);
        assertThat(stats.getAverageRowSize()).isEqualTo(1.0);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(1.0);
        assertThat(stats.getType()).isEqualTo(ColumnStatistic.StatisticType.UNKNOWN);
    }

    @Test
    public void testParseOldFormatWithoutType() {
        // Test parsing old format without type suffix (should detect UNKNOWN if values match)
        String oldFormat = "[-Infinity, Infinity, 0.0, 1.0, 1.0]";
        ColumnStatistic stats = ColumnStatistic.buildFrom(oldFormat).build();

        assertThat(stats.getMinValue()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(stats.getMaxValue()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(stats.getType()).isEqualTo(ColumnStatistic.StatisticType.UNKNOWN);
    }

    @Test
    public void testParseNewLabeledFormatWithoutType() {
        String newFormat = "[MIN: -Infinity, MAX: Infinity, NULLS: 0.0, ROS: 1.0, NDV: 1.0]";
        ColumnStatistic stats = ColumnStatistic.buildFrom(newFormat).build();

        assertThat(stats.getMinValue()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(stats.getMaxValue()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(stats.getType()).isEqualTo(ColumnStatistic.StatisticType.UNKNOWN);
    }

    @Test
    public void testParseOldFormatWithNullFraction() {
        String oldFormat = "[0.0, 1000.0, 0.1, 16.0, 200.0] ESTIMATE";
        ColumnStatistic stats = ColumnStatistic.buildFrom(oldFormat).build();

        assertThat(stats.getMinValue()).isEqualTo(0.0);
        assertThat(stats.getMaxValue()).isEqualTo(1000.0);
        assertThat(stats.getNullsFraction()).isEqualTo(0.1);
        assertThat(stats.getAverageRowSize()).isEqualTo(16.0);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(200.0);
    }

    @Test
    public void testParseNewLabeledFormatWithNullFraction() {
        String newFormat = "[MIN: 0.0, MAX: 1000.0, NULLS: 0.1, ROS: 16.0, NDV: 200.0] ESTIMATE";
        ColumnStatistic stats = ColumnStatistic.buildFrom(newFormat).build();

        assertThat(stats.getMinValue()).isEqualTo(0.0);
        assertThat(stats.getMaxValue()).isEqualTo(1000.0);
        assertThat(stats.getNullsFraction()).isEqualTo(0.1);
        assertThat(stats.getAverageRowSize()).isEqualTo(16.0);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(200.0);
    }

    @Test
    public void testParseOldFormatWithInvalidRange() {
        // When minValue > maxValue, they should be reset to -Infinity and +Infinity
        String oldFormat = "[100.0, 1.0, 0.0, 8.0, 50.0] ESTIMATE";
        ColumnStatistic stats = ColumnStatistic.buildFrom(oldFormat).build();

        assertThat(stats.getMinValue()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(stats.getMaxValue()).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    public void testParseNewLabeledFormatWithInvalidRange() {
        // When minValue > maxValue, they should be reset to -Infinity and +Infinity
        String newFormat = "[MIN: 100.0, MAX: 1.0, NULLS: 0.0, ROS: 8.0, NDV: 50.0] ESTIMATE";
        ColumnStatistic stats = ColumnStatistic.buildFrom(newFormat).build();

        assertThat(stats.getMinValue()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(stats.getMaxValue()).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    public void testParseOldFormatWithNegativeDistinctValues() {
        // When distinctValues <= 0, it should be set to 1
        String oldFormat = "[1.0, 100.0, 0.0, 8.0, -5.0] ESTIMATE";
        ColumnStatistic stats = ColumnStatistic.buildFrom(oldFormat).build();

        assertThat(stats.getDistinctValuesCount()).isEqualTo(1.0);
    }

    @Test
    public void testParseNewLabeledFormatWithNegativeDistinctValues() {
        // When distinctValues <= 0, it should be set to 1
        String newFormat = "[MIN: 1.0, MAX: 100.0, NULLS: 0.0, ROS: 8.0, NDV: 0.0] ESTIMATE";
        ColumnStatistic stats = ColumnStatistic.buildFrom(newFormat).build();

        assertThat(stats.getDistinctValuesCount()).isEqualTo(1.0);
    }

    @Test
    public void testParseOldFormatWithScientificNotation() {
        String oldFormat = "[1.0E-10, 1.0E10, 0.0, 8.0, 1.0E5] ESTIMATE";
        ColumnStatistic stats = ColumnStatistic.buildFrom(oldFormat).build();

        assertThat(stats.getMinValue()).isEqualTo(1.0E-10);
        assertThat(stats.getMaxValue()).isEqualTo(1.0E10);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(1.0E5);
    }

    @Test
    public void testParseNewLabeledFormatWithScientificNotation() {
        String newFormat = "[MIN: 1.0E-10, MAX: 1.0E10, NULLS: 0.0, ROS: 8.0, NDV: 1.0E5] ESTIMATE";
        ColumnStatistic stats = ColumnStatistic.buildFrom(newFormat).build();

        assertThat(stats.getMinValue()).isEqualTo(1.0E-10);
        assertThat(stats.getMaxValue()).isEqualTo(1.0E10);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(1.0E5);
    }

    @Test
    public void testBackwardCompatibilityBothFormatsProduceSameResult() {
        // Ensure both formats produce the same ColumnStatistic object
        String oldFormat = "[1.0, 100.0, 0.1, 8.0, 50.0] ESTIMATE";
        String newFormat = "[MIN: 1.0, MAX: 100.0, NULLS: 0.1, ROS: 8.0, NDV: 50.0] ESTIMATE";

        ColumnStatistic oldStats = ColumnStatistic.buildFrom(oldFormat).build();
        ColumnStatistic newStats = ColumnStatistic.buildFrom(newFormat).build();

        assertThat(oldStats.getMinValue()).isEqualTo(newStats.getMinValue());
        assertThat(oldStats.getMaxValue()).isEqualTo(newStats.getMaxValue());
        assertThat(oldStats.getNullsFraction()).isEqualTo(newStats.getNullsFraction());
        assertThat(oldStats.getAverageRowSize()).isEqualTo(newStats.getAverageRowSize());
        assertThat(oldStats.getDistinctValuesCount()).isEqualTo(newStats.getDistinctValuesCount());
        assertThat(oldStats.getType()).isEqualTo(newStats.getType());
    }
}
