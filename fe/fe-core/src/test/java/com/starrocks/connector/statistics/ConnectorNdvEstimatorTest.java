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

package com.starrocks.connector.statistics;

import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import static com.starrocks.connector.statistics.ConnectorNdvEstimator.TypeCategory.BOOLEAN;
import static com.starrocks.connector.statistics.ConnectorNdvEstimator.TypeCategory.DATE_IN_EPOCH_SECONDS;
import static com.starrocks.connector.statistics.ConnectorNdvEstimator.TypeCategory.FLOAT_LIKE;
import static com.starrocks.connector.statistics.ConnectorNdvEstimator.TypeCategory.INTEGER_LIKE;
import static com.starrocks.connector.statistics.ConnectorNdvEstimator.TypeCategory.OTHER;
import static com.starrocks.connector.statistics.ConnectorNdvEstimator.TypeCategory.STRING_LIKE;
import static com.starrocks.connector.statistics.ConnectorNdvEstimator.TypeCategory.TIMESTAMP_IN_EPOCH_MICROS;
import static com.starrocks.connector.statistics.ConnectorNdvEstimator.TypeCategory.TIMESTAMP_IN_EPOCH_SECONDS;
import static com.starrocks.connector.statistics.ConnectorNdvEstimator.estimate;
import static com.starrocks.connector.statistics.ConnectorNdvEstimator.sizeNdv;
import static com.starrocks.connector.statistics.ConnectorNdvEstimator.typeNdv;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectorNdvEstimatorTest {

    // -------------------------------------------------------------------------
    // Tier 1: range-based
    // -------------------------------------------------------------------------

    @Test
    public void testRangeBased_integer_smallRange() {
        // INT column [1, 10] → 10 distinct values
        double ndv = estimate(INTEGER_LIKE, 1, 10, -1, 0, 1_000_000);
        assertEquals(10.0, ndv, 0.001);
    }

    @Test
    public void testRangeBased_integer_cappedAtRowCount() {
        // INT column [1, 100] but only 5 rows → capped at 5
        double ndv = estimate(INTEGER_LIKE, 1, 100, -1, 0, 5);
        assertEquals(5.0, ndv, 0.001);
    }

    @Test
    public void testRangeBased_boolean() {
        // BOOLEAN [0, 1] → 2 distinct values
        double ndv = estimate(BOOLEAN, 0, 1, -1, 0, 1_000_000);
        assertEquals(2.0, ndv, 0.001);
    }

    @Test
    public void testRangeBased_date_oneYear() {
        // DATE stored as epoch-seconds.
        // 2020-01-01 = 1577836800 s,  2020-12-31 = 1609286400 s
        // diff = 31449600 s → /86400 = 364 days → range = 365
        long jan1 = 1577836800L;
        long dec31 = 1609286400L;
        double ndv = estimate(DATE_IN_EPOCH_SECONDS, jan1, dec31, -1, 0, 1_000_000);
        assertEquals(365.0, ndv, 1.0);
    }

    @Test
    public void testRangeBased_timestamp_epochSeconds_largeRange() {
        // TIMESTAMP stored as epoch-seconds (Delta style).
        // 10-year span = 315_360_000 seconds → far exceeds any typical rowCount → capped
        double ndv = estimate(TIMESTAMP_IN_EPOCH_SECONDS, 0, 315_360_000, -1, 0, 1_000);
        assertEquals(1_000.0, ndv, 0.001);
    }

    @Test
    public void testRangeBased_timestamp_epochMicros_smallRange() {
        // TIMESTAMP stored as epoch-seconds (from epoch-µs ÷ 1e6, Iceberg style).
        // Two values 1000 µs apart, stored as 0.001 s apart in double.
        // After ×1_000_000, diff = 1000 µs → range = 1001.
        double minS = 1_700_000_000.000_000;
        double maxS = 1_700_000_000.001_000; // 1000 µs later
        double ndv = estimate(TIMESTAMP_IN_EPOCH_MICROS, minS, maxS, -1, 0, 1_000_000);
        assertEquals(1001.0, ndv, 1.0);
    }

    @Test
    public void testRangeBased_string_skipsToSizeOrTypeFraction() {
        // STRING is not range-estimable; should fall through to Tier 3
        double ndv = estimate(STRING_LIKE, 0, 100, -1, 0, 100);
        // Tier 3: fraction 0.5 → 50
        assertEquals(50.0, ndv, 0.001);
    }

    // -------------------------------------------------------------------------
    // Tier 2: size-based
    // -------------------------------------------------------------------------

    @Test
    public void testSizeBased_integerColumn() {
        // 1000 bytes / 4 bytes per INT = 250 distinct values
        double ndv = estimate(INTEGER_LIKE, Double.NaN, Double.NaN, 1000, 1000, 1000);
        assertEquals(250.0, ndv, 0.001);
    }

    @Test
    public void testSizeBased_extrapolated() {
        // 500 bytes / 4 bytes = 125, but only covers 500 of 1000 rows → ×2 → 250
        double ndv = estimate(INTEGER_LIKE, Double.NaN, Double.NaN, 500, 500, 1000);
        assertEquals(250.0, ndv, 0.001);
    }

    @Test
    public void testSizeBased_cappedAtRowCount() {
        // 100_000 bytes / 4 = 25_000, but only 100 rows → capped at 100
        double ndv = estimate(INTEGER_LIKE, Double.NaN, Double.NaN, 100_000, 100, 100);
        assertEquals(100.0, ndv, 0.001);
    }

    // -------------------------------------------------------------------------
    // Tier 1 + Tier 2 combined: min of both
    // -------------------------------------------------------------------------

    @Test
    public void testTier1And2Combined_sizeWins() {
        // Range [1, 1_000_000] → rangeNdv = 1_000_000 (or capped)
        // Size 1000 bytes / 4 = 250 (full coverage, no extrapolation) → sizeNdv wins
        double ndv = estimate(INTEGER_LIKE, 1, 1_000_000, 1000, 1_000_000, 1_000_000);
        assertEquals(250.0, ndv, 0.001);
    }

    @Test
    public void testTier1And2Combined_rangeWins() {
        // Range [1, 5] → rangeNdv = 5
        // Size 100_000 bytes / 4 = 25_000 → range wins (is more conservative)
        double ndv = estimate(INTEGER_LIKE, 1, 5, 100_000, 10_000, 10_000);
        assertEquals(5.0, ndv, 0.001);
    }

    // -------------------------------------------------------------------------
    // Tier 3: type-fraction fallback
    // -------------------------------------------------------------------------

    @Test
    public void testTypeFraction_boolean() {
        assertEquals(2.0, typeNdv(BOOLEAN, 1_000), 0.001);
    }

    @Test
    public void testTypeFraction_integer() {
        // 0.3 × 100 = 30
        assertEquals(30.0, typeNdv(INTEGER_LIKE, 100), 0.001);
    }

    @Test
    public void testTypeFraction_string() {
        // 0.5 × 100 = 50
        assertEquals(50.0, typeNdv(STRING_LIKE, 100), 0.001);
    }

    @Test
    public void testTypeFraction_cappedAtRowCount() {
        // 0.3 × 2 = 0.6 → capped to max(1, min(0.6, 2)) = 1
        assertEquals(1.0, typeNdv(INTEGER_LIKE, 2), 0.001);
    }

    // -------------------------------------------------------------------------
    // Edge cases
    // -------------------------------------------------------------------------

    @Test
    public void testZeroRowCount_returnsOne() {
        double ndv = estimate(INTEGER_LIKE, 1, 100, -1, 0, 0);
        assertEquals(1.0, ndv, 0.001);
    }

    @Test
    public void testMinEqualsMax_returnsOne() {
        double ndv = estimate(INTEGER_LIKE, 42, 42, -1, 0, 1_000);
        assertEquals(1.0, ndv, 0.001);
    }

    @Test
    public void testResult_alwaysAtLeastOne() {
        // Even with empty size and tiny row count, NDV should be ≥ 1
        double ndv = estimate(STRING_LIKE, Double.NaN, Double.NaN, -1, 0, 1);
        assertTrue(ndv >= 1.0);
    }

    // -------------------------------------------------------------------------
    // sizeNdv 4-arg convenience overload (delegates to 5-arg with hint=0)
    // -------------------------------------------------------------------------

    @Test
    public void testSizeNdv_4arg_delegatesToDefault() {
        // 400 bytes, 100 rows, 100 total → INTEGER_LIKE default 4 bytes → NDV = 100
        double ndv = sizeNdv(INTEGER_LIKE, 400, 100, 100);
        assertEquals(100.0, ndv, 0.001);
    }

    // -------------------------------------------------------------------------
    // typeNdv edge cases
    // -------------------------------------------------------------------------

    @Test
    public void testTypeFraction_boolean_cappedAt2() {
        assertEquals(2.0, typeNdv(BOOLEAN, 1_000_000), 0.001);
    }

    @Test
    public void testTypeFraction_boolean_smallRowCount() {
        assertEquals(1.0, typeNdv(BOOLEAN, 1), 0.001);
    }

    @Test
    public void testTypeFraction_string_half() {
        // STRING_LIKE fraction = 0.5
        assertEquals(50.0, typeNdv(STRING_LIKE, 100), 0.001);
    }

    @Test
    public void testTypeFraction_other_threeDecile() {
        // OTHER fraction = 0.3
        assertEquals(30.0, typeNdv(OTHER, 100), 0.001);
    }

    // -------------------------------------------------------------------------
    // minBytesPerDistinctValue
    // -------------------------------------------------------------------------

    @Test
    public void testMinBytesPerDistinctValue_boolean() {
        assertEquals(1, ConnectorNdvEstimator.minBytesPerDistinctValue(BOOLEAN));
    }

    @Test
    public void testMinBytesPerDistinctValue_timestamp() {
        assertEquals(8, ConnectorNdvEstimator.minBytesPerDistinctValue(TIMESTAMP_IN_EPOCH_SECONDS));
        assertEquals(8, ConnectorNdvEstimator.minBytesPerDistinctValue(TIMESTAMP_IN_EPOCH_MICROS));
    }

    @Test
    public void testMinBytesPerDistinctValue_otherCategories() {
        assertEquals(4, ConnectorNdvEstimator.minBytesPerDistinctValue(INTEGER_LIKE));
        assertEquals(4, ConnectorNdvEstimator.minBytesPerDistinctValue(FLOAT_LIKE));
        assertEquals(4, ConnectorNdvEstimator.minBytesPerDistinctValue(DATE_IN_EPOCH_SECONDS));
        assertEquals(4, ConnectorNdvEstimator.minBytesPerDistinctValue(STRING_LIKE));
        assertEquals(4, ConnectorNdvEstimator.minBytesPerDistinctValue(OTHER));
    }

    // -------------------------------------------------------------------------
    // fromStarRocksType
    // -------------------------------------------------------------------------

    @Test
    public void testFromStarRocksType_allCategories() {
        assertEquals(BOOLEAN, ConnectorNdvEstimator.fromStarRocksType(BooleanType.BOOLEAN));
        assertEquals(INTEGER_LIKE, ConnectorNdvEstimator.fromStarRocksType(IntegerType.INT));
        assertEquals(INTEGER_LIKE, ConnectorNdvEstimator.fromStarRocksType(IntegerType.BIGINT));
        assertEquals(FLOAT_LIKE, ConnectorNdvEstimator.fromStarRocksType(FloatType.FLOAT));
        assertEquals(FLOAT_LIKE, ConnectorNdvEstimator.fromStarRocksType(FloatType.DOUBLE));
        assertEquals(FLOAT_LIKE, ConnectorNdvEstimator.fromStarRocksType(DecimalType.DEFAULT_DECIMAL32));
        assertEquals(DATE_IN_EPOCH_SECONDS, ConnectorNdvEstimator.fromStarRocksType(DateType.DATE));
        assertEquals(TIMESTAMP_IN_EPOCH_SECONDS, ConnectorNdvEstimator.fromStarRocksType(DateType.DATETIME));
        assertEquals(STRING_LIKE, ConnectorNdvEstimator.fromStarRocksType(VarcharType.VARCHAR));
        assertEquals(OTHER, ConnectorNdvEstimator.fromStarRocksType(JsonType.JSON));
    }
}
