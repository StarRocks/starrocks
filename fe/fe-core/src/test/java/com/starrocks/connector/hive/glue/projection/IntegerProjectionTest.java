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

package com.starrocks.connector.hive.glue.projection;

import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntegerProjectionTest {

    @Test
    public void testGetProjectedValuesNoFilter() {
        IntegerProjection projection = new IntegerProjection(
                "year", "2020,2025", Optional.empty(), Optional.empty());

        List<String> values = projection.getProjectedValues(Optional.empty());

        assertEquals(6, values.size());
        assertEquals("2020", values.get(0));
        assertEquals("2021", values.get(1));
        assertEquals("2022", values.get(2));
        assertEquals("2023", values.get(3));
        assertEquals("2024", values.get(4));
        assertEquals("2025", values.get(5));
    }

    @Test
    public void testGetProjectedValuesWithInterval() {
        IntegerProjection projection = new IntegerProjection(
                "year", "2020,2030", Optional.of("2"), Optional.empty());

        List<String> values = projection.getProjectedValues(Optional.empty());

        assertEquals(6, values.size());
        assertEquals("2020", values.get(0));
        assertEquals("2022", values.get(1));
        assertEquals("2024", values.get(2));
        assertEquals("2026", values.get(3));
        assertEquals("2028", values.get(4));
        assertEquals("2030", values.get(5));
    }

    @Test
    public void testGetProjectedValuesWithDigits() {
        IntegerProjection projection = new IntegerProjection(
                "month", "1,12", Optional.empty(), Optional.of("2"));

        List<String> values = projection.getProjectedValues(Optional.empty());

        assertEquals(12, values.size());
        assertEquals("01", values.get(0));
        assertEquals("02", values.get(1));
        assertEquals("10", values.get(9));
        assertEquals("12", values.get(11));
    }

    @Test
    public void testGetProjectedValuesWithMatchingFilter() {
        IntegerProjection projection = new IntegerProjection(
                "year", "2020,2025", Optional.empty(), Optional.empty());

        List<String> values = projection.getProjectedValues(Optional.of(2023));

        assertEquals(1, values.size());
        assertEquals("2023", values.get(0));
    }

    @Test
    public void testGetProjectedValuesWithMatchingFilterString() {
        IntegerProjection projection = new IntegerProjection(
                "year", "2020,2025", Optional.empty(), Optional.empty());

        List<String> values = projection.getProjectedValues(Optional.of("2023"));

        assertEquals(1, values.size());
        assertEquals("2023", values.get(0));
    }

    @Test
    public void testGetProjectedValuesWithFilterOutOfRange() {
        IntegerProjection projection = new IntegerProjection(
                "year", "2020,2025", Optional.empty(), Optional.empty());

        List<String> values = projection.getProjectedValues(Optional.of(2019));
        assertTrue(values.isEmpty());

        values = projection.getProjectedValues(Optional.of(2026));
        assertTrue(values.isEmpty());
    }

    @Test
    public void testGetProjectedValuesWithFilterNotOnInterval() {
        IntegerProjection projection = new IntegerProjection(
                "year", "2020,2030", Optional.of("5"), Optional.empty());

        // 2023 is not on the interval (2020, 2025, 2030)
        List<String> values = projection.getProjectedValues(Optional.of(2023));
        assertTrue(values.isEmpty());

        // 2025 is on the interval
        values = projection.getProjectedValues(Optional.of(2025));
        assertEquals(1, values.size());
        assertEquals("2025", values.get(0));
    }

    @Test
    public void testFormatValueWithDigits() {
        IntegerProjection projection = new IntegerProjection(
                "month", "1,12", Optional.empty(), Optional.of("2"));

        assertEquals("01", projection.formatValue(1));
        assertEquals("05", projection.formatValue(5));
        assertEquals("12", projection.formatValue(12));
    }

    @Test
    public void testFormatValueWithoutDigits() {
        IntegerProjection projection = new IntegerProjection(
                "year", "2020,2025", Optional.empty(), Optional.empty());

        assertEquals("2023", projection.formatValue(2023));
        assertEquals("1", projection.formatValue(1));
    }

    @Test
    public void testGetColumnType() {
        IntegerProjection projection = new IntegerProjection(
                "year", "2020,2025", Optional.empty(), Optional.empty());

        assertEquals(IntegerType.BIGINT, projection.getColumnType());
    }

    @Test
    public void testGetColumnName() {
        IntegerProjection projection = new IntegerProjection(
                "year", "2020,2025", Optional.empty(), Optional.empty());

        assertEquals("year", projection.getColumnName());
    }

    @Test
    public void testConstructorWithNullRange() {
        assertThrows(IllegalArgumentException.class, () ->
                new IntegerProjection("year", null, Optional.empty(), Optional.empty()));
    }

    @Test
    public void testConstructorWithEmptyRange() {
        assertThrows(IllegalArgumentException.class, () ->
                new IntegerProjection("year", "", Optional.empty(), Optional.empty()));
    }

    @Test
    public void testConstructorWithInvalidRange() {
        assertThrows(IllegalArgumentException.class, () ->
                new IntegerProjection("year", "2020", Optional.empty(), Optional.empty()));

        assertThrows(IllegalArgumentException.class, () ->
                new IntegerProjection("year", "abc,def", Optional.empty(), Optional.empty()));
    }

    @Test
    public void testConstructorWithReversedRange() {
        assertThrows(IllegalArgumentException.class, () ->
                new IntegerProjection("year", "2025,2020", Optional.empty(), Optional.empty()));
    }

    @Test
    public void testConstructorWithZeroInterval() {
        assertThrows(IllegalArgumentException.class, () ->
                new IntegerProjection("year", "2020,2025", Optional.of("0"), Optional.empty()));
    }

    @Test
    public void testConstructorWithNegativeInterval() {
        assertThrows(IllegalArgumentException.class, () ->
                new IntegerProjection("year", "2020,2025", Optional.of("-1"), Optional.empty()));
    }

    @Test
    public void testConstructorWithZeroDigits() {
        assertThrows(IllegalArgumentException.class, () ->
                new IntegerProjection("year", "2020,2025", Optional.empty(), Optional.of("0")));
    }

    @Test
    public void testRequiresFilter() {
        IntegerProjection projection = new IntegerProjection(
                "year", "2020,2025", Optional.empty(), Optional.empty());

        assertFalse(projection.requiresFilter());
    }

    @Test
    public void testConstructorWithOverflowRange() {
        // Range from Long.MIN_VALUE to Long.MAX_VALUE causes arithmetic overflow
        // when calculating (rightBound - leftBound)
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                new IntegerProjection("id", Long.MIN_VALUE + "," + Long.MAX_VALUE,
                        Optional.empty(), Optional.empty()));

        assertTrue(exception.getMessage().contains("arithmetic overflow"));
    }

    @Test
    public void testConstructorWithLargePositiveRange() {
        // Large positive range that would overflow: 0 to Long.MAX_VALUE
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                new IntegerProjection("id", "0," + Long.MAX_VALUE,
                        Optional.empty(), Optional.empty()));

        // Should fail either due to overflow or exceeding MAX_VALUES
        assertTrue(exception.getMessage().contains("overflow") ||
                   exception.getMessage().contains("exceeding limit"));
    }

    @Test
    public void testConstructorWithNegativeToPositiveRange() {
        // Range from negative to positive that causes overflow
        String range = (Long.MIN_VALUE / 2) + "," + (Long.MAX_VALUE / 2);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                new IntegerProjection("id", range, Optional.empty(), Optional.empty()));

        assertTrue(exception.getMessage().contains("overflow") ||
                   exception.getMessage().contains("exceeding limit"));
    }
}
