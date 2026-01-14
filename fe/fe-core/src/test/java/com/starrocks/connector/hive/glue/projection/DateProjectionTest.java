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

import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DateProjectionTest {

    @Test
    public void testGetProjectedValuesNoFilter() {
        DateProjection projection = new DateProjection(
                "dt", "2024-01-01,2024-01-05", "yyyy-MM-dd",
                Optional.empty(), Optional.empty());

        List<String> values = projection.getProjectedValues(Optional.empty());

        assertEquals(5, values.size());
        assertEquals("2024-01-01", values.get(0));
        assertEquals("2024-01-02", values.get(1));
        assertEquals("2024-01-03", values.get(2));
        assertEquals("2024-01-04", values.get(3));
        assertEquals("2024-01-05", values.get(4));
    }

    @Test
    public void testGetProjectedValuesWithInterval() {
        DateProjection projection = new DateProjection(
                "dt", "2024-01-01,2024-01-10", "yyyy-MM-dd",
                Optional.of("2"), Optional.of("DAYS"));

        List<String> values = projection.getProjectedValues(Optional.empty());

        assertEquals(5, values.size());
        assertEquals("2024-01-01", values.get(0));
        assertEquals("2024-01-03", values.get(1));
        assertEquals("2024-01-05", values.get(2));
        assertEquals("2024-01-07", values.get(3));
        assertEquals("2024-01-09", values.get(4));
    }

    @Test
    public void testGetProjectedValuesWithMonthInterval() {
        DateProjection projection = new DateProjection(
                "month", "2024-01,2024-06", "yyyy-MM",
                Optional.of("1"), Optional.of("MONTHS"));

        List<String> values = projection.getProjectedValues(Optional.empty());

        assertEquals(6, values.size());
        assertEquals("2024-01", values.get(0));
        assertEquals("2024-02", values.get(1));
        assertEquals("2024-06", values.get(5));
    }

    @Test
    public void testGetProjectedValuesWithMatchingFilter() {
        DateProjection projection = new DateProjection(
                "dt", "2024-01-01,2024-01-31", "yyyy-MM-dd",
                Optional.empty(), Optional.empty());

        List<String> values = projection.getProjectedValues(Optional.of("2024-01-15"));

        assertEquals(1, values.size());
        assertEquals("2024-01-15", values.get(0));
    }

    @Test
    public void testGetProjectedValuesWithFilterOutOfRange() {
        DateProjection projection = new DateProjection(
                "dt", "2024-01-01,2024-01-31", "yyyy-MM-dd",
                Optional.empty(), Optional.empty());

        // Before range
        List<String> values = projection.getProjectedValues(Optional.of("2023-12-31"));
        assertTrue(values.isEmpty());

        // After range
        values = projection.getProjectedValues(Optional.of("2024-02-01"));
        assertTrue(values.isEmpty());
    }

    @Test
    public void testGetProjectedValuesWithInvalidFilterFormat() {
        DateProjection projection = new DateProjection(
                "dt", "2024-01-01,2024-01-31", "yyyy-MM-dd",
                Optional.empty(), Optional.empty());

        // Invalid date format
        List<String> values = projection.getProjectedValues(Optional.of("invalid-date"));
        assertTrue(values.isEmpty());
    }

    @Test
    public void testGetColumnType() {
        DateProjection projection = new DateProjection(
                "dt", "2024-01-01,2024-01-31", "yyyy-MM-dd",
                Optional.empty(), Optional.empty());

        assertEquals(VarcharType.VARCHAR, projection.getColumnType());
    }

    @Test
    public void testGetColumnName() {
        DateProjection projection = new DateProjection(
                "dt", "2024-01-01,2024-01-31", "yyyy-MM-dd",
                Optional.empty(), Optional.empty());

        assertEquals("dt", projection.getColumnName());
    }

    @Test
    public void testConstructorWithNullRange() {
        assertThrows(IllegalArgumentException.class, () ->
                new DateProjection("dt", null, "yyyy-MM-dd",
                        Optional.empty(), Optional.empty()));
    }

    @Test
    public void testConstructorWithEmptyRange() {
        assertThrows(IllegalArgumentException.class, () ->
                new DateProjection("dt", "", "yyyy-MM-dd",
                        Optional.empty(), Optional.empty()));
    }

    @Test
    public void testConstructorWithNullFormat() {
        assertThrows(IllegalArgumentException.class, () ->
                new DateProjection("dt", "2024-01-01,2024-01-31", null,
                        Optional.empty(), Optional.empty()));
    }

    @Test
    public void testConstructorWithInvalidFormat() {
        assertThrows(IllegalArgumentException.class, () ->
                new DateProjection("dt", "2024-01-01,2024-01-31", "invalid{format}",
                        Optional.empty(), Optional.empty()));
    }

    @Test
    public void testConstructorWithZeroInterval() {
        assertThrows(IllegalArgumentException.class, () ->
                new DateProjection("dt", "2024-01-01,2024-01-31", "yyyy-MM-dd",
                        Optional.of("0"), Optional.empty()));
    }

    @Test
    public void testConstructorWithNegativeInterval() {
        assertThrows(IllegalArgumentException.class, () ->
                new DateProjection("dt", "2024-01-01,2024-01-31", "yyyy-MM-dd",
                        Optional.of("-1"), Optional.empty()));
    }

    @Test
    public void testRequiresFilter() {
        DateProjection projection = new DateProjection(
                "dt", "2024-01-01,2024-01-31", "yyyy-MM-dd",
                Optional.empty(), Optional.empty());

        assertFalse(projection.requiresFilter());
    }

    @Test
    public void testFormatValue() {
        DateProjection projection = new DateProjection(
                "dt", "2024-01-01,2024-01-31", "yyyy-MM-dd",
                Optional.empty(), Optional.empty());

        assertEquals("2024-01-15", projection.formatValue("2024-01-15"));
        assertEquals("123", projection.formatValue(123));
    }

    @Test
    public void testGetFormatPattern() {
        DateProjection projection = new DateProjection(
                "dt", "2024-01-01,2024-01-31", "yyyy-MM-dd",
                Optional.empty(), Optional.empty());

        assertEquals("yyyy-MM-dd", projection.getFormatPattern());
    }

    @Test
    public void testInvalidRangeStartAfterEnd() {
        DateProjection projection = new DateProjection(
                "dt", "2024-01-31,2024-01-01", "yyyy-MM-dd",
                Optional.empty(), Optional.empty());

        assertThrows(IllegalArgumentException.class, () ->
                projection.getProjectedValues(Optional.empty()));
    }

    @Test
    public void testDifferentIntervalUnits() {
        // Test WEEKS
        DateProjection weekProjection = new DateProjection(
                "week", "2024-01-01,2024-01-29", "yyyy-MM-dd",
                Optional.of("1"), Optional.of("WEEKS"));

        List<String> weekValues = weekProjection.getProjectedValues(Optional.empty());
        assertEquals(5, weekValues.size());
        assertEquals("2024-01-01", weekValues.get(0));
        assertEquals("2024-01-08", weekValues.get(1));
    }

    @Test
    public void testYearInterval() {
        DateProjection projection = new DateProjection(
                "year", "2020,2024", "yyyy",
                Optional.of("1"), Optional.of("YEARS"));

        List<String> values = projection.getProjectedValues(Optional.empty());

        assertEquals(5, values.size());
        assertEquals("2020", values.get(0));
        assertEquals("2021", values.get(1));
        assertEquals("2024", values.get(4));
    }

    @Test
    public void testYearOnlyFormatInfersYearsInterval() {
        // Test that year-only format (yyyy) automatically infers YEARS interval unit
        // without explicit interval.unit property - this prevents generating duplicate values
        DateProjection projection = new DateProjection(
                "year", "2020,2024", "yyyy",
                Optional.empty(), Optional.empty());

        List<String> values = projection.getProjectedValues(Optional.empty());

        // Should generate 5 unique years, not thousands of duplicate days
        assertEquals(5, values.size());
        assertEquals("2020", values.get(0));
        assertEquals("2021", values.get(1));
        assertEquals("2022", values.get(2));
        assertEquals("2023", values.get(3));
        assertEquals("2024", values.get(4));
    }

    @Test
    public void testMonthOnlyFormatInfersMonthsInterval() {
        // Test that month-only format (yyyy-MM) automatically infers MONTHS interval unit
        // without explicit interval.unit property - this prevents generating duplicate values
        DateProjection projection = new DateProjection(
                "month", "2024-01,2024-06", "yyyy-MM",
                Optional.empty(), Optional.empty());

        List<String> values = projection.getProjectedValues(Optional.empty());

        // Should generate 6 unique months, not duplicates
        assertEquals(6, values.size());
        assertEquals("2024-01", values.get(0));
        assertEquals("2024-02", values.get(1));
        assertEquals("2024-03", values.get(2));
        assertEquals("2024-04", values.get(3));
        assertEquals("2024-05", values.get(4));
        assertEquals("2024-06", values.get(5));
    }

    @Test
    public void testYearMonthFormatWithoutDayInfersMonths() {
        // Alternative month format without separator (yyyyMM)
        DateProjection projection = new DateProjection(
                "month", "202401,202403", "yyyyMM",
                Optional.empty(), Optional.empty());

        List<String> values = projection.getProjectedValues(Optional.empty());

        assertEquals(3, values.size());
        assertEquals("202401", values.get(0));
        assertEquals("202402", values.get(1));
        assertEquals("202403", values.get(2));
    }

    @Test
    public void testCompactDateFormatInfersDays() {
        // Compact date format without separators (yyyyMMdd) should infer DAYS
        DateProjection projection = new DateProjection(
                "dt", "20240101,20240105", "yyyyMMdd",
                Optional.empty(), Optional.empty());

        List<String> values = projection.getProjectedValues(Optional.empty());

        // Should generate 5 daily values, not 1 monthly value
        assertEquals(5, values.size());
        assertEquals("20240101", values.get(0));
        assertEquals("20240102", values.get(1));
        assertEquals("20240103", values.get(2));
        assertEquals("20240104", values.get(3));
        assertEquals("20240105", values.get(4));
    }

    @Test
    public void testDayOfYearFormatInfersDays() {
        // Day-of-year format (yyyyDDD) should infer DAYS, not YEARS
        // D = day-of-year (1-366), different from d = day-of-month (1-31)
        DateProjection projection = new DateProjection(
                "dt", "2024001,2024005", "yyyyDDD",
                Optional.empty(), Optional.empty());

        List<String> values = projection.getProjectedValues(Optional.empty());

        // Should generate 5 daily values
        assertEquals(5, values.size());
        assertEquals("2024001", values.get(0));
        assertEquals("2024002", values.get(1));
        assertEquals("2024003", values.get(2));
        assertEquals("2024004", values.get(3));
        assertEquals("2024005", values.get(4));
    }
}
