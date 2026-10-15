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

package com.starrocks.common.util;

import com.starrocks.analysis.TimestampArithmeticExpr.TimeUnit;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.WeekFields;
import java.util.List;
import java.util.Locale;

public class PartitionTimeUtilsTest {

    @Test
    public void testFormatPartitionBoundKeepsYearZero() {
        LocalDateTime yearZero = LocalDateTime.of(0, 1, 1, 0, 0, 0);
        Assertions.assertEquals("0000-01-01", PartitionTimeUtils.formatPartitionBound(yearZero, Type.DATE));
        Assertions.assertEquals("0000-01-01 00:00:00",
                PartitionTimeUtils.formatPartitionBound(yearZero, Type.DATETIME));
    }

    @Test
    public void testFormatPartitionBound() {
        LocalDateTime time = LocalDateTime.of(2024, 3, 5, 10, 30, 15);
        Assertions.assertEquals("2024-03-05", PartitionTimeUtils.formatPartitionBound(time, Type.DATE));
        Assertions.assertEquals("2024-03-05 10:30:15", PartitionTimeUtils.formatPartitionBound(time, Type.DATETIME));
    }

    @Test
    public void testFormatPartitionNameSuffixKeepsYearZero() {
        LocalDateTime yearZero = LocalDateTime.of(0, 1, 1, 0, 0, 0);
        Assertions.assertEquals("0000", PartitionTimeUtils.formatPartitionNameSuffix(yearZero, TimeUnit.YEAR));
        Assertions.assertEquals("000001", PartitionTimeUtils.formatPartitionNameSuffix(yearZero, TimeUnit.MONTH));
        Assertions.assertEquals("00000101", PartitionTimeUtils.formatPartitionNameSuffix(yearZero, TimeUnit.DAY));
        Assertions.assertEquals("0000010100", PartitionTimeUtils.formatPartitionNameSuffix(yearZero, TimeUnit.HOUR));
        Assertions.assertEquals("000001010000",
                PartitionTimeUtils.formatPartitionNameSuffix(yearZero, TimeUnit.MINUTE));
    }

    @Test
    public void testFormatPartitionNameSuffix() {
        LocalDateTime time = LocalDateTime.of(2024, 3, 5, 10, 30, 15);
        Assertions.assertEquals("2024", PartitionTimeUtils.formatPartitionNameSuffix(time, TimeUnit.YEAR));
        Assertions.assertEquals("202403", PartitionTimeUtils.formatPartitionNameSuffix(time, TimeUnit.MONTH));
        Assertions.assertEquals("20240305", PartitionTimeUtils.formatPartitionNameSuffix(time, TimeUnit.DAY));
        Assertions.assertEquals("2024030510", PartitionTimeUtils.formatPartitionNameSuffix(time, TimeUnit.HOUR));
        Assertions.assertEquals("202403051030", PartitionTimeUtils.formatPartitionNameSuffix(time, TimeUnit.MINUTE));
    }

    @Test
    public void testFormatWeekPartitionNameSuffix() {
        // Calendar puts 2019-12-30 in the first week of 2020; we keep naming it week 53 of 2019.
        LocalDateTime lastWeekOf2019 = LocalDateTime.of(2019, 12, 30, 0, 0, 0);
        Assertions.assertEquals("2019_53",
                PartitionTimeUtils.formatPartitionNameSuffix(lastWeekOf2019, TimeUnit.WEEK));
    }

    @Test
    public void testFormatWeekPartitionNameSuffixOnYearZero() {
        // The expected week number is the locale's own week rules read off the proleptic date.
        // Resolving year 0000 through GregorianCalendar instead lands on 1 BCE in the Julian
        // calendar, two days off, which shifts the week under every locale whose week starts on
        // Sunday. The year must also be padded to four digits like every other unit.
        WeekFields weekFields = WeekFields.of(Locale.getDefault(Locale.Category.FORMAT));
        for (LocalDate monday : List.of(LocalDate.of(0, 1, 3), LocalDate.of(0, 1, 10),
                LocalDate.of(0, 3, 6), LocalDate.of(0, 6, 5))) {
            Assertions.assertEquals(DayOfWeek.MONDAY, monday.getDayOfWeek(), monday.toString());
            String suffix = PartitionTimeUtils.formatPartitionNameSuffix(monday.atStartOfDay(), TimeUnit.WEEK);
            Assertions.assertTrue(suffix.startsWith("0000_"), suffix);
            Assertions.assertEquals(String.format("0000_%02d", monday.get(weekFields.weekOfWeekBasedYear())), suffix);
        }
        // Years below 1000 are padded too, matching what the YEAR unit produces for the same year.
        Assertions.assertEquals("0001",
                PartitionTimeUtils.formatPartitionNameSuffix(LocalDateTime.of(1, 1, 1, 0, 0, 0), TimeUnit.YEAR));
        Assertions.assertTrue(
                PartitionTimeUtils.formatPartitionNameSuffix(LocalDateTime.of(1, 1, 1, 0, 0, 0), TimeUnit.WEEK)
                        .startsWith("0001_"));
    }

    @Test
    public void testFormatWeekPartitionNameSuffixRejectsWeekBeforeMinDate() {
        // Aligning 0000-01-01 (a Saturday) back to Monday lands on -0001-12-27, which no DATE can
        // hold; naming it would yield the invalid partition name p-1_52.
        LocalDateTime beforeMinDate = LocalDateTime.of(0, 1, 1, 0, 0, 0)
                .with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
        Assertions.assertEquals(-1, beforeMinDate.getYear());
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> PartitionTimeUtils.formatPartitionNameSuffix(beforeMinDate, TimeUnit.WEEK));
        Assertions.assertTrue(e.getMessage().contains("week begins before 0000-01-01"), e.getMessage());
    }

    @Test
    public void testAlignToUnitStart() {
        LocalDateTime time = LocalDateTime.of(2024, 3, 5, 10, 30, 15);
        Assertions.assertEquals(LocalDateTime.of(2024, 3, 5, 10, 0, 0),
                PartitionTimeUtils.alignToUnitStart(time, TimeUnit.HOUR, 1, 1));
        Assertions.assertEquals(LocalDateTime.of(2024, 3, 5, 0, 0, 0),
                PartitionTimeUtils.alignToUnitStart(time, TimeUnit.DAY, 1, 1));
        // 2024-03-05 is a Tuesday; WEEK / MONTH / YEAR keep the time of day.
        Assertions.assertEquals(LocalDateTime.of(2024, 3, 4, 10, 30, 15),
                PartitionTimeUtils.alignToUnitStart(time, TimeUnit.WEEK, 1, 1));
        Assertions.assertEquals(LocalDateTime.of(2024, 3, 1, 10, 30, 15),
                PartitionTimeUtils.alignToUnitStart(time, TimeUnit.MONTH, 1, 1));
        Assertions.assertEquals(LocalDateTime.of(2024, 3, 15, 10, 30, 15),
                PartitionTimeUtils.alignToUnitStart(time, TimeUnit.MONTH, 1, 15));
        Assertions.assertEquals(LocalDateTime.of(2024, 1, 1, 10, 30, 15),
                PartitionTimeUtils.alignToUnitStart(time, TimeUnit.YEAR, 1, 1));
        Assertions.assertThrows(SemanticException.class,
                () -> PartitionTimeUtils.alignToUnitStart(time, TimeUnit.MINUTE, 1, 1));
    }

    @Test
    public void testTruncateToUnitStart() {
        LocalDateTime time = LocalDateTime.of(2024, 3, 5, 10, 30, 15);
        Assertions.assertEquals(LocalDateTime.of(2024, 3, 5, 10, 30, 0),
                PartitionTimeUtils.truncateToUnitStart(time, TimeUnit.MINUTE));
        Assertions.assertEquals(LocalDateTime.of(2024, 3, 5, 10, 0, 0),
                PartitionTimeUtils.truncateToUnitStart(time, TimeUnit.HOUR));
        Assertions.assertEquals(LocalDateTime.of(2024, 3, 5, 0, 0, 0),
                PartitionTimeUtils.truncateToUnitStart(time, TimeUnit.DAY));
        // Unlike alignToUnitStart, this also clears the time of day.
        Assertions.assertEquals(LocalDateTime.of(2024, 3, 1, 0, 0, 0),
                PartitionTimeUtils.truncateToUnitStart(time, TimeUnit.MONTH));
        Assertions.assertEquals(LocalDateTime.of(2024, 1, 1, 0, 0, 0),
                PartitionTimeUtils.truncateToUnitStart(time, TimeUnit.YEAR));
        Assertions.assertThrows(SemanticException.class,
                () -> PartitionTimeUtils.truncateToUnitStart(time, TimeUnit.WEEK));
    }

    @Test
    public void testPlus() {
        LocalDateTime time = LocalDateTime.of(0, 1, 1, 0, 0, 0);
        Assertions.assertEquals(LocalDateTime.of(0, 1, 1, 0, 1, 0), PartitionTimeUtils.plus(time, TimeUnit.MINUTE, 1));
        Assertions.assertEquals(LocalDateTime.of(0, 1, 1, 1, 0, 0), PartitionTimeUtils.plus(time, TimeUnit.HOUR, 1));
        Assertions.assertEquals(LocalDateTime.of(0, 1, 2, 0, 0, 0), PartitionTimeUtils.plus(time, TimeUnit.DAY, 1));
        Assertions.assertEquals(LocalDateTime.of(0, 1, 8, 0, 0, 0), PartitionTimeUtils.plus(time, TimeUnit.WEEK, 1));
        Assertions.assertEquals(LocalDateTime.of(0, 2, 1, 0, 0, 0), PartitionTimeUtils.plus(time, TimeUnit.MONTH, 1));
        Assertions.assertEquals(LocalDateTime.of(1, 1, 1, 0, 0, 0), PartitionTimeUtils.plus(time, TimeUnit.YEAR, 1));
        Assertions.assertThrows(SemanticException.class, () -> PartitionTimeUtils.plus(time, TimeUnit.SECOND, 1));
    }
}
