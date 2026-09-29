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

import com.google.common.collect.ImmutableSet;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr.TimeUnit;
import com.starrocks.type.Type;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.WeekFields;
import java.util.Locale;

/**
 * Aligns and renders the time values a date range partition is built from: its bounds and its name.
 *
 * <p>Every formatter here is a {@code *_FORMATTER_UNIX} one, built on the proleptic
 * {@code ChronoField.YEAR}.
 */
public class PartitionTimeUtils {

    /** Time units batch DDL accepts. */
    public static final ImmutableSet<TimeUnit> BATCH_PARTITION_TIME_UNITS = ImmutableSet.of(
            TimeUnit.HOUR,
            TimeUnit.DAY,
            TimeUnit.WEEK,
            TimeUnit.MONTH,
            TimeUnit.YEAR
    );

    /** Time units automatic partitioning accepts. */
    public static final ImmutableSet<TimeUnit> AUTO_PARTITION_TIME_UNITS = ImmutableSet.of(
            TimeUnit.MINUTE,
            TimeUnit.HOUR,
            TimeUnit.DAY,
            TimeUnit.MONTH,
            TimeUnit.YEAR
    );

    private PartitionTimeUtils() {
    }

    /** Formatter for a bound of a date or datetime partition column. */
    public static DateTimeFormatter getPartitionBoundFormatter(Type partitionColumnType) {
        if (partitionColumnType.isDatetime()) {
            return DateUtils.DATE_TIME_FORMATTER_UNIX;
        }
        return DateUtils.DATE_FORMATTER_UNIX;
    }

    /** Renders {@code time} as a partition bound. */
    public static String formatPartitionBound(LocalDateTime time, Type partitionColumnType) {
        return time.format(getPartitionBoundFormatter(partitionColumnType));
    }

    /**
     * Formatter for the name suffix of {@code timeUnit}, e.g. {@code YEAR -> "0000"}.
     * {@link TimeUnit#WEEK} has no single formatter; use {@link #formatPartitionNameSuffix}.
     */
    public static DateTimeFormatter getPartitionNameFormatter(TimeUnit timeUnit) {
        switch (timeUnit) {
            case MINUTE:
                return DateUtils.MINUTE_FORMATTER_UNIX;
            case HOUR:
                return DateUtils.HOUR_FORMATTER_UNIX;
            case DAY:
                return DateUtils.DATEKEY_FORMATTER_UNIX;
            case MONTH:
                return DateUtils.MONTH_FORMATTER_UNIX;
            case YEAR:
                return DateUtils.YEAR_FORMATTER_UNIX;
            default:
                throw new SemanticException("Partition name is not defined for time unit: " + timeUnit);
        }
    }

    /** Renders the name suffix of the partition starting at {@code time}, which must already be aligned. */
    public static String formatPartitionNameSuffix(LocalDateTime time, TimeUnit timeUnit) {
        if (timeUnit == TimeUnit.WEEK) {
            return formatWeekNameSuffix(time);
        }
        return time.format(getPartitionNameFormatter(timeUnit));
    }

    /**
     * Aligns {@code time} down to the start of its time unit, keeping the time of day for
     * {@code WEEK}, {@code MONTH} and {@code YEAR}, since batch DDL allows an unaligned START.
     * {@code dayOfWeek} / {@code dayOfMonth} follow the {@code dynamic_partition.start_day_of_week}
     * / {@code .start_day_of_month} properties.
     */
    public static LocalDateTime alignToUnitStart(LocalDateTime time, TimeUnit timeUnit, int dayOfWeek, int dayOfMonth) {
        switch (timeUnit) {
            case HOUR:
                return time.withMinute(0).withSecond(0).withNano(0);
            case DAY:
                return time.withHour(0).withMinute(0).withSecond(0).withNano(0);
            case WEEK:
                return time.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek)));
            case MONTH:
                return time.withDayOfMonth(dayOfMonth);
            case YEAR:
                return time.withDayOfYear(1);
            default:
                throw new SemanticException("Batch build partition does not support time interval type: " + timeUnit);
        }
    }

    /**
     * Truncates {@code time} down to the exact start of its time unit, clearing every smaller
     * field, as an automatically created partition always begins there.
     */
    public static LocalDateTime truncateToUnitStart(LocalDateTime time, TimeUnit timeUnit) {
        switch (timeUnit) {
            case MINUTE:
                return time.withSecond(0).withNano(0);
            case HOUR:
                return time.withMinute(0).withSecond(0).withNano(0);
            case DAY:
                return time.withHour(0).withMinute(0).withSecond(0).withNano(0);
            case MONTH:
                return time.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
            case YEAR:
                return time.withDayOfYear(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
            default:
                throw new SemanticException("Unsupported automatic partition granularity: " + timeUnit);
        }
    }

    /** Moves {@code time} forward by {@code interval} units, to the start of the next partition. */
    public static LocalDateTime plus(LocalDateTime time, TimeUnit timeUnit, long interval) {
        switch (timeUnit) {
            case MINUTE:
                return time.plusMinutes(interval);
            case HOUR:
                return time.plusHours(interval);
            case DAY:
                return time.plusDays(interval);
            case WEEK:
                return time.plusWeeks(interval);
            case MONTH:
                return time.plusMonths(interval);
            case YEAR:
                return time.plusYears(interval);
            default:
                throw new SemanticException("Partition interval is not defined for time unit: " + timeUnit);
        }
    }

    /**
     * Week partitions are named after the week of year of the JVM locale, the same rules
     * {@link java.util.Calendar} applies, so batch and dynamic partitioning name the same week
     * identically. The week is read off the proleptic date: passing year 0000 through
     * {@code GregorianCalendar} would resolve it as 1 BCE on the Julian calendar, whose weeks are
     * two days off, and the resulting week number is one short under the locales whose week starts
     * on Sunday.
     */
    private static String formatWeekNameSuffix(LocalDateTime time) {
        if (time.getYear() < 0) {
            // The week holding START begins before 0000-01-01, so it has no name in range: aligning
            // 0000-01-01 back to a Monday, say, lands on -0001-12-27.
            throw new SemanticException("Batch build partition can not create a week partition whose " +
                    "week begins before 0000-01-01, which is outside the DATE range. " +
                    "Move START forward to the first day of a week.");
        }
        WeekFields weekFields = WeekFields.of(Locale.getDefault(Locale.Category.FORMAT));
        int weekOfYear = time.get(weekFields.weekOfWeekBasedYear());
        if (weekOfYear <= 1 && time.getMonthValue() == 12) {
            // eg: JDK think 2019-12-30 as the first week of year 2020, we need to handle this.
            // to make it as the 53rd week of year 2019.
            weekOfYear += 52;
        }
        // The year is formatted rather than printed as an int so that it is padded to four digits
        // like every other unit.
        return String.format("%s_%02d", time.format(DateUtils.YEAR_FORMATTER_UNIX), weekOfYear);
    }
}
