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

import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Partition projection for date/time ranges.
 *
 * The date projection type defines a range of dates with a format and interval.
 * Supports both absolute dates and relative expressions (NOW, NOW-3DAYS, etc.).
 *
 * Example table properties:
 * <pre>
 *   'projection.dt.type' = 'date',
 *   'projection.dt.range' = '2024-01-01,NOW',
 *   'projection.dt.format' = 'yyyy-MM-dd',
 *   'projection.dt.interval' = '1',
 *   'projection.dt.interval.unit' = 'DAYS'
 * </pre>
 *
 * Supported interval units: YEARS, MONTHS, WEEKS, DAYS, HOURS, MINUTES, SECONDS
 */
public class DateProjection implements ColumnProjection {

    // Maximum number of values to generate to prevent memory issues
    private static final int MAX_VALUES = 100000;

    // Pattern for relative date expressions: NOW, NOW-3DAYS, NOW+1MONTH, etc.
    private static final Pattern RELATIVE_DATE_PATTERN = Pattern.compile(
            "\\s*NOW\\s*(?:([-+])\\s*(\\d+)\\s*(YEARS?|MONTHS?|WEEKS?|DAYS?|HOURS?|MINUTES?|SECONDS?)\\s*)?",
            Pattern.CASE_INSENSITIVE);

    private final String columnName;
    private final String rangeProperty;
    private final DateTimeFormatter dateFormat;
    private final String formatPattern;
    private final long interval;
    private final ChronoUnit intervalUnit;

    /**
     * Creates a date projection for a partition column.
     *
     * @param columnName the partition column name
     * @param rangeProperty range as "start,end" where each can be a date or NOW expression
     * @param formatProperty date format pattern (e.g., "yyyy-MM-dd")
     * @param intervalProperty optional interval between values (default: 1)
     * @param intervalUnitProperty optional interval unit (default: inferred from format)
     */
    public DateProjection(String columnName, String rangeProperty, String formatProperty,
                          Optional<String> intervalProperty,
                          Optional<String> intervalUnitProperty) {
        if (rangeProperty == null || rangeProperty.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Date projection for column '" + columnName + "' requires a range property");
        }
        if (formatProperty == null || formatProperty.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Date projection for column '" + columnName + "' requires a format property");
        }

        this.columnName = columnName;
        this.rangeProperty = rangeProperty;
        this.formatPattern = formatProperty;

        try {
            this.dateFormat = DateTimeFormatter.ofPattern(formatProperty, Locale.ENGLISH);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid date format for column '" + columnName + "': " + formatProperty, e);
        }

        // Parse interval (default: 1)
        this.interval = intervalProperty
                .map(s -> {
                    try {
                        long val = Long.parseLong(s.trim());
                        if (val <= 0) {
                            throw new IllegalArgumentException(
                                    "Date projection interval must be positive, got: " + val);
                        }
                        return val;
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(
                                "Invalid interval value for column '" + columnName + "': " + s, e);
                    }
                })
                .orElse(1L);

        // Parse interval unit (default: inferred from format)
        this.intervalUnit = intervalUnitProperty
                .map(this::parseIntervalUnit)
                .orElseGet(() -> inferIntervalUnit(formatProperty));
    }

    private ChronoUnit parseIntervalUnit(String unit) {
        if (unit == null) {
            return ChronoUnit.DAYS;
        }
        // Normalize: remove trailing 'S' and convert to uppercase
        String normalized = unit.toUpperCase().replaceAll("S$", "");
        switch (normalized) {
            case "YEAR":
                return ChronoUnit.YEARS;
            case "MONTH":
                return ChronoUnit.MONTHS;
            case "WEEK":
                return ChronoUnit.WEEKS;
            case "DAY":
                return ChronoUnit.DAYS;
            case "HOUR":
                return ChronoUnit.HOURS;
            case "MINUTE":
                return ChronoUnit.MINUTES;
            case "SECOND":
                return ChronoUnit.SECONDS;
            default:
                throw new IllegalArgumentException("Unknown interval unit: " + unit);
        }
    }

    private ChronoUnit inferIntervalUnit(String format) {
        // If format includes time components, default to hours
        if (format.contains("HH") || format.contains("hh") ||
                format.contains("mm") || format.contains("ss")) {
            return ChronoUnit.HOURS;
        }
        return ChronoUnit.DAYS;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public List<String> getProjectedValues(Optional<Object> filterValue) {
        // Parse bounds at query time to handle NOW expressions correctly
        Instant now = Instant.now();
        Instant leftBound = parseDate(getRangeStart(), now);
        Instant rightBound = parseDate(getRangeEnd(), now);

        if (filterValue.isPresent()) {
            // Validate filter value is within range
            String filter = filterValue.get().toString();
            try {
                Instant filterInstant = parseDate(filter, now);
                if (filterInstant.isBefore(leftBound) || filterInstant.isAfter(rightBound)) {
                    // Filter value is outside the range - return empty list
                    return Collections.emptyList();
                }
                return Collections.singletonList(filter);
            } catch (IllegalArgumentException e) {
                // Invalid date format - return empty list
                return Collections.emptyList();
            }
        }

        if (leftBound.isAfter(rightBound)) {
            throw new IllegalArgumentException(
                    "Date projection for column '" + columnName +
                            "' has invalid range: start is after end");
        }

        // Generate all values in range
        List<String> result = new ArrayList<>();
        Instant current = leftBound;
        int count = 0;

        while (!current.isAfter(rightBound) && count < MAX_VALUES) {
            result.add(formatInstant(current));
            current = plus(current, interval, intervalUnit);
            count++;
        }

        if (count >= MAX_VALUES) {
            throw new IllegalArgumentException(
                    "Date projection for column '" + columnName +
                            "' would generate too many values (limit: " + MAX_VALUES + ")");
        }

        return result;
    }

    private String getRangeStart() {
        String[] parts = rangeProperty.split(",");
        return parts[0].trim();
    }

    private String getRangeEnd() {
        String[] parts = rangeProperty.split(",");
        if (parts.length < 2) {
            throw new IllegalArgumentException(
                    "Date projection range must be in format 'start,end', got: " + rangeProperty);
        }
        return parts[1].trim();
    }

    private Instant parseDate(String dateStr, Instant now) {
        Matcher matcher = RELATIVE_DATE_PATTERN.matcher(dateStr);
        if (matcher.matches()) {
            // Relative date (NOW, NOW-3DAYS, etc.)
            if (matcher.group(1) == null) {
                // Just "NOW"
                return now;
            }
            String sign = matcher.group(1);
            long amount = Long.parseLong(matcher.group(2));
            ChronoUnit unit = parseIntervalUnit(matcher.group(3));

            if ("-".equals(sign)) {
                return minus(now, amount, unit);
            } else {
                return plus(now, amount, unit);
            }
        }

        // Absolute date - parse with the configured format
        // Try different temporal types based on format pattern
        try {
            // Try parsing as LocalDateTime first (full date + time)
            LocalDateTime dateTime = LocalDateTime.parse(dateStr, dateFormat);
            return dateTime.atZone(ZoneId.systemDefault()).toInstant();
        } catch (DateTimeParseException e1) {
            try {
                // Try parsing as LocalDate (year-month-day)
                LocalDate date = LocalDate.parse(dateStr, dateFormat);
                return date.atStartOfDay(ZoneId.systemDefault()).toInstant();
            } catch (DateTimeParseException e2) {
                try {
                    // Try parsing as YearMonth (year-month only, e.g., '2024-01')
                    YearMonth yearMonth = YearMonth.parse(dateStr, dateFormat);
                    return yearMonth.atDay(1).atStartOfDay(ZoneId.systemDefault()).toInstant();
                } catch (DateTimeParseException e3) {
                    try {
                        // Try parsing as Year (year only, e.g., '2024')
                        Year year = Year.parse(dateStr, dateFormat);
                        return year.atDay(1).atStartOfDay(ZoneId.systemDefault()).toInstant();
                    } catch (DateTimeParseException e4) {
                        throw new IllegalArgumentException(
                                "Cannot parse date '" + dateStr + "' with format '" + formatPattern + "'", e4);
                    }
                }
            }
        }
    }

    private Instant plus(Instant instant, long amount, ChronoUnit unit) {
        // For date-based units, we need to work with LocalDateTime
        if (unit == ChronoUnit.YEARS || unit == ChronoUnit.MONTHS || unit == ChronoUnit.WEEKS) {
            LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            switch (unit) {
                case YEARS:
                    ldt = ldt.plusYears(amount);
                    break;
                case MONTHS:
                    ldt = ldt.plusMonths(amount);
                    break;
                case WEEKS:
                    ldt = ldt.plusWeeks(amount);
                    break;
                default:
                    break;
            }
            return ldt.atZone(ZoneId.systemDefault()).toInstant();
        }
        return instant.plus(amount, unit);
    }

    private Instant minus(Instant instant, long amount, ChronoUnit unit) {
        // For date-based units, we need to work with LocalDateTime
        if (unit == ChronoUnit.YEARS || unit == ChronoUnit.MONTHS || unit == ChronoUnit.WEEKS) {
            LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            switch (unit) {
                case YEARS:
                    ldt = ldt.minusYears(amount);
                    break;
                case MONTHS:
                    ldt = ldt.minusMonths(amount);
                    break;
                case WEEKS:
                    ldt = ldt.minusWeeks(amount);
                    break;
                default:
                    break;
            }
            return ldt.atZone(ZoneId.systemDefault()).toInstant();
        }
        return instant.minus(amount, unit);
    }

    private String formatInstant(Instant instant) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        return dateFormat.format(dateTime);
    }

    @Override
    public String formatValue(Object value) {
        return value.toString();
    }

    @Override
    public Type getColumnType() {
        return VarcharType.VARCHAR;
    }

    public String getFormatPattern() {
        return formatPattern;
    }

    public long getInterval() {
        return interval;
    }

    public ChronoUnit getIntervalUnit() {
        return intervalUnit;
    }

    @Override
    public String toString() {
        return "DateProjection{" +
                "columnName='" + columnName + '\'' +
                ", rangeProperty='" + rangeProperty + '\'' +
                ", formatPattern='" + formatPattern + '\'' +
                ", interval=" + interval +
                ", intervalUnit=" + intervalUnit +
                '}';
    }
}
