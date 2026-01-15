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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/TimeUtils.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.util;

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.text.ParsePosition;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TODO(dhc) add nanosecond timer for coordinator's root profile
public class TimeUtils {
    private static final Logger LOG = LogManager.getLogger(TimeUtils.class);

    public static final String DEFAULT_TIME_ZONE = "Asia/Shanghai";

    private static final ZoneId TIME_ZONE = ZoneOffset.ofTotalSeconds(8 * 3600);

    // set CST to +08:00 instead of America/Chicago
    public static final ImmutableMap<String, String> TIME_ZONE_ALIAS_MAP = ImmutableMap.of(
            "CST", DEFAULT_TIME_ZONE, "PRC", DEFAULT_TIME_ZONE);

    //Used to convert a string to a date. Compatible: 2013-2-29
    private static final DateTimeFormatter STRING_TO_DATE_FORMAT = DateTimeFormatter.ofPattern("y-M-d").withZone(TIME_ZONE);
    //Used to convert a string to a datetime. Compatible: 2013-2-28 2:3:4
    private static final DateTimeFormatter STRING_TO_DATETIME_FORMAT =
            DateTimeFormatter.ofPattern("y-M-d H:m:s").withZone(TIME_ZONE);

    //Used to convert a date to a string.
    private static final DateTimeFormatter DATE_TO_STRING_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(TIME_ZONE);
    //Used to convert a datetime to a string.
    private static final DateTimeFormatter DATETIME_TO_STRING_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);

    private static final Pattern DATETIME_FORMAT_REG =
            Pattern.compile("^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?"
                    + "((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?"
                    + "((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|("
                    + "\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))"
                    + "[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?"
                    + "((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))"
                    + "(\\s(((0?[0-9])|([1][0-9])|([2][0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$");

    // A regular expressions that will support the pattern like ('Date/DateTime TimeZone').
    // For example ('2024-09-09 11:40:40.123 Asia/Shanghai'), ('2024-09-09 Asia/Shanghai').
    public static final Pattern DATETIME_WITH_TIME_ZONE_PATTERN =
            Pattern.compile("(?<year>[-+]?\\d{4,})-(?<month>\\d{1,2})-(?<day>\\d{1,2})"
                    + "( (?:(?<hour>\\d{1,2}):(?<minute>\\d{1,2})(?::(?<second>\\d{1,2})(?:\\.(?<fraction>\\d+))?)?)?"
                    + "\\s*(?<timezone>.+)?)?");

    private static final Pattern TIMEZONE_OFFSET_FORMAT_REG = Pattern.compile("^[+-]{0,1}\\d{1,2}\\:\\d{2}$");

    public static final LocalDate MIN_DATE = LocalDate.of(0, 1, 1);
    public static final LocalDate MAX_DATE = LocalDate.of(9999, 12, 31);

    public static final LocalDateTime MIN_DATETIME = LocalDateTime.of(0, 1, 1, 0, 0, 0);
    public static final LocalDateTime MAX_DATETIME = LocalDateTime.of(9999, 12, 31, 23, 59, 59);

    // It's really hard to define max unix timestamp because of timezone.
    // so this value is 253402329599(UTC 9999-12-31 23:59:59) - 24 * 3600(for all timezones)
    public static Long MAX_UNIX_TIMESTAMP = 253402243199L;

    public static long getStartTime() {
        return System.nanoTime();
    }

    public static long getEstimatedTime(long startTime) {
        return System.nanoTime() - startTime;
    }

    public static String getCurrentFormatTime() {
        return DATETIME_TO_STRING_FORMAT.format(LocalDateTime.now());
    }

    public static TimeZone getTimeZone() {
        String timezone = getSessionTimeZone();
        return TimeZone.getTimeZone(ZoneId.of(timezone, TIME_ZONE_ALIAS_MAP));
    }

    // return the time zone of current system
    public static TimeZone getSystemTimeZone() {
        return TimeZone.getTimeZone(ZoneId.of(ZoneId.systemDefault().getId(), TIME_ZONE_ALIAS_MAP));
    }

    // Return now with system timezone
    public static LocalDateTime getSystemNow() {
        return LocalDateTime.now(getSystemTimeZone().toZoneId());
    }

    // get time zone of given zone name, or return system time zone if name is null.
    public static TimeZone getOrSystemTimeZone(String timeZone) {
        if (timeZone == null) {
            return getSystemTimeZone();
        }
        return TimeZone.getTimeZone(ZoneId.of(timeZone, TIME_ZONE_ALIAS_MAP));
    }

    /**
     * Get UNIX timestamp/Epoch second at system timezone
     */
    public static long getEpochSeconds() {
        return Clock.systemDefaultZone().instant().getEpochSecond();
    }

    public static long toEpochSeconds(LocalDateTime time) {
        return time.atZone(getSystemTimeZone().toZoneId()).toInstant().getEpochSecond();
    }

    public static String longToTimeString(long timeStamp, DateTimeFormatter dateFormat) {
        if (timeStamp <= 0L) {
            return FeConstants.NULL_STRING;
        }
        return dateFormat.format(Instant.ofEpochMilli(timeStamp));
    }

    public static String longToTimeString(long timeStamp) {
        if (timeStamp <= 0L) {
            return FeConstants.NULL_STRING;
        }
        return longToTimeString(timeStamp, DATETIME_TO_STRING_FORMAT);
    }

    public static LocalDate parseDate(String dateStr) throws AnalysisException {
        LocalDate date;
        Matcher matcher = DATETIME_FORMAT_REG.matcher(dateStr);
        if (!matcher.matches()) {
            throw new AnalysisException("Invalid date string: " + dateStr);
        }
        ParsePosition pos = new ParsePosition(0);
        try {
            date = STRING_TO_DATE_FORMAT.parse(dateStr, pos).query(LocalDate::from);
        } catch (RuntimeException e) {
            throw new AnalysisException("Invalid date string: " + dateStr);
        }
        if (pos.getIndex() != dateStr.length() || date == null) {
            throw new AnalysisException("Invalid date string: " + dateStr);
        }
        return date;
    }

    public static LocalDateTime parseDateTime(String dateTimeStr) throws AnalysisException {
        LocalDateTime dateTime;
        Matcher matcher = DATETIME_FORMAT_REG.matcher(dateTimeStr);
        if (!matcher.matches()) {
            throw new AnalysisException("Invalid date string: " + dateTimeStr);
        }
        try {
            dateTime = STRING_TO_DATETIME_FORMAT.parse(dateTimeStr).query(LocalDateTime::from);
        } catch (RuntimeException e) {
            throw new AnalysisException("Invalid date string: " + dateTimeStr);
        }
        return dateTime;
    }

    public static long timeStringToLong(String timeStr) {
        LocalDateTime dateTime;
        try {
            dateTime = STRING_TO_DATETIME_FORMAT.parse(timeStr).query(LocalDateTime::from);
        } catch (RuntimeException e) {
            return -1;
        }
        return dateTime.atZone(TIME_ZONE).toInstant().toEpochMilli();
    }

    // Check if the time zone_value is valid
    public static String checkTimeZoneValidAndStandardize(String value) throws DdlException {
        try {
            if (value == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TIME_ZONE, "null");
            }
            // match offset type, such as +08:00, -07:00
            Matcher matcher = TIMEZONE_OFFSET_FORMAT_REG.matcher(value);
            // it supports offset and region timezone type, "CST" use here is compatibility purposes.
            boolean match = matcher.matches();

            // match only check offset format timezone,
            // for others format like UTC the validation will be checked by ZoneId.of method
            if (match) {
                boolean postive = value.charAt(0) != '-';
                value = (postive ? "+" : "-") + String.format("%02d:%02d",
                        Integer.parseInt(value.replaceAll("[+-]", "").split(":")[0]),
                        Integer.parseInt(value.replaceAll("[+-]", "").split(":")[1]));

                // timezone offsets around the world extended from -12:00 to +14:00
                int tz = Integer.parseInt(value.substring(1, 3)) * 100 + Integer.parseInt(value.substring(4, 6));
                if (value.charAt(0) == '-' && tz > 1200) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TIME_ZONE, value);
                } else if (value.charAt(0) == '+' && tz > 1400) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TIME_ZONE, value);
                }
            }
            ZoneId.of(value, TIME_ZONE_ALIAS_MAP);
            return value;
        } catch (DateTimeException ex) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TIME_ZONE, value);
        }
        throw new DdlException("Parse time zone " + value + " error");
    }

    public static TimeUnit convertUnitIdentifierToTimeUnit(String unitIdentifierDescription) throws DdlException {
        switch (unitIdentifierDescription) {
            case "SECOND":
                return TimeUnit.SECONDS;
            case "MINUTE":
                return TimeUnit.MINUTES;
            case "HOUR":
                return TimeUnit.HOURS;
            case "DAY":
                return TimeUnit.DAYS;
            default:
                throw new DdlException(
                        "Can not get TimeUnit from UnitIdentifier description: " + unitIdentifierDescription);
        }
    }

    public static long convertTimeUnitValueToSecond(long value, TimeUnit unit) {
        return TimeUnit.SECONDS.convert(value, unit);
    }

    /**
     * Based on the start seconds, get the seconds closest and greater than the target second by interval,
     * the interval use period and time unit to calculate.
     *
     * @param startTimeSecond  start time second
     * @param targetTimeSecond target time second
     * @param period           period
     * @param timeUnit         time unit
     * @return next valid time second
     * @throws DdlException
     */
    public static long getNextValidTimeSecond(long startTimeSecond, long targetTimeSecond,
                                              long period, TimeUnit timeUnit) throws DdlException {
        if (startTimeSecond > targetTimeSecond) {
            return startTimeSecond;
        }
        long intervalSecond = convertTimeUnitValueToSecond(period, timeUnit);
        if (intervalSecond < 1) {
            throw new DdlException("Can not get next valid time second," +
                    "startTimeSecond:" + startTimeSecond +
                    " period:" + period +
                    " timeUnit:" + timeUnit);
        }
        long difference = targetTimeSecond - startTimeSecond;
        long step = difference / intervalSecond + 1;
        return startTimeSecond + step * intervalSecond;
    }

    public static PeriodDuration parseHumanReadablePeriodOrDuration(String text) {
        try {
            return PeriodDuration.of(PeriodStyle.LONG.parse(text));
        } catch (DateTimeParseException ignored) {
            return PeriodDuration.of(DurationStyle.LONG.parse(text));
        }
    }

    public static String toHumanReadableString(PeriodDuration periodDuration) {
        if (periodDuration.getPeriod().isZero()) {
            return DurationStyle.LONG.toString(periodDuration.getDuration());
        } else if (periodDuration.getDuration().isZero()) {
            return PeriodStyle.LONG.toString(periodDuration.getPeriod());
        } else {
            return PeriodStyle.LONG.toString(periodDuration.getPeriod()) + " "
                    + DurationStyle.LONG.toString(periodDuration.getDuration());
        }
    }

    public static String getSessionTimeZone() {
        String timezone;
        if (ConnectContext.get() != null) {
            timezone = ConnectContext.get().getSessionVariable().getTimeZone();
        } else {
            timezone = GlobalStateMgr.getCurrentState().getVariableMgr().getDefaultSessionVariable().getTimeZone();
        }
        return timezone;
    }

    /**
     * Parse the time zone of the given timestampLiteral
     * using DATETIME_WITH_TIME_ZONE_PATTERN regular expressions
     *
     * @param value the value of the timestampLiteral
     * @return `null` or zone id of the parsed timezone
     */
    public static ZoneId parseTimeZoneFromString(String value) {
        Matcher matcher = DATETIME_WITH_TIME_ZONE_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid TIMESTAMP:" + value);
        }
        String timeZone = matcher.group("timezone");
        return timeZone == null ? null : ZoneId.of(timeZone);
    }

    /**
     * Parse the date or dateTime string of the given timestampLiteral
     * using DATETIME_WITH_TIME_ZONE_PATTERN regular expressions
     *
     * @param value the value of the timestampLiteral
     * @return the date or dateTime string
     */
    public static String parseDateTimeFromString(String value) {
        Matcher matcher = DATETIME_WITH_TIME_ZONE_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid TIMESTAMP:" + value);
        }
        int timezoneStart = matcher.start("timezone");
        if (timezoneStart != -1) {
            return value.substring(0, timezoneStart).trim();
        }
        return value;
    }
}
