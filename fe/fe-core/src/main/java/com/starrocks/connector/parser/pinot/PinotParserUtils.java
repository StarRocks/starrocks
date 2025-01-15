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
package com.starrocks.connector.parser.pinot;

import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.util.DateUtils;

import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PinotParserUtils {

    /**
     * Functions that return DATETIME as the result type
     */
    private static Set<String> DATETIME_RETURNING_FUNCTIONS = new HashSet<>();
    private static final Set<String> JAVA_DATE_FORMATS = createDateFormatSet();
    private static final Map<String, String> UNIT_MAPPING = createUnitMapping();

    static {
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.TIMESTAMPADD);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.DATE_TRUNC);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.STR_TO_DATE);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.DATE_ADD);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.DATE_SUB);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.DAYS_ADD);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.DAYS_SUB);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.CURRENT_TIMESTAMP);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.YEARS_ADD);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.YEARS_SUB);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.NOW);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.WEEKS_ADD);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.WEEKS_SUB);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.TIMESTAMP);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.TIME_SLICE);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.SECONDS_ADD);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.SECONDS_SUB);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.MONTHS_ADD);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.MONTHS_SUB);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.MINUTES_ADD);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.MINUTES_SUB);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.MICROSECONDS_ADD);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.MICROSECONDS_SUB);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.HOURS_ADD);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.HOURS_SUB);
        DATETIME_RETURNING_FUNCTIONS.add(FunctionSet.CONVERT_TZ);
    }

    private static Set<String> createDateFormatSet() {
        Set<String> formats = new HashSet<>();
        formats.add("yyyy");
        formats.add("yy");
        formats.add("MM");
        formats.add("M");
        formats.add("dd");
        formats.add("d");
        formats.add("HH");
        formats.add("H");
        formats.add("hh");
        formats.add("h");
        formats.add("mm");
        formats.add("ss");
        formats.add("SSS");
        formats.add("a");
        formats.add("E");
        formats.add("MMM");
        formats.add("MMMM");
        return Collections.unmodifiableSet(formats);
    }

    private static Map<String, String> createUnitMapping() {
        Map<String, String> mapping = new HashMap<>();
        mapping.put("DAYS", "DAY");
        mapping.put("HOURS", "HOUR");
        mapping.put("MINUTES", "MINUTE");
        mapping.put("SECONDS", "SECOND");
        mapping.put("MILLISECONDS", "MILLISECOND");
        mapping.put("MICROSECONDS", "MICROSECOND");
        mapping.put("NANOSECONDS", "NANOSECOND");
        return mapping;
    }

    public static boolean isDateTimeType(Expr expr) {
        // type of expr could be Type.INVALID till now, hence we need to examine many other possible cases
        if (expr.getType().isDatetime()) {
            return true;
        } else if (expr instanceof FunctionCallExpr) {
            return DATETIME_RETURNING_FUNCTIONS.contains(((FunctionCallExpr) expr).getFnName().getFunction().toLowerCase());
        } else if (expr instanceof CastExpr) {
            return ((CastExpr) expr).getTargetTypeDef().getType().isDatetime();
        } else if (expr instanceof com.starrocks.analysis.StringLiteral) {
            String literal = ((StringLiteral) expr).getStringValue();
            try {
                DateUtils.DATE_TIME_FORMATTER_UNIX.parse(literal);
            } catch (DateTimeParseException e) {
                try {
                    DateUtils.DATE_TIME_MS_FORMATTER_UNIX.parse(literal);
                } catch (DateTimeParseException eMs) {
                    return false;
                }
                return true;
            }
            return true;
        }
        return false;
    }

    public static boolean isJavaDateFormat(String format) {
        for (String javaFormat : JAVA_DATE_FORMATS) {
            if (format.contains(javaFormat)) {
                return true;
            }
        }
        return false;
    }

    public static String convertToStrftimeFormat(String javaDateFormat) {
        String cleanedFormat = javaDateFormat.replace("Z", "").replace("'", "");

        Map<String, String> formatMapping = new LinkedHashMap<>();
        formatMapping.put("yyyy", "%Y");
        formatMapping.put("MM", "%m");
        formatMapping.put("dd", "%d");
        formatMapping.put("HH", "%H");
        formatMapping.put("mm", "%i");
        formatMapping.put("ss", "%S");
        formatMapping.put("SSS", "%f");
        formatMapping.put("yy", "%y");
        formatMapping.put("hh", "%I");
        formatMapping.put("h", "%I");
        formatMapping.put("a", "%p");
        formatMapping.put("E", "%a");
        formatMapping.put("MMM", "%b");
        formatMapping.put("MMMM", "%B");

        String strftimeFormat = cleanedFormat;
        for (Map.Entry<String, String> entry : formatMapping.entrySet()) {
            strftimeFormat = strftimeFormat.replace(entry.getKey(), entry.getValue());
        }
        return strftimeFormat;
    }

    public static List<String> parseTime(String input) {
        String[] parts = input.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid input format. Expected <time size>:<time unit>");
        }

        String timeSize = parts[0].trim();
        String timeUnit = parts[1].trim().toUpperCase();

        if (UNIT_MAPPING.containsKey(timeUnit)) {
            timeUnit = UNIT_MAPPING.get(timeUnit);
        } else {
            throw new IllegalArgumentException("Invalid time unit: " + timeUnit);
        }

        List<String> result = new ArrayList<>();
        result.add(timeSize);
        result.add(timeUnit);
        return result;
    }

    public static List<String> parseFormat(String format) {
        String[] parts = format.split(":", 4);
        if (parts.length < 3) {
            throw new IllegalArgumentException("Invalid input format. Expected <time size>:<time unit>:<time format>:<pattern>");
        }

        String timeSize = parts[0].trim();
        String timeUnit = parts[1].trim().toUpperCase(Locale.ROOT);
        String timeFormat = parts[2].trim().toUpperCase(Locale.ROOT);
        String pattern = parts.length == 4 ? parts[3] : null;

        List<String> result = new ArrayList<>();
        result.add(timeSize);
        result.add(timeUnit);
        result.add(timeFormat);
        result.add(pattern);
        return result;
    }

    public static double getMultiplier(String outputTimeUnitStr) {
        switch (outputTimeUnitStr.toUpperCase()) {
            case "NANOSECONDS":
                return 1000000000.0;
            case "MICROSECONDS":
                return 1000000.0;
            case "MILLISECONDS":
                return 1000.0;
            case "SECONDS":
                return 1.0;
            case "MINUTES":
                return 1.0 / 60;
            case "HOURS":
                return 1.0 / 3600;
            case "DAYS":
                return 1.0 / 86400;
            default:
                throw new IllegalArgumentException("Invalid outputTimeUnitStr: " + outputTimeUnitStr);
        }
    }

    public static String[] parseDateFormat(String pattern) {
        String regex = "^(.*?)( tz\\((.*?)\\))?$";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(pattern);

        String dateFormat = null;
        String timeZone = null;

        if (m.matches()) {
            dateFormat = m.group(1);
            timeZone = m.group(3);
        }

        return new String[]{dateFormat, timeZone};
    }

}
