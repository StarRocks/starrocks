package com.starrocks.sql.analyzer;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PinotFunctionSet {
    public static final String DATETRUNC = "datetrunc";
    public static final String DATETIMECONVERT = "datetimeconvert";
    public static final String TODATETIME = "todatetime";
    public static final String FROMDATETIME = "fromdatetime";

    private static final Map<String, String> formatMapping = new HashMap<>();

    private static final Set<String> dateFunctions = Set.of(FunctionSet.STR_TO_DATE, FunctionSet.DATE_SUB, FunctionSet.DATE_ADD, FunctionSet.TO_DATE, FunctionSet.DAYS_SUB, FunctionSet.TIMEDIFF,
            FunctionSet.DATE_TRUNC, FunctionSet.CURRENT_TIMESTAMP, FunctionSet.NOW, FunctionSet.LOCALTIME, FunctionSet.LOCALTIMESTAMP, FunctionSet.TIMESTAMPADD, FunctionSet.YEARS_ADD, FunctionSet.WEEKS_SUB,
            FunctionSet.WEEKS_ADD, FunctionSet.TIMESTAMP, FunctionSet.TIME_SLICE, FunctionSet.SECONDS_SUB, FunctionSet.SECONDS_ADD, FunctionSet.MONTHS_SUB, FunctionSet.MONTHS_ADD, FunctionSet.MINUTES_SUB, FunctionSet.MINUTES_ADD,
            FunctionSet.MICROSECONDS_SUB, FunctionSet.MICROSECONDS_ADD, FunctionSet.HOURS_SUB, FunctionSet.HOURS_ADD, FunctionSet.CONVERT_TZ, FunctionSet.ADD_MONTHS);

    static {
        formatMapping.put("yyyy", "%Y");
        formatMapping.put("MM", "%m");
        formatMapping.put("dd", "%d");
        formatMapping.put("HH", "%H");
        formatMapping.put("mm", "%i");
        formatMapping.put("ss", "%s");
        formatMapping.put("SSS", "%f");
        formatMapping.put("T", "T"); // Literal character
        formatMapping.put("Z", "");  // Ignoring timezone as no direct mapping is provided
    }

    public static String getShortenedTimeUnit(String timeUnit) {
        switch (timeUnit) {
            case "DAYS":
                return "day";
            case "HOURS":
                return "hour";
            case "MINUTES":
                return "minute";
            case "SECONDS":
                return "second";
            case "MILLISECONDS":
                return "millisecond";
            case "MICROSECONDS":
                return "microsecond";
            case "NANOSECONDS":
                return "nanosecond";
            default:
                throw new IllegalArgumentException("Invalid time unit: " + timeUnit);
        }
    }

    public static void replaceAll(StringBuilder sb, String oldStr, String newStr) {
        int index = 0;
        while ((index = sb.indexOf(oldStr, index)) != -1) {
            sb.replace(index, index + oldStr.length(), newStr);
            index += newStr.length();
        }
    }

    public static String convertDateFormat(String javaFormat) {
        StringBuilder specifiedFormat = new StringBuilder();
        int i = 0;

        while (i < javaFormat.length()) {
            boolean matched = false;

            // Check for multi-character patterns first
            for (String javaPattern : formatMapping.keySet()) {
                if (javaFormat.startsWith(javaPattern, i)) {
                    specifiedFormat.append(formatMapping.get(javaPattern));
                    i += javaPattern.length();
                    matched = true;
                    break;
                }
            }

            // If no pattern matched, append the character as is
            if (!matched) {
                specifiedFormat.append(javaFormat.charAt(i));
                i++;
            }
        }

        return specifiedFormat.toString();
    }

    public static boolean isBigIntReturned(String nodeContent, Table table) {
        if (table.containColumn(nodeContent)) {
            PrimitiveType type = table.getColumn(nodeContent).getPrimitiveType();
            return type == PrimitiveType.BIGINT;
        }

        if (isBigIntConstant(nodeContent) || isDatetimeConstant(nodeContent)) {
            return isBigIntConstant(nodeContent);
        }

        if (isFunctionString(nodeContent)) {
            return nodeContent.equalsIgnoreCase(DATETRUNC) || nodeContent.equalsIgnoreCase(DATETIMECONVERT);

        }
        return false;
    }

    public static boolean isDateTimeReturned(String nodeContent, Table table) {
        if (table.containColumn(nodeContent)) {
            PrimitiveType type = table.getColumn(nodeContent).getPrimitiveType();
            return type == PrimitiveType.DATETIME;
        }

        if (isBigIntConstant(nodeContent) || isDatetimeConstant(nodeContent)) {
            return isDatetimeConstant(nodeContent);
        }

        if (isFunctionString(nodeContent)) {
            return dateFunctions.contains(nodeContent.toLowerCase());

        }
        return false;
    }

    private static boolean isBigIntConstant(String value) {
        try {
            Long.parseLong(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isDatetimeConstant(String value) {
        return value.matches("(\\d{4}-\\d{2}-\\d{2}( \\d{2}:\\d{2}:\\d{2})?)");
    }

    private static boolean isFunctionString(String value) {
        return !isBigIntConstant(value) && !isDatetimeConstant(value) && value.matches("\\w+");
    }
}
