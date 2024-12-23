package com.starrocks.sql.analyzer;

import java.util.HashMap;
import java.util.Map;

public class PinotFunctionSet {
    public static final String DATETRUNC = "datetrunc";
    public static final String DATETIMECONVERT = "datetimeconvert";
    public static final String TODATETIME = "todatetime";
    public static final String FROMDATETIME = "fromdatetime";

    private static final Map<String, String> formatMapping = new HashMap<>();

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
}
