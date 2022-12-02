// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.common.config;

import java.util.Map;

public class PropertyUtil {

    private PropertyUtil() {}

    public static boolean propertyAsBoolean(
            Map<String, String> properties, String property, boolean defaultValue) {
        String value = properties.get(property);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return defaultValue;
    }

    public static Boolean propertyAsNullableBoolean(Map<String, String> properties, String property) {
        String value = properties.get(property);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return null;
    }

    public static double propertyAsDouble(
            Map<String, String> properties, String property, double defaultValue) {
        String value = properties.get(property);
        if (value != null) {
            return Double.parseDouble(value);
        }
        return defaultValue;
    }

    public static int propertyAsInt(
            Map<String, String> properties, String property, int defaultValue) {
        String value = properties.get(property);
        if (value != null) {
            return Integer.parseInt(value);
        }
        return defaultValue;
    }

    public static Integer propertyAsNullableInt(Map<String, String> properties, String property) {
        String value = properties.get(property);
        if (value != null) {
            return Integer.parseInt(value);
        }
        return null;
    }

    public static long propertyAsLong(
            Map<String, String> properties, String property, long defaultValue) {
        String value = properties.get(property);
        if (value != null) {
            return Long.parseLong(value);
        }
        return defaultValue;
    }

    public static Long propertyAsNullableLong(Map<String, String> properties, String property) {
        String value = properties.get(property);
        if (value != null) {
            return Long.parseLong(value);
        }
        return null;
    }

    public static String propertyAsString(
            Map<String, String> properties, String property, String defaultValue) {
        String value = properties.get(property);
        if (value != null) {
            return value;
        }
        return defaultValue;
    }
}