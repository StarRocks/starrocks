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

package com.starrocks.load.routineload;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Descriptive names of routine load job properties. Each one is another name of a legacy property, and the legacy
 * name stays accepted.
 *
 * <p>A statement is resolved into the legacy names before the existing checks run, so a job keeps its settings in
 * the legacy fields and units, and an older FE that only knows the legacy names still reads them. The descriptive
 * value is kept in the job properties only to report the setting under the name the user chose.
 */
public final class RoutineLoadPropertyNames {
    public static final String LOAD_INTERVAL = "load_interval";
    public static final String MAX_CONSUME_DURATION = "max_consume_duration";
    public static final String LOAD_TIMEOUT = "load_timeout";
    public static final String MAX_CONCURRENCY = "max_concurrency";
    public static final String MAX_ERROR_ROWS = "max_error_rows";
    public static final String ERROR_WINDOW_ROWS = "error_window_rows";

    // descriptive name -> legacy name
    public static final ImmutableMap<String, String> LEGACY_NAMES = ImmutableMap.<String, String>builder()
            .put(LOAD_INTERVAL, CreateRoutineLoadStmt.MAX_BATCH_INTERVAL_SEC_PROPERTY)
            .put(MAX_CONSUME_DURATION, CreateRoutineLoadStmt.TASK_CONSUME_SECOND)
            .put(LOAD_TIMEOUT, CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND)
            .put(MAX_CONCURRENCY, CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY)
            .put(MAX_ERROR_ROWS, CreateRoutineLoadStmt.MAX_ERROR_NUMBER_PROPERTY)
            .put(ERROR_WINDOW_ROWS, CreateRoutineLoadStmt.MAX_BATCH_ROWS_PROPERTY)
            .build();

    // RoutineLoadJob counts error rows in a window of max_batch_rows * 10 rows; error_window_rows is that window.
    static final long ERROR_WINDOW_ROWS_PER_BATCH_ROW = 10;
    // the legacy lower bound max_batch_rows >= 200000 in rows of the window
    static final long MIN_ERROR_WINDOW_ROWS = 200000L * ERROR_WINDOW_ROWS_PER_BATCH_ROW;
    // the legacy lower bound max_batch_interval >= 5 in milliseconds
    static final long MIN_LOAD_INTERVAL_MS = 5000L;

    // A unit is required: a bare "500" could be read as either milliseconds or seconds.
    private static final Pattern DURATION_PATTERN = Pattern.compile("^\\s*(\\d+)\\s*(ms|s)\\s*$",
            Pattern.CASE_INSENSITIVE);

    private RoutineLoadPropertyNames() {
    }

    /**
     * Returns a copy of {@code properties} in which every descriptive name holds its normalized value and is joined
     * by its legacy name, converted to the legacy unit. Fails when a property is given under both names, or when a
     * descriptive value is invalid.
     */
    public static Map<String, String> resolve(Map<String, String> properties) throws AnalysisException {
        Map<String, String> resolved = Maps.newHashMap(properties);
        for (Map.Entry<String, String> entry : LEGACY_NAMES.entrySet()) {
            String name = entry.getKey();
            String legacyName = entry.getValue();
            if (!properties.containsKey(name)) {
                continue;
            }
            if (properties.containsKey(legacyName)) {
                throw new AnalysisException(name + " and " + legacyName
                        + " are two names of the same property and cannot be set together");
            }
            String value = properties.get(name);
            resolved.put(name, normalize(name, value, MIN_LOAD_INTERVAL_MS));
            resolved.put(legacyName, String.valueOf(toLegacyValue(name, value, MIN_LOAD_INTERVAL_MS)));
        }
        checkTimeoutExceedsConsumeDuration(properties, resolved);
        return resolved;
    }

    // A task consumes and times out in whole seconds, so the timeout has to stay larger once both are rounded up. The
    // error names the properties as the user gave them; a pair given under the legacy names is left to the legacy check.
    private static void checkTimeoutExceedsConsumeDuration(Map<String, String> properties,
                                                           Map<String, String> resolved) throws AnalysisException {
        boolean consumeDescriptive = properties.containsKey(MAX_CONSUME_DURATION);
        boolean timeoutDescriptive = properties.containsKey(LOAD_TIMEOUT);
        if (!consumeDescriptive && !timeoutDescriptive) {
            return;
        }
        Long consumeSeconds = parseLongOrNull(resolved.get(CreateRoutineLoadStmt.TASK_CONSUME_SECOND));
        Long timeoutSeconds = parseLongOrNull(resolved.get(CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND));
        if (consumeSeconds != null && timeoutSeconds != null && timeoutSeconds <= consumeSeconds) {
            String consumeName = consumeDescriptive ? MAX_CONSUME_DURATION : CreateRoutineLoadStmt.TASK_CONSUME_SECOND;
            String timeoutName = timeoutDescriptive ? LOAD_TIMEOUT : CreateRoutineLoadStmt.TASK_TIMEOUT_SECOND;
            throw new AnalysisException(timeoutName + " must be larger than " + consumeName
                    + " after rounding up to whole seconds");
        }
    }

    // null for a missing value, or for a malformed legacy value that the legacy check reports
    private static Long parseLongOrNull(String value) {
        try {
            return value == null ? null : Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Returns the descriptive names present in {@code resolvedProperties}, with their normalized values.
     */
    public static Map<String, String> descriptiveValues(Map<String, String> resolvedProperties) {
        Map<String, String> values = Maps.newHashMap();
        for (String name : LEGACY_NAMES.keySet()) {
            if (resolvedProperties.containsKey(name)) {
                values.put(name, resolvedProperties.get(name));
            }
        }
        return values;
    }

    /**
     * Whether a job keeps a setting under a descriptive name, that is, one of its settings was given that way.
     */
    public static boolean hasDescriptiveValues(Map<String, String> jobProperties) {
        return LEGACY_NAMES.keySet().stream().anyMatch(jobProperties::containsKey);
    }

    /**
     * Drops the descriptive value of every property that {@code changedProperties} sets through its legacy name
     * only, so the job is reported under the name it was last set with.
     */
    public static void forgetReplacedDescriptiveValues(Map<String, String> changedProperties,
                                                       Map<String, String> jobProperties) {
        for (Map.Entry<String, String> entry : LEGACY_NAMES.entrySet()) {
            if (changedProperties.containsKey(entry.getValue()) && !changedProperties.containsKey(entry.getKey())) {
                jobProperties.remove(entry.getKey());
            }
        }
    }

    /**
     * Returns the descriptive value stored for {@code name} when it still converts to {@code legacyValue}, or null.
     * A stored value stops matching when an FE that only knows the legacy name changed the setting.
     */
    public static String matchingDescriptiveValue(Map<String, String> jobProperties, String name, long legacyValue) {
        String value = jobProperties.get(name);
        if (value == null) {
            return null;
        }
        try {
            // the value was checked against the minimum when it was set
            return toLegacyValue(name, value, 0) == legacyValue ? value : null;
        } catch (AnalysisException e) {
            return null;
        }
    }

    // parses a duration such as "500ms" or "2s" into milliseconds
    private static long parseDurationMs(String name, String value) throws AnalysisException {
        Matcher matcher = value == null ? null : DURATION_PATTERN.matcher(value);
        if (matcher == null || !matcher.matches()) {
            throw new AnalysisException(name + " should be a duration with a unit, such as \"500ms\" or \"2s\", but is "
                    + value);
        }
        long amount;
        try {
            amount = Long.parseLong(matcher.group(1));
        } catch (NumberFormatException e) {
            throw new AnalysisException(name + " is out of range: " + value);
        }
        boolean seconds = matcher.group(2).equalsIgnoreCase("s");
        if (seconds && amount > Long.MAX_VALUE / 1000) {
            throw new AnalysisException(name + " is out of range: " + value);
        }
        return seconds ? amount * 1000 : amount;
    }

    public static String formatDurationMs(long ms) {
        return ms % 1000 == 0 ? (ms / 1000) + "s" : ms + "ms";
    }

    private static String normalize(String name, String value, long minLoadIntervalMs) throws AnalysisException {
        switch (name) {
            case LOAD_INTERVAL:
            case MAX_CONSUME_DURATION:
            case LOAD_TIMEOUT:
                return formatDurationMs(checkedDurationMs(name, value, minLoadIntervalMs));
            default:
                return String.valueOf(checkedCount(name, value));
        }
    }

    // converts a descriptive value to the unit of the legacy property
    private static long toLegacyValue(String name, String value, long minLoadIntervalMs) throws AnalysisException {
        switch (name) {
            case LOAD_INTERVAL:
            case MAX_CONSUME_DURATION:
            case LOAD_TIMEOUT:
                return ceilSeconds(checkedDurationMs(name, value, minLoadIntervalMs));
            case ERROR_WINDOW_ROWS:
                return ceilDiv(checkedCount(name, value), ERROR_WINDOW_ROWS_PER_BATCH_ROW);
            default:
                return checkedCount(name, value);
        }
    }

    private static long checkedDurationMs(String name, String value, long minLoadIntervalMs)
            throws AnalysisException {
        long ms = parseDurationMs(name, value);
        if (name.equals(LOAD_INTERVAL) && ms < minLoadIntervalMs) {
            throw new AnalysisException(name + " should be at least " + formatDurationMs(minLoadIntervalMs)
                    + ", but is " + value);
        }
        if (ms <= 0) {
            throw new AnalysisException(name + " should be larger than 0, but is " + value);
        }
        return ms;
    }

    private static long checkedCount(String name, String value) throws AnalysisException {
        long count;
        try {
            count = Long.parseLong(value == null ? null : value.trim());
        } catch (NumberFormatException e) {
            throw new AnalysisException(name + " should be an integer, but is " + value);
        }
        switch (name) {
            case MAX_CONCURRENCY:
                if (count <= 0 || count > Integer.MAX_VALUE) {
                    throw new AnalysisException(name + " should be between 1 and " + Integer.MAX_VALUE
                            + ", but is " + value);
                }
                break;
            case MAX_ERROR_ROWS:
                if (count < 0) {
                    throw new AnalysisException(name + " should be at least 0, but is " + value);
                }
                break;
            case ERROR_WINDOW_ROWS:
                if (count < MIN_ERROR_WINDOW_ROWS) {
                    throw new AnalysisException(name + " should be at least " + MIN_ERROR_WINDOW_ROWS
                            + ", but is " + value);
                }
                break;
            default:
                break;
        }
        return count;
    }

    private static long ceilSeconds(long ms) {
        return ceilDiv(ms, 1000);
    }

    // rounds up without the overflow of (value + divisor - 1) near Long.MAX_VALUE; value is never negative here
    private static long ceilDiv(long value, long divisor) {
        return value / divisor + (value % divisor == 0 ? 0 : 1);
    }
}
