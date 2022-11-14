// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

public class LoadPriority {
    public static final String NORMAL = "NORMAL";
    public static final String HIGH = "HIGH";
    public static final String HIGHEST = "HIGHEST";
    public static final String LOW = "LOW";
    public static final String LOWEST = "LOWEST";

    public static final Integer NORMAL_VALUE = 0;
    public static final Integer HIGH_VALUE = 1;
    public static final Integer HIGHEST_VALUE = 2;
    public static final Integer LOW_VALUE = -1;
    public static final Integer LOWEST_VALUE = -2;

    private static final ImmutableMap<String, Integer> T_NAME_TO_PRIORITY =
            (new ImmutableSortedMap.Builder<String, Integer>(String.CASE_INSENSITIVE_ORDER))
                    .put(HIGHEST, HIGHEST_VALUE)
                    .put(HIGH, HIGH_VALUE)
                    .put(NORMAL, NORMAL_VALUE)
                    .put(LOW, LOW_VALUE)
                    .put(LOWEST, LOWEST_VALUE)
                    .build();

    private static final ImmutableMap<Integer, String> T_PRIORITY_TO_NAME =
            (new ImmutableMap.Builder<Integer, String>())
                    .put(HIGHEST_VALUE, HIGHEST)
                    .put(HIGH_VALUE, HIGH)
                    .put(NORMAL_VALUE, NORMAL)
                    .put(LOW_VALUE, LOW)
                    .put(LOWEST_VALUE, LOWEST)
                    .build();

    public static Integer priorityByName(String name) {
        return T_NAME_TO_PRIORITY.get(name);
    }

    public static String priorityToName(Integer priority) {
        return T_PRIORITY_TO_NAME.get(priority);
    }
}