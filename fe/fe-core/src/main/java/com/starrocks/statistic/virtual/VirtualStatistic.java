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

package com.starrocks.statistic.virtual;

import com.google.common.base.CharMatcher;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Represents a virtual statistic that is derived from an existing column.
 * Virtual statistics allow collecting statistics on transformed/derived expressions
 * from base columns (e.g., unnest of an array column).
 */
public interface VirtualStatistic {

    UnnestStatistic UNNEST = new UnnestStatistic();
    List<VirtualStatistic> INSTANCES = List.of(UNNEST);

    /**
     * Used as name prefix for virtual entries in the stats table.
     */
    static String getColumnNamePrefix() {
        return "VIRTUAL_STATISTIC";
    }

    static boolean isVirtualColumnName(String columnName) {
        return columnName.startsWith(getColumnNamePrefix());
    }

    private static int countUnderscore(String string) {
        return CharMatcher.is('_').countIn(string);
    }

    static Optional<VirtualStatistic> fromColumnName(String columnName) {
        if (!isVirtualColumnName(columnName)) {
            return Optional.empty();
        }

        if (countUnderscore(columnName) <
                (countUnderscore(getColumnNamePrefix()) + 2 /* column base name + stat name underscore */)) {
            return Optional.empty();
        }

        final var lastUnderscoreIndex = columnName.lastIndexOf("_");
        final var statisticName = columnName.substring(lastUnderscoreIndex + 1);
        for (VirtualStatistic statistic : INSTANCES) {
            if (statistic.getName().equals(statisticName)) {
                return Optional.of(statistic);
            }
        }

        return Optional.empty();
    }

    default String getBaseColumn(String columnName) {
        if (!isVirtualColumnName(columnName)) {
            throw new IllegalArgumentException("Not a virtual column name: " + columnName);
        }

        final var withoutPrefix = columnName.substring(getColumnNamePrefix().length() + 1); // Remove prefix.

        return withoutPrefix.substring(0, withoutPrefix.length() - (getName().length() + 1)); // Remove suffix.
    }

    /**
     * Gets the virtual column name for a given base column and virtual statistic.
     */
    default String getVirtualColumnName(String baseColumnName) {
        return String.format("%s_%s_%s", getColumnNamePrefix(), baseColumnName, getName());
    }

    /**
     * Whether creating this virtual statistic is enabled.
     */
    default boolean isEnabledInStatsJobProperties(Map<String, String> statsJobProperties) {
        final var analyzePropertyKey = getAnalyzePropertyKey();
        if (statsJobProperties.containsKey(analyzePropertyKey)) {
            final var valueString = statsJobProperties.get(analyzePropertyKey);
            return Boolean.parseBoolean(valueString);
        }

        return false;
    }

    /**
     * Whether the statistic requires a lateral join in the FROM clause to be executed.
     */
    default boolean requiresLateralJoin() {
        return false;
    }

    /**
     * The (unique) name of the virtual statistic. Note: The name may not contain an underscore (_).
     */
    String getName();

    /**
     * The property name to pass in the ANALYZE to enable creating this virtual statistic.
     */
    String getAnalyzePropertyKey();

    /**
     * Checks if the given column type can produce this virtual statistic.
     */
    boolean appliesTo(Type columnType);

    /**
     * Returns the type of the virtual statistic given the source column type.
     */
    Type getVirtualExpressionType(Type sourceType);

    /**
     * Given a column, returns the expression to use for collecting statistics.
     */
    String getVirtualExpression(String columnName);

    /*
     * Whether querying the stats is enabled.
     */
    boolean isQueryingEnabled();
}
