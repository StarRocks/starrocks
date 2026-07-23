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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.base.Preconditions;

/**
 * Parser for the legacy text form of {@link ColumnStatistic} (i.e. the output of
 * {@link ColumnStatistic#toString()}), kept solely so that query dumps produced by older StarRocks
 * versions can still be replayed.
 **/
public final class LegacyColumnStatisticParser {

    private LegacyColumnStatisticParser() {
    }

    public static ColumnStatistic.Builder parse(String columnStatistic) {
        int endIndex = columnStatistic.indexOf(']');
        String valueString = columnStatistic.substring(1, endIndex);
        String typeString = endIndex == columnStatistic.length() - 1 ? "" : columnStatistic.substring(endIndex + 2);

        // Check if this is the new labeled format (contains "MIN:")
        boolean isLabeledFormat = valueString.contains("MIN:");

        double minValue;
        double maxValue;
        double nullsFraction;
        double averageRowSize;
        double distinctValues;

        if (isLabeledFormat) {
            // Parse labeled format: MIN: 1.0, MAX: 100.0, NULLS: 0.0, ROS: 8.0, NDV: 50.0
            String[] parts = valueString.split(",");
            Preconditions.checkState(parts.length == 5,
                    "statistic value: %s is illegal", valueString);

            minValue = parseLabeledValue(parts[0].trim(), "MIN:");
            maxValue = parseLabeledValue(parts[1].trim(), "MAX:");
            nullsFraction = parseLabeledValue(parts[2].trim(), "NULLS:");
            averageRowSize = parseLabeledValue(parts[3].trim(), "ROS:");
            distinctValues = parseLabeledValue(parts[4].trim(), "NDV:");
        } else {
            // Parse old format: 1.0, 100.0, 0.0, 8.0, 50.0
            String[] valueArray = valueString.split(",");
            Preconditions.checkState(valueArray.length == 5,
                    "statistic value: %s is illegal", valueString);

            minValue = Double.parseDouble(valueArray[0]);
            maxValue = Double.parseDouble(valueArray[1]);
            nullsFraction = Double.parseDouble(valueArray[2]);
            averageRowSize = Double.parseDouble(valueArray[3]);
            distinctValues = Double.parseDouble(valueArray[4]);
        }

        if (minValue > maxValue) {
            minValue = Double.NEGATIVE_INFINITY;
            maxValue = Double.POSITIVE_INFINITY;
        }

        if (distinctValues <= 0) {
            distinctValues = 1;
        }

        ColumnStatistic.Builder
                builder = new ColumnStatistic.Builder(minValue, maxValue, nullsFraction, averageRowSize, distinctValues);
        ColumnStatistic.StatisticType parsedType = parseTrailingStatisticType(typeString);
        if (parsedType != null) {
            builder.setType(parsedType);
        } else if (builder.build().isUnknownValue()) {
            builder.setType(ColumnStatistic.StatisticType.UNKNOWN);
        }
        return builder;
    }

    // The tail may carry "COS: .."/"MCV: [..]" before the type token; take only the trailing enum
    // so a dump with a histogram/collection-size no longer breaks StatisticType.valueOf on replay.
    private static ColumnStatistic.StatisticType parseTrailingStatisticType(String typeString) {
        if (typeString == null) {
            return null;
        }
        String trimmed = typeString.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        for (ColumnStatistic.StatisticType statisticType : ColumnStatistic.StatisticType.values()) {
            String name = statisticType.name();
            if (trimmed.equals(name) || trimmed.endsWith(" " + name)) {
                return statisticType;
            }
        }
        return null;
    }

    private static double parseLabeledValue(String part, String label) {
        Preconditions.checkState(part.startsWith(label),
                "Expected label %s but got: %s", label, part);
        return Double.parseDouble(part.substring(label.length()).trim());
    }
}


