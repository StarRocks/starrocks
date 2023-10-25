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

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.analyzer.SemanticException;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Convert external table partitions to MV partitions.
 * <p>
 * External tables support various partitions type, like Hive column partition, each partition represents a value.
 * But for MySQL external table, which supports native range-partition/range-column-partition/hash-partition.
 * <p>
 * Meanwhile, MV also supports various types of partitions, including RANGE/LIST...
 */
abstract class MvPartitionConverter {

    abstract Map<String, Range<PartitionKey>> convert(Map<String, PartitionKey> partitions);

    public static MvPartitionConverter buildConverter(Expr partitionExpr, Column partitionColumn) {
        boolean isConvertToDate = PartitionUtil.isConvertToDate(partitionExpr, partitionColumn);
        PrimitiveType partitionColumnType = partitionColumn.getPrimitiveType();

        // TODO: support more converter types
        if (partitionColumnType == PrimitiveType.DATE || partitionColumnType.isNumericType()) {
            return new ArithmeticTypePartitionConverter(partitionColumnType);
        } else {
            return new ConsecutivePartitionConverter(isConvertToDate, partitionColumnType);
        }
    }

    /**
     * Convert the col=value to [value, value+1), so the data type must be arithmetic
     */
    static class ArithmeticTypePartitionConverter extends MvPartitionConverter {

        private PrimitiveType columnType;

        public ArithmeticTypePartitionConverter(PrimitiveType columnType) {
            this.columnType = columnType;
        }

        @Override
        public Map<String, Range<PartitionKey>> convert(Map<String, PartitionKey> sourcePartitions) {
            Map<String, Range<PartitionKey>> mvPartitionRangeMap = new LinkedHashMap<>();
            boolean hasNull = false;
            String nullPartitionName = null;
            for (Map.Entry<String, PartitionKey> entry : sourcePartitions.entrySet()) {
                String partitionName = entry.getKey();
                PartitionKey startKey = entry.getValue();

                if (startKey.isNullLiteral()) {
                    hasNull = true;
                    nullPartitionName = partitionName;
                } else {
                    PartitionKey endKey = startKey.successor();
                    mvPartitionRangeMap.put(partitionName, Range.closedOpen(startKey, endKey));
                }
            }

            // create the null partition as [0000-00-00, MIN_VALUE) if has null
            if (hasNull) {
                try {
                    PartitionKey minLiteral = PartitionKey.createInfinityPartitionKeyWithType(
                            ImmutableList.of(columnType), false);
                    PartitionKey minValue = sourcePartitions.values().stream()
                            .filter(x -> !x.isNullLiteral())
                            .min(Comparator.naturalOrder())
                            .orElseThrow(() -> new SemanticException("only null partition "));
                    mvPartitionRangeMap.put(nullPartitionName, Range.closedOpen(minLiteral, minValue));
                } catch (AnalysisException ignored) {
                }
            }

            return mvPartitionRangeMap;
        }
    }

    /**
     * Convert the partitions [[a, b, c]] to [[MIN, a), [a, b), [b, c)].
     * So the original values must be consecutive.
     */
    static class ConsecutivePartitionConverter extends MvPartitionConverter {

        private final boolean isConvertToDate;
        private final PrimitiveType partitionColumnType;

        public ConsecutivePartitionConverter(boolean isConvertToDate, PrimitiveType partitionColumnType) {
            this.isConvertToDate = isConvertToDate;
            this.partitionColumnType = partitionColumnType;
        }

        private static PartitionKey convertToDate(PartitionKey partitionKey) {
            PartitionKey newPartitionKey = new PartitionKey();
            String dateLiteral = partitionKey.getKeys().get(0).getStringValue();
            LocalDateTime dateValue = DateUtils.parseStrictDateTime(dateLiteral);
            try {
                newPartitionKey.pushColumn(new DateLiteral(dateValue, Type.DATE), PrimitiveType.DATE);
                return newPartitionKey;
            } catch (AnalysisException e) {
                throw new SemanticException("create date string:{} failed:",
                        partitionKey.getKeys().get(0).getStringValue(),
                        e);
            }
        }

        @Override
        public Map<String, Range<PartitionKey>> convert(Map<String, PartitionKey> sourcePartitions) {
            Map<String, Range<PartitionKey>> mvPartitionRangeMap = new LinkedHashMap<>();
            PrimitiveType resultColumnType = isConvertToDate ? PrimitiveType.DATE : partitionColumnType;

            // sort the partitions by value
            LinkedHashMap<String, PartitionKey> sortedPartitionLinkMap = sourcePartitions.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(PartitionKey::compareTo))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1,
                            LinkedHashMap::new));

            // build the range map by discrete values
            int index = 0;
            PartitionKey lastPartitionKey = null;
            String lastPartitionName = null;
            for (Map.Entry<String, PartitionKey> entry : sortedPartitionLinkMap.entrySet()) {
                if (index == 0) {
                    lastPartitionName = entry.getKey();
                    lastPartitionKey = isConvertToDate ? convertToDate(entry.getValue()) : entry.getValue();
                    if (lastPartitionKey.getKeys().get(0).isNullable()) {
                        // If partition key is NULL literal, rewrite it to min value.
                        try {
                            lastPartitionKey = PartitionKey.createInfinityPartitionKeyWithType(
                                    ImmutableList.of(resultColumnType), false);
                        } catch (Exception ignored) {
                        }
                    }
                    ++index;
                    continue;
                }
                Preconditions.checkState(!mvPartitionRangeMap.containsKey(lastPartitionName));
                PartitionKey upperBound = isConvertToDate ? convertToDate(entry.getValue()) : entry.getValue();
                mvPartitionRangeMap.put(lastPartitionName, Range.closedOpen(lastPartitionKey, upperBound));
                lastPartitionName = entry.getKey();
                lastPartitionKey = upperBound;
            }
            if (lastPartitionName != null) {
                PartitionKey endKey = new PartitionKey();
                endKey.pushColumn(PartitionUtil.addOffsetForLiteralUnchecked(lastPartitionKey.getKeys().get(0), 1),
                        resultColumnType);

                Preconditions.checkState(!mvPartitionRangeMap.containsKey(lastPartitionName));
                mvPartitionRangeMap.put(lastPartitionName, Range.closedOpen(lastPartitionKey, endKey));
            }
            return mvPartitionRangeMap;
        }
    }
}
