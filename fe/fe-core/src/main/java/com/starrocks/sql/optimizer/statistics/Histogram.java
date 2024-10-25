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

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.statistic.StatisticUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Histogram {

    private final List<Bucket> buckets;
    private final Map<String, Long> mcv;

    public Histogram(List<Bucket> buckets, Map<String, Long> mcv) {
        this.buckets = buckets;
        this.mcv = mcv;

    }

    public long getTotalRows() {
        long totalRows = 0;
        if (buckets != null && !buckets.isEmpty()) {
            totalRows += buckets.get(buckets.size() - 1).getCount();
        }
        if (mcv != null) {
            totalRows += mcv.values().stream().reduce(Long::sum).orElse(0L);
        }
        return Math.max(1, totalRows);
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public Map<String, Long> getMCV() {
        return mcv;
    }

    public String getMcvString() {
        int printMcvSize = 5;
        StringBuilder sb = new StringBuilder();
        sb.append("MCV: [");
        mcv.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(printMcvSize)
                .forEach(entry -> sb.append("[").append(entry.getKey()).append(":").append(entry.getValue()).append("]"));
        sb.append("]");
        return sb.toString();
    }

    public Optional<Long> getRowCountInBucket(ConstantOperator constantOperator, double distinctValuesCount) {
        Optional<Double> valueOpt = StatisticUtils.convertStatisticsToDouble(constantOperator.getType(),
                constantOperator.toString());
        if (!valueOpt.isPresent()) {
            return Optional.empty();
        }

        double value = valueOpt.get();

        int left = 0;
        int right = buckets.size() - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            Bucket bucket = buckets.get(mid);

            if (bucket.getLower() <= value && value < bucket.getUpper()) {
                long rowCount = bucket.getCount() - bucket.getUpperRepeats();

                if (mid > 0) {
                    rowCount -= buckets.get(mid - 1).getCount();
                }

                if (constantOperator.getType().isFixedPointType()) {
                    rowCount = (long) Math.ceil(Math.max(1, rowCount / Math.max(1, (bucket.getUpper() - bucket.getLower()))));
                } else {
                    rowCount = (long) Math.ceil(Math.max(1, rowCount / Math.max(1, distinctValuesCount / buckets.size())));
                }

                return Optional.of(rowCount);
            } else if (bucket.getUpper() == value) {
                return Optional.of(bucket.getUpperRepeats());
            }

            if (value < bucket.getLower()) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return Optional.empty();
    }
}
