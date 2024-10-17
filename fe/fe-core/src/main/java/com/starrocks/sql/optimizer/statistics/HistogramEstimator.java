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

import com.starrocks.qe.ConnectContext;

import java.util.Collections;
import java.util.List;

/**
 * Use histogram to estimate cardinality
 */
public class HistogramEstimator {

    /**
     * Return null if failed to estimate
     */
    public static Double estimateEqualToSelectivity(ColumnStatistic left, ColumnStatistic right) {
        ConnectContext context = ConnectContext.get();
        if (context != null && !context.getSessionVariable().isCboEnableHistogramJoinEstimation()) {
            return null;
        }
        if (left.getHistogram() == null || right.getHistogram() == null) {
            return null;
        }

        Histogram lhs = left.getHistogram();
        Histogram rhs = right.getHistogram();
        for (Bucket bucket : lhs.getBuckets()) {
            Collections.binarySearch(rhs.getBuckets(), new Bucket(1, 1, 1, 1));
            List<Bucket> overlapped = rhs.getOverlapped(bucket);
            long overlapCount = 0;
            for (Bucket overlap : overlapped) {
                StatisticRangeValues leftRange =
                        new StatisticRangeValues(bucket.getLower(), bucket.getUpper(), bucket.getUpperRepeats());
                StatisticRangeValues rightRange =
                        new StatisticRangeValues(overlap.getLower(), overlap.getUpper(), bucket.getUpperRepeats());
                double overlapLength = leftRange.overlapLength(rightRange);
                overlapCount += overlapLength;
            }
            return 1.0 * overlapCount / lhs.getTotalRows();
        }
    }

}
